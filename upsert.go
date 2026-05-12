package vapordb

import (
	"regexp"
	"strings"
)

// ── = ANY(…) / <> ALL(…) rewriter ────────────────────────────────────────────

// anyEqRE matches  = ANY(  (case-insensitive) and replaces it with  IN (
var anyEqRE = regexp.MustCompile(`(?i)=\s*ANY\s*\(`)

// allNeqRE matches  <> ALL(  or  != ALL(  and replaces with  NOT IN (
var allNeqRE = regexp.MustCompile(`(?i)(<>|!=)\s*ALL\s*\(`)

// rewriteAnyAll rewrites PostgreSQL-style set operators to standard IN / NOT IN
// so the MySQL-dialect parser can handle them:
//
//	col = ANY(…)   →  col IN (…)
//	col <> ALL(…)  →  col NOT IN (…)
//	col != ALL(…)  →  col NOT IN (…)
func rewriteAnyAll(sql string) string {
	sql = anyEqRE.ReplaceAllString(sql, "IN (")
	sql = allNeqRE.ReplaceAllString(sql, "NOT IN (")
	return sql
}

// partialIndexRE matches the optional "WHERE pred" between ON CONFLICT (…) and DO.
// This is the partial-index predicate form (e.g. ON CONFLICT (id) WHERE deleted_at IS NULL DO …).
// vapordb strips the predicate before further processing because it treats every
// conflict-column match as a conflict regardless of additional row predicates.
//
// The predicate must not contain parentheses; function calls inside the partial
// index predicate are not supported and are a known limitation.
var partialIndexRE = regexp.MustCompile(
	`(?i)(ON\s+CONFLICT\s*\([^)]+\))\s+WHERE\s+[^()\n]+\s+(DO\b)`,
)

// onConflictRE matches the PostgreSQL ON CONFLICT clause at the end of an
// INSERT statement. Two forms are supported:
//
//	ON CONFLICT (col, …) DO UPDATE SET col = EXCLUDED.col, …
//	ON CONFLICT (col, …) DO NOTHING
//
// The partial-index WHERE predicate (Gap 3) is stripped by partialIndexRE before
// this regex is evaluated.
//
// Matching is case-insensitive. The captured groups are:
//
//	[1] – comma-separated conflict column list  (e.g. "id")
//	[2] – everything after DO:  "UPDATE SET …"  or  "NOTHING"
var onConflictRE = regexp.MustCompile(
	`(?i)\s+ON\s+CONFLICT\s*\(([^)]+)\)\s+DO\s+(UPDATE\s+SET\s+[\s\S]+|NOTHING)\s*$`,
)

// excludedRE rewrites   EXCLUDED.col   to   VALUES(col)
// so the MySQL-dialect parser accepts it.
var excludedRE = regexp.MustCompile(`(?i)EXCLUDED\.(\w+)`)

// rewriteOnConflict detects a PostgreSQL-style ON CONFLICT clause, rewrites it
// to the MySQL ON DUPLICATE KEY UPDATE form that the sql-parser understands, and
// returns the modified SQL together with the conflict-column names.
//
// Gap 4 — optimistic-lock WHERE: if the DO UPDATE SET … body ends with a WHERE
// clause (e.g. "WHERE table.version = :v"), it is stripped from the MySQL rewrite
// and returned separately as upsertWhere so that execInsert can evaluate it
// against the existing row before applying the update assignments.
//
// If no ON CONFLICT clause is present the original SQL is returned unchanged
// with an empty slice and empty upsertWhere.
func rewriteOnConflict(sql string) (rewritten string, conflictCols []string, doNothing bool, upsertWhere string) {
	// Gap 3: strip the partial-index WHERE predicate (e.g. ON CONFLICT (id) WHERE pred DO …)
	// before the main regex runs.
	sql = partialIndexRE.ReplaceAllString(sql, "$1 $2")

	m := onConflictRE.FindStringSubmatchIndex(sql)
	if m == nil {
		return sql, nil, false, ""
	}

	// m[2:4] → conflict columns, m[4:6] → DO … body
	colsPart := sql[m[2]:m[3]]
	doBody := strings.TrimSpace(sql[m[4]:m[5]])

	for _, c := range strings.Split(colsPart, ",") {
		if col := strings.TrimSpace(c); col != "" {
			conflictCols = append(conflictCols, strings.ToLower(col))
		}
	}

	// Strip the ON CONFLICT … clause from the original SQL.
	base := sql[:m[0]]

	if strings.EqualFold(doBody, "NOTHING") {
		return base, conflictCols, true, ""
	}

	// DO UPDATE SET … – convert EXCLUDED.col references and append as
	// MySQL ON DUPLICATE KEY UPDATE.
	setPart := doBody[len("UPDATE SET "):]
	setPart = excludedRE.ReplaceAllStringFunc(setPart, func(match string) string {
		inner := excludedRE.FindStringSubmatch(match)
		return "VALUES(" + inner[1] + ")"
	})

	// Gap 4: detect and strip a trailing WHERE clause (optimistic-lock predicate).
	// e.g. "col = VALUES(col) WHERE table.version = 5" → setPart stripped, upsertWhere = "table.version = 5"
	if idx := findTrailingWHERE(setPart); idx >= 0 {
		upsertWhere = strings.TrimSpace(setPart[idx+5:]) // +5 = len("WHERE")
		setPart = strings.TrimSpace(setPart[:idx])
	}

	return base + " ON DUPLICATE KEY UPDATE " + setPart, conflictCols, false, upsertWhere
}

// findTrailingWHERE returns the byte index of the LAST standalone WHERE keyword
// in s that is not inside a single-quoted string literal, or -1 if none found.
func findTrailingWHERE(s string) int {
	result := -1
	i := 0
	n := len(s)
	for i < n {
		if s[i] == '\'' {
			i = fSkipQuote(s, i) // reuse helper from rewrite_filter.go
			continue
		}
		if i+5 <= n && strings.EqualFold(s[i:i+5], "WHERE") {
			before := i == 0 || !fIsIdentChar(s[i-1])
			after := i+5 >= n || !fIsIdentChar(s[i+5])
			if before && after {
				result = i
			}
		}
		i++
	}
	return result
}
