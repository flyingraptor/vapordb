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

// onConflictRE matches the PostgreSQL ON CONFLICT clause at the end of an
// INSERT statement. Two forms are supported:
//
//	ON CONFLICT (col, …) DO UPDATE SET col = EXCLUDED.col, …
//	ON CONFLICT (col, …) DO NOTHING
//
// Matching is case-insensitive. The captured groups are:
//
//	[1] – comma-separated conflict column list  (e.g. "id")
//	[2] – everything after DO:  "UPDATE SET …"  or  "NOTHING"
var onConflictRE = regexp.MustCompile(
	`(?i)\s+ON\s+CONFLICT\s*\(([^)]+)\)\s+DO\s+(UPDATE\s+SET\s+.+|NOTHING)\s*$`,
)

// excludedRE rewrites   EXCLUDED.col   to   VALUES(col)
// so the MySQL-dialect parser accepts it.
var excludedRE = regexp.MustCompile(`(?i)EXCLUDED\.(\w+)`)

// rewriteOnConflict detects a PostgreSQL-style ON CONFLICT clause, rewrites it
// to the MySQL ON DUPLICATE KEY UPDATE form that the sql-parser understands, and
// returns the modified SQL together with the conflict-column names.
//
// If no ON CONFLICT clause is present the original SQL is returned unchanged
// with an empty slice.
func rewriteOnConflict(sql string) (rewritten string, conflictCols []string, doNothing bool) {
	m := onConflictRE.FindStringSubmatchIndex(sql)
	if m == nil {
		return sql, nil, false
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
		return base, conflictCols, true
	}

	// DO UPDATE SET … – convert EXCLUDED.col references and append as
	// MySQL ON DUPLICATE KEY UPDATE.
	setPart := doBody[len("UPDATE SET "):]
	setPart = excludedRE.ReplaceAllStringFunc(setPart, func(match string) string {
		inner := excludedRE.FindStringSubmatch(match)
		return "VALUES(" + inner[1] + ")"
	})
	return base + " ON DUPLICATE KEY UPDATE " + setPart, conflictCols, false
}
