package vapordb

import (
	"fmt"
	"regexp"
	"strings"
)

// This file is the pre-parse SQL rewrite layer. The MySQL-dialect parser
// (github.com/xwb1989/sqlparser) does not understand several PostgreSQL /
// standard-SQL constructs, so every entry point first rewrites the SQL text
// into an equivalent the parser accepts. Keeping every rewriter and the chain
// order in one file makes the (order-sensitive) pipeline auditable in one place.
//
// Pipeline order (shared base chain, applied by rewritePreParse):
//   1. rewriteDoubleQuotedIdents — "ident" → `ident`. Runs first so quoted
//      identifiers cannot be mistaken for string literals by later passes;
//      genuine single-quoted string literals are skipped verbatim.
//   2. rewriteJSONOps            — ->, ->>, @>, <@ → json_extract / json_contains.
//   3. rewriteILIKE              — col ILIKE x → LOWER(col) LIKE LOWER(x).
//   4. rewriteFilterAggregates   — AGG(x) FILTER (WHERE c) → AGG(CASE WHEN c THEN x END).
//   5. rewriteFullOuterJoins     — FULL [OUTER] JOIN → STRAIGHT_JOIN marker
//      (walkTableExpr maps it back to full-join semantics after parsing).
//
// Two further passes run only on the DML entry points, after the base chain
// (see rewriteDML): rewriteAnyAll (= ANY / <> ALL → IN / NOT IN) and
// rewriteOnConflict (ON CONFLICT extraction, defined in upsert.go). The SELECT
// path (DB.Query) instead runs extractWindowFuncs between the base chain and
// rewriteAnyAll, because window extraction must see the un-rewritten set
// operators. rewriteFilterAggregates and rewriteOnConflict have enough
// dedicated machinery that they keep their own files (this one references them).

// rewritePreParse applies the shared base rewrite chain that every entry point
// runs before parsing. See the file-level comment for the ordering rationale.
func rewritePreParse(sql string) string {
	return rewriteFullOuterJoins(
		rewriteFilterAggregates(
			rewriteILIKE(
				rewriteJSONOps(
					rewriteDoubleQuotedIdents(sql)))))
}

// rewriteDML applies the full pre-parse chain for INSERT / UPDATE / DELETE:
// the shared base chain, then the PostgreSQL set-operator rewrite, then
// ON CONFLICT extraction. It returns the rewritten SQL plus the conflict-target
// metadata parsed out of any ON CONFLICT clause. Both DB.Exec and the RETURNING
// DML path call this so the ordering lives in exactly one place.
func rewriteDML(sql string) (rewritten string, conflictCols []string, doNothing bool, upsertWhere string) {
	return rewriteOnConflict(rewriteAnyAll(rewritePreParse(sql)))
}

// rewriteDoubleQuotedIdents converts standard-SQL / PostgreSQL double-quoted
// identifiers to MySQL backtick-quoted identifiers so the MySQL-dialect parser
// accepts column and table names that happen to be reserved words (e.g. "name",
// "type", "value", "key", "status", "schema").
//
// Only genuine identifier quotes are rewritten. Single-quoted string literals
// are skipped verbatim, including the ” escape sequence inside them. The ""
// escape sequence inside a double-quoted identifier (standard SQL) is preserved
// as a literal double-quote inside the backtick identifier.
//
//	SELECT "name", "type" FROM "orders"   →   SELECT `name`, `type` FROM `orders`
func rewriteDoubleQuotedIdents(sql string) string {
	// Fast path: no double-quote in the input.
	if !strings.ContainsRune(sql, '"') {
		return sql
	}
	var b strings.Builder
	b.Grow(len(sql))
	i := 0
	for i < len(sql) {
		ch := sql[i]
		switch ch {
		case '\'': // single-quoted string literal — copy verbatim
			b.WriteByte(ch)
			i++
			for i < len(sql) {
				c := sql[i]
				b.WriteByte(c)
				i++
				if c == '\'' {
					// '' is the SQL escape for a literal single-quote inside a string.
					if i < len(sql) && sql[i] == '\'' {
						b.WriteByte(sql[i])
						i++
					} else {
						break
					}
				}
			}
		case '"': // double-quoted identifier → backtick-quoted
			b.WriteByte('`')
			i++
			for i < len(sql) {
				c := sql[i]
				i++
				if c == '"' {
					// "" is the standard-SQL escape for a literal double-quote inside
					// a double-quoted identifier. Preserve it as a literal " inside
					// the backtick identifier.
					if i < len(sql) && sql[i] == '"' {
						b.WriteByte('"')
						i++
					} else {
						break // closing quote
					}
				} else {
					b.WriteByte(c)
				}
			}
			b.WriteByte('`')
		default:
			b.WriteByte(ch)
			i++
		}
	}
	return b.String()
}

// jsonArrowTextRE rewrites col->>'$.path' → json_unquote(json_extract(col, '$.path'))
// Must be processed before jsonArrowRE to avoid a partial ->> match.
var jsonArrowTextRE = regexp.MustCompile(`(?i)(\w+)\s*->>\s*('[^']*')`)

// jsonArrowRE rewrites col->'$.path' → json_extract(col, '$.path')
var jsonArrowRE = regexp.MustCompile(`(?i)(\w+)\s*->\s*('[^']*')`)

// jsonContainsAtRE rewrites col @> expr → json_contains(col, expr)
var jsonContainsAtRE = regexp.MustCompile(`(?i)(\w+)\s*@>\s*(\w+|'[^']*')`)

// jsonContainedInRE rewrites col <@ expr → json_contains(expr, col)
var jsonContainedInRE = regexp.MustCompile(`(?i)(\w+)\s*<@\s*(\w+|'[^']*')`)

// rewriteJSONOps rewrites PostgreSQL/MySQL JSON shorthand operators into
// equivalent json_extract / json_contains function calls that the MySQL-dialect
// parser understands.
func rewriteJSONOps(sql string) string {
	sql = jsonArrowTextRE.ReplaceAllString(sql, "json_unquote(json_extract($1, $2))")
	sql = jsonArrowRE.ReplaceAllString(sql, "json_extract($1, $2)")
	sql = jsonContainsAtRE.ReplaceAllString(sql, "json_contains($1, $2)")
	sql = jsonContainedInRE.ReplaceAllString(sql, "json_contains($2, $1)")
	return sql
}

// rewriteILIKE rewrites PostgreSQL ILIKE / NOT ILIKE to case-insensitive LIKE
// comparisons that the MySQL-dialect parser understands:
//
//	col ILIKE 'pattern'     →  LOWER(col) LIKE LOWER('pattern')
//	col NOT ILIKE 'pattern' →  LOWER(col) NOT LIKE LOWER('pattern')
//
// Both sides of the operator are wrapped in LOWER() so the comparison is truly
// case-insensitive regardless of collation. Single-quoted string literals in
// the input are skipped verbatim so occurrences of the word ILIKE inside a
// string value are never touched.
//
// Left-hand operand may be a simple identifier, a qualified identifier
// (table.column), a backtick-quoted identifier, or a parenthesised expression.
// Right-hand operand may be a string literal, a parenthesised expression, or
// a parameter placeholder (?, :name, $1).
func rewriteILIKE(sql string) string {
	if len(sql) < 5 {
		return sql
	}
	upper := strings.ToUpper(sql)
	if !strings.Contains(upper, "ILIKE") {
		return sql
	}

	isWordChar := func(c byte) bool {
		return (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') ||
			(c >= '0' && c <= '9') || c == '_'
	}

	type patch struct {
		start, end int
		repl       string
	}
	var patches []patch

	n := len(sql)
	i := 0

	for i < n {
		// Skip single-quoted string literals verbatim.
		if sql[i] == '\'' {
			i++
			for i < n {
				c := sql[i]
				i++
				if c == '\'' {
					if i < n && sql[i] == '\'' {
						i++ // '' escape sequence
					} else {
						break
					}
				}
			}
			continue
		}

		// Detect "ILIKE" token at a word boundary.
		if i+5 > n || !strings.EqualFold(sql[i:i+5], "ILIKE") {
			i++
			continue
		}
		if i > 0 && isWordChar(sql[i-1]) { // not a word boundary on the left
			i++
			continue
		}
		if i+5 < n && isWordChar(sql[i+5]) { // not a word boundary on the right
			i++
			continue
		}

		ilikeStart := i
		ilikeEnd := i + 5

		// ── Scan backward for optional NOT and left operand ──────────────────

		k := ilikeStart - 1
		isWS := func(c byte) bool { return c == ' ' || c == '\t' || c == '\n' || c == '\r' }
		for k >= 0 && isWS(sql[k]) {
			k--
		}
		if k < 0 {
			i++
			continue
		}

		// Check for preceding NOT keyword.
		notOp := false
		if k >= 2 && strings.EqualFold(sql[k-2:k+1], "NOT") &&
			(k-3 < 0 || !isWordChar(sql[k-3])) {
			notOp = true
			k -= 3
			for k >= 0 && isWS(sql[k]) {
				k--
			}
			if k < 0 {
				i++
				continue
			}
		}

		// Find the left operand boundary.
		leftEnd := k + 1 // exclusive
		var leftStart int
		switch {
		case sql[k] == ')': // parenthesised expression
			depth := 0
			for k >= 0 {
				if sql[k] == ')' {
					depth++
				} else if sql[k] == '(' {
					depth--
					if depth == 0 {
						break
					}
				}
				k--
			}
			if k < 0 {
				i++
				continue
			}
			leftStart = k
		case sql[k] == '`': // backtick-quoted identifier
			k--
			for k >= 0 && sql[k] != '`' {
				k--
			}
			if k < 0 {
				i++
				continue
			}
			leftStart = k
		case isWordChar(sql[k]) || sql[k] == '.':
			for k >= 0 && (isWordChar(sql[k]) || sql[k] == '.') {
				k--
			}
			leftStart = k + 1
		default:
			i++
			continue
		}

		left := sql[leftStart:leftEnd]

		// ── Scan forward for right operand ────────────────────────────────────

		j := ilikeEnd
		for j < n && isWS(sql[j]) {
			j++
		}
		if j >= n {
			i++
			continue
		}

		rightStart := j
		switch {
		case sql[j] == '\'': // string literal
			j++
			for j < n {
				c := sql[j]
				j++
				if c == '\'' {
					if j < n && sql[j] == '\'' {
						j++ // '' escape
					} else {
						break
					}
				}
			}
		case sql[j] == '(': // parenthesised expression
			depth := 0
			for j < n {
				if sql[j] == '(' {
					depth++
				} else if sql[j] == ')' {
					depth--
					if depth == 0 {
						j++
						break
					}
				}
				j++
			}
		case sql[j] == '?': // anonymous placeholder
			j++
		case sql[j] == ':' || sql[j] == '$' || isWordChar(sql[j]): // :name / $1 / identifier
			for j < n && (isWordChar(sql[j]) || sql[j] == ':' || sql[j] == '$') {
				j++
			}
		default:
			i++
			continue
		}

		right := sql[rightStart:j]

		var repl string
		if notOp {
			repl = "LOWER(" + left + ") NOT LIKE LOWER(" + right + ")"
		} else {
			repl = "LOWER(" + left + ") LIKE LOWER(" + right + ")"
		}

		patches = append(patches, patch{leftStart, j, repl})
		i = j // advance past the whole expression
	}

	if len(patches) == 0 {
		return sql
	}

	var b strings.Builder
	b.Grow(len(sql) + len(patches)*20)
	pos := 0
	for _, p := range patches {
		b.WriteString(sql[pos:p.start])
		b.WriteString(p.repl)
		pos = p.end
	}
	b.WriteString(sql[pos:])
	return b.String()
}

// fullOuterRE matches FULL [OUTER] JOIN (case-insensitive) at word boundaries.
var fullOuterRE = regexp.MustCompile(`(?i)\bFULL\s+(?:OUTER\s+)?JOIN\b`)

// rewriteFullOuterJoins replaces FULL [OUTER] JOIN with STRAIGHT_JOIN so the
// MySQL-dialect parser (which has no FULL OUTER JOIN production) accepts the
// statement. walkTableExpr maps "straight_join" back to "full join" after
// parsing so that applyJoin can apply the correct FULL OUTER semantics.
func rewriteFullOuterJoins(sql string) string {
	return fullOuterRE.ReplaceAllString(sql, "STRAIGHT_JOIN")
}

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

// rewriteFilterAggregates rewrites every AGG(expr) FILTER (WHERE cond) in sql
// to AGG(CASE WHEN cond THEN expr END), enabling the MySQL-dialect parser to
// handle PostgreSQL-style aggregate filter clauses.
//
// Examples:
//
//	SUM(amount) FILTER (WHERE type = 'X')   →  SUM(CASE WHEN type = 'X' THEN amount END)
//	COUNT(*) FILTER (WHERE active)           →  COUNT(CASE WHEN active THEN 1 END)
//	array_agg(id) FILTER (WHERE id IS NOT NULL) → array_agg(CASE WHEN id IS NOT NULL THEN id END)
//
// The CASE … THEN NULL fallback causes aggregate functions to skip non-matching
// rows, which is exactly the PostgreSQL FILTER semantics.
func rewriteFilterAggregates(sql string) string {
	if !strings.Contains(strings.ToUpper(sql), "FILTER") {
		return sql
	}
	for {
		next, changed := rewriteFirstFilter(sql)
		if !changed {
			return sql
		}
		sql = next
	}
}

// rewriteFirstFilter finds and rewrites the left-most AGG(expr) FILTER (WHERE cond)
// occurrence. Returns (rewritten, true) on success, (original, false) otherwise.
func rewriteFirstFilter(s string) (string, bool) {
	n := len(s)
	for i := 0; i < n; {
		// Skip single-quoted string literals unchanged.
		if s[i] == '\'' {
			i = fSkipQuote(s, i)
			continue
		}

		// We scan for ')' — if FILTER ( WHERE follows it, we have a candidate.
		if s[i] != ')' {
			i++
			continue
		}
		closeIdx := i

		// Skip whitespace after ')'
		j := fSkipWS(s, closeIdx+1)

		// Expect "FILTER"
		if !fMatchFold(s, j, "FILTER") {
			i++
			continue
		}
		j += 6

		// Skip whitespace before '('
		j = fSkipWS(s, j)
		if j >= n || s[j] != '(' {
			i++
			continue
		}
		filterOpenParen := j
		j++ // past '('

		// Skip whitespace before "WHERE"
		j = fSkipWS(s, j)
		if !fMatchFold(s, j, "WHERE") {
			i++
			continue
		}
		j += 5
		// "WHERE" must be followed by a non-identifier character (space, '(', etc.)
		// to avoid matching "WHEREVER" or similar.
		if j < n && fIsIdentChar(s[j]) {
			i++
			continue
		}
		condStart := j

		// Find the matching ')' for filterOpenParen (forward scan, tracking depth).
		filterCloseParen := fMatchingClose(s, filterOpenParen)
		if filterCloseParen < 0 {
			i++
			continue
		}
		cond := strings.TrimSpace(s[condStart:filterCloseParen])

		// Find the matching '(' for closeIdx (backward scan, tracking depth).
		aggOpenParen := fMatchingOpen(s, closeIdx)
		if aggOpenParen < 0 {
			i++
			continue
		}
		aggExpr := strings.TrimSpace(s[aggOpenParen+1 : closeIdx])

		// Extract the function name immediately before aggOpenParen.
		funcStart, funcName := fExtractFuncName(s, aggOpenParen)
		if funcName == "" || !fIsAggFunc(funcName) {
			i++
			continue
		}

		// Build the CASE WHEN replacement.
		var repl string
		if strings.EqualFold(funcName, "count") && aggExpr == "*" {
			repl = fmt.Sprintf("%s(CASE WHEN %s THEN 1 END)", funcName, cond)
		} else {
			repl = fmt.Sprintf("%s(CASE WHEN %s THEN %s END)", funcName, cond, aggExpr)
		}

		return s[:funcStart] + repl + s[filterCloseParen+1:], true
	}
	return s, false
}

// ── helpers ───────────────────────────────────────────────────────────────────

// fSkipQuote advances past a single-quoted SQL string literal starting at i.
func fSkipQuote(s string, i int) int {
	i++ // skip opening '
	for i < len(s) {
		if s[i] == '\'' {
			i++
			if i < len(s) && s[i] == '\'' {
				i++ // '' escape — continue
				continue
			}
			return i
		}
		i++
	}
	return i
}

// fSkipWS skips ASCII whitespace.
func fSkipWS(s string, i int) int {
	for i < len(s) && (s[i] == ' ' || s[i] == '\t' || s[i] == '\n' || s[i] == '\r') {
		i++
	}
	return i
}

// fMatchFold returns true if s[i:i+len(sub)] equals sub (case-insensitive).
func fMatchFold(s string, i int, sub string) bool {
	if i+len(sub) > len(s) {
		return false
	}
	return strings.EqualFold(s[i:i+len(sub)], sub)
}

// fIsIdentChar returns true for characters valid inside a SQL identifier.
func fIsIdentChar(c byte) bool {
	return (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') ||
		(c >= '0' && c <= '9') || c == '_'
}

// fMatchingClose finds the ')' that matches the '(' at openIdx (forward scan).
// String literals are skipped so quoted ')' characters are not counted.
func fMatchingClose(s string, openIdx int) int {
	depth := 1
	i := openIdx + 1
	for i < len(s) {
		switch {
		case s[i] == '\'':
			i = fSkipQuote(s, i)
			continue
		case s[i] == '(':
			depth++
		case s[i] == ')':
			depth--
			if depth == 0 {
				return i
			}
		}
		i++
	}
	return -1
}

// fMatchingOpen finds the '(' that matches the ')' at closeIdx (backward scan).
// Note: backward scanning through string literals is not implemented here because
// aggregate argument lists (e.g. SUM(amount)) virtually never contain quoted strings.
func fMatchingOpen(s string, closeIdx int) int {
	depth := 1
	for i := closeIdx - 1; i >= 0; i-- {
		switch s[i] {
		case ')':
			depth++
		case '(':
			depth--
			if depth == 0 {
				return i
			}
		}
	}
	return -1
}

// fExtractFuncName extracts the identifier immediately before the '(' at openIdx.
// Returns (startIndex, name). Returns (openIdx, "") if no identifier is found.
func fExtractFuncName(s string, openIdx int) (int, string) {
	end := openIdx - 1
	// Skip optional whitespace between name and '('
	for end >= 0 && (s[end] == ' ' || s[end] == '\t') {
		end--
	}
	if end < 0 || !fIsIdentChar(s[end]) {
		return openIdx, ""
	}
	start := end
	for start > 0 && fIsIdentChar(s[start-1]) {
		start--
	}
	return start, s[start : end+1]
}

// fIsAggFunc returns true for aggregate functions that support FILTER (WHERE …).
func fIsAggFunc(name string) bool {
	switch strings.ToLower(name) {
	case "sum", "count", "avg", "min", "max",
		"array_agg", "string_agg", "json_agg", "jsonb_agg",
		"bit_and", "bit_or", "bool_and", "bool_or":
		return true
	}
	return false
}
