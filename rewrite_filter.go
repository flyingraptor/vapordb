package vapordb

import (
	"fmt"
	"strings"
)

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
