package vapordb

import (
	"fmt"
	"strings"
)

// cteRef holds one CTE definition from a WITH clause.
type cteRef struct {
	name  string // lower-cased CTE name used as virtual table name
	query string // body of the AS (…) clause
}

// parseCTEs detects and extracts CTE definitions from a WITH clause.
//
//	WITH cte1 AS (SELECT …), cte2 AS (SELECT …) SELECT …
//
// Returns the CTE list, the remaining main query, and a boolean indicating
// whether a WITH clause was present. Single-quoted string literals inside
// subqueries are skipped so parentheses within strings do not confuse the
// balanced-paren tracker.
func parseCTEs(sql string) (ctes []cteRef, mainQuery string, ok bool) {
	s := strings.TrimSpace(sql)

	// Must begin with WITH (case-insensitive) followed by whitespace or (.
	if len(s) < 5 {
		return nil, sql, false
	}
	if !strings.EqualFold(s[:4], "WITH") {
		return nil, sql, false
	}
	if len(s) > 4 && isCTEIdentChar(s[4]) {
		return nil, sql, false // e.g. "WITHIN" — not a CTE
	}

	pos := 4

	for {
		pos = cteSkipWS(s, pos)

		// Read CTE name.
		nameStart := pos
		for pos < len(s) && isCTEIdentChar(s[pos]) {
			pos++
		}
		if pos == nameStart {
			break
		}
		name := strings.ToLower(s[nameStart:pos])

		pos = cteSkipWS(s, pos)

		// Expect AS.
		if pos+2 > len(s) || !strings.EqualFold(s[pos:pos+2], "AS") {
			return nil, sql, false
		}
		pos += 2
		pos = cteSkipWS(s, pos)

		// Expect opening paren.
		if pos >= len(s) || s[pos] != '(' {
			return nil, sql, false
		}
		pos++ // consume '('

		// Find matching ')' tracking depth and skipping string literals.
		queryStart := pos
		depth := 1
		for pos < len(s) && depth > 0 {
			ch := s[pos]
			if ch == '\'' {
				// Skip single-quoted literal ('' is an escaped quote inside).
				pos++
				for pos < len(s) {
					c := s[pos]
					pos++
					if c == '\'' {
						if pos < len(s) && s[pos] == '\'' {
							pos++ // escaped ''
						} else {
							break
						}
					}
				}
			} else if ch == '(' {
				depth++
				pos++
			} else if ch == ')' {
				depth--
				pos++
			} else {
				pos++
			}
		}
		// pos now points one past the closing ')'.
		body := strings.TrimSpace(s[queryStart : pos-1])
		ctes = append(ctes, cteRef{name: name, query: body})

		pos = cteSkipWS(s, pos)

		// Comma → more CTEs; anything else → done.
		if pos < len(s) && s[pos] == ',' {
			pos++
		} else {
			break
		}
	}

	if len(ctes) == 0 {
		return nil, sql, false
	}
	return ctes, strings.TrimSpace(s[pos:]), true
}

// resolveCTEs detects a WITH clause in sql, executes each CTE subquery in
// order against a shallow DB copy (so later CTEs can reference earlier ones),
// and returns the augmented DB together with the main query string.
//
// If no WITH clause is present the original db and sql are returned unchanged.
func resolveCTEs(db *DB, sql string) (*DB, string, error) {
	ctes, mainQuery, hasCTE := parseCTEs(sql)
	if !hasCTE {
		return db, sql, nil
	}

	// Shallow copy: real tables are shared (read-only during CTE execution).
	tempDB := &DB{Tables: make(map[string]*Table, len(db.Tables)+len(ctes))}
	for k, v := range db.Tables {
		tempDB.Tables[k] = v
	}

	for _, cte := range ctes {
		rows, err := tempDB.Query(cte.query)
		if err != nil {
			return nil, "", fmt.Errorf("CTE %q: %w", cte.name, err)
		}
		// Build a Table from the result rows so normal table-lookup paths work.
		tbl := &Table{Schema: make(map[string]Kind), Rows: rows}
		for _, row := range rows {
			for col, val := range row {
				if existing, exists := tbl.Schema[col]; !exists || val.Kind > existing {
					tbl.Schema[col] = val.Kind
				}
			}
		}
		tempDB.Tables[cte.name] = tbl
	}

	return tempDB, mainQuery, nil
}

func cteSkipWS(s string, pos int) int {
	for pos < len(s) && (s[pos] == ' ' || s[pos] == '\t' || s[pos] == '\n' || s[pos] == '\r') {
		pos++
	}
	return pos
}

func isCTEIdentChar(c byte) bool {
	return (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') ||
		(c >= '0' && c <= '9') || c == '_'
}

