package vapordb

import "testing"

// These tests lock the *ordering* and *interaction* of the pre-parse rewrite
// pipeline (rewrite.go). The individual rewriters have their own unit tests
// (dquote_test.go, ilike_test.go, anyall_test.go, upsert_test.go) and the
// features are covered end-to-end elsewhere; what is NOT covered there is what
// happens when several passes touch the *same* statement, where the order of
// rewritePreParse / rewriteDML matters. If someone reorders the chain, these
// tests fail even though the per-rewriter unit tests still pass.

func TestRewritePreParse_ChainOrder(t *testing.T) {
	cases := []struct {
		name string
		in   string
		want string
	}{
		{
			// Double-quoted identifier is also the ILIKE left operand: proves
			// rewriteDoubleQuotedIdents runs before rewriteILIKE (so ILIKE sees a
			// backtick identifier, not a bare double-quoted one).
			name: "double-quoted ident as ILIKE operand",
			in:   `SELECT "name" FROM users WHERE "name" ILIKE 'a%'`,
			want: "SELECT `name` FROM users WHERE LOWER(`name`) LIKE LOWER('a%')",
		},
		{
			// JSON ->> operator inside a FILTER (WHERE …) condition: proves
			// rewriteJSONOps runs before rewriteFilterAggregates, so the CASE body
			// contains the already-rewritten json_extract call.
			name: "JSON operator inside FILTER condition",
			in:   `SELECT COUNT(*) FILTER (WHERE data->>'$.k' = 'v') FROM t`,
			want: `SELECT COUNT(CASE WHEN json_unquote(json_extract(data, '$.k')) = 'v' THEN 1 END) FROM t`,
		},
		{
			// ILIKE in a FULL OUTER JOIN ON clause: proves rewriteILIKE and
			// rewriteFullOuterJoins compose on one statement.
			name: "ILIKE inside FULL OUTER JOIN ON clause",
			in:   `SELECT * FROM a FULL OUTER JOIN b ON a.tag ILIKE 'x%'`,
			want: `SELECT * FROM a STRAIGHT_JOIN b ON LOWER(a.tag) LIKE LOWER('x%')`,
		},
		{
			// A single-quoted string literal containing the word ILIKE and a
			// double-quoted fragment must survive the whole chain untouched, while
			// a genuine ILIKE elsewhere is still rewritten. Proves every pass
			// skips string literals.
			name: "string literal untouched, real ILIKE rewritten",
			in:   `SELECT 'ILIKE "x"' AS c FROM t WHERE tag ILIKE 'p%'`,
			want: `SELECT 'ILIKE "x"' AS c FROM t WHERE LOWER(tag) LIKE LOWER('p%')`,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := rewritePreParse(tc.in); got != tc.want {
				t.Errorf("rewritePreParse(%q)\n  got  %q\n  want %q", tc.in, got, tc.want)
			}
		})
	}
}

func TestRewriteDML_Chain(t *testing.T) {
	cases := []struct {
		name          string
		in            string
		wantSQL       string
		wantCols      []string
		wantDoNothing bool
		wantWhere     string
	}{
		{
			// = ANY(…) is rewritten to IN (…) by the DML-only rewriteAnyAll pass,
			// and the absence of ON CONFLICT leaves the conflict metadata empty.
			name:    "ANY rewritten, no ON CONFLICT",
			in:      `DELETE FROM t WHERE id = ANY('{1,2,3}')`,
			wantSQL: `DELETE FROM t WHERE id IN ('{1,2,3}')`,
		},
		{
			// ON CONFLICT … DO UPDATE is extracted: conflict columns are returned,
			// the clause becomes MySQL ON DUPLICATE KEY UPDATE, and EXCLUDED.col
			// becomes VALUES(col).
			name:     "ON CONFLICT DO UPDATE with EXCLUDED",
			in:       `INSERT INTO t (id, n) VALUES (1, 'a') ON CONFLICT (id) DO UPDATE SET n = EXCLUDED.n`,
			wantSQL:  `INSERT INTO t (id, n) VALUES (1, 'a') ON DUPLICATE KEY UPDATE n = VALUES(n)`,
			wantCols: []string{"id"},
		},
		{
			// ON CONFLICT … DO NOTHING is stripped and flagged.
			name:          "ON CONFLICT DO NOTHING",
			in:            `INSERT INTO t (id) VALUES (1) ON CONFLICT (id) DO NOTHING`,
			wantSQL:       `INSERT INTO t (id) VALUES (1)`,
			wantCols:      []string{"id"},
			wantDoNothing: true,
		},
		{
			// The shared base chain also runs for DML: double-quoted identifiers
			// in an UPDATE are rewritten to backticks.
			name:    "base chain runs for DML (double-quoted idents)",
			in:      `UPDATE t SET "name" = 'x' WHERE "type" = 'y'`,
			wantSQL: "UPDATE t SET `name` = 'x' WHERE `type` = 'y'",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			gotSQL, gotCols, gotDoNothing, gotWhere := rewriteDML(tc.in)
			if gotSQL != tc.wantSQL {
				t.Errorf("rewriteDML(%q) SQL\n  got  %q\n  want %q", tc.in, gotSQL, tc.wantSQL)
			}
			if !equalStrings(gotCols, tc.wantCols) {
				t.Errorf("rewriteDML(%q) conflictCols = %v, want %v", tc.in, gotCols, tc.wantCols)
			}
			if gotDoNothing != tc.wantDoNothing {
				t.Errorf("rewriteDML(%q) doNothing = %v, want %v", tc.in, gotDoNothing, tc.wantDoNothing)
			}
			if gotWhere != tc.wantWhere {
				t.Errorf("rewriteDML(%q) upsertWhere = %q, want %q", tc.in, gotWhere, tc.wantWhere)
			}
		})
	}
}

func equalStrings(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
