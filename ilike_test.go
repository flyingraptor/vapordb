package vapordb

import (
	"testing"
)

// ── Unit tests for the rewrite function ──────────────────────────────────────

func TestRewriteILIKE_Basic(t *testing.T) {
	cases := []struct {
		name string
		in   string
		want string
	}{
		{
			name: "no ilike — passthrough",
			in:   "SELECT name FROM t WHERE name LIKE '%foo%'",
			want: "SELECT name FROM t WHERE name LIKE '%foo%'",
		},
		{
			name: "simple ILIKE string literal",
			in:   "SELECT * FROM t WHERE name ILIKE '%alice%'",
			want: "SELECT * FROM t WHERE LOWER(name) LIKE LOWER('%alice%')",
		},
		{
			name: "NOT ILIKE",
			in:   "SELECT * FROM t WHERE name NOT ILIKE 'admin%'",
			want: "SELECT * FROM t WHERE LOWER(name) NOT LIKE LOWER('admin%')",
		},
		{
			name: "qualified identifier",
			in:   "SELECT * FROM t WHERE t.name ILIKE '%foo%'",
			want: "SELECT * FROM t WHERE LOWER(t.name) LIKE LOWER('%foo%')",
		},
		{
			name: "named parameter :name",
			in:   "SELECT * FROM t WHERE name ILIKE :pat",
			want: "SELECT * FROM t WHERE LOWER(name) LIKE LOWER(:pat)",
		},
		{
			name: "positional parameter ?",
			in:   "SELECT * FROM t WHERE name ILIKE ?",
			want: "SELECT * FROM t WHERE LOWER(name) LIKE LOWER(?)",
		},
		{
			name: "positional parameter $1",
			in:   "SELECT * FROM t WHERE name ILIKE $1",
			want: "SELECT * FROM t WHERE LOWER(name) LIKE LOWER($1)",
		},
		{
			name: "ILIKE inside single-quoted string — untouched",
			in:   "SELECT * FROM t WHERE note = 'use ILIKE for case-insensitive search'",
			want: "SELECT * FROM t WHERE note = 'use ILIKE for case-insensitive search'",
		},
		{
			name: "multiple ILIKE in one query",
			in:   "SELECT * FROM t WHERE first ILIKE '%a%' AND last ILIKE '%b%'",
			want: "SELECT * FROM t WHERE LOWER(first) LIKE LOWER('%a%') AND LOWER(last) LIKE LOWER('%b%')",
		},
		{
			name: "case-insensitive keyword ILIKE",
			in:   "SELECT * FROM t WHERE name ilike '%foo%'",
			want: "SELECT * FROM t WHERE LOWER(name) LIKE LOWER('%foo%')",
		},
		{
			name: "ILIKE mixed case",
			in:   "SELECT * FROM t WHERE name IlIkE '%foo%'",
			want: "SELECT * FROM t WHERE LOWER(name) LIKE LOWER('%foo%')",
		},
		{
			name: "backtick-quoted identifier on left",
			in:   "SELECT * FROM t WHERE `name` ILIKE '%foo%'",
			want: "SELECT * FROM t WHERE LOWER(`name`) LIKE LOWER('%foo%')",
		},
		{
			name: "empty string — passthrough",
			in:   "",
			want: "",
		},
		{
			name: "string with escaped quote before ILIKE",
			in:   "SELECT * FROM t WHERE prefix = 'it''s' AND name ILIKE '%foo%'",
			want: "SELECT * FROM t WHERE prefix = 'it''s' AND LOWER(name) LIKE LOWER('%foo%')",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := rewriteILIKE(tc.in)
			if got != tc.want {
				t.Errorf("\n  in   %q\n  got  %q\n  want %q", tc.in, got, tc.want)
			}
		})
	}
}

// ── Integration tests through the engine ─────────────────────────────────────

func TestILIKE_BasicMatch(t *testing.T) {
	db := New()
	if err := db.Exec(`INSERT INTO users (id, name) VALUES (1, 'Alice'), (2, 'BOB'), (3, 'charlie')`); err != nil {
		t.Fatal(err)
	}
	rows, err := db.Query(`SELECT name FROM users WHERE name ILIKE 'alice'`)
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	if len(rows) != 1 {
		t.Fatalf("want 1 row, got %d", len(rows))
	}
	if got, _ := rows[0]["name"].V.(string); got != "Alice" {
		t.Errorf("want Alice, got %q", got)
	}
}

func TestILIKE_WildcardMatch(t *testing.T) {
	db := New()
	if err := db.Exec(`INSERT INTO products (id, name) VALUES (1, 'Widget'), (2, 'GADGET'), (3, 'doohickey')`); err != nil {
		t.Fatal(err)
	}
	rows, err := db.Query(`SELECT name FROM products WHERE name ILIKE '%get%'`)
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	if len(rows) != 2 {
		t.Fatalf("want 2 rows, got %d: %v", len(rows), rows)
	}
}

func TestILIKE_NotILIKE(t *testing.T) {
	db := New()
	if err := db.Exec(`INSERT INTO items (id, status) VALUES (1, 'Active'), (2, 'INACTIVE'), (3, 'Pending')`); err != nil {
		t.Fatal(err)
	}
	rows, err := db.Query(`SELECT status FROM items WHERE status NOT ILIKE 'active'`)
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	if len(rows) != 2 {
		t.Fatalf("want 2 rows, got %d: %v", len(rows), rows)
	}
}

func TestILIKE_NamedParameter(t *testing.T) {
	db := New()
	if err := db.Exec(`INSERT INTO users (id, name) VALUES (1, 'ALICE'), (2, 'bob')`); err != nil {
		t.Fatal(err)
	}
	rows, err := db.QueryNamed(`SELECT name FROM users WHERE name ILIKE :pat`, map[string]any{"pat": "%alice%"})
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	if len(rows) != 1 {
		t.Fatalf("want 1 row, got %d", len(rows))
	}
}

func TestILIKE_StringLiteralWithILIKEWordUntouched(t *testing.T) {
	db := New()
	if err := db.Exec(`INSERT INTO notes (id, body) VALUES (1, 'use ILIKE for case-insensitive search'), (2, 'normal note')`); err != nil {
		t.Fatal(err)
	}
	// The string literal must be left intact; only the real ILIKE operator is rewritten.
	rows, err := db.Query(`SELECT body FROM notes WHERE body ILIKE '%ILIKE%'`)
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	if len(rows) != 1 {
		t.Fatalf("want 1 row, got %d: %v", len(rows), rows)
	}
	if got, _ := rows[0]["body"].V.(string); got != "use ILIKE for case-insensitive search" {
		t.Errorf("unexpected body: %q", got)
	}
}

func TestILIKE_MultipleConditions(t *testing.T) {
	db := New()
	if err := db.Exec(`INSERT INTO people (id, first, last) VALUES (1, 'John', 'DOE'), (2, 'JANE', 'doe'), (3, 'Bob', 'Smith')`); err != nil {
		t.Fatal(err)
	}
	rows, err := db.Query(`SELECT first FROM people WHERE first ILIKE 'j%' AND last ILIKE '%doe%'`)
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	if len(rows) != 2 {
		t.Fatalf("want 2 rows, got %d: %v", len(rows), rows)
	}
}

func TestILIKE_CaseSensitiveLIKEUnaffected(t *testing.T) {
	db := New()
	if err := db.Exec(`INSERT INTO t (id, v) VALUES (1, 'Hello'), (2, 'hello'), (3, 'HELLO')`); err != nil {
		t.Fatal(err)
	}
	// LIKE should remain case-sensitive (no rewrite).
	rows, err := db.Query(`SELECT v FROM t WHERE v LIKE 'Hello'`)
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	if len(rows) != 1 {
		t.Fatalf("LIKE should be case-sensitive, want 1 row, got %d: %v", len(rows), rows)
	}
}
