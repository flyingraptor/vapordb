package vapordb

import (
	"testing"
)

// Unit tests for rewriteDoubleQuotedIdents.

func TestRewriteDoubleQuotedIdents_Basic(t *testing.T) {
	cases := []struct {
		name  string
		in    string
		want  string
	}{
		{
			name: "no double quotes — passthrough",
			in:   "SELECT name, type FROM orders",
			want: "SELECT name, type FROM orders",
		},
		{
			name: "simple identifiers",
			in:   `SELECT "name", "type" FROM "orders"`,
			want:  "SELECT `name`, `type` FROM `orders`",
		},
		{
			name: "reserved word column",
			in:   `SELECT "status", "value", "key" FROM t`,
			want:  "SELECT `status`, `value`, `key` FROM t",
		},
		{
			name: "single-quoted string left untouched",
			in:   `SELECT 'hello "world"' AS greeting`,
			want:  "SELECT 'hello \"world\"' AS greeting",
		},
		{
			name: "mix of string literal and identifier",
			in:   `SELECT "name" FROM t WHERE label = 'a "b" c'`,
			want:  "SELECT `name` FROM t WHERE label = 'a \"b\" c'",
		},
		{
			name: "double-double-quote escape inside identifier",
			in:   `SELECT "col""name" FROM t`,
			want:  `SELECT ` + "`" + `col"name` + "`" + ` FROM t`,
		},
		{
			name: "single-quote escape inside string literal",
			in:   `SELECT 'it''s fine' AS s`,
			want:  "SELECT 'it''s fine' AS s",
		},
		{
			name: "empty string — passthrough",
			in:   "",
			want: "",
		},
		{
			name: "UPDATE with double-quoted col",
			in:   `UPDATE orders SET "status" = 'done' WHERE "type" = 'widget'`,
			want:  "UPDATE orders SET `status` = 'done' WHERE `type` = 'widget'",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := rewriteDoubleQuotedIdents(tc.in)
			if got != tc.want {
				t.Errorf("rewriteDoubleQuotedIdents(%q)\n  got  %q\n  want %q", tc.in, got, tc.want)
			}
		})
	}
}

// Integration tests — double-quoted identifiers round-trip through the engine.

func TestDoubleQuotedIdents_SelectReservedColumns(t *testing.T) {
	db := New()
	if err := db.Exec(`INSERT INTO orders (id, name, type, status) VALUES (1, 'widget', 'physical', 'open')`); err != nil {
		t.Fatal(err)
	}
	rows, err := db.Query(`SELECT "name", "type", "status" FROM orders WHERE "status" = 'open'`)
	if err != nil {
		t.Fatalf("query error: %v", err)
	}
	if len(rows) != 1 {
		t.Fatalf("want 1 row, got %d", len(rows))
	}
	if got, _ := rows[0]["name"].V.(string); got != "widget" {
		t.Errorf("name: want 'widget', got %q", got)
	}
	if got, _ := rows[0]["type"].V.(string); got != "physical" {
		t.Errorf("type: want 'physical', got %q", got)
	}
}

func TestDoubleQuotedIdents_InsertReservedColumns(t *testing.T) {
	db := New()
	if err := db.Exec(`INSERT INTO catalog ("name", "value", "key") VALUES ('thing', 42, 'k1')`); err != nil {
		t.Fatalf("insert error: %v", err)
	}
	rows, err := db.Query(`SELECT "name", "value", "key" FROM catalog`)
	if err != nil {
		t.Fatalf("query error: %v", err)
	}
	if len(rows) != 1 {
		t.Fatalf("want 1 row, got %d", len(rows))
	}
	if got, _ := rows[0]["name"].V.(string); got != "thing" {
		t.Errorf("name: want 'thing', got %q", got)
	}
	if got, _ := rows[0]["value"].V.(int64); got != 42 {
		t.Errorf("value: want 42, got %d", got)
	}
}

func TestDoubleQuotedIdents_UpdateReservedColumns(t *testing.T) {
	db := New()
	if err := db.Exec(`INSERT INTO items (id, status, type) VALUES (1, 'pending', 'widget')`); err != nil {
		t.Fatal(err)
	}
	if err := db.Exec(`UPDATE items SET "status" = 'done' WHERE "type" = 'widget'`); err != nil {
		t.Fatalf("update error: %v", err)
	}
	rows, err := db.Query(`SELECT "status" FROM items WHERE id = 1`)
	if err != nil {
		t.Fatalf("query error: %v", err)
	}
	if got, _ := rows[0]["status"].V.(string); len(rows) != 1 || got != "done" {
		t.Errorf("want status='done', got %v", rows)
	}
}

func TestDoubleQuotedIdents_MixedWithStringLiterals(t *testing.T) {
	db := New()
	// Insert using plain identifiers, query with double-quoted ones while the
	// WHERE clause contains a string literal with double-quote-like content.
	if err := db.Exec(`INSERT INTO msgs (id, name, body) VALUES (1, 'alice', 'say "hello"')`); err != nil {
		t.Fatal(err)
	}
	rows, err := db.Query(`SELECT "name", body FROM msgs WHERE "name" = 'alice'`)
	if err != nil {
		t.Fatalf("query error: %v", err)
	}
	if len(rows) != 1 {
		t.Fatalf("want 1 row, got %d", len(rows))
	}
	if got, _ := rows[0]["name"].V.(string); got != "alice" {
		t.Errorf("want 'alice', got %q", got)
	}
	if got, _ := rows[0]["body"].V.(string); got != `say "hello"` {
		t.Errorf("want 'say \"hello\"', got %q", got)
	}
}

func TestDoubleQuotedIdents_CTEAndSubquery(t *testing.T) {
	db := New()
	if err := db.Exec(`INSERT INTO products (id, name, type) VALUES (1, 'a', 'x'), (2, 'b', 'y')`); err != nil {
		t.Fatal(err)
	}
	rows, err := db.Query(`WITH p AS (SELECT "name", "type" FROM products) SELECT "name" FROM p WHERE "type" = 'x'`)
	if err != nil {
		t.Fatalf("cte query error: %v", err)
	}
	if got, _ := rows[0]["name"].V.(string); len(rows) != 1 || got != "a" {
		t.Errorf("want [{name:a}], got %v", rows)
	}
}

func TestDoubleQuotedIdents_DeleteReservedColumns(t *testing.T) {
	db := New()
	if err := db.Exec(`INSERT INTO stock (id, status) VALUES (1, 'old'), (2, 'new')`); err != nil {
		t.Fatal(err)
	}
	if err := db.Exec(`DELETE FROM stock WHERE "status" = 'old'`); err != nil {
		t.Fatalf("delete error: %v", err)
	}
	rows, err := db.Query(`SELECT * FROM stock`)
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 1 {
		t.Fatalf("want 1 row after delete, got %d", len(rows))
	}
	if got, _ := rows[0]["status"].V.(string); got != "new" {
		t.Errorf("want 'new', got %q", got)
	}
}
