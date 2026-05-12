package vapordb

// Tests for PostgreSQL array parameter binding.
//
// pq.Array([]T{…}).Value() returns a PostgreSQL array literal string like
// "{A001,A002}" which vapordb must expand to a SQL comma-separated literal
// list so it can be used inside IN (…) after = ANY(…) is rewritten.
//
// We simulate pq.Array using a local pgArray helper (implementing driver.Valuer)
// so the tests do not require importing github.com/lib/pq.

import (
	"database/sql/driver"
	"testing"
)

// pgArray is a test stub that mimics pq.Array's driver.Valuer implementation.
// It returns the provided string as the Value(), simulating what pq.Array
// produces for slices of various element types.
type pgArray struct{ pgLiteral string }

func (a pgArray) Value() (driver.Value, error) { return a.pgLiteral, nil }

// ── Unit tests for expandPGArrayLiteral ──────────────────────────────────────

func TestExpandPGArrayLiteral_Strings(t *testing.T) {
	expanded, ok := expandPGArrayLiteral("{A001,A002,A003}")
	if !ok {
		t.Fatal("expected ok=true")
	}
	want := "'A001', 'A002', 'A003'"
	if expanded != want {
		t.Errorf("got %q, want %q", expanded, want)
	}
}

func TestExpandPGArrayLiteral_Integers(t *testing.T) {
	expanded, ok := expandPGArrayLiteral("{1,2,3}")
	if !ok {
		t.Fatal("expected ok=true")
	}
	want := "1, 2, 3"
	if expanded != want {
		t.Errorf("got %q, want %q", expanded, want)
	}
}

func TestExpandPGArrayLiteral_Empty(t *testing.T) {
	expanded, ok := expandPGArrayLiteral("{}")
	if !ok {
		t.Fatal("expected ok=true for empty array")
	}
	if expanded != "NULL" {
		t.Errorf("got %q, want \"NULL\"", expanded)
	}
}

func TestExpandPGArrayLiteral_SingleElement(t *testing.T) {
	expanded, ok := expandPGArrayLiteral("{hello}")
	if !ok {
		t.Fatal("expected ok=true")
	}
	if expanded != "'hello'" {
		t.Errorf("got %q, want \"'hello'\"", expanded)
	}
}

func TestExpandPGArrayLiteral_WithNULL(t *testing.T) {
	expanded, ok := expandPGArrayLiteral("{NULL,1,2}")
	if !ok {
		t.Fatal("expected ok=true")
	}
	want := "NULL, 1, 2"
	if expanded != want {
		t.Errorf("got %q, want %q", expanded, want)
	}
}

func TestExpandPGArrayLiteral_QuotedElements(t *testing.T) {
	// PostgreSQL quotes elements with commas: {"hello,world","foo"}
	expanded, ok := expandPGArrayLiteral(`{"hello,world","foo"}`)
	if !ok {
		t.Fatal("expected ok=true")
	}
	want := "'hello,world', 'foo'"
	if expanded != want {
		t.Errorf("got %q, want %q", expanded, want)
	}
}

func TestExpandPGArrayLiteral_WithApostrophe(t *testing.T) {
	// Elements containing apostrophes should be escaped.
	expanded, ok := expandPGArrayLiteral("{it's}")
	if !ok {
		t.Fatal("expected ok=true")
	}
	want := "'it''s'"
	if expanded != want {
		t.Errorf("got %q, want %q", expanded, want)
	}
}

func TestExpandPGArrayLiteral_NotAnArray(t *testing.T) {
	_, ok := expandPGArrayLiteral("hello")
	if ok {
		t.Error("plain string should return ok=false")
	}
	_, ok2 := expandPGArrayLiteral("")
	if ok2 {
		t.Error("empty string should return ok=false")
	}
}

func TestExpandPGArrayLiteral_UUIDs(t *testing.T) {
	// UUIDs are strings — should be quoted.
	expanded, ok := expandPGArrayLiteral("{550e8400-e29b-41d4-a716-446655440000,123e4567-e89b-12d3-a456-426614174000}")
	if !ok {
		t.Fatal("expected ok=true")
	}
	want := "'550e8400-e29b-41d4-a716-446655440000', '123e4567-e89b-12d3-a456-426614174000'"
	if expanded != want {
		t.Errorf("got %q, want %q", expanded, want)
	}
}

// ── Integration tests via anyToSQLLiteral (native named-param path) ───────────

func TestPGArray_NativeAPI_StringSlice(t *testing.T) {
	db := New()
	mustExec(t, db, `INSERT INTO accts (code, amount) VALUES ('A001', 100)`)
	mustExec(t, db, `INSERT INTO accts (code, amount) VALUES ('A002', 200)`)
	mustExec(t, db, `INSERT INTO accts (code, amount) VALUES ('A003', 300)`)

	// Use a pgArray stub simulating pq.Array([]string{"A001","A002"}).Value()
	rows, err := db.QueryNamed(
		`SELECT code, amount FROM accts WHERE code = ANY(:codes) ORDER BY code`,
		map[string]any{"codes": pgArray{"{A001,A002}"}},
	)
	if err != nil {
		t.Fatalf("QueryNamed: %v", err)
	}
	if len(rows) != 2 {
		t.Fatalf("expected 2 rows, got %d: %v", len(rows), rows)
	}
	if rows[0]["code"].V != "A001" || rows[1]["code"].V != "A002" {
		t.Errorf("unexpected codes: %v %v", rows[0]["code"], rows[1]["code"])
	}
}

func TestPGArray_NativeAPI_IntSlice(t *testing.T) {
	db := New()
	mustExec(t, db, `INSERT INTO nums (id, v) VALUES (1, 10)`)
	mustExec(t, db, `INSERT INTO nums (id, v) VALUES (2, 20)`)
	mustExec(t, db, `INSERT INTO nums (id, v) VALUES (3, 30)`)

	// pq.Array([]int64{1,3}).Value() → "{1,3}"
	rows, err := db.QueryNamed(
		`SELECT id, v FROM nums WHERE id = ANY(:ids) ORDER BY id`,
		map[string]any{"ids": pgArray{"{1,3}"}},
	)
	if err != nil {
		t.Fatalf("QueryNamed: %v", err)
	}
	if len(rows) != 2 {
		t.Fatalf("expected 2 rows, got %d", len(rows))
	}
	if rows[0]["id"].V != int64(1) || rows[1]["id"].V != int64(3) {
		t.Errorf("unexpected ids: %v %v", rows[0]["id"], rows[1]["id"])
	}
}

func TestPGArray_NativeAPI_PlainStringSlice(t *testing.T) {
	// Plain []string{} — already worked before, regression check.
	db := New()
	mustExec(t, db, `INSERT INTO words (w) VALUES ('hello')`)
	mustExec(t, db, `INSERT INTO words (w) VALUES ('world')`)
	mustExec(t, db, `INSERT INTO words (w) VALUES ('foo')`)

	rows, err := db.QueryNamed(
		`SELECT w FROM words WHERE w = ANY(:ws) ORDER BY w`,
		map[string]any{"ws": []string{"hello", "world"}},
	)
	if err != nil {
		t.Fatalf("QueryNamed: %v", err)
	}
	if len(rows) != 2 {
		t.Fatalf("expected 2 rows, got %d", len(rows))
	}
}

func TestPGArray_NativeAPI_PlainIntSlice(t *testing.T) {
	// Plain []int64{} — already worked, regression check.
	db := New()
	mustExec(t, db, `INSERT INTO vals (n) VALUES (10)`)
	mustExec(t, db, `INSERT INTO vals (n) VALUES (20)`)
	mustExec(t, db, `INSERT INTO vals (n) VALUES (30)`)

	rows, err := db.QueryNamed(
		`SELECT n FROM vals WHERE n = ANY(:ns)`,
		map[string]any{"ns": []int64{10, 30}},
	)
	if err != nil {
		t.Fatalf("QueryNamed: %v", err)
	}
	if len(rows) != 2 {
		t.Fatalf("expected 2 rows, got %d", len(rows))
	}
}

func TestPGArray_NativeAPI_Empty(t *testing.T) {
	// Empty array → IN (NULL) → no rows returned.
	db := New()
	mustExec(t, db, `INSERT INTO things (x) VALUES (1)`)

	rows, err := db.QueryNamed(
		`SELECT x FROM things WHERE x = ANY(:xs)`,
		map[string]any{"xs": pgArray{"{}"}},
	)
	if err != nil {
		t.Fatalf("QueryNamed: %v", err)
	}
	if len(rows) != 0 {
		t.Errorf("expected 0 rows for empty array, got %d", len(rows))
	}
}

func TestPGArray_NativeAPI_PlainEmptySlice(t *testing.T) {
	db := New()
	mustExec(t, db, `INSERT INTO things2 (x) VALUES (1)`)

	rows, err := db.QueryNamed(
		`SELECT x FROM things2 WHERE x = ANY(:xs)`,
		map[string]any{"xs": []int64{}},
	)
	if err != nil {
		t.Fatalf("QueryNamed: %v", err)
	}
	if len(rows) != 0 {
		t.Errorf("expected 0 rows for empty slice, got %d", len(rows))
	}
}

func TestPGArray_NativeAPI_NotInVariant(t *testing.T) {
	// <> ALL(:codes) → NOT IN (:codes)
	db := New()
	mustExec(t, db, `INSERT INTO fruits (name) VALUES ('apple')`)
	mustExec(t, db, `INSERT INTO fruits (name) VALUES ('banana')`)
	mustExec(t, db, `INSERT INTO fruits (name) VALUES ('cherry')`)

	rows, err := db.QueryNamed(
		`SELECT name FROM fruits WHERE name <> ALL(:excluded) ORDER BY name`,
		map[string]any{"excluded": pgArray{"{apple,banana}"}},
	)
	if err != nil {
		t.Fatalf("QueryNamed: %v", err)
	}
	if len(rows) != 1 || rows[0]["name"].V != "cherry" {
		t.Errorf("expected only 'cherry', got %v", rows)
	}
}

// ── Full budget-service pattern: FILTER + ANY + GROUP BY ─────────────────────

func TestPGArray_BudgetServicePattern(t *testing.T) {
	// Mirrors the core query from budget-management-service:
	//   SELECT account_code,
	//          SUM(amount) FILTER (WHERE type = 'RESERVED')  AS reserved_amount,
	//          SUM(amount) FILTER (WHERE type = 'COMMITTED') AS committed_amount,
	//          SUM(amount)                                    AS total_amount
	//   FROM transactions
	//   WHERE account_id   = :account_id
	//     AND account_code = ANY(:account_codes)
	//   GROUP BY account_code
	db := New()
	mustExec(t, db, `INSERT INTO txns2 (account_id, account_code, type, amount) VALUES (7, 'A001', 'RESERVED',  100)`)
	mustExec(t, db, `INSERT INTO txns2 (account_id, account_code, type, amount) VALUES (7, 'A001', 'COMMITTED', 200)`)
	mustExec(t, db, `INSERT INTO txns2 (account_id, account_code, type, amount) VALUES (7, 'A002', 'RESERVED',  50)`)
	mustExec(t, db, `INSERT INTO txns2 (account_id, account_code, type, amount) VALUES (7, 'A002', 'ACTUAL',    150)`)
	mustExec(t, db, `INSERT INTO txns2 (account_id, account_code, type, amount) VALUES (8, 'A001', 'RESERVED',  999)`) // different account_id

	rows, err := db.QueryNamed(`
		SELECT
			account_code,
			COALESCE(SUM(amount) FILTER (WHERE type = 'RESERVED'),  0) AS reserved_amount,
			COALESCE(SUM(amount) FILTER (WHERE type = 'COMMITTED'), 0) AS committed_amount,
			SUM(amount) AS total_amount
		FROM txns2
		WHERE account_id   = :account_id
		  AND account_code = ANY(:account_codes)
		GROUP BY account_code
		ORDER BY account_code`,
		map[string]any{
			"account_id":    int64(7),
			"account_codes": pgArray{"{A001,A002}"},
		},
	)
	if err != nil {
		t.Fatalf("QueryNamed: %v", err)
	}
	if len(rows) != 2 {
		t.Fatalf("expected 2 rows, got %d: %v", len(rows), rows)
	}

	// A001
	if rows[0]["account_code"].V != "A001" {
		t.Errorf("row0 account_code: got %v", rows[0]["account_code"])
	}
	if rows[0]["reserved_amount"].V != int64(100) {
		t.Errorf("A001 reserved: got %v, want 100", rows[0]["reserved_amount"].V)
	}
	if rows[0]["committed_amount"].V != int64(200) {
		t.Errorf("A001 committed: got %v, want 200", rows[0]["committed_amount"].V)
	}
	if rows[0]["total_amount"].V != int64(300) {
		t.Errorf("A001 total: got %v, want 300", rows[0]["total_amount"].V)
	}

	// A002
	if rows[1]["account_code"].V != "A002" {
		t.Errorf("row1 account_code: got %v", rows[1]["account_code"])
	}
	if rows[1]["reserved_amount"].V != int64(50) {
		t.Errorf("A002 reserved: got %v, want 50", rows[1]["reserved_amount"].V)
	}
	if rows[1]["committed_amount"].V != int64(0) {
		t.Errorf("A002 committed: got %v, want 0 (coalesced)", rows[1]["committed_amount"].V)
	}
}
