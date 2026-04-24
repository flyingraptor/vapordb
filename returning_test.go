package vapordb

import (
	"testing"
)

// ── extractReturning unit tests ───────────────────────────────────────────────

func TestExtractReturningInsert(t *testing.T) {
	sql := "INSERT INTO t (id) VALUES (1) RETURNING id"
	stripped, cols, ok := extractReturning(sql)
	if !ok {
		t.Fatal("expected RETURNING to be found")
	}
	if stripped != "INSERT INTO t (id) VALUES (1)" {
		t.Fatalf("stripped=%q", stripped)
	}
	if cols != "id" {
		t.Fatalf("cols=%q", cols)
	}
}

func TestExtractReturningUpdate(t *testing.T) {
	sql := "UPDATE t SET name = 'alice' WHERE id = 1 RETURNING id, name"
	stripped, cols, ok := extractReturning(sql)
	if !ok {
		t.Fatal("expected RETURNING")
	}
	if stripped != "UPDATE t SET name = 'alice' WHERE id = 1" {
		t.Fatalf("stripped=%q", stripped)
	}
	if cols != "id, name" {
		t.Fatalf("cols=%q", cols)
	}
}

func TestExtractReturningStar(t *testing.T) {
	sql := "DELETE FROM t WHERE id = 1 RETURNING *"
	stripped, cols, ok := extractReturning(sql)
	if !ok {
		t.Fatal("expected RETURNING")
	}
	if cols != "*" {
		t.Fatalf("cols=%q", cols)
	}
	if stripped != "DELETE FROM t WHERE id = 1" {
		t.Fatalf("stripped=%q", stripped)
	}
}

func TestExtractReturningAbsent(t *testing.T) {
	sql := "INSERT INTO t (id) VALUES (1)"
	_, _, ok := extractReturning(sql)
	if ok {
		t.Fatal("should not find RETURNING")
	}
}

func TestExtractReturningInsideStringNotMatched(t *testing.T) {
	// The word "returning" appears inside a string literal — must not trigger.
	sql := "INSERT INTO t (note) VALUES ('returning home') RETURNING id"
	stripped, cols, ok := extractReturning(sql)
	if !ok {
		t.Fatal("expected real RETURNING at end")
	}
	if cols != "id" {
		t.Fatalf("cols=%q", cols)
	}
	_ = stripped
}

func TestExtractReturningCaseInsensitive(t *testing.T) {
	sql := "INSERT INTO t (id) VALUES (1) returning id, name"
	stripped, cols, ok := extractReturning(sql)
	if !ok {
		t.Fatal("expected RETURNING (case-insensitive)")
	}
	if cols != "id, name" {
		t.Fatalf("cols=%q", cols)
	}
	_ = stripped
}

// ── projectReturning unit tests ───────────────────────────────────────────────

func TestProjectReturningStar(t *testing.T) {
	rows := []Row{{"id": intVal(1), "name": strVal("alice")}}
	result, err := projectReturning(rows, "*")
	if err != nil {
		t.Fatal(err)
	}
	if len(result) != 1 || result[0]["name"] != strVal("alice") {
		t.Fatalf("unexpected result: %v", result)
	}
}

func TestProjectReturningSpecific(t *testing.T) {
	rows := []Row{{"id": intVal(1), "name": strVal("alice"), "age": intVal(30)}}
	result, err := projectReturning(rows, "id, name")
	if err != nil {
		t.Fatal(err)
	}
	if len(result) != 1 {
		t.Fatalf("expected 1 row")
	}
	if _, hasAge := result[0]["age"]; hasAge {
		t.Fatal("age should not be in result")
	}
	if result[0]["id"] != intVal(1) || result[0]["name"] != strVal("alice") {
		t.Fatalf("unexpected values: %v", result[0])
	}
}

func TestProjectReturningAlias(t *testing.T) {
	rows := []Row{{"id": intVal(5)}}
	result, err := projectReturning(rows, "id AS user_id")
	if err != nil {
		t.Fatal(err)
	}
	if result[0]["user_id"] != intVal(5) {
		t.Fatalf("expected user_id=5, got %v", result[0])
	}
	if _, hasID := result[0]["id"]; hasID {
		t.Fatal("id should be renamed to user_id")
	}
}

// ── INSERT RETURNING integration tests ───────────────────────────────────────

func TestInsertReturningStar(t *testing.T) {
	db := New()
	rows, err := db.Query("INSERT INTO users (id, name, age) VALUES (1, 'alice', 30) RETURNING *")
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 1 {
		t.Fatalf("expected 1 row, got %d", len(rows))
	}
	if rows[0]["id"] != intVal(1) || rows[0]["name"] != strVal("alice") {
		t.Fatalf("unexpected row: %v", rows[0])
	}
}

func TestInsertReturningSpecificCols(t *testing.T) {
	db := New()
	rows, err := db.Query("INSERT INTO users (id, name, age) VALUES (2, 'bob', 25) RETURNING id, name")
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 1 {
		t.Fatalf("expected 1 row")
	}
	if rows[0]["id"] != intVal(2) {
		t.Fatalf("id=%v", rows[0]["id"])
	}
	if rows[0]["name"] != strVal("bob") {
		t.Fatalf("name=%v", rows[0]["name"])
	}
	if _, hasAge := rows[0]["age"]; hasAge {
		t.Fatal("age should not be projected")
	}
}

func TestInsertReturningMultipleRows(t *testing.T) {
	db := New()
	rows, err := db.Query(`
		INSERT INTO items (id, label)
		VALUES (1, 'a'), (2, 'b'), (3, 'c')
		RETURNING id`)
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 3 {
		t.Fatalf("expected 3 rows, got %d", len(rows))
	}
	for i, row := range rows {
		if row["id"] != intVal(int64(i+1)) {
			t.Fatalf("row %d: id=%v", i, row["id"])
		}
	}
}

func TestInsertReturningAlias(t *testing.T) {
	db := New()
	rows, err := db.Query("INSERT INTO t (id) VALUES (42) RETURNING id AS created_id")
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 1 || rows[0]["created_id"] != intVal(42) {
		t.Fatalf("unexpected: %v", rows)
	}
}

func TestInsertReturningDoesNotContaminateTable(t *testing.T) {
	db := New()
	_, err := db.Query("INSERT INTO t (id, val) VALUES (1, 'x') RETURNING id")
	if err != nil {
		t.Fatal(err)
	}
	// The row must actually be in the table.
	rows := mustQuery(t, db, "SELECT id, val FROM t")
	if len(rows) != 1 || rows[0]["val"] != strVal("x") {
		t.Fatalf("row not in table: %v", rows)
	}
}

// ── UPDATE RETURNING integration tests ───────────────────────────────────────

func TestUpdateReturningStar(t *testing.T) {
	db := New()
	mustExec(t, db, "INSERT INTO users (id, name, age) VALUES (1, 'alice', 30)")
	mustExec(t, db, "INSERT INTO users (id, name, age) VALUES (2, 'bob', 25)")

	rows, err := db.Query("UPDATE users SET age = 31 WHERE id = 1 RETURNING *")
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 1 {
		t.Fatalf("expected 1 row, got %d", len(rows))
	}
	if rows[0]["age"] != intVal(31) {
		t.Fatalf("expected age=31, got %v", rows[0]["age"])
	}
	// Table is also updated.
	got := mustQuery(t, db, "SELECT age FROM users WHERE id = 1")
	if got[0]["age"] != intVal(31) {
		t.Fatal("table not updated")
	}
}

func TestUpdateReturningSpecificCols(t *testing.T) {
	db := New()
	mustExec(t, db, "INSERT INTO users (id, name, age) VALUES (3, 'carol', 22)")

	rows, err := db.Query("UPDATE users SET name = 'caroline' WHERE id = 3 RETURNING id, name")
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 1 {
		t.Fatalf("expected 1 row")
	}
	if rows[0]["name"] != strVal("caroline") {
		t.Fatalf("name=%v", rows[0]["name"])
	}
	if _, hasAge := rows[0]["age"]; hasAge {
		t.Fatal("age should not be projected")
	}
}

func TestUpdateReturningMultipleRows(t *testing.T) {
	db := New()
	mustExec(t, db, "INSERT INTO scores (id, score) VALUES (1, 10)")
	mustExec(t, db, "INSERT INTO scores (id, score) VALUES (2, 10)")
	mustExec(t, db, "INSERT INTO scores (id, score) VALUES (3, 20)")

	rows, err := db.Query("UPDATE scores SET score = 99 WHERE score = 10 RETURNING id")
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 2 {
		t.Fatalf("expected 2 rows, got %d", len(rows))
	}
}

func TestUpdateReturningNoMatch(t *testing.T) {
	db := New()
	mustExec(t, db, "INSERT INTO t (id) VALUES (1)")

	rows, err := db.Query("UPDATE t SET id = 99 WHERE id = 999 RETURNING id")
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 0 {
		t.Fatalf("expected 0 rows, got %d", len(rows))
	}
}

// ── DELETE RETURNING integration tests ───────────────────────────────────────

func TestDeleteReturningStar(t *testing.T) {
	db := New()
	mustExec(t, db, "INSERT INTO users (id, name) VALUES (1, 'alice')")
	mustExec(t, db, "INSERT INTO users (id, name) VALUES (2, 'bob')")

	rows, err := db.Query("DELETE FROM users WHERE id = 1 RETURNING *")
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 1 {
		t.Fatalf("expected 1 row, got %d", len(rows))
	}
	if rows[0]["name"] != strVal("alice") {
		t.Fatalf("name=%v", rows[0]["name"])
	}
	// Row must be gone from the table.
	remaining := mustQuery(t, db, "SELECT id FROM users")
	if len(remaining) != 1 || remaining[0]["id"] != intVal(2) {
		t.Fatalf("unexpected remaining: %v", remaining)
	}
}

func TestDeleteReturningSpecificCols(t *testing.T) {
	db := New()
	mustExec(t, db, "INSERT INTO items (id, label, qty) VALUES (10, 'widget', 5)")

	rows, err := db.Query("DELETE FROM items WHERE id = 10 RETURNING id, label")
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 1 {
		t.Fatalf("expected 1 row")
	}
	if rows[0]["id"] != intVal(10) || rows[0]["label"] != strVal("widget") {
		t.Fatalf("unexpected: %v", rows[0])
	}
	if _, hasQty := rows[0]["qty"]; hasQty {
		t.Fatal("qty should not be projected")
	}
}

func TestDeleteReturningMultiple(t *testing.T) {
	db := New()
	mustExec(t, db, "INSERT INTO t (id, val) VALUES (1, 'a')")
	mustExec(t, db, "INSERT INTO t (id, val) VALUES (2, 'b')")
	mustExec(t, db, "INSERT INTO t (id, val) VALUES (3, 'c')")

	rows, err := db.Query("DELETE FROM t WHERE id < 3 RETURNING id")
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 2 {
		t.Fatalf("expected 2 rows, got %d", len(rows))
	}
	// Remaining row.
	remaining := mustQuery(t, db, "SELECT id FROM t")
	if len(remaining) != 1 || remaining[0]["id"] != intVal(3) {
		t.Fatalf("unexpected remaining: %v", remaining)
	}
}

func TestDeleteReturningAll(t *testing.T) {
	db := New()
	mustExec(t, db, "INSERT INTO t (id) VALUES (1)")
	mustExec(t, db, "INSERT INTO t (id) VALUES (2)")

	rows, err := db.Query("DELETE FROM t RETURNING id")
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 2 {
		t.Fatalf("expected 2 deleted rows, got %d", len(rows))
	}
	remaining := mustQuery(t, db, "SELECT id FROM t")
	if len(remaining) != 0 {
		t.Fatal("table should be empty")
	}
}

// ── Named-parameter RETURNING ─────────────────────────────────────────────────

func TestInsertReturningNamed(t *testing.T) {
	db := New()
	rows, err := db.QueryNamed(
		"INSERT INTO users (id, name) VALUES (:id, :name) RETURNING id, name",
		map[string]any{"id": int64(7), "name": "dave"},
	)
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 1 || rows[0]["id"] != intVal(7) || rows[0]["name"] != strVal("dave") {
		t.Fatalf("unexpected: %v", rows)
	}
}
