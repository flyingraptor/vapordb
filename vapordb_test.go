package vapordb

import (
	"fmt"
	"os"
	"testing"
)

// ─── helpers ─────────────────────────────────────────────────────────────────

func mustExec(t *testing.T, db *DB, sql string) {
	t.Helper()
	if err := db.Exec(sql); err != nil {
		t.Fatalf("Exec(%q): %v", sql, err)
	}
}

func mustQuery(t *testing.T, db *DB, sql string) []Row {
	t.Helper()
	rows, err := db.Query(sql)
	if err != nil {
		t.Fatalf("Query(%q): %v", sql, err)
	}
	return rows
}

func intVal(n int64) Value { return Value{Kind: KindInt, V: n} }
func strVal(s string) Value { return Value{Kind: KindString, V: s} }

// ─── basic INSERT + SELECT ────────────────────────────────────────────────────

func TestBasicInsertSelect(t *testing.T) {
	db := New()
	mustExec(t, db, `INSERT INTO users (id, name, age) VALUES (1, 'Alice', 30)`)
	mustExec(t, db, `INSERT INTO users (id, name, age) VALUES (2, 'Bob', 25)`)
	mustExec(t, db, `INSERT INTO users (id, name, age) VALUES (3, 'Carol', 28)`)

	rows := mustQuery(t, db, `SELECT name, age FROM users WHERE age > 26 ORDER BY age DESC`)
	if len(rows) != 2 {
		t.Fatalf("expected 2 rows, got %d: %v", len(rows), rows)
	}
	// DESC order: age 30 (Alice) before age 28 (Carol).
	if rows[0]["name"] != strVal("Alice") {
		t.Errorf("expected Alice first (age=30), got %v", rows[0])
	}
	if rows[1]["name"] != strVal("Carol") {
		t.Errorf("expected Carol second (age=28), got %v", rows[1])
	}
}

// ─── auto schema inference ────────────────────────────────────────────────────

func TestNewColumnAutoAdded(t *testing.T) {
	db := New()
	mustExec(t, db, `INSERT INTO users (id, name) VALUES (1, 'Alice')`)
	mustExec(t, db, `INSERT INTO users (id, name) VALUES (2, 'Bob')`)
	// Add email column for the third row only.
	mustExec(t, db, `INSERT INTO users (id, name, email) VALUES (3, 'Carol', 'carol@example.com')`)

	tbl := db.Tables["users"]
	if _, ok := tbl.Schema["email"]; !ok {
		t.Fatal("schema should contain 'email' column")
	}
	// First two rows should have email = NULL.
	for _, r := range tbl.Rows[:2] {
		if r["email"].Kind != KindNull {
			t.Errorf("expected NULL email for row %v", r)
		}
	}
}

// ─── safe widening ────────────────────────────────────────────────────────────

func TestSafeWideningIntToFloat(t *testing.T) {
	db := New()
	mustExec(t, db, `INSERT INTO t (x) VALUES (1)`)
	if db.Tables["t"].Schema["x"] != KindInt {
		t.Fatal("expected KindInt schema after first insert")
	}
	mustExec(t, db, `INSERT INTO t (x) VALUES (3.14)`)
	if db.Tables["t"].Schema["x"] != KindFloat {
		t.Fatalf("expected KindFloat after float insert, got %v", db.Tables["t"].Schema["x"])
	}
	if len(db.Tables["t"].Rows) != 2 {
		t.Fatalf("expected 2 rows (no wipe), got %d", len(db.Tables["t"].Rows))
	}
}

// ─── unsafe conflict (wipe) ───────────────────────────────────────────────────

func TestUnsafeConflictWipes(t *testing.T) {
	db := New()
	mustExec(t, db, `INSERT INTO t (x) VALUES (1)`)
	mustExec(t, db, `INSERT INTO t (x) VALUES (2)`)
	if len(db.Tables["t"].Rows) != 2 {
		t.Fatal("expected 2 rows before conflict")
	}
	// Inserting a string into an int column crosses numeric↔string boundary → conflict → wipe.
	mustExec(t, db, `INSERT INTO t (x) VALUES ('hello')`)
	if len(db.Tables["t"].Rows) != 1 {
		t.Fatalf("expected 1 row after wipe, got %d: %v", len(db.Tables["t"].Rows), db.Tables["t"].Rows)
	}
	if db.Tables["t"].Schema["x"] != KindString {
		t.Errorf("expected KindString schema after conflict, got %v", db.Tables["t"].Schema["x"])
	}
}

// ─── UPDATE ──────────────────────────────────────────────────────────────────

func TestUpdate(t *testing.T) {
	db := New()
	mustExec(t, db, `INSERT INTO users (id, name, age) VALUES (1, 'Alice', 30)`)
	mustExec(t, db, `INSERT INTO users (id, name, age) VALUES (2, 'Bob', 25)`)
	mustExec(t, db, `UPDATE users SET age = 31 WHERE id = 1`)

	rows := mustQuery(t, db, `SELECT age FROM users WHERE id = 1`)
	if len(rows) != 1 || rows[0]["age"] != intVal(31) {
		t.Errorf("expected age=31, got %v", rows)
	}
}

// ─── DELETE ──────────────────────────────────────────────────────────────────

func TestDelete(t *testing.T) {
	db := New()
	mustExec(t, db, `INSERT INTO users (id, name) VALUES (1, 'Alice')`)
	mustExec(t, db, `INSERT INTO users (id, name) VALUES (2, 'Bob')`)
	mustExec(t, db, `DELETE FROM users WHERE id = 2`)

	rows := mustQuery(t, db, `SELECT id FROM users`)
	if len(rows) != 1 || rows[0]["id"] != intVal(1) {
		t.Errorf("expected 1 row with id=1 after delete, got %v", rows)
	}
}

// ─── LIMIT / OFFSET ──────────────────────────────────────────────────────────

func TestLimitOffset(t *testing.T) {
	db := New()
	for i := 1; i <= 5; i++ {
		mustExec(t, db, fmt.Sprintf(`INSERT INTO t (n) VALUES (%d)`, i))
	}
	rows := mustQuery(t, db, `SELECT n FROM t ORDER BY n ASC LIMIT 2 OFFSET 1`)
	if len(rows) != 2 {
		t.Fatalf("expected 2 rows, got %d", len(rows))
	}
	if rows[0]["n"] != intVal(2) || rows[1]["n"] != intVal(3) {
		t.Errorf("unexpected rows: %v", rows)
	}
}

// ─── GROUP BY + COUNT ─────────────────────────────────────────────────────────

func TestGroupByCount(t *testing.T) {
	db := New()
	mustExec(t, db, `INSERT INTO sales (dept, amount) VALUES ('eng', 100)`)
	mustExec(t, db, `INSERT INTO sales (dept, amount) VALUES ('eng', 200)`)
	mustExec(t, db, `INSERT INTO sales (dept, amount) VALUES ('sales', 50)`)

	rows := mustQuery(t, db, `SELECT dept, COUNT(*) AS cnt FROM sales GROUP BY dept ORDER BY dept ASC`)
	if len(rows) != 2 {
		t.Fatalf("expected 2 groups, got %d: %v", len(rows), rows)
	}
	for _, r := range rows {
		dept, _ := r["dept"].V.(string)
		cnt, _ := r["cnt"].V.(int64)
		switch dept {
		case "eng":
			if cnt != 2 {
				t.Errorf("eng: expected cnt=2, got %d", cnt)
			}
		case "sales":
			if cnt != 1 {
				t.Errorf("sales: expected cnt=1, got %d", cnt)
			}
		}
	}
}

// ─── INNER JOIN ───────────────────────────────────────────────────────────────

func TestInnerJoin(t *testing.T) {
	db := New()
	mustExec(t, db, `INSERT INTO users (id, name) VALUES (1, 'Alice')`)
	mustExec(t, db, `INSERT INTO users (id, name) VALUES (2, 'Bob')`)
	mustExec(t, db, `INSERT INTO orders (id, user_id, total) VALUES (10, 1, 99)`)
	mustExec(t, db, `INSERT INTO orders (id, user_id, total) VALUES (11, 1, 150)`)
	mustExec(t, db, `INSERT INTO orders (id, user_id, total) VALUES (12, 2, 50)`)

	rows := mustQuery(t, db, `
		SELECT u.name, o.total
		FROM users u
		JOIN orders o ON u.id = o.user_id
		WHERE o.total > 60
		ORDER BY o.total DESC
	`)
	if len(rows) != 2 {
		t.Fatalf("expected 2 rows, got %d: %v", len(rows), rows)
	}
	if rows[0]["total"] != intVal(150) || rows[1]["total"] != intVal(99) {
		t.Errorf("unexpected order: %v", rows)
	}
	if rows[0]["name"] != strVal("Alice") {
		t.Errorf("expected Alice, got %v", rows[0]["name"])
	}
}

// ─── LEFT JOIN ────────────────────────────────────────────────────────────────

func TestLeftJoin(t *testing.T) {
	db := New()
	mustExec(t, db, `INSERT INTO users (id, name) VALUES (1, 'Alice')`)
	mustExec(t, db, `INSERT INTO users (id, name) VALUES (2, 'Bob')`)
	mustExec(t, db, `INSERT INTO orders (id, user_id, total) VALUES (10, 1, 99)`)

	rows := mustQuery(t, db, `
		SELECT u.name, o.total
		FROM users u
		LEFT JOIN orders o ON u.id = o.user_id
		ORDER BY u.id ASC
	`)
	if len(rows) != 2 {
		t.Fatalf("expected 2 rows (including NULL right), got %d: %v", len(rows), rows)
	}
	if rows[1]["name"] != strVal("Bob") {
		t.Errorf("expected Bob in second row, got %v", rows[1])
	}
	if rows[1]["total"].Kind != KindNull {
		t.Errorf("expected NULL total for Bob, got %v", rows[1]["total"])
	}
}

// ─── SAVE / LOAD ─────────────────────────────────────────────────────────────

func TestSaveLoad(t *testing.T) {
	db := New()
	mustExec(t, db, `INSERT INTO users (id, name, age) VALUES (1, 'Alice', 30)`)
	mustExec(t, db, `INSERT INTO users (id, name, age) VALUES (2, 'Bob', 25)`)

	path := t.TempDir() + "/state.json"
	if err := db.Save(path); err != nil {
		t.Fatalf("Save: %v", err)
	}

	db2 := New()
	if err := db2.Load(path); err != nil {
		t.Fatalf("Load: %v", err)
	}
	rows := mustQuery(t, db2, `SELECT name FROM users ORDER BY id ASC`)
	if len(rows) != 2 || rows[0]["name"] != strVal("Alice") || rows[1]["name"] != strVal("Bob") {
		t.Errorf("loaded data mismatch: %v", rows)
	}
	// Loaded schema should be correct.
	if db2.Tables["users"].Schema["age"] != KindInt {
		t.Errorf("expected KindInt for age schema after load")
	}

	_ = os.Remove(path)
}

// ─── DISTINCT ────────────────────────────────────────────────────────────────

func TestDistinct(t *testing.T) {
	db := New()
	mustExec(t, db, `INSERT INTO t (x) VALUES (1)`)
	mustExec(t, db, `INSERT INTO t (x) VALUES (1)`)
	mustExec(t, db, `INSERT INTO t (x) VALUES (2)`)

	rows := mustQuery(t, db, `SELECT DISTINCT x FROM t ORDER BY x ASC`)
	if len(rows) != 2 {
		t.Fatalf("expected 2 distinct rows, got %d", len(rows))
	}
}

// ─── LIKE ────────────────────────────────────────────────────────────────────

func TestLike(t *testing.T) {
	db := New()
	mustExec(t, db, `INSERT INTO t (name) VALUES ('Alice')`)
	mustExec(t, db, `INSERT INTO t (name) VALUES ('Bob')`)
	mustExec(t, db, `INSERT INTO t (name) VALUES ('Alicia')`)

	rows := mustQuery(t, db, `SELECT name FROM t WHERE name LIKE 'Ali%'`)
	if len(rows) != 2 {
		t.Fatalf("expected 2 LIKE matches, got %d: %v", len(rows), rows)
	}
}

// ─── IN ──────────────────────────────────────────────────────────────────────

func TestIn(t *testing.T) {
	db := New()
	mustExec(t, db, `INSERT INTO t (id) VALUES (1)`)
	mustExec(t, db, `INSERT INTO t (id) VALUES (2)`)
	mustExec(t, db, `INSERT INTO t (id) VALUES (3)`)

	rows := mustQuery(t, db, `SELECT id FROM t WHERE id IN (1, 3)`)
	if len(rows) != 2 {
		t.Fatalf("expected 2 rows for IN, got %d", len(rows))
	}
}

// ─── BETWEEN ─────────────────────────────────────────────────────────────────

func TestBetween(t *testing.T) {
	db := New()
	for i := 1; i <= 5; i++ {
		mustExec(t, db, fmt.Sprintf(`INSERT INTO t (n) VALUES (%d)`, i))
	}
	rows := mustQuery(t, db, `SELECT n FROM t WHERE n BETWEEN 2 AND 4`)
	if len(rows) != 3 {
		t.Fatalf("expected 3 rows for BETWEEN, got %d", len(rows))
	}
}

// ─── aggregate: SUM / AVG ─────────────────────────────────────────────────────

func TestAggSumAvg(t *testing.T) {
	db := New()
	mustExec(t, db, `INSERT INTO t (v) VALUES (10)`)
	mustExec(t, db, `INSERT INTO t (v) VALUES (20)`)
	mustExec(t, db, `INSERT INTO t (v) VALUES (30)`)

	rows := mustQuery(t, db, `SELECT SUM(v) AS s, AVG(v) AS a FROM t`)
	if len(rows) != 1 {
		t.Fatalf("expected 1 row, got %d", len(rows))
	}
	if rows[0]["s"] != intVal(60) {
		t.Errorf("SUM: expected 60, got %v", rows[0]["s"])
	}
	avg, _ := rows[0]["a"].V.(float64)
	if avg != 20.0 {
		t.Errorf("AVG: expected 20.0, got %v", rows[0]["a"])
	}
}

// ─── CASE expression ─────────────────────────────────────────────────────────

func TestCaseExpr(t *testing.T) {
	db := New()
	mustExec(t, db, `INSERT INTO t (score) VALUES (90)`)
	mustExec(t, db, `INSERT INTO t (score) VALUES (70)`)
	mustExec(t, db, `INSERT INTO t (score) VALUES (50)`)

	rows := mustQuery(t, db, `
		SELECT score,
		       CASE WHEN score >= 80 THEN 'A' WHEN score >= 60 THEN 'B' ELSE 'C' END AS grade
		FROM t ORDER BY score DESC
	`)
	grades := []string{"A", "B", "C"}
	for i, g := range grades {
		if rows[i]["grade"] != strVal(g) {
			t.Errorf("row %d: expected grade %s, got %v", i, g, rows[i]["grade"])
		}
	}
}
