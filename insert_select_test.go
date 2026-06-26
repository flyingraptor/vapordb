package vapordb

import (
	"sort"
	"strings"
	"testing"
)

// ─── helpers ──────────────────────────────────────────────────────────────────

func execExpectErr(t *testing.T, db *DB, sql, want string) {
	t.Helper()
	err := db.Exec(sql)
	if err == nil {
		t.Fatalf("expected error containing %q, got nil", want)
	}
	if !strings.Contains(err.Error(), want) {
		t.Fatalf("error %q does not contain %q", err.Error(), want)
	}
}

// sortedInts pulls one int64 column out of rows and returns it sorted.
func sortedInts(t *testing.T, rows []Row, col string) []int64 {
	t.Helper()
	out := make([]int64, 0, len(rows))
	for _, r := range rows {
		v, ok := r[col]
		if !ok {
			t.Fatalf("row missing column %q: %v", col, r)
		}
		n, ok := v.V.(int64)
		if !ok {
			t.Fatalf("column %q is not int64: %#v", col, v)
		}
		out = append(out, n)
	}
	sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
	return out
}

// ─── basic INSERT … SELECT ─────────────────────────────────────────────────────

func TestInsertSelectBasicCopy(t *testing.T) {
	db := New()
	mustExec(t, db, `INSERT INTO src (id, name) VALUES (1, 'Alice')`)
	mustExec(t, db, `INSERT INTO src (id, name) VALUES (2, 'Bob')`)

	mustExec(t, db, `INSERT INTO dst (id, name) SELECT id, name FROM src`)

	rows := mustQuery(t, db, `SELECT id, name FROM dst ORDER BY id`)
	if len(rows) != 2 {
		t.Fatalf("want 2 rows, got %d: %v", len(rows), rows)
	}
	if rows[0]["id"] != intVal(1) || rows[0]["name"] != strVal("Alice") {
		t.Errorf("row0 = %v", rows[0])
	}
	if rows[1]["id"] != intVal(2) || rows[1]["name"] != strVal("Bob") {
		t.Errorf("row1 = %v", rows[1])
	}
}

func TestInsertSelectWithWhere(t *testing.T) {
	db := New()
	mustExec(t, db, `INSERT INTO src (id, name, active) VALUES (1, 'Alice', true)`)
	mustExec(t, db, `INSERT INTO src (id, name, active) VALUES (2, 'Bob', false)`)
	mustExec(t, db, `INSERT INTO src (id, name, active) VALUES (3, 'Carol', true)`)

	mustExec(t, db, `INSERT INTO dst (id, name) SELECT id, name FROM src WHERE active = true`)

	rows := mustQuery(t, db, `SELECT id FROM dst`)
	got := sortedInts(t, rows, "id")
	if len(got) != 2 || got[0] != 1 || got[1] != 3 {
		t.Fatalf("want ids [1 3], got %v", got)
	}
}

// Target columns receive SELECT outputs positionally, not by name.
func TestInsertSelectPositionalMapping(t *testing.T) {
	db := New()
	mustExec(t, db, `INSERT INTO src (a, b) VALUES (10, 20)`)

	// dst.x <- src.b, dst.y <- src.a (reversed order on purpose).
	mustExec(t, db, `INSERT INTO dst (x, y) SELECT b, a FROM src`)

	rows := mustQuery(t, db, `SELECT x, y FROM dst`)
	if len(rows) != 1 {
		t.Fatalf("want 1 row, got %d", len(rows))
	}
	if rows[0]["x"] != intVal(20) || rows[0]["y"] != intVal(10) {
		t.Fatalf("positional mapping wrong: %v", rows[0])
	}
}

// Computed expressions and constant literals in the projection.
func TestInsertSelectExpressionsAndLiterals(t *testing.T) {
	db := New()
	mustExec(t, db, `INSERT INTO src (id, price, qty) VALUES (1, 5, 3)`)
	mustExec(t, db, `INSERT INTO src (id, price, qty) VALUES (2, 10, 2)`)

	mustExec(t, db, `INSERT INTO dst (id, total, status)
		SELECT id, price * qty, 'imported' FROM src`)

	rows := mustQuery(t, db, `SELECT id, total, status FROM dst ORDER BY id`)
	if len(rows) != 2 {
		t.Fatalf("want 2 rows, got %d", len(rows))
	}
	if rows[0]["total"] != intVal(15) || rows[0]["status"] != strVal("imported") {
		t.Errorf("row0 = %v", rows[0])
	}
	if rows[1]["total"] != intVal(20) || rows[1]["status"] != strVal("imported") {
		t.Errorf("row1 = %v", rows[1])
	}
}

// Inserting from the same table must read a stable snapshot (no infinite loop /
// reading rows we just wrote).
func TestInsertSelectFromSameTable(t *testing.T) {
	db := New()
	mustExec(t, db, `INSERT INTO t (id, name) VALUES (1, 'a')`)
	mustExec(t, db, `INSERT INTO t (id, name) VALUES (2, 'b')`)

	mustExec(t, db, `INSERT INTO t (id, name) SELECT id + 100, name FROM t`)

	rows := mustQuery(t, db, `SELECT id FROM t`)
	got := sortedInts(t, rows, "id")
	want := []int64{1, 2, 101, 102}
	if len(got) != len(want) {
		t.Fatalf("want %v, got %v", want, got)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("want %v, got %v", want, got)
		}
	}
}

func TestInsertSelectFromJoin(t *testing.T) {
	db := New()
	mustExec(t, db, `INSERT INTO users (id, name) VALUES (1, 'Alice')`)
	mustExec(t, db, `INSERT INTO users (id, name) VALUES (2, 'Bob')`)
	mustExec(t, db, `INSERT INTO orders (user_id, amount) VALUES (1, 100)`)
	mustExec(t, db, `INSERT INTO orders (user_id, amount) VALUES (2, 250)`)

	mustExec(t, db, `INSERT INTO report (name, amount)
		SELECT u.name, o.amount FROM users u JOIN orders o ON u.id = o.user_id`)

	rows := mustQuery(t, db, `SELECT name, amount FROM report ORDER BY amount`)
	if len(rows) != 2 {
		t.Fatalf("want 2 rows, got %d: %v", len(rows), rows)
	}
	if rows[0]["name"] != strVal("Alice") || rows[0]["amount"] != intVal(100) {
		t.Errorf("row0 = %v", rows[0])
	}
	if rows[1]["name"] != strVal("Bob") || rows[1]["amount"] != intVal(250) {
		t.Errorf("row1 = %v", rows[1])
	}
}

func TestInsertSelectWithAggregate(t *testing.T) {
	db := New()
	mustExec(t, db, `INSERT INTO sales (region, amount) VALUES ('east', 10)`)
	mustExec(t, db, `INSERT INTO sales (region, amount) VALUES ('east', 30)`)
	mustExec(t, db, `INSERT INTO sales (region, amount) VALUES ('west', 5)`)

	mustExec(t, db, `INSERT INTO totals (region, total)
		SELECT region, SUM(amount) AS total FROM sales GROUP BY region`)

	rows := mustQuery(t, db, `SELECT region, total FROM totals ORDER BY region`)
	if len(rows) != 2 {
		t.Fatalf("want 2 rows, got %d: %v", len(rows), rows)
	}
	if rows[0]["region"] != strVal("east") || rows[0]["total"] != intVal(40) {
		t.Errorf("row0 = %v", rows[0])
	}
	if rows[1]["region"] != strVal("west") || rows[1]["total"] != intVal(5) {
		t.Errorf("row1 = %v", rows[1])
	}
}

func TestInsertSelectWithOrderByLimit(t *testing.T) {
	db := New()
	for i := 1; i <= 5; i++ {
		mustExec(t, db, `INSERT INTO src (id) VALUES (`+itoa(i)+`)`)
	}

	mustExec(t, db, `INSERT INTO dst (id) SELECT id FROM src ORDER BY id DESC LIMIT 2`)

	rows := mustQuery(t, db, `SELECT id FROM dst`)
	got := sortedInts(t, rows, "id")
	if len(got) != 2 || got[0] != 4 || got[1] != 5 {
		t.Fatalf("want top-2 ids [4 5], got %v", got)
	}
}

func TestInsertSelectDistinct(t *testing.T) {
	db := New()
	mustExec(t, db, `INSERT INTO src (tag) VALUES ('a')`)
	mustExec(t, db, `INSERT INTO src (tag) VALUES ('a')`)
	mustExec(t, db, `INSERT INTO src (tag) VALUES ('b')`)

	mustExec(t, db, `INSERT INTO dst (tag) SELECT DISTINCT tag FROM src`)

	rows := mustQuery(t, db, `SELECT tag FROM dst`)
	if len(rows) != 2 {
		t.Fatalf("want 2 distinct rows, got %d: %v", len(rows), rows)
	}
}

func TestInsertSelectEmptyResult(t *testing.T) {
	db := New()
	mustExec(t, db, `INSERT INTO src (id) VALUES (1)`)
	// Pre-create dst so the SELECT-into produces zero rows but doesn't error.
	mustExec(t, db, `INSERT INTO dst (id) VALUES (99)`)

	mustExec(t, db, `INSERT INTO dst (id) SELECT id FROM src WHERE id > 100`)

	rows := mustQuery(t, db, `SELECT id FROM dst`)
	if len(rows) != 1 {
		t.Fatalf("want 1 row (unchanged), got %d: %v", len(rows), rows)
	}
}

func TestInsertSelectFromDerivedTable(t *testing.T) {
	db := New()
	mustExec(t, db, `INSERT INTO src (id, v) VALUES (1, 5)`)
	mustExec(t, db, `INSERT INTO src (id, v) VALUES (2, 15)`)

	mustExec(t, db, `INSERT INTO dst (id, v)
		SELECT id, v FROM (SELECT id, v FROM src WHERE v > 10) AS sub`)

	rows := mustQuery(t, db, `SELECT id, v FROM dst`)
	if len(rows) != 1 || rows[0]["id"] != intVal(2) {
		t.Fatalf("want single row id=2, got %v", rows)
	}
}

// ─── UNION sources ─────────────────────────────────────────────────────────────

func TestInsertSelectUnionAll(t *testing.T) {
	db := New()
	mustExec(t, db, `INSERT INTO a (id, name) VALUES (1, 'x')`)
	mustExec(t, db, `INSERT INTO b (id, name) VALUES (2, 'y')`)

	mustExec(t, db, `INSERT INTO dst (id, name)
		SELECT id, name FROM a UNION ALL SELECT id, name FROM b`)

	rows := mustQuery(t, db, `SELECT id FROM dst`)
	got := sortedInts(t, rows, "id")
	if len(got) != 2 || got[0] != 1 || got[1] != 2 {
		t.Fatalf("want [1 2], got %v", got)
	}
}

func TestInsertSelectUnionDistinct(t *testing.T) {
	db := New()
	mustExec(t, db, `INSERT INTO a (id, name) VALUES (1, 'x')`)
	mustExec(t, db, `INSERT INTO b (id, name) VALUES (1, 'x')`)
	mustExec(t, db, `INSERT INTO b (id, name) VALUES (2, 'y')`)

	mustExec(t, db, `INSERT INTO dst (id, name)
		SELECT id, name FROM a UNION SELECT id, name FROM b`)

	rows := mustQuery(t, db, `SELECT id FROM dst`)
	got := sortedInts(t, rows, "id")
	if len(got) != 2 || got[0] != 1 || got[1] != 2 {
		t.Fatalf("UNION should dedup to [1 2], got %v", got)
	}
}

// ─── ON CONFLICT interaction ───────────────────────────────────────────────────

func TestInsertSelectOnConflictDoNothing(t *testing.T) {
	db := New()
	mustExec(t, db, `INSERT INTO dst (id, name) VALUES (1, 'original')`)
	mustExec(t, db, `INSERT INTO src (id, name) VALUES (1, 'fromsrc')`)
	mustExec(t, db, `INSERT INTO src (id, name) VALUES (2, 'newrow')`)

	mustExec(t, db, `INSERT INTO dst (id, name)
		SELECT id, name FROM src ON CONFLICT (id) DO NOTHING`)

	rows := mustQuery(t, db, `SELECT id, name FROM dst ORDER BY id`)
	if len(rows) != 2 {
		t.Fatalf("want 2 rows, got %d: %v", len(rows), rows)
	}
	if rows[0]["name"] != strVal("original") {
		t.Errorf("id=1 should be untouched, got %v", rows[0])
	}
	if rows[1]["name"] != strVal("newrow") {
		t.Errorf("id=2 should be inserted, got %v", rows[1])
	}
}

func TestInsertSelectOnConflictDoUpdate(t *testing.T) {
	db := New()
	mustExec(t, db, `INSERT INTO dst (id, name) VALUES (1, 'original')`)
	mustExec(t, db, `INSERT INTO src (id, name) VALUES (1, 'updated')`)
	mustExec(t, db, `INSERT INTO src (id, name) VALUES (2, 'fresh')`)

	mustExec(t, db, `INSERT INTO dst (id, name)
		SELECT id, name FROM src
		ON CONFLICT (id) DO UPDATE SET name = EXCLUDED.name`)

	rows := mustQuery(t, db, `SELECT id, name FROM dst ORDER BY id`)
	if len(rows) != 2 {
		t.Fatalf("want 2 rows, got %d: %v", len(rows), rows)
	}
	if rows[0]["name"] != strVal("updated") {
		t.Errorf("id=1 should be updated, got %v", rows[0])
	}
	if rows[1]["name"] != strVal("fresh") {
		t.Errorf("id=2 should be inserted, got %v", rows[1])
	}
}

// ─── RETURNING interaction ─────────────────────────────────────────────────────

func TestInsertSelectReturning(t *testing.T) {
	db := New()
	mustExec(t, db, `INSERT INTO src (id, name) VALUES (1, 'Alice')`)
	mustExec(t, db, `INSERT INTO src (id, name) VALUES (2, 'Bob')`)

	rows := mustQuery(t, db, `INSERT INTO dst (id, name) SELECT id, name FROM src RETURNING id, name`)
	if len(rows) != 2 {
		t.Fatalf("want 2 returned rows, got %d: %v", len(rows), rows)
	}
	got := sortedInts(t, rows, "id")
	if got[0] != 1 || got[1] != 2 {
		t.Fatalf("returned ids = %v", got)
	}
}

func TestInsertSelectReturningStar(t *testing.T) {
	db := New()
	mustExec(t, db, `INSERT INTO src (id, name) VALUES (7, 'g')`)

	rows := mustQuery(t, db, `INSERT INTO dst (id, name) SELECT id, name FROM src RETURNING *`)
	if len(rows) != 1 {
		t.Fatalf("want 1 returned row, got %d", len(rows))
	}
	if rows[0]["id"] != intVal(7) || rows[0]["name"] != strVal("g") {
		t.Fatalf("returned row = %v", rows[0])
	}
}

// The fixed RETURNING path must report rows touched by DO UPDATE, not just
// freshly appended ones — exercised here through an INSERT … SELECT upsert.
func TestInsertSelectOnConflictDoUpdateReturning(t *testing.T) {
	db := New()
	mustExec(t, db, `INSERT INTO dst (id, name) VALUES (1, 'original')`)
	mustExec(t, db, `INSERT INTO src (id, name) VALUES (1, 'updated')`)
	mustExec(t, db, `INSERT INTO src (id, name) VALUES (2, 'fresh')`)

	rows := mustQuery(t, db, `INSERT INTO dst (id, name)
		SELECT id, name FROM src
		ON CONFLICT (id) DO UPDATE SET name = EXCLUDED.name
		RETURNING id, name`)
	if len(rows) != 2 {
		t.Fatalf("want 2 returned rows (1 updated + 1 inserted), got %d: %v", len(rows), rows)
	}

	byID := map[int64]string{}
	for _, r := range rows {
		byID[r["id"].V.(int64)] = r["name"].V.(string)
	}
	if byID[1] != "updated" {
		t.Errorf("expected updated row 1 returned, got %v", byID)
	}
	if byID[2] != "fresh" {
		t.Errorf("expected inserted row 2 returned, got %v", byID)
	}
}

// ─── schema inference through INSERT … SELECT ──────────────────────────────────

func TestInsertSelectCreatesTableAndInfersSchema(t *testing.T) {
	db := New()
	mustExec(t, db, `INSERT INTO src (id, name, score) VALUES (1, 'Alice', 9)`)

	// dst does not exist yet — it must be created from the SELECT output.
	mustExec(t, db, `INSERT INTO dst (id, name, score) SELECT id, name, score FROM src`)

	tbl := db.Tables["dst"]
	if tbl == nil {
		t.Fatal("dst table was not created")
	}
	if tbl.Schema["id"] != KindInt || tbl.Schema["name"] != KindString || tbl.Schema["score"] != KindInt {
		t.Fatalf("unexpected inferred schema: %v", tbl.Schema)
	}
}

// ─── error cases ───────────────────────────────────────────────────────────────

func TestInsertSelectStarRejected(t *testing.T) {
	db := New()
	mustExec(t, db, `INSERT INTO src (id, name) VALUES (1, 'a')`)
	execExpectErr(t, db, `INSERT INTO dst (id, name) SELECT * FROM src`, "*")
}

func TestInsertSelectColumnCountMismatch(t *testing.T) {
	db := New()
	mustExec(t, db, `INSERT INTO src (id, name) VALUES (1, 'a')`)
	execExpectErr(t, db, `INSERT INTO dst (id) SELECT id, name FROM src`, "mismatch")
}

func TestInsertSelectDuplicateOutputColumn(t *testing.T) {
	db := New()
	mustExec(t, db, `INSERT INTO src (id, other) VALUES (1, 2)`)
	// Both projection terms resolve to output key "id".
	execExpectErr(t, db,
		`INSERT INTO dst (a, b) SELECT id, other AS id FROM src`,
		"duplicate output column")
}

func TestInsertSelectNoColumnListRejected(t *testing.T) {
	db := New()
	mustExec(t, db, `INSERT INTO src (id) VALUES (1)`)
	execExpectErr(t, db, `INSERT INTO dst SELECT id FROM src`, "explicit column list")
}

func TestInsertSelectUnionMismatchedNamesRejected(t *testing.T) {
	db := New()
	mustExec(t, db, `INSERT INTO a (id, name) VALUES (1, 'x')`)
	mustExec(t, db, `INSERT INTO b (pid, pname) VALUES (2, 'y')`)
	// Branch column names differ → reject rather than silently insert NULLs.
	execExpectErr(t, db,
		`INSERT INTO dst (id, name) SELECT id, name FROM a UNION ALL SELECT pid, pname FROM b`,
		"mismatched output column")
}

func TestInsertSelectUnionMatchedViaAlias(t *testing.T) {
	db := New()
	mustExec(t, db, `INSERT INTO a (id, name) VALUES (1, 'x')`)
	mustExec(t, db, `INSERT INTO b (pid, pname) VALUES (2, 'y')`)
	// Aliasing both branches to the same output names makes the mapping valid.
	mustExec(t, db, `INSERT INTO dst (id, name)
		SELECT id AS id, name AS name FROM a
		UNION ALL
		SELECT pid AS id, pname AS name FROM b`)

	rows := mustQuery(t, db, `SELECT id, name FROM dst ORDER BY id`)
	if len(rows) != 2 {
		t.Fatalf("want 2 rows, got %d: %v", len(rows), rows)
	}
	if rows[0]["name"] != strVal("x") || rows[1]["name"] != strVal("y") {
		t.Fatalf("rows = %v", rows)
	}
}

// ─── FULL OUTER JOIN source ────────────────────────────────────────────────────

func TestInsertSelectFullOuterJoin(t *testing.T) {
	db := New()
	mustExec(t, db, `INSERT INTO a (id) VALUES (1)`)
	mustExec(t, db, `INSERT INTO b (id) VALUES (2)`)

	mustExec(t, db, `INSERT INTO dst (x, y)
		SELECT a.id AS x, b.id AS y FROM a FULL OUTER JOIN b ON a.id = b.id`)

	rows := mustQuery(t, db, `SELECT x, y FROM dst`)
	if len(rows) != 2 {
		t.Fatalf("want 2 rows, got %d: %v", len(rows), rows)
	}
	// One row has x=1,y=NULL; the other x=NULL,y=2.
	var sawLeft, sawRight bool
	for _, r := range rows {
		if r["x"] == intVal(1) && r["y"] == Null {
			sawLeft = true
		}
		if r["x"] == Null && r["y"] == intVal(2) {
			sawRight = true
		}
	}
	if !sawLeft || !sawRight {
		t.Fatalf("FULL OUTER JOIN rows wrong: %v", rows)
	}
}

// ─── CTE → INSERT … SELECT ─────────────────────────────────────────────────────

func TestInsertSelectFromCTENewTable(t *testing.T) {
	db := New()
	mustExec(t, db, `INSERT INTO src (id, v) VALUES (1, 10)`)
	mustExec(t, db, `INSERT INTO src (id, v) VALUES (2, 20)`)

	// dst does not exist yet — created by the WITH … INSERT … SELECT and must
	// be persisted back to the real DB (not stranded on the CTE temp DB).
	mustExec(t, db, `WITH big AS (SELECT id, v FROM src WHERE v >= 20)
		INSERT INTO dst (id, v) SELECT id, v FROM big`)

	rows := mustQuery(t, db, `SELECT id, v FROM dst`)
	if len(rows) != 1 || rows[0]["id"] != intVal(2) {
		t.Fatalf("want single row id=2, got %v", rows)
	}
	// The CTE table must not leak into the real database.
	if _, leaked := db.Tables["big"]; leaked {
		t.Fatal("CTE table 'big' leaked into the real DB")
	}
}

func TestInsertSelectFromCTEExistingTable(t *testing.T) {
	db := New()
	mustExec(t, db, `INSERT INTO src (id, v) VALUES (2, 20)`)
	mustExec(t, db, `INSERT INTO dst (id, v) VALUES (99, 0)`)

	mustExec(t, db, `WITH big AS (SELECT id, v FROM src)
		INSERT INTO dst (id, v) SELECT id, v FROM big`)

	rows := mustQuery(t, db, `SELECT id FROM dst`)
	got := sortedInts(t, rows, "id")
	if len(got) != 2 || got[0] != 2 || got[1] != 99 {
		t.Fatalf("want [2 99], got %v", got)
	}
}

// Regression: WITH … INSERT through Exec used to unlock the wrong mutex
// (panic + permanently-locked DB). The DB must stay usable afterward.
func TestWithInsertDoesNotDeadlockDB(t *testing.T) {
	db := New()
	mustExec(t, db, `INSERT INTO src (id) VALUES (1)`)

	mustExec(t, db, `WITH c AS (SELECT id FROM src) INSERT INTO dst (id) SELECT id FROM c`)

	// If the prior call had left the lock held, this would hang/deadlock.
	mustExec(t, db, `INSERT INTO after (id) VALUES (1)`)
	rows := mustQuery(t, db, `SELECT id FROM after`)
	if len(rows) != 1 {
		t.Fatalf("DB unusable after WITH … INSERT: %v", rows)
	}
}

func TestInsertSelectFromCTEReturning(t *testing.T) {
	db := New()
	mustExec(t, db, `INSERT INTO src (id, v) VALUES (1, 5)`)
	mustExec(t, db, `INSERT INTO src (id, v) VALUES (2, 50)`)

	rows := mustQuery(t, db, `WITH big AS (SELECT id, v FROM src WHERE v > 10)
		INSERT INTO dst (id, v) SELECT id, v FROM big RETURNING id, v`)
	if len(rows) != 1 || rows[0]["id"] != intVal(2) {
		t.Fatalf("want returned row id=2, got %v", rows)
	}
	// And it was actually persisted.
	persisted := mustQuery(t, db, `SELECT id FROM dst`)
	if len(persisted) != 1 || persisted[0]["id"] != intVal(2) {
		t.Fatalf("row not persisted: %v", persisted)
	}
}

