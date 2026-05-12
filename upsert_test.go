package vapordb

import "testing"

// ── rewriteOnConflict unit tests ──────────────────────────────────────────────

func TestRewriteOnConflictDoUpdate(t *testing.T) {
	sql := "INSERT INTO t (id, name) VALUES (1, 'alice') ON CONFLICT (id) DO UPDATE SET name = EXCLUDED.name"
	got, cols, doNothing, _ := rewriteOnConflict(sql)

	if doNothing {
		t.Fatal("expected doNothing=false")
	}
	if len(cols) != 1 || cols[0] != "id" {
		t.Errorf("conflict cols: want [id], got %v", cols)
	}
	want := "INSERT INTO t (id, name) VALUES (1, 'alice') ON DUPLICATE KEY UPDATE name = VALUES(name)"
	if got != want {
		t.Errorf("rewrite mismatch:\n  got  %q\n  want %q", got, want)
	}
}

func TestRewriteOnConflictDoNothing(t *testing.T) {
	sql := "INSERT INTO t (id, name) VALUES (1, 'alice') ON CONFLICT (id) DO NOTHING"
	got, cols, doNothing, _ := rewriteOnConflict(sql)

	if !doNothing {
		t.Fatal("expected doNothing=true")
	}
	if len(cols) != 1 || cols[0] != "id" {
		t.Errorf("conflict cols: want [id], got %v", cols)
	}
	want := "INSERT INTO t (id, name) VALUES (1, 'alice')"
	if got != want {
		t.Errorf("rewrite mismatch:\n  got  %q\n  want %q", got, want)
	}
}

func TestRewriteOnConflictMultipleConflictCols(t *testing.T) {
	sql := "INSERT INTO t (a, b, c) VALUES (1, 2, 3) ON CONFLICT (a, b) DO UPDATE SET c = EXCLUDED.c"
	_, cols, _, _ := rewriteOnConflict(sql)
	if len(cols) != 2 || cols[0] != "a" || cols[1] != "b" {
		t.Errorf("expected [a b], got %v", cols)
	}
}

func TestRewriteNoOnConflict(t *testing.T) {
	sql := "INSERT INTO t (id, name) VALUES (1, 'alice')"
	got, cols, doNothing, _ := rewriteOnConflict(sql)
	if got != sql || len(cols) != 0 || doNothing {
		t.Errorf("plain INSERT should pass through unchanged; got %q %v %v", got, cols, doNothing)
	}
}

// ── integration tests ─────────────────────────────────────────────────────────

func TestUpsertUpdateOnConflict(t *testing.T) {
	db := New()
	mustExec(t, db, `INSERT INTO users (id, name, age) VALUES (1, 'alice', 30)`)

	// Upsert same id — should update name and age.
	mustExec(t, db, `INSERT INTO users (id, name, age) VALUES (1, 'Alice', 31)
		ON CONFLICT (id) DO UPDATE SET name = EXCLUDED.name, age = EXCLUDED.age`)

	rows := mustQuery(t, db, `SELECT id, name, age FROM users`)
	if len(rows) != 1 {
		t.Fatalf("want 1 row, got %d", len(rows))
	}
	if rows[0]["name"] != strVal("Alice") {
		t.Errorf("name: want 'Alice', got %v", rows[0]["name"])
	}
	if rows[0]["age"] != intVal(31) {
		t.Errorf("age: want 31, got %v", rows[0]["age"])
	}
}

func TestUpsertInsertOnNoConflict(t *testing.T) {
	db := New()
	mustExec(t, db, `INSERT INTO users (id, name) VALUES (1, 'alice')`)
	mustExec(t, db, `INSERT INTO users (id, name) VALUES (2, 'bob')
		ON CONFLICT (id) DO UPDATE SET name = EXCLUDED.name`)

	rows := mustQuery(t, db, `SELECT id FROM users ORDER BY id`)
	if len(rows) != 2 {
		t.Fatalf("want 2 rows, got %d", len(rows))
	}
}

func TestUpsertPartialUpdate(t *testing.T) {
	db := New()
	mustExec(t, db, `INSERT INTO products (sku, name, price) VALUES ('A1', 'Widget', 9.99)`)

	// Only update price, leave name untouched.
	mustExec(t, db, `INSERT INTO products (sku, name, price) VALUES ('A1', 'Widget', 14.99)
		ON CONFLICT (sku) DO UPDATE SET price = EXCLUDED.price`)

	rows := mustQuery(t, db, `SELECT sku, name, price FROM products`)
	if rows[0]["name"] != strVal("Widget") {
		t.Errorf("name should be unchanged 'Widget', got %v", rows[0]["name"])
	}
	if rows[0]["price"] != floatVal(14.99) {
		t.Errorf("price: want 14.99, got %v", rows[0]["price"])
	}
}

func TestUpsertDoNothing(t *testing.T) {
	db := New()
	mustExec(t, db, `INSERT INTO users (id, name) VALUES (1, 'alice')`)
	mustExec(t, db, `INSERT INTO users (id, name) VALUES (1, 'bob') ON CONFLICT (id) DO NOTHING`)

	rows := mustQuery(t, db, `SELECT name FROM users`)
	if len(rows) != 1 {
		t.Fatalf("want 1 row, got %d", len(rows))
	}
	if rows[0]["name"] != strVal("alice") {
		t.Errorf("DO NOTHING should preserve original; got %v", rows[0]["name"])
	}
}

func TestUpsertDoNothingNoConflict(t *testing.T) {
	db := New()
	mustExec(t, db, `INSERT INTO users (id, name) VALUES (1, 'alice')`)
	mustExec(t, db, `INSERT INTO users (id, name) VALUES (2, 'bob') ON CONFLICT (id) DO NOTHING`)

	rows := mustQuery(t, db, `SELECT id FROM users ORDER BY id`)
	if len(rows) != 2 {
		t.Fatalf("want 2 rows after DO NOTHING with no conflict, got %d", len(rows))
	}
}

func TestUpsertMultiBatch(t *testing.T) {
	db := New()
	mustExec(t, db, `INSERT INTO kv (k, v) VALUES ('a', 1), ('b', 2)`)

	// Second batch: 'a' conflicts, 'c' is new.
	mustExec(t, db, `INSERT INTO kv (k, v) VALUES ('a', 10), ('c', 3)
		ON CONFLICT (k) DO UPDATE SET v = EXCLUDED.v`)

	rows := mustQuery(t, db, `SELECT k, v FROM kv ORDER BY k`)
	if len(rows) != 3 {
		t.Fatalf("want 3 rows, got %d", len(rows))
	}
	wantV := map[string]int64{"a": 10, "b": 2, "c": 3}
	for _, r := range rows {
		k := r["k"].V.(string)
		got := r["v"].V.(int64)
		if got != wantV[k] {
			t.Errorf("kv[%q]: want %d, got %d", k, wantV[k], got)
		}
	}
}

func TestUpsertCompositeKey(t *testing.T) {
	db := New()
	mustExec(t, db, `INSERT INTO scores (user_id, game, score) VALUES (1, 'chess', 100)`)
	mustExec(t, db, `INSERT INTO scores (user_id, game, score) VALUES (1, 'chess', 200)
		ON CONFLICT (user_id, game) DO UPDATE SET score = EXCLUDED.score`)

	rows := mustQuery(t, db, `SELECT score FROM scores`)
	if len(rows) != 1 {
		t.Fatalf("want 1 row, got %d", len(rows))
	}
	if rows[0]["score"] != intVal(200) {
		t.Errorf("composite conflict: want score=200, got %v", rows[0]["score"])
	}
}

func TestUpsertConstantInUpdate(t *testing.T) {
	db := New()
	mustExec(t, db, `INSERT INTO t (id, status) VALUES (1, 'pending')`)
	mustExec(t, db, `INSERT INTO t (id, status) VALUES (1, 'done')
		ON CONFLICT (id) DO UPDATE SET status = 'active'`)

	rows := mustQuery(t, db, `SELECT status FROM t`)
	if rows[0]["status"] != strVal("active") {
		t.Errorf("constant update: want 'active', got %v", rows[0]["status"])
	}
}

// ── RETURNING on upsert ──────────────────────────────────────────────────────

func TestUpsertReturningOnConflictUpdate(t *testing.T) {
	db := New()
	mustExec(t, db, `INSERT INTO t (id, name) VALUES (1, 'a')`)
	rows, err := db.Query(`
		INSERT INTO t (id, name) VALUES (1, 'b')
		ON CONFLICT (id) DO UPDATE SET name = EXCLUDED.name
		RETURNING id, name`)
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 1 || rows[0]["id"] != intVal(1) || rows[0]["name"] != strVal("b") {
		t.Fatalf("want one row id=1 name=b, got %v", rows)
	}
}

func TestUpsertReturningOnConflictDoNothing(t *testing.T) {
	db := New()
	mustExec(t, db, `INSERT INTO t (id, name) VALUES (1, 'a')`)
	rows, err := db.Query(`
		INSERT INTO t (id, name) VALUES (1, 'b')
		ON CONFLICT (id) DO NOTHING
		RETURNING id, name`)
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 0 {
		t.Fatalf("DO NOTHING: want no RETURNING rows, got %d %v", len(rows), rows)
	}
}

func TestUpsertReturningMixedValuesOrder(t *testing.T) {
	db := New()
	mustExec(t, db, `INSERT INTO t (id, v) VALUES (1, 1)`)
	rows, err := db.Query(`
		INSERT INTO t (id, v) VALUES (2, 2), (1, 99)
		ON CONFLICT (id) DO UPDATE SET v = EXCLUDED.v
		RETURNING id, v`)
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 2 {
		t.Fatalf("want 2 rows, got %v", rows)
	}
	if rows[0]["id"] != intVal(2) || rows[0]["v"] != intVal(2) {
		t.Fatalf("row0: want id=2 v=2, got %v", rows[0])
	}
	if rows[1]["id"] != intVal(1) || rows[1]["v"] != intVal(99) {
		t.Fatalf("row1: want id=1 v=99, got %v", rows[1])
	}
}
