package vapordb

import "testing"

// ── Basic derived table ───────────────────────────────────────────────────────

func TestDerivedTableBasic(t *testing.T) {
	db := New()
	mustExec(t, db, `INSERT INTO users (id, name, age) VALUES (1,'alice',30),(2,'bob',20),(3,'carol',25)`)

	rows := mustQuery(t, db, `
		SELECT name FROM (SELECT id, name, age FROM users WHERE age >= 25) AS sub
		ORDER BY name`)

	if len(rows) != 2 {
		t.Fatalf("want 2 rows, got %d", len(rows))
	}
	if rows[0]["name"] != strVal("alice") || rows[1]["name"] != strVal("carol") {
		t.Errorf("unexpected: %v", rows)
	}
}

func TestDerivedTableStar(t *testing.T) {
	db := New()
	mustExec(t, db, `INSERT INTO t (id, val) VALUES (1,10),(2,20),(3,30)`)

	rows := mustQuery(t, db, `SELECT * FROM (SELECT id, val FROM t WHERE val > 10) AS sub ORDER BY id`)
	if len(rows) != 2 {
		t.Fatalf("want 2 rows, got %d", len(rows))
	}
	if rows[0]["id"] != intVal(2) || rows[1]["id"] != intVal(3) {
		t.Errorf("unexpected rows: %v", rows)
	}
}

// ── Outer WHERE on derived table ──────────────────────────────────────────────

func TestDerivedTableOuterWhere(t *testing.T) {
	db := New()
	mustExec(t, db, `INSERT INTO users (id, name, age) VALUES (1,'alice',30),(2,'bob',20),(3,'carol',25)`)

	rows := mustQuery(t, db, `
		SELECT name FROM (SELECT id, name, age FROM users) AS u
		WHERE age > 22
		ORDER BY age`)

	if len(rows) != 2 {
		t.Fatalf("want 2, got %d", len(rows))
	}
	if rows[0]["name"] != strVal("carol") || rows[1]["name"] != strVal("alice") {
		t.Errorf("unexpected: %v", rows)
	}
}

// ── Computed columns in derived table ────────────────────────────────────────

func TestDerivedTableComputedColumn(t *testing.T) {
	db := New()
	mustExec(t, db, `INSERT INTO orders (id, amount) VALUES (1,100),(2,200),(3,50)`)

	rows := mustQuery(t, db, `
		SELECT total FROM (SELECT id, amount * 2 AS total FROM orders) AS sub
		ORDER BY total`)

	if len(rows) != 3 {
		t.Fatalf("want 3 rows, got %d", len(rows))
	}
	if rows[0]["total"] != intVal(100) || rows[1]["total"] != intVal(200) || rows[2]["total"] != intVal(400) {
		t.Errorf("unexpected totals: %v", rows)
	}
}

// ── Aggregate in derived table ────────────────────────────────────────────────

func TestDerivedTableAggregate(t *testing.T) {
	db := New()
	mustExec(t, db, `INSERT INTO orders (user_id, amount) VALUES (1,100),(1,50),(2,200)`)

	rows := mustQuery(t, db, `
		SELECT user_id, total FROM
		  (SELECT user_id, SUM(amount) AS total FROM orders GROUP BY user_id) AS agg
		ORDER BY user_id`)

	if len(rows) != 2 {
		t.Fatalf("want 2 rows, got %d", len(rows))
	}
	if rows[0]["total"] != intVal(150) {
		t.Errorf("user 1 total: want 150, got %v", rows[0]["total"])
	}
	if rows[1]["total"] != intVal(200) {
		t.Errorf("user 2 total: want 200, got %v", rows[1]["total"])
	}
}

// ── Qualified column names (sub.col) ─────────────────────────────────────────

func TestDerivedTableQualifiedColumn(t *testing.T) {
	db := New()
	mustExec(t, db, `INSERT INTO users (id, name) VALUES (1,'alice'),(2,'bob')`)

	rows := mustQuery(t, db, `
		SELECT sub.name FROM (SELECT id, name FROM users) AS sub
		WHERE sub.id = 1`)

	if len(rows) != 1 || rows[0]["name"] != strVal("alice") {
		t.Errorf("want [alice], got %v", rows)
	}
}

// ── JOIN with a derived table ─────────────────────────────────────────────────

func TestDerivedTableJoin(t *testing.T) {
	db := New()
	mustExec(t, db, `INSERT INTO users (id, name) VALUES (1,'alice'),(2,'bob'),(3,'carol')`)
	mustExec(t, db, `INSERT INTO orders (user_id, amount) VALUES (1,100),(1,50),(3,200)`)

	rows := mustQuery(t, db, `
		SELECT u.name, agg.total
		FROM users AS u
		INNER JOIN (SELECT user_id, SUM(amount) AS total FROM orders GROUP BY user_id) AS agg
		  ON agg.user_id = u.id
		ORDER BY u.id`)

	if len(rows) != 2 {
		t.Fatalf("want 2 rows, got %d", len(rows))
	}
	if rows[0]["name"] != strVal("alice") || rows[0]["total"] != intVal(150) {
		t.Errorf("row 0: %v", rows[0])
	}
	if rows[1]["name"] != strVal("carol") || rows[1]["total"] != intVal(200) {
		t.Errorf("row 1: %v", rows[1])
	}
}

func TestDerivedTableLeftJoin(t *testing.T) {
	db := New()
	mustExec(t, db, `INSERT INTO users (id, name) VALUES (1,'alice'),(2,'bob')`)
	mustExec(t, db, `INSERT INTO orders (user_id, amount) VALUES (1,100)`)

	rows := mustQuery(t, db, `
		SELECT u.name, agg.total
		FROM users AS u
		LEFT JOIN (SELECT user_id, SUM(amount) AS total FROM orders GROUP BY user_id) AS agg
		  ON agg.user_id = u.id
		ORDER BY u.id`)

	if len(rows) != 2 {
		t.Fatalf("want 2 rows, got %d", len(rows))
	}
	if rows[1]["total"].Kind != KindNull {
		t.Errorf("bob should have NULL total, got %v", rows[1]["total"])
	}
}

// ── ORDER BY and LIMIT inside derived table ───────────────────────────────────

func TestDerivedTableOrderByLimit(t *testing.T) {
	db := New()
	mustExec(t, db, `INSERT INTO t (id, v) VALUES (1,30),(2,10),(3,20)`)

	rows := mustQuery(t, db, `
		SELECT id FROM (SELECT id, v FROM t ORDER BY v LIMIT 2) AS top`)

	if len(rows) != 2 {
		t.Fatalf("want 2 rows, got %d", len(rows))
	}
	// LIMIT 2 on sorted-by-v should give ids 2 and 3
	ids := map[any]bool{rows[0]["id"].V: true, rows[1]["id"].V: true}
	if !ids[int64(2)] || !ids[int64(3)] {
		t.Errorf("unexpected ids: %v", rows)
	}
}

// ── Nested derived tables ─────────────────────────────────────────────────────

func TestDerivedTableNested(t *testing.T) {
	db := New()
	mustExec(t, db, `INSERT INTO t (id, v) VALUES (1,1),(2,2),(3,3),(4,4),(5,5)`)

	rows := mustQuery(t, db, `
		SELECT id FROM
		  (SELECT id, v FROM
		    (SELECT id, v FROM t WHERE v > 1) AS inner_sub
		   WHERE v < 5) AS outer_sub
		ORDER BY id`)

	if len(rows) != 3 {
		t.Fatalf("want 3 rows (v in 2..4), got %d", len(rows))
	}
}
