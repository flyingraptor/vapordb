package vapordb

import "testing"

// ── parseCTEs unit tests ──────────────────────────────────────────────────────

func TestParseCTEsSingle(t *testing.T) {
	ctes, main, ok := parseCTEs("WITH active AS (SELECT id FROM users WHERE active = 1) SELECT * FROM active")
	if !ok || len(ctes) != 1 {
		t.Fatalf("want 1 CTE, got ok=%v ctes=%v", ok, ctes)
	}
	if ctes[0].name != "active" {
		t.Errorf("name: want 'active', got %q", ctes[0].name)
	}
	if main != "SELECT * FROM active" {
		t.Errorf("main: got %q", main)
	}
}

func TestParseCTEsMultiple(t *testing.T) {
	sql := "WITH a AS (SELECT 1), b AS (SELECT 2) SELECT * FROM a"
	ctes, _, ok := parseCTEs(sql)
	if !ok || len(ctes) != 2 {
		t.Fatalf("want 2 CTEs, got %v / %v", ok, ctes)
	}
	if ctes[0].name != "a" || ctes[1].name != "b" {
		t.Errorf("names: %v", ctes)
	}
}

func TestParseCTEsNestedParens(t *testing.T) {
	sql := "WITH sub AS (SELECT id FROM (SELECT id FROM t) AS inner) SELECT * FROM sub"
	ctes, _, ok := parseCTEs(sql)
	if !ok || len(ctes) != 1 {
		t.Fatalf("nested parens: want 1 CTE, got ok=%v", ok)
	}
}

func TestParseCTEsStringLiteralWithParen(t *testing.T) {
	sql := "WITH sub AS (SELECT id FROM t WHERE name = 'foo(bar)') SELECT * FROM sub"
	ctes, _, ok := parseCTEs(sql)
	if !ok || len(ctes) != 1 {
		t.Fatalf("string literal: want 1 CTE, got ok=%v", ok)
	}
}

func TestParseCTEsNoCTE(t *testing.T) {
	sql := "SELECT id FROM users"
	_, _, ok := parseCTEs(sql)
	if ok {
		t.Error("plain SELECT should not be detected as CTE")
	}
}

func TestParseCTEsCaseInsensitive(t *testing.T) {
	sql := "with active as (select id from users) select * from active"
	ctes, _, ok := parseCTEs(sql)
	if !ok || len(ctes) != 1 {
		t.Fatalf("lowercase with: want 1 CTE, got ok=%v", ok)
	}
}

// ── Integration tests ─────────────────────────────────────────────────────────

func TestCTEBasic(t *testing.T) {
	db := New()
	mustExec(t, db, `INSERT INTO users (id, name, active) VALUES (1,'alice',1),(2,'bob',0),(3,'carol',1)`)

	rows := mustQuery(t, db, `
		WITH active_users AS (
			SELECT id, name FROM users WHERE active = 1
		)
		SELECT name FROM active_users ORDER BY name`)

	if len(rows) != 2 {
		t.Fatalf("want 2 rows, got %d", len(rows))
	}
	if rows[0]["name"] != strVal("alice") || rows[1]["name"] != strVal("carol") {
		t.Errorf("unexpected: %v", rows)
	}
}

func TestCTEWithAggregate(t *testing.T) {
	db := New()
	mustExec(t, db, `INSERT INTO orders (user_id, amount) VALUES (1,100),(1,50),(2,200),(3,75)`)

	rows := mustQuery(t, db, `
		WITH totals AS (
			SELECT user_id, SUM(amount) AS total FROM orders GROUP BY user_id
		)
		SELECT user_id, total FROM totals WHERE total > 100 ORDER BY total`)

	if len(rows) != 2 {
		t.Fatalf("want 2, got %d", len(rows))
	}
	if rows[0]["total"] != intVal(150) || rows[1]["total"] != intVal(200) {
		t.Errorf("unexpected totals: %v", rows)
	}
}

func TestCTEMultiple(t *testing.T) {
	db := New()
	mustExec(t, db, `INSERT INTO users (id, name) VALUES (1,'alice'),(2,'bob'),(3,'carol')`)
	mustExec(t, db, `INSERT INTO orders (user_id, amount) VALUES (1,100),(1,50),(3,200)`)

	rows := mustQuery(t, db, `
		WITH
		  order_totals AS (
		    SELECT user_id, SUM(amount) AS total FROM orders GROUP BY user_id
		  ),
		  big_spenders AS (
		    SELECT user_id FROM order_totals WHERE total >= 150
		  )
		SELECT u.name
		FROM users u
		INNER JOIN big_spenders b ON b.user_id = u.id
		ORDER BY u.name`)

	if len(rows) != 2 {
		t.Fatalf("want 2, got %d", len(rows))
	}
	if rows[0]["name"] != strVal("alice") || rows[1]["name"] != strVal("carol") {
		t.Errorf("unexpected: %v", rows)
	}
}

func TestCTEReferencingEarlierCTE(t *testing.T) {
	db := New()
	mustExec(t, db, `INSERT INTO t (id, v) VALUES (1,10),(2,20),(3,30),(4,40),(5,50)`)

	rows := mustQuery(t, db, `
		WITH
		  filtered AS (SELECT id, v FROM t WHERE v > 15),
		  ranked   AS (SELECT id, v FROM filtered WHERE v < 45)
		SELECT id FROM ranked ORDER BY id`)

	if len(rows) != 3 {
		t.Fatalf("want 3 (v=20,30,40), got %d", len(rows))
	}
}

func TestCTEJoinRealTable(t *testing.T) {
	db := New()
	mustExec(t, db, `INSERT INTO users (id, name) VALUES (1,'alice'),(2,'bob')`)
	mustExec(t, db, `INSERT INTO orders (user_id, amount) VALUES (1,99),(2,1)`)

	rows := mustQuery(t, db, `
		WITH premium AS (
		  SELECT user_id FROM orders WHERE amount > 50
		)
		SELECT u.name
		FROM users u
		INNER JOIN premium p ON p.user_id = u.id`)

	if len(rows) != 1 || rows[0]["name"] != strVal("alice") {
		t.Errorf("want [alice], got %v", rows)
	}
}

func TestCTEWithExistsInMainQuery(t *testing.T) {
	db := New()
	mustExec(t, db, `INSERT INTO users (id, name) VALUES (1,'alice'),(2,'bob')`)
	mustExec(t, db, `INSERT INTO orders (user_id) VALUES (1)`)

	rows := mustQuery(t, db, `
		WITH all_users AS (SELECT id, name FROM users)
		SELECT name FROM all_users
		WHERE EXISTS (SELECT 1 FROM orders WHERE orders.user_id = all_users.id)`)

	if len(rows) != 1 || rows[0]["name"] != strVal("alice") {
		t.Errorf("want [alice], got %v", rows)
	}
}

func TestCTEWithUnion(t *testing.T) {
	db := New()
	mustExec(t, db, `INSERT INTO a (id) VALUES (1),(2)`)
	mustExec(t, db, `INSERT INTO b (id) VALUES (2),(3)`)

	rows := mustQuery(t, db, `
		WITH combined AS (
		  SELECT id FROM a
		  UNION
		  SELECT id FROM b
		)
		SELECT id FROM combined ORDER BY id`)

	if len(rows) != 3 {
		t.Fatalf("want 3 distinct, got %d", len(rows))
	}
}

func TestCTECaseInsensitiveKeywords(t *testing.T) {
	db := New()
	mustExec(t, db, `INSERT INTO users (id, name) VALUES (1,'alice')`)

	rows := mustQuery(t, db, `with u as (select id, name from users) select name from u`)
	if len(rows) != 1 || rows[0]["name"] != strVal("alice") {
		t.Errorf("want [alice], got %v", rows)
	}
}
