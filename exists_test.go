package vapordb

import "testing"

// ── SELECT EXISTS (…) at the top level ───────────────────────────────────────

func TestExistsTopLevelTrue(t *testing.T) {
	db := New()
	mustExec(t, db, `INSERT INTO users (id, name) VALUES (1, 'alice')`)

	rows := mustQuery(t, db, `SELECT EXISTS (SELECT 1 FROM users WHERE id = 1)`)
	if len(rows) != 1 {
		t.Fatalf("want 1 row, got %d", len(rows))
	}
	// The single column holds a bool.
	for _, v := range rows[0] {
		if v.Kind != KindBool || v.V.(bool) != true {
			t.Errorf("want true, got %v", v)
		}
	}
}

func TestExistsTopLevelFalse(t *testing.T) {
	db := New()
	mustExec(t, db, `INSERT INTO users (id) VALUES (1)`)

	rows := mustQuery(t, db, `SELECT EXISTS (SELECT 1 FROM users WHERE id = 99)`)
	for _, v := range rows[0] {
		if v.Kind != KindBool || v.V.(bool) != false {
			t.Errorf("want false, got %v", v)
		}
	}
}

func TestExistsEmptyTable(t *testing.T) {
	db := New()
	// table doesn't exist → empty → false
	rows := mustQuery(t, db, `SELECT EXISTS (SELECT 1 FROM missing WHERE id = 1)`)
	for _, v := range rows[0] {
		if v.Kind != KindBool || v.V.(bool) != false {
			t.Errorf("want false for empty table, got %v", v)
		}
	}
}

// ── EXISTS as a projected column ─────────────────────────────────────────────

func TestExistsAsColumn(t *testing.T) {
	db := New()
	mustExec(t, db, `INSERT INTO users (id, name) VALUES (1, 'alice'), (2, 'bob')`)
	mustExec(t, db, `INSERT INTO orders (user_id, product) VALUES (1, 'widget')`)

	rows := mustQuery(t, db,
		`SELECT id, EXISTS (SELECT 1 FROM orders WHERE orders.user_id = users.id) AS has_orders
		 FROM users ORDER BY id`)

	if len(rows) != 2 {
		t.Fatalf("want 2 rows, got %d", len(rows))
	}
	if rows[0]["has_orders"].V.(bool) != true {
		t.Errorf("user 1 should have orders")
	}
	if rows[1]["has_orders"].V.(bool) != false {
		t.Errorf("user 2 should have no orders")
	}
}

// ── EXISTS in WHERE ───────────────────────────────────────────────────────────

func TestExistsInWhere(t *testing.T) {
	db := New()
	mustExec(t, db, `INSERT INTO users (id, name) VALUES (1, 'alice'), (2, 'bob'), (3, 'carol')`)
	mustExec(t, db, `INSERT INTO orders (user_id) VALUES (1), (3)`)

	rows := mustQuery(t, db,
		`SELECT name FROM users
		 WHERE EXISTS (SELECT 1 FROM orders WHERE orders.user_id = users.id)
		 ORDER BY id`)

	if len(rows) != 2 {
		t.Fatalf("want 2 rows, got %d", len(rows))
	}
	if rows[0]["name"] != strVal("alice") || rows[1]["name"] != strVal("carol") {
		t.Errorf("unexpected names: %v", rows)
	}
}

func TestNotExistsInWhere(t *testing.T) {
	db := New()
	mustExec(t, db, `INSERT INTO users (id, name) VALUES (1, 'alice'), (2, 'bob'), (3, 'carol')`)
	mustExec(t, db, `INSERT INTO orders (user_id) VALUES (1), (3)`)

	rows := mustQuery(t, db,
		`SELECT name FROM users
		 WHERE NOT EXISTS (SELECT 1 FROM orders WHERE orders.user_id = users.id)
		 ORDER BY id`)

	if len(rows) != 1 {
		t.Fatalf("want 1 row, got %d", len(rows))
	}
	if rows[0]["name"] != strVal("bob") {
		t.Errorf("want 'bob', got %v", rows[0]["name"])
	}
}

func TestExistsInWhereWithAnd(t *testing.T) {
	db := New()
	mustExec(t, db, `INSERT INTO users (id, name, active) VALUES (1, 'alice', 1), (2, 'bob', 0), (3, 'carol', 1)`)
	mustExec(t, db, `INSERT INTO orders (user_id) VALUES (1), (2), (3)`)

	// Only active users that have orders.
	rows := mustQuery(t, db,
		`SELECT name FROM users
		 WHERE active = 1
		   AND EXISTS (SELECT 1 FROM orders WHERE orders.user_id = users.id)
		 ORDER BY id`)

	if len(rows) != 2 {
		t.Fatalf("want 2 rows, got %d", len(rows))
	}
}

// ── correlated EXISTS with additional filter in subquery ─────────────────────

func TestExistsCorrelatedSubFilter(t *testing.T) {
	db := New()
	mustExec(t, db, `INSERT INTO users (id, name) VALUES (1, 'alice'), (2, 'bob')`)
	mustExec(t, db, `INSERT INTO orders (user_id, status) VALUES (1, 'open'), (2, 'closed')`)

	// Only users with an open order.
	rows := mustQuery(t, db,
		`SELECT name FROM users
		 WHERE EXISTS (SELECT 1 FROM orders WHERE orders.user_id = users.id AND orders.status = 'open')`)

	if len(rows) != 1 || rows[0]["name"] != strVal("alice") {
		t.Errorf("want [alice], got %v", rows)
	}
}

// ── uncorrelated EXISTS ───────────────────────────────────────────────────────

func TestExistsUncorrelated(t *testing.T) {
	db := New()
	mustExec(t, db, `INSERT INTO users (id) VALUES (1), (2)`)
	mustExec(t, db, `INSERT INTO flags (enabled) VALUES (1)`)

	// All users returned because the uncorrelated EXISTS is always true.
	rows := mustQuery(t, db,
		`SELECT id FROM users WHERE EXISTS (SELECT 1 FROM flags WHERE enabled = 1) ORDER BY id`)
	if len(rows) != 2 {
		t.Fatalf("want 2, got %d", len(rows))
	}
}
