package vapordb

import "testing"

// ── helpers ──────────────────────────────────────────────────────────────────

func setupInSubqueryDB(t *testing.T) *DB {
	t.Helper()
	db := New()
	mustExec(t, db, `INSERT INTO regions (id, name) VALUES (1, 'North'), (2, 'South'), (3, 'East')`)
	mustExec(t, db, `INSERT INTO users (id, name, region_id) VALUES (1, 'Alice', 1), (2, 'Bob', 2), (3, 'Carol', 1), (4, 'Dave', 3)`)
	return db
}

// ── uncorrelated IN (subquery) ────────────────────────────────────────────────

func TestInSubqueryUncorrelated(t *testing.T) {
	db := setupInSubqueryDB(t)
	rows := mustQuery(t, db, `
		SELECT name FROM users
		WHERE region_id IN (SELECT id FROM regions WHERE name LIKE 'North%')
		ORDER BY name`)
	if len(rows) != 2 {
		t.Fatalf("want 2 rows, got %d", len(rows))
	}
	if rows[0]["name"].V != "Alice" || rows[1]["name"].V != "Carol" {
		t.Fatalf("unexpected rows: %v", rows)
	}
}

func TestInSubqueryUncorrelatedNotIn(t *testing.T) {
	db := setupInSubqueryDB(t)
	rows := mustQuery(t, db, `
		SELECT name FROM users
		WHERE region_id NOT IN (SELECT id FROM regions WHERE name LIKE 'North%')
		ORDER BY name`)
	if len(rows) != 2 {
		t.Fatalf("want 2 rows, got %d; rows=%v", len(rows), rows)
	}
	if rows[0]["name"].V != "Bob" || rows[1]["name"].V != "Dave" {
		t.Fatalf("unexpected rows: %v", rows)
	}
}

func TestInSubqueryEmptySubquery(t *testing.T) {
	db := setupInSubqueryDB(t)
	// subquery returns zero rows → IN is always false
	rows := mustQuery(t, db, `
		SELECT name FROM users
		WHERE region_id IN (SELECT id FROM regions WHERE name = 'Nowhere')`)
	if len(rows) != 0 {
		t.Fatalf("want 0 rows, got %d", len(rows))
	}
}

func TestNotInSubqueryEmptySubquery(t *testing.T) {
	db := setupInSubqueryDB(t)
	// NOT IN empty set → all rows
	rows := mustQuery(t, db, `
		SELECT name FROM users
		WHERE region_id NOT IN (SELECT id FROM regions WHERE name = 'Nowhere')
		ORDER BY id`)
	if len(rows) != 4 {
		t.Fatalf("want 4 rows, got %d", len(rows))
	}
}

// ── correlated IN (subquery) ──────────────────────────────────────────────────

func TestInSubqueryCorrelated(t *testing.T) {
	db := New()
	mustExec(t, db, `INSERT INTO orders (id, user_id, status) VALUES (1, 1, 'open'), (2, 2, 'closed'), (3, 3, 'open')`)
	mustExec(t, db, `INSERT INTO users (id, name) VALUES (1, 'Alice'), (2, 'Bob'), (3, 'Carol'), (4, 'Dave')`)

	rows := mustQuery(t, db, `
		SELECT name FROM users
		WHERE id IN (SELECT user_id FROM orders WHERE orders.status = 'open')
		ORDER BY name`)
	if len(rows) != 2 {
		t.Fatalf("want 2 rows, got %d: %v", len(rows), rows)
	}
	if rows[0]["name"].V != "Alice" || rows[1]["name"].V != "Carol" {
		t.Fatalf("unexpected rows: %v", rows)
	}
}

// ── IN (subquery) as a projected column ───────────────────────────────────────

func TestInSubqueryAsProjectedColumn(t *testing.T) {
	db := setupInSubqueryDB(t)
	rows := mustQuery(t, db, `
		SELECT name,
		       region_id IN (SELECT id FROM regions WHERE name LIKE 'North%') AS in_north
		FROM users ORDER BY id`)
	if len(rows) != 4 {
		t.Fatalf("want 4 rows, got %d", len(rows))
	}
	// Alice (region 1 = North) → true
	if rows[0]["in_north"].V != true {
		t.Errorf("Alice: want in_north=true, got %v", rows[0]["in_north"])
	}
	// Bob (region 2 = South) → false
	if rows[1]["in_north"].V != false {
		t.Errorf("Bob: want in_north=false, got %v", rows[1]["in_north"])
	}
}

// ── error cases ───────────────────────────────────────────────────────────────

func TestInSubqueryMultiColumnError(t *testing.T) {
	db := setupInSubqueryDB(t)
	_, err := db.Query(`SELECT name FROM users WHERE id IN (SELECT id, name FROM users)`)
	if err == nil {
		t.Fatal("expected error for multi-column subquery, got nil")
	}
}
