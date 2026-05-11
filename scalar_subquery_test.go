package vapordb

import "testing"

// ── helpers ──────────────────────────────────────────────────────────────────

func setupScalarDB(t *testing.T) *DB {
	t.Helper()
	db := New()
	mustExec(t, db, `INSERT INTO users   (id, name, dept_id) VALUES (1,'Alice',10),(2,'Bob',20),(3,'Carol',10),(4,'Dave',20)`)
	mustExec(t, db, `INSERT INTO orders  (id, user_id, amount) VALUES (1,1,100),(2,1,200),(3,2,50),(4,3,300)`)
	mustExec(t, db, `INSERT INTO depts   (id, name) VALUES (10,'Engineering'),(20,'Marketing')`)
	return db
}

// ── uncorrelated scalar subqueries ───────────────────────────────────────────

func TestScalarSubqueryUncorrelatedAgg(t *testing.T) {
	db := setupScalarDB(t)
	// SELECT MAX id from users = 4
	rows := mustQuery(t, db, `SELECT (SELECT MAX(id) FROM users) AS max_id`)
	if len(rows) != 1 {
		t.Fatalf("want 1 row, got %d", len(rows))
	}
	if rows[0]["max_id"].V != int64(4) {
		t.Fatalf("want 4, got %v", rows[0]["max_id"])
	}
}

func TestScalarSubqueryCountInProjection(t *testing.T) {
	db := setupScalarDB(t)
	rows := mustQuery(t, db, `
		SELECT name,
		       (SELECT COUNT(*) FROM orders WHERE orders.user_id = users.id) AS order_count
		FROM users ORDER BY id`)
	if len(rows) != 4 {
		t.Fatalf("want 4 rows, got %d", len(rows))
	}
	// Alice has 2 orders, Bob 1, Carol 1, Dave 0
	counts := []int64{2, 1, 1, 0}
	names := []string{"Alice", "Bob", "Carol", "Dave"}
	for i, row := range rows {
		if row["name"].V != names[i] {
			t.Errorf("row %d: want name %q, got %v", i, names[i], row["name"])
		}
		if row["order_count"].V != counts[i] {
			t.Errorf("row %d: want order_count %d, got %v", i, counts[i], row["order_count"])
		}
	}
}

func TestScalarSubqueryInWhere(t *testing.T) {
	db := setupScalarDB(t)
	// Users whose total order amount is above the overall average
	rows := mustQuery(t, db, `
		SELECT name FROM users
		WHERE (SELECT SUM(amount) FROM orders WHERE orders.user_id = users.id)
		    > (SELECT AVG(amount) FROM orders)
		ORDER BY name`)
	// Alice total=300, Carol total=300; avg=162.5 → both qualify
	// Bob total=50, Dave total=NULL (no orders) → don't qualify
	if len(rows) != 2 {
		t.Fatalf("want 2 rows, got %d: %v", len(rows), rows)
	}
	if rows[0]["name"].V != "Alice" || rows[1]["name"].V != "Carol" {
		t.Fatalf("unexpected names: %v", rows)
	}
}

func TestScalarSubqueryNullForNoRows(t *testing.T) {
	db := setupScalarDB(t)
	// Dave has no orders → SUM subquery returns NULL
	rows := mustQuery(t, db, `
		SELECT name,
		       (SELECT SUM(amount) FROM orders WHERE orders.user_id = users.id) AS total
		FROM users WHERE id = 4`)
	if len(rows) != 1 {
		t.Fatalf("want 1 row, got %d", len(rows))
	}
	if rows[0]["total"].Kind != KindNull {
		t.Fatalf("want NULL, got %v", rows[0]["total"])
	}
}

func TestScalarSubqueryComparisonOperand(t *testing.T) {
	db := setupScalarDB(t)
	// = (SELECT …) on RHS of a comparison
	rows := mustQuery(t, db, `SELECT name FROM users WHERE id = (SELECT MAX(id) FROM users)`)
	if len(rows) != 1 || rows[0]["name"].V != "Dave" {
		t.Fatalf("want Dave, got %v", rows)
	}
}

func TestScalarSubqueryWithOrderByLimit(t *testing.T) {
	db := setupScalarDB(t)
	// Cheapest order amount for Alice
	rows := mustQuery(t, db, `
		SELECT (SELECT amount FROM orders WHERE user_id = 1 ORDER BY amount ASC LIMIT 1) AS cheapest`)
	if len(rows) != 1 || rows[0]["cheapest"].V != int64(100) {
		t.Fatalf("want 100, got %v", rows)
	}
}

func TestScalarSubqueryDeptName(t *testing.T) {
	db := setupScalarDB(t)
	// Look up department name for each user (correlated scalar)
	rows := mustQuery(t, db, `
		SELECT name, (SELECT depts.name FROM depts WHERE depts.id = users.dept_id) AS dept
		FROM users ORDER BY id`)
	if len(rows) != 4 {
		t.Fatalf("want 4 rows, got %d", len(rows))
	}
	if rows[0]["dept"].V != "Engineering" {
		t.Errorf("Alice: want Engineering, got %v", rows[0]["dept"])
	}
	if rows[1]["dept"].V != "Marketing" {
		t.Errorf("Bob: want Marketing, got %v", rows[1]["dept"])
	}
}

// ── error cases ───────────────────────────────────────────────────────────────

func TestScalarSubqueryTooManyRows(t *testing.T) {
	db := setupScalarDB(t)
	_, err := db.Query(`SELECT (SELECT id FROM users) AS x`)
	if err == nil {
		t.Fatal("expected error for subquery returning multiple rows, got nil")
	}
}

func TestScalarSubqueryMultiColumnError(t *testing.T) {
	db := setupScalarDB(t)
	_, err := db.Query(`SELECT (SELECT id, name FROM users WHERE id = 1) AS x`)
	if err == nil {
		t.Fatal("expected error for multi-column scalar subquery, got nil")
	}
}
