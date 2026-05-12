package vapordb

// Tests for IN (subquery) with GROUP BY, HAVING, ORDER BY, LIMIT, DISTINCT,
// and UNION inside the subquery.
//
// Before this fix, all of these returned an "unsupported" error.

import (
	"fmt"
	"testing"
)

// ── helpers ──────────────────────────────────────────────────────────────────

func seedInSubqueryDB(t *testing.T) *DB {
	t.Helper()
	db := New()

	// departments
	for _, row := range []struct {
		id   int
		name string
	}{
		{1, "eng"}, {2, "sales"}, {3, "hr"},
	} {
		mustExec(t, db, fmt.Sprintf(`INSERT INTO depts (id, name) VALUES (%d, '%s')`, row.id, row.name))
	}

	// employees (dept_id → depts.id, salary)
	for _, row := range []struct {
		id, deptID, salary int
		name               string
	}{
		{1, 1, 90000, "Alice"},
		{2, 1, 80000, "Bob"},
		{3, 2, 70000, "Carol"},
		{4, 2, 75000, "Dave"},
		{5, 3, 60000, "Eve"},
	} {
		mustExec(t, db, fmt.Sprintf(
			`INSERT INTO emps (id, dept_id, salary, name) VALUES (%d, %d, %d, '%s')`,
			row.id, row.deptID, row.salary, row.name))
	}

	return db
}

// ── GROUP BY inside IN (subquery) ────────────────────────────────────────────

func TestInSubquery_GroupBy(t *testing.T) {
	db := seedInSubqueryDB(t)

	// Departments that have more than one employee.
	rows := mustQuery(t, db, `
		SELECT name FROM depts
		WHERE id IN (
			SELECT dept_id FROM emps GROUP BY dept_id HAVING COUNT(*) > 1
		)
		ORDER BY name`)
	if len(rows) != 2 {
		t.Fatalf("want 2 depts, got %d: %v", len(rows), rows)
	}
	if rows[0]["name"].V != "eng" || rows[1]["name"].V != "sales" {
		t.Errorf("unexpected names: %v %v", rows[0]["name"].V, rows[1]["name"].V)
	}
}

func TestInSubquery_GroupByAvg(t *testing.T) {
	db := seedInSubqueryDB(t)

	// Departments whose average salary exceeds 70 000.
	rows := mustQuery(t, db, `
		SELECT name FROM depts
		WHERE id IN (
			SELECT dept_id FROM emps GROUP BY dept_id HAVING AVG(salary) > 70000
		)
		ORDER BY name`)
	// eng avg = 85000 ✓, sales avg = 72500 ✓, hr avg = 60000 ✗
	if len(rows) != 2 {
		t.Fatalf("want 2, got %d: %v", len(rows), rows)
	}
}

func TestInSubquery_GroupBySum(t *testing.T) {
	db := seedInSubqueryDB(t)

	// Departments whose total salary >= 150 000.
	rows := mustQuery(t, db, `
		SELECT name FROM depts
		WHERE id IN (
			SELECT dept_id FROM emps GROUP BY dept_id HAVING SUM(salary) >= 150000
		)
		ORDER BY name`)
	// eng sum = 170000 ✓, sales sum = 145000 ✗, hr sum = 60000 ✗
	if len(rows) != 1 || rows[0]["name"].V != "eng" {
		t.Errorf("want [eng], got %v", rows)
	}
}

// ── HAVING without GROUP BY (scalar aggregate) ────────────────────────────────

func TestInSubquery_HavingAgg(t *testing.T) {
	db := New()
	mustExec(t, db, `INSERT INTO vals (n) VALUES (10)`)
	mustExec(t, db, `INSERT INTO vals (n) VALUES (20)`)
	mustExec(t, db, `INSERT INTO vals (n) VALUES (30)`)
	mustExec(t, db, `INSERT INTO ref (v) VALUES (55)`)

	// v is IN (MAX of vals) iff MAX = 30; since 55 != 30 → NOT IN.
	rows := mustQuery(t, db, `
		SELECT v FROM ref
		WHERE v NOT IN (SELECT MAX(n) FROM vals)`)
	if len(rows) != 1 {
		t.Fatalf("want 1 row, got %d", len(rows))
	}
}

// ── ORDER BY + LIMIT inside IN (subquery) ────────────────────────────────────

func TestInSubquery_OrderByLimit(t *testing.T) {
	db := seedInSubqueryDB(t)

	// Top-2 earners by salary — get their dept_id.
	// Alice (90000, dept 1) and Bob (80000, dept 1) → only eng should appear.
	rows := mustQuery(t, db, `
		SELECT name FROM depts
		WHERE id IN (
			SELECT dept_id FROM emps ORDER BY salary DESC LIMIT 2
		)
		ORDER BY name`)
	if len(rows) != 1 || rows[0]["name"].V != "eng" {
		t.Errorf("want [eng], got %v", rows)
	}
}

func TestInSubquery_LimitOne(t *testing.T) {
	db := seedInSubqueryDB(t)

	// The cheapest employee's dept must NOT be eng (Eve, hr, 60000).
	rows := mustQuery(t, db, `
		SELECT name FROM depts
		WHERE id IN (SELECT dept_id FROM emps ORDER BY salary ASC LIMIT 1)`)
	if len(rows) != 1 || rows[0]["name"].V != "hr" {
		t.Errorf("want [hr], got %v", rows)
	}
}

// ── DISTINCT inside IN (subquery) ────────────────────────────────────────────

func TestInSubquery_Distinct(t *testing.T) {
	db := New()
	// Duplicate dept_id values — DISTINCT must deduplicate for the caller.
	for i := 0; i < 3; i++ {
		mustExec(t, db, `INSERT INTO emps2 (dept_id) VALUES (1)`)
	}
	mustExec(t, db, `INSERT INTO emps2 (dept_id) VALUES (2)`)
	mustExec(t, db, `INSERT INTO depts2 (id, name) VALUES (1, 'eng')`)
	mustExec(t, db, `INSERT INTO depts2 (id, name) VALUES (2, 'sales')`)
	mustExec(t, db, `INSERT INTO depts2 (id, name) VALUES (3, 'hr')`)

	rows := mustQuery(t, db, `
		SELECT name FROM depts2
		WHERE id IN (SELECT DISTINCT dept_id FROM emps2)
		ORDER BY name`)
	if len(rows) != 2 {
		t.Fatalf("want 2, got %d: %v", len(rows), rows)
	}
	if rows[0]["name"].V != "eng" || rows[1]["name"].V != "sales" {
		t.Errorf("unexpected: %v", rows)
	}
}

// ── UNION inside IN (subquery) ────────────────────────────────────────────────

func TestInSubquery_Union(t *testing.T) {
	db := New()
	mustExec(t, db, `INSERT INTO allowed_a (id) VALUES (1)`)
	mustExec(t, db, `INSERT INTO allowed_a (id) VALUES (2)`)
	mustExec(t, db, `INSERT INTO allowed_b (id) VALUES (3)`)
	mustExec(t, db, `INSERT INTO items (id, label) VALUES (1, 'one')`)
	mustExec(t, db, `INSERT INTO items (id, label) VALUES (2, 'two')`)
	mustExec(t, db, `INSERT INTO items (id, label) VALUES (3, 'three')`)
	mustExec(t, db, `INSERT INTO items (id, label) VALUES (4, 'four')`)

	rows := mustQuery(t, db, `
		SELECT label FROM items
		WHERE id IN (
			SELECT id FROM allowed_a
			UNION
			SELECT id FROM allowed_b
		)
		ORDER BY id`)
	if len(rows) != 3 {
		t.Fatalf("want 3, got %d: %v", len(rows), rows)
	}
	labels := []string{rows[0]["label"].V.(string), rows[1]["label"].V.(string), rows[2]["label"].V.(string)}
	for i, want := range []string{"one", "two", "three"} {
		if labels[i] != want {
			t.Errorf("[%d] want %s, got %s", i, want, labels[i])
		}
	}
}

func TestInSubquery_UnionAll(t *testing.T) {
	db := New()
	mustExec(t, db, `INSERT INTO src_a (v) VALUES (10)`)
	mustExec(t, db, `INSERT INTO src_a (v) VALUES (10)`) // duplicate
	mustExec(t, db, `INSERT INTO src_b (v) VALUES (20)`)
	mustExec(t, db, `INSERT INTO nums (n) VALUES (10)`)
	mustExec(t, db, `INSERT INTO nums (n) VALUES (20)`)
	mustExec(t, db, `INSERT INTO nums (n) VALUES (30)`)

	// UNION ALL keeps duplicates but IN still just checks membership.
	rows := mustQuery(t, db, `
		SELECT n FROM nums
		WHERE n IN (SELECT v FROM src_a UNION ALL SELECT v FROM src_b)
		ORDER BY n`)
	if len(rows) != 2 {
		t.Fatalf("want 2, got %d: %v", len(rows), rows)
	}
}

func TestInSubquery_UnionWithGroupBy(t *testing.T) {
	db := seedInSubqueryDB(t)

	// depts whose id appears in both:
	// – depts with > 1 emp (GROUP BY path)
	// – depts with avg salary > 80000 (GROUP BY HAVING path)
	// Expected: only eng (id=1)
	rows := mustQuery(t, db, `
		SELECT name FROM depts
		WHERE id IN (
			SELECT dept_id FROM emps GROUP BY dept_id HAVING COUNT(*) > 1
			UNION
			SELECT dept_id FROM emps GROUP BY dept_id HAVING AVG(salary) > 80000
		)
		ORDER BY name`)
	// UNION deduplicates: eng appears in both branches, sales only in first.
	// Result: eng AND sales (appear in at least one branch).
	if len(rows) != 2 {
		t.Fatalf("want 2, got %d: %v", len(rows), rows)
	}
}

// ── NOT IN with GROUP BY ──────────────────────────────────────────────────────

func TestNotInSubquery_GroupBy(t *testing.T) {
	db := seedInSubqueryDB(t)

	// Departments that have only one employee (or fewer).
	rows := mustQuery(t, db, `
		SELECT name FROM depts
		WHERE id NOT IN (
			SELECT dept_id FROM emps GROUP BY dept_id HAVING COUNT(*) > 1
		)
		ORDER BY name`)
	// Only hr has exactly 1 employee.
	if len(rows) != 1 || rows[0]["name"].V != "hr" {
		t.Errorf("want [hr], got %v", rows)
	}
}

// ── Correlated IN subquery with GROUP BY ─────────────────────────────────────

func TestInSubquery_CorrelatedGroupBy(t *testing.T) {
	db := New()
	// orders: id, customer_id, amount
	for _, r := range []struct{ id, cid, amount int }{
		{1, 1, 100}, {2, 1, 200}, {3, 2, 150}, {4, 3, 50},
	} {
		mustExec(t, db, fmt.Sprintf(
			`INSERT INTO orders (id, customer_id, amount) VALUES (%d, %d, %d)`,
			r.id, r.cid, r.amount))
	}
	// customers: id, name
	mustExec(t, db, `INSERT INTO customers (id, name) VALUES (1, 'Alice')`)
	mustExec(t, db, `INSERT INTO customers (id, name) VALUES (2, 'Bob')`)
	mustExec(t, db, `INSERT INTO customers (id, name) VALUES (3, 'Carol')`)

	// Customers who have placed at least 2 orders.
	rows := mustQuery(t, db, `
		SELECT name FROM customers
		WHERE id IN (
			SELECT customer_id FROM orders
			GROUP BY customer_id
			HAVING COUNT(*) >= 2
		)`)
	if len(rows) != 1 || rows[0]["name"].V != "Alice" {
		t.Errorf("want [Alice], got %v", rows)
	}
}

// ── IN subquery with ORDER BY + LIMIT + OFFSET ───────────────────────────────

func TestInSubquery_LimitOffset(t *testing.T) {
	db := seedInSubqueryDB(t)

	// Skip the top earner (Alice 90000) with OFFSET 1, take next 2 → Bob(80k,eng) Dave(75k,sales).
	rows := mustQuery(t, db, `
		SELECT name FROM depts
		WHERE id IN (
			SELECT dept_id FROM emps ORDER BY salary DESC LIMIT 2 OFFSET 1
		)
		ORDER BY name`)
	// Bob → dept 1 (eng), Dave → dept 2 (sales)
	if len(rows) != 2 {
		t.Fatalf("want 2, got %d: %v", len(rows), rows)
	}
	names := rows[0]["name"].V.(string) + "," + rows[1]["name"].V.(string)
	if names != "eng,sales" {
		t.Errorf("want eng,sales got %s", names)
	}
}

// ── Aggregate-only subquery (no GROUP BY) ────────────────────────────────────

func TestInSubquery_AggregateNoGroupBy(t *testing.T) {
	db := New()
	mustExec(t, db, `INSERT INTO scores (val) VALUES (10)`)
	mustExec(t, db, `INSERT INTO scores (val) VALUES (20)`)
	mustExec(t, db, `INSERT INTO scores (val) VALUES (30)`)
	mustExec(t, db, `INSERT INTO candidates (v) VALUES (30)`)
	mustExec(t, db, `INSERT INTO candidates (v) VALUES (15)`)

	// Only the candidate equal to MAX is in the set.
	rows := mustQuery(t, db, `
		SELECT v FROM candidates
		WHERE v IN (SELECT MAX(val) FROM scores)
		ORDER BY v`)
	if len(rows) != 1 || rows[0]["v"].V != int64(30) {
		t.Errorf("want [30], got %v", rows)
	}
}
