package vapordb

import (
	"testing"
)

// ── fixture ───────────────────────────────────────────────────────────────────

// outerDB seeds two small tables:
//
//	employees: id=1 alice dept_id=10, id=2 bob dept_id=20, id=3 carol dept_id=99 (no dept)
//	departments: id=10 engineering, id=20 marketing, id=30 finance (no employee)
func outerDB(t *testing.T) *DB {
	t.Helper()
	db := New()
	for _, q := range []string{
		`INSERT INTO employees (id, name, dept_id) VALUES (1, 'alice', 10)`,
		`INSERT INTO employees (id, name, dept_id) VALUES (2, 'bob',   20)`,
		`INSERT INTO employees (id, name, dept_id) VALUES (3, 'carol', 99)`,
		`INSERT INTO departments (id, dname) VALUES (10, 'engineering')`,
		`INSERT INTO departments (id, dname) VALUES (20, 'marketing')`,
		`INSERT INTO departments (id, dname) VALUES (30, 'finance')`,
	} {
		mustExec(t, db, q)
	}
	return db
}

// ── FULL OUTER JOIN ───────────────────────────────────────────────────────────

// Rows that match on both sides appear exactly once with both sides populated.
func TestFullOuterJoinMatchedRows(t *testing.T) {
	db := outerDB(t)
	rows, err := db.Query(`
		SELECT e.name, d.dname
		FROM employees e
		FULL OUTER JOIN departments d ON e.dept_id = d.id`)
	if err != nil {
		t.Fatal(err)
	}
	// Columns after projection of "e.name" and "d.dname" are "name" and "dname".
	found := map[string]string{}
	for _, row := range rows {
		if row["name"].Kind != KindNull && row["dname"].Kind != KindNull {
			found[row["name"].V.(string)] = row["dname"].V.(string)
		}
	}
	if found["alice"] != "engineering" {
		t.Fatalf("alice→engineering not found in matched rows; found=%v", found)
	}
	if found["bob"] != "marketing" {
		t.Fatalf("bob→marketing not found in matched rows; found=%v", found)
	}
}

// Unmatched left rows appear with NULL right columns.
func TestFullOuterJoinUnmatchedLeft(t *testing.T) {
	db := outerDB(t)
	rows, err := db.Query(`
		SELECT e.name, d.dname
		FROM employees e
		FULL OUTER JOIN departments d ON e.dept_id = d.id`)
	if err != nil {
		t.Fatal(err)
	}
	// carol (dept_id=99) has no matching department → dname should be NULL.
	carolFound := false
	for _, row := range rows {
		if row["name"].Kind == KindNull {
			continue
		}
		if row["name"].V.(string) == "carol" {
			carolFound = true
			if row["dname"].Kind != KindNull {
				t.Fatalf("carol: expected dname=NULL, got %v", row["dname"])
			}
		}
	}
	if !carolFound {
		t.Fatal("carol not found in result")
	}
}

// Unmatched right rows appear with NULL left columns.
func TestFullOuterJoinUnmatchedRight(t *testing.T) {
	db := outerDB(t)
	rows, err := db.Query(`
		SELECT e.name, d.dname
		FROM employees e
		FULL OUTER JOIN departments d ON e.dept_id = d.id`)
	if err != nil {
		t.Fatal(err)
	}
	// Finance (id=30) has no employee → name should be NULL.
	financeFound := false
	for _, row := range rows {
		if row["dname"].Kind == KindNull {
			continue
		}
		if row["dname"].V.(string) == "finance" {
			financeFound = true
			if row["name"].Kind != KindNull {
				t.Fatalf("finance dept: expected name=NULL, got %v", row["name"])
			}
		}
	}
	if !financeFound {
		t.Fatal("finance department not found in result")
	}
}

// Total row count: 2 matched + 1 unmatched-left (carol) + 1 unmatched-right (finance) = 4.
func TestFullOuterJoinRowCount(t *testing.T) {
	db := outerDB(t)
	rows, err := db.Query(`
		SELECT e.name, d.dname
		FROM employees e
		FULL OUTER JOIN departments d ON e.dept_id = d.id`)
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 4 {
		t.Fatalf("expected 4 rows (2 matched + 1 unmatched-left + 1 unmatched-right), got %d: %v", len(rows), rows)
	}
}

// FULL JOIN (without OUTER keyword) must work identically.
func TestFullJoinWithoutOuterKeyword(t *testing.T) {
	db := outerDB(t)
	rows, err := db.Query(`
		SELECT e.name, d.dname
		FROM employees e
		FULL JOIN departments d ON e.dept_id = d.id`)
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 4 {
		t.Fatalf("expected 4 rows, got %d", len(rows))
	}
}

// When the left table is empty, all right rows appear with NULL left columns.
func TestFullOuterJoinEmptyLeft(t *testing.T) {
	db := New()
	mustExec(t, db, `INSERT INTO foj_right (id, v) VALUES (1, 'x')`)
	mustExec(t, db, `INSERT INTO foj_right (id, v) VALUES (2, 'y')`)
	// foj_left has no rows.
	rows, err := db.Query(`
		SELECT l.id AS lid, r.v
		FROM foj_left l
		FULL OUTER JOIN foj_right r ON l.id = r.id`)
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 2 {
		t.Fatalf("expected 2 rows from right side, got %d", len(rows))
	}
	for _, row := range rows {
		if row["lid"].Kind != KindNull {
			t.Fatalf("expected lid=NULL for empty left, got %v", row["lid"])
		}
	}
}

// When the right table is empty, all left rows appear with NULL right columns.
func TestFullOuterJoinEmptyRight(t *testing.T) {
	db := New()
	mustExec(t, db, `INSERT INTO foj_left2 (id, v) VALUES (1, 'a')`)
	mustExec(t, db, `INSERT INTO foj_left2 (id, v) VALUES (2, 'b')`)
	rows, err := db.Query(`
		SELECT l.v, r.id AS rid
		FROM foj_left2 l
		FULL OUTER JOIN foj_right2 r ON l.id = r.id`)
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 2 {
		t.Fatalf("expected 2 rows from left side, got %d", len(rows))
	}
	for _, row := range rows {
		if row["rid"].Kind != KindNull {
			t.Fatalf("expected rid=NULL for empty right, got %v", row["rid"])
		}
	}
}

// When no rows match at all, all rows from both sides appear with opposite NULLs.
func TestFullOuterJoinNoMatches(t *testing.T) {
	db := New()
	mustExec(t, db, `INSERT INTO foj_aa (id) VALUES (1)`)
	mustExec(t, db, `INSERT INTO foj_aa (id) VALUES (2)`)
	mustExec(t, db, `INSERT INTO foj_bb (id) VALUES (100)`)
	mustExec(t, db, `INSERT INTO foj_bb (id) VALUES (200)`)
	rows, err := db.Query(`
		SELECT a.id AS aid, b.id AS bid
		FROM foj_aa a FULL OUTER JOIN foj_bb b ON a.id = b.id`)
	if err != nil {
		t.Fatal(err)
	}
	// 2 from foj_aa (bid=NULL) + 2 from foj_bb (aid=NULL) = 4.
	if len(rows) != 4 {
		t.Fatalf("expected 4 rows, got %d", len(rows))
	}
	nullAid, nullBid := 0, 0
	for _, row := range rows {
		if row["aid"].Kind == KindNull {
			nullAid++
		}
		if row["bid"].Kind == KindNull {
			nullBid++
		}
	}
	if nullAid != 2 {
		t.Fatalf("expected 2 rows with aid=NULL, got %d", nullAid)
	}
	if nullBid != 2 {
		t.Fatalf("expected 2 rows with bid=NULL, got %d", nullBid)
	}
}

// FULL OUTER JOIN with WHERE that isolates rows where only one side is populated.
func TestFullOuterJoinWhereNullFilter(t *testing.T) {
	db := outerDB(t)
	rows, err := db.Query(`
		SELECT e.name, d.dname
		FROM employees e
		FULL OUTER JOIN departments d ON e.dept_id = d.id
		WHERE e.name IS NULL OR d.dname IS NULL`)
	if err != nil {
		t.Fatal(err)
	}
	// carol (name='carol', dname=NULL) and finance (name=NULL, dname='finance').
	if len(rows) != 2 {
		t.Fatalf("expected 2 unmatched rows, got %d: %v", len(rows), rows)
	}
}

// FULL OUTER JOIN inside a CTE.
func TestFullOuterJoinInCTE(t *testing.T) {
	db := outerDB(t)
	rows, err := db.Query(`
		WITH combined AS (
			SELECT e.name AS ename, d.dname
			FROM employees e
			FULL OUTER JOIN departments d ON e.dept_id = d.id
		)
		SELECT ename, dname FROM combined WHERE ename IS NULL OR dname IS NULL
		ORDER BY dname`)
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 2 {
		t.Fatalf("expected 2 rows, got %d: %v", len(rows), rows)
	}
}

// FULL OUTER JOIN + ORDER BY produces deterministic ordering.
func TestFullOuterJoinOrderBy(t *testing.T) {
	db := outerDB(t)
	rows, err := db.Query(`
		SELECT e.name, d.dname
		FROM employees e
		FULL OUTER JOIN departments d ON e.dept_id = d.id
		ORDER BY d.id`)
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 4 {
		t.Fatalf("expected 4 rows, got %d", len(rows))
	}
	// d.id=10→alice, 20→bob, 30→finance(name=NULL), NULL→carol(dname=NULL)
	// NULLs sort last so carol (d.id=NULL) should be last.
	last := rows[len(rows)-1]
	if last["name"].Kind == KindNull {
		// Last row is finance (NULL name) — also acceptable depending on sort order for NULLs.
		if last["dname"].V.(string) != "finance" {
			t.Fatalf("unexpected last row: %v", last)
		}
	} else if last["name"].V.(string) != "carol" {
		t.Fatalf("expected carol or finance as last row, got %v", last)
	}
}

// ── RIGHT JOIN ────────────────────────────────────────────────────────────────

// RIGHT JOIN: all right rows present; left side is NULL when no match.
func TestRightJoin(t *testing.T) {
	db := outerDB(t)
	rows, err := db.Query(`
		SELECT e.name, d.dname
		FROM employees e
		RIGHT JOIN departments d ON e.dept_id = d.id`)
	if err != nil {
		t.Fatal(err)
	}
	// Three departments → three rows.
	if len(rows) != 3 {
		t.Fatalf("expected 3 rows (all departments), got %d", len(rows))
	}
	// Finance (id=30) should have name=NULL.
	financeFound := false
	for _, row := range rows {
		if row["dname"].Kind == KindNull {
			continue
		}
		if row["dname"].V.(string) == "finance" {
			financeFound = true
			if row["name"].Kind != KindNull {
				t.Fatalf("finance row: expected name=NULL, got %v", row["name"])
			}
		}
	}
	if !financeFound {
		t.Fatal("finance department not found in RIGHT JOIN result")
	}
}

// RIGHT OUTER JOIN (with OUTER keyword) works identically to RIGHT JOIN.
func TestRightOuterJoin(t *testing.T) {
	db := outerDB(t)
	rows, err := db.Query(`
		SELECT e.name, d.dname
		FROM employees e
		RIGHT OUTER JOIN departments d ON e.dept_id = d.id`)
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 3 {
		t.Fatalf("expected 3 rows, got %d", len(rows))
	}
}

// RIGHT JOIN drops unmatched left rows (carol has no matching department).
func TestRightJoinDropsUnmatchedLeft(t *testing.T) {
	db := outerDB(t)
	rows, err := db.Query(`
		SELECT e.name, d.dname
		FROM employees e
		RIGHT JOIN departments d ON e.dept_id = d.id`)
	if err != nil {
		t.Fatal(err)
	}
	for _, row := range rows {
		if row["name"].Kind != KindNull && row["name"].V.(string) == "carol" {
			t.Fatal("carol (unmatched left) should not appear in RIGHT JOIN result")
		}
	}
}

// RIGHT JOIN with empty left table returns all right rows with NULL left columns.
func TestRightJoinEmptyLeft(t *testing.T) {
	db := New()
	mustExec(t, db, `INSERT INTO rj_right (id, v) VALUES (1, 'r1')`)
	mustExec(t, db, `INSERT INTO rj_right (id, v) VALUES (2, 'r2')`)
	rows, err := db.Query(`
		SELECT l.id AS lid, r.v
		FROM rj_left l
		RIGHT JOIN rj_right r ON l.id = r.id`)
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 2 {
		t.Fatalf("expected 2 rows, got %d", len(rows))
	}
	for _, row := range rows {
		if row["lid"].Kind != KindNull {
			t.Fatalf("expected lid=NULL, got %v", row["lid"])
		}
	}
}

// RIGHT JOIN + GROUP BY: unmatched right rows show up with zero aggregate.
func TestRightJoinWithGroupBy(t *testing.T) {
	db := outerDB(t)
	rows, err := db.Query(`
		SELECT d.dname, COUNT(e.id) AS emp_count
		FROM employees e
		RIGHT JOIN departments d ON e.dept_id = d.id
		GROUP BY d.dname
		ORDER BY d.dname`)
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 3 {
		t.Fatalf("expected 3 department rows, got %d", len(rows))
	}
	// Finance has no employees → COUNT(e.id) should be 0 (COUNT ignores NULLs).
	for _, row := range rows {
		if row["dname"].Kind != KindNull && row["dname"].V.(string) == "finance" {
			cnt := row["emp_count"].V.(int64)
			if cnt != 0 {
				t.Fatalf("finance emp_count: expected 0, got %d", cnt)
			}
		}
	}
}
