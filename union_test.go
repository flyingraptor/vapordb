package vapordb

import "testing"

// ── UNION (distinct) ──────────────────────────────────────────────────────────

func TestUnionDeduplicates(t *testing.T) {
	db := New()
	mustExec(t, db, `INSERT INTO a (id, name) VALUES (1,'alice'),(2,'bob')`)
	mustExec(t, db, `INSERT INTO b (id, name) VALUES (2,'bob'),(3,'carol')`)

	rows := mustQuery(t, db, `SELECT id, name FROM a UNION SELECT id, name FROM b`)
	if len(rows) != 3 {
		t.Fatalf("UNION: want 3 distinct rows, got %d", len(rows))
	}
}

func TestUnionNoOverlap(t *testing.T) {
	db := New()
	mustExec(t, db, `INSERT INTO a (id) VALUES (1),(2)`)
	mustExec(t, db, `INSERT INTO b (id) VALUES (3),(4)`)

	rows := mustQuery(t, db, `SELECT id FROM a UNION SELECT id FROM b`)
	if len(rows) != 4 {
		t.Fatalf("want 4, got %d", len(rows))
	}
}

func TestUnionAllKeepsDuplicates(t *testing.T) {
	db := New()
	mustExec(t, db, `INSERT INTO a (id, name) VALUES (1,'alice'),(2,'bob')`)
	mustExec(t, db, `INSERT INTO b (id, name) VALUES (2,'bob'),(3,'carol')`)

	rows := mustQuery(t, db, `SELECT id, name FROM a UNION ALL SELECT id, name FROM b`)
	if len(rows) != 4 {
		t.Fatalf("UNION ALL: want 4 rows (duplicates kept), got %d", len(rows))
	}
}

// ── ORDER BY / LIMIT on UNION ─────────────────────────────────────────────────

func TestUnionOrderBy(t *testing.T) {
	db := New()
	mustExec(t, db, `INSERT INTO a (id) VALUES (3),(1)`)
	mustExec(t, db, `INSERT INTO b (id) VALUES (4),(2)`)

	rows := mustQuery(t, db, `SELECT id FROM a UNION SELECT id FROM b ORDER BY id`)
	if len(rows) != 4 {
		t.Fatalf("want 4, got %d", len(rows))
	}
	for i, want := range []int64{1, 2, 3, 4} {
		if rows[i]["id"].V.(int64) != want {
			t.Errorf("row %d: want %d, got %v", i, want, rows[i]["id"])
		}
	}
}

func TestUnionAllLimit(t *testing.T) {
	db := New()
	mustExec(t, db, `INSERT INTO a (id) VALUES (1),(2),(3)`)
	mustExec(t, db, `INSERT INTO b (id) VALUES (4),(5),(6)`)

	rows := mustQuery(t, db, `SELECT id FROM a UNION ALL SELECT id FROM b ORDER BY id LIMIT 4`)
	if len(rows) != 4 {
		t.Fatalf("want 4, got %d", len(rows))
	}
	if rows[3]["id"].V.(int64) != 4 {
		t.Errorf("4th row: want 4, got %v", rows[3]["id"])
	}
}

// ── Three-way chained UNION ───────────────────────────────────────────────────

func TestUnionThreeWay(t *testing.T) {
	db := New()
	mustExec(t, db, `INSERT INTO a (id) VALUES (1),(2)`)
	mustExec(t, db, `INSERT INTO b (id) VALUES (2),(3)`)
	mustExec(t, db, `INSERT INTO c (id) VALUES (3),(4)`)

	rows := mustQuery(t, db, `SELECT id FROM a UNION SELECT id FROM b UNION SELECT id FROM c ORDER BY id`)
	if len(rows) != 4 {
		t.Fatalf("3-way UNION: want 4 distinct, got %d", len(rows))
	}
	for i, want := range []int64{1, 2, 3, 4} {
		if rows[i]["id"].V.(int64) != want {
			t.Errorf("row %d: want %d, got %v", i, want, rows[i]["id"])
		}
	}
}

func TestUnionAllThreeWay(t *testing.T) {
	db := New()
	mustExec(t, db, `INSERT INTO a (id) VALUES (1)`)
	mustExec(t, db, `INSERT INTO b (id) VALUES (1)`)
	mustExec(t, db, `INSERT INTO c (id) VALUES (1)`)

	rows := mustQuery(t, db, `SELECT id FROM a UNION ALL SELECT id FROM b UNION ALL SELECT id FROM c`)
	if len(rows) != 3 {
		t.Fatalf("3-way UNION ALL: want 3, got %d", len(rows))
	}
}

// ── UNION with WHERE in individual branches ───────────────────────────────────

func TestUnionBranchWhere(t *testing.T) {
	db := New()
	mustExec(t, db, `INSERT INTO users (id, role) VALUES (1,'admin'),(2,'user'),(3,'admin')`)

	rows := mustQuery(t, db, `
		SELECT id FROM users WHERE role = 'admin'
		UNION
		SELECT id FROM users WHERE id = 2
		ORDER BY id`)

	if len(rows) != 3 {
		t.Fatalf("want 3, got %d", len(rows))
	}
}

// ── UNION with aggregates ─────────────────────────────────────────────────────

func TestUnionWithAggregate(t *testing.T) {
	db := New()
	mustExec(t, db, `INSERT INTO sales (region, amount) VALUES ('north',100),('north',200),('south',150)`)

	rows := mustQuery(t, db, `
		SELECT 'north' AS region, SUM(amount) AS total FROM sales WHERE region = 'north'
		UNION ALL
		SELECT 'south' AS region, SUM(amount) AS total FROM sales WHERE region = 'south'
		ORDER BY region`)

	if len(rows) != 2 {
		t.Fatalf("want 2, got %d", len(rows))
	}
	if rows[0]["total"] != intVal(300) {
		t.Errorf("north: want 300, got %v", rows[0]["total"])
	}
	if rows[1]["total"] != intVal(150) {
		t.Errorf("south: want 150, got %v", rows[1]["total"])
	}
}

// ── Mixed UNION / UNION ALL chain ─────────────────────────────────────────────

func TestMixedUnionAndUnionAll(t *testing.T) {
	db := New()
	mustExec(t, db, `INSERT INTO a (id) VALUES (1),(2)`)
	mustExec(t, db, `INSERT INTO b (id) VALUES (2),(3)`)
	mustExec(t, db, `INSERT INTO c (id) VALUES (3),(4)`)

	// (A UNION ALL B) UNION C
	// UNION ALL keeps 2 from a+b; outer UNION deduplicates with c
	rows := mustQuery(t, db, `
		SELECT id FROM a
		UNION ALL SELECT id FROM b
		UNION SELECT id FROM c
		ORDER BY id`)

	// Combined before outer dedup: [1,2,2,3] union-distinct [3,4] → [1,2,2,3,4]? 
	// Actually: (a UNION ALL b) gives [1,2,2,3], then UNION c([3,4]) deduplicates → [1,2,2,3,4]
	// Wait: distinctRows works on the full combined set [1,2,2,3,3,4] → [1,2,3,4]
	// No: (A UNION ALL B) is left, right is C. Combined = [1,2,2,3] + [3,4] = [1,2,2,3,3,4], then distinct = [1,2,3,4]
	if len(rows) != 4 {
		t.Fatalf("want 4, got %d: %v", len(rows), rows)
	}
}
