package vapordb

import "testing"

// ── rewriteAnyAll unit tests ──────────────────────────────────────────────────

func TestRewriteAnyEq(t *testing.T) {
	in := "SELECT * FROM t WHERE id = ANY(1, 2, 3)"
	want := "SELECT * FROM t WHERE id IN (1, 2, 3)"
	if got := rewriteAnyAll(in); got != want {
		t.Errorf("got  %q\nwant %q", got, want)
	}
}

func TestRewriteAllNeqArrow(t *testing.T) {
	in := "SELECT * FROM t WHERE status <> ALL('a', 'b')"
	want := "SELECT * FROM t WHERE status NOT IN ('a', 'b')"
	if got := rewriteAnyAll(in); got != want {
		t.Errorf("got  %q\nwant %q", got, want)
	}
}

func TestRewriteAllNeqBang(t *testing.T) {
	in := "SELECT * FROM t WHERE status != ALL('a', 'b')"
	want := "SELECT * FROM t WHERE status NOT IN ('a', 'b')"
	if got := rewriteAnyAll(in); got != want {
		t.Errorf("got  %q\nwant %q", got, want)
	}
}

func TestRewriteAnyAllNoOp(t *testing.T) {
	in := "SELECT * FROM t WHERE id IN (1, 2, 3)"
	if got := rewriteAnyAll(in); got != in {
		t.Errorf("plain IN should pass through, got %q", got)
	}
}

// ── integration: literal ANY / ALL in Query ───────────────────────────────────

func TestAnyEqLiteralList(t *testing.T) {
	db := New()
	mustExec(t, db, `INSERT INTO t (id, name) VALUES (1, 'alice'), (2, 'bob'), (3, 'carol')`)

	rows := mustQuery(t, db, `SELECT name FROM t WHERE id = ANY(1, 3) ORDER BY id`)
	if len(rows) != 2 {
		t.Fatalf("want 2 rows, got %d", len(rows))
	}
	if rows[0]["name"] != strVal("alice") || rows[1]["name"] != strVal("carol") {
		t.Errorf("unexpected names: %v", rows)
	}
}

func TestAllNeqLiteralList(t *testing.T) {
	db := New()
	mustExec(t, db, `INSERT INTO t (id, name) VALUES (1, 'alice'), (2, 'bob'), (3, 'carol')`)

	rows := mustQuery(t, db, `SELECT name FROM t WHERE name <> ALL('alice', 'carol') ORDER BY id`)
	if len(rows) != 1 {
		t.Fatalf("want 1 row, got %d", len(rows))
	}
	if rows[0]["name"] != strVal("bob") {
		t.Errorf("want 'bob', got %v", rows[0]["name"])
	}
}

func TestAnyEqStringLiterals(t *testing.T) {
	db := New()
	mustExec(t, db, `INSERT INTO t (id, tag) VALUES (1, 'go'), (2, 'rust'), (3, 'zig')`)

	rows := mustQuery(t, db, `SELECT tag FROM t WHERE tag = ANY('go', 'zig') ORDER BY id`)
	if len(rows) != 2 {
		t.Fatalf("want 2 rows, got %d", len(rows))
	}
}

// ── integration: ANY / ALL with named slice parameters ────────────────────────

func TestAnyEqNamedSliceInt(t *testing.T) {
	db := New()
	mustExec(t, db, `INSERT INTO t (id, name) VALUES (1, 'alice'), (2, 'bob'), (3, 'carol')`)

	rows, err := db.QueryNamed(
		`SELECT name FROM t WHERE id = ANY(:ids) ORDER BY id`,
		map[string]any{"ids": []int{1, 3}},
	)
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 2 {
		t.Fatalf("want 2 rows, got %d", len(rows))
	}
	if rows[0]["name"] != strVal("alice") || rows[1]["name"] != strVal("carol") {
		t.Errorf("unexpected names: %v", rows)
	}
}

func TestAllNeqNamedSliceString(t *testing.T) {
	db := New()
	mustExec(t, db, `INSERT INTO t (id, name) VALUES (1, 'alice'), (2, 'bob'), (3, 'carol')`)

	rows, err := db.QueryNamed(
		`SELECT name FROM t WHERE name <> ALL(:excluded) ORDER BY id`,
		map[string]any{"excluded": []string{"alice", "carol"}},
	)
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 1 || rows[0]["name"] != strVal("bob") {
		t.Errorf("want [bob], got %v", rows)
	}
}

func TestAnyEqNamedSliceFloat(t *testing.T) {
	db := New()
	mustExec(t, db, `INSERT INTO scores (id, score) VALUES (1, 9.5), (2, 7.0), (3, 8.5)`)

	rows, err := db.QueryNamed(
		`SELECT id FROM scores WHERE score = ANY(:scores) ORDER BY id`,
		map[string]any{"scores": []float64{9.5, 8.5}},
	)
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 2 {
		t.Fatalf("want 2 rows, got %d", len(rows))
	}
}

func TestAnyEqNamedEmptySlice(t *testing.T) {
	db := New()
	mustExec(t, db, `INSERT INTO t (id) VALUES (1), (2)`)

	rows, err := db.QueryNamed(
		`SELECT id FROM t WHERE id = ANY(:ids)`,
		map[string]any{"ids": []int{}},
	)
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 0 {
		t.Errorf("empty slice should match nothing, got %d rows", len(rows))
	}
}

func TestAnyEqNamedSliceStruct(t *testing.T) {
	type Filter struct {
		IDs []int `db:"ids"`
	}
	db := New()
	mustExec(t, db, `INSERT INTO t (id, val) VALUES (1, 'a'), (2, 'b'), (3, 'c')`)

	rows, err := db.QueryNamed(
		`SELECT val FROM t WHERE id = ANY(:ids) ORDER BY id`,
		Filter{IDs: []int{2, 3}},
	)
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 2 {
		t.Fatalf("want 2, got %d", len(rows))
	}
	if rows[0]["val"] != strVal("b") || rows[1]["val"] != strVal("c") {
		t.Errorf("unexpected: %v", rows)
	}
}

// ANY also works in ExecNamed UPDATE / DELETE.

func TestAnyEqNamedInDelete(t *testing.T) {
	db := New()
	mustExec(t, db, `INSERT INTO t (id) VALUES (1), (2), (3)`)

	if err := db.ExecNamed(
		`DELETE FROM t WHERE id = ANY(:ids)`,
		map[string]any{"ids": []int{1, 3}},
	); err != nil {
		t.Fatal(err)
	}
	rows := mustQuery(t, db, `SELECT id FROM t`)
	if len(rows) != 1 || rows[0]["id"] != intVal(2) {
		t.Errorf("want only id=2, got %v", rows)
	}
}
