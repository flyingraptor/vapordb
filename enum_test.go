package vapordb

import (
	"strings"
	"testing"
)

func TestEnum_ValidInsert(t *testing.T) {
	db := New()
	db.DeclareEnum("orders", "state", "pending", "active", "done")
	mustExec(t, db, `INSERT INTO orders (state, qty) VALUES ('pending', 1)`)
	mustExec(t, db, `INSERT INTO orders (state, qty) VALUES ('active', 2)`)
	mustExec(t, db, `INSERT INTO orders (state, qty) VALUES ('done', 3)`)

	rows := mustQuery(t, db, `SELECT state, qty FROM orders ORDER BY qty`)
	if len(rows) != 3 {
		t.Fatalf("expected 3 rows, got %d", len(rows))
	}
}

func TestEnum_InvalidInsert(t *testing.T) {
	db := New()
	db.DeclareEnum("orders", "state", "pending", "active", "done")
	err := db.Exec(`INSERT INTO orders (state) VALUES ('unknown')`)
	if err == nil {
		t.Fatal("expected error for value not in enum set, got nil")
	}
	if !strings.Contains(err.Error(), "not in the enum set") {
		t.Fatalf("unexpected error message: %v", err)
	}
}

func TestEnum_NullAllowed(t *testing.T) {
	db := New()
	db.DeclareEnum("t", "col", "a", "b")
	mustExec(t, db, `INSERT INTO t (col) VALUES (NULL)`)
	rows := mustQuery(t, db, `SELECT col FROM t`)
	if len(rows) != 1 || rows[0]["col"].Kind != KindNull {
		t.Fatalf("expected one NULL row, got %v", rows)
	}
}

func TestEnum_InvalidUpdate(t *testing.T) {
	db := New()
	db.DeclareEnum("t", "state", "on", "off")
	mustExec(t, db, `INSERT INTO t (state) VALUES ('on')`)
	err := db.Exec(`UPDATE t SET state = 'broken'`)
	if err == nil {
		t.Fatal("expected error for invalid UPDATE value, got nil")
	}
	if !strings.Contains(err.Error(), "not in the enum set") {
		t.Fatalf("unexpected error message: %v", err)
	}
}

func TestEnum_ValidUpdate(t *testing.T) {
	db := New()
	db.DeclareEnum("t", "state", "on", "off")
	mustExec(t, db, `INSERT INTO t (state) VALUES ('on')`)
	mustExec(t, db, `UPDATE t SET state = 'off'`)
	rows := mustQuery(t, db, `SELECT state FROM t`)
	if valueString(rows[0]["state"]) != "off" {
		t.Fatalf("expected 'off', got %q", valueString(rows[0]["state"]))
	}
}

func TestEnum_Widens(t *testing.T) {
	db := New()
	db.DeclareEnum("t", "col", "a", "b")
	db.DeclareEnum("t", "col", "c") // widen — adds 'c'

	if len(db.Tables["t"].EnumSets["col"]) != 3 {
		t.Fatalf("expected 3 enum values, got %v", db.Tables["t"].EnumSets["col"])
	}
	mustExec(t, db, `INSERT INTO t (col) VALUES ('c')`)
}

func TestEnum_NoConstraintByDefault(t *testing.T) {
	db := New()
	// No DeclareEnum: any string value is accepted.
	mustExec(t, db, `INSERT INTO t (col) VALUES ('anything')`)
	mustExec(t, db, `INSERT INTO t (col) VALUES ('goes')`)
}

func TestEnum_NonEnumColumnsUnaffected(t *testing.T) {
	db := New()
	db.DeclareEnum("t", "kind", "x", "y")
	mustExec(t, db, `INSERT INTO t (kind, name, score) VALUES ('x', 'alice', 42)`)
	rows := mustQuery(t, db, `SELECT name, score FROM t`)
	if len(rows) != 1 {
		t.Fatalf("expected 1 row, got %d", len(rows))
	}
}

func TestEnum_SaveLoad(t *testing.T) {
	path := t.TempDir() + "/db.json"

	db := New()
	db.DeclareEnum("tickets", "state", "open", "closed")
	mustExec(t, db, `INSERT INTO tickets (title, state) VALUES ('bug', 'open')`)
	if err := db.Save(path); err != nil {
		t.Fatal(err)
	}

	db2 := New()
	if err := db2.Load(path); err != nil {
		t.Fatal(err)
	}

	// Constraint survives the round-trip.
	if len(db2.Tables["tickets"].EnumSets["state"]) != 2 {
		t.Fatalf("enum set not restored: %v", db2.Tables["tickets"].EnumSets["state"])
	}
	err := db2.Exec(`INSERT INTO tickets (title, state) VALUES ('feat', 'invalid')`)
	if err == nil {
		t.Fatal("expected enum validation error after load")
	}
}
