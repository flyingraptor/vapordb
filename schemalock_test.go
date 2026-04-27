package vapordb

import (
	"strings"
	"testing"
)

// ─── LockTable ────────────────────────────────────────────────────────────────

func TestLockTable_RejectsNewColumn(t *testing.T) {
	db := New()
	mustExec(t, db, `INSERT INTO users (id, name) VALUES (1, 'alice')`)
	db.LockTable("users")

	err := db.Exec(`INSERT INTO users (id, name, age) VALUES (2, 'bob', 30)`)
	if err == nil {
		t.Fatal("expected error when adding column to locked table")
	}
	if !strings.Contains(err.Error(), "schema-locked") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestLockTable_RejectsTypeWidening(t *testing.T) {
	db := New()
	mustExec(t, db, `INSERT INTO t (v) VALUES (1)`) // KindInt
	db.LockTable("t")

	err := db.Exec(`INSERT INTO t (v) VALUES (1.5)`) // would widen to KindFloat
	if err == nil {
		t.Fatal("expected error when widening type in locked table")
	}
	if !strings.Contains(err.Error(), "schema-locked") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestLockTable_RejectsTypeConflict(t *testing.T) {
	db := New()
	mustExec(t, db, `INSERT INTO t (v) VALUES ('hello')`) // KindString
	db.LockTable("t")

	err := db.Exec(`INSERT INTO t (v) VALUES (42)`) // conflict: int vs string
	if err == nil {
		t.Fatal("expected error on type conflict in locked table")
	}
	if !strings.Contains(err.Error(), "schema-locked") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestLockTable_AcceptsCompatibleInsert(t *testing.T) {
	db := New()
	mustExec(t, db, `INSERT INTO users (id, name) VALUES (1, 'alice')`)
	db.LockTable("users")

	mustExec(t, db, `INSERT INTO users (id, name) VALUES (2, 'bob')`)
	rows := mustQuery(t, db, `SELECT id FROM users ORDER BY id`)
	if len(rows) != 2 {
		t.Fatalf("expected 2 rows, got %d", len(rows))
	}
}

func TestLockTable_AcceptsNullForNewColumn(t *testing.T) {
	db := New()
	mustExec(t, db, `INSERT INTO t (a) VALUES (1)`)
	db.LockTable("t")

	// NULL doesn't trigger schema mutation, so it should be accepted.
	mustExec(t, db, `INSERT INTO t (a) VALUES (NULL)`)
}

func TestLockTable_NoOpOnMissingTable(t *testing.T) {
	db := New()
	db.LockTable("nonexistent") // must not panic
}

// ─── UnlockTable ─────────────────────────────────────────────────────────────

func TestUnlockTable_ResumesEvolution(t *testing.T) {
	db := New()
	mustExec(t, db, `INSERT INTO t (v) VALUES (1)`)
	db.LockTable("t")
	db.UnlockTable("t")

	mustExec(t, db, `INSERT INTO t (v, extra) VALUES (2, 'new')`)
}

// ─── LockSchema ──────────────────────────────────────────────────────────────

func TestLockSchema_LocksAllExistingTables(t *testing.T) {
	db := New()
	mustExec(t, db, `INSERT INTO a (x) VALUES (1)`)
	mustExec(t, db, `INSERT INTO b (y) VALUES ('hi')`)
	db.LockSchema()

	for _, tbl := range []string{"a", "b"} {
		if !db.Tables[tbl].Locked {
			t.Errorf("expected table %q to be locked", tbl)
		}
	}
}

func TestLockSchema_NewTableAfterLockIsNotLocked(t *testing.T) {
	db := New()
	mustExec(t, db, `INSERT INTO existing (x) VALUES (1)`)
	db.LockSchema()

	// New table created after LockSchema should evolve freely.
	mustExec(t, db, `INSERT INTO fresh (a) VALUES (1)`)
	mustExec(t, db, `INSERT INTO fresh (a, b) VALUES (2, 'new col')`)

	if db.Tables["fresh"].Locked {
		t.Error("table created after LockSchema should not be locked")
	}
}

func TestLockSchema_RejectsNewColumnOnAllTables(t *testing.T) {
	db := New()
	mustExec(t, db, `INSERT INTO t (v) VALUES (1)`)
	db.LockSchema()

	err := db.Exec(`INSERT INTO t (v, extra) VALUES (2, 'oops')`)
	if err == nil || !strings.Contains(err.Error(), "schema-locked") {
		t.Fatalf("expected schema-locked error, got %v", err)
	}
}

// ─── UnlockSchema ────────────────────────────────────────────────────────────

func TestUnlockSchema_ThawsAll(t *testing.T) {
	db := New()
	mustExec(t, db, `INSERT INTO t (v) VALUES (1)`)
	db.LockSchema()
	db.UnlockSchema()

	mustExec(t, db, `INSERT INTO t (v, extra) VALUES (2, 'allowed again')`)
}

// ─── Save / Load ─────────────────────────────────────────────────────────────

func TestSchemaLock_PersistsThroughSaveLoad(t *testing.T) {
	path := t.TempDir() + "/db.json"

	db := New()
	mustExec(t, db, `INSERT INTO t (v) VALUES (1)`)
	db.LockTable("t")
	if err := db.Save(path); err != nil {
		t.Fatal(err)
	}

	db2 := New()
	if err := db2.Load(path); err != nil {
		t.Fatal(err)
	}
	if !db2.Tables["t"].Locked {
		t.Fatal("expected table to still be locked after load")
	}

	err := db2.Exec(`INSERT INTO t (v, newcol) VALUES (2, 'x')`)
	if err == nil || !strings.Contains(err.Error(), "schema-locked") {
		t.Fatalf("expected schema-locked error after load, got %v", err)
	}
}
