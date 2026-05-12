package vapordb

import (
	"testing"
)

func TestTxCommit(t *testing.T) {
	db := New()
	mustExec(t, db, `INSERT INTO accounts (id, balance) VALUES (1, 100)`)

	tx, err := db.Begin()
	if err != nil {
		t.Fatalf("Begin: %v", err)
	}

	if err := tx.Exec(`UPDATE accounts SET balance = 200 WHERE id = 1`); err != nil {
		t.Fatalf("tx.Exec UPDATE: %v", err)
	}
	if err := tx.Exec(`INSERT INTO accounts (id, balance) VALUES (2, 50)`); err != nil {
		t.Fatalf("tx.Exec INSERT: %v", err)
	}

	if err := tx.Commit(); err != nil {
		t.Fatalf("Commit: %v", err)
	}

	rows, err := db.Query(`SELECT id, balance FROM accounts ORDER BY id`)
	if err != nil {
		t.Fatalf("Query after commit: %v", err)
	}
	if len(rows) != 2 {
		t.Fatalf("want 2 rows after commit, got %d", len(rows))
	}
	if rows[0]["balance"] != (Value{Kind: KindInt, V: int64(200)}) {
		t.Errorf("want balance=200 for id=1, got %v", rows[0]["balance"])
	}
}

func TestTxRollback(t *testing.T) {
	db := New()
	mustExec(t, db, `INSERT INTO accounts (id, balance) VALUES (1, 100)`)

	tx, err := db.Begin()
	if err != nil {
		t.Fatalf("Begin: %v", err)
	}

	if err := tx.Exec(`UPDATE accounts SET balance = 999 WHERE id = 1`); err != nil {
		t.Fatalf("tx.Exec UPDATE: %v", err)
	}
	if err := tx.Exec(`INSERT INTO accounts (id, balance) VALUES (2, 777)`); err != nil {
		t.Fatalf("tx.Exec INSERT: %v", err)
	}

	if err := tx.Rollback(); err != nil {
		t.Fatalf("Rollback: %v", err)
	}

	rows, err := db.Query(`SELECT id, balance FROM accounts`)
	if err != nil {
		t.Fatalf("Query after rollback: %v", err)
	}
	if len(rows) != 1 {
		t.Fatalf("want 1 row after rollback, got %d", len(rows))
	}
	if rows[0]["balance"] != (Value{Kind: KindInt, V: int64(100)}) {
		t.Errorf("want balance=100 after rollback, got %v", rows[0]["balance"])
	}
}

func TestTxDoubleCommit(t *testing.T) {
	db := New()
	tx, _ := db.Begin()
	if err := tx.Commit(); err != nil {
		t.Fatalf("first Commit: %v", err)
	}
	if err := tx.Commit(); err == nil {
		t.Error("expected error on second Commit, got nil")
	}
}

func TestTxCommitThenRollback(t *testing.T) {
	db := New()
	tx, _ := db.Begin()
	_ = tx.Commit()
	if err := tx.Rollback(); err == nil {
		t.Error("expected error on Rollback after Commit, got nil")
	}
}

func TestTxQuery(t *testing.T) {
	db := New()
	mustExec(t, db, `INSERT INTO items (id, name) VALUES (1, 'alpha')`)
	mustExec(t, db, `INSERT INTO items (id, name) VALUES (2, 'beta')`)

	tx, _ := db.Begin()
	defer tx.Rollback() //nolint:errcheck

	// Insert inside tx is visible to tx.Query
	if err := tx.Exec(`INSERT INTO items (id, name) VALUES (3, 'gamma')`); err != nil {
		t.Fatalf("tx.Exec: %v", err)
	}
	rows, err := tx.Query(`SELECT id FROM items ORDER BY id`)
	if err != nil {
		t.Fatalf("tx.Query: %v", err)
	}
	if len(rows) != 3 {
		t.Fatalf("want 3 rows inside tx, got %d", len(rows))
	}

	// Rollback removes the third row
	_ = tx.Rollback()
	rows, err = db.Query(`SELECT id FROM items ORDER BY id`)
	if err != nil {
		t.Fatalf("Query after rollback: %v", err)
	}
	if len(rows) != 2 {
		t.Fatalf("want 2 rows after rollback, got %d", len(rows))
	}
}

func TestSnapshotTables(t *testing.T) {
	db := New()
	mustExec(t, db, `INSERT INTO t (v) VALUES (1)`)

	snap := snapshotTables(db.Tables)
	// Mutate original
	mustExec(t, db, `UPDATE t SET v = 99`)

	// Snapshot should still have the old value
	if len(snap["t"].Rows) != 1 {
		t.Fatalf("snapshot should have 1 row")
	}
	if snap["t"].Rows[0]["v"] != (Value{Kind: KindInt, V: int64(1)}) {
		t.Errorf("snapshot row should have v=1, got %v", snap["t"].Rows[0]["v"])
	}
}
