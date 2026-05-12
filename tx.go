package vapordb

import "fmt"

// Tx is an in-progress database transaction. All mutations performed through a
// Tx are applied to the parent DB immediately, but a snapshot taken at Begin
// time lets Rollback restore the database to its pre-transaction state.
//
// Tx is not safe for concurrent use; callers must not share a Tx across
// goroutines.
type Tx struct {
	db       *DB
	snapshot map[string]*Table
	done     bool
}

// Begin starts a new transaction and returns a [Tx]. A shallow copy of every
// table's row slice is captured as a rollback snapshot.
func (db *DB) Begin() (*Tx, error) {
	db.mu.RLock()
	snap := snapshotTables(db.Tables)
	db.mu.RUnlock()
	return &Tx{db: db, snapshot: snap}, nil
}

// Exec executes an INSERT, UPDATE, or DELETE within the transaction.
func (tx *Tx) Exec(sql string, opts ...WriteOption) error {
	if tx.done {
		return fmt.Errorf("vapordb: transaction already completed")
	}
	return tx.db.Exec(sql, opts...)
}

// ExecNamed executes an INSERT, UPDATE, or DELETE with named :param
// placeholders within the transaction.
func (tx *Tx) ExecNamed(sql string, params any, opts ...WriteOption) error {
	if tx.done {
		return fmt.Errorf("vapordb: transaction already completed")
	}
	return tx.db.ExecNamed(sql, params, opts...)
}

// Query executes a SELECT or DML-with-RETURNING within the transaction.
func (tx *Tx) Query(sql string, opts ...WriteOption) ([]Row, error) {
	if tx.done {
		return nil, fmt.Errorf("vapordb: transaction already completed")
	}
	return tx.db.Query(sql, opts...)
}

// QueryNamed executes a SELECT or DML-with-RETURNING with named :param
// placeholders within the transaction.
func (tx *Tx) QueryNamed(sql string, params any, opts ...WriteOption) ([]Row, error) {
	if tx.done {
		return nil, fmt.Errorf("vapordb: transaction already completed")
	}
	return tx.db.QueryNamed(sql, params, opts...)
}

// Commit commits the transaction, making all changes permanent. The rollback
// snapshot is discarded.
func (tx *Tx) Commit() error {
	if tx.done {
		return fmt.Errorf("vapordb: transaction already completed")
	}
	tx.done = true
	tx.snapshot = nil
	return nil
}

// Rollback aborts the transaction, restoring the database to the state it was
// in when Begin was called. Changes made during the transaction are discarded.
func (tx *Tx) Rollback() error {
	if tx.done {
		return fmt.Errorf("vapordb: transaction already completed")
	}
	tx.done = true
	tx.db.mu.Lock()
	tx.db.Tables = tx.snapshot
	tx.db.mu.Unlock()
	tx.snapshot = nil
	return nil
}

// snapshotTables returns a deep copy of the tables map. Each table's row slice
// is copied so that mutations during a transaction do not corrupt the snapshot.
func snapshotTables(tables map[string]*Table) map[string]*Table {
	snap := make(map[string]*Table, len(tables))
	for name, tbl := range tables {
		snapTbl := &Table{
			Schema:   make(map[string]Kind, len(tbl.Schema)),
			EnumSets: make(map[string][]string, len(tbl.EnumSets)),
			Locked:   tbl.Locked,
			Rows:     make([]Row, len(tbl.Rows)),
		}
		for k, v := range tbl.Schema {
			snapTbl.Schema[k] = v
		}
		for k, v := range tbl.EnumSets {
			copied := make([]string, len(v))
			copy(copied, v)
			snapTbl.EnumSets[k] = copied
		}
		for i, row := range tbl.Rows {
			r := make(Row, len(row))
			for k, v := range row {
				r[k] = v
			}
			snapTbl.Rows[i] = r
		}
		snap[name] = snapTbl
	}
	return snap
}
