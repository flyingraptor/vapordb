// Package vapordb is an in-memory relational database with automatic schema inference.
// Write data — tables and columns appear. No CREATE TABLE, no ALTER TABLE.
package vapordb

import (
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"sync"

	"github.com/xwb1989/sqlparser"
)

// DB is the top-level in-memory database.
// All public methods are safe for concurrent use by multiple goroutines.
type DB struct {
	mu     sync.RWMutex
	Tables map[string]*Table
}

// New creates an empty database.
func New() *DB {
	return &DB{Tables: make(map[string]*Table)}
}

// LockSchema freezes the schema of every table that currently exists in the
// database. Subsequent INSERTs that would add a new column, widen a type, or
// trigger an unsafe type change on any of those tables will return an error.
// Tables created after LockSchema is called are not affected.
func (db *DB) LockSchema() {
	db.mu.Lock()
	defer db.mu.Unlock()
	for _, tbl := range db.Tables {
		tbl.Locked = true
	}
}

// UnlockSchema thaws the schema of every table in the database, re-enabling
// automatic schema evolution.
func (db *DB) UnlockSchema() {
	db.mu.Lock()
	defer db.mu.Unlock()
	for _, tbl := range db.Tables {
		tbl.Locked = false
	}
}

// LockTable freezes the schema of a single named table. If the table does not
// exist the call is a no-op.
func (db *DB) LockTable(name string) {
	db.mu.Lock()
	defer db.mu.Unlock()
	if tbl, ok := db.Tables[strings.ToLower(name)]; ok {
		tbl.Locked = true
	}
}

// UnlockTable thaws the schema of a single named table. If the table does not
// exist the call is a no-op.
func (db *DB) UnlockTable(name string) {
	db.mu.Lock()
	defer db.mu.Unlock()
	if tbl, ok := db.Tables[strings.ToLower(name)]; ok {
		tbl.Locked = false
	}
}

// DeclareEnum registers an allowed-value constraint for a column in table.
// Any INSERT or UPDATE that sets the column to a value outside the declared set
// returns an error. NULL is always accepted.
//
// Calling DeclareEnum again for the same column adds new variants to the set
// (widening); existing variants are never removed automatically.
//
// DeclareEnum can be called before the table exists; the constraint will be
// enforced once rows are written.
func (db *DB) DeclareEnum(table, column string, values ...string) {
	db.mu.Lock()
	defer db.mu.Unlock()

	tableName := strings.ToLower(table)
	colName := strings.ToLower(column)

	tbl, exists := db.Tables[tableName]
	if !exists {
		tbl = &Table{
			Schema:   make(map[string]Kind),
			EnumSets: make(map[string][]string),
			Rows:     make([]Row, 0),
		}
		db.Tables[tableName] = tbl
	}
	if tbl.EnumSets == nil {
		tbl.EnumSets = make(map[string][]string)
	}
	tbl.EnumSets[colName] = mergeEnumValues(tbl.EnumSets[colName], values)
}

// Query executes a SELECT, UNION, WITH (CTE), or DML with RETURNING and returns rows.
//
// For DML statements (INSERT / UPDATE / DELETE) append a RETURNING clause to get
// back the affected rows:
//
//	db.Query(`INSERT INTO t (id, name) VALUES (1, 'alice') RETURNING *`)
//	db.Query(`UPDATE t SET name = 'bob' WHERE id = 1 RETURNING id, name`)
//	db.Query(`DELETE FROM t WHERE id = 1 RETURNING id`)
func (db *DB) Query(sql string) ([]Row, error) {
	// Choose locking strategy before acquiring any lock: DML+RETURNING mutates
	// the database and requires an exclusive write lock; pure SELECTs only need
	// a shared read lock so multiple concurrent reads can proceed in parallel.
	if _, _, hasDML := extractReturning(sql); hasDML {
		db.mu.Lock()
		defer db.mu.Unlock()
	} else {
		db.mu.RLock()
		defer db.mu.RUnlock()
	}

	target, mainSQL, err := resolveCTEs(db, sql)
	if err != nil {
		return nil, err
	}

	// DML with RETURNING is handled before SELECT-only processing.
	if dmlSQL, retCols, hasReturning := extractReturning(mainSQL); hasReturning {
		return execDMLReturning(target, dmlSQL, retCols)
	}

	mainSQL, winSpecs, err := extractWindowFuncs(mainSQL)
	if err != nil {
		return nil, err
	}
	stmt, err := sqlparser.Parse(rewriteAnyAll(mainSQL))
	if err != nil {
		return nil, fmt.Errorf("parse error: %w", err)
	}
	rows, err := execSelectStatement(target, stmt)
	if err != nil {
		return nil, err
	}
	if len(winSpecs) > 0 {
		rows, err = applyWindowFuncs(rows, winSpecs)
		if err != nil {
			return nil, err
		}
	}
	return rows, nil
}

// execSelectStatement dispatches a parsed statement to execSelect or execUnion.
func execSelectStatement(db *DB, stmt sqlparser.Statement) ([]Row, error) {
	switch s := stmt.(type) {
	case *sqlparser.Select:
		return execSelect(db, s)
	case *sqlparser.Union:
		return execUnion(db, s)
	default:
		return nil, fmt.Errorf("Query only accepts SELECT/UNION statements; use Exec for %T", stmt)
	}
}

// Exec executes an INSERT, UPDATE, DELETE, or WITH … SELECT statement.
func (db *DB) Exec(sql string) error {
	db.mu.Lock()
	defer db.mu.Unlock()

	target, mainSQL, err := resolveCTEs(db, sql)
	if err != nil {
		return err
	}
	db = target
	sql = mainSQL
	rewritten, conflictCols, doNothing := rewriteOnConflict(rewriteAnyAll(sql))
	stmt, err := sqlparser.Parse(rewritten)
	if err != nil {
		return fmt.Errorf("parse error: %w", err)
	}
	switch s := stmt.(type) {
	case *sqlparser.Insert:
		return execInsert(db, s, conflictCols, doNothing)
	case *sqlparser.Update:
		return execUpdate(db, s)
	case *sqlparser.Delete:
		return execDelete(db, s)
	case *sqlparser.Select:
		_, err := execSelect(db, s)
		return err
	case *sqlparser.Union:
		_, err := execUnion(db, s)
		return err
	default:
		return fmt.Errorf("unsupported statement type: %T", stmt)
	}
}

// ─── PERSISTENCE ─────────────────────────────────────────────────────────────

type dbState struct {
	Tables map[string]*Table `json:"tables"`
}

// Save serialises the entire database to a JSON file at path.
func (db *DB) Save(path string) error {
	db.mu.RLock()
	defer db.mu.RUnlock()

	data, err := json.MarshalIndent(dbState{Tables: db.Tables}, "", "  ")
	if err != nil {
		return fmt.Errorf("marshal: %w", err)
	}
	if err := os.WriteFile(path, data, 0o644); err != nil {
		return fmt.Errorf("write %s: %w", path, err)
	}
	return nil
}

// Load restores the database from a JSON file previously created by Save.
func (db *DB) Load(path string) error {
	db.mu.Lock()
	defer db.mu.Unlock()

	data, err := os.ReadFile(path)
	if err != nil {
		return fmt.Errorf("read %s: %w", path, err)
	}
	var state dbState
	if err := json.Unmarshal(data, &state); err != nil {
		return fmt.Errorf("unmarshal: %w", err)
	}
	db.Tables = state.Tables
	if db.Tables == nil {
		db.Tables = make(map[string]*Table)
	}
	return nil
}
