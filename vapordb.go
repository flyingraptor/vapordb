// Package vapordb is an in-memory relational database with automatic schema inference.
// Write data — tables and columns appear. No CREATE TABLE, no ALTER TABLE.
package vapordb

import (
	"encoding/json"
	"fmt"
	"os"

	"github.com/xwb1989/sqlparser"
)

// DB is the top-level in-memory database.
type DB struct {
	Tables map[string]*Table
}

// New creates an empty database.
func New() *DB {
	return &DB{Tables: make(map[string]*Table)}
}

// Query executes a SELECT, UNION, or WITH (CTE) statement and returns the matching rows.
func (db *DB) Query(sql string) ([]Row, error) {
	target, mainSQL, err := resolveCTEs(db, sql)
	if err != nil {
		return nil, err
	}
	stmt, err := sqlparser.Parse(rewriteAnyAll(mainSQL))
	if err != nil {
		return nil, fmt.Errorf("parse error: %w", err)
	}
	return execSelectStatement(target, stmt)
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
