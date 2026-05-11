// Package vapordb is an in-memory relational database with automatic schema inference.
// Write data — tables and columns appear. No CREATE TABLE, no ALTER TABLE.
package vapordb

import (
	"encoding/json"
	"fmt"
	"os"
	"regexp"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/xwb1989/sqlparser"
)

// fullOuterRE matches FULL [OUTER] JOIN (case-insensitive) at word boundaries.
var fullOuterRE = regexp.MustCompile(`(?i)\bFULL\s+(?:OUTER\s+)?JOIN\b`)

// rewriteFullOuterJoins replaces FULL [OUTER] JOIN with STRAIGHT_JOIN so the
// MySQL-dialect parser (which has no FULL OUTER JOIN production) accepts the
// statement. walkTableExpr maps "straight_join" back to "full join" after
// parsing so that applyJoin can apply the correct FULL OUTER semantics.
func rewriteFullOuterJoins(sql string) string {
	return fullOuterRE.ReplaceAllString(sql, "STRAIGHT_JOIN")
}

// DB is the top-level in-memory database.
// All public methods are safe for concurrent use by multiple goroutines.
type DB struct {
	mu      sync.RWMutex
	Tables  map[string]*Table
	logPath atomic.Pointer[string] // set by Save/Load; nil means no query logging

	// forceWipeOnSchemaConflict selects legacy behaviour when an INSERT would
	// introduce an incompatible column type: wipe all rows and adopt the new
	// type. By default (false) such inserts return an error instead.
	forceWipeOnSchemaConflict bool
}

// Option configures a [DB] created with [New].
type Option func(*DB)

// WithForceWipeOnSchemaConflict enables (true) or disables (false) the legacy
// schema-conflict behaviour: incompatible types on an existing column clear
// the table and adopt the incoming type. When false (the default), those
// inserts return an error so mistakes are visible. Schema-locked tables always
// reject conflicts regardless of this setting.
//
// This sets the default for the lifetime of the [DB]; override per call with
// [WithWriteForceWipeOnSchemaConflict].
func WithForceWipeOnSchemaConflict(v bool) Option {
	return func(db *DB) {
		db.forceWipeOnSchemaConflict = v
	}
}

// New creates an empty database. Pass zero or more [Option] values to tune
// behaviour (for example [WithForceWipeOnSchemaConflict]).
func New(opts ...Option) *DB {
	db := &DB{Tables: make(map[string]*Table)}
	for _, o := range opts {
		if o != nil {
			o(db)
		}
	}
	return db
}

// writeOpts holds per-call overrides for mutating operations.
type writeOpts struct {
	forceWipe *bool
}

// WriteOption configures a single [DB.Exec], [DB.ExecNamed], [DB.Query] (for
// DML with RETURNING), or [DB.QueryNamed] when the expanded statement mutates
// data.
type WriteOption func(*writeOpts)

// WithWriteForceWipeOnSchemaConflict overrides the database default from [New]
// and [WithForceWipeOnSchemaConflict] for this call only. True enables the
// legacy wipe-on-incompatible-type behaviour for inserts in this execution;
// false forces rejection even when the DB default would wipe.
func WithWriteForceWipeOnSchemaConflict(v bool) WriteOption {
	return func(o *writeOpts) {
		o.forceWipe = &v
	}
}

func effectiveForceWipeOnSchemaConflict(db *DB, opts []WriteOption) bool {
	var o writeOpts
	for _, fn := range opts {
		if fn != nil {
			fn(&o)
		}
	}
	if o.forceWipe != nil {
		return *o.forceWipe
	}
	return db.forceWipeOnSchemaConflict
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
//
// Optional [WriteOption] values apply when the statement is DML (for example
// [WithWriteForceWipeOnSchemaConflict] on INSERT … RETURNING); they are ignored
// for plain SELECTs.
func (db *DB) Query(sql string, opts ...WriteOption) (rows []Row, retErr error) {
	start := time.Now()
	// Choose locking strategy before acquiring any lock: DML+RETURNING mutates
	// the database and requires an exclusive write lock; pure SELECTs only need
	// a shared read lock so multiple concurrent reads can proceed in parallel.
	_, _, hasDML := extractReturning(sql)
	if hasDML {
		db.mu.Lock()
	} else {
		db.mu.RLock()
	}
	logPath := ""
	if p := db.logPath.Load(); p != nil {
		logPath = *p
	}
	defer func() {
		if hasDML {
			db.mu.Unlock()
		} else {
			db.mu.RUnlock()
		}
		appendQueryLog(logPath, "query", sql, len(rows), time.Since(start), retErr)
	}()

	forceWipe := effectiveForceWipeOnSchemaConflict(db, opts)
	target, mainSQL, err := resolveCTEs(db, sql, forceWipe)
	if err != nil {
		return nil, err
	}

	// DML with RETURNING is handled before SELECT-only processing.
	if dmlSQL, retCols, hasReturning := extractReturning(mainSQL); hasReturning {
		return execDMLReturning(target, dmlSQL, retCols, forceWipe)
	}

	mainSQL = rewriteFullOuterJoins(mainSQL)
	mainSQL, winSpecs, err := extractWindowFuncs(mainSQL)
	if err != nil {
		return nil, err
	}
	stmt, err := sqlparser.Parse(rewriteAnyAll(mainSQL))
	if err != nil {
		return nil, fmt.Errorf("parse error: %w", err)
	}
	// Window functions must see the full row set before LIMIT/OFFSET; execSelect
	// applies LIMIT internally, so defer the outer limit until after applyWindowFuncs.
	var deferredLimit *sqlparser.Limit
	if len(winSpecs) > 0 {
		deferredLimit = detachOuterLimit(stmt)
	}
	rows, err = execSelectStatement(target, stmt)
	if err != nil {
		return nil, err
	}
	if len(winSpecs) > 0 {
		rows, err = applyWindowFuncs(rows, winSpecs)
		if err != nil {
			return nil, err
		}
	}
	if deferredLimit != nil {
		rows, err = applyLimit(db, rows, deferredLimit)
		if err != nil {
			return nil, err
		}
	}
	return rows, nil
}

// detachOuterLimit removes LIMIT/OFFSET from the outermost SELECT or UNION.
// Used with window queries so the executor runs on all rows; callers re-apply
// via applyLimit afterwards.
func detachOuterLimit(stmt sqlparser.Statement) *sqlparser.Limit {
	switch s := stmt.(type) {
	case *sqlparser.Select:
		lim := s.Limit
		s.Limit = nil
		return lim
	case *sqlparser.Union:
		lim := s.Limit
		s.Limit = nil
		return lim
	default:
		return nil
	}
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
// Optional [WriteOption] values apply when the statement is an INSERT that may
// trigger schema inference (for example [WithWriteForceWipeOnSchemaConflict]).
func (db *DB) Exec(sql string, opts ...WriteOption) (retErr error) {
	start := time.Now()
	db.mu.Lock()
	logPath := ""
	if p := db.logPath.Load(); p != nil {
		logPath = *p
	}
	defer func() {
		db.mu.Unlock()
		appendQueryLog(logPath, "exec", sql, 0, time.Since(start), retErr)
	}()

	forceWipe := effectiveForceWipeOnSchemaConflict(db, opts)
	target, mainSQL, err := resolveCTEs(db, sql, forceWipe)
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
		return execInsert(db, s, conflictCols, doNothing, forceWipe, nil, nil)
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
// After a successful save, all subsequent Query and Exec calls are appended
// to a companion query-log file at the same location
// (e.g. "db.json" → "db_queries.jsonl").
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
	lp := logPathFor(path)
	db.logPath.Store(&lp)
	return nil
}

// Load restores the database from a JSON file previously created by Save.
// Like Save, it enables the query log at the companion path.
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
	lp := logPathFor(path)
	db.logPath.Store(&lp)
	return nil
}
