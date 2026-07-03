package vapordb

import (
	"fmt"

	"github.com/xwb1989/sqlparser"
)

// INSERT / UPSERT / UPDATE / DELETE execution.

// ─── INSERT / UPSERT ─────────────────────────────────────────────────────────

// execInsert handles plain INSERT and the two upsert variants:
//
//   - conflictCols non-nil + stmt.OnDup non-nil  →  update existing row on match
//   - conflictCols non-nil + doNothing true       →  skip insert on conflict
//
// The insert source may be a VALUES list or a SELECT/UNION (INSERT … SELECT);
// see insertSourceRows for how each source is materialised into target rows.
//
// When affectedRowIdx is non-nil, each inserted or upsert-updated row's table
// index is appended (in source-row order), so INSERT … RETURNING can include
// ON CONFLICT DO UPDATE results. DO NOTHING conflicts append nothing. Skipped
// when nil (e.g. plain Exec).
func execInsert(db *DB, stmt *sqlparser.Insert, conflictCols []string, doNothing bool, forceWipeOnSchemaConflict bool, trackSchemaWipe *bool, affectedRowIdx *[]int, upsertWhere string) error {
	tableName := stmt.Table.Name.String()

	if len(stmt.Columns) == 0 {
		return fmt.Errorf("INSERT requires an explicit column list")
	}

	cols := make([]string, len(stmt.Columns))
	for i, c := range stmt.Columns {
		cols[i] = c.Lowered()
	}

	// Build the incoming rows from either a VALUES list or a SELECT/UNION
	// source (INSERT … SELECT). The SELECT is materialised up front so that
	// reading from and writing to the same table in one statement is safe.
	incoming, err := insertSourceRows(db, stmt, cols)
	if err != nil {
		return err
	}

	for _, row := range incoming {
		// Validate enum constraints before touching the schema.
		if existing := db.Tables[tableName]; existing != nil {
			if err := validateEnum(existing, row); err != nil {
				return err
			}
		}

		// ── Upsert path ──────────────────────────────────────────────────────
		if len(conflictCols) > 0 {
			w, err := UpsertSchema(db, tableName, row, forceWipeOnSchemaConflict)
			if err != nil {
				return err
			}
			if trackSchemaWipe != nil && w {
				*trackSchemaWipe = true
			}
			tbl := db.Tables[tableName]
			if idx := findConflict(tbl, conflictCols, row); idx >= 0 {
				// Conflict found.
				if doNothing {
					continue // skip this row silently
				}
				// Gap 4: optimistic-lock predicate — evaluate against the existing row.
				// If the predicate is false the update is silently skipped (matching
				// PostgreSQL's "no rows updated" semantics for failed lock checks).
				if upsertWhere != "" {
					ok, err := evalUpsertWhere(db, upsertWhere, tbl.Rows[idx])
					if err != nil {
						return fmt.Errorf("evaluating upsert WHERE predicate: %w", err)
					}
					if !ok {
						continue
					}
				}
				// Apply ON DUPLICATE KEY UPDATE assignments.
				changed, err := applyOnDup(db, tbl.Rows[idx], row, stmt.OnDup)
				if err != nil {
					return err
				}
				// Drop only the indexes whose key columns the update touched.
				tbl.invalidateConflictIdxForCols(changed)
				// Re-validate after the update assignments.
				if err := validateEnum(tbl, tbl.Rows[idx]); err != nil {
					return err
				}
				if affectedRowIdx != nil {
					*affectedRowIdx = append(*affectedRowIdx, idx)
				}
				continue
			}
		}
		// ── Normal insert ────────────────────────────────────────────────────
		w, err := UpsertSchema(db, tableName, row, forceWipeOnSchemaConflict)
		if err != nil {
			return err
		}
		if trackSchemaWipe != nil && w {
			*trackSchemaWipe = true
		}
		tbl := db.Tables[tableName]
		for col := range tbl.Schema {
			if _, exists := row[col]; !exists {
				row[col] = Null
			}
		}
		tbl.Rows = append(tbl.Rows, row)
		tbl.indexAppendRow(len(tbl.Rows)-1, row)
		if affectedRowIdx != nil {
			*affectedRowIdx = append(*affectedRowIdx, len(tbl.Rows)-1)
		}
	}
	return nil
}

// insertSourceRows materialises the rows an INSERT will apply, from either a
// VALUES list or a SELECT/UNION source (INSERT … SELECT). Each returned Row is
// keyed by the INSERT's target column list (cols). For a SELECT source the
// SELECT's output columns are mapped positionally onto cols.
func insertSourceRows(db *DB, stmt *sqlparser.Insert, cols []string) ([]Row, error) {
	switch src := stmt.Rows.(type) {
	case sqlparser.Values:
		rows := make([]Row, 0, len(src))
		for _, valTuple := range src {
			if len(valTuple) != len(cols) {
				return nil, fmt.Errorf("column/value count mismatch: %d columns vs %d values",
					len(cols), len(valTuple))
			}
			row := make(Row, len(cols))
			for i, expr := range valTuple {
				val, err := evalExpr(db, expr, Row{})
				if err != nil {
					return nil, fmt.Errorf("evaluating value for column %q: %w", cols[i], err)
				}
				row[cols[i]] = val
			}
			rows = append(rows, row)
		}
		return rows, nil

	case sqlparser.SelectStatement:
		return insertSelectRows(db, src, cols)

	default:
		return nil, fmt.Errorf("unsupported INSERT source type: %T", stmt.Rows)
	}
}

// insertSelectRows runs a SELECT/UNION source and maps each result row onto the
// INSERT target columns positionally: target column i receives the value of the
// SELECT's i-th output column. The SELECT must project an explicit column list
// (no bare "*" / "tbl.*") so that output columns are ordered and countable, and
// the output-column count must match the target-column count.
func insertSelectRows(db *DB, sel sqlparser.SelectStatement, cols []string) ([]Row, error) {
	names, err := selectOutputColumns(sel)
	if err != nil {
		return nil, err
	}
	if len(names) != len(cols) {
		return nil, fmt.Errorf("column/value count mismatch: %d columns vs %d selected columns",
			len(cols), len(names))
	}
	// Reject duplicate output names: the row representation is a map, so two
	// columns that resolve to the same output key cannot be read back
	// positionally without data loss. Ask the user to disambiguate with AS.
	seen := make(map[string]struct{}, len(names))
	for _, n := range names {
		if _, dup := seen[n]; dup {
			return nil, fmt.Errorf("INSERT … SELECT: duplicate output column %q; add an alias (AS …) to disambiguate", n)
		}
		seen[n] = struct{}{}
	}

	resultRows, err := execSelectStatement(db, asStatement(sel))
	if err != nil {
		return nil, err
	}

	out := make([]Row, 0, len(resultRows))
	for _, rr := range resultRows {
		row := make(Row, len(cols))
		for i, target := range cols {
			if v, ok := rr[names[i]]; ok {
				row[target] = v
			} else {
				row[target] = Null
			}
		}
		out = append(out, row)
	}
	return out, nil
}

// asStatement unwraps a ParenSelect to the concrete SELECT/UNION statement that
// execSelectStatement understands.
func asStatement(sel sqlparser.SelectStatement) sqlparser.Statement {
	if ps, ok := sel.(*sqlparser.ParenSelect); ok {
		return asStatement(ps.Select)
	}
	return sel
}

// selectOutputColumns returns the ordered output-column names of a SELECT/UNION
// source, derived from its projection list using the same naming rule as
// projectRow (so the names match the keys present in the executed result rows).
// A UNION takes its column names from the left-most SELECT, matching SQL
// semantics. Bare "*" / "tbl.*" projections are rejected because the row
// representation is an unordered map, so a deterministic positional mapping is
// not possible.
func selectOutputColumns(sel sqlparser.SelectStatement) ([]string, error) {
	switch s := sel.(type) {
	case *sqlparser.Select:
		names := make([]string, 0, len(s.SelectExprs))
		for _, se := range s.SelectExprs {
			ae, ok := se.(*sqlparser.AliasedExpr)
			if !ok {
				return nil, fmt.Errorf("INSERT … SELECT requires an explicit column list in the SELECT; '*' is not supported, name each column instead")
			}
			names = append(names, outputKey(ae))
		}
		if len(names) == 0 {
			return nil, fmt.Errorf("INSERT … SELECT: the SELECT has no output columns")
		}
		return names, nil
	case *sqlparser.Union:
		// Every UNION branch must project the same ordered output names so the
		// positional mapping is unambiguous: result rows are keyed by each
		// branch's own output names, so divergent names would silently insert
		// NULLs for the mismatched branch. Require explicit AS aliases instead.
		left, err := selectOutputColumns(s.Left)
		if err != nil {
			return nil, err
		}
		right, err := selectOutputColumns(s.Right)
		if err != nil {
			return nil, err
		}
		if len(left) != len(right) {
			return nil, fmt.Errorf("INSERT … SELECT: UNION branches project different column counts (%d vs %d)", len(left), len(right))
		}
		for i := range left {
			if left[i] != right[i] {
				return nil, fmt.Errorf("INSERT … SELECT: UNION branches have mismatched output column %q vs %q; give both the same name with AS", left[i], right[i])
			}
		}
		return left, nil
	case *sqlparser.ParenSelect:
		return selectOutputColumns(s.Select)
	default:
		return nil, fmt.Errorf("INSERT … SELECT: unsupported SELECT type %T", sel)
	}
}

// findConflict returns the index of the first row in tbl whose values for all
// conflictCols match the incoming row, or -1 if no conflict exists.
//
// It uses a cached conflict-key index (O(1) lookup) when the key values are
// encodable, falling back to a linear scan for Date / JSON keys so the match
// semantics remain identical to a direct value comparison.
func findConflict(tbl *Table, conflictCols []string, incoming Row) int {
	if tbl == nil {
		return -1
	}
	if idx, used := tbl.lookupConflict(conflictCols, incoming); used {
		return idx
	}
	// Fallback: linear scan (non-encodable key values, e.g. Date / JSON).
	for i, existing := range tbl.Rows {
		match := true
		for _, col := range conflictCols {
			if existing[col] != incoming[col] {
				match = false
				break
			}
		}
		if match {
			return i
		}
	}
	return -1
}

// applyOnDup updates target in-place using the OnDup assignment list from the
// parsed INSERT statement. Each assignment is either VALUES(col) — meaning
// "use the value from the incoming row" — or any other expression evaluated
// against the incoming row as context.
// applyOnDup updates target in place and returns the names of the columns it
// wrote, so the caller can invalidate only the conflict indexes those columns
// participate in (indexes on untouched columns stay valid).
func applyOnDup(db *DB, target Row, incoming Row, onDup sqlparser.OnDup) ([]string, error) {
	changed := make([]string, 0, len(onDup))
	for _, upd := range onDup {
		colName := upd.Name.Name.Lowered()
		var newVal Value
		switch expr := upd.Expr.(type) {
		case *sqlparser.ValuesFuncExpr:
			// VALUES(col) → take the value from the incoming row.
			ref := expr.Name.Name.Lowered()
			v, ok := incoming[ref]
			if !ok {
				v = Null
			}
			newVal = v
		default:
			// General expression evaluated in the context of the incoming row.
			var err error
			newVal, err = evalExpr(db, expr, incoming)
			if err != nil {
				return nil, fmt.Errorf("ON CONFLICT update expr for %q: %w", colName, err)
			}
		}
		target[colName] = newVal
		changed = append(changed, colName)
	}
	return changed, nil
}

// evalUpsertWhere parses a bare SQL predicate string and evaluates it against
// existingRow. It is used to implement the optimistic-lock WHERE clause of
// ON CONFLICT DO UPDATE … WHERE <pred>.
func evalUpsertWhere(db *DB, pred string, existingRow Row) (bool, error) {
	stmt, err := sqlparser.Parse("SELECT 1 WHERE " + pred)
	if err != nil {
		return false, fmt.Errorf("invalid upsert WHERE predicate %q: %w", pred, err)
	}
	sel, ok := stmt.(*sqlparser.Select)
	if !ok || sel.Where == nil {
		return false, fmt.Errorf("could not extract WHERE expression from upsert predicate: %s", pred)
	}
	return evalBoolWithDB(db, sel.Where.Expr, existingRow)
}

// ─── UPDATE ──────────────────────────────────────────────────────────────────

func execUpdate(db *DB, stmt *sqlparser.Update) error {
	if len(stmt.TableExprs) == 0 {
		return fmt.Errorf("UPDATE requires a table name")
	}
	ate, ok := stmt.TableExprs[0].(*sqlparser.AliasedTableExpr)
	if !ok {
		return fmt.Errorf("unsupported UPDATE table expression type: %T", stmt.TableExprs[0])
	}
	tn, ok := ate.Expr.(sqlparser.TableName)
	if !ok {
		return fmt.Errorf("unsupported UPDATE table expression")
	}
	tableName := tn.Name.String()

	tbl := db.Tables[tableName]
	if tbl == nil {
		return nil
	}
	// UPDATE can change any column value; drop cached conflict indexes so a
	// later upsert rebuilds them from the mutated rows.
	tbl.invalidateConflictIdx()

	for i, row := range tbl.Rows {
		if stmt.Where != nil {
			match, err := evalBoolWithDB(db, stmt.Where.Expr, row)
			if err != nil {
				return err
			}
			if !match {
				continue
			}
		}
		for _, upd := range stmt.Exprs {
			col := upd.Name.Name.Lowered()
			val, err := evalExpr(db, upd.Expr, row)
			if err != nil {
				return fmt.Errorf("evaluating SET %s: %w", col, err)
			}
			if err := validateEnumColumn(tbl, col, val); err != nil {
				return err
			}
			tbl.Rows[i][col] = val
			// Evolve schema (no wipe on UPDATE — just widen or accept new type).
			if val.Kind != KindNull {
				if existing, has := tbl.Schema[col]; !has || val.Kind > existing {
					tbl.Schema[col] = val.Kind
				} else if !has {
					tbl.Schema[col] = val.Kind
				}
			}
		}
	}
	return nil
}

// ─── DELETE ──────────────────────────────────────────────────────────────────

func execDelete(db *DB, stmt *sqlparser.Delete) error {
	if len(stmt.TableExprs) == 0 {
		return fmt.Errorf("DELETE requires a table name")
	}
	ate, ok := stmt.TableExprs[0].(*sqlparser.AliasedTableExpr)
	if !ok {
		return fmt.Errorf("unsupported DELETE table expression type: %T", stmt.TableExprs[0])
	}
	tn, ok := ate.Expr.(sqlparser.TableName)
	if !ok {
		return fmt.Errorf("unsupported DELETE table expression")
	}
	tableName := tn.Name.String()

	tbl := db.Tables[tableName]
	if tbl == nil {
		return nil
	}
	// DELETE removes rows and shifts indices; drop cached conflict indexes.
	tbl.invalidateConflictIdx()

	if stmt.Where == nil {
		tbl.Rows = make([]Row, 0)
		return nil
	}

	kept := make([]Row, 0, len(tbl.Rows))
	for _, row := range tbl.Rows {
		match, err := evalBoolWithDB(db, stmt.Where.Expr, row)
		if err != nil {
			return err
		}
		if !match {
			kept = append(kept, row)
		}
	}
	tbl.Rows = kept
	return nil
}
