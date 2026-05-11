package vapordb

import (
	"fmt"
	"strings"

	"github.com/xwb1989/sqlparser"
)

// ── Pre-processing ────────────────────────────────────────────────────────────

// extractReturning finds and removes a trailing RETURNING clause from a DML
// statement. It scans at paren-depth 0, skipping string literals, so it is
// safe for statements that contain subqueries or string values with the word
// "returning" inside them.
//
// Returns (stripped sql, column-list string, true) when found, or (sql, "", false).
func extractReturning(sql string) (string, string, bool) {
	// Walk forward, track depth and string literals; record the position of the
	// last RETURNING keyword seen at depth 0.
	lower := strings.ToLower(sql)
	depth := 0
	found := -1
	i := 0
	for i < len(sql) {
		switch {
		case sql[i] == '(':
			depth++
			i++
		case sql[i] == ')':
			depth--
			i++
		case sql[i] == '\'' || sql[i] == '"':
			q := sql[i]
			i++
			for i < len(sql) && sql[i] != q {
				i++
			}
			i++
		case depth == 0 && i+9 <= len(sql) && lower[i:i+9] == "returning":
			prevOK := i == 0 || !isRetAlphaNum(sql[i-1])
			nextOK := i+9 >= len(sql) || !isRetAlphaNum(sql[i+9])
			if prevOK && nextOK {
				found = i
			}
			i++
		default:
			i++
		}
	}
	if found < 0 {
		return sql, "", false
	}
	stripped := strings.TrimRight(sql[:found], " \t\n")
	retCols := strings.TrimSpace(sql[found+9:])
	return stripped, retCols, true
}

func isRetAlphaNum(b byte) bool {
	return (b >= 'a' && b <= 'z') || (b >= 'A' && b <= 'Z') ||
		(b >= '0' && b <= '9') || b == '_'
}

// ── Column projection ─────────────────────────────────────────────────────────

// projectReturning applies a RETURNING column list to a set of rows.
// Accepts "*" (all columns) or a comma-separated list of "col [AS alias]" terms.
func projectReturning(rows []Row, retCols string) ([]Row, error) {
	retCols = strings.TrimSpace(retCols)
	if retCols == "*" || retCols == "" {
		return rows, nil
	}

	type colSpec struct{ col, alias string }
	var specs []colSpec
	for _, part := range strings.Split(retCols, ",") {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		fields := strings.Fields(part)
		col := strings.ToLower(fields[0])
		alias := col
		if len(fields) == 3 && strings.EqualFold(fields[1], "AS") {
			alias = strings.ToLower(fields[2])
		}
		specs = append(specs, colSpec{col, alias})
	}
	if len(specs) == 0 {
		return rows, nil
	}

	result := make([]Row, len(rows))
	for i, row := range rows {
		projected := make(Row, len(specs))
		for _, s := range specs {
			v, ok := row[s.col]
			if !ok {
				// Suffix match for qualified names like "t.id" when asking for "id".
				suffix := "." + s.col
				for k, rv := range row {
					if strings.HasSuffix(k, suffix) {
						v, ok = rv, true
						break
					}
				}
			}
			if !ok {
				v = Null
			}
			projected[s.alias] = v
		}
		result[i] = projected
	}
	return result, nil
}

func copyRowSlice(rows []Row) []Row {
	result := make([]Row, len(rows))
	for i, r := range rows {
		cp := make(Row, len(r))
		for k, v := range r {
			cp[k] = v
		}
		result[i] = cp
	}
	return result
}

// ── DML-with-RETURNING helpers ────────────────────────────────────────────────

// execInsertReturning runs an INSERT and returns rows for RETURNING: one row
// per VALUES tuple that was inserted or updated by upsert (DO UPDATE), in
// order. DO NOTHING skips do not appear. If a schema conflict wipe occurred,
// all rows currently in the table are returned.
func execInsertReturning(db *DB, stmt *sqlparser.Insert, conflictCols []string, doNothing bool, forceWipeOnSchemaConflict bool) ([]Row, error) {
	tblName := stmt.Table.Name.String()

	var affectedIdx []int
	var schemaWiped bool
	if err := execInsert(db, stmt, conflictCols, doNothing, forceWipeOnSchemaConflict, &schemaWiped, &affectedIdx); err != nil {
		return nil, err
	}

	tbl := db.Tables[tblName]
	if tbl == nil || len(tbl.Rows) == 0 {
		return nil, nil
	}
	if schemaWiped {
		return copyRowSlice(tbl.Rows), nil
	}
	if len(affectedIdx) == 0 {
		return nil, nil
	}

	result := make([]Row, len(affectedIdx))
	for i, idx := range affectedIdx {
		r := tbl.Rows[idx]
		cp := make(Row, len(r))
		for k, v := range r {
			cp[k] = v
		}
		result[i] = cp
	}
	return result, nil
}

// execUpdateReturning runs an UPDATE and returns the rows in their post-update
// state. Rows are identified by their index in the table before the UPDATE runs,
// so the returned rows always reflect the new values.
func execUpdateReturning(db *DB, stmt *sqlparser.Update) ([]Row, error) {
	if len(stmt.TableExprs) == 0 {
		return nil, fmt.Errorf("UPDATE requires a table name")
	}
	ate, ok := stmt.TableExprs[0].(*sqlparser.AliasedTableExpr)
	if !ok {
		return nil, fmt.Errorf("unsupported UPDATE table expression type: %T", stmt.TableExprs[0])
	}
	tn, ok := ate.Expr.(sqlparser.TableName)
	if !ok {
		return nil, fmt.Errorf("unsupported UPDATE table expression")
	}
	tableName := tn.Name.String()

	tbl := db.Tables[tableName]
	if tbl == nil {
		return nil, nil
	}

	// Identify matching row indices before the update runs.
	var matchedIdx []int
	for i, row := range tbl.Rows {
		if stmt.Where != nil {
			match, err := evalBoolWithDB(db, stmt.Where.Expr, row)
			if err != nil {
				return nil, err
			}
			if !match {
				continue
			}
		}
		matchedIdx = append(matchedIdx, i)
	}

	if err := execUpdate(db, stmt); err != nil {
		return nil, err
	}

	// Return those rows in their new state (indices are stable because UPDATE
	// modifies in place without reordering).
	result := make([]Row, len(matchedIdx))
	for i, idx := range matchedIdx {
		cp := make(Row, len(tbl.Rows[idx]))
		for k, v := range tbl.Rows[idx] {
			cp[k] = v
		}
		result[i] = cp
	}
	return result, nil
}

// execDeleteReturning collects the rows that will be deleted, runs the DELETE,
// and returns the collected rows (their state before deletion).
func execDeleteReturning(db *DB, stmt *sqlparser.Delete) ([]Row, error) {
	if len(stmt.TableExprs) == 0 {
		return nil, fmt.Errorf("DELETE requires a table name")
	}
	ate, ok := stmt.TableExprs[0].(*sqlparser.AliasedTableExpr)
	if !ok {
		return nil, fmt.Errorf("unsupported DELETE table expression type: %T", stmt.TableExprs[0])
	}
	tn, ok := ate.Expr.(sqlparser.TableName)
	if !ok {
		return nil, fmt.Errorf("unsupported DELETE table expression")
	}
	tableName := tn.Name.String()

	tbl := db.Tables[tableName]
	if tbl == nil {
		return nil, nil
	}

	// Collect matching rows before deletion.
	var deleted []Row
	if stmt.Where == nil {
		deleted = make([]Row, len(tbl.Rows))
		for i, r := range tbl.Rows {
			cp := make(Row, len(r))
			for k, v := range r {
				cp[k] = v
			}
			deleted[i] = cp
		}
	} else {
		for _, row := range tbl.Rows {
			match, err := evalBoolWithDB(db, stmt.Where.Expr, row)
			if err != nil {
				return nil, err
			}
			if match {
				cp := make(Row, len(row))
				for k, v := range row {
					cp[k] = v
				}
				deleted = append(deleted, cp)
			}
		}
	}

	if err := execDelete(db, stmt); err != nil {
		return nil, err
	}

	return deleted, nil
}

// ── Dispatcher ────────────────────────────────────────────────────────────────

// execDMLReturning parses a DML statement (INSERT / UPDATE / DELETE), executes
// it, and returns the affected rows projected through the RETURNING column list.
func execDMLReturning(db *DB, sql string, retCols string, forceWipeOnSchemaConflict bool) ([]Row, error) {
	rewritten, conflictCols, doNothing := rewriteOnConflict(rewriteAnyAll(sql))
	stmt, err := sqlparser.Parse(rewritten)
	if err != nil {
		return nil, fmt.Errorf("parse error: %w", err)
	}

	var rows []Row
	switch s := stmt.(type) {
	case *sqlparser.Insert:
		rows, err = execInsertReturning(db, s, conflictCols, doNothing, forceWipeOnSchemaConflict)
	case *sqlparser.Update:
		rows, err = execUpdateReturning(db, s)
	case *sqlparser.Delete:
		rows, err = execDeleteReturning(db, s)
	default:
		return nil, fmt.Errorf("RETURNING requires INSERT, UPDATE, or DELETE; got %T", stmt)
	}
	if err != nil {
		return nil, err
	}

	return projectReturning(rows, retCols)
}
