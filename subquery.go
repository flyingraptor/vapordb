package vapordb

import (
	"fmt"
	"strings"

	"github.com/xwb1989/sqlparser"
)

// EXISTS / correlated / IN / scalar subquery execution.

// ─── EXISTS / CORRELATED SUBQUERY ────────────────────────────────────────────

// correlatedSubqueryFromWhere runs inner FROM / JOIN / WHERE with outerRow
// merged into the predicate row (inner columns win on key conflicts). Returns
// surviving inner rows (not projected).
func correlatedSubqueryFromWhere(db *DB, inner *sqlparser.Select, outerRow Row) ([]Row, error) {
	refs, joins, err := extractFromClause(db, inner.From)
	if err != nil {
		return nil, err
	}
	if len(refs) == 0 {
		return nil, fmt.Errorf("subquery: no tables in FROM clause")
	}

	// Always qualify the first table's columns (e.g. "orders.id") so that
	// outer-row columns with the same bare name (e.g. "id" from users) are not
	// shadowed when the two rows are merged for correlated predicate evaluation.
	// resolveColumn's suffix-search fallback still finds "orders.user_id" when
	// a predicate uses the bare name "user_id".
	rows := rowsForRef(db, refs[0], true)
	for _, jd := range joins {
		if rows, err = applyJoin(db, rows, jd); err != nil {
			return nil, err
		}
	}
	if inner.Where != nil {
		qualifiedOuter := qualifyOuterRow(db, outerRow)
		filtered := rows[:0]
		for _, r := range rows {
			merged := mergeRowsOuter(qualifiedOuter, r)
			ok, err := evalBoolWithDB(db, inner.Where.Expr, merged)
			if err != nil {
				return nil, err
			}
			if ok {
				filtered = append(filtered, r)
			}
		}
		rows = filtered
	}
	return rows, nil
}

// execSelectCorrelated runs an EXISTS subquery, merging outerRow as a fallback
// so that correlated column references (e.g. users.id in an inner WHERE) resolve
// correctly against the outer driving row.
func execSelectCorrelated(db *DB, ex *sqlparser.ExistsExpr, outerRow Row) ([]Row, error) {
	inner, ok := ex.Subquery.Select.(*sqlparser.Select)
	if !ok {
		return nil, fmt.Errorf("EXISTS: unsupported subquery type %T", ex.Subquery.Select)
	}
	return correlatedSubqueryFromWhere(db, inner, outerRow)
}

// mergeRowsOuter builds a merged row for correlated predicate evaluation.
//
// Priority rules (highest to lowest):
//  1. Inner qualified keys  (e.g. "orders.user_id" — never overwritten)
//  2. Inner bare keys       (e.g. "user_id" derived from "orders.user_id")
//  3. Outer qualified keys  (e.g. "users.id"  — added so predicates like
//     `users.id = …` resolve to the outer value)
//  4. Outer bare keys       (e.g. "id" from outer — added last, only when
//     no inner bare key with the same name is present)
//
// This ensures that unqualified column references in the inner WHERE (e.g.
// bare `name`) resolve to the inner table, not the outer row, while
// qualified outer references (e.g. `users.id`) still find the outer value.
func mergeRowsOuter(outer, inner Row) Row {
	result := copyRow(inner) // step 1: all inner keys (qualified)

	// Step 2: bare versions of inner's qualified keys.
	for k, v := range inner {
		if dot := strings.IndexByte(k, '.'); dot > 0 {
			bare := k[dot+1:]
			if _, exists := result[bare]; !exists {
				result[bare] = v
			}
		}
	}

	// Steps 3 & 4: outer keys, only where not already present.
	for k, v := range outer {
		if _, exists := result[k]; !exists {
			result[k] = v
		}
	}
	return result
}

// qualifyOuterRow adds qualified copies (tablename.col) of bare keys in row by
// matching the row's column set against the DB schema. This allows correlated
// predicates that reference the outer table by name (e.g. `users.id`) to find
// the correct outer value even when the outer query stored the row with bare
// keys. Called only when the row has no qualified keys (single-table outer).
// Access to db.Tables is NOT lock-guarded here because this is always called
// from within a query that already holds db.mu.RLock.
func qualifyOuterRow(db *DB, row Row) Row {
	if len(row) == 0 {
		return row
	}
	// If row already has qualified keys (join / alias outer), return as-is.
	for k := range row {
		if strings.Contains(k, ".") {
			return row
		}
	}
	// Find the table whose schema is the largest subset of the row's bare keys.
	// Using subset-fit (rather than exact-fit) handles enriched rows where
	// computed aliases (e.g. ORDER BY enrichment) have added extra columns.
	tableName := ""
	bestLen := 0
	for name, tbl := range db.Tables {
		if len(tbl.Schema) > len(row) {
			continue // schema has more columns than the row → can't match
		}
		match := true
		for k := range tbl.Schema {
			if _, ok := row[k]; !ok {
				match = false
				break
			}
		}
		if match && len(tbl.Schema) > bestLen {
			tableName = name
			bestLen = len(tbl.Schema)
		}
	}
	if tableName == "" {
		return row
	}
	// Build a copy that has both bare keys and tablename.col keys.
	out := make(Row, len(row)*2)
	for k, v := range row {
		out[k] = v
		out[tableName+"."+k] = v
	}
	return out
}

// execSelectForIn runs a subquery on the RHS of IN / NOT IN with outerRow merged
// for correlation, returning one projected row per surviving inner row.
// Supports the full SELECT pipeline: GROUP BY, HAVING, ORDER BY, LIMIT, DISTINCT, UNION.
func execSelectForIn(db *DB, sub *sqlparser.Subquery, outerRow Row) ([]Row, error) {
	switch sel := sub.Select.(type) {
	case *sqlparser.Select:
		return execSelectForInSelect(db, sel, outerRow)
	case *sqlparser.Union:
		// For correlated UNION subqueries, run each branch with correlation then
		// re-apply the top-level ORDER BY / LIMIT using the existing execUnion
		// helpers. Correlation is handled inside execSelectForInSelect per branch.
		return execSelectForInUnion(db, sel, outerRow)
	default:
		return nil, fmt.Errorf("IN (subquery): unsupported subquery type %T", sub.Select)
	}
}

// execSelectForInSelect runs a single SELECT as an IN subquery, supporting the
// full pipeline: correlated WHERE, GROUP BY / aggregates, HAVING, ORDER BY,
// LIMIT, DISTINCT. Must project exactly one column.
func execSelectForInSelect(db *DB, inner *sqlparser.Select, outerRow Row) ([]Row, error) {
	if len(inner.SelectExprs) != 1 {
		return nil, fmt.Errorf("IN (subquery): inner SELECT must have exactly one column")
	}
	if _, ok := inner.SelectExprs[0].(*sqlparser.StarExpr); ok {
		return nil, fmt.Errorf("IN (subquery): use a single explicit column instead of SELECT *")
	}

	// FROM + JOINs + correlated WHERE.
	rows, err := correlatedSubqueryFromWhere(db, inner, outerRow)
	if err != nil {
		return nil, err
	}

	// GROUP BY / aggregates.
	if len(inner.GroupBy) > 0 || selectHasAggregates(inner.SelectExprs) {
		var havingExpr sqlparser.Expr
		if inner.Having != nil {
			havingExpr = inner.Having.Expr
		}
		rows, err = applyGroupBy(db, rows, inner.GroupBy, inner.SelectExprs, havingExpr)
		if err != nil {
			return nil, err
		}
	}

	// HAVING.
	if inner.Having != nil {
		qualifiedOuter := qualifyOuterRow(db, outerRow)
		filtered := rows[:0]
		for _, r := range rows {
			merged := mergeRowsOuter(qualifiedOuter, r)
			ok, herr := evalBoolWithDB(db, inner.Having.Expr, merged)
			if herr != nil {
				return nil, herr
			}
			if ok {
				filtered = append(filtered, r)
			}
		}
		rows = filtered
	}

	// ORDER BY.
	if len(inner.OrderBy) > 0 {
		rows, err = enrichWithAliases(db, rows, inner.SelectExprs)
		if err != nil {
			return nil, err
		}
		if err = sortRows(db, rows, inner.OrderBy); err != nil {
			return nil, err
		}
	}

	// LIMIT.
	if inner.Limit != nil {
		rows, err = applyLimit(db, rows, inner.Limit)
		if err != nil {
			return nil, err
		}
	}

	// Project.
	qualifiedOuter := qualifyOuterRow(db, outerRow)
	out := make([]Row, 0, len(rows))
	for _, r := range rows {
		merged := mergeRowsOuter(qualifiedOuter, r)
		pr, err := projectRow(db, merged, inner.SelectExprs, true)
		if err != nil {
			return nil, err
		}
		out = append(out, pr)
	}

	// DISTINCT.
	if inner.Distinct == sqlparser.DistinctStr {
		out = distinctRows(out)
	}

	return out, nil
}

// execSelectForInUnion handles UNION / UNION ALL inside an IN (subquery).
func execSelectForInUnion(db *DB, stmt *sqlparser.Union, outerRow Row) ([]Row, error) {
	rows, err := execSelectForInUnionNode(db, stmt, outerRow)
	if err != nil {
		return nil, err
	}
	if len(stmt.OrderBy) > 0 {
		if err := sortRows(db, rows, stmt.OrderBy); err != nil {
			return nil, err
		}
	}
	if stmt.Limit != nil {
		rows, err = applyLimit(db, rows, stmt.Limit)
		if err != nil {
			return nil, err
		}
	}
	return rows, nil
}

func execSelectForInUnionNode(db *DB, stmt *sqlparser.Union, outerRow Row) ([]Row, error) {
	var leftRows []Row
	var err error
	switch l := stmt.Left.(type) {
	case *sqlparser.Select:
		leftRows, err = execSelectForInSelect(db, l, outerRow)
	case *sqlparser.Union:
		leftRows, err = execSelectForInUnionNode(db, l, outerRow)
	default:
		return nil, fmt.Errorf("IN (subquery) UNION: unsupported left side %T", stmt.Left)
	}
	if err != nil {
		return nil, err
	}
	rightSel, ok := stmt.Right.(*sqlparser.Select)
	if !ok {
		return nil, fmt.Errorf("IN (subquery) UNION: unsupported right side %T", stmt.Right)
	}
	rightRows, err := execSelectForInSelect(db, rightSel, outerRow)
	if err != nil {
		return nil, err
	}
	combined := append(leftRows, rightRows...)
	if strings.EqualFold(stmt.Type, "union all") {
		return combined, nil
	}
	return distinctRows(combined), nil
}

// projectedSingleColumn returns the sole value from a one-column projected row.
func projectedSingleColumn(row Row) (Value, error) {
	if len(row) != 1 {
		return Null, fmt.Errorf("subquery: expected one projected column, got %d", len(row))
	}
	for _, v := range row {
		return v, nil
	}
	return Null, nil
}

// execScalarSubquery runs a scalar subquery (one column, zero or one row) with
// outerRow merged as a correlation fallback. Supports the full SELECT pipeline:
// FROM, JOINs, correlated WHERE, GROUP BY, HAVING, ORDER BY, LIMIT, DISTINCT.
// Returns NULL if the subquery produces no rows; errors if it produces 2 or more.
func execScalarSubquery(db *DB, inner *sqlparser.Select, outerRow Row) (Value, error) {
	if len(inner.SelectExprs) != 1 {
		return Null, fmt.Errorf("scalar subquery must SELECT exactly one column, got %d", len(inner.SelectExprs))
	}
	if _, ok := inner.SelectExprs[0].(*sqlparser.StarExpr); ok {
		return Null, fmt.Errorf("scalar subquery: use a single explicit column instead of SELECT *")
	}

	// FROM + JOINs + correlated WHERE.
	// correlatedSubqueryFromWhere always qualifies inner-table column keys
	// (e.g. "orders.id") so that bare outer keys (e.g. "id" from users) are
	// never shadowed during merge-based correlation evaluation.
	rows, err := correlatedSubqueryFromWhere(db, inner, outerRow)
	if err != nil {
		return Null, err
	}

	// GROUP BY / aggregates (e.g. SELECT COUNT(*) FROM …).
	hasAgg := selectHasAggregates(inner.SelectExprs)
	if len(inner.GroupBy) > 0 || hasAgg {
		var havingExpr sqlparser.Expr
		if inner.Having != nil {
			havingExpr = inner.Having.Expr
		}
		rows, err = applyGroupBy(db, rows, inner.GroupBy, inner.SelectExprs, havingExpr)
		if err != nil {
			return Null, err
		}
		if inner.Having != nil {
			filtered := rows[:0]
			for _, r := range rows {
				merged := mergeRowsOuter(outerRow, r)
				ok, herr := evalBoolWithDB(db, inner.Having.Expr, merged)
				if herr != nil {
					return Null, herr
				}
				if ok {
					filtered = append(filtered, r)
				}
			}
			rows = filtered
		}
	}

	// ORDER BY.
	if len(inner.OrderBy) > 0 {
		rows, err = enrichWithAliases(db, rows, inner.SelectExprs)
		if err != nil {
			return Null, err
		}
		if err = sortRows(db, rows, inner.OrderBy); err != nil {
			return Null, err
		}
	}

	// LIMIT.
	if inner.Limit != nil {
		rows, err = applyLimit(db, rows, inner.Limit)
		if err != nil {
			return Null, err
		}
	}

	// Project — inner rows may have qualified keys; projectRow resolves via
	// resolveColumn's suffix-search fallback so bare column names still work.
	projected, err := projectRows(db, rows, inner.SelectExprs, true)
	if err != nil {
		return Null, err
	}

	if inner.Distinct == sqlparser.DistinctStr {
		projected = distinctRows(projected)
	}

	switch len(projected) {
	case 0:
		return Null, nil
	case 1:
		return projectedSingleColumn(projected[0])
	default:
		return Null, fmt.Errorf("scalar subquery returned more than one row")
	}
}

// evalExprWithDB evaluates an expression with database access for EXISTS and IN (subquery).
