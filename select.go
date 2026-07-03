package vapordb

import (
	"fmt"
	"sort"
	"strings"

	"github.com/xwb1989/sqlparser"
)

// SELECT pipeline: execSelect, WHERE, ORDER BY, LIMIT, projection, DISTINCT.

// ─── SELECT ──────────────────────────────────────────────────────────────────

func execSelect(db *DB, stmt *sqlparser.Select) ([]Row, error) {
	refs, joins, err := extractFromClause(db, stmt.From)
	if err != nil {
		return nil, err
	}
	if len(refs) == 0 {
		return nil, fmt.Errorf("no tables in FROM clause")
	}

	isMultiTable := len(refs) > 1
	qualifyFirst := isMultiTable || refs[0].explicitAlias

	// Build initial rows from the first table (real or derived).
	firstRef := refs[0]
	var rows []Row
	if firstRef.name == "dual" {
		// MySQL's implicit dummy table: SELECT expr (no real FROM clause).
		rows = []Row{{}}
	} else {
		rows = rowsForRef(db, firstRef, qualifyFirst)
	}

	// Apply joins.
	for _, jd := range joins {
		rows, err = applyJoin(db, rows, jd)
		if err != nil {
			return nil, err
		}
	}

	// WHERE.
	if stmt.Where != nil {
		rows, err = applyWhere(db, rows, stmt.Where)
		if err != nil {
			return nil, err
		}
	}

	// GROUP BY / aggregates.
	// Pass HAVING expression so aggregates referenced only in HAVING (e.g.
	// HAVING COUNT(*) > 1 when SELECT has no COUNT) are pre-computed per group.
	hasAgg := selectHasAggregates(stmt.SelectExprs)
	if len(stmt.GroupBy) > 0 || hasAgg {
		var havingExpr sqlparser.Expr
		if stmt.Having != nil {
			havingExpr = stmt.Having.Expr
		}
		rows, err = applyGroupBy(db, rows, stmt.GroupBy, stmt.SelectExprs, havingExpr)
		if err != nil {
			return nil, err
		}
		if stmt.Having != nil {
			rows, err = applyWhere(db, rows, stmt.Having)
			if err != nil {
				return nil, err
			}
		}
	}

	// ORDER BY.
	// Enrich each working row with the SELECT-expression aliases so that
	// ORDER BY can reference computed columns like `ORDER BY revenue` when
	// `revenue` is defined as `price * qty AS revenue` in the SELECT list.
	if len(stmt.OrderBy) > 0 {
		rows, err = enrichWithAliases(db, rows, stmt.SelectExprs)
		if err != nil {
			return nil, err
		}
		if err = sortRows(db, rows, stmt.OrderBy); err != nil {
			return nil, err
		}
	}

	// LIMIT.
	if stmt.Limit != nil {
		rows, err = applyLimit(db, rows, stmt.Limit)
		if err != nil {
			return nil, err
		}
	}

	// Project columns.
	projected, err := projectRows(db, rows, stmt.SelectExprs, isMultiTable)
	if err != nil {
		return nil, err
	}

	// DISTINCT.
	if stmt.Distinct == sqlparser.DistinctStr {
		projected = distinctRows(projected)
	}

	return projected, nil
}

// nullRowLike returns a row whose keys mirror the first row in rows, with every
// value set to NULL. Used to generate null-padded left-side rows for RIGHT JOIN
// and FULL OUTER JOIN when a right row has no matching left partner.
func applyWhere(db *DB, rows []Row, where *sqlparser.Where) ([]Row, error) {
	result := make([]Row, 0, len(rows))
	for _, row := range rows {
		ok, err := evalBoolWithDB(db, where.Expr, row)
		if err != nil {
			return nil, err
		}
		if ok {
			result = append(result, row)
		}
	}
	return result, nil
}

// applyGroupBy groups rows by groupBy keys, applies aggregate functions from
// selectExprs (and optionally a HAVING expr so that HAVING aggregates not
// present in SELECT are still pre-computed), and returns one projected row per
// group. Callers should pass a non-nil having when they intend to filter the
// result with applyWhere / evalBoolWithDB afterwards.
func sortRows(db *DB, rows []Row, orderBy sqlparser.OrderBy) error {
	var sortErr error
	sort.SliceStable(rows, func(i, j int) bool {
		if sortErr != nil {
			return false
		}
		for _, order := range orderBy {
			a, err := evalExpr(db, order.Expr, rows[i])
			if err != nil {
				sortErr = err
				return false
			}
			b, err := evalExpr(db, order.Expr, rows[j])
			if err != nil {
				sortErr = err
				return false
			}
			cmp := Compare(a, b)
			if order.Direction == sqlparser.DescScr {
				cmp = -cmp
			}
			if cmp != 0 {
				return cmp < 0
			}
		}
		return false
	})
	return sortErr
}

func applyLimit(db *DB, rows []Row, limit *sqlparser.Limit) ([]Row, error) {
	offset := 0
	if limit.Offset != nil {
		v, err := evalExpr(nil, limit.Offset, Row{})
		if err != nil {
			return nil, err
		}
		if v.Kind == KindInt {
			offset = int(v.V.(int64))
		}
	}
	rowcount := len(rows)
	if limit.Rowcount != nil {
		v, err := evalExpr(nil, limit.Rowcount, Row{})
		if err != nil {
			return nil, err
		}
		if v.Kind == KindInt {
			rowcount = int(v.V.(int64))
		}
	}
	if offset >= len(rows) {
		return []Row{}, nil
	}
	end := offset + rowcount
	if end > len(rows) {
		end = len(rows)
	}
	return rows[offset:end], nil
}

func projectRows(db *DB, rows []Row, selectExprs sqlparser.SelectExprs, isJoin bool) ([]Row, error) {
	result := make([]Row, 0, len(rows))
	for _, row := range rows {
		out, err := projectRow(db, row, selectExprs, isJoin)
		if err != nil {
			return nil, err
		}
		result = append(result, out)
	}
	return result, nil
}

func projectRow(db *DB, row Row, selectExprs sqlparser.SelectExprs, isJoin bool) (Row, error) {
	out := make(Row)
	for _, se := range selectExprs {
		switch s := se.(type) {
		case *sqlparser.StarExpr:
			if s.TableName.IsEmpty() {
				// * → all columns, stripping the alias prefix.
				for k, v := range row {
					if parts := strings.SplitN(k, ".", 2); len(parts) == 2 {
						out[parts[1]] = v
					} else {
						out[k] = v
					}
				}
			} else {
				// alias.* → columns from that table alias.
				prefix := s.TableName.Name.String() + "."
				for k, v := range row {
					if strings.HasPrefix(k, prefix) {
						out[k[len(prefix):]] = v
					}
				}
			}
		case *sqlparser.AliasedExpr:
			key := outputKey(s)
			val, err := evalExprWithDB(db, s.Expr, row)
			if err != nil {
				return nil, err
			}
			out[key] = val
		}
	}
	return out, nil
}

func outputKey(ae *sqlparser.AliasedExpr) string {
	if !ae.As.IsEmpty() {
		return ae.As.Lowered()
	}
	switch e := ae.Expr.(type) {
	case *sqlparser.ColName:
		return e.Name.Lowered()
	default:
		return strings.ToLower(sqlparser.String(e))
	}
}

// enrichWithAliases adds SELECT-expression alias values to each working row so
// that ORDER BY can reference them by alias name before final projection.
// Existing keys are never overwritten, so real table columns take precedence.
func enrichWithAliases(db *DB, rows []Row, selectExprs sqlparser.SelectExprs) ([]Row, error) {
	result := make([]Row, len(rows))
	for i, row := range rows {
		enriched := copyRow(row)
		for _, se := range selectExprs {
			ae, ok := se.(*sqlparser.AliasedExpr)
			if !ok {
				continue
			}
			key := outputKey(ae)
			if _, exists := enriched[key]; exists {
				continue // don't shadow a real column
			}
			val, err := evalExprWithDB(db, ae.Expr, row)
			if err != nil {
				return nil, err
			}
			enriched[key] = val
		}
		result[i] = enriched
	}
	return result, nil
}

func distinctRows(rows []Row) []Row {
	seen := make(map[string]bool, len(rows))
	result := make([]Row, 0, len(rows))
	for _, row := range rows {
		k := rowKey(row)
		if !seen[k] {
			seen[k] = true
			result = append(result, row)
		}
	}
	return result
}
