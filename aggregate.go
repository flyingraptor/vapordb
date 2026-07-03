package vapordb

import (
	"fmt"
	"strings"

	"github.com/xwb1989/sqlparser"
)

// GROUP BY and aggregate functions (COUNT/SUM/AVG/MIN/MAX/ARRAY_AGG).

func applyGroupBy(db *DB, rows []Row, groupBy sqlparser.GroupBy, selectExprs sqlparser.SelectExprs, having ...sqlparser.Expr) ([]Row, error) {
	if len(groupBy) == 0 {
		// No GROUP BY but aggregates present → treat all rows as one group.
		out, err := computeGroup(db, rows, selectExprs, having...)
		if err != nil {
			return nil, err
		}
		return []Row{out}, nil
	}

	groupMap := make(map[string][]Row)
	var groupOrder []string
	for _, row := range rows {
		key, err := computeGroupKey(db, row, groupBy)
		if err != nil {
			return nil, err
		}
		if _, exists := groupMap[key]; !exists {
			groupOrder = append(groupOrder, key)
		}
		groupMap[key] = append(groupMap[key], row)
	}

	result := make([]Row, 0, len(groupOrder))
	for _, key := range groupOrder {
		out, err := computeGroup(db, groupMap[key], selectExprs, having...)
		if err != nil {
			return nil, err
		}
		result = append(result, out)
	}
	return result, nil
}

func computeGroupKey(db *DB, row Row, groupBy sqlparser.GroupBy) (string, error) {
	parts := make([]string, 0, len(groupBy))
	for _, expr := range groupBy {
		val, err := evalExpr(db, expr, row)
		if err != nil {
			return "", err
		}
		parts = append(parts, fmt.Sprintf("%T\x00%v", val.V, val.V))
	}
	return strings.Join(parts, "\x01"), nil
}

func computeGroup(db *DB, rows []Row, selectExprs sqlparser.SelectExprs, extraExprs ...sqlparser.Expr) (Row, error) {
	firstRow := Row{}
	if len(rows) > 0 {
		firstRow = rows[0]
	}

	// Build an "aggRow" that contains all non-NULL aggregate sub-expressions
	// pre-computed and stored under their canonical SQL string keys.
	// evalExpr already knows to look up aggregate functions in the row
	// so COALESCE(SUM(…), 0) and HAVING COUNT(*) > 1 work automatically
	// once the aggregate result is in the row.
	aggRow := precomputeGroupAggs(db, rows, selectExprs, firstRow, extraExprs...)

	out := make(Row)
	for _, se := range selectExprs {
		switch s := se.(type) {
		case *sqlparser.StarExpr:
			for k, v := range firstRow {
				out[k] = v
			}
		case *sqlparser.AliasedExpr:
			outKey := outputKey(s)
			var val Value
			var err error
			if fe, ok := s.Expr.(*sqlparser.FuncExpr); ok && isAggFunc(fe.Name.Lowered()) {
				val, err = evalAggFunc(db, fe, rows)
			} else {
				// Use aggRow so that aggregate sub-expressions nested inside
				// scalar wrappers (COALESCE, IFNULL, arithmetic, …) resolve
				// to their pre-computed group values.
				val, err = evalExpr(db, s.Expr, aggRow)
			}
			if err != nil {
				return nil, err
			}
			out[outKey] = val
			// Also store by canonical key so HAVING can look it up.
			canonical := strings.ToLower(sqlparser.String(s.Expr))
			if canonical != outKey {
				out[canonical] = val
			}
		}
	}

	// Copy pre-computed aggregate values (e.g. "sum(case when …)") into the
	// output row so that the subsequent projectRows call can resolve aggregate
	// sub-expressions that are wrapped inside scalar functions like COALESCE.
	for k, v := range aggRow {
		if _, exists := out[k]; !exists {
			if _, inFirstRow := firstRow[k]; !inFirstRow {
				out[k] = v
			}
		}
	}

	return out, nil
}

// precomputeGroupAggs recursively collects every aggregate FuncExpr that
// appears inside selectExprs (and optional extra expressions such as a HAVING
// clause), evaluates each against rows, and stores the result in a copy of
// firstRow keyed by the canonical SQL string. evalExpr will find these values
// when it encounters the same aggregate function expression.
func precomputeGroupAggs(db *DB, rows []Row, selectExprs sqlparser.SelectExprs, firstRow Row, extraExprs ...sqlparser.Expr) Row {
	aggRow := make(Row, len(firstRow)+8)
	for k, v := range firstRow {
		aggRow[k] = v
	}
	for _, se := range selectExprs {
		if ae, ok := se.(*sqlparser.AliasedExpr); ok {
			collectGroupAggs(db, ae.Expr, rows, aggRow)
		}
	}
	for _, expr := range extraExprs {
		if expr != nil {
			collectGroupAggs(db, expr, rows, aggRow)
		}
	}
	return aggRow
}

// collectGroupAggs walks expr, finds aggregate FuncExprs, evaluates each one,
// and stores the result in aggRow under the canonical SQL string key.
func collectGroupAggs(db *DB, expr sqlparser.Expr, rows []Row, aggRow Row) {
	if expr == nil {
		return
	}
	switch e := expr.(type) {
	case *sqlparser.FuncExpr:
		if isAggFunc(e.Name.Lowered()) {
			key := strings.ToLower(sqlparser.String(e))
			if _, already := aggRow[key]; !already {
				if v, err := evalAggFunc(db, e, rows); err == nil {
					aggRow[key] = v
				}
			}
			return // don't recurse into aggregate arguments
		}
		for _, arg := range e.Exprs {
			if ae, ok := arg.(*sqlparser.AliasedExpr); ok {
				collectGroupAggs(db, ae.Expr, rows, aggRow)
			}
		}
	case *sqlparser.BinaryExpr:
		collectGroupAggs(db, e.Left, rows, aggRow)
		collectGroupAggs(db, e.Right, rows, aggRow)
	case *sqlparser.ComparisonExpr:
		collectGroupAggs(db, e.Left, rows, aggRow)
		collectGroupAggs(db, e.Right, rows, aggRow)
	case *sqlparser.AndExpr:
		collectGroupAggs(db, e.Left, rows, aggRow)
		collectGroupAggs(db, e.Right, rows, aggRow)
	case *sqlparser.OrExpr:
		collectGroupAggs(db, e.Left, rows, aggRow)
		collectGroupAggs(db, e.Right, rows, aggRow)
	case *sqlparser.NotExpr:
		collectGroupAggs(db, e.Expr, rows, aggRow)
	case *sqlparser.ParenExpr:
		collectGroupAggs(db, e.Expr, rows, aggRow)
	case *sqlparser.CaseExpr:
		for _, when := range e.Whens {
			collectGroupAggs(db, when.Cond, rows, aggRow)
			collectGroupAggs(db, when.Val, rows, aggRow)
		}
		if e.Else != nil {
			collectGroupAggs(db, e.Else, rows, aggRow)
		}
	case *sqlparser.IsExpr:
		collectGroupAggs(db, e.Expr, rows, aggRow)
	case *sqlparser.RangeCond:
		collectGroupAggs(db, e.Left, rows, aggRow)
		collectGroupAggs(db, e.From, rows, aggRow)
		collectGroupAggs(db, e.To, rows, aggRow)
	}
}

func evalAggFunc(db *DB, fe *sqlparser.FuncExpr, rows []Row) (Value, error) {
	switch fe.Name.Lowered() {
	case "count":
		return aggCount(db, fe, rows)
	case "sum":
		return aggSum(db, fe, rows)
	case "avg":
		return aggAvg(db, fe, rows)
	case "min":
		return aggMin(db, fe, rows)
	case "max":
		return aggMax(db, fe, rows)
	case "array_agg":
		return aggArrayAgg(db, fe, rows)
	}
	return Null, fmt.Errorf("unknown aggregate function: %s", fe.Name.Lowered())
}

func aggCount(db *DB, fe *sqlparser.FuncExpr, rows []Row) (Value, error) {
	if aggIsStar(fe) {
		return Value{Kind: KindInt, V: int64(len(rows))}, nil
	}
	argExpr, err := aggArgExpr(fe)
	if err != nil {
		return Null, err
	}
	if fe.Distinct {
		seen := make(map[string]bool)
		for _, row := range rows {
			v, err := evalExpr(db, argExpr, row)
			if err != nil {
				return Null, err
			}
			if v.Kind == KindNull {
				continue
			}
			key := fmt.Sprintf("%T\x00%v", v.V, v.V)
			seen[key] = true
		}
		return Value{Kind: KindInt, V: int64(len(seen))}, nil
	}
	count := int64(0)
	for _, row := range rows {
		v, err := evalExpr(db, argExpr, row)
		if err != nil {
			return Null, err
		}
		if v.Kind != KindNull {
			count++
		}
	}
	return Value{Kind: KindInt, V: count}, nil
}

func aggSum(db *DB, fe *sqlparser.FuncExpr, rows []Row) (Value, error) {
	argExpr, err := aggArgExpr(fe)
	if err != nil {
		return Null, err
	}
	sum := float64(0)
	allInt := true
	any := false
	for _, row := range rows {
		v, err := evalExpr(db, argExpr, row)
		if err != nil {
			return Null, err
		}
		if v.Kind == KindNull {
			continue
		}
		if v.Kind != KindInt {
			allInt = false
		}
		sum += numericFloat(v)
		any = true
	}
	if !any {
		return Null, nil
	}
	if allInt {
		return Value{Kind: KindInt, V: int64(sum)}, nil
	}
	return Value{Kind: KindFloat, V: sum}, nil
}

func aggAvg(db *DB, fe *sqlparser.FuncExpr, rows []Row) (Value, error) {
	argExpr, err := aggArgExpr(fe)
	if err != nil {
		return Null, err
	}
	sum, count := float64(0), 0
	for _, row := range rows {
		v, err := evalExpr(db, argExpr, row)
		if err != nil {
			return Null, err
		}
		if v.Kind == KindNull {
			continue
		}
		sum += numericFloat(v)
		count++
	}
	if count == 0 {
		return Null, nil
	}
	return Value{Kind: KindFloat, V: sum / float64(count)}, nil
}

func aggMin(db *DB, fe *sqlparser.FuncExpr, rows []Row) (Value, error) {
	argExpr, err := aggArgExpr(fe)
	if err != nil {
		return Null, err
	}
	var minVal *Value
	for _, row := range rows {
		v, err := evalExpr(db, argExpr, row)
		if err != nil {
			return Null, err
		}
		if v.Kind == KindNull {
			continue
		}
		if minVal == nil || Compare(v, *minVal) < 0 {
			cp := v
			minVal = &cp
		}
	}
	if minVal == nil {
		return Null, nil
	}
	return *minVal, nil
}

func aggMax(db *DB, fe *sqlparser.FuncExpr, rows []Row) (Value, error) {
	argExpr, err := aggArgExpr(fe)
	if err != nil {
		return Null, err
	}
	var maxVal *Value
	for _, row := range rows {
		v, err := evalExpr(db, argExpr, row)
		if err != nil {
			return Null, err
		}
		if v.Kind == KindNull {
			continue
		}
		if maxVal == nil || Compare(v, *maxVal) > 0 {
			cp := v
			maxVal = &cp
		}
	}
	if maxVal == nil {
		return Null, nil
	}
	return *maxVal, nil
}

// aggArrayAgg collects all non-NULL values of its argument into a JSON array
// (KindJSON with []any underlying type). NULL values are silently skipped.
// When no rows produce a non-NULL value, NULL is returned (PostgreSQL semantics).
func aggArrayAgg(db *DB, fe *sqlparser.FuncExpr, rows []Row) (Value, error) {
	argExpr, err := aggArgExpr(fe)
	if err != nil {
		return Null, err
	}
	var arr []any
	for _, row := range rows {
		v, err := evalExpr(db, argExpr, row)
		if err != nil {
			return Null, err
		}
		if v.Kind == KindNull {
			continue
		}
		arr = append(arr, v.V)
	}
	if arr == nil {
		return Null, nil
	}
	return Value{Kind: KindJSON, V: arr}, nil
}

func aggIsStar(fe *sqlparser.FuncExpr) bool {
	if len(fe.Exprs) != 1 {
		return false
	}
	_, ok := fe.Exprs[0].(*sqlparser.StarExpr)
	return ok
}

func aggArgExpr(fe *sqlparser.FuncExpr) (sqlparser.Expr, error) {
	if len(fe.Exprs) == 0 {
		return nil, fmt.Errorf("aggregate %s() requires an argument", fe.Name.Lowered())
	}
	ae, ok := fe.Exprs[0].(*sqlparser.AliasedExpr)
	if !ok {
		return nil, fmt.Errorf("unsupported argument type %T in aggregate", fe.Exprs[0])
	}
	return ae.Expr, nil
}

func isAggFunc(name string) bool {
	switch name {
	case "count", "sum", "avg", "min", "max", "array_agg":
		return true
	}
	return false
}

func selectHasAggregates(exprs sqlparser.SelectExprs) bool {
	for _, se := range exprs {
		ae, ok := se.(*sqlparser.AliasedExpr)
		if !ok {
			continue
		}
		if exprContainsAgg(ae.Expr) {
			return true
		}
	}
	return false
}

func exprContainsAgg(expr sqlparser.Expr) bool {
	switch e := expr.(type) {
	case *sqlparser.FuncExpr:
		if isAggFunc(e.Name.Lowered()) {
			return true
		}
		// Non-aggregate function: recurse into its arguments.
		for _, arg := range e.Exprs {
			if ae, ok := arg.(*sqlparser.AliasedExpr); ok {
				if exprContainsAgg(ae.Expr) {
					return true
				}
			}
		}
		return false
	case *sqlparser.BinaryExpr:
		return exprContainsAgg(e.Left) || exprContainsAgg(e.Right)
	case *sqlparser.ParenExpr:
		return exprContainsAgg(e.Expr)
	}
	return false
}
