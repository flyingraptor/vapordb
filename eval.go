package vapordb

import (
	"fmt"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/xwb1989/sqlparser"
)

// Expression evaluation: evalExpr and comparison/logical/arithmetic helpers.

func evalExprWithDB(db *DB, expr sqlparser.Expr, row Row) (Value, error) {
	return evalExpr(db, expr, row)
}

// evalBoolWithDB evaluates a boolean expression, handling ExistsExpr at any
// depth inside AND / OR / NOT / parentheses. Falls through to evalBool for
// subtrees that contain no EXISTS.
func evalBoolWithDB(db *DB, expr sqlparser.Expr, row Row) (bool, error) {
	switch e := expr.(type) {
	case *sqlparser.ExistsExpr:
		rows, err := execSelectCorrelated(db, e, row)
		return len(rows) > 0, err
	case *sqlparser.AndExpr:
		left, err := evalBoolWithDB(db, e.Left, row)
		if err != nil || !left {
			return left, err
		}
		return evalBoolWithDB(db, e.Right, row)
	case *sqlparser.OrExpr:
		v, err := evalOrPipe(db, e, row)
		if err != nil {
			return false, err
		}
		return isTruthy(v), nil
	case *sqlparser.NotExpr:
		v, err := evalBoolWithDB(db, e.Expr, row)
		return !v, err
	case *sqlparser.ParenExpr:
		return evalBoolWithDB(db, e.Expr, row)
	}
	return evalBool(db, expr, row)
}

// ─── EXPRESSION EVALUATION ───────────────────────────────────────────────────

// evalBool evaluates an expression and returns its boolean truth value.
func evalBool(db *DB, expr sqlparser.Expr, row Row) (bool, error) {
	val, err := evalExpr(db, expr, row)
	if err != nil {
		return false, err
	}
	return isTruthy(val), nil
}

func isTruthy(v Value) bool {
	switch v.Kind {
	case KindNull:
		return false
	case KindBool:
		return v.V.(bool)
	case KindInt:
		return v.V.(int64) != 0
	case KindFloat:
		return v.V.(float64) != 0
	case KindString:
		return v.V.(string) != ""
	case KindDate:
		if t, ok := v.V.(time.Time); ok {
			return !t.IsZero()
		}
	case KindJSON:
		return v.V != nil
	}
	return false
}

// evalExpr evaluates a SQL expression against a working row and returns a Value.
func evalExpr(db *DB, expr sqlparser.Expr, row Row) (Value, error) {
	if row == nil {
		row = Row{}
	}
	switch e := expr.(type) {
	case *sqlparser.NullVal:
		return Null, nil

	case sqlparser.BoolVal:
		return Value{Kind: KindBool, V: bool(e)}, nil

	case *sqlparser.SQLVal:
		return parseSQLVal(e)

	case *sqlparser.ColName:
		v, _ := resolveColumn(e, row)
		return v, nil

	case *sqlparser.ExistsExpr:
		rows, err := execSelectCorrelated(db, e, row)
		if err != nil {
			return Null, err
		}
		return Value{Kind: KindBool, V: len(rows) > 0}, nil

	case *sqlparser.Subquery:
		// Scalar subquery: (SELECT expr FROM …) — must produce 0 or 1 rows.
		inner, ok := e.Select.(*sqlparser.Select)
		if !ok {
			return Null, fmt.Errorf("scalar subquery: UNION is not supported")
		}
		return execScalarSubquery(db, inner, row)

	case *sqlparser.ComparisonExpr:
		return evalComparison(db, e, row)

	case *sqlparser.AndExpr:
		left, err := evalExpr(db, e.Left, row)
		if err != nil {
			return Null, err
		}
		if !isTruthy(left) {
			return Value{Kind: KindBool, V: false}, nil
		}
		right, err := evalExpr(db, e.Right, row)
		if err != nil {
			return Null, err
		}
		return Value{Kind: KindBool, V: isTruthy(right)}, nil

	case *sqlparser.OrExpr:
		return evalOrPipe(db, e, row)

	case *sqlparser.NotExpr:
		val, err := evalExpr(db, e.Expr, row)
		if err != nil {
			return Null, err
		}
		return Value{Kind: KindBool, V: !isTruthy(val)}, nil

	case *sqlparser.IsExpr:
		return evalIsExpr(db, e, row)

	case *sqlparser.ParenExpr:
		return evalExpr(db, e.Expr, row)

	case *sqlparser.BinaryExpr:
		return evalBinaryArith(db, e, row)

	case *sqlparser.UnaryExpr:
		val, err := evalExpr(db, e.Expr, row)
		if err != nil {
			return Null, err
		}
		switch e.Operator {
		case "-":
			switch x := val.V.(type) {
			case int64:
				return Value{Kind: KindInt, V: -x}, nil
			case float64:
				return Value{Kind: KindFloat, V: -x}, nil
			}
		case "!":
			return Value{Kind: KindBool, V: !isTruthy(val)}, nil
		}
		return val, nil

	case *sqlparser.FuncExpr:
		// If the aggregate was already computed and stored in the row (HAVING context),
		// return the pre-computed value instead of re-evaluating.
		if isAggFunc(e.Name.Lowered()) {
			key := strings.ToLower(sqlparser.String(e))
			if v, ok := row[key]; ok {
				return v, nil
			}
			return Null, nil
		}
		return evalScalarFunc(db, e, row)

	case *sqlparser.RangeCond:
		return evalRangeCond(db, e, row)

	case *sqlparser.CaseExpr:
		return evalCaseExpr(db, e, row)

	case *sqlparser.IntervalExpr:
		// INTERVAL n UNIT — evaluated to a string "n UNIT" consumed by DATE_ADD/DATE_SUB.
		v, err := evalExpr(db, e.Expr, row)
		if err != nil {
			return Null, err
		}
		return Value{Kind: KindString, V: valueString(v) + " " + strings.ToUpper(e.Unit)}, nil

	case *sqlparser.ConvertExpr:
		return evalConvertExpr(db, e, row)

	case *sqlparser.ConvertUsingExpr:
		v, err := evalExpr(db, e.Expr, row)
		if err != nil {
			return Null, err
		}
		if v.Kind == KindNull {
			return Null, nil
		}
		return Value{Kind: KindString, V: valueString(v)}, nil

	case sqlparser.ValTuple:
		// ValTuples appear as the RHS of IN; they should not be evaluated standalone.
		return Null, fmt.Errorf("unexpected ValTuple in scalar context")

	default:
		return Null, fmt.Errorf("unsupported expression type: %T", expr)
	}
}

func resolveColumn(col *sqlparser.ColName, row Row) (Value, bool) {
	qualifier := col.Qualifier.Name.String()
	name := col.Name.Lowered()

	if qualifier != "" {
		// Exact qualified lookup: "alias.col"
		if v, ok := row[qualifier+"."+name]; ok {
			return v, true
		}
		// Bare name fallback (column stored without alias, e.g. in a derived table).
		if v, ok := row[name]; ok {
			return v, true
		}
		// Suffix search only for a qualified reference: accept keys where the
		// qualifier part itself matches (handles "tableName.col" when written as
		// "alias.col" where alias == tableName). Do NOT accept keys from other
		// tables — that would shadow e.g. a null-padded left row with a value
		// from the right row in an outer join.
		suffix := "." + qualifier + "." + name // e.g. ".employees.id" for "e.id"
		for k, v := range row {
			if strings.HasSuffix(k, suffix) {
				return v, true
			}
		}
		return Null, false
	}

	// Unqualified reference: check bare name then any "alias.col" key.
	if v, ok := row[name]; ok {
		return v, true
	}
	suffix := "." + name
	for k, v := range row {
		if strings.HasSuffix(k, suffix) {
			return v, true
		}
	}
	return Null, false
}

func parseSQLVal(sv *sqlparser.SQLVal) (Value, error) {
	switch sv.Type {
	case sqlparser.IntVal:
		n, err := strconv.ParseInt(string(sv.Val), 10, 64)
		if err != nil {
			f, err2 := strconv.ParseFloat(string(sv.Val), 64)
			if err2 != nil {
				return Null, err
			}
			return Value{Kind: KindInt, V: int64(f)}, nil
		}
		return Value{Kind: KindInt, V: n}, nil

	case sqlparser.FloatVal:
		f, err := strconv.ParseFloat(string(sv.Val), 64)
		if err != nil {
			return Null, err
		}
		return Value{Kind: KindFloat, V: f}, nil

	case sqlparser.StrVal:
		return Value{Kind: KindString, V: string(sv.Val)}, nil

	case sqlparser.HexVal:
		n, err := strconv.ParseInt(string(sv.Val), 16, 64)
		if err != nil {
			return Value{Kind: KindString, V: string(sv.Val)}, nil
		}
		return Value{Kind: KindInt, V: n}, nil

	case sqlparser.BitVal:
		n, err := strconv.ParseInt(string(sv.Val), 2, 64)
		if err != nil {
			return Null, err
		}
		return Value{Kind: KindInt, V: n}, nil

	default:
		return Value{Kind: KindString, V: string(sv.Val)}, nil
	}
}

func evalComparison(db *DB, e *sqlparser.ComparisonExpr, row Row) (Value, error) {
	op := e.Operator

	// IN / NOT IN
	if op == "in" || op == "not in" {
		left, err := evalExpr(db, e.Left, row)
		if err != nil {
			return Null, err
		}
		switch rhs := e.Right.(type) {
		case sqlparser.ValTuple:
			for _, item := range rhs {
				val, err := evalExpr(db, item, row)
				if err != nil {
					return Null, err
				}
				if Equal(left, val) {
					return Value{Kind: KindBool, V: op == "in"}, nil
				}
			}
			return Value{Kind: KindBool, V: op == "not in"}, nil
		case *sqlparser.Subquery:
			subRows, err := execSelectForIn(db, rhs, row)
			if err != nil {
				return Null, err
			}
			for _, pr := range subRows {
				val, err := projectedSingleColumn(pr)
				if err != nil {
					return Null, err
				}
				if Equal(left, val) {
					return Value{Kind: KindBool, V: op == "in"}, nil
				}
			}
			return Value{Kind: KindBool, V: op == "not in"}, nil
		default:
			return Null, fmt.Errorf("expected value list or subquery for IN, got %T", e.Right)
		}
	}

	left, err := evalExpr(db, e.Left, row)
	if err != nil {
		return Null, err
	}
	right, err := evalExpr(db, e.Right, row)
	if err != nil {
		return Null, err
	}

	// LIKE / NOT LIKE — NULL on either side propagates to false.
	if op == "like" || op == "not like" {
		if left.Kind == KindNull || right.Kind == KindNull {
			return Value{Kind: KindBool, V: false}, nil
		}
		pat := valueString(right)
		val := valueString(left)
		var match bool
		if e.Escape != nil {
			escVal, err := evalExpr(db, e.Escape, row)
			if err != nil {
				return Null, err
			}
			if escVal.Kind == KindNull {
				return Value{Kind: KindBool, V: false}, nil
			}
			escStr := valueString(escVal)
			if escStr == "" {
				// ESCAPE '' — treat as no escape character (PostgreSQL-compatible).
				match = LikeMatch(pat, val)
			} else if utf8.RuneCountInString(escStr) != 1 {
				return Null, fmt.Errorf("LIKE ESCAPE must be exactly one character")
			} else {
				escR, _ := utf8.DecodeRuneInString(escStr)
				match = LikeMatchEscape(pat, val, escR)
			}
		} else {
			match = LikeMatch(pat, val)
		}
		if op == "not like" {
			match = !match
		}
		return Value{Kind: KindBool, V: match}, nil
	}

	// NULL propagation for comparison operators
	if left.Kind == KindNull || right.Kind == KindNull {
		return Value{Kind: KindBool, V: false}, nil
	}

	cmp := Compare(left, right)
	var result bool
	switch op {
	case "=", "<=>":
		result = cmp == 0
	case "!=", "<>":
		result = cmp != 0
	case "<":
		result = cmp < 0
	case ">":
		result = cmp > 0
	case "<=":
		result = cmp <= 0
	case ">=":
		result = cmp >= 0
	default:
		return Null, fmt.Errorf("unsupported comparison operator: %s", op)
	}
	return Value{Kind: KindBool, V: result}, nil
}

func evalIsExpr(db *DB, e *sqlparser.IsExpr, row Row) (Value, error) {
	val, err := evalExpr(db, e.Expr, row)
	if err != nil {
		return Null, err
	}
	var result bool
	switch e.Operator {
	case "is null":
		result = val.Kind == KindNull
	case "is not null":
		result = val.Kind != KindNull
	case "is true":
		result = isTruthy(val)
	case "is false":
		result = !isTruthy(val)
	case "is not true":
		result = !isTruthy(val)
	case "is not false":
		result = isTruthy(val)
	default:
		return Null, fmt.Errorf("unsupported IS expression: %s", e.Operator)
	}
	return Value{Kind: KindBool, V: result}, nil
}

// combinePipeOr implements PostgreSQL-style ||: concatenate when either operand
// is a string; otherwise boolean OR (short-circuit truthiness on the left).
func combinePipeOr(left, right Value) Value {
	if left.Kind == KindNull || right.Kind == KindNull {
		return Null
	}
	if left.Kind == KindString || right.Kind == KindString {
		return Value{Kind: KindString, V: valueString(left) + valueString(right)}
	}
	if isTruthy(left) {
		return Value{Kind: KindBool, V: true}
	}
	return Value{Kind: KindBool, V: isTruthy(right)}
}

func evalOrPipe(db *DB, e *sqlparser.OrExpr, row Row) (Value, error) {
	left, err := evalExpr(db, e.Left, row)
	if err != nil {
		return Null, err
	}
	right, err := evalExpr(db, e.Right, row)
	if err != nil {
		return Null, err
	}
	return combinePipeOr(left, right), nil
}

func evalBinaryArith(db *DB, e *sqlparser.BinaryExpr, row Row) (Value, error) {
	left, err := evalExpr(db, e.Left, row)
	if err != nil {
		return Null, err
	}
	right, err := evalExpr(db, e.Right, row)
	if err != nil {
		return Null, err
	}
	if left.Kind == KindNull || right.Kind == KindNull {
		return Null, nil
	}

	// Parser may emit `||` as BinaryExpr in some contexts; match OrExpr / pipe rules.
	if e.Operator == "||" {
		return combinePipeOr(left, right), nil
	}

	// String concatenation with +
	if e.Operator == "+" && (left.Kind == KindString || right.Kind == KindString) {
		return Value{Kind: KindString, V: valueString(left) + valueString(right)}, nil
	}

	lf, rf := numericFloat(left), numericFloat(right)
	var result float64
	switch e.Operator {
	case "+":
		result = lf + rf
	case "-":
		result = lf - rf
	case "*":
		result = lf * rf
	case "/":
		if rf == 0 {
			return Null, nil
		}
		result = lf / rf
	case "div":
		if rf == 0 {
			return Null, nil
		}
		return Value{Kind: KindInt, V: int64(lf / rf)}, nil
	case "%":
		if rf == 0 {
			return Null, nil
		}
		return Value{Kind: KindInt, V: int64(lf) % int64(rf)}, nil
	default:
		return Null, fmt.Errorf("unsupported binary operator: %s", e.Operator)
	}
	if left.Kind == KindInt && right.Kind == KindInt && e.Operator != "/" {
		return Value{Kind: KindInt, V: int64(result)}, nil
	}
	return Value{Kind: KindFloat, V: result}, nil
}

func evalRangeCond(db *DB, e *sqlparser.RangeCond, row Row) (Value, error) {
	val, err := evalExpr(db, e.Left, row)
	if err != nil {
		return Null, err
	}
	// NULL on the tested value always produces false (unknown → not matched).
	if val.Kind == KindNull {
		return Value{Kind: KindBool, V: false}, nil
	}
	from, err := evalExpr(db, e.From, row)
	if err != nil {
		return Null, err
	}
	to, err := evalExpr(db, e.To, row)
	if err != nil {
		return Null, err
	}
	inRange := Compare(val, from) >= 0 && Compare(val, to) <= 0
	if e.Operator == "not between" {
		inRange = !inRange
	}
	return Value{Kind: KindBool, V: inRange}, nil
}

func evalCaseExpr(db *DB, e *sqlparser.CaseExpr, row Row) (Value, error) {
	var base *Value
	if e.Expr != nil {
		v, err := evalExpr(db, e.Expr, row)
		if err != nil {
			return Null, err
		}
		base = &v
	}
	for _, when := range e.Whens {
		cond, err := evalExpr(db, when.Cond, row)
		if err != nil {
			return Null, err
		}
		matched := false
		if base != nil {
			matched = Equal(*base, cond)
		} else {
			matched = isTruthy(cond)
		}
		if matched {
			return evalExpr(db, when.Val, row)
		}
	}
	if e.Else != nil {
		return evalExpr(db, e.Else, row)
	}
	return Null, nil
}
