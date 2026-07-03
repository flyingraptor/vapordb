package vapordb

import (
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/xwb1989/sqlparser"
)

// CAST / CONVERT value coercion.

func evalConvertExpr(db *DB, e *sqlparser.ConvertExpr, row Row) (Value, error) {
	v, err := evalExpr(db, e.Expr, row)
	if err != nil {
		return Null, err
	}
	if v.Kind == KindNull || e.Type == nil {
		return v, nil
	}
	return castValueToConvertType(v, e.Type)
}

// castValueToConvertType implements CAST(expr AS type) / CONVERT(expr, type)
// for the common targets used in portable SQL.
func castValueToConvertType(v Value, ct *sqlparser.ConvertType) (Value, error) {
	raw := strings.ToLower(strings.TrimSpace(ct.Type))

	if strings.Contains(raw, "unsigned") {
		return castSQLInt(v, true)
	}
	if strings.Contains(raw, "bool") {
		return castSQLBool(v), nil
	}
	if strings.Contains(raw, "double") || strings.Contains(raw, "float") || raw == "real" ||
		strings.HasPrefix(raw, "decimal") || strings.HasPrefix(raw, "numeric") {
		return castSQLFloat(v)
	}
	if strings.Contains(raw, "char") || raw == "text" || strings.HasPrefix(raw, "nchar") ||
		strings.HasPrefix(raw, "nvarchar") || strings.HasPrefix(raw, "binary") {
		return Value{Kind: KindString, V: valueString(v)}, nil
	}
	if strings.Contains(raw, "datetime") || strings.Contains(raw, "timestamp") {
		return castSQLDateTime(v)
	}
	if raw == "date" || strings.HasPrefix(raw, "date(") {
		return castSQLDate(v)
	}
	if raw == "time" || strings.HasPrefix(raw, "time(") {
		return castSQLTime(v)
	}
	if strings.Contains(raw, "int") || strings.Contains(raw, "signed") || raw == "integer" ||
		raw == "bigint" || raw == "smallint" || raw == "tinyint" {
		return castSQLInt(v, false)
	}
	if raw == "json" || raw == "jsonb" {
		if v.Kind == KindJSON {
			return v, nil
		}
		if v.Kind == KindString {
			jv, err := parseJSONValue(v.V.(string))
			if err != nil {
				return Null, fmt.Errorf("CAST … AS JSON: %w", err)
			}
			return jv, nil
		}
		if v.Kind == KindNull {
			return Null, nil
		}
		return Null, fmt.Errorf("CAST/CONVERT: cannot convert %v to JSON", v.Kind)
	}
	return Null, fmt.Errorf("CAST/CONVERT: unsupported type %q", ct.Type)
}

func castSQLInt(v Value, unsigned bool) (Value, error) {
	if v.Kind == KindNull {
		return Null, nil
	}
	var n int64
	switch v.Kind {
	case KindInt:
		n = v.V.(int64)
	case KindFloat:
		n = int64(v.V.(float64))
	case KindBool:
		if v.V.(bool) {
			n = 1
		}
	case KindString:
		s := strings.TrimSpace(v.V.(string))
		if unsigned {
			u, err := strconv.ParseUint(s, 10, 64)
			if err != nil {
				n = 0
			} else {
				n = int64(u)
			}
		} else {
			var err error
			n, err = strconv.ParseInt(s, 10, 64)
			if err != nil {
				n = 0
			}
		}
	case KindDate:
		t := v.V.(time.Time).UTC()
		n = int64(t.Year())*10000 + int64(t.Month())*100 + int64(t.Day())
	default:
		n = 0
	}
	if unsigned && n < 0 {
		n = 0
	}
	return Value{Kind: KindInt, V: n}, nil
}

func castSQLFloat(v Value) (Value, error) {
	if v.Kind == KindNull {
		return Null, nil
	}
	switch v.Kind {
	case KindFloat:
		return v, nil
	case KindInt:
		return Value{Kind: KindFloat, V: float64(v.V.(int64))}, nil
	case KindBool:
		if v.V.(bool) {
			return Value{Kind: KindFloat, V: 1}, nil
		}
		return Value{Kind: KindFloat, V: 0}, nil
	case KindString:
		f, err := strconv.ParseFloat(strings.TrimSpace(v.V.(string)), 64)
		if err != nil {
			return Value{Kind: KindFloat, V: 0}, nil
		}
		return Value{Kind: KindFloat, V: f}, nil
	case KindDate:
		return Value{Kind: KindFloat, V: float64(v.V.(time.Time).Unix())}, nil
	default:
		return Value{Kind: KindFloat, V: 0}, nil
	}
}

func castSQLBool(v Value) Value {
	if v.Kind == KindNull {
		return Null
	}
	return Value{Kind: KindBool, V: isTruthy(v)}
}

func castSQLDate(v Value) (Value, error) {
	if v.Kind == KindNull {
		return Null, nil
	}
	switch v.Kind {
	case KindDate:
		t := v.V.(time.Time).UTC()
		trunc := time.Date(t.Year(), t.Month(), t.Day(), 0, 0, 0, 0, time.UTC)
		return Value{Kind: KindDate, V: trunc}, nil
	case KindString:
		if t, ok := tryParseDate(strings.TrimSpace(v.V.(string))); ok {
			utc := t.UTC()
			trunc := time.Date(utc.Year(), utc.Month(), utc.Day(), 0, 0, 0, 0, time.UTC)
			return Value{Kind: KindDate, V: trunc}, nil
		}
		return Null, nil
	default:
		return Null, nil
	}
}

func castSQLDateTime(v Value) (Value, error) {
	if v.Kind == KindNull {
		return Null, nil
	}
	switch v.Kind {
	case KindDate:
		return v, nil
	case KindString:
		if t, ok := tryParseDate(strings.TrimSpace(v.V.(string))); ok {
			return Value{Kind: KindDate, V: t.UTC()}, nil
		}
		return Null, nil
	default:
		return Null, nil
	}
}

func castSQLTime(v Value) (Value, error) {
	if v.Kind == KindNull {
		return Null, nil
	}
	clock := func(t time.Time) Value {
		u := t.UTC()
		ref := time.Date(2000, 1, 1, u.Hour(), u.Minute(), u.Second(), u.Nanosecond(), time.UTC)
		return Value{Kind: KindDate, V: ref}
	}
	switch v.Kind {
	case KindString:
		s := strings.TrimSpace(v.V.(string))
		for _, f := range []string{"15:04:05", "15:04"} {
			if t, err := time.Parse(f, s); err == nil {
				return clock(t), nil
			}
		}
		if t, ok := tryParseDate(s); ok {
			return clock(t), nil
		}
		return Null, nil
	case KindDate:
		return clock(v.V.(time.Time)), nil
	default:
		return Null, nil
	}
}

// resolveColumn looks up a column in the working row, handling qualified names.
