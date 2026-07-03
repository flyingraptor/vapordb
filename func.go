package vapordb

import (
	"fmt"
	"math"
	"strconv"
	"strings"
	"time"

	"github.com/xwb1989/sqlparser"
)

// Scalar function evaluation (evalScalarFunc) and date/interval helpers.

func evalScalarFunc(db *DB, e *sqlparser.FuncExpr, row Row) (Value, error) {
	name := e.Name.Lowered()

	args := make([]Value, 0, len(e.Exprs))
	for _, se := range e.Exprs {
		ae, ok := se.(*sqlparser.AliasedExpr)
		if !ok {
			continue
		}
		v, err := evalExpr(db, ae.Expr, row)
		if err != nil {
			return Null, err
		}
		args = append(args, v)
	}

	switch name {
	case "upper":
		if len(args) < 1 || args[0].Kind == KindNull {
			return Null, nil
		}
		return Value{Kind: KindString, V: strings.ToUpper(valueString(args[0]))}, nil

	case "lower":
		if len(args) < 1 || args[0].Kind == KindNull {
			return Null, nil
		}
		return Value{Kind: KindString, V: strings.ToLower(valueString(args[0]))}, nil

	case "length", "char_length", "character_length":
		if len(args) < 1 || args[0].Kind == KindNull {
			return Null, nil
		}
		return Value{Kind: KindInt, V: int64(len(valueString(args[0])))}, nil

	case "concat":
		var sb strings.Builder
		for _, a := range args {
			if a.Kind == KindNull {
				return Null, nil
			}
			sb.WriteString(valueString(a))
		}
		return Value{Kind: KindString, V: sb.String()}, nil

	case "coalesce":
		for _, a := range args {
			if a.Kind != KindNull {
				return a, nil
			}
		}
		return Null, nil

	case "ifnull", "nvl":
		if len(args) < 2 {
			return Null, nil
		}
		if args[0].Kind != KindNull {
			return args[0], nil
		}
		return args[1], nil

	case "nullif":
		if len(args) < 2 {
			return Null, nil
		}
		if Equal(args[0], args[1]) {
			return Null, nil
		}
		return args[0], nil

	case "abs":
		if len(args) < 1 || args[0].Kind == KindNull {
			return Null, nil
		}
		switch x := args[0].V.(type) {
		case int64:
			if x < 0 {
				return Value{Kind: KindInt, V: -x}, nil
			}
		case float64:
			if x < 0 {
				return Value{Kind: KindFloat, V: -x}, nil
			}
		}
		return args[0], nil

	case "round":
		if len(args) < 1 || args[0].Kind == KindNull {
			return Null, nil
		}
		f := numericFloat(args[0])
		if len(args) >= 2 {
			decimals := numericFloat(args[1])
			factor := math.Pow(10, decimals)
			return Value{Kind: KindFloat, V: math.Round(f*factor) / factor}, nil
		}
		return Value{Kind: KindInt, V: int64(math.Round(f))}, nil

	case "floor":
		if len(args) < 1 || args[0].Kind == KindNull {
			return Null, nil
		}
		return Value{Kind: KindInt, V: int64(math.Floor(numericFloat(args[0])))}, nil

	case "ceil", "ceiling":
		if len(args) < 1 || args[0].Kind == KindNull {
			return Null, nil
		}
		return Value{Kind: KindInt, V: int64(math.Ceil(numericFloat(args[0])))}, nil

	case "cast", "convert":
		if len(args) < 1 {
			return Null, nil
		}
		return args[0], nil

	// ── Date / time functions ────────────────────────────────────────────────

	case "now", "sysdate", "current_timestamp":
		return Value{Kind: KindDate, V: time.Now()}, nil

	case "curdate", "current_date":
		t := time.Now().UTC()
		return Value{Kind: KindDate, V: time.Date(t.Year(), t.Month(), t.Day(), 0, 0, 0, 0, time.UTC)}, nil

	case "curtime", "current_time":
		return Value{Kind: KindDate, V: time.Now()}, nil

	case "date":
		// DATE(expr) — truncate a datetime to date-only (midnight UTC) or parse
		// a string literal as a date.
		if len(args) < 1 || args[0].Kind == KindNull {
			return Null, nil
		}
		t := valueToTime(args[0])
		return Value{Kind: KindDate, V: time.Date(t.Year(), t.Month(), t.Day(), 0, 0, 0, 0, time.UTC)}, nil

	case "datetime", "timestamp":
		// DATETIME(expr) / TIMESTAMP(expr) — parse a string or date value as a
		// datetime, preserving the time-of-day component.
		if len(args) < 1 || args[0].Kind == KindNull {
			return Null, nil
		}
		t := valueToTime(args[0])
		return Value{Kind: KindDate, V: t.UTC()}, nil

	case "year":
		if len(args) < 1 || args[0].Kind == KindNull {
			return Null, nil
		}
		return Value{Kind: KindInt, V: int64(valueToTime(args[0]).Year())}, nil

	case "month":
		if len(args) < 1 || args[0].Kind == KindNull {
			return Null, nil
		}
		return Value{Kind: KindInt, V: int64(valueToTime(args[0]).Month())}, nil

	case "day", "dayofmonth":
		if len(args) < 1 || args[0].Kind == KindNull {
			return Null, nil
		}
		return Value{Kind: KindInt, V: int64(valueToTime(args[0]).Day())}, nil

	case "hour":
		if len(args) < 1 || args[0].Kind == KindNull {
			return Null, nil
		}
		return Value{Kind: KindInt, V: int64(valueToTime(args[0]).Hour())}, nil

	case "minute":
		if len(args) < 1 || args[0].Kind == KindNull {
			return Null, nil
		}
		return Value{Kind: KindInt, V: int64(valueToTime(args[0]).Minute())}, nil

	case "second":
		if len(args) < 1 || args[0].Kind == KindNull {
			return Null, nil
		}
		return Value{Kind: KindInt, V: int64(valueToTime(args[0]).Second())}, nil

	case "weekday":
		// Returns 0=Monday … 6=Sunday (MySQL convention).
		if len(args) < 1 || args[0].Kind == KindNull {
			return Null, nil
		}
		wd := valueToTime(args[0]).Weekday() // Sunday=0 in Go
		mysql := int64((int(wd) + 6) % 7)    // convert to Monday=0
		return Value{Kind: KindInt, V: mysql}, nil

	case "dayofweek":
		// Returns 1=Sunday … 7=Saturday (MySQL convention).
		if len(args) < 1 || args[0].Kind == KindNull {
			return Null, nil
		}
		return Value{Kind: KindInt, V: int64(valueToTime(args[0]).Weekday()) + 1}, nil

	case "datediff":
		// DATEDIFF(d1, d2) → number of days from d2 to d1.
		if len(args) < 2 {
			return Null, nil
		}
		if args[0].Kind == KindNull || args[1].Kind == KindNull {
			return Null, nil
		}
		t1 := valueToTime(args[0])
		t2 := valueToTime(args[1])
		d1 := time.Date(t1.Year(), t1.Month(), t1.Day(), 0, 0, 0, 0, time.UTC)
		d2 := time.Date(t2.Year(), t2.Month(), t2.Day(), 0, 0, 0, 0, time.UTC)
		days := int64(d1.Sub(d2).Hours() / 24)
		return Value{Kind: KindInt, V: days}, nil

	case "timestampdiff":
		// TIMESTAMPDIFF(unit, d1, d2) → integer difference in the given unit.
		// The unit is passed as a bare identifier (SECOND, MINUTE, HOUR, DAY, MONTH, YEAR).
		// sqlparser wraps it in a string value, so args[0] is a KindString.
		if len(args) < 3 {
			return Null, nil
		}
		if args[1].Kind == KindNull || args[2].Kind == KindNull {
			return Null, nil
		}
		unit := strings.ToUpper(valueString(args[0]))
		t1 := valueToTime(args[1])
		t2 := valueToTime(args[2])
		diff := t2.Sub(t1)
		var result int64
		switch unit {
		case "SECOND":
			result = int64(diff.Seconds())
		case "MINUTE":
			result = int64(diff.Minutes())
		case "HOUR":
			result = int64(diff.Hours())
		case "DAY":
			result = int64(diff.Hours() / 24)
		case "MONTH":
			years := t2.Year() - t1.Year()
			months := int(t2.Month()) - int(t1.Month())
			result = int64(years*12 + months)
		case "YEAR":
			result = int64(t2.Year() - t1.Year())
		default:
			return Null, fmt.Errorf("unsupported TIMESTAMPDIFF unit: %s", unit)
		}
		return Value{Kind: KindInt, V: result}, nil

	case "date_format":
		// DATE_FORMAT(date, format) — formats a date using MySQL format specifiers.
		if len(args) < 2 || args[0].Kind == KindNull || args[1].Kind == KindNull {
			return Null, nil
		}
		t := valueToTime(args[0])
		goFmt := mysqlFormatToGo(valueString(args[1]))
		return Value{Kind: KindString, V: t.UTC().Format(goFmt)}, nil

	case "date_add", "adddate":
		// DATE_ADD(date, INTERVAL n unit)
		// sqlparser passes the interval value as a string like "1 DAY".
		if len(args) < 2 || args[0].Kind == KindNull || args[1].Kind == KindNull {
			return Null, nil
		}
		t := valueToTime(args[0])
		result, err := applyInterval(t, valueString(args[1]), false)
		if err != nil {
			return Null, err
		}
		return Value{Kind: KindDate, V: result}, nil

	case "date_sub", "subdate":
		if len(args) < 2 || args[0].Kind == KindNull || args[1].Kind == KindNull {
			return Null, nil
		}
		t := valueToTime(args[0])
		result, err := applyInterval(t, valueString(args[1]), true)
		if err != nil {
			return Null, err
		}
		return Value{Kind: KindDate, V: result}, nil

	// ── JSON functions ────────────────────────────────────────────────────────

	case "json_extract":
		if len(args) < 2 {
			return Null, nil
		}
		path := valueString(args[1])
		return jsonExtract(args[0], path), nil

	case "json_unquote":
		if len(args) < 1 {
			return Null, nil
		}
		return jsonUnquote(args[0]), nil

	case "json_contains":
		if len(args) < 2 {
			return Null, nil
		}
		if jsonContainsCheck(args[0], args[1]) {
			return Value{Kind: KindBool, V: true}, nil
		}
		return Value{Kind: KindBool, V: false}, nil

	case "json_parse":
		if len(args) < 1 || args[0].Kind == KindNull {
			return Null, nil
		}
		v, err := parseJSONValue(valueString(args[0]))
		if err != nil {
			return Null, err
		}
		return v, nil

	case "json_array_length":
		if len(args) < 1 {
			return Null, nil
		}
		return jsonArrayLength(args[0]), nil

	case "json_keys":
		if len(args) < 1 {
			return Null, nil
		}
		return jsonKeys(args[0]), nil

	case "json_type":
		if len(args) < 1 || args[0].Kind == KindNull {
			return Null, nil
		}
		jv := args[0]
		if jv.Kind == KindJSON {
			switch jv.V.(type) {
			case map[string]any:
				return Value{Kind: KindString, V: "OBJECT"}, nil
			case []any:
				return Value{Kind: KindString, V: "ARRAY"}, nil
			default:
				return Value{Kind: KindString, V: "SCALAR"}, nil
			}
		}
		switch jv.Kind {
		case KindString:
			return Value{Kind: KindString, V: "STRING"}, nil
		case KindInt, KindFloat:
			return Value{Kind: KindString, V: "INTEGER"}, nil
		case KindBool:
			return Value{Kind: KindString, V: "BOOLEAN"}, nil
		case KindDate:
			return Value{Kind: KindString, V: "STRING"}, nil
		}
		return Null, nil
	}

	return Null, fmt.Errorf("unknown function: %s", name)
}

// mysqlFormatToGo converts a MySQL DATE_FORMAT format string to a Go time format.
func mysqlFormatToGo(format string) string {
	r := strings.NewReplacer(
		"%Y", "2006",
		"%y", "06",
		"%m", "01",
		"%c", "1",
		"%d", "02",
		"%e", "2",
		"%H", "15",
		"%h", "03",
		"%I", "03",
		"%i", "04",
		"%s", "05",
		"%S", "05",
		"%p", "PM",
		"%M", "January",
		"%b", "Jan",
		"%W", "Monday",
		"%a", "Mon",
		"%j", "002",
		"%%", "%",
	)
	return r.Replace(format)
}

// applyInterval adds or subtracts an interval expressed as "n UNIT" to t.
func applyInterval(t time.Time, interval string, subtract bool) (time.Time, error) {
	parts := strings.Fields(interval)
	if len(parts) < 2 {
		return t, fmt.Errorf("invalid interval: %q", interval)
	}
	n, err := strconv.Atoi(parts[0])
	if err != nil {
		return t, fmt.Errorf("invalid interval value: %q", parts[0])
	}
	if subtract {
		n = -n
	}
	unit := strings.ToUpper(parts[1])
	switch unit {
	case "SECOND":
		return t.Add(time.Duration(n) * time.Second), nil
	case "MINUTE":
		return t.Add(time.Duration(n) * time.Minute), nil
	case "HOUR":
		return t.Add(time.Duration(n) * time.Hour), nil
	case "DAY":
		return t.AddDate(0, 0, n), nil
	case "WEEK":
		return t.AddDate(0, 0, n*7), nil
	case "MONTH":
		return t.AddDate(0, n, 0), nil
	case "YEAR":
		return t.AddDate(n, 0, 0), nil
	default:
		return t, fmt.Errorf("unsupported interval unit: %s", unit)
	}
}
