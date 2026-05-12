package vapordb

import (
	"database/sql"
	"database/sql/driver"
	"encoding"
	"encoding/json"
	"fmt"
	"reflect"
	"strconv"
	"strings"
	"time"
)

// InsertStruct builds and executes an INSERT statement from a struct.
// Each exported field with a `db:"column_name"` tag becomes a column.
// Fields tagged `db:"-"` are ignored.
//
// Example:
//
//	type User struct {
//	    ID   int    `db:"id"`
//	    Name string `db:"name"`
//	}
//	db.InsertStruct("users", User{ID: 1, Name: "Alice"})
func (db *DB) InsertStruct(table string, v any) error {
	rv := reflect.ValueOf(v)
	if rv.Kind() == reflect.Pointer {
		rv = rv.Elem()
	}
	rt := rv.Type()

	var cols, vals []string
	for i := 0; i < rt.NumField(); i++ {
		tag := rt.Field(i).Tag.Get("db")
		if tag == "" || tag == "-" {
			continue
		}
		cols = append(cols, tag)
		vals = append(vals, structFieldToSQL(rv.Field(i)))
	}
	if len(cols) == 0 {
		return fmt.Errorf("InsertStruct: no fields with `db` tags found on %T", v)
	}

	sql := fmt.Sprintf(
		"INSERT INTO %s (%s) VALUES (%s)",
		table,
		strings.Join(cols, ", "),
		strings.Join(vals, ", "),
	)
	return db.Exec(sql)
}

// ScanRows maps a []Row result into a typed slice using `db` struct tags.
// T must be a struct; fields without a matching `db` tag are left at their
// zero value. NULL columns are also left at their zero value.
// Anonymous embedded struct (or *struct) fields are walked recursively so
// promoted `db` tags on the embedded type are scanned like database/sql.
// The embedded field must be exported (capitalized type name) so reflect
// can assign through it from this package, matching normal Go visibility rules.
//
// Example:
//
//	type User struct {
//	    ID   int    `db:"id"`
//	    Name string `db:"name"`
//	}
//	rows, _ := db.Query("SELECT id, name FROM users")
//	users := vapordb.ScanRows[User](rows)
func ScanRows[T any](rows []Row) []T {
	var zero T
	rt := reflect.TypeOf(zero)
	if rt.Kind() == reflect.Pointer {
		rt = rt.Elem()
	}

	// column name → field index path (handles embedded anonymous structs).
	tagPaths := collectDBTagPaths(rt)

	result := make([]T, len(rows))
	for i, row := range rows {
		rv := reflect.New(rt).Elem()
		for col, val := range row {
			path, ok := tagPaths[col]
			if !ok || val.Kind == KindNull {
				continue
			}
			setFieldByPath(rv, path, val)
		}
		result[i] = rv.Interface().(T)
	}
	return result
}

// collectDBTagPaths maps lowered column names to struct field index paths.
// Anonymous struct or *struct fields are walked recursively so promoted `db`
// tags behave like database/sql / sqlx.
func collectDBTagPaths(rt reflect.Type) map[string][]int {
	out := make(map[string][]int)
	collectDBTagPathsRec(rt, nil, out)
	return out
}

func collectDBTagPathsRec(rt reflect.Type, prefix []int, out map[string][]int) {
	for i := 0; i < rt.NumField(); i++ {
		sf := rt.Field(i)
		path := append(append([]int(nil), prefix...), i)

		// Anonymous embedded fields may be unexported (e.g. `baseFilter`); still
		// walk them so promoted `db` tags work when the caller package matches.
		if sf.Anonymous {
			switch sf.Type.Kind() {
			case reflect.Struct:
				collectDBTagPathsRec(sf.Type, path, out)
				continue
			case reflect.Pointer:
				if sf.Type.Elem().Kind() == reflect.Struct {
					collectDBTagPathsRec(sf.Type.Elem(), path, out)
					continue
				}
			}
		}

		if !sf.IsExported() {
			continue
		}

		tag := sf.Tag.Get("db")
		if tag == "" || tag == "-" {
			continue
		}
		out[strings.ToLower(tag)] = path
	}
}

// setFieldByPath walks rv to path[len-1] (allocating nil pointer embeds) then
// writes val into the leaf field via setStructField.
func setFieldByPath(root reflect.Value, path []int, val Value) {
	if len(path) == 0 {
		return
	}
	cur := root
	for _, idx := range path[:len(path)-1] {
		cur = cur.Field(idx)
		if cur.Kind() == reflect.Pointer {
			if cur.IsNil() {
				cur.Set(reflect.New(cur.Type().Elem()))
			}
			cur = cur.Elem()
		}
	}
	setStructField(cur.Field(path[len(path)-1]), val)
}

// ── internal helpers ──────────────────────────────────────────────────────────

var (
	timeType         = reflect.TypeOf(time.Time{})
	stringerType     = reflect.TypeOf((*fmt.Stringer)(nil)).Elem()
	textUnmarshaller = reflect.TypeOf((*encoding.TextUnmarshaler)(nil)).Elem()
	driverValuerType = reflect.TypeOf((*driver.Valuer)(nil)).Elem()
	sqlScannerType   = reflect.TypeOf((*sql.Scanner)(nil)).Elem()
)

// structFieldToSQL converts a struct field value into its SQL literal.
// Resolution order:
//  1. Nil pointer         → NULL
//  2. Pointer             → dereference and recurse
//  3. time.Time           → DATE('…')
//  4. driver.Valuer       → convert the driver.Value to a SQL literal
//  5. fmt.Stringer        → quoted String() result
//  6. Primitive kinds     (string, int, float, bool)
//  7. Anything else       → NULL
func structFieldToSQL(v reflect.Value) string {
	// 1 & 2. Dereference pointers; nil pointer → NULL.
	for v.Kind() == reflect.Pointer {
		if v.IsNil() {
			return "NULL"
		}
		v = v.Elem()
	}

	// 3. time.Time — handle explicitly so we control the DATE/DATETIME format.
	if v.Type() == timeType {
		t := v.Interface().(time.Time)
		if t.IsZero() {
			return "NULL"
		}
		u := t.UTC()
		if u.Hour() != 0 || u.Minute() != 0 || u.Second() != 0 || u.Nanosecond() != 0 {
			return fmt.Sprintf("DATETIME('%s')", u.Format("2006-01-02 15:04:05"))
		}
		return fmt.Sprintf("DATE('%s')", u.Format("2006-01-02"))
	}

	// 4. driver.Valuer (value receiver or pointer receiver).
	if dv, ok := valuerOf(v); ok {
		dbVal, err := dv.Value()
		if err != nil || dbVal == nil {
			return "NULL"
		}
		return driverValueToSQL(dbVal)
	}

	// 5. fmt.Stringer — value receiver or pointer receiver.
	if v.Type().Implements(stringerType) {
		return quotedString(v.Interface().(fmt.Stringer).String())
	}
	if v.CanAddr() && v.Addr().Type().Implements(stringerType) {
		return quotedString(v.Addr().Interface().(fmt.Stringer).String())
	}

	// 6. Primitive kinds.
	switch v.Kind() { //nolint:exhaustive
	case reflect.String:
		return quotedString(v.String())
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return strconv.FormatInt(v.Int(), 10)
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		return strconv.FormatUint(v.Uint(), 10)
	case reflect.Float32, reflect.Float64:
		return strconv.FormatFloat(v.Float(), 'f', -1, 64)
	case reflect.Bool:
		if v.Bool() {
			return "1"
		}
		return "0"
	case reflect.Map, reflect.Slice:
		// Marshal maps and slices as JSON and wrap with json_parse(…).
		b, err := json.Marshal(v.Interface())
		if err != nil {
			return "NULL"
		}
		return "json_parse(" + quotedString(string(b)) + ")"
	}
	return "NULL"
}

// setStructField writes a vapordb Value into a reflect.Value struct field.
// Resolution order:
//  1. Pointer field              → allocate, recurse into the pointed-to type
//  2. time.Time                  → parse / assign directly
//  3. sql.Scanner                → Scan(val.V)
//  4. encoding.TextUnmarshaler   → UnmarshalText(string value)
//  5. Primitive kinds            (string, int, float, bool)
func setStructField(field reflect.Value, val Value) {
	// 1. Pointer field: allocate a new value and recurse.
	if field.Kind() == reflect.Pointer {
		if val.Kind == KindNull {
			return // leave nil
		}
		inner := reflect.New(field.Type().Elem())
		setStructField(inner.Elem(), val)
		field.Set(inner)
		return
	}

	// 2. time.Time
	if field.Type() == timeType {
		switch x := val.V.(type) {
		case time.Time:
			field.Set(reflect.ValueOf(x))
		case string:
			if t, ok := tryParseDate(x); ok {
				field.Set(reflect.ValueOf(t))
			}
		}
		return
	}

	if field.CanAddr() {
		ptr := field.Addr().Interface()

		// 3. sql.Scanner — the standard database/sql scanning interface.
		if sc, ok := ptr.(sql.Scanner); ok {
			_ = sc.Scan(val.V)
			return
		}

		// 4. encoding.TextUnmarshaler (e.g. uuid.UUID, net.IP, …).
		if tu, ok := ptr.(encoding.TextUnmarshaler); ok {
			var text string
			switch x := val.V.(type) {
			case string:
				text = x
			default:
				text = fmt.Sprintf("%v", x)
			}
			_ = tu.UnmarshalText([]byte(text))
			return
		}
	}

	// 5. Primitive kinds.
	switch field.Kind() { //nolint:exhaustive
	case reflect.String:
		if val.Kind == KindJSON {
			// Serialise JSON back to a string when the target field is string.
			b, err := json.Marshal(val.V)
			if err == nil {
				field.SetString(string(b))
				return
			}
		}
		field.SetString(fmt.Sprintf("%v", val.V))
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		switch n := val.V.(type) {
		case int64:
			field.SetInt(n)
		case float64:
			field.SetInt(int64(n))
		}
	case reflect.Float32, reflect.Float64:
		switch n := val.V.(type) {
		case float64:
			field.SetFloat(n)
		case int64:
			field.SetFloat(float64(n))
		}
	case reflect.Bool:
		switch b := val.V.(type) {
		case bool:
			field.SetBool(b)
		case int64:
			field.SetBool(b != 0)
		}
	case reflect.Map, reflect.Slice, reflect.Interface:
		// Deserialise a KindJSON value into the target map/slice/interface field.
		if val.Kind == KindJSON && field.CanSet() {
			jsonBytes, err := json.Marshal(val.V)
			if err != nil {
				return
			}
			ptr := reflect.New(field.Type())
			if err := json.Unmarshal(jsonBytes, ptr.Interface()); err == nil {
				field.Set(ptr.Elem())
			}
		}
	}
}

func quotedString(s string) string {
	return "'" + strings.ReplaceAll(s, "'", "''") + "'"
}

// valuerOf returns a driver.Valuer if v (or &v) implements the interface.
func valuerOf(v reflect.Value) (driver.Valuer, bool) {
	if v.Type().Implements(driverValuerType) {
		return v.Interface().(driver.Valuer), true
	}
	if v.CanAddr() && v.Addr().Type().Implements(driverValuerType) {
		return v.Addr().Interface().(driver.Valuer), true
	}
	return nil, false
}

// driverValueToSQL converts a driver.Value (one of the allowed primitive types)
// into its SQL literal representation.
func driverValueToSQL(v driver.Value) string {
	switch x := v.(type) {
	case nil:
		return "NULL"
	case string:
		// Detect PostgreSQL array literals produced by pq.Array(slice).Value()
		// e.g. "{A001,A002}" → 'A001', 'A002'  (for use inside IN (…))
		if expanded, ok := expandPGArrayLiteral(x); ok {
			return expanded
		}
		return quotedString(x)
	case []byte:
		return quotedString(string(x))
	case int64:
		return strconv.FormatInt(x, 10)
	case float64:
		return strconv.FormatFloat(x, 'f', -1, 64)
	case bool:
		if x {
			return "1"
		}
		return "0"
	case time.Time:
		if x.IsZero() {
			return "NULL"
		}
		u := x.UTC()
		if u.Hour() != 0 || u.Minute() != 0 || u.Second() != 0 || u.Nanosecond() != 0 {
			return fmt.Sprintf("DATETIME('%s')", u.Format("2006-01-02 15:04:05"))
		}
		return fmt.Sprintf("DATE('%s')", u.Format("2006-01-02"))
	default:
		return quotedString(fmt.Sprintf("%v", x))
	}
}

// ── PostgreSQL array literal helpers ─────────────────────────────────────────
//
// pq.Array(slice).Value() returns a PostgreSQL array string like "{A001,A002}".
// These helpers detect that format and expand it to a SQL comma-separated
// literal list suitable for use inside IN (…).

// isPGArrayLiteral reports whether s looks like a PostgreSQL array literal.
func isPGArrayLiteral(s string) bool {
	return len(s) >= 2 && s[0] == '{' && s[len(s)-1] == '}'
}

// expandPGArrayLiteral parses a PostgreSQL array literal such as "{A001,A002}"
// or "{1,2,3}" and returns a SQL comma-separated list of literals.
//
// Empty array "{}" returns ("NULL", true) — IN (NULL) never matches any row,
// which is the correct semantics for an empty exclusion/inclusion set.
//
// Returns ("", false) if s is not a PostgreSQL array literal.
func expandPGArrayLiteral(s string) (string, bool) {
	if !isPGArrayLiteral(s) {
		return "", false
	}
	inner := s[1 : len(s)-1]
	if inner == "" {
		return "NULL", true // empty array → safe no-op for IN
	}
	elems := parsePGArrayElems(inner)
	lits := make([]string, len(elems))
	for i, e := range elems {
		if strings.EqualFold(e, "NULL") {
			lits[i] = "NULL"
		} else if _, err := strconv.ParseInt(e, 10, 64); err == nil {
			lits[i] = e // numeric — no quotes needed
		} else if _, err := strconv.ParseFloat(e, 64); err == nil {
			lits[i] = e
		} else {
			lits[i] = quotedString(e)
		}
	}
	return strings.Join(lits, ", "), true
}

// parsePGArrayElems splits the inner content of a PostgreSQL array literal
// (i.e. what's between the outer { and }) by commas, honoring double-quoted
// elements that may contain commas or backslash-escaped characters.
func parsePGArrayElems(s string) []string {
	var elems []string
	var cur strings.Builder
	i := 0
	for i < len(s) {
		switch s[i] {
		case '"':
			i++ // skip opening "
			for i < len(s) && s[i] != '"' {
				if s[i] == '\\' {
					i++
					if i < len(s) {
						cur.WriteByte(s[i])
						i++
					}
				} else {
					cur.WriteByte(s[i])
					i++
				}
			}
			if i < len(s) {
				i++ // skip closing "
			}
		case ',':
			elems = append(elems, cur.String())
			cur.Reset()
			i++
		default:
			cur.WriteByte(s[i])
			i++
		}
	}
	elems = append(elems, cur.String())
	return elems
}
