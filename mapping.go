package vapordb

import (
	"fmt"
	"reflect"
	"strconv"
	"strings"
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

	// build column-name → field-index lookup once per call
	tagIndex := make(map[string]int, rt.NumField())
	for i := 0; i < rt.NumField(); i++ {
		if tag := rt.Field(i).Tag.Get("db"); tag != "" && tag != "-" {
			tagIndex[tag] = i
		}
	}

	result := make([]T, len(rows))
	for i, row := range rows {
		rv := reflect.New(rt).Elem()
		for col, val := range row {
			idx, ok := tagIndex[col]
			if !ok || val.Kind == KindNull {
				continue
			}
			setStructField(rv.Field(idx), val)
		}
		result[i] = rv.Interface().(T)
	}
	return result
}

// ── internal helpers ──────────────────────────────────────────────────────────

func structFieldToSQL(v reflect.Value) string {
	switch v.Kind() { //nolint:exhaustive
	case reflect.String:
		s := strings.ReplaceAll(v.String(), "'", "''")
		return "'" + s + "'"
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
	default:
		return "NULL"
	}
}

func setStructField(field reflect.Value, val Value) {
	switch field.Kind() { //nolint:exhaustive
	case reflect.String:
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
	}
}
