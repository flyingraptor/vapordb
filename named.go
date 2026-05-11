package vapordb

import (
	"encoding"
	"fmt"
	"reflect"
	"strconv"
	"strings"
	"time"
)

var textMarshalerType = reflect.TypeOf((*encoding.TextMarshaler)(nil)).Elem()

// QueryNamed executes a SELECT statement with named :param placeholders.
// params may be a map[string]any or a struct with `db` field tags.
// Optional [WriteOption] values are forwarded to [DB.Query] (for DML with RETURNING).
//
// Example:
//
//	rows, err := db.QueryNamed(
//	    `SELECT * FROM orders WHERE user_id = :uid AND status = :status`,
//	    map[string]any{"uid": 42, "status": "open"},
//	)
func (db *DB) QueryNamed(sql string, params any, opts ...WriteOption) ([]Row, error) {
	expanded, err := expandNamed(sql, params)
	if err != nil {
		return nil, err
	}
	return db.Query(expanded, opts...)
}

// ExecNamed executes an INSERT, UPDATE, or DELETE with named :param placeholders.
// params may be a map[string]any or a struct with `db` field tags.
// Optional [WriteOption] values are forwarded to [DB.Exec].
//
// Example:
//
//	err := db.ExecNamed(
//	    `INSERT INTO users (id, name) VALUES (:id, :name)`,
//	    User{ID: 1, Name: "Alice"},
//	)
func (db *DB) ExecNamed(sql string, params any, opts ...WriteOption) error {
	expanded, err := expandNamed(sql, params)
	if err != nil {
		return err
	}
	return db.Exec(expanded, opts...)
}

// expandNamed replaces every :param placeholder in sql with its SQL literal
// value looked up from params. String literals inside single quotes are left
// untouched so that e.g. WHERE note = ':not_a_param' is safe.
func expandNamed(sql string, params any) (string, error) {
	m, err := toParamMap(params)
	if err != nil {
		return "", err
	}

	var b strings.Builder
	i := 0
	for i < len(sql) {
		ch := sql[i]

		// Pass single-quoted string literals through unchanged.
		if ch == '\'' {
			b.WriteByte(ch)
			i++
			for i < len(sql) {
				c := sql[i]
				b.WriteByte(c)
				i++
				if c == '\'' {
					// '' is an escaped quote inside the string; keep going.
					if i < len(sql) && sql[i] == '\'' {
						b.WriteByte(sql[i])
						i++
					} else {
						break
					}
				}
			}
			continue
		}

		// Named placeholder: :identifier
		if ch == ':' && i+1 < len(sql) && isNamedIdentStart(sql[i+1]) {
			j := i + 1
			for j < len(sql) && isNamedIdentChar(sql[j]) {
				j++
			}
			name := sql[i+1 : j]
			val, ok := m[name]
			if !ok {
				return "", fmt.Errorf("named parameter :%s not provided", name)
			}
			lit, err := anyToSQLLiteral(val)
			if err != nil {
				return "", fmt.Errorf("named parameter :%s: %w", name, err)
			}
			b.WriteString(lit)
			i = j
			continue
		}

		b.WriteByte(ch)
		i++
	}
	return b.String(), nil
}

// toParamMap converts params into a name→value map.
// Accepts map[string]any directly, or a struct whose exported fields carry `db` tags.
func toParamMap(params any) (map[string]any, error) {
	if params == nil {
		return map[string]any{}, nil
	}
	if m, ok := params.(map[string]any); ok {
		return m, nil
	}

	rv := reflect.ValueOf(params)
	if rv.Kind() == reflect.Pointer {
		rv = rv.Elem()
	}
	if rv.Kind() != reflect.Struct {
		return nil, fmt.Errorf("params must be a map[string]any or a struct, got %T", params)
	}

	rt := rv.Type()
	m := make(map[string]any, rt.NumField()*2)
	appendStructFieldsToParamMap(rv, rt, m)
	return m, nil
}

// appendStructFieldsToParamMap adds `db`-tagged values from rv/rt into m,
// recursing into embedded anonymous struct or *struct fields.
func appendStructFieldsToParamMap(rv reflect.Value, rt reflect.Type, m map[string]any) {
	for i := 0; i < rt.NumField(); i++ {
		sf := rt.Field(i)
		fv := rv.Field(i)

		if sf.Anonymous {
			switch sf.Type.Kind() {
			case reflect.Struct:
				appendStructFieldsToParamMap(fv, sf.Type, m)
				continue
			case reflect.Pointer:
				if sf.Type.Elem().Kind() == reflect.Struct {
					if fv.IsNil() {
						continue
					}
					appendStructFieldsToParamMap(fv.Elem(), sf.Type.Elem(), m)
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
		m[tag] = fv.Interface()
	}
}

// anyToSQLLiteral converts a Go value to its SQL literal representation.
// Slices are expanded to a comma-separated list of literals so they can be
// used directly inside IN (…) after = ANY(:param) / <> ALL(:param) rewriting.
func anyToSQLLiteral(v any) (string, error) {
	if v == nil {
		return "NULL", nil
	}
	rv := reflect.ValueOf(v)
	// Dereference pointers; nil pointer → NULL.
	for rv.Kind() == reflect.Pointer {
		if rv.IsNil() {
			return "NULL", nil
		}
		rv = rv.Elem()
	}

	// time.Time, driver.Valuer, TextMarshaler, fmt.Stringer — aligned with
	// [structFieldToSQL]. Must run before the slice branch so net.IP ([]byte
	// with Stringer) becomes a quoted address, not a list of byte literals.
	if rv.Type() == timeType {
		t := rv.Interface().(time.Time)
		if t.IsZero() {
			return "NULL", nil
		}
		return fmt.Sprintf("DATE('%s')", t.UTC().Format("2006-01-02 15:04:05")), nil
	}
	if dv, ok := valuerOf(rv); ok {
		dbVal, err := dv.Value()
		if err != nil {
			return "", fmt.Errorf("driver.Valuer: %w", err)
		}
		if dbVal == nil {
			return "NULL", nil
		}
		return driverValueToSQL(dbVal), nil
	}
	if rv.Type().Implements(textMarshalerType) {
		b, err := rv.Interface().(encoding.TextMarshaler).MarshalText()
		if err != nil {
			return "", fmt.Errorf("MarshalText: %w", err)
		}
		return quotedString(string(b)), nil
	}
	if rv.CanAddr() && rv.Addr().Type().Implements(textMarshalerType) {
		b, err := rv.Addr().Interface().(encoding.TextMarshaler).MarshalText()
		if err != nil {
			return "", fmt.Errorf("MarshalText: %w", err)
		}
		return quotedString(string(b)), nil
	}
	if rv.Type().Implements(stringerType) {
		return quotedString(rv.Interface().(fmt.Stringer).String()), nil
	}
	if rv.CanAddr() && rv.Addr().Type().Implements(stringerType) {
		return quotedString(rv.Addr().Interface().(fmt.Stringer).String()), nil
	}

	// Slices → comma-separated literals (used inside IN (…)).
	if rv.Kind() == reflect.Slice {
		if rv.Len() == 0 {
			return "NULL", nil // IN (NULL) matches nothing — safe no-op
		}
		parts := make([]string, rv.Len())
		for i := 0; i < rv.Len(); i++ {
			lit, err := anyToSQLLiteral(rv.Index(i).Interface())
			if err != nil {
				return "", fmt.Errorf("slice element %d: %w", i, err)
			}
			parts[i] = lit
		}
		return strings.Join(parts, ", "), nil
	}
	switch rv.Kind() { //nolint:exhaustive
	case reflect.String:
		s := strings.ReplaceAll(rv.String(), "'", "''")
		return "'" + s + "'", nil
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return strconv.FormatInt(rv.Int(), 10), nil
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		return strconv.FormatUint(rv.Uint(), 10), nil
	case reflect.Float32, reflect.Float64:
		return strconv.FormatFloat(rv.Float(), 'f', -1, 64), nil
	case reflect.Bool:
		if rv.Bool() {
			return "1", nil
		}
		return "0", nil
	default:
		return "", fmt.Errorf("unsupported param type %T", v)
	}
}

func isNamedIdentStart(c byte) bool {
	return (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || c == '_'
}

func isNamedIdentChar(c byte) bool {
	return isNamedIdentStart(c) || (c >= '0' && c <= '9')
}
