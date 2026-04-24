package vapordb

import (
	"fmt"
	"reflect"
	"strconv"
	"strings"
)

// QueryNamed executes a SELECT statement with named :param placeholders.
// params may be a map[string]any or a struct with `db` field tags.
//
// Example:
//
//	rows, err := db.QueryNamed(
//	    `SELECT * FROM orders WHERE user_id = :uid AND status = :status`,
//	    map[string]any{"uid": 42, "status": "open"},
//	)
func (db *DB) QueryNamed(sql string, params any) ([]Row, error) {
	expanded, err := expandNamed(sql, params)
	if err != nil {
		return nil, err
	}
	return db.Query(expanded)
}

// ExecNamed executes an INSERT, UPDATE, or DELETE with named :param placeholders.
// params may be a map[string]any or a struct with `db` field tags.
//
// Example:
//
//	err := db.ExecNamed(
//	    `INSERT INTO users (id, name) VALUES (:id, :name)`,
//	    User{ID: 1, Name: "Alice"},
//	)
func (db *DB) ExecNamed(sql string, params any) error {
	expanded, err := expandNamed(sql, params)
	if err != nil {
		return err
	}
	return db.Exec(expanded)
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
	m := make(map[string]any, rt.NumField())
	for i := 0; i < rt.NumField(); i++ {
		tag := rt.Field(i).Tag.Get("db")
		if tag == "" || tag == "-" {
			continue
		}
		m[tag] = rv.Field(i).Interface()
	}
	return m, nil
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
