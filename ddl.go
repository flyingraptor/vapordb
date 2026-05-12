package vapordb

import (
	"fmt"
	"sort"
	"strings"
)

// GenerateDDL inspects the live schema and emits a CREATE TABLE script for
// every table in the database. dialect must be "mysql" or "postgres".
//
// Columns are emitted in alphabetical order. Enum-constrained columns are
// rendered as ENUM(…) in MySQL and as TEXT with a CHECK constraint in Postgres.
// The output is ready to paste into a real database.
func (db *DB) GenerateDDL(dialect string) (string, error) {
	db.mu.RLock()
	defer db.mu.RUnlock()

	d := strings.ToLower(strings.TrimSpace(dialect))
	if d != "mysql" && d != "postgres" {
		return "", fmt.Errorf("vapordb: unsupported DDL dialect %q; supported: mysql, postgres", dialect)
	}

	tableNames := make([]string, 0, len(db.Tables))
	for name := range db.Tables {
		tableNames = append(tableNames, name)
	}
	sort.Strings(tableNames)

	var sb strings.Builder
	for _, name := range tableNames {
		ddl, err := generateTableDDL(name, db.Tables[name], d)
		if err != nil {
			return "", err
		}
		sb.WriteString(ddl)
		sb.WriteByte('\n')
	}
	return sb.String(), nil
}

func generateTableDDL(name string, tbl *Table, dialect string) (string, error) {
	colNames := make([]string, 0, len(tbl.Schema))
	for col := range tbl.Schema {
		colNames = append(colNames, col)
	}
	sort.Strings(colNames)

	var sb strings.Builder
	sb.WriteString("CREATE TABLE ")
	sb.WriteString(quoteIdentifier(name, dialect))
	sb.WriteString(" (\n")

	// Collect CHECK constraints (Postgres enums)
	var checks []string

	defs := make([]string, 0, len(colNames))
	for _, col := range colNames {
		kind := tbl.Schema[col]
		var sqlType string

		if tbl.EnumSets != nil {
			if vals, ok := tbl.EnumSets[col]; ok && len(vals) > 0 {
				if dialect == "mysql" {
					quoted := make([]string, len(vals))
					for i, v := range vals {
						quoted[i] = "'" + strings.ReplaceAll(v, "'", "''") + "'"
					}
					sqlType = "ENUM(" + strings.Join(quoted, ", ") + ")"
				} else {
					sqlType = "TEXT"
					enumQuoted := make([]string, len(vals))
					for i, v := range vals {
						enumQuoted[i] = "'" + strings.ReplaceAll(v, "'", "''") + "'"
					}
					checks = append(checks,
						fmt.Sprintf("    CONSTRAINT %s CHECK (%s IN (%s))",
							quoteIdentifier(name+"_"+col+"_check", dialect),
							quoteIdentifier(col, dialect),
							strings.Join(enumQuoted, ", ")))
				}
			}
		}
		if sqlType == "" {
			sqlType = kindToSQLType(kind, dialect)
		}

		defs = append(defs, fmt.Sprintf("    %s %s", quoteIdentifier(col, dialect), sqlType))
	}

	allParts := append(defs, checks...)
	sb.WriteString(strings.Join(allParts, ",\n"))
	sb.WriteString("\n);\n")
	return sb.String(), nil
}

// kindToSQLType maps a vapordb Kind to the appropriate SQL column type for the
// target dialect.
func kindToSQLType(k Kind, dialect string) string {
	switch k {
	case KindNull:
		return "TEXT"
	case KindBool:
		if dialect == "mysql" {
			return "TINYINT(1)"
		}
		return "BOOLEAN"
	case KindInt:
		return "BIGINT"
	case KindFloat:
		if dialect == "mysql" {
			return "DOUBLE"
		}
		return "DOUBLE PRECISION"
	case KindString:
		return "TEXT"
	case KindDate:
		if dialect == "mysql" {
			return "DATETIME"
		}
		return "TIMESTAMP"
	case KindJSON:
		if dialect == "mysql" {
			return "JSON"
		}
		return "JSONB"
	default:
		return "TEXT"
	}
}

// quoteIdentifier wraps an identifier in the dialect-appropriate quotes.
func quoteIdentifier(name, dialect string) string {
	if dialect == "mysql" {
		return "`" + strings.ReplaceAll(name, "`", "``") + "`"
	}
	return `"` + strings.ReplaceAll(name, `"`, `""`) + `"`
}
