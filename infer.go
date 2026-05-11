package vapordb

import "fmt"

// UpsertSchema creates or evolves the schema for tableName based on the
// incoming row. The returned bool is true when a forced type conflict cleared
// all rows in the table (legacy wipe).
//   - Unknown table → create table + schema from the row.
//   - New column → add to schema; backfill existing rows with NULL.
//   - NULL incoming value → no schema change.
//   - Safe widening (incoming > existing in Kind hierarchy) → widen silently.
//   - Unsafe type conflict (see [IsConflict]) → by default returns an error so
//     data is never silently discarded. When forceWipeOnSchemaConflict is true
//     (from [WithForceWipeOnSchemaConflict] on [New] or [WithWriteForceWipeOnSchemaConflict]
//     on [DB.Exec]/[DB.Query]), all rows are wiped and the column adopts the
//     incoming type instead (legacy behaviour).
//
// If the table is schema-locked, any mutation that would alter the schema
// (new column, type widening, or type conflict) is rejected with an error.
// Rows that fit within the existing schema are accepted without error.
//
// The bool is true if the table's row slice was cleared due to a forced
// schema conflict (legacy wipe); otherwise false.
func UpsertSchema(db *DB, tableName string, row Row, forceWipeOnSchemaConflict bool) (wiped bool, err error) {
	tbl, exists := db.Tables[tableName]
	if !exists {
		tbl = &Table{
			Schema: make(map[string]Kind),
			Rows:   make([]Row, 0),
		}
		db.Tables[tableName] = tbl
		for col, val := range row {
			tbl.Schema[col] = val.Kind
		}
		return false, nil
	}

	for col, val := range row {
		incoming := val.Kind
		existing, hasCol := tbl.Schema[col]

		switch {
		case !hasCol:
			if tbl.Locked {
				return wiped, fmt.Errorf("table %q is schema-locked: cannot add new column %q", tableName, col)
			}
			// Brand-new column: add to schema and backfill existing rows with NULL.
			tbl.Schema[col] = incoming
			for _, r := range tbl.Rows {
				if _, ok := r[col]; !ok {
					r[col] = Null
				}
			}
		case incoming == KindNull:
			// NULL never changes the schema.
		case IsConflict(existing, incoming):
			if tbl.Locked {
				return wiped, fmt.Errorf("table %q is schema-locked: cannot change type of column %q (existing %v, incoming %v)", tableName, col, existing, incoming)
			}
			if !forceWipeOnSchemaConflict {
				return wiped, fmt.Errorf("table %q column %q: incompatible types (existing %v, incoming %v); use vapordb.WithForceWipeOnSchemaConflict(true) on [New] or vapordb.WithWriteForceWipeOnSchemaConflict(true) on this [DB.Exec]/[DB.Query] to wipe rows and adopt the incoming type", tableName, col, existing, incoming)
			}
			// Legacy: wipe the whole table and adopt the new type.
			tbl.Rows = make([]Row, 0)
			wiped = true
			tbl.Schema[col] = incoming
		case incoming > existing:
			if tbl.Locked {
				return wiped, fmt.Errorf("table %q is schema-locked: cannot widen column %q from %v to %v", tableName, col, existing, incoming)
			}
			// Safe widening.
			tbl.Schema[col] = incoming
		}
	}
	return wiped, nil
}
