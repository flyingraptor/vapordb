package vapordb

import "fmt"

// UpsertSchema creates or evolves the schema for tableName based on the
// incoming row. Rules:
//   - Unknown table → create table + schema from the row.
//   - New column → add to schema; backfill existing rows with NULL.
//   - NULL incoming value → no schema change.
//   - Safe widening (incoming > existing in Kind hierarchy) → widen silently.
//   - Unsafe conflict (incoming < existing, both non-null) → wipe all rows,
//     adopt the incoming type.
//
// If the table is schema-locked, any mutation that would alter the schema
// (new column, type widening, or type conflict) is rejected with an error.
// Rows that fit within the existing schema are accepted without error.
func UpsertSchema(db *DB, tableName string, row Row) error {
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
		return nil
	}

	for col, val := range row {
		incoming := val.Kind
		existing, hasCol := tbl.Schema[col]

		switch {
		case !hasCol:
			if tbl.Locked {
				return fmt.Errorf("table %q is schema-locked: cannot add new column %q", tableName, col)
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
				return fmt.Errorf("table %q is schema-locked: cannot change type of column %q (existing %v, incoming %v)", tableName, col, existing, incoming)
			}
			// Unsafe downgrade: wipe the whole table and adopt the new type.
			tbl.Rows = make([]Row, 0)
			tbl.Schema[col] = incoming
		case incoming > existing:
			if tbl.Locked {
				return fmt.Errorf("table %q is schema-locked: cannot widen column %q from %v to %v", tableName, col, existing, incoming)
			}
			// Safe widening.
			tbl.Schema[col] = incoming
		}
	}
	return nil
}
