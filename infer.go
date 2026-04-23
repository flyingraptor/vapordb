package vapordb

// UpsertSchema creates or evolves the schema for tableName based on the
// incoming row. Rules:
//   - Unknown table → create table + schema from the row.
//   - New column → add to schema; backfill existing rows with NULL.
//   - NULL incoming value → no schema change.
//   - Safe widening (incoming > existing in Kind hierarchy) → widen silently.
//   - Unsafe conflict (incoming < existing, both non-null) → wipe all rows,
//     adopt the incoming type.
func UpsertSchema(db *DB, tableName string, row Row) {
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
		return
	}

	for col, val := range row {
		incoming := val.Kind
		existing, hasCol := tbl.Schema[col]

		switch {
		case !hasCol:
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
			// Unsafe downgrade: wipe the whole table and adopt the new type.
			tbl.Rows = make([]Row, 0)
			tbl.Schema[col] = incoming
		case incoming > existing:
			// Safe widening.
			tbl.Schema[col] = incoming
		}
	}
}
