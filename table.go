package vapordb

// Row is a single database record: a map from column name to Value.
type Row map[string]Value

// Table holds a schema map and all rows for a single named table.
type Table struct {
	Schema   map[string]Kind     `json:"schema"`
	EnumSets map[string][]string `json:"enum_sets,omitempty"` // col → allowed values
	Locked   bool                `json:"locked,omitempty"`    // true → schema is frozen
	Rows     []Row               `json:"rows"`

	// conflictIdx caches conflict-key → first-matching-row-index maps, keyed by
	// the conflict-column signature (see index.go). It turns ON CONFLICT upsert
	// detection from an O(N) full-table scan per row into an O(1) lookup. It is
	// maintained incrementally on append and invalidated on any other row
	// mutation (update / delete / schema wipe). Unexported → never serialized,
	// and always safe to drop and rebuild lazily.
	conflictIdx map[string]*conflictIndex
}
