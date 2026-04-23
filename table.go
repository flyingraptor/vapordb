package vapordb

// Row is a single database record: a map from column name to Value.
type Row map[string]Value

// Table holds a schema map and all rows for a single named table.
type Table struct {
	Schema map[string]Kind `json:"schema"`
	Rows   []Row           `json:"rows"`
}
