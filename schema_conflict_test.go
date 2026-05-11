package vapordb

import "testing"

// ── reject-by-default across kind transitions ───────────────────────────────

func TestSchemaConflictRejectManyKindTransitions(t *testing.T) {
	type rejectCase struct {
		name         string
		setup        []string
		rejectInsert string
		wantKind     Kind
		wantRows     int
	}
	cases := []rejectCase{
		{
			name:         "int_to_string",
			setup:        []string{`INSERT INTO t (x) VALUES (1)`, `INSERT INTO t (x) VALUES (2)`},
			rejectInsert: `INSERT INTO t (x) VALUES ('z')`,
			wantKind:     KindInt,
			wantRows:     2,
		},
		{
			name:         "string_to_int",
			setup:        []string{`INSERT INTO t (x) VALUES ('alpha')`},
			rejectInsert: `INSERT INTO t (x) VALUES (99)`,
			wantKind:     KindString,
			wantRows:     1,
		},
		{
			name:         "string_to_float",
			setup:        []string{`INSERT INTO t (x) VALUES ('text')`},
			rejectInsert: `INSERT INTO t (x) VALUES (3.14)`,
			wantKind:     KindString,
			wantRows:     1,
		},
		{
			name:         "float_to_int_downgrade",
			setup:        []string{`INSERT INTO t (x) VALUES (1.25)`},
			rejectInsert: `INSERT INTO t (x) VALUES (7)`,
			wantKind:     KindFloat,
			wantRows:     1,
		},
		{
			name:         "float_to_bool_downgrade",
			setup:        []string{`INSERT INTO t (x) VALUES (2.5)`},
			rejectInsert: `INSERT INTO t (x) VALUES (TRUE)`,
			wantKind:     KindFloat,
			wantRows:     1,
		},
		{
			name:         "int_to_bool_downgrade",
			setup:        []string{`INSERT INTO t (x) VALUES (10)`},
			rejectInsert: `INSERT INTO t (x) VALUES (FALSE)`,
			wantKind:     KindInt,
			wantRows:     1,
		},
		{
			name:         "bool_to_string",
			setup:        []string{`INSERT INTO t (x) VALUES (TRUE)`},
			rejectInsert: `INSERT INTO t (x) VALUES ('no')`,
			wantKind:     KindBool,
			wantRows:     1,
		},
		{
			name:         "date_to_int",
			setup:        []string{`INSERT INTO t (x) VALUES (DATE('2024-03-15'))`},
			rejectInsert: `INSERT INTO t (x) VALUES (1)`,
			wantKind:     KindDate,
			wantRows:     1,
		},
		{
			name:         "int_to_date",
			setup:        []string{`INSERT INTO t (x) VALUES (42)`},
			rejectInsert: `INSERT INTO t (x) VALUES (DATE('2025-01-01'))`,
			wantKind:     KindInt,
			wantRows:     1,
		},
		{
			name:         "date_to_string",
			setup:        []string{`INSERT INTO t (x) VALUES (DATE('2024-01-01'))`},
			rejectInsert: `INSERT INTO t (x) VALUES ('epoch')`,
			wantKind:     KindDate,
			wantRows:     1,
		},
		{
			name:         "string_to_date",
			setup:        []string{`INSERT INTO t (x) VALUES ('plain')`},
			rejectInsert: `INSERT INTO t (x) VALUES (DATE('2024-12-12'))`,
			wantKind:     KindString,
			wantRows:     1,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			db := New()
			for _, sql := range tc.setup {
				mustExec(t, db, sql)
			}
			if err := db.Exec(tc.rejectInsert); err == nil {
				t.Fatalf("expected reject insert %q to error", tc.rejectInsert)
			}
			if got := db.Tables["t"].Schema["x"]; got != tc.wantKind {
				t.Fatalf("schema: want %v, got %v", tc.wantKind, got)
			}
			if n := len(db.Tables["t"].Rows); n != tc.wantRows {
				t.Fatalf("row count: want %d, got %d", tc.wantRows, n)
			}
		})
	}
}

// ── safe widening (no conflict) ───────────────────────────────────────────────

func TestSchemaWidenNumericAndBool(t *testing.T) {
	t.Run("bool_to_int", func(t *testing.T) {
		db := New()
		mustExec(t, db, `INSERT INTO t (x) VALUES (TRUE)`)
		mustExec(t, db, `INSERT INTO t (x) VALUES (7)`)
		if db.Tables["t"].Schema["x"] != KindInt {
			t.Fatalf("want KindInt, got %v", db.Tables["t"].Schema["x"])
		}
		if len(db.Tables["t"].Rows) != 2 {
			t.Fatalf("want 2 rows, got %d", len(db.Tables["t"].Rows))
		}
	})
	t.Run("bool_to_float", func(t *testing.T) {
		db := New()
		mustExec(t, db, `INSERT INTO t (x) VALUES (FALSE)`)
		mustExec(t, db, `INSERT INTO t (x) VALUES (2.5)`)
		if db.Tables["t"].Schema["x"] != KindFloat {
			t.Fatalf("want KindFloat, got %v", db.Tables["t"].Schema["x"])
		}
	})
	t.Run("int_to_float", func(t *testing.T) {
		db := New()
		mustExec(t, db, `INSERT INTO t (x) VALUES (1)`)
		mustExec(t, db, `INSERT INTO t (x) VALUES (2.5)`)
		if db.Tables["t"].Schema["x"] != KindFloat {
			t.Fatalf("want KindFloat, got %v", db.Tables["t"].Schema["x"])
		}
		if len(db.Tables["t"].Rows) != 2 {
			t.Fatalf("want 2 rows, got %d", len(db.Tables["t"].Rows))
		}
	})
}

// ── per-exec force wipe adopts incoming kind ──────────────────────────────────

func TestSchemaConflictForceWipeManyKindTransitions(t *testing.T) {
	type forceCase struct {
		name         string
		setup        []string
		wipeInsert   string
		wantKind     Kind
		checkVal func(t *testing.T, v Value)
	}
	cases := []forceCase{
		{
			name:       "int_to_string",
			setup:      []string{`INSERT INTO t (x) VALUES (1)`, `INSERT INTO t (x) VALUES (2)`},
			wipeInsert: `INSERT INTO t (x) VALUES ('z')`,
			wantKind:   KindString,
			checkVal: func(t *testing.T, v Value) {
				t.Helper()
				if v != strVal("z") {
					t.Fatalf("want str z, got %v", v)
				}
			},
		},
		{
			name:       "string_to_int",
			setup:      []string{`INSERT INTO t (x) VALUES ('old')`},
			wipeInsert: `INSERT INTO t (x) VALUES (99)`,
			wantKind:   KindInt,
			checkVal: func(t *testing.T, v Value) {
				t.Helper()
				if v != intVal(99) {
					t.Fatalf("want 99, got %v", v)
				}
			},
		},
		{
			name:       "float_to_int",
			setup:      []string{`INSERT INTO t (x) VALUES (2.5)`},
			wipeInsert: `INSERT INTO t (x) VALUES (7)`,
			wantKind:   KindInt,
			checkVal: func(t *testing.T, v Value) {
				t.Helper()
				if v != intVal(7) {
					t.Fatalf("want 7, got %v", v)
				}
			},
		},
		{
			name:       "date_to_string",
			setup:      []string{`INSERT INTO t (x) VALUES (DATE('2024-06-01'))`},
			wipeInsert: `INSERT INTO t (x) VALUES ('later')`,
			wantKind:   KindString,
			checkVal: func(t *testing.T, v Value) {
				t.Helper()
				if v != strVal("later") {
					t.Fatalf("want later, got %v", v)
				}
			},
		},
		{
			name:       "int_to_date",
			setup:      []string{`INSERT INTO t (x) VALUES (42)`},
			wipeInsert: `INSERT INTO t (x) VALUES (DATE('2025-01-01'))`,
			wantKind:   KindDate,
			checkVal: func(t *testing.T, v Value) {
				t.Helper()
				if v.Kind != KindDate {
					t.Fatalf("want KindDate, got %v", v.Kind)
				}
			},
		},
		{
			name:       "string_to_float",
			setup:      []string{`INSERT INTO t (x) VALUES ('x')`},
			wipeInsert: `INSERT INTO t (x) VALUES (1.5)`,
			wantKind:   KindFloat,
			checkVal: func(t *testing.T, v Value) {
				t.Helper()
				if v != floatVal(1.5) {
					t.Fatalf("want 1.5, got %v", v)
				}
			},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			db := New()
			for _, sql := range tc.setup {
				mustExec(t, db, sql)
			}
			if err := db.Exec(tc.wipeInsert, WithWriteForceWipeOnSchemaConflict(true)); err != nil {
				t.Fatal(err)
			}
			if db.Tables["t"].Schema["x"] != tc.wantKind {
				t.Fatalf("schema: want %v, got %v", tc.wantKind, db.Tables["t"].Schema["x"])
			}
			if len(db.Tables["t"].Rows) != 1 {
				t.Fatalf("want 1 row after wipe, got %d", len(db.Tables["t"].Rows))
			}
			tc.checkVal(t, db.Tables["t"].Rows[0]["x"])
		})
	}
}
