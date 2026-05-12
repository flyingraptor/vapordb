package vapordb

// Comprehensive round-trip tests for Save / Load.
//
// Covers every Kind (Null, Bool, Int, Float, String, Date, JSON), as well as
// the Table metadata that must survive serialisation:
//   - EnumSets  (DeclareEnum)
//   - Locked    (LockSchema / LockTable)
//   - Schema    (column → Kind map)

import (
	"os"
	"strings"
	"testing"
	"time"
)

// ── helpers ───────────────────────────────────────────────────────────────────

func saveLoad(t *testing.T, db *DB) *DB {
	t.Helper()
	path := t.TempDir() + "/state.json"
	if err := db.Save(path); err != nil {
		t.Fatalf("Save: %v", err)
	}
	db2 := New()
	if err := db2.Load(path); err != nil {
		t.Fatalf("Load: %v", err)
	}
	t.Cleanup(func() { os.Remove(path) })
	return db2
}

// ── KindNull ─────────────────────────────────────────────────────────────────

func TestPersist_Null(t *testing.T) {
	db := New()
	mustExec(t, db, `INSERT INTO t (id, note) VALUES (1, NULL)`)

	db2 := saveLoad(t, db)

	rows := mustQuery(t, db2, `SELECT id, note FROM t`)
	if len(rows) != 1 {
		t.Fatalf("want 1 row, got %d", len(rows))
	}
	if rows[0]["note"].Kind != KindNull {
		t.Errorf("note: want KindNull, got Kind=%d", rows[0]["note"].Kind)
	}
}

// ── KindBool ─────────────────────────────────────────────────────────────────

func TestPersist_Bool(t *testing.T) {
	db := New()
	mustExec(t, db, `INSERT INTO flags (name, active) VALUES ('a', TRUE)`)
	mustExec(t, db, `INSERT INTO flags (name, active) VALUES ('b', FALSE)`)

	db2 := saveLoad(t, db)

	rows := mustQuery(t, db2, `SELECT name, active FROM flags ORDER BY name`)
	if len(rows) != 2 {
		t.Fatalf("want 2 rows, got %d", len(rows))
	}
	if rows[0]["active"].V != true {
		t.Errorf("a: want true, got %v", rows[0]["active"].V)
	}
	if rows[1]["active"].V != false {
		t.Errorf("b: want false, got %v", rows[1]["active"].V)
	}
	if db2.Tables["flags"].Schema["active"] != KindBool {
		t.Errorf("schema: want KindBool for active")
	}
}

// ── KindInt ──────────────────────────────────────────────────────────────────

func TestPersist_Int(t *testing.T) {
	db := New()
	mustExec(t, db, `INSERT INTO counts (n) VALUES (42)`)
	mustExec(t, db, `INSERT INTO counts (n) VALUES (-1000)`)

	db2 := saveLoad(t, db)

	rows := mustQuery(t, db2, `SELECT n FROM counts ORDER BY n`)
	if len(rows) != 2 {
		t.Fatalf("want 2 rows, got %d", len(rows))
	}
	if rows[0]["n"].V != int64(-1000) || rows[1]["n"].V != int64(42) {
		t.Errorf("unexpected values: %v %v", rows[0]["n"].V, rows[1]["n"].V)
	}
}

// ── KindFloat ────────────────────────────────────────────────────────────────

func TestPersist_Float(t *testing.T) {
	db := New()
	mustExec(t, db, `INSERT INTO prices (p) VALUES (3.14)`)
	mustExec(t, db, `INSERT INTO prices (p) VALUES (-0.001)`)

	db2 := saveLoad(t, db)

	rows := mustQuery(t, db2, `SELECT p FROM prices ORDER BY p`)
	if len(rows) != 2 {
		t.Fatalf("want 2 rows, got %d", len(rows))
	}
	if rows[0]["p"].V.(float64) >= 0 {
		t.Errorf("first row should be negative, got %v", rows[0]["p"].V)
	}
	if db2.Tables["prices"].Schema["p"] != KindFloat {
		t.Errorf("schema: want KindFloat for p")
	}
}

// ── KindString ───────────────────────────────────────────────────────────────

func TestPersist_String(t *testing.T) {
	db := New()
	mustExec(t, db, `INSERT INTO words (w) VALUES ('hello')`)
	mustExec(t, db, `INSERT INTO words (w) VALUES ('it''s')`) // escaped apostrophe

	db2 := saveLoad(t, db)

	rows := mustQuery(t, db2, `SELECT w FROM words ORDER BY w`)
	if len(rows) != 2 {
		t.Fatalf("want 2 rows, got %d", len(rows))
	}
	if rows[0]["w"].V != "hello" {
		t.Errorf("want 'hello', got %v", rows[0]["w"].V)
	}
	if rows[1]["w"].V != "it's" {
		t.Errorf("want \"it's\", got %v", rows[1]["w"].V)
	}
}

// ── KindDate ─────────────────────────────────────────────────────────────────

func TestPersist_Date(t *testing.T) {
	db := New()
	ts := time.Date(2024, 3, 15, 10, 30, 0, 0, time.UTC)
	if err := db.ExecNamed(
		`INSERT INTO events (name, happened_at) VALUES (:name, :happened_at)`,
		map[string]any{"name": "launch", "happened_at": ts},
	); err != nil {
		t.Fatalf("insert: %v", err)
	}

	db2 := saveLoad(t, db)

	rows := mustQuery(t, db2, `SELECT name, happened_at FROM events`)
	if len(rows) != 1 {
		t.Fatalf("want 1 row, got %d", len(rows))
	}
	if rows[0]["name"].V != "launch" {
		t.Errorf("name: got %v", rows[0]["name"].V)
	}
	got, ok := rows[0]["happened_at"].V.(time.Time)
	if !ok {
		t.Fatalf("happened_at is not time.Time: %T", rows[0]["happened_at"].V)
	}
	if !got.Equal(ts) {
		t.Errorf("date: want %v, got %v", ts, got)
	}
	if db2.Tables["events"].Schema["happened_at"] != KindDate {
		t.Errorf("schema: want KindDate for happened_at")
	}
}

// ── KindJSON — object ────────────────────────────────────────────────────────

func TestPersist_JSONObject(t *testing.T) {
	db := New()
	if err := db.ExecNamed(
		`INSERT INTO docs (id, payload) VALUES (:id, :payload)`,
		map[string]any{
			"id":      int64(1),
			"payload": map[string]any{"key": "value", "score": float64(99)},
		},
	); err != nil {
		t.Fatalf("insert: %v", err)
	}

	db2 := saveLoad(t, db)

	if db2.Tables["docs"].Schema["payload"] != KindJSON {
		t.Errorf("schema: want KindJSON for payload")
	}

	rows := mustQuery(t, db2, `SELECT id, payload FROM docs`)
	if len(rows) != 1 {
		t.Fatalf("want 1 row, got %d", len(rows))
	}
	if rows[0]["payload"].Kind != KindJSON {
		t.Errorf("payload kind: want KindJSON, got %d", rows[0]["payload"].Kind)
	}
	m, ok := rows[0]["payload"].V.(map[string]any)
	if !ok {
		t.Fatalf("payload V is not map[string]any: %T", rows[0]["payload"].V)
	}
	if m["key"] != "value" {
		t.Errorf("payload.key: want 'value', got %v", m["key"])
	}

	// json_extract survives the round-trip.
	extracted := mustQuery(t, db2, `SELECT json_extract(payload, '$.key') AS k FROM docs`)
	if extracted[0]["k"].V != "value" {
		t.Errorf("json_extract after load: got %v", extracted[0]["k"])
	}
}

// ── KindJSON — array (produced by array_agg) ─────────────────────────────────

func TestPersist_JSONArray(t *testing.T) {
	db := New()
	for _, v := range []int64{10, 20, 30} {
		if err := db.ExecNamed(
			`INSERT INTO nums (n) VALUES (:n)`,
			map[string]any{"n": v},
		); err != nil {
			t.Fatalf("insert: %v", err)
		}
	}

	// Materialise an array_agg result into a second table using a JSON wrapper
	// so the value is stored as KindJSON rather than expanded as an IN list.
	agg := mustQuery(t, db, `SELECT array_agg(n) AS arr FROM nums`)
	if err := db.ExecNamed(
		`INSERT INTO agg_results (payload) VALUES (:payload)`,
		map[string]any{"payload": map[string]any{"arr": agg[0]["arr"].V}},
	); err != nil {
		t.Fatalf("insert agg: %v", err)
	}

	db2 := saveLoad(t, db)

	if db2.Tables["agg_results"].Schema["payload"] != KindJSON {
		t.Errorf("schema: want KindJSON for payload")
	}
	rows := mustQuery(t, db2, `SELECT payload FROM agg_results`)
	m, ok := rows[0]["payload"].V.(map[string]any)
	if !ok {
		t.Fatalf("payload V is not map[string]any: %T", rows[0]["payload"].V)
	}
	slice, ok := m["arr"].([]any)
	if !ok {
		t.Fatalf("payload.arr is not []any: %T", m["arr"])
	}
	if len(slice) != 3 {
		t.Errorf("arr: want len=3, got %d: %v", len(slice), slice)
	}
}

// ── EnumSets survive Save/Load ────────────────────────────────────────────────

func TestPersist_EnumSets(t *testing.T) {
	db := New()
	mustExec(t, db, `INSERT INTO orders (id, status) VALUES (1, 'DRAFT')`)
	db.DeclareEnum("orders", "status", "DRAFT", "ACTIVE", "CLOSED")

	db2 := saveLoad(t, db)

	// Enum values should be restored.
	enumVals, ok := db2.Tables["orders"].EnumSets["status"]
	if !ok {
		t.Fatal("EnumSets not restored after Load")
	}
	if len(enumVals) != 3 {
		t.Errorf("want 3 enum values, got %d: %v", len(enumVals), enumVals)
	}

	// Constraint must be active — unlisted value should fail.
	err := db2.Exec(`INSERT INTO orders (id, status) VALUES (2, 'UNKNOWN')`)
	if err == nil {
		t.Error("expected error inserting unlisted enum value after Load, got nil")
	}

	// Listed value must succeed.
	if err := db2.Exec(`INSERT INTO orders (id, status) VALUES (2, 'ACTIVE')`); err != nil {
		t.Errorf("valid enum value rejected after Load: %v", err)
	}
}

// ── Locked flag survives Save/Load ────────────────────────────────────────────

func TestPersist_LockedTable(t *testing.T) {
	db := New()
	// Seed the full schema for this table first.
	mustExec(t, db, `INSERT INTO products (id, name, price) VALUES (1, 'Widget', 9.99)`)
	db.LockTable("products")

	db2 := saveLoad(t, db)

	if !db2.Tables["products"].Locked {
		t.Error("Locked flag not restored after Load")
	}

	// Adding a new column to a locked table must fail.
	err := db2.Exec(`INSERT INTO products (id, name, price, weight) VALUES (2, 'Gadget', 19.99, 1.5)`)
	if err == nil {
		t.Error("expected error adding column to locked table after Load, got nil")
	}

	// Insert without extra column must succeed.
	if err := db2.Exec(`INSERT INTO products (id, name, price) VALUES (2, 'Gadget', 19.99)`); err != nil {
		t.Errorf("valid insert into locked table failed: %v", err)
	}
}

func TestPersist_LockSchema(t *testing.T) {
	db := New()
	mustExec(t, db, `INSERT INTO a (x) VALUES (1)`)
	mustExec(t, db, `INSERT INTO b (y) VALUES ('hello')`)
	db.LockSchema()

	db2 := saveLoad(t, db)

	// Both tables must be locked.
	for _, tbl := range []string{"a", "b"} {
		if !db2.Tables[tbl].Locked {
			t.Errorf("table %q: Locked not restored after Load", tbl)
		}
	}
}

// ── Mixed-kind table: all kinds in one row ────────────────────────────────────

func TestPersist_AllKinds(t *testing.T) {
	db := New()
	ts := time.Date(2025, 1, 2, 3, 4, 5, 0, time.UTC)

	// Use SQL TRUE/FALSE literals so KindBool is actually stored.
	// Use ExecNamed for the remaining types.
	mustExec(t, db, `INSERT INTO mixed (a_bool) VALUES (TRUE)`)
	mustExec(t, db, `UPDATE mixed SET a_bool = TRUE`) // ensure column exists

	// Start fresh with a full row; bool must be a SQL literal.
	db2raw := New()
	if err := db2raw.ExecNamed(`
		INSERT INTO mixed (a_null, a_bool, a_int, a_float, a_str, a_date, a_datetime, a_json)
		VALUES (:a_null, TRUE, :a_int, :a_float, :a_str, :a_date, :a_datetime, :a_json)`,
		map[string]any{
			"a_null":     nil,
			"a_int":      int64(7),
			"a_float":    3.14,
			"a_str":      "hello",
			"a_date":     time.Date(2025, 1, 2, 0, 0, 0, 0, time.UTC), // date-only
			"a_datetime": ts,                                           // has time component
			"a_json":     map[string]any{"ok": true},
		},
	); err != nil {
		t.Fatalf("insert: %v", err)
	}

	db3 := saveLoad(t, db2raw)

	rows := mustQuery(t, db3, `SELECT * FROM mixed`)
	if len(rows) != 1 {
		t.Fatalf("want 1 row, got %d", len(rows))
	}
	r := rows[0]

	checks := []struct {
		col  string
		kind Kind
		v    any
	}{
		{"a_null", KindNull, nil},
		{"a_bool", KindBool, true},
		{"a_int", KindInt, int64(7)},
		{"a_float", KindFloat, 3.14},
		{"a_str", KindString, "hello"},
		{"a_date", KindDate, time.Date(2025, 1, 2, 0, 0, 0, 0, time.UTC)},
		{"a_datetime", KindDate, ts},
	}
	for _, c := range checks {
		if r[c.col].Kind != c.kind {
			t.Errorf("%s: want kind %d, got %d", c.col, c.kind, r[c.col].Kind)
		}
		if c.v != nil && r[c.col].V != c.v {
			t.Errorf("%s: want %v, got %v", c.col, c.v, r[c.col].V)
		}
	}

	// JSON — check separately because map comparison isn't ==.
	if r["a_json"].Kind != KindJSON {
		t.Errorf("a_json: want KindJSON, got %d", r["a_json"].Kind)
	}
	m, _ := r["a_json"].V.(map[string]any)
	if m["ok"] != true {
		t.Errorf("a_json.ok: want true, got %v", m["ok"])
	}
}

// ── Queries still work on a loaded DB (regression) ───────────────────────────

func TestPersist_QueriesAfterLoad(t *testing.T) {
	db := New()
	mustExec(t, db, `INSERT INTO users (id, name, score) VALUES (1, 'Alice', 90)`)
	mustExec(t, db, `INSERT INTO users (id, name, score) VALUES (2, 'Bob', 70)`)
	mustExec(t, db, `INSERT INTO users (id, name, score) VALUES (3, 'Carol', 90)`)

	db2 := saveLoad(t, db)

	// WHERE filter.
	rows := mustQuery(t, db2, `SELECT name FROM users WHERE score = 90 ORDER BY name`)
	if len(rows) != 2 || rows[0]["name"].V != "Alice" || rows[1]["name"].V != "Carol" {
		t.Errorf("WHERE after load: %v", rows)
	}

	// Aggregate.
	rows = mustQuery(t, db2, `SELECT AVG(score) AS avg_score FROM users`)
	avg, _ := rows[0]["avg_score"].V.(float64)
	if avg < 82 || avg > 84 {
		t.Errorf("AVG after load: want ~83.3, got %v", avg)
	}

	// INSERT into loaded DB.
	mustExec(t, db2, `INSERT INTO users (id, name, score) VALUES (4, 'Dave', 80)`)
	rows = mustQuery(t, db2, `SELECT COUNT(*) AS n FROM users`)
	if rows[0]["n"].V != int64(4) {
		t.Errorf("COUNT after insert on loaded DB: want 4, got %v", rows[0]["n"].V)
	}
}

// ── Multiple Save/Load round-trips ────────────────────────────────────────────

func TestPersist_MultipleRoundTrips(t *testing.T) {
	db := New()
	mustExec(t, db, `INSERT INTO items (k, v) VALUES ('a', 1)`)

	for i := 0; i < 3; i++ {
		db = saveLoad(t, db)
		// Add a row between trips to verify the data accumulates correctly.
		mustExec(t, db, `INSERT INTO items (k, v) VALUES ('b', 2)`)
	}

	// After 3 trips we have: original 'a' row + one 'b' per trip = 4 rows.
	rows := mustQuery(t, db, `SELECT COUNT(*) AS n FROM items`)
	if rows[0]["n"].V != int64(4) {
		t.Errorf("after 3 trips: want 4 rows, got %v", rows[0]["n"].V)
	}
}

// ── GenerateDDL on a loaded DB ────────────────────────────────────────────────

func TestPersist_GenerateDDL(t *testing.T) {
	db := New()
	// Insert with SQL TRUE literal so active is stored as KindBool (→ BOOLEAN in DDL).
	mustExec(t, db, `INSERT INTO catalog (id, label, price, active, meta) VALUES (1, 'Widget', 9.99, TRUE, NULL)`)
	if err := db.ExecNamed(
		`UPDATE catalog SET meta = :meta WHERE id = 1`,
		map[string]any{"meta": map[string]any{"tags": []any{"sale"}}},
	); err != nil {
		t.Fatalf("update meta: %v", err)
	}

	db2 := saveLoad(t, db)

	ddl, err2 := db2.GenerateDDL("postgres")
	if err2 != nil {
		t.Fatalf("GenerateDDL: %v", err2)
	}
	if ddl == "" {
		t.Fatal("GenerateDDL returned empty string after Load")
	}
	// Spot-check that expected column types appear (identifiers are quoted).
	for _, want := range []string{
		`CREATE TABLE "catalog"`,
		`"active" BOOLEAN`,
		`"id" BIGINT`,
		`"meta" JSONB`,
		`"price" DOUBLE PRECISION`,
	} {
		if !strings.Contains(ddl, want) {
			t.Errorf("DDL missing %q\n---\n%s", want, ddl)
		}
	}
}
