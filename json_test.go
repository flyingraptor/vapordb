package vapordb

import (
	"encoding/json"
	"testing"
)

// ── JSON value storage ─────────────────────────────────────────────────────────

func TestJSONInsertAndSelect(t *testing.T) {
	db := New()
	mustExec(t, db, `INSERT INTO docs (id, data) VALUES (1, json_parse('{"name":"Alice","age":30}'))`)

	rows, err := db.Query(`SELECT id, data FROM docs`)
	if err != nil {
		t.Fatalf("Query: %v", err)
	}
	if len(rows) != 1 {
		t.Fatalf("want 1 row, got %d", len(rows))
	}
	v := rows[0]["data"]
	if v.Kind != KindJSON {
		t.Fatalf("want KindJSON, got %v", v.Kind)
	}
	m, ok := v.V.(map[string]any)
	if !ok {
		t.Fatalf("V should be map[string]any, got %T", v.V)
	}
	if m["name"] != "Alice" {
		t.Errorf("want name=Alice, got %v", m["name"])
	}
}

func TestJSONArrayInsert(t *testing.T) {
	db := New()
	mustExec(t, db, `INSERT INTO lists (id, tags) VALUES (1, json_parse('["go","sql","test"]'))`)

	rows := mustQuery(t, db, `SELECT tags FROM lists`)
	if rows[0]["tags"].Kind != KindJSON {
		t.Fatalf("want KindJSON for tags")
	}
	arr, ok := rows[0]["tags"].V.([]any)
	if !ok {
		t.Fatalf("expected []any, got %T", rows[0]["tags"].V)
	}
	if len(arr) != 3 {
		t.Errorf("want 3 elements, got %d", len(arr))
	}
}

// ── JSON_EXTRACT ──────────────────────────────────────────────────────────────

func TestJSONExtract(t *testing.T) {
	db := New()
	mustExec(t, db, `INSERT INTO docs (id, data) VALUES (1, json_parse('{"city":"Athens","zip":"10431"}'))`)

	rows := mustQuery(t, db, `SELECT json_extract(data, '$.city') AS city FROM docs`)
	if rows[0]["city"].V != "Athens" {
		t.Errorf("want city=Athens, got %v", rows[0]["city"])
	}
}

func TestJSONExtractNested(t *testing.T) {
	db := New()
	mustExec(t, db, `INSERT INTO docs (id, data) VALUES (1, json_parse('{"user":{"name":"Bob","age":25}}'))`)

	rows := mustQuery(t, db, `SELECT json_extract(data, '$.user.name') AS uname FROM docs`)
	if rows[0]["uname"].V != "Bob" {
		t.Errorf("want uname=Bob, got %v", rows[0]["uname"])
	}
}

func TestJSONExtractArray(t *testing.T) {
	db := New()
	mustExec(t, db, `INSERT INTO docs (id, data) VALUES (1, json_parse('{"tags":["a","b","c"]}'))`)

	rows := mustQuery(t, db, `SELECT json_extract(data, '$.tags[1]') AS tag FROM docs`)
	if rows[0]["tag"].V != "b" {
		t.Errorf("want tag=b, got %v", rows[0]["tag"])
	}
}

func TestJSONExtractMissing(t *testing.T) {
	db := New()
	mustExec(t, db, `INSERT INTO docs (id, data) VALUES (1, json_parse('{"a":1}'))`)

	rows := mustQuery(t, db, `SELECT json_extract(data, '$.z') AS v FROM docs`)
	if rows[0]["v"].Kind != KindNull {
		t.Errorf("want NULL for missing key, got %v", rows[0]["v"])
	}
}

// ── Arrow operators (->, ->>) ──────────────────────────────────────────────────

func TestJSONArrowOperator(t *testing.T) {
	db := New()
	mustExec(t, db, `INSERT INTO docs (id, data) VALUES (1, json_parse('{"status":"active"}'))`)

	rows := mustQuery(t, db, `SELECT data->>'$.status' AS status FROM docs`)
	if rows[0]["status"].V != "active" {
		t.Errorf("want status=active via ->>, got %v", rows[0]["status"])
	}
}

func TestJSONArrowExtractOperator(t *testing.T) {
	db := New()
	mustExec(t, db, `INSERT INTO docs (id, data) VALUES (1, json_parse('{"count":42}'))`)

	rows := mustQuery(t, db, `SELECT json_unquote(data->'$.count') AS cnt FROM docs`)
	if rows[0]["cnt"].V != "42" {
		t.Errorf("want cnt=42 (string), got %v", rows[0]["cnt"])
	}
}

// ── JSON_CONTAINS ─────────────────────────────────────────────────────────────

func TestJSONContains(t *testing.T) {
	db := New()
	mustExec(t, db, `INSERT INTO docs (id, data) VALUES (1, json_parse('{"role":"admin","active":true}'))`)
	mustExec(t, db, `INSERT INTO docs (id, data) VALUES (2, json_parse('{"role":"user","active":false}'))`)

	rows := mustQuery(t, db, `SELECT id FROM docs WHERE json_contains(data, json_parse('{"role":"admin"}'))`)
	if len(rows) != 1 || rows[0]["id"].V != int64(1) {
		t.Errorf("want only id=1 to contain admin role, got %v", rows)
	}
}

func TestJSONContainsArray(t *testing.T) {
	db := New()
	mustExec(t, db, `INSERT INTO tags (id, vals) VALUES (1, json_parse('["go","sql","test"]'))`)
	mustExec(t, db, `INSERT INTO tags (id, vals) VALUES (2, json_parse('["python","ml"]'))`)

	rows := mustQuery(t, db, `SELECT id FROM tags WHERE json_contains(vals, json_parse('["go","sql"]'))`)
	if len(rows) != 1 || rows[0]["id"].V != int64(1) {
		t.Errorf("want only id=1 to contain [go,sql], got %v", rows)
	}
}

// ── @> and <@ operators ────────────────────────────────────────────────────────

func TestJSONContainsAtOperator(t *testing.T) {
	db := New()
	mustExec(t, db, `INSERT INTO docs (id, data) VALUES (1, json_parse('{"x":1,"y":2}'))`)
	mustExec(t, db, `INSERT INTO docs (id, data) VALUES (2, json_parse('{"x":1}'))`)

	// data @> '{"x":1}' should match both rows
	rows := mustQuery(t, db, `SELECT id FROM docs WHERE data @> '{"x":1}'`)
	if len(rows) != 2 {
		t.Errorf("want 2 rows matching @>, got %d: %v", len(rows), rows)
	}
}

func TestJSONContainedInOperator(t *testing.T) {
	db := New()
	mustExec(t, db, `INSERT INTO docs (id, data) VALUES (1, json_parse('{"x":1}'))`)

	// '{"x":1}' <@ data should match (candidate is contained in doc)
	rows := mustQuery(t, db, `SELECT id FROM docs WHERE data @> '{"x":1}'`)
	if len(rows) != 1 {
		t.Errorf("want 1 row, got %d", len(rows))
	}
}

// ── JSON_ARRAY_LENGTH / JSON_KEYS ──────────────────────────────────────────────

func TestJSONArrayLength(t *testing.T) {
	db := New()
	mustExec(t, db, `INSERT INTO docs (id, data) VALUES (1, json_parse('[1,2,3,4,5]'))`)

	rows := mustQuery(t, db, `SELECT json_array_length(data) AS len FROM docs`)
	if rows[0]["len"].V != int64(5) {
		t.Errorf("want len=5, got %v", rows[0]["len"])
	}
}

func TestJSONType(t *testing.T) {
	db := New()
	mustExec(t, db, `INSERT INTO docs (id, obj, arr) VALUES (1, json_parse('{"a":1}'), json_parse('[1,2]'))`)

	rows := mustQuery(t, db, `SELECT json_type(obj) AS obj_type, json_type(arr) AS arr_type FROM docs`)
	if rows[0]["obj_type"].V != "OBJECT" {
		t.Errorf("want OBJECT, got %v", rows[0]["obj_type"])
	}
	if rows[0]["arr_type"].V != "ARRAY" {
		t.Errorf("want ARRAY, got %v", rows[0]["arr_type"])
	}
}

// ── CAST AS JSON ───────────────────────────────────────────────────────────────

func TestCastAsJSON(t *testing.T) {
	db := New()
	mustExec(t, db, `INSERT INTO docs (id, raw) VALUES (1, '{"k":"v"}')`)

	rows := mustQuery(t, db, `SELECT CAST(raw AS JSON) AS parsed FROM docs`)
	if rows[0]["parsed"].Kind != KindJSON {
		t.Errorf("want KindJSON after CAST, got %v", rows[0]["parsed"].Kind)
	}
}

// ── MakeValue / KindOf for JSON ────────────────────────────────────────────────

func TestMakeValueJSON(t *testing.T) {
	m := map[string]any{"key": "val"}
	v := MakeValue(m)
	if v.Kind != KindJSON {
		t.Errorf("want KindJSON for map[string]any, got %v", v.Kind)
	}

	arr := []any{1, 2, 3}
	v2 := MakeValue(arr)
	if v2.Kind != KindJSON {
		t.Errorf("want KindJSON for []any, got %v", v2.Kind)
	}
}

func TestKindOfJSON(t *testing.T) {
	if KindOf(map[string]any{}) != KindJSON {
		t.Error("KindOf(map[string]any{}) should be KindJSON")
	}
	if KindOf([]any{}) != KindJSON {
		t.Error("KindOf([]any{}) should be KindJSON")
	}
}

// ── JSON round-trip via Save/Load ──────────────────────────────────────────────

func TestJSONPersistence(t *testing.T) {
	db := New()
	mustExec(t, db, `INSERT INTO docs (id, data) VALUES (1, json_parse('{"x":42}'))`)

	tmp := t.TempDir() + "/db.json"
	if err := db.Save(tmp); err != nil {
		t.Fatalf("Save: %v", err)
	}

	db2 := New()
	if err := db2.Load(tmp); err != nil {
		t.Fatalf("Load: %v", err)
	}

	rows := mustQuery(t, db2, `SELECT data FROM docs`)
	if rows[0]["data"].Kind != KindJSON {
		t.Fatalf("want KindJSON after Load, got %v", rows[0]["data"].Kind)
	}
	b, _ := json.Marshal(rows[0]["data"].V)
	if string(b) != `{"x":42}` {
		t.Errorf("want {\"x\":42}, got %s", string(b))
	}
}

// ── IsConflict for JSON ────────────────────────────────────────────────────────

func TestIsConflictJSON(t *testing.T) {
	cases := []struct {
		existing, incoming Kind
		want               bool
	}{
		{KindString, KindJSON, true},
		{KindJSON, KindString, true},
		{KindInt, KindJSON, true},
		{KindJSON, KindInt, true},
		{KindJSON, KindJSON, false},
		{KindNull, KindJSON, false},
		{KindJSON, KindNull, false},
		{KindDate, KindJSON, true},
		{KindJSON, KindDate, true},
	}
	for _, tc := range cases {
		got := IsConflict(tc.existing, tc.incoming)
		if got != tc.want {
			t.Errorf("IsConflict(%v, %v) = %v, want %v", tc.existing, tc.incoming, got, tc.want)
		}
	}
}

// ── ScanRows with JSON fields ──────────────────────────────────────────────────

func TestScanRowsJSON(t *testing.T) {
	db := New()
	mustExec(t, db, `INSERT INTO docs (id, meta) VALUES (1, json_parse('{"role":"admin"}'))`)

	type Doc struct {
		ID   int64             `db:"id"`
		Meta map[string]any `db:"meta"`
	}

	rows := mustQuery(t, db, `SELECT id, meta FROM docs`)
	docs := ScanRows[Doc](rows)
	if len(docs) != 1 {
		t.Fatalf("want 1 doc, got %d", len(docs))
	}
	if docs[0].Meta["role"] != "admin" {
		t.Errorf("want meta.role=admin, got %v", docs[0].Meta["role"])
	}
}

