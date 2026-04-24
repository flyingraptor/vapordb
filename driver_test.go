package vapordb

// Tests for driver.Valuer / sql.Scanner support in InsertStruct and ScanRows.
//
// Uses two concrete types from the standard library:
//   - sql.NullString  (implements both driver.Valuer and sql.Scanner)
//   - sql.NullInt64   (implements both driver.Valuer and sql.Scanner)
//
// And a minimal custom type to demonstrate the contract.

import (
	"database/sql"
	"database/sql/driver"
	"fmt"
	"testing"
)

// ── sql.NullString ────────────────────────────────────────────────────────────

type WithNullString struct {
	ID    int            `db:"id"`
	Label sql.NullString `db:"label"`
}

func TestInsertStructDriverValuerValidString(t *testing.T) {
	db := New()
	if err := db.InsertStruct("t", WithNullString{
		ID:    1,
		Label: sql.NullString{String: "hello", Valid: true},
	}); err != nil {
		t.Fatal(err)
	}
	rows := mustQuery(t, db, `SELECT label FROM t WHERE id = 1`)
	if rows[0]["label"] != strVal("hello") {
		t.Errorf("NullString valid: want 'hello', got %v", rows[0]["label"])
	}
}

func TestInsertStructDriverValuerNullString(t *testing.T) {
	db := New()
	if err := db.InsertStruct("t", WithNullString{
		ID:    1,
		Label: sql.NullString{Valid: false},
	}); err != nil {
		t.Fatal(err)
	}
	rows := mustQuery(t, db, `SELECT label FROM t WHERE id = 1`)
	if rows[0]["label"].Kind != KindNull {
		t.Errorf("NullString null: want KindNull, got %v", rows[0]["label"])
	}
}

func TestScanRowsSQLScanner(t *testing.T) {
	db := New()
	mustExec(t, db, `INSERT INTO t (id, label) VALUES (1, 'world')`)
	mustExec(t, db, `INSERT INTO t (id, label) VALUES (2, NULL)`)

	rows := mustQuery(t, db, `SELECT id, label FROM t ORDER BY id`)
	results := ScanRows[WithNullString](rows)

	if !results[0].Label.Valid || results[0].Label.String != "world" {
		t.Errorf("Scanner valid: want {world true}, got %v", results[0].Label)
	}
	if results[1].Label.Valid {
		t.Errorf("Scanner null: want {false}, got %v", results[1].Label)
	}
}

func TestNullStringRoundTrip(t *testing.T) {
	db := New()
	rows := []WithNullString{
		{ID: 1, Label: sql.NullString{String: "foo", Valid: true}},
		{ID: 2, Label: sql.NullString{Valid: false}},
	}
	for _, r := range rows {
		if err := db.InsertStruct("t", r); err != nil {
			t.Fatal(err)
		}
	}
	got := ScanRows[WithNullString](mustQuery(t, db, `SELECT id, label FROM t ORDER BY id`))

	if !got[0].Label.Valid || got[0].Label.String != "foo" {
		t.Errorf("round-trip[0]: want {foo true}, got %v", got[0].Label)
	}
	if got[1].Label.Valid {
		t.Errorf("round-trip[1]: want {false}, got %v", got[1].Label)
	}
}

// ── sql.NullInt64 ─────────────────────────────────────────────────────────────

type WithNullInt struct {
	ID    int          `db:"id"`
	Score sql.NullInt64 `db:"score"`
}

func TestNullInt64RoundTrip(t *testing.T) {
	db := New()
	db.InsertStruct("t", WithNullInt{ID: 1, Score: sql.NullInt64{Int64: 42, Valid: true}})
	db.InsertStruct("t", WithNullInt{ID: 2, Score: sql.NullInt64{Valid: false}})

	got := ScanRows[WithNullInt](mustQuery(t, db, `SELECT id, score FROM t ORDER BY id`))

	if !got[0].Score.Valid || got[0].Score.Int64 != 42 {
		t.Errorf("NullInt64[0]: want {42 true}, got %v", got[0].Score)
	}
	if got[1].Score.Valid {
		t.Errorf("NullInt64[1]: want {false}, got %v", got[1].Score)
	}
}

// ── custom type ───────────────────────────────────────────────────────────────

// Color is a custom string type that stores itself as a hex code via driver.Valuer
// and restores itself via sql.Scanner.
type Color struct{ Hex string }

func (c Color) Value() (driver.Value, error) {
	if c.Hex == "" {
		return nil, nil
	}
	return c.Hex, nil
}

func (c *Color) Scan(src any) error {
	if src == nil {
		c.Hex = ""
		return nil
	}
	switch v := src.(type) {
	case string:
		c.Hex = v
	default:
		c.Hex = fmt.Sprintf("%v", v)
	}
	return nil
}

type WithColor struct {
	ID    int   `db:"id"`
	Color Color `db:"color"`
}

func TestCustomValuerScanner(t *testing.T) {
	db := New()
	if err := db.InsertStruct("palette", WithColor{ID: 1, Color: Color{Hex: "#ff0000"}}); err != nil {
		t.Fatal(err)
	}
	if err := db.InsertStruct("palette", WithColor{ID: 2, Color: Color{}}); err != nil {
		t.Fatal(err)
	}

	rows := mustQuery(t, db, `SELECT id, color FROM palette ORDER BY id`)

	// row 1: stored as string "#ff0000"
	if rows[0]["color"] != strVal("#ff0000") {
		t.Errorf("custom Valuer: want '#ff0000', got %v", rows[0]["color"])
	}
	// row 2: empty Hex → nil → NULL
	if rows[1]["color"].Kind != KindNull {
		t.Errorf("custom Valuer null: want KindNull, got %v", rows[1]["color"])
	}

	// Scan back
	got := ScanRows[WithColor](rows)
	if got[0].Color.Hex != "#ff0000" {
		t.Errorf("custom Scanner: want '#ff0000', got %q", got[0].Color.Hex)
	}
	if got[1].Color.Hex != "" {
		t.Errorf("custom Scanner null: want '', got %q", got[1].Color.Hex)
	}
}

func TestCustomValuerRoundTrip(t *testing.T) {
	db := New()
	original := []WithColor{
		{ID: 1, Color: Color{Hex: "#00ff00"}},
		{ID: 2, Color: Color{Hex: "#0000ff"}},
	}
	for _, r := range original {
		db.InsertStruct("palette", r)
	}

	got := ScanRows[WithColor](mustQuery(t, db, `SELECT id, color FROM palette ORDER BY id`))
	for i, o := range original {
		if got[i].Color.Hex != o.Color.Hex {
			t.Errorf("round-trip[%d]: want %q, got %q", i, o.Color.Hex, got[i].Color.Hex)
		}
	}
}
