package vapordb

// Tests for InsertStruct / ScanRows pointer and fmt.Stringer support.

import (
	"net"
	"testing"
)

// ── pointer fields ────────────────────────────────────────────────────────────

type WithPointers struct {
	ID    int     `db:"id"`
	Name  *string `db:"name"`  // nil → NULL, non-nil → value
	Score *float64 `db:"score"` // nil → NULL, non-nil → value
	Age   *int    `db:"age"`
}

func strPtr(s string) *string  { return &s }
func f64Ptr(f float64) *float64 { return &f }
func intPtr(i int) *int        { return &i }

func TestInsertStructNilPointerIsNull(t *testing.T) {
	db := New()
	if err := db.InsertStruct("p", WithPointers{ID: 1, Name: nil, Score: nil}); err != nil {
		t.Fatal(err)
	}
	rows := mustQuery(t, db, `SELECT name, score FROM p WHERE id = 1`)
	if rows[0]["name"].Kind != KindNull {
		t.Errorf("nil *string: want NULL, got %v", rows[0]["name"])
	}
	if rows[0]["score"].Kind != KindNull {
		t.Errorf("nil *float64: want NULL, got %v", rows[0]["score"])
	}
}

func TestInsertStructNonNilPointer(t *testing.T) {
	db := New()
	if err := db.InsertStruct("p", WithPointers{ID: 1, Name: strPtr("Alice"), Score: f64Ptr(9.5), Age: intPtr(30)}); err != nil {
		t.Fatal(err)
	}
	rows := mustQuery(t, db, `SELECT name, score, age FROM p WHERE id = 1`)
	if rows[0]["name"] != strVal("Alice") {
		t.Errorf("*string: want Alice, got %v", rows[0]["name"])
	}
	if rows[0]["score"] != floatVal(9.5) {
		t.Errorf("*float64: want 9.5, got %v", rows[0]["score"])
	}
	if rows[0]["age"] != intVal(30) {
		t.Errorf("*int: want 30, got %v", rows[0]["age"])
	}
}

func TestScanRowsIntoPointerFields(t *testing.T) {
	db := New()
	mustExec(t, db, `INSERT INTO p (id, name, score) VALUES (1, 'Bob', 7.5)`)
	mustExec(t, db, `INSERT INTO p (id, name, score) VALUES (2, NULL, NULL)`)

	rows := mustQuery(t, db, `SELECT id, name, score FROM p ORDER BY id`)
	results := ScanRows[WithPointers](rows)

	// row 1: non-null values
	if results[0].Name == nil || *results[0].Name != "Bob" {
		t.Errorf("*string scan: want Bob, got %v", results[0].Name)
	}
	if results[0].Score == nil || *results[0].Score != 7.5 {
		t.Errorf("*float64 scan: want 7.5, got %v", results[0].Score)
	}

	// row 2: NULL columns → nil pointers
	if results[1].Name != nil {
		t.Errorf("NULL *string: want nil, got %v", results[1].Name)
	}
	if results[1].Score != nil {
		t.Errorf("NULL *float64: want nil, got %v", results[1].Score)
	}
}

func TestScanRowsPointerRoundTrip(t *testing.T) {
	db := New()
	original := WithPointers{ID: 1, Name: strPtr("Carol"), Score: f64Ptr(88.0), Age: intPtr(28)}
	if err := db.InsertStruct("p", original); err != nil {
		t.Fatal(err)
	}
	rows := mustQuery(t, db, `SELECT id, name, score, age FROM p`)
	got := ScanRows[WithPointers](rows)

	if got[0].ID != original.ID {
		t.Errorf("ID: want %d, got %d", original.ID, got[0].ID)
	}
	if got[0].Name == nil || *got[0].Name != *original.Name {
		t.Errorf("Name: want %v, got %v", original.Name, got[0].Name)
	}
	if got[0].Score == nil || *got[0].Score != *original.Score {
		t.Errorf("Score: want %v, got %v", original.Score, got[0].Score)
	}
	if got[0].Age == nil || *got[0].Age != *original.Age {
		t.Errorf("Age: want %v, got %v", original.Age, got[0].Age)
	}
}

// ── fmt.Stringer (net.IP) ─────────────────────────────────────────────────────

// net.IP implements fmt.Stringer (String() returns "1.2.3.4")
// and encoding.TextUnmarshaler (UnmarshalText parses it back).

type WithIP struct {
	ID int    `db:"id"`
	IP net.IP `db:"ip"`
}

func TestInsertStructStringer(t *testing.T) {
	db := New()
	ip := net.ParseIP("192.168.1.1")
	if err := db.InsertStruct("hosts", WithIP{ID: 1, IP: ip}); err != nil {
		t.Fatal(err)
	}
	rows := mustQuery(t, db, `SELECT ip FROM hosts WHERE id = 1`)
	// stored as the string representation
	if rows[0]["ip"].Kind != KindString {
		t.Errorf("Stringer: want KindString, got %v", rows[0]["ip"].Kind)
	}
	if rows[0]["ip"].V != ip.String() {
		t.Errorf("Stringer: want %q, got %q", ip.String(), rows[0]["ip"].V)
	}
}

func TestScanRowsTextUnmarshaler(t *testing.T) {
	db := New()
	mustExec(t, db, `INSERT INTO hosts (id, ip) VALUES (1, '10.0.0.1')`)

	rows := mustQuery(t, db, `SELECT id, ip FROM hosts`)
	results := ScanRows[WithIP](rows)

	want := net.ParseIP("10.0.0.1")
	if !results[0].IP.Equal(want) {
		t.Errorf("TextUnmarshaler scan: want %v, got %v", want, results[0].IP)
	}
}

func TestIPRoundTrip(t *testing.T) {
	db := New()
	original := WithIP{ID: 1, IP: net.ParseIP("172.16.0.42")}
	if err := db.InsertStruct("hosts", original); err != nil {
		t.Fatal(err)
	}
	rows := mustQuery(t, db, `SELECT id, ip FROM hosts`)
	got := ScanRows[WithIP](rows)

	if !got[0].IP.Equal(original.IP) {
		t.Errorf("IP round-trip: want %v, got %v", original.IP, got[0].IP)
	}
}

// ── embedded anonymous structs (ScanRows) ────────────────────────────────────

type RegionCore struct {
	ID   int64  `db:"id"`
	Name string `db:"name"`
}

type regionWithStat struct {
	RegionCore
	TotalCount int `db:"total_count"`
}

func TestScanRowsEmbeddedAnonymousStruct(t *testing.T) {
	db := New()
	mustExec(t, db, `INSERT INTO regions (id, name) VALUES (10, 'North')`)
	rows := mustQuery(t, db, `SELECT id, name, 42 AS total_count FROM regions WHERE id = 10`)
	out := ScanRows[regionWithStat](rows)
	if len(out) != 1 {
		t.Fatalf("want 1 row, got %d", len(out))
	}
	if out[0].ID != 10 || out[0].Name != "North" || out[0].TotalCount != 42 {
		t.Fatalf("embedded scan: got %+v", out[0])
	}
}

type EmbedPtrBase struct {
	N int `db:"n"`
}

type rowWithPtrEmbed struct {
	*EmbedPtrBase
	K string `db:"k"`
}

func TestScanRowsEmbeddedPointerStructAllocates(t *testing.T) {
	rows := []Row{{"n": intVal(7), "k": strVal("hi")}}
	out := ScanRows[rowWithPtrEmbed](rows)
	if out[0].EmbedPtrBase == nil || out[0].N != 7 {
		t.Fatalf("nil *embed should allocate: got %+v", out[0])
	}
	if out[0].K != "hi" {
		t.Fatalf("K: %q", out[0].K)
	}
}

// ── nil Stringer pointer ──────────────────────────────────────────────────────

type WithIPPtr struct {
	ID int    `db:"id"`
	IP *net.IP `db:"ip"`
}

func TestNilStringerPointerIsNull(t *testing.T) {
	db := New()
	if err := db.InsertStruct("hosts", WithIPPtr{ID: 1, IP: nil}); err != nil {
		t.Fatal(err)
	}
	rows := mustQuery(t, db, `SELECT ip FROM hosts WHERE id = 1`)
	if rows[0]["ip"].Kind != KindNull {
		t.Errorf("nil *net.IP: want NULL, got %v", rows[0]["ip"])
	}
}
