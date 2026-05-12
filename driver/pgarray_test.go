package driver

// Tests for PostgreSQL array parameter binding at the database/sql driver level.
//
// The driver's driverValueToSQL expands PG array literals (produced by
// pq.Array(slice).Value()) into SQL comma-separated literal lists.
// CheckNamedValue converts plain Go slices to PG array literals so that
// database/sql does not reject them before they reach the driver.

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"fmt"
	"testing"

	"github.com/flyingraptor/vapordb"
)

// pgArrayValuer is a test stub that mimics pq.Array's driver.Valuer, returning
// a PostgreSQL array literal string without importing lib/pq.
type pgArrayValuer struct{ pgLiteral string }

func (a pgArrayValuer) Value() (driver.Value, error) { return a.pgLiteral, nil }

func newTestDB(t *testing.T) (*sql.DB, *vapordb.DB) {
	t.Helper()
	vdb := vapordb.New()
	name := fmt.Sprintf("test-%p", t)
	Register(name, vdb)
	t.Cleanup(func() { Unregister(name) })
	sqlDB, err := sql.Open("vapordb", name)
	if err != nil {
		t.Fatalf("sql.Open: %v", err)
	}
	t.Cleanup(func() { sqlDB.Close() })
	return sqlDB, vdb
}

// ── Unit tests for driverExpandPGArray ───────────────────────────────────────

func TestDriverExpandPGArray_Strings(t *testing.T) {
	got, ok := driverExpandPGArray("{A001,A002}")
	if !ok || got != "'A001', 'A002'" {
		t.Errorf("got (%q, %v), want ('A001', 'A002', true)", got, ok)
	}
}

func TestDriverExpandPGArray_Integers(t *testing.T) {
	got, ok := driverExpandPGArray("{10,20,30}")
	if !ok || got != "10, 20, 30" {
		t.Errorf("got (%q, %v)", got, ok)
	}
}

func TestDriverExpandPGArray_Empty(t *testing.T) {
	got, ok := driverExpandPGArray("{}")
	if !ok || got != "NULL" {
		t.Errorf("got (%q, %v), want (NULL, true)", got, ok)
	}
}

func TestDriverExpandPGArray_NotArray(t *testing.T) {
	_, ok := driverExpandPGArray("hello world")
	if ok {
		t.Error("plain string must not be treated as array")
	}
}

func TestDriverExpandPGArray_WithNULL(t *testing.T) {
	got, ok := driverExpandPGArray("{NULL,foo}")
	if !ok || got != "NULL, 'foo'" {
		t.Errorf("got (%q, %v)", got, ok)
	}
}

// ── Integration: pq.Array-style Valuer via $N placeholder ────────────────────

func TestDriver_PGArray_DollarPlaceholder(t *testing.T) {
	sqlDB, _ := newTestDB(t)

	_, err := sqlDB.ExecContext(context.Background(),
		`INSERT INTO accts (code, amount) VALUES ('A001', 100)`)
	if err != nil {
		t.Fatal(err)
	}
	_, err = sqlDB.ExecContext(context.Background(),
		`INSERT INTO accts (code, amount) VALUES ('A002', 200)`)
	if err != nil {
		t.Fatal(err)
	}
	_, err = sqlDB.ExecContext(context.Background(),
		`INSERT INTO accts (code, amount) VALUES ('A003', 300)`)
	if err != nil {
		t.Fatal(err)
	}

	// pgArrayValuer simulates pq.Array([]string{"A001","A002"}).Value()
	rows, err := sqlDB.QueryContext(context.Background(),
		`SELECT code, amount FROM accts WHERE code = ANY($1) ORDER BY code`,
		pgArrayValuer{"{A001,A002}"},
	)
	if err != nil {
		t.Fatalf("QueryContext: %v", err)
	}
	defer rows.Close()

	// vapordb returns columns in alphabetical order: amount, code
	var results []struct {
		Code   string
		Amount int64
	}
	for rows.Next() {
		var r struct {
			Code   string
			Amount int64
		}
		if err := rows.Scan(&r.Amount, &r.Code); err != nil {
			t.Fatal(err)
		}
		results = append(results, r)
	}
	if len(results) != 2 {
		t.Fatalf("expected 2 rows, got %d", len(results))
	}
	if results[0].Code != "A001" || results[1].Code != "A002" {
		t.Errorf("unexpected codes: %v", results)
	}
}

func TestDriver_PGArray_QMarkPlaceholder(t *testing.T) {
	sqlDB, _ := newTestDB(t)

	for _, row := range []struct{ code string; amt int }{
		{"X001", 10}, {"X002", 20}, {"X003", 30},
	} {
		if _, err := sqlDB.ExecContext(context.Background(),
			`INSERT INTO xaccts (code, amt) VALUES (?, ?)`,
			row.code, row.amt); err != nil {
			t.Fatal(err)
		}
	}

	rows, err := sqlDB.QueryContext(context.Background(),
		`SELECT code, amt FROM xaccts WHERE code = ANY(?) ORDER BY code`,
		pgArrayValuer{"{X001,X003}"},
	)
	if err != nil {
		t.Fatalf("QueryContext: %v", err)
	}
	defer rows.Close()

	// vapordb returns columns alphabetically: amt, code
	var codes []string
	for rows.Next() {
		var code string
		var amt int64
		if err := rows.Scan(&amt, &code); err != nil {
			t.Fatal(err)
		}
		codes = append(codes, code)
	}
	if len(codes) != 2 || codes[0] != "X001" || codes[1] != "X003" {
		t.Errorf("got %v", codes)
	}
}

func TestDriver_PGArray_PlainSliceViaCheckNamedValue(t *testing.T) {
	// Plain Go slices are NOT valid driver.Value types; CheckNamedValue
	// should intercept them and convert them to PG array literals.
	sqlDB, _ := newTestDB(t)

	for _, code := range []string{"P001", "P002", "P003"} {
		if _, err := sqlDB.ExecContext(context.Background(),
			`INSERT INTO parts (code) VALUES (?)`, code); err != nil {
			t.Fatal(err)
		}
	}

	rows, err := sqlDB.QueryContext(context.Background(),
		`SELECT code FROM parts WHERE code = ANY($1) ORDER BY code`,
		[]string{"P001", "P003"},
	)
	if err != nil {
		t.Fatalf("QueryContext with plain []string: %v", err)
	}
	defer rows.Close()

	var codes []string
	for rows.Next() {
		var c string
		if err := rows.Scan(&c); err != nil {
			t.Fatal(err)
		}
		codes = append(codes, c)
	}
	if len(codes) != 2 || codes[0] != "P001" || codes[1] != "P003" {
		t.Errorf("plain []string: got %v", codes)
	}
}

func TestDriver_PGArray_PlainIntSliceViaCheckNamedValue(t *testing.T) {
	sqlDB, _ := newTestDB(t)

	for _, n := range []int64{1, 2, 3, 4, 5} {
		if _, err := sqlDB.ExecContext(context.Background(),
			`INSERT INTO ns (n) VALUES (?)`, n); err != nil {
			t.Fatal(err)
		}
	}

	rows, err := sqlDB.QueryContext(context.Background(),
		`SELECT n FROM ns WHERE n = ANY($1) ORDER BY n`,
		[]int64{2, 4},
	)
	if err != nil {
		t.Fatalf("QueryContext with plain []int64: %v", err)
	}
	defer rows.Close()

	var ns []int64
	for rows.Next() {
		var n int64
		if err := rows.Scan(&n); err != nil {
			t.Fatal(err)
		}
		ns = append(ns, n)
	}
	if len(ns) != 2 || ns[0] != 2 || ns[1] != 4 {
		t.Errorf("plain []int64: got %v", ns)
	}
}

func TestDriver_PGArray_EmptySlice(t *testing.T) {
	sqlDB, _ := newTestDB(t)

	if _, err := sqlDB.ExecContext(context.Background(),
		`INSERT INTO items (x) VALUES (?)`, "something"); err != nil {
		t.Fatal(err)
	}

	// Empty pq.Array → no rows should match.
	rows, err := sqlDB.QueryContext(context.Background(),
		`SELECT x FROM items WHERE x = ANY($1)`,
		pgArrayValuer{"{}"},
	)
	if err != nil {
		t.Fatalf("QueryContext: %v", err)
	}
	defer rows.Close()
	count := 0
	for rows.Next() {
		count++
	}
	if count != 0 {
		t.Errorf("empty array: expected 0 rows, got %d", count)
	}
}

func TestDriver_PGArray_EmptyPlainSlice(t *testing.T) {
	sqlDB, _ := newTestDB(t)

	if _, err := sqlDB.ExecContext(context.Background(),
		`INSERT INTO items2 (x) VALUES (?)`, "something"); err != nil {
		t.Fatal(err)
	}

	rows, err := sqlDB.QueryContext(context.Background(),
		`SELECT x FROM items2 WHERE x = ANY($1)`,
		[]string{},
	)
	if err != nil {
		t.Fatalf("QueryContext with empty slice: %v", err)
	}
	defer rows.Close()
	count := 0
	for rows.Next() {
		count++
	}
	if count != 0 {
		t.Errorf("empty slice: expected 0 rows, got %d", count)
	}
}

func TestDriver_PGArray_NotIn(t *testing.T) {
	// <> ALL($1) → NOT IN ($1)
	sqlDB, _ := newTestDB(t)

	for _, c := range []string{"red", "green", "blue"} {
		if _, err := sqlDB.ExecContext(context.Background(),
			`INSERT INTO colors (c) VALUES (?)`, c); err != nil {
			t.Fatal(err)
		}
	}

	rows, err := sqlDB.QueryContext(context.Background(),
		`SELECT c FROM colors WHERE c <> ALL($1) ORDER BY c`,
		pgArrayValuer{"{red,green}"},
	)
	if err != nil {
		t.Fatalf("QueryContext: %v", err)
	}
	defer rows.Close()

	var got []string
	for rows.Next() {
		var c string
		if err := rows.Scan(&c); err != nil {
			t.Fatal(err)
		}
		got = append(got, c)
	}
	if len(got) != 1 || got[0] != "blue" {
		t.Errorf("NOT IN: expected [blue], got %v", got)
	}
}

func TestDriver_PGArray_UUIDs(t *testing.T) {
	// UUIDs are just strings from the vapordb perspective.
	sqlDB, _ := newTestDB(t)

	uuids := []string{
		"550e8400-e29b-41d4-a716-446655440000",
		"123e4567-e89b-12d3-a456-426614174000",
		"aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee",
	}
	for _, u := range uuids {
		if _, err := sqlDB.ExecContext(context.Background(),
			`INSERT INTO uuids (id) VALUES (?)`, u); err != nil {
			t.Fatal(err)
		}
	}

	// Simulate pq.Array([]uuid.UUID{uuid1, uuid2}).Value()
	pgLit := fmt.Sprintf("{%s,%s}", uuids[0], uuids[1])
	rows, err := sqlDB.QueryContext(context.Background(),
		`SELECT id FROM uuids WHERE id = ANY($1) ORDER BY id`,
		pgArrayValuer{pgLit},
	)
	if err != nil {
		t.Fatalf("QueryContext UUIDs: %v", err)
	}
	defer rows.Close()

	var found []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			t.Fatal(err)
		}
		found = append(found, id)
	}
	if len(found) != 2 {
		t.Fatalf("expected 2 UUIDs, got %d: %v", len(found), found)
	}
}

// ── driverSliceToPGArray ──────────────────────────────────────────────────────

func TestDriverSliceToPGArray(t *testing.T) {
	tests := []struct {
		name  string
		input any
		want  string
	}{
		{"string slice", []string{"A001", "A002"}, "{A001,A002}"},
		{"int64 slice", []int64{1, 2, 3}, "{1,2,3}"},
		{"empty slice", []string{}, "{}"},
		{"single element", []string{"only"}, "{only}"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			import_reflect_via_driverSliceToPGArray(t, tc.input, tc.want)
		})
	}
}

// import_reflect_via_driverSliceToPGArray avoids a direct import of reflect.
func import_reflect_via_driverSliceToPGArray(t *testing.T, v any, want string) {
	t.Helper()
	// The CheckNamedValue method populates nv.Value; invoke it indirectly
	// through the driver by passing v to a real query.
	// For unit test, call driverSliceToPGArray via reflect ourselves:
	// — we exercise it through the public binding path instead.
	sqlDB, _ := newTestDB(t)
	// ensure table exists
	sqlDB.ExecContext(context.Background(), `INSERT INTO dummy (x) VALUES (1)`)

	nv := &driver.NamedValue{Ordinal: 1, Value: v}
	s := &stmt{}
	err := s.CheckNamedValue(nv)
	if err != nil && err != driver.ErrSkip {
		t.Fatalf("CheckNamedValue error: %v", err)
	}
	if got, ok := nv.Value.(string); ok {
		if got != want {
			t.Errorf("CheckNamedValue: got %q, want %q", got, want)
		}
	} else if want != "" {
		t.Errorf("CheckNamedValue: nv.Value is not string: %T", nv.Value)
	}
}
