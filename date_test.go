package vapordb

// Tests for date / datetime support.

import (
	"testing"
	"time"
)

// ─── shared fixture ───────────────────────────────────────────────────────────

// dateDB creates a table with a KindDate column using DATE() literals.
//
//	events (id, name, created_at)
//	  1  → 'launch'    2024-01-15
//	  2  → 'beta'      2024-06-01
//	  3  → 'release'   2024-12-31
//	  4  → 'preview'   2023-08-20
//	  5  → 'hotfix'    NULL
func dateDB(t *testing.T) *DB {
	t.Helper()
	db := New()
	mustExec(t, db, `INSERT INTO events (id, name, created_at) VALUES (1, 'launch',  DATE('2024-01-15'))`)
	mustExec(t, db, `INSERT INTO events (id, name, created_at) VALUES (2, 'beta',    DATE('2024-06-01'))`)
	mustExec(t, db, `INSERT INTO events (id, name, created_at) VALUES (3, 'release', DATE('2024-12-31'))`)
	mustExec(t, db, `INSERT INTO events (id, name, created_at) VALUES (4, 'preview', DATE('2023-08-20'))`)
	mustExec(t, db, `INSERT INTO events (id, name, created_at) VALUES (5, 'hotfix',  NULL)`)
	return db
}

func dateVal(s string) Value {
	t, ok := tryParseDate(s)
	if !ok {
		panic("dateVal: cannot parse " + s)
	}
	return Value{Kind: KindDate, V: t}
}

// ─── basic storage and retrieval ─────────────────────────────────────────────

func TestDateInsertAndSelect(t *testing.T) {
	db := dateDB(t)
	rows := mustQuery(t, db, `SELECT id, created_at FROM events WHERE id = 1`)
	if len(rows) != 1 {
		t.Fatalf("want 1 row, got %d", len(rows))
	}
	got := rows[0]["created_at"]
	if got.Kind != KindDate {
		t.Fatalf("want KindDate, got Kind=%d", got.Kind)
	}
	want := dateVal("2024-01-15")
	if Compare(got, want) != 0 {
		t.Errorf("want 2024-01-15, got %v", got.V)
	}
}

func TestDateNullColumn(t *testing.T) {
	db := dateDB(t)
	rows := mustQuery(t, db, `SELECT created_at FROM events WHERE id = 5`)
	if len(rows) != 1 {
		t.Fatalf("want 1 row, got %d", len(rows))
	}
	if rows[0]["created_at"].Kind != KindNull {
		t.Errorf("expected NULL for id=5")
	}
}

// ─── date comparisons in WHERE ────────────────────────────────────────────────

func TestDateGreaterThan(t *testing.T) {
	db := dateDB(t)
	// id 1,2,3 are in 2024; id 4 is in 2023; id 5 is NULL
	rows := mustQuery(t, db, `SELECT id FROM events WHERE created_at > '2024-01-01' ORDER BY id`)
	wantIDs := []int64{1, 2, 3}
	if got := ids(rows); !eqInt64Slice(got, wantIDs) {
		t.Errorf("want %v, got %v", wantIDs, got)
	}
}

func TestDateLessThan(t *testing.T) {
	db := dateDB(t)
	rows := mustQuery(t, db, `SELECT id FROM events WHERE created_at < '2024-01-01' ORDER BY id`)
	wantIDs := []int64{4}
	if got := ids(rows); !eqInt64Slice(got, wantIDs) {
		t.Errorf("want %v, got %v", wantIDs, got)
	}
}

func TestDateEqual(t *testing.T) {
	db := dateDB(t)
	rows := mustQuery(t, db, `SELECT id FROM events WHERE created_at = '2024-06-01'`)
	if len(rows) != 1 || rows[0]["id"] != intVal(2) {
		t.Errorf("= date: want [2], got %v", ids(rows))
	}
}

// ─── BETWEEN ─────────────────────────────────────────────────────────────────

func TestDateBetween(t *testing.T) {
	db := dateDB(t)
	rows := mustQuery(t, db, `SELECT id FROM events WHERE created_at BETWEEN '2024-01-01' AND '2024-06-30' ORDER BY id`)
	wantIDs := []int64{1, 2}
	if got := ids(rows); !eqInt64Slice(got, wantIDs) {
		t.Errorf("BETWEEN: want %v, got %v", wantIDs, got)
	}
}

func TestDateNotBetween(t *testing.T) {
	db := dateDB(t)
	// NOT BETWEEN '2024-01-01' AND '2024-06-30' → id 3 (2024-12-31), id 4 (2023-08-20); id 5 NULL excluded
	rows := mustQuery(t, db, `SELECT id FROM events WHERE created_at NOT BETWEEN '2024-01-01' AND '2024-06-30' ORDER BY id`)
	wantIDs := []int64{3, 4}
	if got := ids(rows); !eqInt64Slice(got, wantIDs) {
		t.Errorf("NOT BETWEEN: want %v, got %v", wantIDs, got)
	}
}

// ─── ORDER BY date ────────────────────────────────────────────────────────────

func TestDateOrderByAsc(t *testing.T) {
	db := dateDB(t)
	rows := mustQuery(t, db, `SELECT id FROM events WHERE created_at IS NOT NULL ORDER BY created_at ASC`)
	wantIDs := []int64{4, 1, 2, 3} // 2023-08-20, 2024-01-15, 2024-06-01, 2024-12-31
	if got := ids(rows); !eqInt64Slice(got, wantIDs) {
		t.Errorf("ORDER BY ASC: want %v, got %v", wantIDs, got)
	}
}

func TestDateOrderByDesc(t *testing.T) {
	db := dateDB(t)
	rows := mustQuery(t, db, `SELECT id FROM events WHERE created_at IS NOT NULL ORDER BY created_at DESC`)
	wantIDs := []int64{3, 2, 1, 4}
	if got := ids(rows); !eqInt64Slice(got, wantIDs) {
		t.Errorf("ORDER BY DESC: want %v, got %v", wantIDs, got)
	}
}

// ─── MIN / MAX ────────────────────────────────────────────────────────────────

func TestDateMinMax(t *testing.T) {
	db := dateDB(t)
	rows := mustQuery(t, db, `SELECT MIN(created_at) AS mn, MAX(created_at) AS mx FROM events`)
	if len(rows) != 1 {
		t.Fatalf("want 1 row, got %d", len(rows))
	}
	mn, mx := rows[0]["mn"], rows[0]["mx"]
	if Compare(mn, dateVal("2023-08-20")) != 0 {
		t.Errorf("MIN: want 2023-08-20, got %v", mn.V)
	}
	if Compare(mx, dateVal("2024-12-31")) != 0 {
		t.Errorf("MAX: want 2024-12-31, got %v", mx.V)
	}
}

// ─── Date functions ───────────────────────────────────────────────────────────

func TestDateFuncYearMonthDay(t *testing.T) {
	db := dateDB(t)
	rows := mustQuery(t, db, `SELECT YEAR(created_at) AS y, MONTH(created_at) AS mo, DAY(created_at) AS d FROM events WHERE id = 2`)
	if len(rows) != 1 {
		t.Fatalf("want 1 row, got %d", len(rows))
	}
	r := rows[0]
	if r["y"] != intVal(2024) {
		t.Errorf("YEAR: want 2024, got %v", r["y"].V)
	}
	if r["mo"] != intVal(6) {
		t.Errorf("MONTH: want 6, got %v", r["mo"].V)
	}
	if r["d"] != intVal(1) {
		t.Errorf("DAY: want 1, got %v", r["d"].V)
	}
}

func TestDateFuncDatediff(t *testing.T) {
	db := New()
	mustExec(t, db, `INSERT INTO t (a, b) VALUES (DATE('2024-01-10'), DATE('2024-01-01'))`)
	rows := mustQuery(t, db, `SELECT DATEDIFF(a, b) AS diff FROM t`)
	if len(rows) != 1 {
		t.Fatalf("want 1 row, got %d", len(rows))
	}
	if rows[0]["diff"] != intVal(9) {
		t.Errorf("DATEDIFF: want 9, got %v", rows[0]["diff"].V)
	}
}

func TestDateFuncDateFormat(t *testing.T) {
	db := dateDB(t)
	rows := mustQuery(t, db, `SELECT DATE_FORMAT(created_at, '%Y/%m/%d') AS fmt FROM events WHERE id = 3`)
	if len(rows) != 1 {
		t.Fatalf("want 1 row, got %d", len(rows))
	}
	if rows[0]["fmt"] != strVal("2024/12/31") {
		t.Errorf("DATE_FORMAT: want '2024/12/31', got %v", rows[0]["fmt"].V)
	}
}

func TestDateFuncNow(t *testing.T) {
	db := New()
	rows := mustQuery(t, db, `SELECT NOW() AS ts`)
	if len(rows) != 1 {
		t.Fatalf("want 1 row, got %d", len(rows))
	}
	ts := rows[0]["ts"]
	if ts.Kind != KindDate {
		t.Errorf("NOW() should return KindDate, got Kind=%d", ts.Kind)
	}
	now := time.Now()
	diff := now.Sub(valueToTime(ts)).Abs()
	if diff > 5*time.Second {
		t.Errorf("NOW() returned time too far from now: %v", ts.V)
	}
}

func TestDateFuncCurdate(t *testing.T) {
	db := New()
	rows := mustQuery(t, db, `SELECT CURDATE() AS d`)
	if len(rows) != 1 {
		t.Fatalf("want 1 row, got %d", len(rows))
	}
	d := rows[0]["d"]
	if d.Kind != KindDate {
		t.Errorf("CURDATE() should return KindDate")
	}
	today := time.Now().UTC()
	got := valueToTime(d)
	if got.Year() != today.Year() || got.Month() != today.Month() || got.Day() != today.Day() {
		t.Errorf("CURDATE() mismatch: want %v, got %v", today.Format("2006-01-02"), got.Format("2006-01-02"))
	}
}

func TestDateFuncDateAdd(t *testing.T) {
	db := New()
	mustExec(t, db, `INSERT INTO t (d) VALUES (DATE('2024-01-01'))`)
	rows := mustQuery(t, db, `SELECT DATE_ADD(d, INTERVAL 10 DAY) AS result FROM t`)
	if len(rows) != 1 {
		t.Fatalf("want 1 row, got %d", len(rows))
	}
	want := dateVal("2024-01-11")
	if Compare(rows[0]["result"], want) != 0 {
		t.Errorf("DATE_ADD: want 2024-01-11, got %v", rows[0]["result"].V)
	}
}

func TestDateFuncDateSub(t *testing.T) {
	db := New()
	mustExec(t, db, `INSERT INTO t (d) VALUES (DATE('2024-03-01'))`)
	rows := mustQuery(t, db, `SELECT DATE_SUB(d, INTERVAL 1 MONTH) AS result FROM t`)
	if len(rows) != 1 {
		t.Fatalf("want 1 row, got %d", len(rows))
	}
	want := dateVal("2024-02-01")
	if Compare(rows[0]["result"], want) != 0 {
		t.Errorf("DATE_SUB: want 2024-02-01, got %v", rows[0]["result"].V)
	}
}

// ─── Struct mapping ───────────────────────────────────────────────────────────

type eventRecord struct {
	ID        int64     `db:"id"`
	Name      string    `db:"name"`
	CreatedAt time.Time `db:"created_at"`
}

func TestDateInsertStruct(t *testing.T) {
	db := New()
	ev := eventRecord{
		ID:        1,
		Name:      "launch",
		CreatedAt: time.Date(2024, 3, 15, 0, 0, 0, 0, time.UTC),
	}
	if err := db.InsertStruct("events", ev); err != nil {
		t.Fatalf("InsertStruct: %v", err)
	}
	rows := mustQuery(t, db, `SELECT id, created_at FROM events WHERE id = 1`)
	if len(rows) != 1 {
		t.Fatalf("want 1 row, got %d", len(rows))
	}
	if rows[0]["created_at"].Kind != KindDate {
		t.Errorf("expected KindDate after InsertStruct, got Kind=%d", rows[0]["created_at"].Kind)
	}
	want := dateVal("2024-03-15")
	if Compare(rows[0]["created_at"], want) != 0 {
		t.Errorf("wrong date after InsertStruct: %v", rows[0]["created_at"].V)
	}
}

func TestDateScanRows(t *testing.T) {
	db := New()
	mustExec(t, db, `INSERT INTO events (id, name, created_at) VALUES (1, 'launch', DATE('2024-03-15'))`)
	rows, err := db.Query(`SELECT id, name, created_at FROM events`)
	if err != nil {
		t.Fatalf("Query: %v", err)
	}
	events := ScanRows[eventRecord](rows)
	if len(events) != 1 {
		t.Fatalf("want 1 event, got %d", len(events))
	}
	want := time.Date(2024, 3, 15, 0, 0, 0, 0, time.UTC)
	if !events[0].CreatedAt.Equal(want) {
		t.Errorf("ScanRows: want %v, got %v", want, events[0].CreatedAt)
	}
}

// ─── Persistence (Save / Load) ────────────────────────────────────────────────

func TestDateSaveLoad(t *testing.T) {
	db := dateDB(t)
	path := t.TempDir() + "/dates.json"
	if err := db.Save(path); err != nil {
		t.Fatalf("Save: %v", err)
	}

	db2 := New()
	if err := db2.Load(path); err != nil {
		t.Fatalf("Load: %v", err)
	}

	rows := mustQuery(t, db2, `SELECT id FROM events WHERE created_at BETWEEN '2024-01-01' AND '2024-12-31' ORDER BY id`)
	wantIDs := []int64{1, 2, 3}
	if got := ids(rows); !eqInt64Slice(got, wantIDs) {
		t.Errorf("after Load BETWEEN: want %v, got %v", wantIDs, got)
	}
}

// ─── IS NULL / IS NOT NULL on dates ──────────────────────────────────────────

func TestDateIsNull(t *testing.T) {
	db := dateDB(t)
	rows := mustQuery(t, db, `SELECT id FROM events WHERE created_at IS NULL`)
	if len(rows) != 1 || rows[0]["id"] != intVal(5) {
		t.Errorf("IS NULL: want [5], got %v", ids(rows))
	}
}

func TestDateIsNotNull(t *testing.T) {
	db := dateDB(t)
	rows := mustQuery(t, db, `SELECT id FROM events WHERE created_at IS NOT NULL ORDER BY id`)
	wantIDs := []int64{1, 2, 3, 4}
	if got := ids(rows); !eqInt64Slice(got, wantIDs) {
		t.Errorf("IS NOT NULL: want %v, got %v", wantIDs, got)
	}
}

// ─── IN with dates ────────────────────────────────────────────────────────────

func TestDateIn(t *testing.T) {
	db := dateDB(t)
	rows := mustQuery(t, db, `SELECT id FROM events WHERE created_at IN ('2024-01-15', '2024-12-31') ORDER BY id`)
	wantIDs := []int64{1, 3}
	if got := ids(rows); !eqInt64Slice(got, wantIDs) {
		t.Errorf("IN: want %v, got %v", wantIDs, got)
	}
}

// ─── YEAR() filter ────────────────────────────────────────────────────────────

func TestDateFilterByYear(t *testing.T) {
	db := dateDB(t)
	rows := mustQuery(t, db, `SELECT id FROM events WHERE YEAR(created_at) = 2024 ORDER BY id`)
	wantIDs := []int64{1, 2, 3}
	if got := ids(rows); !eqInt64Slice(got, wantIDs) {
		t.Errorf("YEAR filter: want %v, got %v", wantIDs, got)
	}
}

// ─── helpers ─────────────────────────────────────────────────────────────────

func eqInt64Slice(a, b []int64) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
