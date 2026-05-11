package vapordb

import (
	"testing"
)

// ── helpers ───────────────────────────────────────────────────────────────────

func seedWindowDB(t *testing.T) *DB {
	t.Helper()
	db := New()
	rows := []string{
		`INSERT INTO emp (id, dept, name, salary) VALUES (1, 'eng',  'alice', 90000)`,
		`INSERT INTO emp (id, dept, name, salary) VALUES (2, 'eng',  'bob',   80000)`,
		`INSERT INTO emp (id, dept, name, salary) VALUES (3, 'mkt',  'carol', 70000)`,
		`INSERT INTO emp (id, dept, name, salary) VALUES (4, 'mkt',  'dave',  70000)`,
		`INSERT INTO emp (id, dept, name, salary) VALUES (5, 'eng',  'eve',   80000)`,
	}
	for _, q := range rows {
		if err := db.Exec(q); err != nil {
			t.Fatalf("seed: %v", err)
		}
	}
	return db
}

// ── parseOverClause unit tests ────────────────────────────────────────────────

func TestParseOverClauseEmpty(t *testing.T) {
	oc := parseOverClause("")
	if len(oc.partBy) != 0 || len(oc.orderBy) != 0 {
		t.Fatalf("expected empty, got partBy=%v orderBy=%v", oc.partBy, oc.orderBy)
	}
}

func TestParseOverClausePartitionOnly(t *testing.T) {
	oc := parseOverClause("PARTITION BY dept")
	if len(oc.partBy) != 1 || oc.partBy[0] != "dept" {
		t.Fatalf("partBy=%v", oc.partBy)
	}
	if len(oc.orderBy) != 0 {
		t.Fatalf("orderBy=%v", oc.orderBy)
	}
}

func TestParseOverClauseOrderOnly(t *testing.T) {
	oc := parseOverClause("ORDER BY salary DESC")
	if len(oc.partBy) != 0 {
		t.Fatalf("partBy=%v", oc.partBy)
	}
	if len(oc.orderBy) != 1 || oc.orderBy[0].col != "salary" || !oc.orderBy[0].desc {
		t.Fatalf("orderBy=%v", oc.orderBy)
	}
}

func TestParseOverClauseBoth(t *testing.T) {
	oc := parseOverClause("PARTITION BY dept ORDER BY salary DESC")
	if len(oc.partBy) != 1 || oc.partBy[0] != "dept" {
		t.Fatalf("partBy=%v", oc.partBy)
	}
	if len(oc.orderBy) != 1 || oc.orderBy[0].col != "salary" || !oc.orderBy[0].desc {
		t.Fatalf("orderBy=%v", oc.orderBy)
	}
}

func TestParseOverClauseMultiPartition(t *testing.T) {
	oc := parseOverClause("PARTITION BY dept, region ORDER BY salary ASC")
	if len(oc.partBy) != 2 || oc.partBy[0] != "dept" || oc.partBy[1] != "region" {
		t.Fatalf("partBy=%v", oc.partBy)
	}
	if len(oc.orderBy) != 1 || oc.orderBy[0].col != "salary" || oc.orderBy[0].desc {
		t.Fatalf("orderBy=%v", oc.orderBy)
	}
}

func TestParseOverClauseRowsFrame(t *testing.T) {
	oc := parseOverClause("ORDER BY id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW")
	if len(oc.orderBy) != 1 || oc.orderBy[0].col != "id" {
		t.Fatalf("orderBy=%v", oc.orderBy)
	}
	if !oc.hasFrame || !oc.frameRows {
		t.Fatalf("expected ROWS frame, got hasFrame=%v frameRows=%v", oc.hasFrame, oc.frameRows)
	}
	if oc.frameStart.kind != winBoundUnboundedPreceding {
		t.Fatalf("frameStart=%+v", oc.frameStart)
	}
	if oc.frameEnd.kind != winBoundCurrentRow {
		t.Fatalf("frameEnd=%+v", oc.frameEnd)
	}
}

func TestParseOverClauseRangeFrame(t *testing.T) {
	oc := parseOverClause("PARTITION BY dept ORDER BY salary RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING")
	if len(oc.partBy) != 1 || oc.partBy[0] != "dept" {
		t.Fatalf("partBy=%v", oc.partBy)
	}
	if len(oc.orderBy) != 1 || oc.orderBy[0].col != "salary" {
		t.Fatalf("orderBy=%v", oc.orderBy)
	}
	if !oc.hasFrame || oc.frameRows {
		t.Fatalf("expected RANGE frame")
	}
	if oc.frameStart.kind != winBoundUnboundedPreceding || oc.frameEnd.kind != winBoundUnboundedFollowing {
		t.Fatalf("frame bounds: start=%+v end=%+v", oc.frameStart, oc.frameEnd)
	}
}

func TestParseOverClauseNPrecedingFollowing(t *testing.T) {
	oc := parseOverClause("ORDER BY id ROWS BETWEEN 2 PRECEDING AND 1 FOLLOWING")
	if !oc.hasFrame || !oc.frameRows {
		t.Fatalf("expected ROWS frame")
	}
	if oc.frameStart.kind != winBoundPreceding || oc.frameStart.offset != 2 {
		t.Fatalf("frameStart=%+v", oc.frameStart)
	}
	if oc.frameEnd.kind != winBoundFollowing || oc.frameEnd.offset != 1 {
		t.Fatalf("frameEnd=%+v", oc.frameEnd)
	}
}

// ── extractWindowFuncs unit tests ─────────────────────────────────────────────

func TestExtractWindowFuncsCountStar(t *testing.T) {
	sql := "SELECT id, COUNT(*) OVER() AS total FROM emp"
	got, specs, err := extractWindowFuncs(sql)
	if err != nil {
		t.Fatal(err)
	}
	if len(specs) != 1 {
		t.Fatalf("expected 1 spec, got %d", len(specs))
	}
	s := specs[0]
	if s.funcName != "count" || s.arg != "" || s.alias != "total" {
		t.Fatalf("unexpected spec: %+v", s)
	}
	if got != "SELECT id, 0 AS __win_0__ FROM emp" {
		t.Fatalf("rewritten=%q", got)
	}
}

func TestExtractWindowFuncsRowNumberNoAlias(t *testing.T) {
	sql := "SELECT ROW_NUMBER() OVER(ORDER BY id) FROM t"
	_, specs, err := extractWindowFuncs(sql)
	if err != nil {
		t.Fatal(err)
	}
	if len(specs) != 1 {
		t.Fatalf("expected 1 spec")
	}
	s := specs[0]
	if s.funcName != "row_number" || s.alias != "__win_0__" {
		t.Fatalf("spec=%+v", s)
	}
	// ORDER BY columns are rewritten to placeholder names for injection.
	if len(s.orderBy) != 1 || s.orderBy[0].col != "__win_0_k0__" {
		t.Fatalf("orderBy=%v", s.orderBy)
	}
}

func TestExtractWindowFuncsMultiple(t *testing.T) {
	sql := "SELECT ROW_NUMBER() OVER(ORDER BY id) AS rn, SUM(salary) OVER(PARTITION BY dept) AS dept_sum FROM emp"
	_, specs, err := extractWindowFuncs(sql)
	if err != nil {
		t.Fatal(err)
	}
	if len(specs) != 2 {
		t.Fatalf("expected 2 specs, got %d", len(specs))
	}
	if specs[0].funcName != "row_number" || specs[0].alias != "rn" {
		t.Fatalf("specs[0]=%+v", specs[0])
	}
	// arg and partBy are rewritten to placeholder column names during injection.
	if specs[1].funcName != "sum" || specs[1].arg != "__win_1_a__" || specs[1].alias != "dept_sum" {
		t.Fatalf("specs[1]=%+v", specs[1])
	}
}

// ── Integration tests ─────────────────────────────────────────────────────────

func TestWindowCountStarOver(t *testing.T) {
	db := seedWindowDB(t)
	rows, err := db.Query("SELECT id, COUNT(*) OVER() AS total FROM emp ORDER BY id")
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 5 {
		t.Fatalf("expected 5 rows, got %d", len(rows))
	}
	for _, row := range rows {
		if row["total"] != intVal(5) {
			t.Fatalf("expected total=5, got %v", row["total"])
		}
	}
}

// COUNT(*) OVER() must count all rows in the partition, not the post-LIMIT slice.
func TestWindowCountStarOverWithLimit(t *testing.T) {
	db := seedWindowDB(t)
	rows, err := db.Query(`SELECT id, COUNT(*) OVER() AS total FROM emp ORDER BY id LIMIT 2`)
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 2 {
		t.Fatalf("expected 2 rows after LIMIT, got %d", len(rows))
	}
	for _, row := range rows {
		if row["total"] != intVal(5) {
			t.Fatalf("expected total=5 (full table count), got %v", row["total"])
		}
	}
}

func TestWindowCountStarOverWithLimitOffset(t *testing.T) {
	db := seedWindowDB(t)
	rows, err := db.Query(`SELECT id, COUNT(*) OVER() AS total FROM emp ORDER BY id LIMIT 2 OFFSET 1`)
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 2 {
		t.Fatalf("expected 2 rows, got %d", len(rows))
	}
	for _, row := range rows {
		if row["total"] != intVal(5) {
			t.Fatalf("expected total=5, got %v", row["total"])
		}
	}
}

// ROW_NUMBER window is over all rows; final ORDER BY + LIMIT picks which rows appear.
func TestWindowRowNumberOverDescWithFinalOrderLimit(t *testing.T) {
	db := seedWindowDB(t)
	rows, err := db.Query(`
		SELECT id, ROW_NUMBER() OVER(ORDER BY id DESC) AS rn
		FROM emp ORDER BY id LIMIT 2`)
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 2 {
		t.Fatalf("expected 2 rows, got %d", len(rows))
	}
	want := map[int64]int64{1: 5, 2: 4}
	for _, row := range rows {
		id := row["id"].V.(int64)
		rn := row["rn"].V.(int64)
		if want[id] != rn {
			t.Fatalf("id=%d: rn=%d, want %d (window over full set, not LIMIT slice)", id, rn, want[id])
		}
	}
}

func TestWindowRowNumber(t *testing.T) {
	db := seedWindowDB(t)
	rows, err := db.Query("SELECT id, ROW_NUMBER() OVER(ORDER BY id) AS rn FROM emp ORDER BY id")
	if err != nil {
		t.Fatal(err)
	}
	for i, row := range rows {
		if row["rn"] != intVal(int64(i+1)) {
			t.Fatalf("row %d: rn=%v", i, row["rn"])
		}
	}
}

func TestWindowRowNumberDescOrder(t *testing.T) {
	db := seedWindowDB(t)
	rows, err := db.Query("SELECT id, ROW_NUMBER() OVER(ORDER BY id DESC) AS rn FROM emp ORDER BY id")
	if err != nil {
		t.Fatal(err)
	}
	// id=1 should get rn=5, id=5 should get rn=1
	want := map[int64]int64{1: 5, 2: 4, 3: 3, 4: 2, 5: 1}
	for _, row := range rows {
		id := row["id"].V.(int64)
		rn := row["rn"].V.(int64)
		if rn != want[id] {
			t.Fatalf("id=%d: rn=%d, want %d", id, rn, want[id])
		}
	}
}

func TestWindowRank(t *testing.T) {
	db := New()
	for _, q := range []string{
		`INSERT INTO scores (id, score) VALUES (1, 100)`,
		`INSERT INTO scores (id, score) VALUES (2, 90)`,
		`INSERT INTO scores (id, score) VALUES (3, 90)`,
		`INSERT INTO scores (id, score) VALUES (4, 80)`,
	} {
		mustExec(t, db, q)
	}
	rows, err := db.Query("SELECT id, RANK() OVER(ORDER BY score DESC) AS rnk FROM scores ORDER BY id")
	if err != nil {
		t.Fatal(err)
	}
	wantRank := map[int64]int64{1: 1, 2: 2, 3: 2, 4: 4}
	for _, row := range rows {
		id := row["id"].V.(int64)
		rnk := row["rnk"].V.(int64)
		if rnk != wantRank[id] {
			t.Fatalf("id=%d: rank=%d, want %d", id, rnk, wantRank[id])
		}
	}
}

func TestWindowDenseRank(t *testing.T) {
	db := New()
	for _, q := range []string{
		`INSERT INTO scores (id, score) VALUES (1, 100)`,
		`INSERT INTO scores (id, score) VALUES (2, 90)`,
		`INSERT INTO scores (id, score) VALUES (3, 90)`,
		`INSERT INTO scores (id, score) VALUES (4, 80)`,
	} {
		mustExec(t, db, q)
	}
	rows, err := db.Query("SELECT id, DENSE_RANK() OVER(ORDER BY score DESC) AS dr FROM scores ORDER BY id")
	if err != nil {
		t.Fatal(err)
	}
	wantRank := map[int64]int64{1: 1, 2: 2, 3: 2, 4: 3}
	for _, row := range rows {
		id := row["id"].V.(int64)
		dr := row["dr"].V.(int64)
		if dr != wantRank[id] {
			t.Fatalf("id=%d: dense_rank=%d, want %d", id, dr, wantRank[id])
		}
	}
}

func TestWindowPartitionByRowNumber(t *testing.T) {
	db := seedWindowDB(t)
	rows, err := db.Query(`
		SELECT dept, name, salary,
		       ROW_NUMBER() OVER(PARTITION BY dept ORDER BY salary DESC) AS rn
		FROM emp ORDER BY dept, rn`)
	if err != nil {
		t.Fatal(err)
	}
	// eng: alice(90k)=rn1, bob(80k)=rn2, eve(80k)=rn3
	// mkt: carol(70k)=rn1, dave(70k)=rn2
	engRNs := []int64{}
	mktRNs := []int64{}
	for _, row := range rows {
		dept := row["dept"].V.(string)
		rn := row["rn"].V.(int64)
		switch dept {
		case "eng":
			engRNs = append(engRNs, rn)
		case "mkt":
			mktRNs = append(mktRNs, rn)
		}
	}
	if len(engRNs) != 3 || engRNs[0] != 1 {
		t.Fatalf("eng rns=%v", engRNs)
	}
	if len(mktRNs) != 2 || mktRNs[0] != 1 {
		t.Fatalf("mkt rns=%v", mktRNs)
	}
}

func TestWindowSumOver(t *testing.T) {
	db := seedWindowDB(t)
	rows, err := db.Query("SELECT id, salary, SUM(salary) OVER() AS grand_total FROM emp ORDER BY id")
	if err != nil {
		t.Fatal(err)
	}
	// 90000+80000+70000+70000+80000 = 390000
	for _, row := range rows {
		gt := row["grand_total"].V.(int64)
		if gt != 390000 {
			t.Fatalf("grand_total=%d", gt)
		}
	}
}

func TestWindowSumPartitionBy(t *testing.T) {
	db := seedWindowDB(t)
	rows, err := db.Query(`
		SELECT id, dept, salary,
		       SUM(salary) OVER(PARTITION BY dept) AS dept_total
		FROM emp ORDER BY id`)
	if err != nil {
		t.Fatal(err)
	}
	// eng: 90000+80000+80000 = 250000
	// mkt: 70000+70000 = 140000
	wantTotal := map[string]int64{"eng": 250000, "mkt": 140000}
	for _, row := range rows {
		dept := row["dept"].V.(string)
		dt := row["dept_total"].V.(int64)
		if dt != wantTotal[dept] {
			t.Fatalf("dept=%s dept_total=%d want %d", dept, dt, wantTotal[dept])
		}
	}
}

func TestWindowAvgPartitionBy(t *testing.T) {
	db := seedWindowDB(t)
	rows, err := db.Query(`
		SELECT id, dept, salary,
		       AVG(salary) OVER(PARTITION BY dept) AS dept_avg
		FROM emp ORDER BY id`)
	if err != nil {
		t.Fatal(err)
	}
	// eng avg: 250000/3 ≈ 83333.33...
	// mkt avg: 70000
	for _, row := range rows {
		dept := row["dept"].V.(string)
		avg := row["dept_avg"].V.(float64)
		switch dept {
		case "eng":
			if avg < 83333 || avg > 83334 {
				t.Fatalf("eng avg=%f", avg)
			}
		case "mkt":
			if avg != 70000 {
				t.Fatalf("mkt avg=%f", avg)
			}
		}
	}
}

func TestWindowMinMaxOver(t *testing.T) {
	db := seedWindowDB(t)
	rows, err := db.Query(`
		SELECT id,
		       MIN(salary) OVER() AS lo,
		       MAX(salary) OVER() AS hi
		FROM emp ORDER BY id`)
	if err != nil {
		t.Fatal(err)
	}
	for _, row := range rows {
		lo := row["lo"].V.(int64)
		hi := row["hi"].V.(int64)
		if lo != 70000 {
			t.Fatalf("lo=%d", lo)
		}
		if hi != 90000 {
			t.Fatalf("hi=%d", hi)
		}
	}
}

func TestWindowCountStarPartitionBy(t *testing.T) {
	db := seedWindowDB(t)
	rows, err := db.Query(`
		SELECT id, dept, COUNT(*) OVER(PARTITION BY dept) AS dept_count
		FROM emp ORDER BY id`)
	if err != nil {
		t.Fatal(err)
	}
	wantCount := map[string]int64{"eng": 3, "mkt": 2}
	for _, row := range rows {
		dept := row["dept"].V.(string)
		dc := row["dept_count"].V.(int64)
		if dc != wantCount[dept] {
			t.Fatalf("dept=%s count=%d want %d", dept, dc, wantCount[dept])
		}
	}
}

func TestWindowMultipleFuncsInSameQuery(t *testing.T) {
	db := seedWindowDB(t)
	rows, err := db.Query(`
		SELECT id, dept, salary,
		       ROW_NUMBER() OVER(ORDER BY id) AS rn,
		       COUNT(*) OVER() AS total
		FROM emp ORDER BY id`)
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 5 {
		t.Fatalf("expected 5 rows")
	}
	for i, row := range rows {
		if row["rn"].V.(int64) != int64(i+1) {
			t.Fatalf("row %d rn=%v", i, row["rn"])
		}
		if row["total"].V.(int64) != 5 {
			t.Fatalf("row %d total=%v", i, row["total"])
		}
	}
}

func TestWindowWithWhere(t *testing.T) {
	db := seedWindowDB(t)
	// WHERE is applied first; window runs on the filtered set.
	rows, err := db.Query(`
		SELECT id, COUNT(*) OVER() AS total
		FROM emp WHERE dept = 'eng' ORDER BY id`)
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 3 {
		t.Fatalf("expected 3 rows, got %d", len(rows))
	}
	for _, row := range rows {
		if row["total"].V.(int64) != 3 {
			t.Fatalf("total=%v", row["total"])
		}
	}
}

func TestWindowInsideCTE(t *testing.T) {
	db := seedWindowDB(t)
	rows, err := db.Query(`
		WITH ranked AS (
			SELECT id, dept, salary,
			       RANK() OVER(PARTITION BY dept ORDER BY salary DESC) AS rnk
			FROM emp
		)
		SELECT id, dept, salary FROM ranked WHERE rnk = 1 ORDER BY id`)
	if err != nil {
		t.Fatal(err)
	}
	// Top earner per dept: alice(eng,90000), carol or dave (mkt,70000) - both rank 1
	if len(rows) != 3 { // alice + carol + dave all qualify since carol==dave salary
		// Actually carol and dave both have salary=70000, so both rank 1
		t.Fatalf("expected 3 rows, got %d: %v", len(rows), rows)
	}
}

// ── ROWS frame ───────────────────────────────────────────────────────────────

// SUM with explicit ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW → running total.
func TestWindowRunningTotalExplicitROWS(t *testing.T) {
	db := seedWindowDB(t)
	rows, err := db.Query(`
		SELECT id, salary,
		       SUM(salary) OVER(ORDER BY id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_total
		FROM emp ORDER BY id`)
	if err != nil {
		t.Fatal(err)
	}
	// id order: 1(90k),2(80k),3(70k),4(70k),5(80k)
	want := []int64{90000, 170000, 240000, 310000, 390000}
	for i, row := range rows {
		got := row["running_total"].V.(int64)
		if got != want[i] {
			t.Fatalf("row %d: running_total=%d want %d", i, got, want[i])
		}
	}
}

// Without an explicit frame, ORDER BY in OVER triggers the SQL standard default:
// RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW (running aggregate).
func TestWindowRunningTotalImplicitDefault(t *testing.T) {
	db := seedWindowDB(t)
	rows, err := db.Query(`
		SELECT id, salary, SUM(salary) OVER(ORDER BY id) AS running_total
		FROM emp ORDER BY id`)
	if err != nil {
		t.Fatal(err)
	}
	want := []int64{90000, 170000, 240000, 310000, 390000}
	for i, row := range rows {
		got := row["running_total"].V.(int64)
		if got != want[i] {
			t.Fatalf("row %d: running_total=%d want %d", i, got, want[i])
		}
	}
}

// 3-row moving average (1 PRECEDING to 1 FOLLOWING), shrinks at boundaries.
func TestWindowMovingAverage(t *testing.T) {
	db := seedWindowDB(t)
	rows, err := db.Query(`
		SELECT id, salary,
		       AVG(salary) OVER(ORDER BY id ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING) AS moving_avg
		FROM emp ORDER BY id`)
	if err != nil {
		t.Fatal(err)
	}
	// pos=0: avg(90000,80000)=85000; pos=1: avg(90000,80000,70000)=80000;
	// pos=2: avg(80000,70000,70000)≈73333; pos=3: avg(70000,70000,80000)≈73333; pos=4: avg(70000,80000)=75000
	want := []float64{85000, 80000, 73333.333333333336, 73333.333333333336, 75000}
	for i, row := range rows {
		got := row["moving_avg"].V.(float64)
		diff := got - want[i]
		if diff < -1 || diff > 1 {
			t.Fatalf("row %d: moving_avg=%f want ~%f", i, got, want[i])
		}
	}
}

// COUNT with ROWS frame counts only the rows within the frame.
func TestWindowCountWithROWSFrame(t *testing.T) {
	db := seedWindowDB(t)
	rows, err := db.Query(`
		SELECT id, COUNT(*) OVER(ORDER BY id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cnt
		FROM emp ORDER BY id`)
	if err != nil {
		t.Fatal(err)
	}
	for i, row := range rows {
		got := row["cnt"].V.(int64)
		if got != int64(i+1) {
			t.Fatalf("row %d: cnt=%d want %d", i, got, i+1)
		}
	}
}

// RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW groups peers (same ORDER BY value).
func TestWindowRangeFramePeerGrouping(t *testing.T) {
	db := New()
	// salary column has deliberate ties so RANGE vs ROWS produce different results.
	for _, q := range []string{
		`INSERT INTO t2 (id, salary) VALUES (1, 100)`,
		`INSERT INTO t2 (id, salary) VALUES (2, 100)`,
		`INSERT INTO t2 (id, salary) VALUES (3, 200)`,
	} {
		mustExec(t, db, q)
	}
	rows, err := db.Query(`
		SELECT id, SUM(salary) OVER(ORDER BY salary RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS rs
		FROM t2 ORDER BY id`)
	if err != nil {
		t.Fatal(err)
	}
	// salary=100 has two peers; both accumulate to include the full peer group → 200.
	// salary=200 → 200+200 = 400.
	wantRS := map[int64]int64{1: 200, 2: 200, 3: 400}
	for _, row := range rows {
		id := row["id"].V.(int64)
		rs := row["rs"].V.(int64)
		if rs != wantRS[id] {
			t.Fatalf("id=%d rs=%d want %d", id, rs, wantRS[id])
		}
	}
}

// ── LAG ──────────────────────────────────────────────────────────────────────

func TestWindowLag(t *testing.T) {
	db := seedWindowDB(t)
	rows, err := db.Query(`
		SELECT id, salary, LAG(salary) OVER(ORDER BY id) AS prev_salary
		FROM emp ORDER BY id`)
	if err != nil {
		t.Fatal(err)
	}
	// id=1: NULL, id=2: 90000, id=3: 80000, id=4: 70000, id=5: 70000
	wantPrev := map[int64]interface{}{1: nil, 2: int64(90000), 3: int64(80000), 4: int64(70000), 5: int64(70000)}
	for _, row := range rows {
		id := row["id"].V.(int64)
		prev := row["prev_salary"]
		if wantPrev[id] == nil {
			if prev.Kind != KindNull {
				t.Fatalf("id=%d: expected NULL, got %v", id, prev)
			}
		} else {
			if prev.V.(int64) != wantPrev[id].(int64) {
				t.Fatalf("id=%d: prev_salary=%v want %v", id, prev.V, wantPrev[id])
			}
		}
	}
}

func TestWindowLagOffset2(t *testing.T) {
	db := seedWindowDB(t)
	rows, err := db.Query(`
		SELECT id, LAG(salary, 2) OVER(ORDER BY id) AS lag2
		FROM emp ORDER BY id`)
	if err != nil {
		t.Fatal(err)
	}
	wantLag := map[int64]interface{}{1: nil, 2: nil, 3: int64(90000), 4: int64(80000), 5: int64(70000)}
	for _, row := range rows {
		id := row["id"].V.(int64)
		v := row["lag2"]
		if wantLag[id] == nil {
			if v.Kind != KindNull {
				t.Fatalf("id=%d: expected NULL, got %v", id, v)
			}
		} else {
			if v.V.(int64) != wantLag[id].(int64) {
				t.Fatalf("id=%d: lag2=%v want %v", id, v.V, wantLag[id])
			}
		}
	}
}

func TestWindowLagWithDefault(t *testing.T) {
	db := seedWindowDB(t)
	rows, err := db.Query(`
		SELECT id, LAG(salary, 1, 0) OVER(ORDER BY id) AS prev_salary
		FROM emp ORDER BY id`)
	if err != nil {
		t.Fatal(err)
	}
	// id=1 → default 0; others → previous row's salary
	want := map[int64]int64{1: 0, 2: 90000, 3: 80000, 4: 70000, 5: 70000}
	for _, row := range rows {
		id := row["id"].V.(int64)
		got := row["prev_salary"].V.(int64)
		if got != want[id] {
			t.Fatalf("id=%d: prev_salary=%d want %d", id, got, want[id])
		}
	}
}

func TestWindowLagPartitioned(t *testing.T) {
	db := seedWindowDB(t)
	rows, err := db.Query(`
		SELECT id, dept, salary,
		       LAG(salary) OVER(PARTITION BY dept ORDER BY id) AS prev_dept_salary
		FROM emp ORDER BY dept, id`)
	if err != nil {
		t.Fatal(err)
	}
	// eng sorted by id: alice(1,90k)→NULL, bob(2,80k)→90k, eve(5,80k)→80k
	// mkt sorted by id: carol(3,70k)→NULL, dave(4,70k)→70k
	nullIds := map[int64]bool{1: true, 3: true}
	wantPrev := map[int64]int64{2: 90000, 4: 70000, 5: 80000}
	for _, row := range rows {
		id := row["id"].V.(int64)
		prev := row["prev_dept_salary"]
		if nullIds[id] {
			if prev.Kind != KindNull {
				t.Fatalf("id=%d: expected NULL, got %v", id, prev)
			}
		} else {
			if prev.V.(int64) != wantPrev[id] {
				t.Fatalf("id=%d: prev=%v want %d", id, prev.V, wantPrev[id])
			}
		}
	}
}

// ── LEAD ─────────────────────────────────────────────────────────────────────

func TestWindowLead(t *testing.T) {
	db := seedWindowDB(t)
	rows, err := db.Query(`
		SELECT id, salary, LEAD(salary) OVER(ORDER BY id) AS next_salary
		FROM emp ORDER BY id`)
	if err != nil {
		t.Fatal(err)
	}
	// id=1→80000, id=2→70000, id=3→70000, id=4→80000, id=5→NULL
	wantNext := map[int64]interface{}{1: int64(80000), 2: int64(70000), 3: int64(70000), 4: int64(80000), 5: nil}
	for _, row := range rows {
		id := row["id"].V.(int64)
		v := row["next_salary"]
		if wantNext[id] == nil {
			if v.Kind != KindNull {
				t.Fatalf("id=%d: expected NULL, got %v", id, v)
			}
		} else {
			if v.V.(int64) != wantNext[id].(int64) {
				t.Fatalf("id=%d: next_salary=%v want %v", id, v.V, wantNext[id])
			}
		}
	}
}

func TestWindowLeadWithDefault(t *testing.T) {
	db := seedWindowDB(t)
	rows, err := db.Query(`
		SELECT id, LEAD(salary, 1, 0) OVER(ORDER BY id) AS next_salary
		FROM emp ORDER BY id`)
	if err != nil {
		t.Fatal(err)
	}
	// id=5 → default 0 (last row)
	want := map[int64]int64{1: 80000, 2: 70000, 3: 70000, 4: 80000, 5: 0}
	for _, row := range rows {
		id := row["id"].V.(int64)
		got := row["next_salary"].V.(int64)
		if got != want[id] {
			t.Fatalf("id=%d: next_salary=%d want %d", id, got, want[id])
		}
	}
}

// ── FIRST_VALUE / LAST_VALUE / NTH_VALUE ─────────────────────────────────────

func TestWindowFirstValue(t *testing.T) {
	db := seedWindowDB(t)
	rows, err := db.Query(`
		SELECT id, dept,
		       FIRST_VALUE(salary) OVER(PARTITION BY dept ORDER BY id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_sal
		FROM emp ORDER BY id`)
	if err != nil {
		t.Fatal(err)
	}
	// eng first by id: alice(90000). mkt first by id: carol(70000).
	wantFirst := map[string]int64{"eng": 90000, "mkt": 70000}
	for _, row := range rows {
		dept := row["dept"].V.(string)
		got := row["first_sal"].V.(int64)
		if got != wantFirst[dept] {
			t.Fatalf("dept=%s first_sal=%d want %d", dept, got, wantFirst[dept])
		}
	}
}

func TestWindowLastValue(t *testing.T) {
	db := seedWindowDB(t)
	// Need explicit ROWS UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING to see the
	// true last row; the default running frame would only see up to current row.
	rows, err := db.Query(`
		SELECT id, dept,
		       LAST_VALUE(salary) OVER(PARTITION BY dept ORDER BY id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_sal
		FROM emp ORDER BY id`)
	if err != nil {
		t.Fatal(err)
	}
	// eng last by id: eve(id=5, salary=80000). mkt last by id: dave(id=4, salary=70000).
	wantLast := map[string]int64{"eng": 80000, "mkt": 70000}
	for _, row := range rows {
		dept := row["dept"].V.(string)
		got := row["last_sal"].V.(int64)
		if got != wantLast[dept] {
			t.Fatalf("dept=%s last_sal=%d want %d", dept, got, wantLast[dept])
		}
	}
}

func TestWindowNthValue(t *testing.T) {
	db := seedWindowDB(t)
	rows, err := db.Query(`
		SELECT id, dept,
		       NTH_VALUE(salary, 2) OVER(PARTITION BY dept ORDER BY id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_sal
		FROM emp ORDER BY id`)
	if err != nil {
		t.Fatal(err)
	}
	// eng 2nd by id: bob(salary=80000). mkt 2nd by id: dave(salary=70000).
	wantNth := map[string]int64{"eng": 80000, "mkt": 70000}
	for _, row := range rows {
		dept := row["dept"].V.(string)
		got := row["second_sal"].V.(int64)
		if got != wantNth[dept] {
			t.Fatalf("dept=%s second_sal=%d want %d", dept, got, wantNth[dept])
		}
	}
}

// NTH_VALUE beyond the partition size returns NULL.
func TestWindowNthValueOutOfRange(t *testing.T) {
	db := New()
	mustExec(t, db, `INSERT INTO single (id, v) VALUES (1, 42)`)
	rows, err := db.Query(`SELECT NTH_VALUE(v, 5) OVER() AS nth FROM single`)
	if err != nil {
		t.Fatal(err)
	}
	if rows[0]["nth"].Kind != KindNull {
		t.Fatalf("expected NULL for out-of-range NTH_VALUE, got %v", rows[0]["nth"])
	}
}

// ── NTILE ────────────────────────────────────────────────────────────────────

func TestWindowNtile(t *testing.T) {
	db := seedWindowDB(t)
	rows, err := db.Query(`
		SELECT id, NTILE(2) OVER(ORDER BY id) AS bucket
		FROM emp ORDER BY id`)
	if err != nil {
		t.Fatal(err)
	}
	// 5 rows, 2 buckets: first 3 → bucket 1, last 2 → bucket 2.
	wantBucket := map[int64]int64{1: 1, 2: 1, 3: 1, 4: 2, 5: 2}
	for _, row := range rows {
		id := row["id"].V.(int64)
		got := row["bucket"].V.(int64)
		if got != wantBucket[id] {
			t.Fatalf("id=%d: bucket=%d want %d", id, got, wantBucket[id])
		}
	}
}

func TestWindowNtileSingleBucket(t *testing.T) {
	db := seedWindowDB(t)
	rows, err := db.Query(`SELECT id, NTILE(1) OVER(ORDER BY id) AS bucket FROM emp ORDER BY id`)
	if err != nil {
		t.Fatal(err)
	}
	for _, row := range rows {
		if row["bucket"].V.(int64) != 1 {
			t.Fatalf("expected bucket=1, got %v", row["bucket"])
		}
	}
}

func TestWindowNtileMoreBucketsThanRows(t *testing.T) {
	db := New()
	for _, q := range []string{
		`INSERT INTO few (id) VALUES (1)`,
		`INSERT INTO few (id) VALUES (2)`,
	} {
		mustExec(t, db, q)
	}
	rows, err := db.Query(`SELECT id, NTILE(5) OVER(ORDER BY id) AS bucket FROM few ORDER BY id`)
	if err != nil {
		t.Fatal(err)
	}
	// More buckets than rows: each row gets its own bucket.
	for i, row := range rows {
		got := row["bucket"].V.(int64)
		if got != int64(i+1) {
			t.Fatalf("row %d: bucket=%d want %d", i, got, i+1)
		}
	}
}

// ── CUME_DIST ────────────────────────────────────────────────────────────────

func TestWindowCumeDist(t *testing.T) {
	db := seedWindowDB(t)
	rows, err := db.Query(`
		SELECT id, CUME_DIST() OVER(ORDER BY id) AS cd
		FROM emp ORDER BY id`)
	if err != nil {
		t.Fatal(err)
	}
	// No ties (ORDER BY id). cd = (pos+1)/n.
	n := float64(len(rows))
	for i, row := range rows {
		got := row["cd"].V.(float64)
		want := float64(i+1) / n
		if got != want {
			t.Fatalf("row %d: cume_dist=%f want %f", i, got, want)
		}
	}
}

func TestWindowCumeDistWithTies(t *testing.T) {
	db := New()
	for _, q := range []string{
		`INSERT INTO scores2 (id, score) VALUES (1, 100)`,
		`INSERT INTO scores2 (id, score) VALUES (2, 90)`,
		`INSERT INTO scores2 (id, score) VALUES (3, 90)`,
		`INSERT INTO scores2 (id, score) VALUES (4, 80)`,
	} {
		mustExec(t, db, q)
	}
	rows, err := db.Query(`SELECT id, CUME_DIST() OVER(ORDER BY score DESC) AS cd FROM scores2 ORDER BY id`)
	if err != nil {
		t.Fatal(err)
	}
	// score=100 (id=1): last peer pos=0 → cd=1/4=0.25
	// score=90  (id=2,3): last peer pos=2 → cd=3/4=0.75 for both
	// score=80  (id=4): last peer pos=3 → cd=4/4=1.0
	want := map[int64]float64{1: 0.25, 2: 0.75, 3: 0.75, 4: 1.0}
	for _, row := range rows {
		id := row["id"].V.(int64)
		got := row["cd"].V.(float64)
		if got != want[id] {
			t.Fatalf("id=%d: cd=%f want %f", id, got, want[id])
		}
	}
}

// ── PERCENT_RANK ─────────────────────────────────────────────────────────────

func TestWindowPercentRank(t *testing.T) {
	db := seedWindowDB(t)
	rows, err := db.Query(`
		SELECT id, PERCENT_RANK() OVER(ORDER BY id) AS pr
		FROM emp ORDER BY id`)
	if err != nil {
		t.Fatal(err)
	}
	// No ties. pr = (rank-1)/(n-1) = pos/(n-1).
	n := float64(len(rows))
	for i, row := range rows {
		got := row["pr"].V.(float64)
		want := float64(i) / (n - 1)
		if got != want {
			t.Fatalf("row %d: percent_rank=%f want %f", i, got, want)
		}
	}
}

func TestWindowPercentRankSingleRow(t *testing.T) {
	db := New()
	mustExec(t, db, `INSERT INTO one (id) VALUES (1)`)
	rows, err := db.Query(`SELECT PERCENT_RANK() OVER(ORDER BY id) AS pr FROM one`)
	if err != nil {
		t.Fatal(err)
	}
	if rows[0]["pr"].V.(float64) != 0 {
		t.Fatalf("expected 0 for single row, got %v", rows[0]["pr"])
	}
}
