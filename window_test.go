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
	partBy, orderBy := parseOverClause("")
	if len(partBy) != 0 || len(orderBy) != 0 {
		t.Fatalf("expected empty, got partBy=%v orderBy=%v", partBy, orderBy)
	}
}

func TestParseOverClausePartitionOnly(t *testing.T) {
	partBy, orderBy := parseOverClause("PARTITION BY dept")
	if len(partBy) != 1 || partBy[0] != "dept" {
		t.Fatalf("partBy=%v", partBy)
	}
	if len(orderBy) != 0 {
		t.Fatalf("orderBy=%v", orderBy)
	}
}

func TestParseOverClauseOrderOnly(t *testing.T) {
	partBy, orderBy := parseOverClause("ORDER BY salary DESC")
	if len(partBy) != 0 {
		t.Fatalf("partBy=%v", partBy)
	}
	if len(orderBy) != 1 || orderBy[0].col != "salary" || !orderBy[0].desc {
		t.Fatalf("orderBy=%v", orderBy)
	}
}

func TestParseOverClauseBoth(t *testing.T) {
	partBy, orderBy := parseOverClause("PARTITION BY dept ORDER BY salary DESC")
	if len(partBy) != 1 || partBy[0] != "dept" {
		t.Fatalf("partBy=%v", partBy)
	}
	if len(orderBy) != 1 || orderBy[0].col != "salary" || !orderBy[0].desc {
		t.Fatalf("orderBy=%v", orderBy)
	}
}

func TestParseOverClauseMultiPartition(t *testing.T) {
	partBy, orderBy := parseOverClause("PARTITION BY dept, region ORDER BY salary ASC")
	if len(partBy) != 2 || partBy[0] != "dept" || partBy[1] != "region" {
		t.Fatalf("partBy=%v", partBy)
	}
	if len(orderBy) != 1 || orderBy[0].col != "salary" || orderBy[0].desc {
		t.Fatalf("orderBy=%v", orderBy)
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
