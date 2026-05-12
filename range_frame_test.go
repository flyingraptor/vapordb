package vapordb

// Tests for value-based RANGE frame bounds:
//   RANGE BETWEEN N PRECEDING AND CURRENT ROW
//   RANGE BETWEEN CURRENT ROW AND N FOLLOWING
//   RANGE BETWEEN N PRECEDING AND N FOLLOWING
//   RANGE BETWEEN UNBOUNDED PRECEDING AND N FOLLOWING
//   RANGE BETWEEN N PRECEDING AND UNBOUNDED FOLLOWING
//   Mixed with PARTITION BY, DESC order, floats

import (
	"fmt"
	"testing"
)

// seedRangeDB builds a table with integer scores for range frame tests.
func seedRangeDB(t *testing.T) *DB {
	t.Helper()
	db := New()
	for _, row := range []struct {
		id, score int
		grp       string
	}{
		{1, 10, "a"}, {2, 20, "a"}, {3, 30, "a"}, {4, 40, "a"}, {5, 50, "a"},
		{6, 10, "b"}, {7, 30, "b"},
	} {
		mustExec(t, db, fmt.Sprintf(
			`INSERT INTO scores (id, score, grp) VALUES (%d, %d, '%s')`,
			row.id, row.score, row.grp))
	}
	return db
}

// ── RANGE N PRECEDING AND CURRENT ROW ────────────────────────────────────────

func TestRangeFrame_NPrecedingCurrentRow(t *testing.T) {
	db := seedRangeDB(t)

	// For each row in group "a", sum of scores within [score-15, score].
	// id=1 score=10: window [0..10]   → 10
	// id=2 score=20: window [5..20]   → 10+20=30
	// id=3 score=30: window [15..30]  → 20+30=50
	// id=4 score=40: window [25..40]  → 30+40=70
	// id=5 score=50: window [35..50]  → 40+50=90
	rows := mustQuery(t, db, `
		SELECT id, score,
		       SUM(score) OVER (PARTITION BY grp ORDER BY score
		                        RANGE BETWEEN 15 PRECEDING AND CURRENT ROW) AS ws
		FROM scores WHERE grp = 'a'
		ORDER BY id`)

	expected := []int64{10, 30, 50, 70, 90}
	for i, r := range rows {
		got, _ := r["ws"].V.(int64)
		if got != expected[i] {
			t.Errorf("row %d (score=%v): want ws=%d, got %d", i+1, r["score"].V, expected[i], got)
		}
	}
}

func TestRangeFrame_NPrecedingCurrentRow_Exact(t *testing.T) {
	db := seedRangeDB(t)

	// With N=10: [score-10, score].
	// id=1 score=10: [0..10]  → 10
	// id=2 score=20: [10..20] → 10+20=30
	// id=3 score=30: [20..30] → 20+30=50
	// id=4 score=40: [30..40] → 30+40=70
	// id=5 score=50: [40..50] → 40+50=90
	rows := mustQuery(t, db, `
		SELECT id, score,
		       SUM(score) OVER (PARTITION BY grp ORDER BY score
		                        RANGE BETWEEN 10 PRECEDING AND CURRENT ROW) AS ws
		FROM scores WHERE grp = 'a'
		ORDER BY id`)

	expected := []int64{10, 30, 50, 70, 90}
	for i, r := range rows {
		got, _ := r["ws"].V.(int64)
		if got != expected[i] {
			t.Errorf("row %d: want %d, got %d", i+1, expected[i], got)
		}
	}
}

// ── RANGE CURRENT ROW AND N FOLLOWING ────────────────────────────────────────

func TestRangeFrame_CurrentRowNFollowing(t *testing.T) {
	db := seedRangeDB(t)

	// N=15: window [score, score+15].
	// id=1 score=10: [10..25] → 10+20=30
	// id=2 score=20: [20..35] → 20+30=50
	// id=3 score=30: [30..45] → 30+40=70
	// id=4 score=40: [40..55] → 40+50=90
	// id=5 score=50: [50..65] → 50
	rows := mustQuery(t, db, `
		SELECT id, score,
		       SUM(score) OVER (PARTITION BY grp ORDER BY score
		                        RANGE BETWEEN CURRENT ROW AND 15 FOLLOWING) AS ws
		FROM scores WHERE grp = 'a'
		ORDER BY id`)

	expected := []int64{30, 50, 70, 90, 50}
	for i, r := range rows {
		got, _ := r["ws"].V.(int64)
		if got != expected[i] {
			t.Errorf("row %d: want %d, got %d", i+1, expected[i], got)
		}
	}
}

// ── RANGE N PRECEDING AND N FOLLOWING ────────────────────────────────────────

func TestRangeFrame_NPrecedingNFollowing(t *testing.T) {
	db := seedRangeDB(t)

	// N=15: symmetric window [score-15, score+15].
	// id=1 score=10: [max(0,10-15)..25] → includes 10,20 → 30
	// id=2 score=20: [5..35]  → includes 10,20,30 → 60
	// id=3 score=30: [15..45] → includes 20,30,40 → 90
	// id=4 score=40: [25..55] → includes 30,40,50 → 120
	// id=5 score=50: [35..65] → includes 40,50 → 90
	rows := mustQuery(t, db, `
		SELECT id, score,
		       SUM(score) OVER (PARTITION BY grp ORDER BY score
		                        RANGE BETWEEN 15 PRECEDING AND 15 FOLLOWING) AS ws
		FROM scores WHERE grp = 'a'
		ORDER BY id`)

	expected := []int64{30, 60, 90, 120, 90}
	for i, r := range rows {
		got, _ := r["ws"].V.(int64)
		if got != expected[i] {
			t.Errorf("row %d (score=%v): want ws=%d, got %d", i+1, r["score"].V, expected[i], got)
		}
	}
}

// ── RANGE UNBOUNDED PRECEDING AND N FOLLOWING ────────────────────────────────

func TestRangeFrame_UnboundedPrecedingNFollowing(t *testing.T) {
	db := seedRangeDB(t)

	// N=15 following from start.
	// id=1 score=10: [0..25]  → 10+20=30
	// id=2 score=20: [0..35]  → 10+20+30=60
	// id=3 score=30: [0..45]  → 10+20+30+40=100
	// id=4 score=40: [0..55]  → all 5 = 150
	// id=5 score=50: [0..65]  → all 5 = 150
	rows := mustQuery(t, db, `
		SELECT id, score,
		       SUM(score) OVER (PARTITION BY grp ORDER BY score
		                        RANGE BETWEEN UNBOUNDED PRECEDING AND 15 FOLLOWING) AS ws
		FROM scores WHERE grp = 'a'
		ORDER BY id`)

	expected := []int64{30, 60, 100, 150, 150}
	for i, r := range rows {
		got, _ := r["ws"].V.(int64)
		if got != expected[i] {
			t.Errorf("row %d: want %d, got %d", i+1, expected[i], got)
		}
	}
}

// ── RANGE N PRECEDING AND UNBOUNDED FOLLOWING ────────────────────────────────

func TestRangeFrame_NPrecedingUnboundedFollowing(t *testing.T) {
	db := seedRangeDB(t)

	// N=15: from (score-15) to end.
	// id=1 score=10: [max(0,10-15)=0..∞] → all 5 = 150
	// id=2 score=20: [5..∞]              → all 5 = 150
	// id=3 score=30: [15..∞]             → 20+30+40+50=140
	// id=4 score=40: [25..∞]             → 30+40+50=120
	// id=5 score=50: [35..∞]             → 40+50=90
	rows := mustQuery(t, db, `
		SELECT id, score,
		       SUM(score) OVER (PARTITION BY grp ORDER BY score
		                        RANGE BETWEEN 15 PRECEDING AND UNBOUNDED FOLLOWING) AS ws
		FROM scores WHERE grp = 'a'
		ORDER BY id`)

	expected := []int64{150, 150, 140, 120, 90}
	for i, r := range rows {
		got, _ := r["ws"].V.(int64)
		if got != expected[i] {
			t.Errorf("row %d: want %d, got %d", i+1, expected[i], got)
		}
	}
}

// ── RANGE with COUNT ──────────────────────────────────────────────────────────

func TestRangeFrame_Count(t *testing.T) {
	db := seedRangeDB(t)

	// COUNT within [score-15, score+15]
	// id=1 score=10: [0..25]  → 2 (10,20)
	// id=2 score=20: [5..35]  → 3 (10,20,30)
	// id=3 score=30: [15..45] → 3 (20,30,40)
	// id=4 score=40: [25..55] → 3 (30,40,50)
	// id=5 score=50: [35..65] → 2 (40,50)
	rows := mustQuery(t, db, `
		SELECT id, score,
		       COUNT(*) OVER (PARTITION BY grp ORDER BY score
		                      RANGE BETWEEN 15 PRECEDING AND 15 FOLLOWING) AS cnt
		FROM scores WHERE grp = 'a'
		ORDER BY id`)

	expected := []int64{2, 3, 3, 3, 2}
	for i, r := range rows {
		got, _ := r["cnt"].V.(int64)
		if got != expected[i] {
			t.Errorf("row %d: want cnt=%d, got %d", i+1, expected[i], got)
		}
	}
}

// ── RANGE with AVG ────────────────────────────────────────────────────────────

func TestRangeFrame_Avg(t *testing.T) {
	db := seedRangeDB(t)

	// AVG within [score-10, score+10]
	// id=1 score=10: [0..20]  → (10+20)/2=15
	// id=2 score=20: [10..30] → (10+20+30)/3=20
	// id=3 score=30: [20..40] → (20+30+40)/3=30
	// id=4 score=40: [30..50] → (30+40+50)/3=40
	// id=5 score=50: [40..60] → (40+50)/2=45
	rows := mustQuery(t, db, `
		SELECT id, score,
		       AVG(score) OVER (PARTITION BY grp ORDER BY score
		                        RANGE BETWEEN 10 PRECEDING AND 10 FOLLOWING) AS avg_s
		FROM scores WHERE grp = 'a'
		ORDER BY id`)

	expected := []float64{15, 20, 30, 40, 45}
	for i, r := range rows {
		got, _ := r["avg_s"].V.(float64)
		if got != expected[i] {
			t.Errorf("row %d: want avg=%v, got %v", i+1, expected[i], got)
		}
	}
}

// ── RANGE with no ORDER BY falls back to whole-partition RANGE ────────────────

func TestRangeFrame_NoPrecedingCurrentRowNoOrder(t *testing.T) {
	db := New()
	mustExec(t, db, `INSERT INTO t (v) VALUES (10)`)
	mustExec(t, db, `INSERT INTO t (v) VALUES (20)`)
	mustExec(t, db, `INSERT INTO t (v) VALUES (30)`)

	// Default RANGE with no ORDER BY = whole partition.
	rows := mustQuery(t, db, `
		SELECT v, SUM(v) OVER () AS total FROM t ORDER BY v`)
	for _, r := range rows {
		if r["total"].V != int64(60) {
			t.Errorf("total: want 60, got %v", r["total"].V)
		}
	}
}

// ── RANGE with PARTITION BY and multiple groups ───────────────────────────────

func TestRangeFrame_PartitionBy(t *testing.T) {
	db := seedRangeDB(t)

	// Group "b" has scores 10 and 30.
	// N=5:
	// score=10: [5..15]  → only 10 → sum=10
	// score=30: [25..35] → only 30 → sum=30
	rows := mustQuery(t, db, `
		SELECT id, score, grp,
		       SUM(score) OVER (PARTITION BY grp ORDER BY score
		                        RANGE BETWEEN 5 PRECEDING AND 5 FOLLOWING) AS ws
		FROM scores WHERE grp = 'b'
		ORDER BY score`)

	expected := []int64{10, 30}
	for i, r := range rows {
		got, _ := r["ws"].V.(int64)
		if got != expected[i] {
			t.Errorf("row %d: want %d, got %d", i+1, expected[i], got)
		}
	}
}

// ── RANGE with N=0 (only peers) ───────────────────────────────────────────────

func TestRangeFrame_ZeroPreceding(t *testing.T) {
	db := New()
	// Two rows with the same score = peers.
	mustExec(t, db, `INSERT INTO t2 (id, v) VALUES (1, 10)`)
	mustExec(t, db, `INSERT INTO t2 (id, v) VALUES (2, 10)`)
	mustExec(t, db, `INSERT INTO t2 (id, v) VALUES (3, 20)`)

	// RANGE BETWEEN 0 PRECEDING AND CURRENT ROW = peer group only.
	rows := mustQuery(t, db, `
		SELECT id, v,
		       SUM(v) OVER (ORDER BY v RANGE BETWEEN 0 PRECEDING AND CURRENT ROW) AS ws
		FROM t2 ORDER BY id`)

	// Both rows with v=10 form a peer group → sum=20; v=20 → sum=20.
	if rows[0]["ws"].V != int64(20) || rows[1]["ws"].V != int64(20) {
		t.Errorf("peer rows: want ws=20 for both, got %v and %v", rows[0]["ws"].V, rows[1]["ws"].V)
	}
	if rows[2]["ws"].V != int64(20) {
		t.Errorf("row 3: want ws=20, got %v", rows[2]["ws"].V)
	}
}

// ── DESC order range frame ────────────────────────────────────────────────────

func TestRangeFrame_DescOrder(t *testing.T) {
	db := New()
	for _, v := range []int{50, 40, 30, 20, 10} {
		mustExec(t, db, fmt.Sprintf(`INSERT INTO desc_t (v) VALUES (%d)`, v))
	}

	// DESC order: score sorted 50,40,30,20,10
	// RANGE 15 PRECEDING AND CURRENT ROW with DESC means window [cur, cur+15].
	// v=50: [50..65] → only 50 → sum=50
	// v=40: [40..55] → 50+40=90
	// v=30: [30..45] → 40+30=70
	// v=20: [20..35] → 30+20=50
	// v=10: [10..25] → 20+10=30
	rows := mustQuery(t, db, `
		SELECT v,
		       SUM(v) OVER (ORDER BY v DESC
		                    RANGE BETWEEN 15 PRECEDING AND CURRENT ROW) AS ws
		FROM desc_t ORDER BY v DESC`)

	expected := []int64{50, 90, 70, 50, 30}
	for i, r := range rows {
		got, _ := r["ws"].V.(int64)
		if got != expected[i] {
			t.Errorf("row %d (v=%v): want ws=%d, got %d", i+1, r["v"].V, expected[i], got)
		}
	}
}

// ── Float ORDER BY column ─────────────────────────────────────────────────────

func TestRangeFrame_FloatColumn(t *testing.T) {
	db := New()
	// Use non-integer float literals so named params don't collapse to int.
	mustExec(t, db, `INSERT INTO flt (id, val) VALUES (1, 1.5)`)
	mustExec(t, db, `INSERT INTO flt (id, val) VALUES (2, 2.5)`)
	mustExec(t, db, `INSERT INTO flt (id, val) VALUES (3, 4.0)`)

	// RANGE 2 PRECEDING AND CURRENT ROW (floating point ORDER BY).
	// id=1 val=1.5: [1.5-2, 1.5] = [-0.5, 1.5] → 1.5
	// id=2 val=2.5: [0.5, 2.5]   → 1.5 + 2.5 = 4.0
	// id=3 val=4.0: [2.0, 4.0]   → 2.5 + 4.0 = 6.5
	rows := mustQuery(t, db, `
		SELECT id, val,
		       SUM(val) OVER (ORDER BY val RANGE BETWEEN 2 PRECEDING AND CURRENT ROW) AS ws
		FROM flt ORDER BY id`)

	if len(rows) != 3 {
		t.Fatalf("want 3 rows, got %d", len(rows))
	}
	type wantRow struct{ ws float64 }
	wants := []float64{1.5, 4.0, 6.5}
	for i, r := range rows {
		var got float64
		switch x := r["ws"].V.(type) {
		case int64:
			got = float64(x)
		case float64:
			got = x
		}
		if got != wants[i] {
			t.Errorf("row %d: want ws=%.1f, got %.1f", i+1, wants[i], got)
		}
	}
}
