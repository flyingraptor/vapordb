package vapordb

// Stress tests for the join fast path, and regression guards against the
// nested-loop bottleneck they replaced. Originally joins ran as nested loops
// over full-table scans (see applyJoin in executor.go), so a COUNT over joined
// N-sized tables cost ~O(N^k) for a k-table join and per-row time grew with N.
//
// These tests exercise the two join shapes that matter, using a generic
// three-level parent/child/grandchild schema:
//
//	3-table join:  orders ⋈ order_items ⋈ shipments
//	2-table join:  order_items ⋈ shipments
//
// The 3-table chain is the important one: each extra JOIN multiplies the
// nested-loop cross-product. With the hash join, every link in the chain is
// O(N), so the whole pipeline stays linear — if any single join silently fell
// back to a nested-loop scan, the 3-table test below would go quadratic and
// fail.

import (
	"fmt"
	"runtime"
	"testing"
	"time"
)

// buildJoinChainDB builds a generic three-level schema with a 1:1:1 mapping so
// the counts are exactly n and the join fan-out is fixed (isolating the join
// algorithm's cost from result-size growth):
//
//	orders(id, region_id)                 — n rows, all in region 1
//	order_items(id, order_id, active)     — n rows, order_id → orders.id
//	shipments(id, order_item_id)          — n rows, order_item_id → order_items.id
func buildJoinChainDB(tb testing.TB, n int) *DB {
	tb.Helper()
	db := New()
	for i := 1; i <= n; i++ {
		mustSeed(tb, db, fmt.Sprintf(
			`INSERT INTO orders (id, region_id) VALUES (%d, 1)`, i))
		mustSeed(tb, db, fmt.Sprintf(
			`INSERT INTO order_items (id, order_id, active) VALUES (%d, %d, true)`, i, i))
		mustSeed(tb, db, fmt.Sprintf(
			`INSERT INTO shipments (id, order_item_id) VALUES (%d, %d)`, i, i))
	}
	return db
}

func mustSeed(tb testing.TB, db *DB, sql string) {
	tb.Helper()
	if err := db.Exec(sql); err != nil {
		tb.Fatalf("seed %q: %v", sql, err)
	}
}

// countThreeTable is a 3-table join COUNT with a filter on the root table.
const countThreeTable = `
	SELECT COUNT(*) AS n
	FROM orders o
	JOIN order_items oi ON o.id = oi.order_id
	JOIN shipments s ON oi.id = s.order_item_id
	WHERE o.region_id = 1 AND oi.active = true`

// countTwoTable is the 2-table sibling join COUNT.
const countTwoTable = `
	SELECT COUNT(*) AS n
	FROM order_items oi
	JOIN shipments s ON oi.id = s.order_item_id`

// countThreeTableMixedOn is a 3-table join whose middle ON carries a
// single-table filter alongside the equi-join term (equi + residual). This is
// the shape that previously fell back to the nested loop; the hash join now
// splits it into an equi key plus a residual predicate and stays O(N).
const countThreeTableMixedOn = `
	SELECT COUNT(*) AS n
	FROM orders o
	JOIN order_items oi ON o.id = oi.order_id AND oi.active = true
	JOIN shipments s ON oi.id = s.order_item_id
	WHERE o.region_id = 1`

func TestThreeTableJoinScaling(t *testing.T) {
	assertLinearScaling(t, "3-table join", countThreeTable)
}

func TestTwoTableJoinScaling(t *testing.T) {
	assertLinearScaling(t, "2-table join", countTwoTable)
}

func TestMixedOnJoinScaling(t *testing.T) {
	assertLinearScaling(t, "3-table join, mixed ON (equi + filter)", countThreeTableMixedOn)
}

// assertLinearScaling runs a COUNT query at doubling table sizes and asserts
// the read path is ~O(N): with the hash join, per-item time (total / N) stays
// roughly flat as N grows. A regression to the nested-loop join makes per-item
// cost scale with N (roughly doubling each time N doubles) and fails the test.
// See git history for the pre-fix numbers that proved the original
// O(N^2) / O(N^3) bottleneck.
func assertLinearScaling(t *testing.T, label, query string) {
	t.Helper()
	if testing.Short() {
		t.Skip("skipping join scaling stress test in -short mode")
	}

	sizes := []int{200, 400, 800, 1600}

	type sample struct {
		n         int
		nsPerItem float64
	}
	var samples []sample

	t.Logf("%s", label)
	t.Logf("%-8s %-14s %-16s %-12s", "N", "count_time", "ns/item", "vs prev")
	var prevNsPerItem float64
	for _, n := range sizes {
		db := buildJoinChainDB(t, n)

		// Warm up once (parse plan, allocate) so we time steady-state cost.
		if rows, err := db.Query(query); err != nil {
			t.Fatalf("warmup query (N=%d): %v", n, err)
		} else if got := rows[0]["n"]; got != intVal(int64(n)) {
			t.Fatalf("N=%d: expected count %d, got %v", n, n, got)
		}

		runtime.GC()
		start := time.Now()
		rows, err := db.Query(query)
		elapsed := time.Since(start)
		if err != nil {
			t.Fatalf("timed query (N=%d): %v", n, err)
		}
		if got := rows[0]["n"]; got != intVal(int64(n)) {
			t.Fatalf("N=%d: expected count %d, got %v", n, n, got)
		}

		nsPerItem := float64(elapsed.Nanoseconds()) / float64(n)
		ratio := ""
		if prevNsPerItem > 0 {
			ratio = fmt.Sprintf("%.2fx", nsPerItem/prevNsPerItem)
		}
		t.Logf("%-8d %-14s %-16.1f %-12s", n, elapsed, nsPerItem, ratio)
		samples = append(samples, sample{n: n, nsPerItem: nsPerItem})
		prevNsPerItem = nsPerItem
	}

	// Assert near-linear scaling: from the smallest to the largest N (an 8x
	// increase here) a linear read path keeps ns/item roughly constant
	// (ratio ~1x), while an O(N^2) path makes ns/item scale with N (ratio ~8x).
	// We require < 3x growth so the test tolerates timing noise but still
	// catches a regression back to a nested-loop scan.
	first, last := samples[0], samples[len(samples)-1]
	growth := last.nsPerItem / first.nsPerItem
	nFactor := float64(last.n) / float64(first.n)
	t.Logf("per-item cost grew %.2fx while N grew %.0fx (linear ~1x, quadratic ~%.0fx)",
		growth, nFactor, nFactor)
	if growth >= 3.0 {
		t.Errorf("%s: join cost is scaling super-linearly (per-item growth %.2fx over %.0fx N) — "+
			"a join may have regressed to a nested-loop scan", label, growth, nFactor)
	}
}

// BenchmarkThreeTableJoinCount and BenchmarkTwoTableJoinCount give reproducible
// before/after numbers for the join optimisation. Run e.g.:
//
//	go test -run '^$' -bench 'Benchmark.*JoinCount' -benchmem
func BenchmarkThreeTableJoinCount(b *testing.B) { benchCount(b, countThreeTable) }
func BenchmarkTwoTableJoinCount(b *testing.B)    { benchCount(b, countTwoTable) }

func benchCount(b *testing.B, query string) {
	for _, n := range []int{200, 400, 800, 1600} {
		db := buildJoinChainDB(b, n)
		b.Run(fmt.Sprintf("N=%d", n), func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				rows, err := db.Query(query)
				if err != nil {
					b.Fatalf("query: %v", err)
				}
				if rows[0]["n"] != intVal(int64(n)) {
					b.Fatalf("expected %d, got %v", n, rows[0]["n"])
				}
			}
		})
	}
}
