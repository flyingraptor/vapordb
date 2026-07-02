package vapordb

// Stress tests that reproduce the field bug report and guard against its
// regression. The original bottleneck: joins ran as nested loops over
// full-table scans (see applyJoin in executor.go), so a COUNT over joined
// N-sized tables cost ~O(N^k) for a k-table join and per-row time grew with N.
//
// These tests mirror the two production queries from the report, using the
// same table topology:
//
//	CountSelectedInScope — 3-table join:
//	    items ⋈ catalogue_items ⋈ catalogue_items_general_store
//	CountMatching — 2-table join (sibling, same browse request):
//	    catalogue_items ⋈ catalogue_items_general_store
//
// The 3-table chain is the important one: each extra JOIN multiplied the
// nested-loop cross-product (the report measured it ~2.4x worse than the
// 2-table sibling). With the hash join, every link in the chain is O(N), so the
// whole pipeline stays linear — if any single join silently fell back to a
// nested-loop scan, the 3-table test below would go quadratic and fail.

import (
	"fmt"
	"runtime"
	"testing"
	"time"
)

// buildCatalogueDB reproduces the report's schema with a 1:1:1 mapping so the
// counts are exactly n and the join fan-out is fixed (isolating the join
// algorithm's cost from result-size growth):
//
//	items(id, scope_id)                            — n rows, all in scope 1
//	catalogue_items(id, item_id, selected)         — n rows, item_id → items.id
//	catalogue_items_general_store(id, catalogue_item_id)
//	                                               — n rows → catalogue_items.id
func buildCatalogueDB(tb testing.TB, n int) *DB {
	tb.Helper()
	db := New()
	for i := 1; i <= n; i++ {
		mustSeed(tb, db, fmt.Sprintf(
			`INSERT INTO items (id, scope_id) VALUES (%d, 1)`, i))
		mustSeed(tb, db, fmt.Sprintf(
			`INSERT INTO catalogue_items (id, item_id, selected) VALUES (%d, %d, true)`, i, i))
		mustSeed(tb, db, fmt.Sprintf(
			`INSERT INTO catalogue_items_general_store (id, catalogue_item_id) VALUES (%d, %d)`, i, i))
	}
	return db
}

func mustSeed(tb testing.TB, db *DB, sql string) {
	tb.Helper()
	if err := db.Exec(sql); err != nil {
		tb.Fatalf("seed %q: %v", sql, err)
	}
}

// countSelectedInScope is the 3-table production query that blew the deadline.
const countSelectedInScope = `
	SELECT COUNT(*) AS n
	FROM items i
	JOIN catalogue_items ci ON i.id = ci.item_id
	JOIN catalogue_items_general_store cigs ON ci.id = cigs.catalogue_item_id
	WHERE i.scope_id = 1 AND ci.selected = true`

// countMatching is the 2-table sibling that runs in the same browse request.
const countMatching = `
	SELECT COUNT(*) AS n
	FROM catalogue_items ci
	JOIN catalogue_items_general_store cigs ON ci.id = cigs.catalogue_item_id`

func TestCountSelectedInScopeScaling(t *testing.T) {
	assertLinearScaling(t, "CountSelectedInScope (3-table)", countSelectedInScope)
}

func TestCountMatchingScaling(t *testing.T) {
	assertLinearScaling(t, "CountMatching (2-table)", countMatching)
}

// assertLinearScaling runs a COUNT query at doubling catalogue sizes and
// asserts the read path is ~O(N): with the hash join, per-item time
// (total / N) stays roughly flat as N grows. A regression to the nested-loop
// join makes per-item cost scale with N (roughly doubling each time N doubles)
// and fails the test. See git history for the pre-fix numbers that proved the
// original O(N^2)/O(N^3) bottleneck.
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
		db := buildCatalogueDB(t, n)

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

// BenchmarkCountSelectedInScope and BenchmarkCountMatching give reproducible
// before/after numbers for the join optimisation. Run e.g.:
//
//	go test -run '^$' -bench 'BenchmarkCount' -benchmem
func BenchmarkCountSelectedInScope(b *testing.B) { benchCount(b, countSelectedInScope) }
func BenchmarkCountMatching(b *testing.B)        { benchCount(b, countMatching) }

func benchCount(b *testing.B, query string) {
	for _, n := range []int{200, 400, 800, 1600} {
		db := buildCatalogueDB(b, n)
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
