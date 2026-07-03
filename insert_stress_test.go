package vapordb

// Stress test proving that batching commits removes the transaction-per-item
// bottleneck on the write path.
//
// db.Begin() takes a full deep-copy snapshot of the whole database as a
// rollback point (snapshotTables in tx.go). Committing once per inserted item
// therefore takes N snapshots, each O(rows-so-far) → O(N²) total. Wrapping a
// batch of items in a single transaction takes one snapshot per batch instead,
// collapsing the snapshot cost to ~O(N²/batchSize) — effectively linear for a
// reasonable batch size.
//
// This test measures both strategies at doubling N and asserts:
//   - tx-per-item scales super-linearly (the bug), and
//   - batched (1000/commit) scales near-linearly and is dramatically faster.

import (
	"fmt"
	"runtime"
	"testing"
	"time"
)

// insertTxPerItem inserts n rows, committing a fresh transaction per row.
func insertTxPerItem(tb testing.TB, db *DB, n int) {
	tb.Helper()
	for i := 0; i < n; i++ {
		tx, err := db.Begin()
		if err != nil {
			tb.Fatalf("begin: %v", err)
		}
		if err := tx.Exec(fmt.Sprintf(
			`INSERT INTO items (id, name) VALUES (%d, 'item-%d')`, i, i)); err != nil {
			tb.Fatalf("exec: %v", err)
		}
		if err := tx.Commit(); err != nil {
			tb.Fatalf("commit: %v", err)
		}
	}
}

// insertBatched inserts n rows, committing once every batchSize rows.
func insertBatched(tb testing.TB, db *DB, n, batchSize int) {
	tb.Helper()
	for start := 0; start < n; start += batchSize {
		tx, err := db.Begin()
		if err != nil {
			tb.Fatalf("begin: %v", err)
		}
		end := min(start+batchSize, n)
		for i := start; i < end; i++ {
			if err := tx.Exec(fmt.Sprintf(
				`INSERT INTO items (id, name) VALUES (%d, 'item-%d')`, i, i)); err != nil {
				tb.Fatalf("exec: %v", err)
			}
		}
		if err := tx.Commit(); err != nil {
			tb.Fatalf("commit: %v", err)
		}
	}
}

// upsertBatched upserts n rows (all new keys) via ON CONFLICT DO UPDATE,
// committing once every batchSize rows. Batching removes the per-commit
// snapshot cost, but every upsert still calls findConflict, which linearly
// scans the whole table to look for a matching key — so this stays O(N²).
func upsertBatched(tb testing.TB, db *DB, n, batchSize int) {
	tb.Helper()
	for start := 0; start < n; start += batchSize {
		tx, err := db.Begin()
		if err != nil {
			tb.Fatalf("begin: %v", err)
		}
		end := min(start+batchSize, n)
		for i := start; i < end; i++ {
			if err := tx.Exec(fmt.Sprintf(
				`INSERT INTO items (id, name) VALUES (%d, 'item-%d') `+
					`ON CONFLICT (id) DO UPDATE SET name = EXCLUDED.name`, i, i)); err != nil {
				tb.Fatalf("exec: %v", err)
			}
		}
		if err := tx.Commit(); err != nil {
			tb.Fatalf("commit: %v", err)
		}
	}
}

// perItemNs runs insert(fresh db of size n) and returns nanoseconds per row.
func perItemNs(tb testing.TB, n int, insert func(db *DB)) (time.Duration, float64) {
	tb.Helper()
	db := New()
	runtime.GC()
	start := time.Now()
	insert(db)
	elapsed := time.Since(start)
	// Sanity: all rows landed.
	if got := len(db.Tables["items"].Rows); got != n {
		tb.Fatalf("expected %d rows, got %d", n, got)
	}
	return elapsed, float64(elapsed.Nanoseconds()) / float64(n)
}

// bestPerItemNs runs the workload reps times (fresh DB each) and returns the
// fastest run. Taking the minimum denoises timing: it discards runs perturbed
// by GC pauses or scheduler jitter, leaving the closest estimate of the actual
// steady-state cost.
func bestPerItemNs(tb testing.TB, n, reps int, insert func(db *DB)) (time.Duration, float64) {
	tb.Helper()
	bestTotal := time.Duration(1<<63 - 1)
	var bestPer float64
	for r := 0; r < reps; r++ {
		total, per := perItemNs(tb, n, insert)
		if total < bestTotal {
			bestTotal, bestPer = total, per
		}
	}
	return bestTotal, bestPer
}

func TestBatchCommitRemovesTxBottleneck(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping write-path stress test in -short mode")
	}

	const batchSize = 1000
	sizes := []int{500, 1000, 2000, 4000}

	t.Logf("%-8s │ %-22s │ %-22s │ %-8s", "N", "tx-per-item", "batched(1000)", "speedup")
	var firstPerItem, lastPerItem float64
	var firstBatch, lastBatch float64
	var lastN int
	for i, n := range sizes {
		// tx-per-item is inherently slow and robustly super-linear, so a single
		// run suffices; the batched path is fast, so take the best of a few runs
		// to denoise the near-flat measurement.
		perTotal, perNs := perItemNs(t, n, func(db *DB) { insertTxPerItem(t, db, n) })
		batchTotal, batchNs := bestPerItemNs(t, n, 3, func(db *DB) { insertBatched(t, db, n, batchSize) })

		speedup := perNs / batchNs
		t.Logf("%-8d │ %10s %7.0f ns │ %10s %7.0f ns │ %6.1fx",
			n, perTotal.Round(time.Millisecond), perNs,
			batchTotal.Round(time.Millisecond), batchNs, speedup)

		if i == 0 {
			firstPerItem, firstBatch = perNs, batchNs
		}
		lastPerItem, lastBatch = perNs, batchNs
		lastN = n
	}

	perItemGrowth := lastPerItem / firstPerItem
	batchGrowth := lastBatch / firstBatch
	nFactor := float64(lastN) / float64(sizes[0])
	t.Logf("per-item cost growth over %.0fx N — tx-per-item: %.2fx (want super-linear), batched: %.2fx (want ~linear)",
		nFactor, perItemGrowth, batchGrowth)

	// 1. Prove the bottleneck exists: tx-per-item is clearly super-linear.
	if perItemGrowth < 3.0 {
		t.Errorf("expected tx-per-item to scale super-linearly (growth >= 3x over %.0fx N), got %.2fx",
			nFactor, perItemGrowth)
	}
	// 2. Prove batching fixes it: batched per-item cost stays near-flat.
	if batchGrowth >= 3.0 {
		t.Errorf("expected batched inserts to scale ~linearly (growth < 3x over %.0fx N), got %.2fx",
			nFactor, batchGrowth)
	}
	// 3. Prove it matters: batching is substantially faster at the largest N.
	if speedup := lastPerItem / lastBatch; speedup < 3.0 {
		t.Errorf("expected batching to be >= 3x faster at N=%d, got %.1fx", lastN, speedup)
	}
}

// TestUpsertScalesLinearlyWithIndex proves the conflict-key index fix: ON
// CONFLICT upsert detection now uses a hash index (findConflict → Table.
// lookupConflict) instead of a linear full-table scan, so a batched upsert
// import scales linearly like a plain insert, within a constant factor.
//
// Before the index, the upsert/insert per-item ratio grew with N (findConflict
// was O(N) per row → O(N²) total). After the index, both scale linearly and the
// ratio stays bounded. If the index regresses to a scan, upsertGrowth and the
// ratio climb again and this test fails.
func TestUpsertScalesLinearlyWithIndex(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping write-path stress test in -short mode")
	}

	const (
		batchSize = 1000
		reps      = 3
	)
	sizes := []int{1000, 2000, 4000, 8000}

	t.Logf("%-8s │ %-22s │ %-22s │ %-10s", "N", "batched INSERT", "batched UPSERT", "upsert/insert")
	var firstInsert, lastInsert float64
	var firstUpsert, lastUpsert float64
	var lastRatio float64
	var lastN int
	for i, n := range sizes {
		insTotal, insNs := bestPerItemNs(t, n, reps, func(db *DB) { insertBatched(t, db, n, batchSize) })
		upTotal, upNs := bestPerItemNs(t, n, reps, func(db *DB) { upsertBatched(t, db, n, batchSize) })

		ratio := upNs / insNs
		t.Logf("%-8d │ %10s %7.0f ns │ %10s %7.0f ns │ %8.1fx",
			n, insTotal.Round(time.Millisecond), insNs,
			upTotal.Round(time.Millisecond), upNs, ratio)

		if i == 0 {
			firstInsert, firstUpsert = insNs, upNs
		}
		lastInsert, lastUpsert, lastRatio = insNs, upNs, ratio
		lastN = n
	}

	insertGrowth := lastInsert / firstInsert
	upsertGrowth := lastUpsert / firstUpsert
	nFactor := float64(lastN) / float64(sizes[0])
	t.Logf("over %.0fx N — batched INSERT per-item: %.2fx, batched UPSERT per-item: %.2fx (both want ~linear, < 2x), upsert/insert at largest N: %.1fx",
		nFactor, insertGrowth, upsertGrowth, lastRatio)

	// Control: batched plain insert scales ~linearly (per-item stays bounded).
	if insertGrowth >= 2.0 {
		t.Errorf("expected batched INSERT per-item growth < 2x over %.0fx N, got %.2fx", nFactor, insertGrowth)
	}
	// The fix: batched upsert now also scales ~linearly. Before the conflict-key
	// index this was strongly super-linear (findConflict's O(N) scan per row).
	if upsertGrowth >= 2.0 {
		t.Errorf("expected batched UPSERT per-item growth < 2x over %.0fx N (conflict-key index should make it linear), got %.2fx — "+
			"the index may have regressed to a linear scan", nFactor, upsertGrowth)
	}
	// Upsert stays within a small constant factor of a plain insert (index
	// lookup + hash-key build + the longer ON CONFLICT statement), not
	// asymptotically worse.
	if lastRatio > 4.0 {
		t.Errorf("expected batched UPSERT within ~4x of batched INSERT at N=%d, got %.1fx", lastN, lastRatio)
	}
}
