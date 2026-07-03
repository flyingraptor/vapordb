package vapordb

// Correctness tests for the conflict-key index (index.go) that backs
// findConflict. These focus on the maintenance-sensitive paths: any mutation
// that is not a plain append must keep upsert conflict detection correct, and
// key types that the index cannot encode must fall back to the linear scan.

import (
	"fmt"
	"testing"
)

// rowsByInt indexes result rows by an int column for order-independent checks.
func rowsByInt(t *testing.T, rows []Row, col string) map[int64]Row {
	t.Helper()
	m := make(map[int64]Row, len(rows))
	for _, r := range rows {
		v, ok := r[col].V.(int64)
		if !ok {
			t.Fatalf("row %v: column %q is not an int (%v)", r, col, r[col])
		}
		m[v] = r
	}
	return m
}

// ── DELETE invalidation ───────────────────────────────────────────────────────

// After deleting a row, upserting its key must INSERT a fresh row (the deleted
// row is gone), while an untouched key still conflicts. This fails if DELETE
// does not invalidate the cached index.
func TestConflictIndexUpsertAfterDelete(t *testing.T) {
	db := New()
	mustExec(t, db, `INSERT INTO t (id, name) VALUES (1, 'a')`)
	mustExec(t, db, `INSERT INTO t (id, name) VALUES (2, 'b')`)
	mustExec(t, db, `INSERT INTO t (id, name) VALUES (3, 'c')`)

	// Prime the index with an upsert, then delete a row.
	mustExec(t, db, `INSERT INTO t (id, name) VALUES (3, 'c2') ON CONFLICT (id) DO UPDATE SET name = EXCLUDED.name`)
	mustExec(t, db, `DELETE FROM t WHERE id = 2`)

	// id=2 was deleted → this must insert, not update.
	mustExec(t, db, `INSERT INTO t (id, name) VALUES (2, 'B') ON CONFLICT (id) DO UPDATE SET name = EXCLUDED.name`)
	// id=1 still present → this must update.
	mustExec(t, db, `INSERT INTO t (id, name) VALUES (1, 'A') ON CONFLICT (id) DO UPDATE SET name = EXCLUDED.name`)

	rows := mustQuery(t, db, `SELECT id, name FROM t`)
	if len(rows) != 3 {
		t.Fatalf("want 3 rows, got %d: %v", len(rows), rows)
	}
	byID := rowsByInt(t, rows, "id")
	if byID[1]["name"] != strVal("A") {
		t.Errorf("id=1 name: want A, got %v", byID[1]["name"])
	}
	if byID[2]["name"] != strVal("B") {
		t.Errorf("id=2 name: want B (re-inserted), got %v", byID[2]["name"])
	}
	if byID[3]["name"] != strVal("c2") {
		t.Errorf("id=3 name: want c2, got %v", byID[3]["name"])
	}
}

// ── UPDATE invalidation ───────────────────────────────────────────────────────

// After an UPDATE changes a key column, upserting the old key must INSERT and
// upserting the new key must UPDATE. Fails if UPDATE does not invalidate.
func TestConflictIndexUpsertAfterKeyUpdate(t *testing.T) {
	db := New()
	mustExec(t, db, `INSERT INTO t (id, name) VALUES (1, 'a')`)
	// Prime index, then move the key with an UPDATE.
	mustExec(t, db, `INSERT INTO t (id, name) VALUES (1, 'a2') ON CONFLICT (id) DO UPDATE SET name = EXCLUDED.name`)
	mustExec(t, db, `UPDATE t SET id = 99 WHERE id = 1`)

	// Old key gone → insert; new key present → update.
	mustExec(t, db, `INSERT INTO t (id, name) VALUES (1, 'new') ON CONFLICT (id) DO UPDATE SET name = EXCLUDED.name`)
	mustExec(t, db, `INSERT INTO t (id, name) VALUES (99, 'moved') ON CONFLICT (id) DO UPDATE SET name = EXCLUDED.name`)

	rows := mustQuery(t, db, `SELECT id, name FROM t`)
	if len(rows) != 2 {
		t.Fatalf("want 2 rows, got %d: %v", len(rows), rows)
	}
	byID := rowsByInt(t, rows, "id")
	if byID[1]["name"] != strVal("new") {
		t.Errorf("id=1: want name=new (re-inserted), got %v", byID[1]["name"])
	}
	if byID[99]["name"] != strVal("moved") {
		t.Errorf("id=99: want name=moved (updated), got %v", byID[99]["name"])
	}
}

// An ON CONFLICT DO UPDATE that rewrites the key column itself must drop the
// index entry for the old key so the old key can be re-inserted afterwards.
func TestConflictIndexDoUpdateChangesKey(t *testing.T) {
	db := New()
	mustExec(t, db, `INSERT INTO t (id, name) VALUES (1, 'a')`)
	// Conflict on id=1, but the update moves the row to id=2.
	mustExec(t, db, `INSERT INTO t (id, name) VALUES (1, 'b') ON CONFLICT (id) DO UPDATE SET id = 2, name = EXCLUDED.name`)
	// id=1 is now free → must insert a new row.
	mustExec(t, db, `INSERT INTO t (id, name) VALUES (1, 'c') ON CONFLICT (id) DO UPDATE SET name = EXCLUDED.name`)

	rows := mustQuery(t, db, `SELECT id, name FROM t`)
	if len(rows) != 2 {
		t.Fatalf("want 2 rows, got %d: %v", len(rows), rows)
	}
	byID := rowsByInt(t, rows, "id")
	if byID[1]["name"] != strVal("c") {
		t.Errorf("id=1: want name=c, got %v", byID[1]["name"])
	}
	if byID[2]["name"] != strVal("b") {
		t.Errorf("id=2: want name=b, got %v", byID[2]["name"])
	}
}

// Updating a NON-key column via ON CONFLICT must keep the index valid so a
// subsequent conflict on the same key is still detected (no duplicate insert).
func TestConflictIndexNonKeyUpdateKeepsIndex(t *testing.T) {
	db := New()
	mustExec(t, db, `INSERT INTO t (id, name) VALUES (1, 'a')`)
	mustExec(t, db, `INSERT INTO t (id, name) VALUES (1, 'b') ON CONFLICT (id) DO UPDATE SET name = EXCLUDED.name`)
	mustExec(t, db, `INSERT INTO t (id, name) VALUES (1, 'c') ON CONFLICT (id) DO UPDATE SET name = EXCLUDED.name`)

	rows := mustQuery(t, db, `SELECT id, name FROM t`)
	if len(rows) != 1 {
		t.Fatalf("want 1 row (all conflicts on id=1), got %d: %v", len(rows), rows)
	}
	if rows[0]["name"] != strVal("c") {
		t.Errorf("want name=c, got %v", rows[0]["name"])
	}
}

// ── multiple conflict-column sets on one table ────────────────────────────────

// Two different ON CONFLICT targets on the same table maintain independent
// cached indexes. Updating a column through one target must invalidate the
// other target's index when they overlap, so a later conflict check sees the
// current data (not stale keys).
func TestConflictIndexMultipleColumnSets(t *testing.T) {
	db := New()
	mustExec(t, db, `INSERT INTO t (id, email) VALUES (1, 'a@x')`)
	// Build the (email) index and append a new row through it.
	mustExec(t, db, `INSERT INTO t (id, email) VALUES (5, 'b@x') ON CONFLICT (email) DO NOTHING`)
	// Conflict on id=1, update its email → must invalidate the (email) index.
	mustExec(t, db, `INSERT INTO t (id, email) VALUES (1, 'c@x') ON CONFLICT (id) DO UPDATE SET email = EXCLUDED.email`)
	// email 'a@x' no longer exists → this must INSERT, not be skipped by a stale
	// (email) index that still maps 'a@x'.
	mustExec(t, db, `INSERT INTO t (id, email) VALUES (9, 'a@x') ON CONFLICT (email) DO NOTHING`)

	rows := mustQuery(t, db, `SELECT id, email FROM t`)
	if len(rows) != 3 {
		t.Fatalf("want 3 rows, got %d: %v", len(rows), rows)
	}
	byID := rowsByInt(t, rows, "id")
	if byID[1]["email"] != strVal("c@x") {
		t.Errorf("id=1 email: want c@x, got %v", byID[1]["email"])
	}
	if byID[9]["email"] != strVal("a@x") {
		t.Errorf("id=9 email: want a@x (re-inserted), got %v", byID[9]["email"])
	}
}

// ── NULL conflict keys ────────────────────────────────────────────────────────

// NULL key values compare equal under the engine's value equality, so two rows
// with a NULL conflict column conflict. The index must reproduce this.
func TestConflictIndexNullKeyMatches(t *testing.T) {
	db := New()
	mustExec(t, db, `INSERT INTO t (id, code) VALUES (1, 'A')`)
	mustExec(t, db, `INSERT INTO t (id, code) VALUES (2, NULL)`)
	// Conflict on code IS NULL → updates row id=2's id to 3.
	mustExec(t, db, `INSERT INTO t (id, code) VALUES (3, NULL) ON CONFLICT (code) DO UPDATE SET id = EXCLUDED.id`)

	rows := mustQuery(t, db, `SELECT id, code FROM t`)
	if len(rows) != 2 {
		t.Fatalf("want 2 rows, got %d: %v", len(rows), rows)
	}
	byID := rowsByInt(t, rows, "id")
	if _, ok := byID[2]; ok {
		t.Errorf("id=2 should have been updated to id=3, still present: %v", rows)
	}
	if r, ok := byID[3]; !ok || r["code"].Kind != KindNull {
		t.Errorf("want a row id=3 with NULL code, got %v", rows)
	}
}

// ── first-match among duplicate keys ──────────────────────────────────────────

// Plain inserts can create duplicate keys (no conflict clause). A later upsert
// must update the FIRST matching row, matching the original linear scan.
func TestConflictIndexFirstMatchAmongDuplicates(t *testing.T) {
	db := New()
	mustExec(t, db, `INSERT INTO t (id, name) VALUES (1, 'first')`)
	mustExec(t, db, `INSERT INTO t (id, name) VALUES (1, 'second')`)

	mustExec(t, db, `INSERT INTO t (id, name) VALUES (1, 'updated') ON CONFLICT (id) DO UPDATE SET name = EXCLUDED.name`)

	rows := mustQuery(t, db, `SELECT id, name FROM t`)
	if len(rows) != 2 {
		t.Fatalf("want 2 rows, got %d: %v", len(rows), rows)
	}
	// The first row (name=first) should have become 'updated'; the duplicate
	// (name=second) must be untouched.
	var updated, second int
	for _, r := range rows {
		switch r["name"] {
		case strVal("updated"):
			updated++
		case strVal("second"):
			second++
		default:
			t.Errorf("unexpected name %v", r["name"])
		}
	}
	if updated != 1 || second != 1 {
		t.Errorf("want exactly one 'updated' and one 'second', got updated=%d second=%d: %v", updated, second, rows)
	}
}

// ── conflict within a single multi-row statement ──────────────────────────────

// Rows inside one INSERT are processed in order; a later value that conflicts
// with an earlier value from the SAME statement must be detected (the earlier
// row was just appended and registered in the index).
func TestConflictIndexWithinSingleStatement(t *testing.T) {
	db := New()
	mustExec(t, db, `INSERT INTO t (id, name) VALUES (1, 'a'), (1, 'b')
		ON CONFLICT (id) DO UPDATE SET name = EXCLUDED.name`)

	rows := mustQuery(t, db, `SELECT id, name FROM t`)
	if len(rows) != 1 {
		t.Fatalf("want 1 row (second value conflicts with first), got %d: %v", len(rows), rows)
	}
	if rows[0]["name"] != strVal("b") {
		t.Errorf("want name=b, got %v", rows[0]["name"])
	}
}

// ── transaction rollback ──────────────────────────────────────────────────────

// After a rollback, the index must reflect the restored rows: a key inserted
// only inside the rolled-back transaction is gone, so upserting it inserts.
func TestConflictIndexAfterRollback(t *testing.T) {
	db := New()
	mustExec(t, db, `INSERT INTO t (id, name) VALUES (1, 'a')`)
	// Prime the index outside the transaction.
	mustExec(t, db, `INSERT INTO t (id, name) VALUES (1, 'a2') ON CONFLICT (id) DO UPDATE SET name = EXCLUDED.name`)

	tx, err := db.Begin()
	if err != nil {
		t.Fatal(err)
	}
	if err := tx.Exec(`INSERT INTO t (id, name) VALUES (2, 'b') ON CONFLICT (id) DO UPDATE SET name = EXCLUDED.name`); err != nil {
		t.Fatal(err)
	}
	if err := tx.Exec(`INSERT INTO t (id, name) VALUES (1, 'changed') ON CONFLICT (id) DO UPDATE SET name = EXCLUDED.name`); err != nil {
		t.Fatal(err)
	}
	if err := tx.Rollback(); err != nil {
		t.Fatal(err)
	}

	// After rollback only id=1 (name=a2) should remain.
	mustExec(t, db, `INSERT INTO t (id, name) VALUES (2, 'B') ON CONFLICT (id) DO UPDATE SET name = EXCLUDED.name`)
	mustExec(t, db, `INSERT INTO t (id, name) VALUES (1, 'A') ON CONFLICT (id) DO UPDATE SET name = EXCLUDED.name`)

	rows := mustQuery(t, db, `SELECT id, name FROM t`)
	if len(rows) != 2 {
		t.Fatalf("want 2 rows, got %d: %v", len(rows), rows)
	}
	byID := rowsByInt(t, rows, "id")
	if byID[1]["name"] != strVal("A") {
		t.Errorf("id=1: want name=A, got %v", byID[1]["name"])
	}
	if byID[2]["name"] != strVal("B") {
		t.Errorf("id=2: want name=B (rolled back then re-inserted), got %v", byID[2]["name"])
	}
}

// ── non-encodable key types fall back to the linear scan ──────────────────────

// DATE keys are not index-encodable; conflict detection must still work via the
// linear-scan fallback.
func TestConflictIndexDateKeyFallback(t *testing.T) {
	db := New()
	mustExec(t, db, `INSERT INTO t (id, d) VALUES (1, DATE('2024-01-01'))`)
	// Same date → conflict → update id to 2.
	mustExec(t, db, `INSERT INTO t (id, d) VALUES (2, DATE('2024-01-01')) ON CONFLICT (d) DO UPDATE SET id = EXCLUDED.id`)
	// Different date → no conflict → insert.
	mustExec(t, db, `INSERT INTO t (id, d) VALUES (3, DATE('2025-05-05')) ON CONFLICT (d) DO NOTHING`)

	rows := mustQuery(t, db, `SELECT id, d FROM t`)
	if len(rows) != 2 {
		t.Fatalf("want 2 rows, got %d: %v", len(rows), rows)
	}
	byID := rowsByInt(t, rows, "id")
	if _, ok := byID[1]; ok {
		t.Errorf("id=1 should have been updated to id=2: %v", rows)
	}
	if _, ok := byID[2]; !ok {
		t.Errorf("want a row id=2 (date conflict update): %v", rows)
	}
	if _, ok := byID[3]; !ok {
		t.Errorf("want a row id=3 (distinct date insert): %v", rows)
	}
}

// Float and bool conflict keys are index-encodable; verify both match.
func TestConflictIndexFloatAndBoolKeys(t *testing.T) {
	t.Run("float", func(t *testing.T) {
		db := New()
		mustExec(t, db, `INSERT INTO p (id, price) VALUES (1, 9.99)`)
		mustExec(t, db, `INSERT INTO p (id, price) VALUES (2, 9.99) ON CONFLICT (price) DO UPDATE SET id = EXCLUDED.id`)
		mustExec(t, db, `INSERT INTO p (id, price) VALUES (3, 1.50) ON CONFLICT (price) DO NOTHING`)

		rows := mustQuery(t, db, `SELECT id, price FROM p`)
		if len(rows) != 2 {
			t.Fatalf("want 2 rows, got %d: %v", len(rows), rows)
		}
		byID := rowsByInt(t, rows, "id")
		if _, ok := byID[1]; ok {
			t.Errorf("id=1 should have been updated to id=2 (price 9.99 conflict): %v", rows)
		}
		if r, ok := byID[3]; !ok || r["price"] != floatVal(1.50) {
			t.Errorf("want a row id=3 price=1.50: %v", rows)
		}
	})

	t.Run("bool", func(t *testing.T) {
		db := New()
		mustExec(t, db, `INSERT INTO f (id, active) VALUES (1, true)`)
		mustExec(t, db, `INSERT INTO f (id, active) VALUES (2, true) ON CONFLICT (active) DO UPDATE SET id = EXCLUDED.id`)
		mustExec(t, db, `INSERT INTO f (id, active) VALUES (3, false) ON CONFLICT (active) DO NOTHING`)

		rows := mustQuery(t, db, `SELECT id, active FROM f`)
		if len(rows) != 2 {
			t.Fatalf("want 2 rows, got %d: %v", len(rows), rows)
		}
		byID := rowsByInt(t, rows, "id")
		if _, ok := byID[1]; ok {
			t.Errorf("id=1 should have been updated to id=2 (active=true conflict): %v", rows)
		}
		if r, ok := byID[3]; !ok || r["active"] != boolVal(false) {
			t.Errorf("want a row id=3 active=false: %v", rows)
		}
	})
}

// ── composite key partial match ───────────────────────────────────────────────

// A composite conflict key must only match when ALL columns match; a partial
// match (one column differs) is a new row.
func TestConflictIndexCompositePartialMatch(t *testing.T) {
	db := New()
	mustExec(t, db, `INSERT INTO s (user_id, game, score) VALUES (1, 'chess', 100)`)
	// Same user, different game → no conflict → insert.
	mustExec(t, db, `INSERT INTO s (user_id, game, score) VALUES (1, 'go', 50)
		ON CONFLICT (user_id, game) DO UPDATE SET score = EXCLUDED.score`)
	// Full match → conflict → update.
	mustExec(t, db, `INSERT INTO s (user_id, game, score) VALUES (1, 'chess', 200)
		ON CONFLICT (user_id, game) DO UPDATE SET score = EXCLUDED.score`)

	rows := mustQuery(t, db, `SELECT user_id, game, score FROM s ORDER BY game`)
	if len(rows) != 2 {
		t.Fatalf("want 2 rows, got %d: %v", len(rows), rows)
	}
	// chess → 200 (updated), go → 50 (inserted).
	byGame := map[string]Row{}
	for _, r := range rows {
		byGame[r["game"].V.(string)] = r
	}
	if byGame["chess"]["score"] != intVal(200) {
		t.Errorf("chess score: want 200, got %v", byGame["chess"]["score"])
	}
	if byGame["go"]["score"] != intVal(50) {
		t.Errorf("go score: want 50, got %v", byGame["go"]["score"])
	}
}

// ── moderate-scale correctness ────────────────────────────────────────────────

// Insert many distinct keys via upsert, then upsert every key again with new
// values, then add more new keys. Exercises index build + incremental append +
// repeated conflict hits together, checking counts and values throughout.
func TestConflictIndexManyKeysRoundTrip(t *testing.T) {
	db := New()
	const n = 300

	// First pass: all new keys → all inserts.
	for i := 0; i < n; i++ {
		mustExec(t, db, sqlUpsert(i, i))
	}
	if got := len(mustQuery(t, db, `SELECT id FROM t`)); got != n {
		t.Fatalf("after first pass want %d rows, got %d", n, got)
	}

	// Second pass: same keys, new values → all updates, count unchanged.
	for i := 0; i < n; i++ {
		mustExec(t, db, sqlUpsert(i, i*10))
	}
	rows := mustQuery(t, db, `SELECT id, val FROM t`)
	if len(rows) != n {
		t.Fatalf("after second pass want %d rows, got %d", n, len(rows))
	}
	byID := rowsByInt(t, rows, "id")
	for i := 0; i < n; i++ {
		if byID[int64(i)]["val"] != intVal(int64(i*10)) {
			t.Fatalf("id=%d: want val=%d, got %v", i, i*10, byID[int64(i)]["val"])
		}
	}

	// Third pass: add n more new keys.
	for i := n; i < 2*n; i++ {
		mustExec(t, db, sqlUpsert(i, i))
	}
	if got := len(mustQuery(t, db, `SELECT id FROM t`)); got != 2*n {
		t.Fatalf("after third pass want %d rows, got %d", 2*n, got)
	}
}

func sqlUpsert(id, val int) string {
	return fmt.Sprintf(
		"INSERT INTO t (id, val) VALUES (%d, %d) ON CONFLICT (id) DO UPDATE SET val = EXCLUDED.val",
		id, val)
}
