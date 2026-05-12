package vapordb

// Tests for PostgreSQL-compatibility gaps identified from real-world service analysis.
// Covers:
//   - Gap 1: FILTER (WHERE …) on aggregate functions
//   - Gap 2: array_agg aggregate function
//   - Gap 3: ON CONFLICT (cols) WHERE partial_predicate DO UPDATE (partial-index conflict)
//   - Gap 4: ON CONFLICT DO UPDATE SET … WHERE predicate (optimistic-lock update)

import (
	"encoding/json"
	"reflect"
	"testing"
)

// ═══════════════════════════════════════════════════════════════════════════════
// Gap 1 — FILTER (WHERE …) on aggregates
// ═══════════════════════════════════════════════════════════════════════════════

// TestFilterAgg_SUM verifies SUM(col) FILTER (WHERE cond) is correctly rewritten
// and produces the same result as the equivalent CASE WHEN expression.
func TestFilterAgg_SUM(t *testing.T) {
	db := New()
	mustExec(t, db, `INSERT INTO txn (type, amount) VALUES ('RESERVED', 100)`)
	mustExec(t, db, `INSERT INTO txn (type, amount) VALUES ('COMMITTED', 200)`)
	mustExec(t, db, `INSERT INTO txn (type, amount) VALUES ('RESERVED', 50)`)
	mustExec(t, db, `INSERT INTO txn (type, amount) VALUES ('ACTUAL', 300)`)

	rows := mustQuery(t, db, `
		SELECT
			SUM(amount) FILTER (WHERE type = 'RESERVED')  AS reserved,
			SUM(amount) FILTER (WHERE type = 'COMMITTED') AS committed,
			SUM(amount) FILTER (WHERE type = 'ACTUAL')    AS actual,
			SUM(amount)                                    AS total
		FROM txn`)

	if len(rows) != 1 {
		t.Fatalf("expected 1 row, got %d", len(rows))
	}
	r := rows[0]
	if got := r["reserved"]; got.V != int64(150) {
		t.Errorf("reserved: got %v, want 150", got.V)
	}
	if got := r["committed"]; got.V != int64(200) {
		t.Errorf("committed: got %v, want 200", got.V)
	}
	if got := r["actual"]; got.V != int64(300) {
		t.Errorf("actual: got %v, want 300", got.V)
	}
	if got := r["total"]; got.V != int64(650) {
		t.Errorf("total: got %v, want 650", got.V)
	}
}

// TestFilterAgg_COUNT verifies COUNT(*) FILTER (WHERE cond) and COUNT(col) FILTER.
func TestFilterAgg_COUNT(t *testing.T) {
	db := New()
	mustExec(t, db, `INSERT INTO items (active, score) VALUES (TRUE,  10)`)
	mustExec(t, db, `INSERT INTO items (active, score) VALUES (FALSE, 20)`)
	mustExec(t, db, `INSERT INTO items (active, score) VALUES (TRUE,  30)`)
	mustExec(t, db, `INSERT INTO items (active, score) VALUES (TRUE,  NULL)`) // NULL score

	rows := mustQuery(t, db, `
		SELECT
			COUNT(*) FILTER (WHERE active = TRUE)  AS active_count,
			COUNT(*) FILTER (WHERE active = FALSE) AS inactive_count,
			COUNT(score) FILTER (WHERE active = TRUE) AS active_score_count
		FROM items`)

	if len(rows) != 1 {
		t.Fatalf("expected 1 row, got %d", len(rows))
	}
	r := rows[0]
	if got := r["active_count"]; got.V != int64(3) {
		t.Errorf("active_count: got %v, want 3", got.V)
	}
	if got := r["inactive_count"]; got.V != int64(1) {
		t.Errorf("inactive_count: got %v, want 1", got.V)
	}
	if got := r["active_score_count"]; got.V != int64(2) {
		t.Errorf("active_score_count: got %v, want 2 (NULLs not counted)", got.V)
	}
}

// TestFilterAgg_AVG_MIN_MAX exercises the remaining aggregate variants.
func TestFilterAgg_AVG_MIN_MAX(t *testing.T) {
	db := New()
	mustExec(t, db, `INSERT INTO vals (cat, v) VALUES ('A', 10)`)
	mustExec(t, db, `INSERT INTO vals (cat, v) VALUES ('A', 20)`)
	mustExec(t, db, `INSERT INTO vals (cat, v) VALUES ('B', 100)`)
	mustExec(t, db, `INSERT INTO vals (cat, v) VALUES ('B', 200)`)

	rows := mustQuery(t, db, `
		SELECT
			AVG(v) FILTER (WHERE cat = 'A') AS avg_a,
			MIN(v) FILTER (WHERE cat = 'B') AS min_b,
			MAX(v) FILTER (WHERE cat = 'A') AS max_a
		FROM vals`)

	if len(rows) != 1 {
		t.Fatalf("expected 1 row, got %d", len(rows))
	}
	r := rows[0]
	if got := r["avg_a"]; got.V != float64(15) {
		t.Errorf("avg_a: got %v, want 15", got.V)
	}
	if got := r["min_b"]; got.V != int64(100) {
		t.Errorf("min_b: got %v, want 100", got.V)
	}
	if got := r["max_a"]; got.V != int64(20) {
		t.Errorf("max_a: got %v, want 20", got.V)
	}
}

// TestFilterAgg_WithGroupBy tests FILTER inside a GROUP BY query — the real-world
// pattern from budget-management-service.
func TestFilterAgg_WithGroupBy(t *testing.T) {
	db := New()
	mustExec(t, db, `INSERT INTO transactions (account_code, type, amount) VALUES ('A001', 'RESERVED', 100)`)
	mustExec(t, db, `INSERT INTO transactions (account_code, type, amount) VALUES ('A001', 'COMMITTED', 200)`)
	mustExec(t, db, `INSERT INTO transactions (account_code, type, amount) VALUES ('A001', 'ACTUAL', 300)`)
	mustExec(t, db, `INSERT INTO transactions (account_code, type, amount) VALUES ('A002', 'RESERVED', 50)`)
	mustExec(t, db, `INSERT INTO transactions (account_code, type, amount) VALUES ('A002', 'ACTUAL', 150)`)

	rows := mustQuery(t, db, `
		SELECT
			account_code,
			SUM(amount) FILTER (WHERE type = 'RESERVED')  AS reserved_amount,
			SUM(amount) FILTER (WHERE type = 'COMMITTED') AS committed_amount,
			SUM(amount) FILTER (WHERE type = 'ACTUAL')    AS actual_amount,
			SUM(amount)                                    AS total_amount
		FROM transactions
		GROUP BY account_code
		ORDER BY account_code`)

	if len(rows) != 2 {
		t.Fatalf("expected 2 rows, got %d", len(rows))
	}

	// A001
	if got := rows[0]["reserved_amount"]; got.V != int64(100) {
		t.Errorf("A001 reserved: got %v, want 100", got.V)
	}
	if got := rows[0]["committed_amount"]; got.V != int64(200) {
		t.Errorf("A001 committed: got %v, want 200", got.V)
	}
	if got := rows[0]["actual_amount"]; got.V != int64(300) {
		t.Errorf("A001 actual: got %v, want 300", got.V)
	}
	if got := rows[0]["total_amount"]; got.V != int64(600) {
		t.Errorf("A001 total: got %v, want 600", got.V)
	}

	// A002
	if got := rows[1]["reserved_amount"]; got.V != int64(50) {
		t.Errorf("A002 reserved: got %v, want 50", got.V)
	}
	if got := rows[1]["committed_amount"]; got.Kind != KindNull {
		t.Errorf("A002 committed: expected NULL, got %v", got.V)
	}
}

// TestFilterAgg_WithCOALESCE tests NULL-safe wrapping via COALESCE — common pattern.
func TestFilterAgg_WithCOALESCE(t *testing.T) {
	db := New()
	mustExec(t, db, `INSERT INTO t (cat, v) VALUES ('A', 10)`)

	rows := mustQuery(t, db, `
		SELECT
			COALESCE(SUM(v) FILTER (WHERE cat = 'A'), 0) AS sum_a,
			COALESCE(SUM(v) FILTER (WHERE cat = 'Z'), 0) AS sum_z
		FROM t`)

	if len(rows) != 1 {
		t.Fatalf("expected 1 row, got %d", len(rows))
	}
	if got := rows[0]["sum_a"]; got.V != int64(10) {
		t.Errorf("sum_a: got %v, want 10", got.V)
	}
	if got := rows[0]["sum_z"]; got.V != int64(0) {
		t.Errorf("sum_z: got %v, want 0 (coalesced from NULL)", got.V)
	}
}

// TestFilterAgg_MultipleFiltersOnSameCol tests several FILTER clauses on the
// same column in one SELECT — exactly like the budget service query.
func TestFilterAgg_MultipleFiltersOnSameCol(t *testing.T) {
	db := New()
	types := []string{"RESERVED", "COMMITTED", "ACTUAL", "RESERVED", "ACTUAL"}
	amounts := []int{100, 200, 300, 400, 500}
	for i, tp := range types {
		mustExec(t, db, `INSERT INTO tx (type, amount) VALUES ('`+tp+`', `+itoa(amounts[i])+`)`)
	}

	rows := mustQuery(t, db, `
		SELECT
			SUM(amount) FILTER (WHERE type = 'RESERVED')  AS r,
			SUM(amount) FILTER (WHERE type = 'COMMITTED') AS c,
			SUM(amount) FILTER (WHERE type = 'ACTUAL')    AS a
		FROM tx`)

	r := rows[0]
	if r["r"].V != int64(500) { // 100+400
		t.Errorf("RESERVED sum: got %v, want 500", r["r"].V)
	}
	if r["c"].V != int64(200) {
		t.Errorf("COMMITTED sum: got %v, want 200", r["c"].V)
	}
	if r["a"].V != int64(800) { // 300+500
		t.Errorf("ACTUAL sum: got %v, want 800", r["a"].V)
	}
}

// TestFilterAgg_StringCondition verifies conditions using string literals.
func TestFilterAgg_StringCondition(t *testing.T) {
	db := New()
	mustExec(t, db, `INSERT INTO ev (kind, val) VALUES ('click', 1)`)
	mustExec(t, db, `INSERT INTO ev (kind, val) VALUES ('view',  5)`)
	mustExec(t, db, `INSERT INTO ev (kind, val) VALUES ('click', 3)`)

	rows := mustQuery(t, db, `
		SELECT SUM(val) FILTER (WHERE kind = 'click') AS clicks FROM ev`)

	if rows[0]["clicks"].V != int64(4) {
		t.Errorf("clicks: got %v, want 4", rows[0]["clicks"].V)
	}
}

// TestFilterAgg_AllRowsFiltered verifies that when no row passes the filter,
// the aggregate returns NULL (standard SQL behaviour).
func TestFilterAgg_AllRowsFiltered(t *testing.T) {
	db := New()
	mustExec(t, db, `INSERT INTO nums (n) VALUES (1)`)
	mustExec(t, db, `INSERT INTO nums (n) VALUES (2)`)

	rows := mustQuery(t, db, `SELECT SUM(n) FILTER (WHERE n > 100) AS big FROM nums`)
	if rows[0]["big"].Kind != KindNull {
		t.Errorf("expected NULL when filter matches nothing, got %v", rows[0]["big"])
	}
}

// ═══════════════════════════════════════════════════════════════════════════════
// Gap 2 — array_agg
// ═══════════════════════════════════════════════════════════════════════════════

// TestArrayAgg_Basic collects values into a JSON array.
func TestArrayAgg_Basic(t *testing.T) {
	db := New()
	mustExec(t, db, `INSERT INTO names (name) VALUES ('alice')`)
	mustExec(t, db, `INSERT INTO names (name) VALUES ('bob')`)
	mustExec(t, db, `INSERT INTO names (name) VALUES ('carol')`)

	rows := mustQuery(t, db, `SELECT array_agg(name) AS names FROM names ORDER BY name`)
	if len(rows) != 1 {
		t.Fatalf("expected 1 row, got %d", len(rows))
	}
	v := rows[0]["names"]
	if v.Kind != KindJSON {
		t.Fatalf("expected KindJSON, got %v", v.Kind)
	}
	arr, ok := v.V.([]any)
	if !ok {
		t.Fatalf("expected []any, got %T", v.V)
	}
	if len(arr) != 3 {
		t.Errorf("expected 3 elements, got %d: %v", len(arr), arr)
	}
}

// TestArrayAgg_NullsSkipped verifies NULL values are excluded from the result.
func TestArrayAgg_NullsSkipped(t *testing.T) {
	db := New()
	mustExec(t, db, `INSERT INTO ids (id) VALUES (1)`)
	mustExec(t, db, `INSERT INTO ids (id) VALUES (NULL)`)
	mustExec(t, db, `INSERT INTO ids (id) VALUES (3)`)

	rows := mustQuery(t, db, `SELECT array_agg(id) AS ids FROM ids`)
	arr, ok := rows[0]["ids"].V.([]any)
	if !ok {
		t.Fatalf("expected []any, got %T", rows[0]["ids"].V)
	}
	if len(arr) != 2 {
		t.Errorf("expected 2 elements (NULLs excluded), got %d: %v", len(arr), arr)
	}
}

// TestArrayAgg_AllNull verifies that when all rows have NULL values, the result is NULL.
func TestArrayAgg_AllNull(t *testing.T) {
	db := New()
	mustExec(t, db, `INSERT INTO nils (n) VALUES (NULL)`)
	mustExec(t, db, `INSERT INTO nils (n) VALUES (NULL)`)

	rows := mustQuery(t, db, `SELECT array_agg(n) AS agg FROM nils`)
	if rows[0]["agg"].Kind != KindNull {
		t.Errorf("expected NULL when all input rows are NULL, got %v", rows[0]["agg"])
	}
}

// TestArrayAgg_WithGroupBy tests array_agg inside a GROUP BY query.
func TestArrayAgg_WithGroupBy(t *testing.T) {
	db := New()
	mustExec(t, db, `INSERT INTO orders (dept, emp) VALUES ('eng', 'alice')`)
	mustExec(t, db, `INSERT INTO orders (dept, emp) VALUES ('eng', 'bob')`)
	mustExec(t, db, `INSERT INTO orders (dept, emp) VALUES ('mkt', 'carol')`)

	rows := mustQuery(t, db, `
		SELECT dept, array_agg(emp) AS employees
		FROM orders
		GROUP BY dept
		ORDER BY dept`)

	if len(rows) != 2 {
		t.Fatalf("expected 2 rows, got %d", len(rows))
	}
	engArr, _ := rows[0]["employees"].V.([]any)
	if len(engArr) != 2 {
		t.Errorf("eng: expected 2 employees, got %d", len(engArr))
	}
	mktArr, _ := rows[1]["employees"].V.([]any)
	if len(mktArr) != 1 {
		t.Errorf("mkt: expected 1 employee, got %d", len(mktArr))
	}
}

// TestArrayAgg_WithFilter combines array_agg with FILTER (WHERE …).
// This is the exact pattern from the fiscal-year query in budget-management-service:
//
//	array_agg(fyc.client_id) FILTER (WHERE fyc.client_id IS NOT NULL)
func TestArrayAgg_WithFilter(t *testing.T) {
	db := New()
	mustExec(t, db, `INSERT INTO fyc (client_id) VALUES (1)`)
	mustExec(t, db, `INSERT INTO fyc (client_id) VALUES (NULL)`)
	mustExec(t, db, `INSERT INTO fyc (client_id) VALUES (2)`)
	mustExec(t, db, `INSERT INTO fyc (client_id) VALUES (NULL)`)
	mustExec(t, db, `INSERT INTO fyc (client_id) VALUES (3)`)

	rows := mustQuery(t, db, `
		SELECT array_agg(client_id) FILTER (WHERE client_id IS NOT NULL) AS client_ids
		FROM fyc`)

	v := rows[0]["client_ids"]
	if v.Kind != KindJSON {
		t.Fatalf("expected KindJSON, got %v (kind=%v)", v.V, v.Kind)
	}
	arr := v.V.([]any)
	if len(arr) != 3 {
		t.Errorf("expected 3 client_ids (NULLs filtered), got %d: %v", len(arr), arr)
	}
}

// TestArrayAgg_COALESCEDefault mirrors the PostgreSQL pattern
//
//	COALESCE(array_agg(…) FILTER (WHERE …), '{}')
//
// In vapordb, '{}' coalesces to the string "{}" when array_agg returns NULL.
// This test confirms the NULL path works with COALESCE.
func TestArrayAgg_COALESCEDefault(t *testing.T) {
	db := New()
	mustExec(t, db, `INSERT INTO cids (id) VALUES (NULL)`)

	rows := mustQuery(t, db, `
		SELECT COALESCE(array_agg(id) FILTER (WHERE id IS NOT NULL), 'none') AS result
		FROM cids`)

	if rows[0]["result"].V != "none" {
		t.Errorf("expected 'none' from COALESCE fallback, got %v", rows[0]["result"].V)
	}
}

// TestArrayAgg_ScanRowsJSON verifies array_agg result can be scanned into a
// Go slice via ScanRows.
func TestArrayAgg_ScanRowsJSON(t *testing.T) {
	db := New()
	mustExec(t, db, `INSERT INTO tags (tag) VALUES ('go')`)
	mustExec(t, db, `INSERT INTO tags (tag) VALUES ('sql')`)
	mustExec(t, db, `INSERT INTO tags (tag) VALUES ('test')`)

	rows := mustQuery(t, db, `SELECT array_agg(tag) AS tags FROM tags`)

	type Result struct {
		Tags []string `db:"tags"`
	}
	results := ScanRows[Result](rows)
	if len(results) != 1 {
		t.Fatalf("expected 1 result, got %d", len(results))
	}
	// The underlying []any contains string values; we check via JSON round-trip.
	b, _ := json.Marshal(results[0].Tags)
	var back []string
	json.Unmarshal(b, &back)
	if !reflect.DeepEqual(back, []string{"go", "sql", "test"}) {
		t.Errorf("unexpected tags: %v", back)
	}
}

// ═══════════════════════════════════════════════════════════════════════════════
// Gap 3 — ON CONFLICT (cols) WHERE partial_pred DO UPDATE
// ═══════════════════════════════════════════════════════════════════════════════

// TestOnConflict_PartialIndex_Simple tests that a partial-index ON CONFLICT with
// a WHERE predicate on the conflict clause is parsed and executed correctly.
// vapordb ignores the predicate (all conflict-column matches are treated equally)
// but must not return a parse error.
func TestOnConflict_PartialIndex_Simple(t *testing.T) {
	db := New()
	mustExec(t, db, `INSERT INTO fy_clients (fy_id, client_id, deleted_at, data)
		VALUES (1, 10, NULL, 'original')`)

	// Conflict on (fy_id, client_id) — partial index WHERE deleted_at IS NULL.
	mustExec(t, db, `
		INSERT INTO fy_clients (fy_id, client_id, deleted_at, data)
		VALUES (1, 10, NULL, 'updated')
		ON CONFLICT (fy_id, client_id) WHERE deleted_at IS NULL
		DO UPDATE SET data = EXCLUDED.data`)

	rows := mustQuery(t, db, `SELECT data FROM fy_clients WHERE fy_id = 1 AND client_id = 10`)
	if len(rows) != 1 {
		t.Fatalf("expected 1 row, got %d", len(rows))
	}
	if rows[0]["data"].V != "updated" {
		t.Errorf("expected 'updated', got %v", rows[0]["data"].V)
	}
}

// TestOnConflict_PartialIndex_DoNothing tests the DO NOTHING variant with a
// partial-index predicate.
func TestOnConflict_PartialIndex_DoNothing(t *testing.T) {
	db := New()
	mustExec(t, db, `INSERT INTO slots (day, slot, note)
		VALUES ('2024-01-01', 1, 'booked')`)

	// Second insert with same conflict key should silently skip.
	mustExec(t, db, `
		INSERT INTO slots (day, slot, note)
		VALUES ('2024-01-01', 1, 'attempt')
		ON CONFLICT (day, slot) WHERE note IS NOT NULL
		DO NOTHING`)

	rows := mustQuery(t, db, `SELECT note FROM slots`)
	if len(rows) != 1 {
		t.Fatalf("expected 1 row, got %d", len(rows))
	}
	if rows[0]["note"].V != "booked" {
		t.Errorf("expected original 'booked', got %v", rows[0]["note"].V)
	}
}

// TestOnConflict_PartialIndex_NoConflict tests that a non-conflicting row is
// inserted normally even when a partial-index predicate is present.
func TestOnConflict_PartialIndex_NoConflict(t *testing.T) {
	db := New()
	mustExec(t, db, `INSERT INTO items2 (k, v) VALUES (1, 'first')`)

	mustExec(t, db, `
		INSERT INTO items2 (k, v)
		VALUES (2, 'second')
		ON CONFLICT (k) WHERE v IS NOT NULL
		DO UPDATE SET v = EXCLUDED.v`)

	rows := mustQuery(t, db, `SELECT COUNT(*) AS n FROM items2`)
	if rows[0]["n"].V != int64(2) {
		t.Errorf("expected 2 rows after non-conflicting insert, got %v", rows[0]["n"].V)
	}
}

// TestOnConflict_PartialIndex_ComplexPredicate tests a multi-condition predicate.
func TestOnConflict_PartialIndex_ComplexPredicate(t *testing.T) {
	db := New()
	mustExec(t, db, `INSERT INTO reservations (res_id, seat, status, val)
		VALUES (1, 'A1', 'active', 'orig')`)

	mustExec(t, db, `
		INSERT INTO reservations (res_id, seat, status, val)
		VALUES (1, 'A1', 'active', 'new')
		ON CONFLICT (res_id, seat) WHERE status = 'active' AND val IS NOT NULL
		DO UPDATE SET val = EXCLUDED.val`)

	rows := mustQuery(t, db, `SELECT val FROM reservations`)
	if rows[0]["val"].V != "new" {
		t.Errorf("expected 'new', got %v", rows[0]["val"].V)
	}
}

// ═══════════════════════════════════════════════════════════════════════════════
// Gap 4 — ON CONFLICT DO UPDATE SET … WHERE predicate (optimistic lock)
// ═══════════════════════════════════════════════════════════════════════════════

// TestOnConflict_OptimisticLock_Success verifies that an upsert with a matching
// WHERE predicate (lock succeeds) applies the update.
func TestOnConflict_OptimisticLock_Success(t *testing.T) {
	db := New()
	mustExec(t, db, `INSERT INTO budgets (id, amount, version) VALUES (1, 100, 1)`)

	mustExec(t, db, `
		INSERT INTO budgets (id, amount, version) VALUES (1, 200, 2)
		ON CONFLICT (id) DO UPDATE SET
			amount  = EXCLUDED.amount,
			version = EXCLUDED.version
		WHERE budgets.version = 1`)

	rows := mustQuery(t, db, `SELECT amount, version FROM budgets WHERE id = 1`)
	if len(rows) != 1 {
		t.Fatalf("expected 1 row, got %d", len(rows))
	}
	if rows[0]["amount"].V != int64(200) {
		t.Errorf("amount: got %v, want 200 (lock should have succeeded)", rows[0]["amount"].V)
	}
	if rows[0]["version"].V != int64(2) {
		t.Errorf("version: got %v, want 2", rows[0]["version"].V)
	}
}

// TestOnConflict_OptimisticLock_Failure verifies that an upsert with a non-matching
// WHERE predicate (lock fails = stale version) leaves the row unchanged.
func TestOnConflict_OptimisticLock_Failure(t *testing.T) {
	db := New()
	mustExec(t, db, `INSERT INTO budgets2 (id, amount, version) VALUES (1, 100, 5)`)

	// version 3 ≠ current 5 → predicate false → no update.
	mustExec(t, db, `
		INSERT INTO budgets2 (id, amount, version) VALUES (1, 999, 6)
		ON CONFLICT (id) DO UPDATE SET
			amount  = EXCLUDED.amount,
			version = EXCLUDED.version
		WHERE budgets2.version = 3`)

	rows := mustQuery(t, db, `SELECT amount, version FROM budgets2 WHERE id = 1`)
	if rows[0]["amount"].V != int64(100) {
		t.Errorf("amount: got %v, want 100 (stale lock should not update)", rows[0]["amount"].V)
	}
	if rows[0]["version"].V != int64(5) {
		t.Errorf("version: got %v, want 5 (unchanged)", rows[0]["version"].V)
	}
}

// TestOnConflict_OptimisticLock_UnqualifiedColumn verifies that the predicate
// also works when the column is referenced without a table qualifier.
func TestOnConflict_OptimisticLock_UnqualifiedColumn(t *testing.T) {
	db := New()
	mustExec(t, db, `INSERT INTO docs (id, body, rev) VALUES (42, 'hello', 1)`)

	mustExec(t, db, `
		INSERT INTO docs (id, body, rev) VALUES (42, 'world', 2)
		ON CONFLICT (id) DO UPDATE SET
			body = EXCLUDED.body,
			rev  = EXCLUDED.rev
		WHERE rev = 1`)

	rows := mustQuery(t, db, `SELECT body, rev FROM docs WHERE id = 42`)
	if rows[0]["body"].V != "world" {
		t.Errorf("body: got %v, want 'world'", rows[0]["body"].V)
	}
}

// TestOnConflict_OptimisticLock_NoConflict verifies that a non-conflicting insert
// still goes through normally (predicate is not evaluated on a fresh insert).
func TestOnConflict_OptimisticLock_NoConflict(t *testing.T) {
	db := New()
	mustExec(t, db, `INSERT INTO items3 (id, val, v) VALUES (1, 'a', 1)`)

	mustExec(t, db, `
		INSERT INTO items3 (id, val, v) VALUES (2, 'b', 1)
		ON CONFLICT (id) DO UPDATE SET val = EXCLUDED.val
		WHERE items3.v = 99`)

	rows := mustQuery(t, db, `SELECT COUNT(*) AS n FROM items3`)
	if rows[0]["n"].V != int64(2) {
		t.Errorf("expected 2 rows, got %v", rows[0]["n"].V)
	}
}

// TestOnConflict_OptimisticLock_MultipleRows tests the scenario where multiple
// rows are inserted in one statement, some locked-out and some updated.
func TestOnConflict_OptimisticLock_MultipleRows(t *testing.T) {
	db := New()
	mustExec(t, db, `INSERT INTO widgets (id, name, v) VALUES (1, 'alpha', 1)`)
	mustExec(t, db, `INSERT INTO widgets (id, name, v) VALUES (2, 'beta',  3)`)
	mustExec(t, db, `INSERT INTO widgets (id, name, v) VALUES (3, 'gamma', 1)`)

	// Only id=1 and id=3 have v=1 → update; id=2 (v=3) is stale → skip.
	mustExec(t, db, `
		INSERT INTO widgets (id, name, v) VALUES
			(1, 'ALPHA', 2),
			(2, 'BETA',  4),
			(3, 'GAMMA', 2)
		ON CONFLICT (id) DO UPDATE SET
			name = EXCLUDED.name,
			v    = EXCLUDED.v
		WHERE widgets.v = 1`)

	rows := mustQuery(t, db, `SELECT id, name, v FROM widgets ORDER BY id`)
	if rows[0]["name"].V != "ALPHA" {
		t.Errorf("id=1: got %v, want ALPHA (lock ok)", rows[0]["name"].V)
	}
	if rows[1]["name"].V != "beta" {
		t.Errorf("id=2: got %v, want beta (lock failed, unchanged)", rows[1]["name"].V)
	}
	if rows[2]["name"].V != "GAMMA" {
		t.Errorf("id=3: got %v, want GAMMA (lock ok)", rows[2]["name"].V)
	}
}

// TestOnConflict_BothGaps3and4 combines a partial-index predicate (Gap 3) with
// an optimistic-lock WHERE on the SET clause (Gap 4).
func TestOnConflict_BothGaps3and4(t *testing.T) {
	db := New()
	mustExec(t, db, `INSERT INTO mixed (fy_id, cid, deleted_at, val, ver)
		VALUES (1, 10, NULL, 'orig', 1)`)

	// Partial index: WHERE deleted_at IS NULL
	// Optimistic lock: WHERE mixed.ver = 1
	mustExec(t, db, `
		INSERT INTO mixed (fy_id, cid, deleted_at, val, ver)
		VALUES (1, 10, NULL, 'new', 2)
		ON CONFLICT (fy_id, cid) WHERE deleted_at IS NULL
		DO UPDATE SET val = EXCLUDED.val, ver = EXCLUDED.ver
		WHERE mixed.ver = 1`)

	rows := mustQuery(t, db, `SELECT val, ver FROM mixed`)
	if rows[0]["val"].V != "new" {
		t.Errorf("val: got %v, want 'new'", rows[0]["val"].V)
	}
	if rows[0]["ver"].V != int64(2) {
		t.Errorf("ver: got %v, want 2", rows[0]["ver"].V)
	}

	// Now try again with wrong version — should be a no-op.
	mustExec(t, db, `
		INSERT INTO mixed (fy_id, cid, deleted_at, val, ver)
		VALUES (1, 10, NULL, 'rejected', 3)
		ON CONFLICT (fy_id, cid) WHERE deleted_at IS NULL
		DO UPDATE SET val = EXCLUDED.val, ver = EXCLUDED.ver
		WHERE mixed.ver = 1`)

	rows = mustQuery(t, db, `SELECT val, ver FROM mixed`)
	if rows[0]["val"].V != "new" {
		t.Errorf("val after stale lock: got %v, want 'new' (unchanged)", rows[0]["val"].V)
	}
}

// ── internal utility ─────────────────────────────────────────────────────────

// itoa converts an int to its decimal string representation without importing strconv.
func itoa(n int) string {
	if n == 0 {
		return "0"
	}
	neg := n < 0
	if neg {
		n = -n
	}
	var buf [20]byte
	pos := len(buf)
	for n > 0 {
		pos--
		buf[pos] = byte('0' + n%10)
		n /= 10
	}
	if neg {
		pos--
		buf[pos] = '-'
	}
	return string(buf[pos:])
}
