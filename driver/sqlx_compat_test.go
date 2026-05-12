package driver_test

// Tests for sqlx compatibility (Gap 6).
//
// sqlx picks placeholder style from the driver name:
//   "postgres" / "pgx" / …  → DOLLAR  ($1, $2, …)
//   "mysql" / "sqlite3" / … → QUESTION (?)
//   anything else            → QUESTION (?)
//
// vapordb's driver is registered as "vapordb" (QUESTION default) but supports
// BOTH placeholder styles natively.  RegisterAs lets users alias the driver
// under a well-known name so sqlx auto-detects the bind style.
//
// We deliberately do NOT import jmoiron/sqlx to keep zero extra dependencies.
// Instead we exercise the same driver-level behaviour that sqlx relies on and
// provide clear code comments that serve as sqlx usage documentation.

import (
	"context"
	"database/sql"
	"fmt"
	"sync"
	"testing"

	vapordriver "github.com/flyingraptor/vapordb/driver"

	"github.com/flyingraptor/vapordb"
)

// ── RegisterAs ────────────────────────────────────────────────────────────────

// oneTimeAlias ensures the alias is registered exactly once across all test
// runs in this package (sql.Register is process-global).
var (
	aliasOnce  sync.Once
	aliasName  = "vapordb-pgx-test"
)

func registerAlias() {
	aliasOnce.Do(func() {
		vapordriver.RegisterAs(aliasName)
	})
}

func TestRegisterAs_Basic(t *testing.T) {
	registerAlias()

	vdb := vapordb.New()
	vapordriver.Register("alias-basic", vdb)
	defer vapordriver.Unregister("alias-basic")

	// Open via the alias name — should route to the same Driver.
	db, err := sql.Open(aliasName, "alias-basic")
	if err != nil {
		t.Fatalf("sql.Open via alias: %v", err)
	}
	defer db.Close()

	if err := db.Ping(); err != nil {
		t.Fatalf("Ping via alias: %v", err)
	}
}

func TestRegisterAs_Idempotent(t *testing.T) {
	// Calling RegisterAs multiple times with the same name must not panic.
	for i := 0; i < 5; i++ {
		vapordriver.RegisterAs(aliasName) // no-op after first call
	}
}

func TestRegisterAs_ExecQueryViaAlias(t *testing.T) {
	registerAlias()

	vdb := vapordb.New()
	vapordriver.Register("alias-eq", vdb)
	defer vapordriver.Unregister("alias-eq")

	db, err := sql.Open(aliasName, "alias-eq")
	if err != nil {
		t.Fatalf("sql.Open: %v", err)
	}
	defer db.Close()

	// Both placeholder styles must work via the alias.
	if _, err := db.ExecContext(context.Background(),
		`INSERT INTO t (id, v) VALUES (?, ?)`, 1, "hello"); err != nil {
		t.Fatalf("? style: %v", err)
	}
	if _, err := db.ExecContext(context.Background(),
		`INSERT INTO t (id, v) VALUES ($1, $2)`, 2, "world"); err != nil {
		t.Fatalf("$N style: %v", err)
	}

	rows, err := db.QueryContext(context.Background(),
		`SELECT v FROM t WHERE id = $1 OR id = $2 ORDER BY id`, 1, 2)
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	defer rows.Close()

	var got []string
	for rows.Next() {
		var v string
		if err := rows.Scan(&v); err != nil {
			t.Fatal(err)
		}
		got = append(got, v)
	}
	if len(got) != 2 || got[0] != "hello" || got[1] != "world" {
		t.Errorf("unexpected results: %v", got)
	}
}

// ── Simulated sqlx.Rebind behaviour ──────────────────────────────────────────
//
// sqlx.Rebind converts a query from one placeholder style to another.
// Because vapordb supports BOTH styles natively, rebinding is a no-op at the
// driver level: you can pass either style directly without any Rebind call.
//
// The functions below mirror what sqlx.Rebind does so we can test the
// round-trip without importing sqlx.

// rebindDollar converts ? placeholders to $N (mirrors sqlx.DOLLAR rebind).
func rebindDollar(query string) string {
	var (
		out   []byte
		n     int
		inStr bool
	)
	for i := 0; i < len(query); i++ {
		ch := query[i]
		if ch == '\'' {
			if inStr {
				out = append(out, ch)
				i++
				if i < len(query) && query[i] == '\'' {
					out = append(out, query[i])
				} else {
					inStr = false
					i--
				}
			} else {
				inStr = true
				out = append(out, ch)
			}
			continue
		}
		if !inStr && ch == '?' {
			n++
			out = append(out, []byte(fmt.Sprintf("$%d", n))...)
			continue
		}
		out = append(out, ch)
	}
	return string(out)
}

// rebindQuestion converts $N placeholders to ? (mirrors sqlx.QUESTION rebind).
func rebindQuestion(query string) string {
	// naive: replace $\d+ with ?
	var out []byte
	inStr := false
	i := 0
	for i < len(query) {
		ch := query[i]
		if ch == '\'' {
			if inStr {
				out = append(out, ch)
				i++
				if i < len(query) && query[i] == '\'' {
					out = append(out, query[i])
					i++
				} else {
					inStr = false
				}
				continue
			}
			inStr = true
			out = append(out, ch)
			i++
			continue
		}
		if !inStr && ch == '$' && i+1 < len(query) && query[i+1] >= '1' && query[i+1] <= '9' {
			j := i + 1
			for j < len(query) && query[j] >= '0' && query[j] <= '9' {
				j++
			}
			out = append(out, '?')
			i = j
			continue
		}
		out = append(out, ch)
		i++
	}
	return string(out)
}

func TestRebindHelpers(t *testing.T) {
	if got := rebindDollar("SELECT ? FROM t WHERE x = ?"); got != "SELECT $1 FROM t WHERE x = $2" {
		t.Errorf("rebindDollar: got %q", got)
	}
	if got := rebindQuestion("SELECT $1 FROM t WHERE x = $2"); got != "SELECT ? FROM t WHERE x = ?" {
		t.Errorf("rebindQuestion: got %q", got)
	}
}

// TestSqlxRebindPattern_DollarThenExec simulates the common sqlx pattern:
//   query := db.Rebind("INSERT INTO t VALUES (?, ?)")   → "$1, $2" for postgres driver
//   db.ExecContext(ctx, query, v1, v2)
//
// With vapordb, regardless of which style Rebind outputs, both work directly.
func TestSqlxRebindPattern_DollarThenExec(t *testing.T) {
	db, _ := newSQLDB(t)

	// Simulate: code written for QUESTION style, run through DOLLAR rebind
	// (e.g. service using sqlx with postgres driver calls Rebind).
	query := rebindDollar("INSERT INTO products (id, name, price) VALUES (?, ?, ?)")
	// query is now "INSERT INTO products (id, name, price) VALUES ($1, $2, $3)"

	if _, err := db.ExecContext(context.Background(), query, 1, "Widget", 9.99); err != nil {
		t.Fatalf("rebind→dollar exec: %v", err)
	}

	// Verify via ? style query (vapordb supports both in same connection).
	row := db.QueryRowContext(context.Background(),
		`SELECT name, price FROM products WHERE id = ?`, 1)
	var name string
	var price float64
	if err := row.Scan(&name, &price); err != nil {
		t.Fatalf("Scan: %v", err)
	}
	if name != "Widget" || price != 9.99 {
		t.Errorf("got name=%q price=%v", name, price)
	}
}

func TestSqlxRebindPattern_QuestionThenExec(t *testing.T) {
	db, _ := newSQLDB(t)

	// Simulate: code written for DOLLAR style, rebind to QUESTION
	// (e.g. vapordb driver with QUESTION bind type).
	query := rebindQuestion("INSERT INTO orders (id, amount) VALUES ($1, $2)")
	// query is now "INSERT INTO orders (id, amount) VALUES (?, ?)"

	if _, err := db.ExecContext(context.Background(), query, 42, 199.00); err != nil {
		t.Fatalf("rebind→question exec: %v", err)
	}

	row := db.QueryRowContext(context.Background(),
		`SELECT amount FROM orders WHERE id = $1`, 42)
	var amount float64
	if err := row.Scan(&amount); err != nil {
		t.Fatalf("Scan: %v", err)
	}
	if amount != 199.00 {
		t.Errorf("got %v, want 199.00", amount)
	}
}

// ── sqlx.NewDb pattern (documented, driver-level verification) ────────────────
//
// The recommended integration pattern when using sqlx with PostgreSQL-style
// code is:
//
//   sqlDB, _ := sql.Open("vapordb", "mydb")
//   db := sqlx.NewDb(sqlDB, "postgres")   ← tells sqlx to use $N placeholders
//
// The test below verifies the underlying behaviour that makes this work:
// the vapordb driver accepts $N arguments without any translation by sqlx.

func TestSqlxNewDbPattern_DollarStyle(t *testing.T) {
	db, _ := newSQLDB(t)

	// Queries written for PostgreSQL ($N style) work as-is.
	if _, err := db.ExecContext(context.Background(),
		`INSERT INTO employees (id, dept, salary) VALUES ($1, $2, $3)`,
		1, "engineering", 120000); err != nil {
		t.Fatalf("$N insert: %v", err)
	}
	if _, err := db.ExecContext(context.Background(),
		`INSERT INTO employees (id, dept, salary) VALUES ($1, $2, $3)`,
		2, "marketing", 95000); err != nil {
		t.Fatalf("$N insert: %v", err)
	}

	rows, err := db.QueryContext(context.Background(),
		`SELECT id, dept, salary FROM employees WHERE dept = $1 ORDER BY id`, "engineering")
	if err != nil {
		t.Fatalf("$N select: %v", err)
	}
	defer rows.Close()

	// vapordb returns columns alphabetically: dept, id, salary
	var ids []int64
	for rows.Next() {
		var id int64
		var dept string
		var salary int64
		if err := rows.Scan(&dept, &id, &salary); err != nil {
			t.Fatal(err)
		}
		ids = append(ids, id)
	}
	if len(ids) != 1 || ids[0] != 1 {
		t.Errorf("got ids=%v", ids)
	}
}

func TestSqlxNewDbPattern_NamedParams(t *testing.T) {
	// sqlx.NamedExec uses :name placeholders internally (via sqlx.Named).
	// vapordb's native QueryNamed/ExecNamed handles :name style directly.
	// This test shows that named parameter queries (as sqlx would generate)
	// work through the standard database/sql path too, once the caller has
	// already expanded :name → ? via the vapordb named-param layer.
	vdb := vapordb.New()
	const dsn = "sqlx-named-test"
	vapordriver.Register(dsn, vdb)
	defer vapordriver.Unregister(dsn)

	// Simulate what sqlx.NamedExec does: expand :name to ? for QUESTION driver.
	// vapordb provides this via ExecNamed which can be used directly.
	if err := vdb.ExecNamed(
		`INSERT INTO staff (id, name, role) VALUES (:id, :name, :role)`,
		map[string]any{"id": int64(7), "name": "Alice", "role": "admin"},
	); err != nil {
		t.Fatalf("ExecNamed: %v", err)
	}

	sqlDB, err2 := sql.Open("vapordb", dsn)
	if err2 != nil {
		t.Fatalf("sql.Open: %v", err2)
	}
	defer sqlDB.Close()

	row := sqlDB.QueryRowContext(context.Background(),
		`SELECT name, role FROM staff WHERE id = $1`, 7)
	var name, role string
	if err := row.Scan(&name, &role); err != nil {
		t.Fatalf("Scan: %v", err)
	}
	if name != "Alice" || role != "admin" {
		t.Errorf("got name=%q role=%q", name, role)
	}
}

// ── Mixed $N and ? in same session ───────────────────────────────────────────
//
// sqlx.Rebind outputs different styles depending on the configured bind type.
// If code paths mix (e.g. some helpers use ? while others use $N after
// migration), vapordb handles both in the same connection without issue.

func TestMixedPlaceholderStyles(t *testing.T) {
	db, _ := newSQLDB(t)

	// seed via ? style
	for i, name := range []string{"Alice", "Bob", "Charlie"} {
		if _, err := db.ExecContext(context.Background(),
			`INSERT INTO people (id, name) VALUES (?, ?)`, i+1, name); err != nil {
			t.Fatalf("? insert: %v", err)
		}
	}

	// query via $N style
	rows, err := db.QueryContext(context.Background(),
		`SELECT name FROM people WHERE id = $1 OR id = $2 ORDER BY id`, 1, 3)
	if err != nil {
		t.Fatalf("$N query: %v", err)
	}
	defer rows.Close()

	var names []string
	for rows.Next() {
		var n string
		if err := rows.Scan(&n); err != nil {
			t.Fatal(err)
		}
		names = append(names, n)
	}
	if len(names) != 2 || names[0] != "Alice" || names[1] != "Charlie" {
		t.Errorf("mixed styles: got %v", names)
	}
}

// ── Comprehensive budget-service simulation ───────────────────────────────────
//
// Reproduces the style of queries in budget-management-service when running
// under vapordb via the database/sql driver (as a drop-in PostgreSQL replace).
// Uses $N placeholders throughout (PostgreSQL production style).

func TestBudgetServiceDriverSimulation(t *testing.T) {
	db, _ := newSQLDB(t)
	ctx := context.Background()

	// Schema (auto-inferred from first INSERT).
	rows := []struct {
		accountID   int64
		accountCode string
		txType      string
		amount      int64
	}{
		{1, "A001", "RESERVED", 500},
		{1, "A001", "COMMITTED", 300},
		{1, "A002", "RESERVED", 200},
		{1, "A002", "ACTUAL", 700},
		{2, "A001", "RESERVED", 999}, // different account — should be excluded
	}
	for _, r := range rows {
		if _, err := db.ExecContext(ctx,
			`INSERT INTO budget_txns (account_id, account_code, type, amount) VALUES ($1, $2, $3, $4)`,
			r.accountID, r.accountCode, r.txType, r.amount); err != nil {
			t.Fatalf("insert: %v", err)
		}
	}

	// Simulate a GetBudgetUsage-style query:
	//   SELECT account_code,
	//          SUM(amount) FILTER (WHERE type = 'RESERVED')  AS reserved_amount,
	//          SUM(amount) FILTER (WHERE type = 'COMMITTED') AS committed_amount
	//   FROM budget_txns
	//   WHERE account_id = $1 AND account_code = ANY($2)
	//   GROUP BY account_code ORDER BY account_code
	//
	// After rewriteAnyAll: ANY($2) → IN ($2)
	// After CheckNamedValue: []string{"A001","A002"} → "{A001,A002}" → 'A001', 'A002'
	qrows, err := db.QueryContext(ctx, `
		SELECT account_code,
		       COALESCE(SUM(amount) FILTER (WHERE type = 'RESERVED'),  0) AS reserved_amount,
		       COALESCE(SUM(amount) FILTER (WHERE type = 'COMMITTED'), 0) AS committed_amount
		FROM budget_txns
		WHERE account_id   = $1
		  AND account_code = ANY($2)
		GROUP BY account_code
		ORDER BY account_code`,
		int64(1), []string{"A001", "A002"}, // plain slice via CheckNamedValue
	)
	if err != nil {
		t.Fatalf("budget query: %v", err)
	}
	defer qrows.Close()

	type row struct {
		Code      string
		Reserved  int64
		Committed int64
	}
	var results []row
	for qrows.Next() {
		// columns alphabetical: account_code, committed_amount, reserved_amount
		var r row
		if err := qrows.Scan(&r.Code, &r.Committed, &r.Reserved); err != nil {
			t.Fatalf("Scan: %v", err)
		}
		results = append(results, r)
	}

	if len(results) != 2 {
		t.Fatalf("expected 2 rows, got %d: %v", len(results), results)
	}
	// A001: reserved=500, committed=300
	if results[0].Code != "A001" || results[0].Reserved != 500 || results[0].Committed != 300 {
		t.Errorf("A001: got %+v", results[0])
	}
	// A002: reserved=200, committed=0 (coalesced)
	if results[1].Code != "A002" || results[1].Reserved != 200 || results[1].Committed != 0 {
		t.Errorf("A002: got %+v", results[1])
	}
}
