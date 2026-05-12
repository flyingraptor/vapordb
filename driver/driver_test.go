package driver_test

import (
	"database/sql"
	"testing"

	vapordriver "github.com/flyingraptor/vapordb/driver"

	"github.com/flyingraptor/vapordb"
)

func newSQLDB(t *testing.T) (*sql.DB, *vapordb.DB) {
	t.Helper()
	vdb := vapordb.New()
	name := t.Name()
	vapordriver.Register(name, vdb)
	t.Cleanup(func() { vapordriver.Unregister(name) })

	sqlDB, err := sql.Open("vapordb", name)
	if err != nil {
		t.Fatalf("sql.Open: %v", err)
	}
	t.Cleanup(func() { sqlDB.Close() })
	return sqlDB, vdb
}

// ── basic exec / query ─────────────────────────────────────────────────────────

func TestDriverExecQuery(t *testing.T) {
	db, _ := newSQLDB(t)

	if _, err := db.Exec(`INSERT INTO users (id, name) VALUES (1, 'Alice')`); err != nil {
		t.Fatalf("Exec INSERT: %v", err)
	}
	if _, err := db.Exec(`INSERT INTO users (id, name) VALUES (2, 'Bob')`); err != nil {
		t.Fatalf("Exec INSERT: %v", err)
	}

	rows, err := db.Query(`SELECT id, name FROM users ORDER BY id`)
	if err != nil {
		t.Fatalf("Query SELECT: %v", err)
	}
	defer rows.Close()

	cols, _ := rows.Columns()
	t.Logf("columns: %v", cols)

	type user struct{ id int64; name string }
	var users []user
	for rows.Next() {
		var u user
		if err := rows.Scan(&u.id, &u.name); err != nil {
			t.Fatalf("Scan: %v", err)
		}
		users = append(users, u)
	}
	if len(users) != 2 {
		t.Fatalf("want 2 users, got %d", len(users))
	}
}

// ── positional ? parameters ────────────────────────────────────────────────────

func TestDriverQMarkParams(t *testing.T) {
	db, _ := newSQLDB(t)

	if _, err := db.Exec(`INSERT INTO items (id, name, price) VALUES (?, ?, ?)`, 1, "Widget", 9.99); err != nil {
		t.Fatalf("Exec with ?: %v", err)
	}

	rows, err := db.Query(`SELECT name FROM items WHERE id = ?`, 1)
	if err != nil {
		t.Fatalf("Query with ?: %v", err)
	}
	defer rows.Close()

	if !rows.Next() {
		t.Fatal("expected 1 row")
	}
	var name string
	if err := rows.Scan(&name); err != nil {
		t.Fatalf("Scan: %v", err)
	}
	if name != "Widget" {
		t.Errorf("want Widget, got %q", name)
	}
}

// ── Postgres $N parameters ─────────────────────────────────────────────────────

func TestDriverDollarParams(t *testing.T) {
	db, _ := newSQLDB(t)

	if _, err := db.Exec(`INSERT INTO items (id, name) VALUES ($1, $2)`, 42, "Gadget"); err != nil {
		t.Fatalf("Exec with $N: %v", err)
	}

	rows, err := db.Query(`SELECT name FROM items WHERE id = $1`, 42)
	if err != nil {
		t.Fatalf("Query with $N: %v", err)
	}
	defer rows.Close()

	if !rows.Next() {
		t.Fatal("expected 1 row")
	}
	var name string
	_ = rows.Scan(&name)
	if name != "Gadget" {
		t.Errorf("want Gadget, got %q", name)
	}
}

// ── transactions ──────────────────────────────────────────────────────────────

func TestDriverTxCommit(t *testing.T) {
	db, _ := newSQLDB(t)

	if _, err := db.Exec(`INSERT INTO accounts (id, balance) VALUES (1, 100)`); err != nil {
		t.Fatalf("seed: %v", err)
	}

	tx, err := db.Begin()
	if err != nil {
		t.Fatalf("Begin: %v", err)
	}

	if _, err := tx.Exec(`UPDATE accounts SET balance = 200 WHERE id = 1`); err != nil {
		t.Fatalf("tx.Exec: %v", err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("Commit: %v", err)
	}

	row := db.QueryRow(`SELECT balance FROM accounts WHERE id = 1`)
	var balance int64
	if err := row.Scan(&balance); err != nil {
		t.Fatalf("Scan: %v", err)
	}
	if balance != 200 {
		t.Errorf("want balance=200 after commit, got %d", balance)
	}
}

func TestDriverTxRollback(t *testing.T) {
	db, _ := newSQLDB(t)

	if _, err := db.Exec(`INSERT INTO accounts (id, balance) VALUES (1, 100)`); err != nil {
		t.Fatalf("seed: %v", err)
	}

	tx, err := db.Begin()
	if err != nil {
		t.Fatalf("Begin: %v", err)
	}

	if _, err := tx.Exec(`UPDATE accounts SET balance = 999 WHERE id = 1`); err != nil {
		t.Fatalf("tx.Exec: %v", err)
	}
	if err := tx.Rollback(); err != nil {
		t.Fatalf("Rollback: %v", err)
	}

	row := db.QueryRow(`SELECT balance FROM accounts WHERE id = 1`)
	var balance int64
	if err := row.Scan(&balance); err != nil {
		t.Fatalf("Scan: %v", err)
	}
	if balance != 100 {
		t.Errorf("want balance=100 after rollback, got %d", balance)
	}
}

// ── NULL values ───────────────────────────────────────────────────────────────

func TestDriverNullScan(t *testing.T) {
	db, _ := newSQLDB(t)

	if _, err := db.Exec(`INSERT INTO rows (id, note) VALUES (1, NULL)`); err != nil {
		t.Fatalf("Exec: %v", err)
	}

	row := db.QueryRow(`SELECT note FROM rows WHERE id = 1`)
	var note sql.NullString
	if err := row.Scan(&note); err != nil {
		t.Fatalf("Scan: %v", err)
	}
	if note.Valid {
		t.Errorf("expected NULL note, got %q", note.String)
	}
}

// ── Ping ──────────────────────────────────────────────────────────────────────

func TestDriverPing(t *testing.T) {
	db, _ := newSQLDB(t)
	if err := db.Ping(); err != nil {
		t.Fatalf("Ping: %v", err)
	}
}

// ── QueryRow ─────────────────────────────────────────────────────────────────

func TestDriverQueryRow(t *testing.T) {
	db, _ := newSQLDB(t)

	if _, err := db.Exec(`INSERT INTO nums (v) VALUES (42)`); err != nil {
		t.Fatalf("seed: %v", err)
	}

	row := db.QueryRow(`SELECT v FROM nums`)
	var v int64
	if err := row.Scan(&v); err != nil {
		t.Fatalf("Scan: %v", err)
	}
	if v != 42 {
		t.Errorf("want 42, got %d", v)
	}
}

// ── open unknown name ─────────────────────────────────────────────────────────

func TestDriverOpenUnregistered(t *testing.T) {
	db, err := sql.Open("vapordb", "no-such-name-xyz")
	if err != nil {
		t.Fatalf("sql.Open should not fail immediately: %v", err)
	}
	defer db.Close()
	// Ping triggers the actual Open call
	if err := db.Ping(); err == nil {
		t.Error("expected error pinging unregistered database")
	}
}
