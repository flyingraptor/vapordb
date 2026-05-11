package vapordb

import (
	"errors"
	"net"
	"strings"
	"testing"
	"time"
)

// ── fixtures ──────────────────────────────────────────────────────────────────

func namedDB(t *testing.T) *DB {
	t.Helper()
	db := New()
	mustExec(t, db, `INSERT INTO users (id, name, age, score) VALUES (1, 'Alice', 30, 88.5)`)
	mustExec(t, db, `INSERT INTO users (id, name, age, score) VALUES (2, 'Bob',   25, 72.0)`)
	mustExec(t, db, `INSERT INTO users (id, name, age, score) VALUES (3, 'Carol', 28, 95.0)`)
	mustExec(t, db, `INSERT INTO users (id, name, age, score) VALUES (4, 'Dave',  35, NULL)`)
	return db
}

func queryNamed(t *testing.T, db *DB, sql string, params any) []Row {
	t.Helper()
	rows, err := db.QueryNamed(sql, params)
	if err != nil {
		t.Fatalf("QueryNamed error: %v", err)
	}
	return rows
}

func execNamed(t *testing.T, db *DB, sql string, params any) {
	t.Helper()
	if err := db.ExecNamed(sql, params); err != nil {
		t.Fatalf("ExecNamed error: %v", err)
	}
}

// ── map[string]any params ────────────────────────────────────────────────────

func TestNamedQueryWithMap(t *testing.T) {
	db := namedDB(t)
	rows := queryNamed(t, db,
		`SELECT name FROM users WHERE age = :age ORDER BY name`,
		map[string]any{"age": 30},
	)
	if len(rows) != 1 || rows[0]["name"] != strVal("Alice") {
		t.Errorf("want [Alice], got %v", rows)
	}
}

func TestNamedQueryMultipleParams(t *testing.T) {
	db := namedDB(t)
	rows := queryNamed(t, db,
		`SELECT name FROM users WHERE age >= :min AND age <= :max ORDER BY name`,
		map[string]any{"min": 25, "max": 30},
	)
	if len(rows) != 3 {
		t.Errorf("want 3 rows, got %d: %v", len(rows), rows)
	}
}

func TestNamedQueryStringParam(t *testing.T) {
	db := namedDB(t)
	rows := queryNamed(t, db,
		`SELECT id FROM users WHERE name = :name`,
		map[string]any{"name": "Carol"},
	)
	if len(rows) != 1 || rows[0]["id"] != intVal(3) {
		t.Errorf("want id=3, got %v", rows)
	}
}

func TestNamedQueryFloatParam(t *testing.T) {
	db := namedDB(t)
	rows := queryNamed(t, db,
		`SELECT name FROM users WHERE score >= :min ORDER BY name`,
		map[string]any{"min": 88.5},
	)
	if len(rows) != 2 {
		t.Errorf("want 2 rows (Alice, Carol), got %d: %v", len(rows), rows)
	}
}

func TestNamedQueryNullParam(t *testing.T) {
	db := namedDB(t)
	// NULL param: WHERE score = NULL should match nothing (NULL = NULL is false)
	rows := queryNamed(t, db,
		`SELECT name FROM users WHERE score = :s`,
		map[string]any{"s": nil},
	)
	if len(rows) != 0 {
		t.Errorf("NULL = NULL should return no rows, got %v", rows)
	}
}

func TestNamedQueryBoolParam(t *testing.T) {
	db := New()
	mustExec(t, db, `INSERT INTO flags (id, active) VALUES (1, 1)`)
	mustExec(t, db, `INSERT INTO flags (id, active) VALUES (2, 0)`)
	rows := queryNamed(t, db,
		`SELECT id FROM flags WHERE active = :active`,
		map[string]any{"active": true},
	)
	if len(rows) != 1 || rows[0]["id"] != intVal(1) {
		t.Errorf("bool param: want [1], got %v", rows)
	}
}

func TestNamedParamReusedMultipleTimes(t *testing.T) {
	db := namedDB(t)
	// same :age used twice
	rows := queryNamed(t, db,
		`SELECT name FROM users WHERE age > :age OR age < :age ORDER BY name`,
		map[string]any{"age": 30},
	)
	// age > 30: Dave(35); age < 30: Bob(25), Carol(28) — all except Alice
	if len(rows) != 3 {
		t.Errorf("reused param: want 3 rows, got %d: %v", len(rows), rows)
	}
}

// ── struct params ─────────────────────────────────────────────────────────────

func TestNamedQueryWithStruct(t *testing.T) {
	type Filter struct {
		MinAge int    `db:"min"`
		Name   string `db:"name"`
	}
	db := namedDB(t)
	rows := queryNamed(t, db,
		`SELECT id FROM users WHERE age >= :min AND name != :name ORDER BY id`,
		Filter{MinAge: 28, Name: "Dave"},
	)
	// age >= 28: Alice(30), Carol(28), Dave(35); minus Dave → Alice, Carol
	if len(rows) != 2 {
		t.Errorf("struct param: want 2 rows, got %d: %v", len(rows), rows)
	}
}

func TestNamedQueryWithStructPointer(t *testing.T) {
	type Filter struct {
		Age int `db:"age"`
	}
	db := namedDB(t)
	rows := queryNamed(t, db,
		`SELECT name FROM users WHERE age = :age`,
		&Filter{Age: 25},
	)
	if len(rows) != 1 || rows[0]["name"] != strVal("Bob") {
		t.Errorf("struct pointer: want [Bob], got %v", rows)
	}
}

func TestNamedQueryEmbeddedStructParams(t *testing.T) {
	type BaseFilter struct {
		MinAge int `db:"min"`
	}
	type filter struct {
		BaseFilter
		ExcludeName string `db:"name"`
	}
	db := namedDB(t)
	rows := queryNamed(t, db,
		`SELECT id FROM users WHERE age >= :min AND name != :name ORDER BY id`,
		filter{BaseFilter: BaseFilter{MinAge: 28}, ExcludeName: "Dave"},
	)
	if len(rows) != 2 {
		t.Errorf("embedded struct params: want 2 rows, got %d: %v", len(rows), rows)
	}
}

// ── ExecNamed ────────────────────────────────────────────────────────────────

func TestNamedExecInsert(t *testing.T) {
	db := New()
	execNamed(t, db,
		`INSERT INTO products (id, name, price) VALUES (:id, :name, :price)`,
		map[string]any{"id": 1, "name": "Widget", "price": 9.99},
	)
	rows := mustQuery(t, db, `SELECT name, price FROM products WHERE id = 1`)
	if len(rows) != 1 || rows[0]["name"] != strVal("Widget") {
		t.Errorf("named insert: got %v", rows)
	}
}

func TestNamedExecUpdate(t *testing.T) {
	db := namedDB(t)
	execNamed(t, db,
		`UPDATE users SET score = :score WHERE name = :name`,
		map[string]any{"score": 99.5, "name": "Bob"},
	)
	rows := mustQuery(t, db, `SELECT score FROM users WHERE name = 'Bob'`)
	if len(rows) != 1 || rows[0]["score"] != floatVal(99.5) {
		t.Errorf("named update: got %v", rows)
	}
}

func TestNamedExecDelete(t *testing.T) {
	db := namedDB(t)
	execNamed(t, db,
		`DELETE FROM users WHERE age < :age`,
		map[string]any{"age": 28},
	)
	rows := mustQuery(t, db, `SELECT id FROM users ORDER BY id`)
	// only Bob(25) removed → 3 remaining
	if len(rows) != 3 {
		t.Errorf("named delete: want 3 rows, got %d", len(rows))
	}
}

// ── SQL injection safety ──────────────────────────────────────────────────────

func TestNamedParamEscapesSingleQuote(t *testing.T) {
	db := New()
	mustExec(t, db, `INSERT INTO notes (id, body) VALUES (1, 'hello')`)

	// Value contains a single quote — must not break the query
	execNamed(t, db,
		`INSERT INTO notes (id, body) VALUES (:id, :body)`,
		map[string]any{"id": 2, "body": "it's fine"},
	)
	rows := mustQuery(t, db, `SELECT body FROM notes WHERE id = 2`)
	if len(rows) != 1 || rows[0]["body"] != strVal("it's fine") {
		t.Errorf("quote escape: got %v", rows)
	}
}

func TestNamedParamInsideStringLiteralNotReplaced(t *testing.T) {
	db := namedDB(t)
	// :age inside a string literal must NOT be replaced
	rows := queryNamed(t, db,
		`SELECT name FROM users WHERE name != ':age' ORDER BY name`,
		map[string]any{"age": 99},
	)
	// all 4 rows have name != ':age' literal string
	if len(rows) != 4 {
		t.Errorf("literal string: want 4 rows, got %d", len(rows))
	}
}

// ── TextMarshaler / fmt.Stringer / time (e.g. uuid.UUID, net.IP) ─────────────

// namedUUIDStub mirrors github.com/google/uuid.UUID: [16]byte + TextMarshaler.
type namedUUIDStub [16]byte

func (namedUUIDStub) MarshalText() ([]byte, error) {
	return []byte("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"), nil
}

func TestNamedQueryTextMarshalerUUIDLike(t *testing.T) {
	db := New()
	mustExec(t, db, `INSERT INTO docs (id, body) VALUES ('aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa', 'ok')`)
	var id namedUUIDStub
	rows := queryNamed(t, db, `SELECT body FROM docs WHERE id = :id`, map[string]any{"id": id})
	if len(rows) != 1 || rows[0]["body"] != strVal("ok") {
		t.Fatalf("want one row body=ok, got %v", rows)
	}
}

func TestNamedQueryNetIP(t *testing.T) {
	db := New()
	mustExec(t, db, `INSERT INTO hosts (addr) VALUES ('192.0.2.1')`)
	ip := net.ParseIP("192.0.2.1")
	rows := queryNamed(t, db, `SELECT addr FROM hosts WHERE addr = :ip`, map[string]any{"ip": ip})
	if len(rows) != 1 || rows[0]["addr"] != strVal("192.0.2.1") {
		t.Fatalf("want one row addr 192.0.2.1, got %v", rows)
	}
}

func TestNamedQueryTimeParam(t *testing.T) {
	db := New()
	mustExec(t, db, `INSERT INTO evt (id, ts) VALUES (1, DATE('2024-06-01 00:00:00'))`)
	ts := time.Date(2024, 6, 1, 0, 0, 0, 0, time.UTC)
	rows := queryNamed(t, db, `SELECT id FROM evt WHERE ts = :ts`, map[string]any{"ts": ts})
	if len(rows) != 1 || rows[0]["id"] != intVal(1) {
		t.Fatalf("want id=1, got %v", rows)
	}
}

type badMarshal struct{}

func (badMarshal) MarshalText() ([]byte, error) {
	return nil, errors.New("marshal failed")
}

func TestNamedParamMarshalTextError(t *testing.T) {
	db := New()
	_, err := db.QueryNamed(`SELECT 1 AS n FROM DUAL WHERE 1 = :x`, map[string]any{"x": badMarshal{}})
	if err == nil || !strings.Contains(err.Error(), "MarshalText") {
		t.Fatalf("want MarshalText error, got %v", err)
	}
}

// ── error cases ───────────────────────────────────────────────────────────────

func TestNamedMissingParam(t *testing.T) {
	db := namedDB(t)
	_, err := db.QueryNamed(
		`SELECT name FROM users WHERE age = :age`,
		map[string]any{},
	)
	if err == nil {
		t.Error("expected error for missing :age param")
	}
}

func TestNamedInvalidParamsType(t *testing.T) {
	db := namedDB(t)
	_, err := db.QueryNamed(
		`SELECT name FROM users WHERE age = :age`,
		"not a map or struct",
	)
	if err == nil {
		t.Error("expected error for invalid params type")
	}
}

