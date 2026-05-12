package vapordb

import (
	"strings"
	"testing"
)

func TestGenerateDDL_MySQL(t *testing.T) {
	db := New()
	mustExec(t, db, `INSERT INTO users (id, name, active, score) VALUES (1, 'Alice', TRUE, 9.5)`)

	ddl, err := db.GenerateDDL("mysql")
	if err != nil {
		t.Fatalf("GenerateDDL mysql: %v", err)
	}

	if !strings.Contains(ddl, "CREATE TABLE `users`") {
		t.Errorf("expected backtick-quoted table name, got:\n%s", ddl)
	}
	if !strings.Contains(ddl, "`active` TINYINT(1)") {
		t.Errorf("expected bool → TINYINT(1), got:\n%s", ddl)
	}
	if !strings.Contains(ddl, "`id` BIGINT") {
		t.Errorf("expected int → BIGINT, got:\n%s", ddl)
	}
	if !strings.Contains(ddl, "`score` DOUBLE") {
		t.Errorf("expected float → DOUBLE, got:\n%s", ddl)
	}
	if !strings.Contains(ddl, "`name` TEXT") {
		t.Errorf("expected string → TEXT, got:\n%s", ddl)
	}
}

func TestGenerateDDL_Postgres(t *testing.T) {
	db := New()
	mustExec(t, db, `INSERT INTO events (id, ts) VALUES (1, DATE('2024-01-01'))`)

	ddl, err := db.GenerateDDL("postgres")
	if err != nil {
		t.Fatalf("GenerateDDL postgres: %v", err)
	}

	if !strings.Contains(ddl, `CREATE TABLE "events"`) {
		t.Errorf("expected double-quoted table name, got:\n%s", ddl)
	}
	if !strings.Contains(ddl, `"ts" TIMESTAMP`) {
		t.Errorf("expected date → TIMESTAMP, got:\n%s", ddl)
	}
	if !strings.Contains(ddl, `"id" BIGINT`) {
		t.Errorf("expected int → BIGINT, got:\n%s", ddl)
	}
}

func TestGenerateDDL_Enum_MySQL(t *testing.T) {
	db := New()
	db.DeclareEnum("orders", "status", "pending", "shipped", "delivered")
	mustExec(t, db, `INSERT INTO orders (id, status) VALUES (1, 'pending')`)

	ddl, err := db.GenerateDDL("mysql")
	if err != nil {
		t.Fatalf("GenerateDDL: %v", err)
	}

	if !strings.Contains(ddl, "ENUM(") {
		t.Errorf("expected ENUM(…) for enum column, got:\n%s", ddl)
	}
	if !strings.Contains(ddl, "'pending'") {
		t.Errorf("expected 'pending' in ENUM, got:\n%s", ddl)
	}
}

func TestGenerateDDL_Enum_Postgres(t *testing.T) {
	db := New()
	db.DeclareEnum("orders", "status", "open", "closed")
	mustExec(t, db, `INSERT INTO orders (id, status) VALUES (1, 'open')`)

	ddl, err := db.GenerateDDL("postgres")
	if err != nil {
		t.Fatalf("GenerateDDL: %v", err)
	}

	if !strings.Contains(ddl, "CHECK (") {
		t.Errorf("expected CHECK constraint for postgres enum, got:\n%s", ddl)
	}
	if !strings.Contains(ddl, "'open'") {
		t.Errorf("expected 'open' in CHECK constraint, got:\n%s", ddl)
	}
}

func TestGenerateDDL_JSON_MySQL(t *testing.T) {
	db := New()
	mustExec(t, db, `INSERT INTO posts (id, meta) VALUES (1, json_parse('{"k":"v"}'))`)

	ddl, err := db.GenerateDDL("mysql")
	if err != nil {
		t.Fatalf("GenerateDDL: %v", err)
	}

	if !strings.Contains(ddl, "`meta` JSON") {
		t.Errorf("expected JSON type for meta column, got:\n%s", ddl)
	}
}

func TestGenerateDDL_JSON_Postgres(t *testing.T) {
	db := New()
	mustExec(t, db, `INSERT INTO posts (id, meta) VALUES (1, json_parse('{"k":"v"}'))`)

	ddl, err := db.GenerateDDL("postgres")
	if err != nil {
		t.Fatalf("GenerateDDL: %v", err)
	}

	if !strings.Contains(ddl, `"meta" JSONB`) {
		t.Errorf("expected JSONB type for meta column, got:\n%s", ddl)
	}
}

func TestGenerateDDL_UnsupportedDialect(t *testing.T) {
	db := New()
	_, err := db.GenerateDDL("sqlite")
	if err == nil {
		t.Error("expected error for unsupported dialect")
	}
}

func TestGenerateDDL_MultipleTables(t *testing.T) {
	db := New()
	mustExec(t, db, `INSERT INTO alpha (x) VALUES (1)`)
	mustExec(t, db, `INSERT INTO beta (y) VALUES ('hello')`)

	ddl, err := db.GenerateDDL("mysql")
	if err != nil {
		t.Fatalf("GenerateDDL: %v", err)
	}

	// Tables should appear in alphabetical order
	alphaIdx := strings.Index(ddl, "alpha")
	betaIdx := strings.Index(ddl, "beta")
	if alphaIdx < 0 || betaIdx < 0 {
		t.Fatalf("expected both tables in DDL, got:\n%s", ddl)
	}
	if alphaIdx > betaIdx {
		t.Errorf("expected alpha before beta in output")
	}
}
