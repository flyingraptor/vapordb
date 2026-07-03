package vapordb

import (
	"strings"
	"testing"
)

func TestLintPortability_Postgres(t *testing.T) {
	cases := []struct {
		name    string
		sql     string
		wantSub string // substring expected in exactly-one warning
	}{
		{"backtick idents", "SELECT `id` FROM t", "backtick"},
		{"on duplicate key", "INSERT INTO t (id) VALUES (1) ON DUPLICATE KEY UPDATE id = id", "ON DUPLICATE KEY"},
		{"ifnull", "SELECT IFNULL(a, 0) FROM t", "IFNULL"},
		{"regexp", "SELECT * FROM t WHERE a RLIKE 'x'", "RLIKE"},
		{"mysql limit", "SELECT * FROM t LIMIT 10, 5", "LIMIT offset"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := lintPortability(TargetPostgres, tc.sql)
			if !containsSub(got, tc.wantSub) {
				t.Errorf("lintPortability(Postgres, %q) = %v, want a message containing %q", tc.sql, got, tc.wantSub)
			}
		})
	}
}

func TestLintPortability_MySQL(t *testing.T) {
	cases := []struct {
		name    string
		sql     string
		wantSub string
	}{
		{"double-quoted idents", `SELECT "id" FROM t`, "double-quoted"},
		{"on conflict", "INSERT INTO t (id) VALUES (1) ON CONFLICT (id) DO NOTHING", "ON CONFLICT"},
		{"ilike", "SELECT * FROM t WHERE name ILIKE 'a%'", "ILIKE"},
		{"cast operator", "SELECT id::text FROM t", ":: cast"},
		{"json containment", "SELECT * FROM t WHERE tags @> '[1]'", "@> / <@"},
		{"any array", "SELECT * FROM t WHERE id = ANY('{1,2}')", "= ANY"},
		{"nulls ordering", "SELECT * FROM t ORDER BY a NULLS LAST", "NULLS FIRST / NULLS LAST"},
		{"returning", "DELETE FROM t WHERE id = 1 RETURNING id", "RETURNING"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := lintPortability(TargetMySQL, tc.sql)
			if !containsSub(got, tc.wantSub) {
				t.Errorf("lintPortability(MySQL, %q) = %v, want a message containing %q", tc.sql, got, tc.wantSub)
			}
		})
	}
}

func TestLintPortability_Generic_NoWarnings(t *testing.T) {
	if got := lintPortability(TargetGeneric, "SELECT `id` FROM t WHERE name ILIKE 'x'"); got != nil {
		t.Errorf("TargetGeneric should never warn, got %v", got)
	}
}

// A dialect keyword that appears only inside a string literal must not trigger a
// warning — proves maskStringLiterals is applied before matching.
func TestLintPortability_StringLiteralMasked(t *testing.T) {
	sql := "INSERT INTO t (note) VALUES ('use ON CONFLICT or ILIKE or :: here')"
	if got := lintPortability(TargetMySQL, sql); got != nil {
		t.Errorf("keywords inside a string literal must not warn, got %v", got)
	}
}

func TestPortabilityWarnings_AccumulateAndCallback(t *testing.T) {
	var streamed []PortabilityWarning
	db := New(
		WithTarget(TargetPostgres),
		WithPortabilityWarner(func(w PortabilityWarning) { streamed = append(streamed, w) }),
	)
	if err := db.Exec("INSERT INTO users (id, name) VALUES (1, 'a')"); err != nil {
		t.Fatalf("insert: %v", err)
	}
	// A portable INSERT should not have produced any warning.
	if got := db.PortabilityWarnings(); len(got) != 0 {
		t.Fatalf("portable INSERT should not warn, got %v", got)
	}

	// A backtick-quoted identifier is non-portable to Postgres.
	if _, err := db.Query("SELECT `id` FROM users"); err != nil {
		t.Fatalf("query: %v", err)
	}
	warns := db.PortabilityWarnings()
	if len(warns) != 1 {
		t.Fatalf("expected 1 accumulated warning, got %d: %v", len(warns), warns)
	}
	if warns[0].Target != TargetPostgres || !strings.Contains(warns[0].Message, "backtick") {
		t.Errorf("unexpected warning: %+v", warns[0])
	}
	if len(streamed) != 1 || streamed[0].Message != warns[0].Message {
		t.Errorf("callback should have streamed the same warning, got %v", streamed)
	}

	db.ClearPortabilityWarnings()
	if got := db.PortabilityWarnings(); got != nil {
		t.Errorf("ClearPortabilityWarnings should empty the slice, got %v", got)
	}
}

func TestTargetGeneric_NoLintOnQuery(t *testing.T) {
	db := New() // TargetGeneric
	if err := db.Exec("INSERT INTO users (id) VALUES (1)"); err != nil {
		t.Fatalf("insert: %v", err)
	}
	if _, err := db.Query("SELECT `id` FROM users"); err != nil {
		t.Fatalf("query: %v", err)
	}
	if got := db.PortabilityWarnings(); got != nil {
		t.Errorf("default (generic) target must never warn, got %v", got)
	}
}

func TestGenerateDDL_UsesDeclaredTarget(t *testing.T) {
	db := New(WithTarget(TargetPostgres))
	if err := db.Exec("INSERT INTO users (id, name) VALUES (1, 'a')"); err != nil {
		t.Fatalf("insert: %v", err)
	}
	// Empty dialect falls back to the declared target (postgres → double quotes).
	ddl, err := db.GenerateDDL("")
	if err != nil {
		t.Fatalf("GenerateDDL(\"\"): %v", err)
	}
	if !strings.Contains(ddl, `CREATE TABLE "users"`) {
		t.Errorf("expected Postgres-quoted DDL, got:\n%s", ddl)
	}

	// Generic target + empty dialect is an error.
	dbGen := New()
	if err := dbGen.Exec("INSERT INTO users (id) VALUES (1)"); err != nil {
		t.Fatalf("insert: %v", err)
	}
	if _, err := dbGen.GenerateDDL(""); err == nil {
		t.Error("expected error for empty dialect with no declared target")
	}
}

func containsSub(msgs []string, sub string) bool {
	for _, m := range msgs {
		if strings.Contains(m, sub) {
			return true
		}
	}
	return false
}
