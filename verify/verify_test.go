package verify

import (
	"context"
	"database/sql"
	"strings"
	"testing"
	"time"

	"github.com/flyingraptor/vapordb"
)

// dialectCase binds a real database (started in TestMain) to the vapordb dialect
// name and target used to generate DDL and to lint portability for it.
type dialectCase struct {
	name       string         // subtest name
	ddlDialect string         // argument to GenerateDDL
	target     vapordb.Target // WithTarget for the portability lint cross-check
	db         func() *sql.DB // container handle (nil ⇒ skip)
	extra      func() []string
	// nonPortable is a statement the real database must reject AND the vapordb
	// lint for target must flag.
	nonPortable string
}

func dialectCases() []dialectCase {
	return []dialectCase{
		{
			name:        "postgres",
			ddlDialect:  "postgres",
			target:      vapordb.TargetPostgres,
			db:          func() *sql.DB { return pgDB },
			extra:       postgresCorpus,
			nonPortable: "SELECT `id` FROM `users`", // backticks are MySQL-only
		},
		{
			name:        "mysql",
			ddlDialect:  "mysql",
			target:      vapordb.TargetMySQL,
			db:          func() *sql.DB { return myDB },
			extra:       mysqlCorpus,
			nonPortable: "INSERT INTO users (id) VALUES (1) ON CONFLICT (id) DO NOTHING", // ON CONFLICT is Postgres-only
		},
	}
}

// TestGeneratedDDLAndCorpusRun proves, per target, that:
//  1. GenerateDDL(dialect) produces a schema the real database accepts,
//  2. a rich shared corpus (joins, aggregates, subqueries, CTEs, UNION, window
//     functions, …) executes there without error, and
//  3. the dialect-specific corpus (JSON operators, FULL OUTER JOIN, ILIKE,
//     RETURNING, DATE_FORMAT, backticks, …) executes on the matching database.
//
// Execution order: DDL → seed → shared reads → dialect corpus → shared writes.
func TestGeneratedDDLAndCorpusRun(t *testing.T) {
	for _, tc := range dialectCases() {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			db := tc.db()
			if db == nil {
				t.Skipf("%s container not available (Docker required)", tc.name)
			}

			ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
			defer cancel()

			// Isolate this run from any prior one by dropping the tables first.
			dropTables(ctx, t, db, tc.ddlDialect, tableNames()...)

			corpusDB, err := buildCorpusDB()
			if err != nil {
				t.Fatalf("build corpus db: %v", err)
			}
			ddl, err := corpusDB.GenerateDDL(tc.ddlDialect)
			if err != nil {
				t.Fatalf("GenerateDDL(%q): %v", tc.ddlDialect, err)
			}

			runAll(ctx, t, db, tc.name, "DDL", splitStatements(ddl))
			runAll(ctx, t, db, tc.name, "seed", sharedSeed())
			runAll(ctx, t, db, tc.name, "shared read", sharedReads())
			runAll(ctx, t, db, tc.name, tc.name+" corpus", tc.extra())
			runAll(ctx, t, db, tc.name, "shared write", sharedWrites())
		})
	}
}

// TestVapordbAcceptsSharedCorpus is a fast, Docker-free guarantee that the
// shared corpus is genuinely SQL vapordb accepts (not just SQL the real
// databases happen to run). Together with TestGeneratedDDLAndCorpusRun this
// forms the full cross-check: vapordb parses/executes each statement AND both
// real engines do too.
func TestVapordbAcceptsSharedCorpus(t *testing.T) {
	db, err := buildCorpusDB()
	if err != nil {
		t.Fatalf("build corpus db: %v", err)
	}
	for _, stmt := range vapordbSeed() {
		if err := db.Exec(stmt); err != nil {
			t.Fatalf("vapordb rejected seed statement:\n%s\nerror: %v", stmt, err)
		}
	}
	for _, stmt := range sharedReads() {
		if _, err := db.Query(stmt); err != nil {
			t.Fatalf("vapordb rejected read statement:\n%s\nerror: %v", stmt, err)
		}
	}
	for _, stmt := range sharedWrites() {
		if err := db.Exec(stmt); err != nil {
			t.Fatalf("vapordb rejected write statement:\n%s\nerror: %v", stmt, err)
		}
	}
}

// TestNonPortableRejected proves, per target, that a statement flagged as
// non-portable by vapordb's WithTarget lint is genuinely rejected by the real
// database — the lint and reality agree.
func TestNonPortableRejected(t *testing.T) {
	for _, tc := range dialectCases() {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			// 1. vapordb's lint flags the statement for this target.
			lintDB := vapordb.New(vapordb.WithTarget(tc.target))
			// The lint runs before execution; ignore any execution error (the
			// table may not exist) — we only care about the recorded warnings.
			if strings.HasPrefix(strings.ToUpper(strings.TrimSpace(tc.nonPortable)), "SELECT") {
				_, _ = lintDB.Query(tc.nonPortable)
			} else {
				_ = lintDB.Exec(tc.nonPortable)
			}
			if warns := lintDB.PortabilityWarnings(); len(warns) == 0 {
				t.Fatalf("expected vapordb WithTarget(%s) to flag %q as non-portable, got no warnings",
					tc.target, tc.nonPortable)
			}

			// 2. The real database rejects the same statement.
			db := tc.db()
			if db == nil {
				t.Skipf("%s container not available (Docker required)", tc.name)
			}
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()
			if _, err := db.ExecContext(ctx, tc.nonPortable); err == nil {
				t.Fatalf("expected %s to reject non-portable statement %q, but it succeeded",
					tc.name, tc.nonPortable)
			}
		})
	}
}

// runAll executes every statement against the real database, failing with the
// offending statement on the first error.
func runAll(ctx context.Context, t *testing.T, db *sql.DB, dialect, phase string, stmts []string) {
	t.Helper()
	for i, stmt := range stmts {
		if _, err := db.ExecContext(ctx, stmt); err != nil {
			t.Fatalf("[%s] %s statement %d rejected:\n%s\nerror: %v", dialect, phase, i, stmt, err)
		}
	}
}

// splitStatements breaks a multi-statement DDL script into individual
// statements. vapordb's GenerateDDL separates each CREATE TABLE with ';' and a
// newline; identifiers and enum values it emits never contain a semicolon, so a
// plain split is safe here.
func splitStatements(script string) []string {
	parts := strings.Split(script, ";")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		if s := strings.TrimSpace(p); s != "" {
			out = append(out, s)
		}
	}
	return out
}

// dropTables removes the given tables if present, so each dialect run starts
// from a clean schema regardless of prior runs against a reused container.
func dropTables(ctx context.Context, t *testing.T, db *sql.DB, dialect string, names ...string) {
	t.Helper()
	for _, name := range names {
		ident := quote(name, dialect)
		if _, err := db.ExecContext(ctx, "DROP TABLE IF EXISTS "+ident); err != nil {
			t.Fatalf("drop table %s: %v", name, err)
		}
	}
}

func quote(name, dialect string) string {
	if dialect == "mysql" {
		return "`" + name + "`"
	}
	return `"` + name + `"`
}
