package verify

import (
	"context"
	"database/sql"
	"log"
	"os"
	"testing"
	"time"

	_ "github.com/go-sql-driver/mysql"
	_ "github.com/lib/pq"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/mysql"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"
)

// Pinned database images. Bump these deliberately; the verify job in CI runs
// against exactly these versions.
const (
	postgresImage = "postgres:16-alpine"
	mysqlImage    = "mysql:8.0"
)

// Package-level handles shared across all tests. One container per database is
// started once in TestMain and reused for speed. A handle is nil when its
// container could not be started (e.g. Docker unavailable); tests skip in that
// case rather than fail.
var (
	pgDB *sql.DB
	myDB *sql.DB
)

// TestMain boots one Postgres and one MySQL container for the whole test run,
// then tears them down. Startup failures are logged (not fatal) so the suite
// degrades to skips on machines without Docker.
func TestMain(m *testing.M) {
	ctx := context.Background()
	var cleanups []func()

	// VERIFY_SKIP_CONTAINERS=1 skips all container startup so the Docker-free
	// tests (e.g. TestVapordbAcceptsSharedCorpus) run instantly; the
	// container-backed assertions then skip.
	if os.Getenv("VERIFY_SKIP_CONTAINERS") == "1" {
		os.Exit(m.Run())
	}

	if db, cleanup, err := startPostgres(ctx); err != nil {
		log.Printf("verify: skipping Postgres tests, container unavailable: %v", err)
	} else {
		pgDB = db
		cleanups = append(cleanups, cleanup)
	}

	if db, cleanup, err := startMySQL(ctx); err != nil {
		log.Printf("verify: skipping MySQL tests, container unavailable: %v", err)
	} else {
		myDB = db
		cleanups = append(cleanups, cleanup)
	}

	code := m.Run()

	for _, cleanup := range cleanups {
		cleanup()
	}
	os.Exit(code)
}

// startPostgres launches a Postgres container and returns a ready *sql.DB
// (lib/pq) plus a cleanup func that closes the pool and terminates the
// container.
func startPostgres(ctx context.Context) (*sql.DB, func(), error) {
	ctr, err := postgres.Run(ctx, postgresImage,
		postgres.WithDatabase("vapordb_verify"),
		postgres.WithUsername("verify"),
		postgres.WithPassword("verify"),
		testcontainers.WithWaitStrategy(
			wait.ForLog("database system is ready to accept connections").
				WithOccurrence(2).
				WithStartupTimeout(120*time.Second),
		),
	)
	if err != nil {
		return nil, nil, err
	}
	dsn, err := ctr.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		_ = testcontainers.TerminateContainer(ctr)
		return nil, nil, err
	}
	db, err := openAndPing(ctx, "postgres", dsn)
	if err != nil {
		_ = testcontainers.TerminateContainer(ctr)
		return nil, nil, err
	}
	cleanup := func() {
		_ = db.Close()
		_ = testcontainers.TerminateContainer(ctr)
	}
	return db, cleanup, nil
}

// startMySQL launches a MySQL 8 container and returns a ready *sql.DB
// (go-sql-driver/mysql) plus a cleanup func.
func startMySQL(ctx context.Context) (*sql.DB, func(), error) {
	ctr, err := mysql.Run(ctx, mysqlImage,
		mysql.WithDatabase("vapordb_verify"),
		mysql.WithUsername("verify"),
		mysql.WithPassword("verify"),
	)
	if err != nil {
		return nil, nil, err
	}
	dsn, err := ctr.ConnectionString(ctx, "parseTime=true")
	if err != nil {
		_ = testcontainers.TerminateContainer(ctr)
		return nil, nil, err
	}
	db, err := openAndPing(ctx, "mysql", dsn)
	if err != nil {
		_ = testcontainers.TerminateContainer(ctr)
		return nil, nil, err
	}
	cleanup := func() {
		_ = db.Close()
		_ = testcontainers.TerminateContainer(ctr)
	}
	return db, cleanup, nil
}

// openAndPing opens a database/sql pool and pings it with a short retry loop so
// transient "not ready yet" errors immediately after container start do not
// fail the suite.
func openAndPing(ctx context.Context, driver, dsn string) (*sql.DB, error) {
	db, err := sql.Open(driver, dsn)
	if err != nil {
		return nil, err
	}
	deadline := time.Now().Add(60 * time.Second)
	for {
		pingCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
		err = db.PingContext(pingCtx)
		cancel()
		if err == nil {
			return db, nil
		}
		if time.Now().After(deadline) {
			_ = db.Close()
			return nil, err
		}
		time.Sleep(500 * time.Millisecond)
	}
}
