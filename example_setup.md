# Using vapordb in a DDD hexagonal service

This guide shows how to wire **vapordb** into a project that follows
Domain-Driven Design with a hexagonal (ports-and-adapters) architecture:

- samber/do dependency injection
- sqlx + `:name` parameter style (`BindNamed`)
- Repository interfaces over domain stores
- Unit-of-Work / transaction executor
- `pq.Array`, `FILTER (WHERE …)`, `ON CONFLICT … WHERE` (optimistic locking)
- Schema enforcement and how to handle schema changes

---

## 1. Project layout

```
myservice/
├── cmd/server/main.go
├── db/migrations/postgres/        # production .up.sql / .down.sql files
├── internal/
│   ├── app/                       # domain services — no DB awareness
│   │   └── order/
│   │       ├── service.go
│   │       └── service_test.go    # unit tests with mocks
│   ├── dependency/
│   │   └── db_wiring.go           # DI: build *sql.DB / *sqlx.DB
│   ├── domain/
│   │   └── order.go               # pure domain models
│   └── repository/
│       └── order/
│           ├── store.go           # concrete Postgres store
│           ├── queries.go         # SQL constants
│           └── store_test.go      # repository tests using vapordb
├── go.mod
└── go.sum
```

---

## 2. `go.mod`

```go
module github.com/yourorg/myservice

go 1.22

require (
    github.com/flyingraptor/vapordb v0.0.0
    github.com/flyingraptor/vapordb/driver v0.0.0  // same module, sub-package
    github.com/google/uuid v1.6.0
    github.com/jmoiron/sqlx v1.4.0
    github.com/lib/pq v1.12.0
    github.com/samber/do v1.6.0
    // ... your other deps
)
```

> **Note:** vapordb has zero runtime dependencies beyond the SQL parser. In
> production your `go.mod` will also have `lib/pq` or `pgx` for the real
> Postgres driver; vapordb is only used in tests.

---

## 3. Domain model

```go
// internal/domain/order.go
package domain

import (
    "time"
    "github.com/google/uuid"
)

// Order is the aggregate root. IDs are UUIDs; uuid.UUID implements
// encoding.TextMarshaler so vapordb and sqlx both serialise it to its
// canonical string form automatically — no manual .String() calls needed.
type Order struct {
    ID            uuid.UUID
    TenantID      uuid.UUID
    Reference     string
    Currency      string
    Status        string
    LastUpdatedAt time.Time // version field used for optimistic locking
}

type Item struct {
    ID       uuid.UUID
    OrderID  uuid.UUID
    Category string
    Quantity int64
}
```

---

## 4. Repository interface

Define the interface in your **app** or **domain** package so the application
service never depends on a concrete store:

```go
// internal/app/order/repository.go
package order

import (
    "context"

    "github.com/google/uuid"
    "github.com/yourorg/myservice/internal/domain"
)

type Repository interface {
    Save(ctx context.Context, o *domain.Order) error
    FindByIDs(ctx context.Context, tenantID uuid.UUID, ids []uuid.UUID) ([]*domain.Order, error)
    SummariseByCategory(ctx context.Context, tenantID, ownerID uuid.UUID, codes []string) ([]CategorySummary, error)
}

type CategorySummary struct {
    CategoryCode    string
    PendingAmount   int64
    ApprovedAmount  int64
    CompletedAmount int64
}
```

---

## 5. SQL queries

Put all SQL in a `queries.go` file. Use `:name` placeholders throughout — these
are understood natively by vapordb (`QueryNamed`/`ExecNamed`) **and** by sqlx
(`BindNamed`), so the same file works in both production and tests.

```go
// internal/repository/order/queries.go
package orderrepo

const queryOrderUpsert = `
    INSERT INTO orders (
        id, tenant_id, reference, currency, status, last_updated_at
    ) VALUES (
        :id, :tenant_id, :reference, :currency, :status, :last_updated_at
    )
    ON CONFLICT (id) DO UPDATE SET
        reference       = EXCLUDED.reference,
        currency        = EXCLUDED.currency,
        status          = EXCLUDED.status,
        last_updated_at = EXCLUDED.last_updated_at
    WHERE orders.last_updated_at = :version`

// ids is a pq.Array / []string; sqlx expands it via BindNamed
const queryOrderList = `
    SELECT id, tenant_id, reference, currency, status, last_updated_at
    FROM orders
    WHERE tenant_id  = :tenant_id
      AND id         = ANY(:ids)
      AND deleted_at IS NULL
    ORDER BY id`

const queryCategorySummary = `
    SELECT
        category_code,
        COALESCE(SUM(amount) FILTER (WHERE entry_type = 'PENDING'),   0) AS pending_amount,
        COALESCE(SUM(amount) FILTER (WHERE entry_type = 'APPROVED'),  0) AS approved_amount,
        COALESCE(SUM(amount) FILTER (WHERE entry_type = 'COMPLETED'), 0) AS completed_amount
    FROM events
    WHERE tenant_id    = :tenant_id
      AND owner_id     = :owner_id
      AND category_code = ANY(:category_codes)
    GROUP BY category_code
    ORDER BY category_code`
```

---

## 6. Concrete store (sqlx-style, works with both Postgres and vapordb)

The store holds a `*sqlx.DB`. In production it is the real Postgres connection;
in tests it is a vapordb-backed `*sql.DB` wrapped with `sqlx.NewDb`.

```go
// internal/repository/order/store.go
package orderrepo

import (
    "context"
    "fmt"
    "time"

    "github.com/google/uuid"
    "github.com/jmoiron/sqlx"
    "github.com/lib/pq"
    "github.com/yourorg/myservice/internal/app/order"
    "github.com/yourorg/myservice/internal/domain"
)

type Store struct {
    db *sqlx.DB
}

func NewStore(db *sqlx.DB) (*Store, error) {
    if db == nil {
        return nil, fmt.Errorf("order store: db is nil")
    }
    return &Store{db: db}, nil
}

// ── Save (upsert with optimistic locking) ─────────────────────────────────────

// orderUpsertParams is a db-tagged struct passed directly to BindNamed.
// uuid.UUID implements encoding.TextMarshaler, so sqlx / vapordb both
// serialise it to the canonical "xxxxxxxx-xxxx-…" string automatically.
type orderUpsertParams struct {
    ID            uuid.UUID `db:"id"`
    TenantID      uuid.UUID `db:"tenant_id"`
    Reference     string    `db:"reference"`
    Currency      string    `db:"currency"`
    Status        string    `db:"status"`
    LastUpdatedAt time.Time `db:"last_updated_at"`
    Version       time.Time `db:"version"` // must equal the current DB value
}

func (s *Store) Save(ctx context.Context, o *domain.Order) error {
    params := orderUpsertParams{
        ID:            o.ID,
        TenantID:      o.TenantID,
        Reference:     o.Reference,
        Currency:      o.Currency,
        Status:        o.Status,
        LastUpdatedAt: time.Now().UTC(),
        Version:       o.LastUpdatedAt, // optimistic lock
    }

    query, args, err := s.db.BindNamed(queryOrderUpsert, params)
    if err != nil {
        return fmt.Errorf("bind: %w", err)
    }
    _, err = s.db.ExecContext(ctx, query, args...)
    return err
}

// ── FindByIDs (pq.GenericArray for []uuid.UUID IN / ANY) ─────────────────────

// orderListParams uses pq.GenericArray to pass a []uuid.UUID as a Postgres
// array parameter.  Each element is serialised via uuid.UUID.Value()
// (driver.Valuer) for real Postgres, or expanded to 'uuid1', 'uuid2', …
// literals by vapordb's array expansion.
type orderListParams struct {
    TenantID uuid.UUID       `db:"tenant_id"`
    IDs      pq.GenericArray `db:"ids"` // wraps []uuid.UUID
}

// orderRow is the scan target for SELECT results; uuid.UUID implements
// sql.Scanner so it round-trips from the string column automatically.
type orderRow struct {
    ID            uuid.UUID `db:"id"`
    TenantID      uuid.UUID `db:"tenant_id"`
    Reference     string    `db:"reference"`
    Currency      string    `db:"currency"`
    Status        string    `db:"status"`
    LastUpdatedAt time.Time `db:"last_updated_at"`
}

func (s *Store) FindByIDs(ctx context.Context, tenantID uuid.UUID, ids []uuid.UUID) ([]*domain.Order, error) {
    params := orderListParams{
        TenantID: tenantID,
        IDs:      pq.GenericArray{A: ids},
    }

    query, args, err := s.db.BindNamed(queryOrderList, params)
    if err != nil {
        return nil, fmt.Errorf("bind: %w", err)
    }

    var rows []orderRow
    if err := s.db.SelectContext(ctx, &rows, query, args...); err != nil {
        return nil, err
    }

    out := make([]*domain.Order, len(rows))
    for i, r := range rows {
        out[i] = &domain.Order{
            ID:            r.ID,
            TenantID:      r.TenantID,
            Reference:     r.Reference,
            Currency:      r.Currency,
            Status:        r.Status,
            LastUpdatedAt: r.LastUpdatedAt,
        }
    }
    return out, nil
}

// ── SummariseByCategory (FILTER (WHERE …)) ────────────────────────────────────

type categorySummaryParams struct {
    TenantID      uuid.UUID       `db:"tenant_id"`
    OwnerID       uuid.UUID       `db:"owner_id"`
    CategoryCodes pq.GenericArray `db:"category_codes"` // wraps []string
}

// categorySummaryRow is the scan target for aggregation results.
type categorySummaryRow struct {
    CategoryCode    string `db:"category_code"`
    PendingAmount   int64  `db:"pending_amount"`
    ApprovedAmount  int64  `db:"approved_amount"`
    CompletedAmount int64  `db:"completed_amount"`
}

func (s *Store) SummariseByCategory(ctx context.Context, tenantID, ownerID uuid.UUID, codes []string) ([]order.CategorySummary, error) {
    params := categorySummaryParams{
        TenantID:      tenantID,
        OwnerID:       ownerID,
        CategoryCodes: pq.GenericArray{A: codes},
    }

    query, args, err := s.db.BindNamed(queryCategorySummary, params)
    if err != nil {
        return nil, fmt.Errorf("bind: %w", err)
    }

    var rows []categorySummaryRow
    if err := s.db.SelectContext(ctx, &rows, query, args...); err != nil {
        return nil, err
    }

    out := make([]order.CategorySummary, len(rows))
    for i, r := range rows {
        out[i] = order.CategorySummary{
            CategoryCode:    r.CategoryCode,
            PendingAmount:   r.PendingAmount,
            ApprovedAmount:  r.ApprovedAmount,
            CompletedAmount: r.CompletedAmount,
        }
    }
    return out, nil
}
```

### How `pq.GenericArray` and `uuid.UUID` work with vapordb

`pq.GenericArray{A: ids}` where `ids` is `[]uuid.UUID` serialises each element
via `uuid.UUID.Value()` (a `driver.Valuer`) producing a PostgreSQL array literal
string like `"{550e8400-…,123e4567-…}"`.

- With **real Postgres** the driver sends that as a native array parameter.
- With **vapordb** `driverValueToSQL` detects the `{…}` format and expands it
  to `'550e8400-…', '123e4567-…'` so `ANY(:ids)` (rewritten to `IN (:ids)`)
  becomes `IN ('550e8400-…', '123e4567-…')`.

On the **scan side**, `uuid.UUID` implements `sql.Scanner`, so sqlx populates
`orderRow.ID uuid.UUID` directly from the string column without any manual
`uuid.MustParse` call.

No changes to the store code are needed for either environment.

---

## 7. DI wiring — production vs test

### Production (`internal/dependency/db_wiring.go`)

```go
package dependency

import (
    "fmt"

    "github.com/jmoiron/sqlx"
    "github.com/samber/do"
    _ "github.com/lib/pq" // register Postgres driver
)

func BuildDB(i *do.Injector) (*sqlx.DB, error) {
    cfg := do.MustInvoke[Config](i)
    dsn := fmt.Sprintf(
        "host=%s port=%d dbname=%s user=%s password=%s sslmode=disable",
        cfg.DB.Host, cfg.DB.Port, cfg.DB.Name, cfg.DB.User, cfg.DB.Password,
    )
    db, err := sqlx.Open("postgres", dsn)
    if err != nil {
        return nil, fmt.Errorf("open db: %w", err)
    }
    return db, nil
}
```

### Tests — `TestMain` with vapordb

```go
// internal/repository/order/store_test.go
package orderrepo_test

import (
    "context"
    "database/sql"
    "os"
    "testing"
    "time"

    "github.com/google/uuid"
    "github.com/jmoiron/sqlx"

    vapordb "github.com/flyingraptor/vapordb"
    vapordriver "github.com/flyingraptor/vapordb/driver"
    "github.com/yourorg/myservice/internal/domain"
    orderrepo "github.com/yourorg/myservice/internal/repository/order"
)

// openTestDB returns a *sqlx.DB backed by a fresh vapordb instance.
// Passing "postgres" as the sqlx driver name makes db.BindNamed emit $N
// placeholders that vapordb also understands natively.
func openTestDB(t *testing.T) (*sqlx.DB, *vapordb.DB) {
    t.Helper()

    vdb := vapordb.New()
    name := t.Name()
    vapordriver.Register(name, vdb)
    t.Cleanup(func() { vapordriver.Unregister(name) })

    rawDB, err := sql.Open("vapordb", name)
    if err != nil {
        t.Fatalf("sql.Open: %v", err)
    }
    t.Cleanup(func() { rawDB.Close() })

    // Tell sqlx to use $N (DOLLAR) placeholders — matches the production
    // Postgres driver, so BindNamed behaves identically in tests.
    db := sqlx.NewDb(rawDB, "postgres")
    return db, vdb
}
```

> **Tip:** if you call `vapordriver.RegisterAs("pgx")` once in `TestMain`,
> you can use `sqlx.Open("pgx", name)` directly — no `sqlx.NewDb` call needed.
>
> ```go
> func TestMain(m *testing.M) {
>     vapordriver.RegisterAs("pgx") // once per test binary
>     os.Exit(m.Run())
> }
> ```

---

## 8. JSON persistence (optional)

vapordb is an in-memory database — **nothing is persisted automatically**. Every `Exec`, `Query`, and `ExecNamed` call only mutates the in-process state. If the process exits, all data is gone.

`db.Save` / `db.Load` are explicit, opt-in calls:

```go
// Snapshot the entire DB to disk — call this whenever you want a checkpoint.
if err := vdb.Save("data/db.json"); err != nil {
    log.Fatalf("save: %v", err)
}

// On next startup, restore from the last snapshot.
vdb := vapordb.New()
if err := vdb.Load("data/db.json"); err != nil && !os.IsNotExist(err) {
    log.Fatalf("load: %v", err)
}
```

**When to call `Save`:**

| Pattern | When to call |
|---|---|
| Seed data helper | After seeding, once, to snapshot the baseline |
| Integration test teardown | After a full test run to capture the final state for debugging |
| Development REPL / CLI tool | After every mutation batch so restarting doesn't lose work |
| Prototype app | In a background goroutine on a ticker (`time.Tick(30 * time.Second)`) |

A companion query-log file (`db_queries.jsonl`) is started automatically after the first `Save` or `Load` call. Every subsequent `Exec` and `Query` is appended to it in real time — useful for debugging what your code actually ran.

> **Note:** `Save`/`Load` are not a substitute for a real database. Once your data model stabilises, use `db.GenerateDDL("postgres")` to generate a `CREATE TABLE` script and migrate to Postgres.

---

## 9. Repository tests

```go
// Fixed UUIDs so assertions are readable; use uuid.New() for dynamic cases.
var (
    tenantID = uuid.MustParse("00000000-0000-0000-0000-000000000001")
    ownerID  = uuid.MustParse("00000000-0000-0000-0000-000000000002")
    orderID1 = uuid.MustParse("aaaaaaaa-0000-0000-0000-000000000001")
    orderID2 = uuid.MustParse("aaaaaaaa-0000-0000-0000-000000000002")
    orderID3 = uuid.MustParse("aaaaaaaa-0000-0000-0000-000000000003")
)

func TestStore_Save_OptimisticLock(t *testing.T) {
    db, _ := openTestDB(t)
    store, _ := orderrepo.NewStore(db)
    ctx := context.Background()

    // First save — no existing row, so "version" predicate doesn't filter anything.
    o := &domain.Order{
        ID:            orderID1,
        TenantID:      tenantID,
        Reference:     "REF-001",
        Currency:      "USD",
        Status:        "DRAFT",
        LastUpdatedAt: time.Time{}, // zero — no previous version
    }
    if err := store.Save(ctx, o); err != nil {
        t.Fatalf("initial save: %v", err)
    }

    // Read it back to get the persisted timestamp.
    results, _ := store.FindByIDs(ctx, tenantID, []uuid.UUID{orderID1})
    saved := results[0]

    // Valid update — version matches what's in the DB.
    saved.Status = "ACTIVE"
    saved.LastUpdatedAt = time.Now().UTC()
    if err := store.Save(ctx, saved); err != nil {
        t.Fatalf("update: %v", err)
    }

    // Stale update — wrong version; should be silently skipped.
    staleCopy := *saved
    staleCopy.Status = "CANCELLED"
    staleCopy.LastUpdatedAt = time.Now().Add(time.Second) // mis-matched version
    _ = store.Save(ctx, &staleCopy)

    // Status must still be ACTIVE — stale write was rejected.
    results, _ = store.FindByIDs(ctx, tenantID, []uuid.UUID{orderID1})
    if results[0].Status != "ACTIVE" {
        t.Errorf("expected ACTIVE, got %s", results[0].Status)
    }
}

func TestStore_SummariseByCategory_FILTER(t *testing.T) {
    db, vdb := openTestDB(t)
    store, _ := orderrepo.NewStore(db)
    ctx := context.Background()

    // Seed via vapordb's native ExecNamed — uuid.UUID is passed as-is;
    // vapordb calls TextMarshal internally to turn it into a string literal.
    for _, row := range []struct {
        code, entryType string
        amount          int64
    }{
        {"C001", "PENDING", 500}, {"C001", "APPROVED", 300},
        {"C002", "PENDING", 200}, {"C002", "COMPLETED", 700},
    } {
        _ = vdb.ExecNamed(
            `INSERT INTO events (tenant_id, owner_id, category_code, entry_type, amount)
             VALUES (:tenant_id, :owner_id, :category_code, :entry_type, :amount)`,
            map[string]any{
                "tenant_id":     tenantID, // uuid.UUID — works natively
                "owner_id":      ownerID,
                "category_code": row.code,
                "entry_type":    row.entryType,
                "amount":        row.amount,
            },
        )
    }

    summaries, err := store.SummariseByCategory(ctx, tenantID, ownerID, []string{"C001", "C002"})
    if err != nil {
        t.Fatalf("SummariseByCategory: %v", err)
    }
    if len(summaries) != 2 {
        t.Fatalf("want 2, got %d", len(summaries))
    }
    // C001
    s0 := summaries[0] // ordered by category_code
    if s0.PendingAmount != 500 || s0.ApprovedAmount != 300 {
        t.Errorf("C001: %+v", s0)
    }
    // C002
    s1 := summaries[1]
    if s1.PendingAmount != 200 || s1.ApprovedAmount != 0 {
        t.Errorf("C002: %+v", s1)
    }
}

func TestStore_FindByIDs_UUIDArray(t *testing.T) {
    db, vdb := openTestDB(t)
    store, _ := orderrepo.NewStore(db)
    ctx := context.Background()

    for _, id := range []uuid.UUID{orderID1, orderID2, orderID3} {
        _ = vdb.ExecNamed(
            `INSERT INTO orders (id, tenant_id, reference, currency, status, last_updated_at)
             VALUES (:id, :tenant_id, :reference, :currency, :status, :last_updated_at)`,
            map[string]any{
                "id":             id,       // uuid.UUID passed directly
                "tenant_id":      tenantID,
                "reference":      "REF-" + id.String(),
                "currency":       "USD",
                "status":         "DRAFT",
                "last_updated_at": time.Now(),
            },
        )
    }

    // Only fetch two of the three — UUID array expansion via pq.GenericArray.
    results, err := store.FindByIDs(ctx, tenantID, []uuid.UUID{orderID1, orderID3})
    if err != nil {
        t.Fatalf("FindByIDs: %v", err)
    }
    if len(results) != 2 {
        t.Fatalf("want 2, got %d", len(results))
    }
    // IDs round-trip correctly through the string column.
    if results[0].ID != orderID1 {
        t.Errorf("unexpected ID: %v", results[0].ID)
    }
}
```

---

## 10. Transactions (Unit of Work pattern)

vapordb provides `db.Begin()` / `tx.Commit()` / `tx.Rollback()` as a
snapshot-based transaction. Wrap the store to accept either `*vapordb.DB` or
`*vapordb.Tx` by having methods accept an executor interface:

```go
// Thin wrapper that accepts vapordb's native tx for multi-step operations.
// uuid.UUID fields in the param structs are serialised automatically.
func saveOrderWithItems(
    ctx context.Context,
    vdb *vapordb.DB,
    o *domain.Order,
    items []domain.Item,
) error {
    tx, err := vdb.Begin()
    if err != nil {
        return err
    }

    if err := tx.ExecNamed(insertOrderSQL, orderToParams(o)); err != nil {
        tx.Rollback()
        return err
    }
    for _, item := range items {
        if err := tx.ExecNamed(insertItemSQL, itemToParams(item)); err != nil {
            tx.Rollback()
            return err
        }
    }
    return tx.Commit()
}

// orderToParams converts the aggregate into a db-tagged struct.
// No manual o.ID.String() needed — vapordb calls TextMarshal itself.
func orderToParams(o *domain.Order) any {
    return struct {
        ID            uuid.UUID `db:"id"`
        TenantID      uuid.UUID `db:"tenant_id"`
        Reference     string    `db:"reference"`
        Currency      string    `db:"currency"`
        Status        string    `db:"status"`
        LastUpdatedAt time.Time `db:"last_updated_at"`
    }{o.ID, o.TenantID, o.Reference, o.Currency, o.Status, time.Now().UTC()}
}
```
```

When using the `database/sql` driver path, transactions work via the standard
`db.BeginTx` → `tx.ExecContext` → `tx.Commit/Rollback` API — no special
vapordb code needed.

---

## 11. Schema enforcement

### a. Enum constraints

Lock allowed values for status columns to catch typos early:

```go
func setupSchema(vdb *vapordb.DB) {
    vdb.DeclareEnum("orders", "status",
        "DRAFT", "ACTIVE", "CLOSED", "CANCELLED")
    vdb.DeclareEnum("events", "entry_type",
        "PENDING", "APPROVED", "COMPLETED")
}
```

After `DeclareEnum`, any INSERT or UPDATE that uses an unlisted value fails
with an error — same guardrail as a real `CHECK` constraint.

### b. Schema locking

After seeding the initial rows (which infers the schema from the first INSERT),
lock the schema so tests cannot accidentally add new columns:

```go
func TestMain(m *testing.M) {
    vapordriver.RegisterAs("pgx")
    os.Exit(m.Run())
}

func openTestDB(t *testing.T) (*sqlx.DB, *vapordb.DB) {
    // ... (as above) ...

    // Seed one row per table to infer the full schema, then lock it.
    seedSchema(vdb)
    vdb.LockSchema() // any subsequent INSERT with extra columns will error

    return db, vdb
}

func seedSchema(vdb *vapordb.DB) {
    zeroUUID := uuid.Nil // "00000000-0000-0000-0000-000000000000"

    // Each INSERT teaches vapordb the column types for that table.
    // Passing uuid.UUID (TextMarshaler) ensures the column is inferred as
    // KindString with the UUID text representation — matching production.
    _ = vdb.ExecNamed(`
        INSERT INTO orders (id, tenant_id, reference, currency, status, last_updated_at, deleted_at)
        VALUES (:id, :tenant_id, :reference, :currency, :status, :last_updated_at, :deleted_at)`,
        map[string]any{
            "id":             zeroUUID, // uuid.UUID → KindString
            "tenant_id":      zeroUUID,
            "reference":      "",
            "currency":       "",
            "status":         "DRAFT",
            "last_updated_at": time.Time{},
            "deleted_at":     nil,
        },
    )
    _ = vdb.ExecNamed(`
        INSERT INTO events (id, tenant_id, owner_id, category_code, entry_type, amount)
        VALUES (:id, :tenant_id, :owner_id, :category_code, :entry_type, :amount)`,
        map[string]any{
            "id":            zeroUUID,
            "tenant_id":     zeroUUID,
            "owner_id":      zeroUUID,
            "category_code": "",
            "entry_type":    "PENDING",
            "amount":        int64(0),
        },
    )
    // Delete seed rows — schema definition stays locked.
    _ = vdb.Exec(`DELETE FROM orders WHERE id = '` + zeroUUID.String() + `'`)
    _ = vdb.Exec(`DELETE FROM events WHERE id = '` + zeroUUID.String() + `'`)
}
```

### c. Enforcing column type changes

vapordb infers column types from the first INSERT. If production adds a new
column with a type incompatible with its first inferred value, vapordb will
return an error (default) or wipe the column (opt-in legacy mode).

**Strategy when you add a new column to production:**

1. Add the column to your `seedSchema` function.
2. Update the relevant SQL constants (`queryOrderUpsert`, etc.).
3. Run tests — if the new column is the wrong type from an old seeded row,
   the test will fail immediately, telling you to fix `seedSchema`.

There is no migration runner for vapordb (it is not a persistent store), but
**`db.GenerateDDL("postgres")`** can output the schema vapordb has inferred,
which you can diff against your migration files:

```go
func TestSchemaDDL(t *testing.T) {
    _, vdb := openTestDB(t)
    seedSchema(vdb)

    got := vdb.GenerateDDL("postgres")

    // Optionally compare against a golden file:
    // golden, _ := os.ReadFile("testdata/schema.sql")
    // if got != string(golden) { t.Errorf("schema drift:\n%s", got) }

    t.Log(got) // useful during development
}
```

### d. Widening a column type

If you widen a column from `INT` to `FLOAT` (or from string to `time.Time`) in
production, update `seedSchema` to INSERT the new wider type:

```go
// Before: last_updated_at was inserted as string → KindString inferred
// After:  insert as time.Time → KindDate inferred (correct)
"last_updated_at": time.Now(), // ← was: "2024-01-01"
```

Unsafe type transitions (e.g. `KindString` → `KindDate`) default to an error.
To allow the wipe during a transition period:

```go
vdb = vapordb.New(vapordb.WithForceWipeOnSchemaConflict(true))
```

Remove this option once your seed row uses the correct types.

---

## 12. Partial `ON CONFLICT` (partial-index conflicts)

A common pattern for soft-deletable join tables is a partial unique index:

```sql
-- production migration
CREATE UNIQUE INDEX uc_order_tags_active
    ON order_tags (order_id, tag_id)
    WHERE deleted_at IS NULL;
```

In the SQL query file:

```go
const queryOrderTagUpsert = `
    INSERT INTO order_tags (order_id, tag_id, last_updated_at)
    VALUES (:order_id, :tag_id, :last_updated_at)
    ON CONFLICT (order_id, tag_id) WHERE deleted_at IS NULL
    DO UPDATE SET last_updated_at = EXCLUDED.last_updated_at`
```

vapordb strips the `WHERE deleted_at IS NULL` predicate from the conflict
target (since it uses value-equality, not real indexes) and still detects a
conflict on `(order_id, tag_id)`. The test behaviour matches production for
all non-soft-delete cases.

---

## 13. Quick-reference: vapordb vs Postgres driver feature map

| Feature | vapordb | Postgres |
|---|---|---|
| `:name` parameters | native (`QueryNamed`/`ExecNamed`) | via sqlx `BindNamed` |
| `?` parameters | via `database/sql` driver | N/A |
| `$N` parameters | via `database/sql` driver | native |
| `pq.Array(slice)` | ✅ expanded to `IN (…)` literals | ✅ native array |
| Plain `[]T` slice args | ✅ `CheckNamedValue` converts | ❌ must wrap in `pq.Array` |
| `= ANY(:param)` | ✅ rewritten to `IN (:param)` | ✅ native |
| `FILTER (WHERE …)` | ✅ rewritten to `CASE WHEN` | ✅ native |
| `array_agg(col)` | ✅ returns `KindJSON` array | ✅ native |
| `ON CONFLICT … WHERE pred` (partial) | ✅ pred stripped, conflict by value-eq | ✅ uses real index |
| `ON CONFLICT … WHERE cond` (optimistic lock) | ✅ cond evaluated against existing row | ✅ native |
| Transactions | ✅ snapshot copy-on-write | ✅ ACID |
| `LockSchema()` | ✅ rejects unexpected columns | use migration tooling |
| `DeclareEnum()` | ✅ value-set constraint | `CHECK` / `ENUM` type |
| `GenerateDDL("postgres")` | ✅ emits `CREATE TABLE` DDL | migration files |
| Persistence | ❌ in-memory only | ✅ |
| Indexes / foreign keys | ❌ full table scan | ✅ |

---

## 14. Recommended test setup summary

```go
// internal/testutil/db.go  (shared across all test packages)
package testutil

import (
    "database/sql"
    "testing"
    "time"

    "github.com/google/uuid"
    "github.com/jmoiron/sqlx"
    vapordb "github.com/flyingraptor/vapordb"
    vapordriver "github.com/flyingraptor/vapordb/driver"
)

func init() {
    // Register once per binary so sqlx.NewDb(db, "postgres") isn't needed.
    vapordriver.RegisterAs("pgx")
}

// OpenDB returns a fresh vapordb-backed *sqlx.DB for a single test.
// The caller receives the raw *vapordb.DB for seeding via ExecNamed.
func OpenDB(t *testing.T) (*sqlx.DB, *vapordb.DB) {
    t.Helper()

    vdb := vapordb.New()
    vapordriver.Register(t.Name(), vdb)
    t.Cleanup(func() { vapordriver.Unregister(t.Name()) })

    rawDB, err := sql.Open("pgx", t.Name()) // "pgx" → DOLLAR bind style
    if err != nil {
        t.Fatalf("sql.Open: %v", err)
    }
    t.Cleanup(func() { rawDB.Close() })

    // Wrap with sqlx — it sees driver name "pgx" so BindNamed uses $N.
    return sqlx.NewDb(rawDB, "pgx"), vdb
}

// SeedRow inserts a single row via ExecNamed (infers schema on first call).
func SeedRow(t *testing.T, vdb *vapordb.DB, table string, params map[string]any) {
    t.Helper()

    cols, vals := "", ""
    args := map[string]any{}
    for k, v := range params {
        if cols != "" {
            cols += ", "
            vals += ", "
        }
        cols += k
        vals += ":" + k
        args[k] = v
    }
    q := "INSERT INTO " + table + " (" + cols + ") VALUES (" + vals + ")"
    if err := vdb.ExecNamed(q, args); err != nil {
        t.Fatalf("SeedRow %s: %v", table, err)
    }
}

// LockAfterSeed locks the schema so tests fail fast on unexpected column additions.
func LockAfterSeed(vdb *vapordb.DB) {
    vdb.LockSchema()
}

// SetupEnums declares CHECK-constraint-equivalent enum rules.
func SetupEnums(vdb *vapordb.DB) {
    vdb.DeclareEnum("orders", "status",       "DRAFT", "ACTIVE", "CLOSED", "CANCELLED")
    vdb.DeclareEnum("events", "entry_type",   "PENDING", "APPROVED", "COMPLETED")
}
```

Use in each test:

```go
func TestMyRepositoryMethod(t *testing.T) {
    db, vdb := testutil.OpenDB(t)
    testutil.SetupEnums(vdb)
    store, _ := orderrepo.NewStore(db)

    // uuid.UUID passed directly — vapordb calls TextMarshal automatically.
    testutil.SeedRow(t, vdb, "orders", map[string]any{
        "id":             uuid.New(),
        "tenant_id":      uuid.MustParse("00000000-0000-0000-0000-000000000001"),
        "status":         "DRAFT",
        "last_updated_at": time.Now(),
    })
    testutil.LockAfterSeed(vdb)

    // … test store methods …
}
```
