# vapordb/verify

Internal, Docker-backed validation that vapordb's `GenerateDDL` output and a
curated corpus of the SQL vapordb accepts **actually run on real PostgreSQL and
MySQL**.

This is **portability-level** validation only — it proves "does it run", not
semantic result parity. It exists to catch cases where vapordb emits DDL or
accepts SQL that a real target database would reject.

## Why a separate module

vapordb's core module has **no dependencies beyond the SQL parser**. This
validation needs testcontainers and real database drivers, which are heavy. To
keep the core clean, `verify/` is its own Go module
(`github.com/flyingraptor/vapordb/verify`) that depends on the core via a local
`replace` directive. Nothing here can ever touch the core `go.mod` / `go.sum`,
and `go test ./...` in the core module never sees these tests (a nested module is
automatically excluded from the parent's package pattern).

## What it checks

- **Schema round-trip** — builds an in-memory `vapordb.DB` whose inferred schema
  exercises every Value `Kind` (int, float, bool, string, date, JSON) plus an
  enum-constrained column, across three joinable tables (`orders` → `users` →
  `regions`). It runs `db.GenerateDDL("postgres")` / `db.GenerateDDL("mysql")`
  and asserts the real database accepts the DDL.
- **Shared corpus replay** — replays a rich corpus of SQL that vapordb accepts
  **and** that is portable to both engines, and asserts each statement runs.
  Coverage includes:
  - joins: `INNER` / `LEFT` / `RIGHT`, and a three-table chain;
  - aggregates with `GROUP BY` / `HAVING`, `DISTINCT`, `COUNT(DISTINCT …)`;
  - `CASE`, `COALESCE` / `NULLIF`, `BETWEEN` / `IN` / `LIKE`, string/math funcs;
  - scalar + correlated subqueries, `EXISTS` / `NOT EXISTS`, `IN` / `NOT IN (subquery)`;
  - derived tables, single and chained CTEs, `UNION` / `UNION ALL`;
  - window functions: ranking (`ROW_NUMBER`/`RANK`/`DENSE_RANK`), offset
    (`LAG`/`LEAD`), aggregate `OVER (…)` with an explicit `ROWS` frame, and
    whole-partition aggregates.
- **Per-dialect corpus** — features specific to one engine, run only against the
  matching database:
  - Postgres: `FULL OUTER JOIN`, `ILIKE`, `RETURNING`, `::` cast, JSONB `->>` /
    `@>`, `EXTRACT`, `NULLS LAST`;
  - MySQL: backtick identifiers, `IFNULL`, JSON path `->>` / `JSON_EXTRACT` /
    `JSON_CONTAINS`, `DATE_FORMAT`, `YEAR()`, `LIMIT offset, count`.
- **vapordb acceptance (Docker-free)** — `TestVapordbAcceptsSharedCorpus`
  asserts the shared corpus is genuinely SQL vapordb parses and executes, not
  just SQL the real databases happen to run. Together with the replay this is
  the full cross-check: vapordb accepts each statement **and** both real engines
  do too.
- **Non-portable cross-check** — feeds a statement that vapordb's
  `WithTarget(...)` portability lint flags (backticks for Postgres,
  `ON CONFLICT` for MySQL) and asserts the real database rejects it too — the
  lint and reality agree.

The two targets share one table-driven runner (see `verify_test.go`). Execution
order per target is: DDL → seed → shared reads → dialect corpus → shared writes.

> The seed INSERTs differ in exactly two columns between vapordb and the real
> engines: vapordb needs `json_parse(…)` / `DATE(…)` wrappers to infer the JSON
> and date columns on INSERT, whereas Postgres/MySQL coerce plain string
> literals into their JSON / timestamp columns (and have no such functions).
> Everything else — every read and write — is genuinely shared.

## Known portability gap surfaced

`GenerateDDL` emits column types but **no `PRIMARY KEY` / `UNIQUE` constraints**.
Because of that, a real `INSERT … ON CONFLICT (col) …` upsert against the
generated Postgres schema fails ("no unique or exclusion constraint matching the
ON CONFLICT specification"), even though vapordb supports the syntax. Positive
upsert replay is therefore intentionally omitted here; after migrating you must
add the unique constraint the upsert targets.

## Running

Requires **Docker** to be running.

```bash
cd verify
go test ./...
```

Containers are started once per test run (in `TestMain`) and reused for speed.
If Docker is unavailable the container-backed assertions **skip** (they do not
fail); the lint half of the non-portable cross-check still runs.

To run only the Docker-free checks (e.g. the vapordb acceptance test) without
starting any container:

```bash
VERIFY_SKIP_CONTAINERS=1 go test -run TestVapordbAcceptsSharedCorpus ./...
```

## Pinned versions

| Database   | Image                |
|------------|----------------------|
| PostgreSQL | `postgres:16-alpine` |
| MySQL      | `mysql:8.0`          |

Bump these in `harness_test.go` (`postgresImage` / `mysqlImage`) deliberately.
