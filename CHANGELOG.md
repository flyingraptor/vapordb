# vapordb — Changelog

Release history and completed roadmap. See the [README](README.md) for basics and [FEATURES.md](FEATURES.md) for the current feature set.

## Completed

- Named parameters (`db.QueryNamed` / `db.ExecNamed`) ✓
- `fmt.Stringer` / pointer support in struct mapping ✓
- `driver.Valuer` / `sql.Scanner` support ✓
- `ON CONFLICT … DO UPDATE SET` (UPSERT) ✓
- `= ANY(…)` / `<> ALL(…)` array operators ✓
- `SELECT EXISTS (subquery)` ✓
- Subqueries in FROM, UNION / UNION ALL, CTEs ✓
- Window functions with ROWS / RANGE frames, LAG / LEAD / FIRST_VALUE / LAST_VALUE / NTH_VALUE / NTILE / CUME_DIST / PERCENT_RANK ✓
- `FULL OUTER JOIN` / `RIGHT JOIN` ✓
- `IN (subquery)` / `NOT IN (subquery)` — correlated and uncorrelated ✓
- Scalar subqueries — `(SELECT col FROM …)` anywhere a value is expected ✓
- **Transactions** — `db.Begin()` / `tx.Commit()` / `tx.Rollback()` ✓
- **`database/sql` driver** — `github.com/flyingraptor/vapordb/driver` ✓
- **`GenerateDDL(dialect)`** — emit `CREATE TABLE` DDL for MySQL or Postgres ✓
- **`KindJSON`** — first-class JSON type with `json_extract`, `json_unquote`, `json_contains`, `->`, `->>`, `@>`, `<@` ✓
- **`FILTER (WHERE …)` on aggregates** — PostgreSQL-style conditional aggregation ✓
- **`array_agg(col)`** — collect values into a JSON array ✓
- **Partial `ON CONFLICT` target** — `ON CONFLICT (cols) WHERE predicate DO UPDATE SET …` ✓
- **Optimistic locking on upsert** — `ON CONFLICT … DO UPDATE SET … WHERE condition` ✓
- **`pq.Array` / plain-slice parameter binding** — expanded to `IN (…)` literals in both native and `database/sql` driver paths ✓
- **`driver.RegisterAs`** — alias the driver under any SQL driver name for sqlx bind-style auto-detection ✓

- **Persistence round-trip verified** — `Save` / `Load` confirmed correct for every `Kind` (Null, Bool, Int, Float, String, Date, JSON), `EnumSets`, and `Locked` flag. See `persist_test.go`. ✓
- **`DATETIME(expr)` / `TIMESTAMP(expr)` function** — preserves the full time-of-day component when parsing or casting a value to a datetime (complements the existing `DATE(expr)` which truncates to midnight) ✓
- **`time.Time` named-parameter fix** — `ExecNamed` / `ExecStruct` now emit `DATETIME('…')` for times with a non-zero time-of-day component; date-only values continue to use `DATE('…')` ✓

- **`IN (subquery)` full pipeline** — `IN` / `NOT IN` subqueries now support the complete SELECT pipeline inside: `GROUP BY`, `HAVING`, `ORDER BY`, `LIMIT`, `DISTINCT`, and `UNION` / `UNION ALL`. Correlated references to the outer row work in all of these. ✓
- **Value-based `RANGE` frames** — `RANGE BETWEEN N PRECEDING AND N FOLLOWING` (and all mixed combinations with `UNBOUNDED`) compares the numeric ORDER BY column value of each row rather than row offsets. Works with `int64`, `float64`, and `DESC` ordering. ✓
- **`HAVING` aggregates not in `SELECT`** — `HAVING COUNT(*) > 1` now works even when `COUNT(*)` does not appear in the `SELECT` list, for both the main pipeline and all subquery paths. ✓
- **Double-quoted identifier support** — `"name"`, `"type"`, `"status"` and all other standard-SQL / PostgreSQL double-quoted identifiers are transparently rewritten to MySQL backtick identifiers before parsing. ✓
- **`ILIKE` / `NOT ILIKE`** — case-insensitive `LIKE`; rewritten to `LOWER(x) LIKE LOWER(y)` before parsing. ✓
- **Streaming persistence** — `db.SaveTo(io.Writer)` / `db.LoadFrom(io.Reader)` persist to and restore from any stream (gzip, HTTP body, `bytes.Buffer`, embedded `fs.FS`), complementing the file-based `Save` / `Load`. ✓
- **`INSERT … SELECT`** — populate a table from a query in one statement. Source columns map onto the target list positionally and the full SELECT pipeline (`WHERE` / `JOIN` incl. `FULL OUTER JOIN` / `GROUP BY` / `DISTINCT` / `ORDER BY` / `LIMIT` / derived tables / `UNION` / correlated + scalar subqueries) is available. Composes with `ON CONFLICT …`, `RETURNING`, named parameters, and CTEs (`WITH … INSERT … SELECT`). Window functions in the projection are the one unsupported case — compute them in a CTE first. (This work also fixed a latent panic where any `WITH … <DML>` through `db.Exec` unlocked the wrong mutex and deadlocked the database.) ✓
- **Hash join for equi-joins** — `col = col` joins (single or `AND`-chained) build a hash table on the right input and probe it from the left, so a join of two N-row tables is `O(N)` instead of the previous `O(N²)` nested-loop scan. Applies to every link in a multi-table join chain, and all join types (`INNER` / `LEFT` / `RIGHT` / `FULL OUTER`). Mixed `ON` conditions (equi term + extra filter such as `… AND b.deleted_at IS NULL`) are split into a hash key plus a residual predicate applied to matched candidates, staying `O(N)` while preserving outer-join null-padding. Purely non-equi conditions, cross joins, and key types that need value coercion (dates, JSON, mixed numeric/string families) transparently fall back to the nested loop, so results are identical. ✓
- **Conflict-key index for upserts** — `ON CONFLICT` conflict detection uses a per-table hash index on the conflict-target columns (`conflict-key → row`) instead of scanning the whole table for every row, so a bulk upsert import is `O(N)` instead of `O(N²)`. The index is maintained incrementally on insert and invalidated precisely on `UPDATE` / `DELETE` / schema wipe (and only for overlapping columns after an `ON CONFLICT DO UPDATE`). Key encoding matches the engine's value equality (`NULL` matches `NULL`; `int 1` ≠ `float 1.0`); `DATE` / `JSON` keys fall back to the linear scan, so results are unchanged. ✓
- **`WithTarget` portability lint** — declare the database you intend to migrate to (`WithTarget(TargetPostgres|TargetMySQL)`) and every `Query` / `Exec` is checked for non-portable SQL; issues are recorded as `PortabilityWarning`s (retrieve via `db.PortabilityWarnings()` or stream via `WithPortabilityWarner`). Warn-only — execution and results are unchanged. ✓
- **Docker-backed portability validation (`verify/` nested module)** — an isolated Go module (`github.com/flyingraptor/vapordb/verify`) uses testcontainers to prove that `GenerateDDL` output and a corpus of vapordb-accepted SQL actually run on real PostgreSQL 16 and MySQL 8, and that statements the `WithTarget` lint flags are genuinely rejected by the real database. All heavy dependencies live in that nested module, so the core `go.mod` stays dependency-free. ✓

## Releases

### 2026-07-03

**Added**

- **Declare a target database (`WithTarget`)** — a prototype in vapordb is usually thrown away and re-created in a real database once the model stabilises. You can now declare that target up front so vapordb keeps you honest along the way:

  ```go
  db := vapordb.New(vapordb.WithTarget(vapordb.TargetPostgres))
  ```

  With a target set (other than the default `TargetGeneric`), every `Query` / `Exec` (and the named variants) is checked for SQL that would **not** port cleanly to that database, and each issue is recorded as a `PortabilityWarning` (retrievable via `db.PortabilityWarnings()`, or streamed live through `WithPortabilityWarner`). Examples: a `TargetPostgres` database warns on backtick identifiers, `ON DUPLICATE KEY UPDATE`, `IFNULL()`, `RLIKE`/`REGEXP`, and `LIMIT offset, count`; a `TargetMySQL` database warns on double-quoted identifiers, `ON CONFLICT`, `ILIKE`, `::` casts, `@>`/`<@`, `= ANY`/`<> ALL`, `NULLS FIRST/LAST`, and `RETURNING`. Keywords inside string literals never trigger a warning.

  As a convenience, `db.GenerateDDL("")` (empty dialect) now uses the declared target's dialect. This is a **warn-only lint** — it never changes execution or results, and the permissive prototyping core (schema inference, type widening) is untouched. The default remains `TargetGeneric`, so existing code is unaffected.

**Performance**

- **Conflict-key index for upserts** — `ON CONFLICT` conflict detection previously scanned the entire table for every incoming row (`findConflict` compared each existing row against the new one), making a bulk upsert import `O(N²)`. Conflict detection now uses a per-table hash index keyed on the conflict-target columns (`conflict-key → first matching row`), so each lookup is `O(1)` and an import is `O(N)`.

  The index is maintained incrementally as rows are appended and invalidated precisely by other mutations: dropped wholesale on `UPDATE`, `DELETE`, and forced schema wipe, and — after an `ON CONFLICT DO UPDATE` — dropped only for the indexes whose columns the update actually touched (so updating a non-key column leaves the index intact). Transactions are safe because `Begin`'s snapshot and `Load` produce fresh tables whose index is rebuilt lazily.

  Correctness is preserved by design: the key encoding mirrors the engine's existing value equality (kind-tagged, so `int 1` and `float 1.0` stay distinct; two `NULL`s match), and key types that cannot be encoded identically (`DATE`, `JSON`) transparently fall back to the original linear scan.

  Measured on a batched upsert import (1000 rows/commit): per-item cost, which previously rose with N (~71 µs → ~285 µs from 1k to 8k rows), is now flat (~46–72 µs) and stays within ~2× of a plain insert. See `conflict_index_test.go` for correctness coverage (delete / update / rollback / composite / NULL / date / bool / float cases) and `insert_stress_test.go` for the scaling guard that fails if upserts regress to a linear scan.

  **Note for write-heavy imports:** `Begin` snapshots the whole database, so committing once per row is separately `O(N²)`. Wrap bulk imports in a single transaction (or commit in batches) — batching plus this index makes an upsert import linear end to end.

**Internal**

- **Docker-backed portability validation (`verify/` nested module)** — a new, isolated Go module (`github.com/flyingraptor/vapordb/verify`) validates vapordb against **real** databases using testcontainers, without adding a single dependency to the core module. It builds an in-memory `vapordb.DB` whose inferred schema exercises every Value `Kind` (int, float, bool, string, date, JSON) plus an enum-constrained column across three joinable tables, runs `GenerateDDL("postgres")` / `GenerateDDL("mysql")` against PostgreSQL 16 and MySQL 8 containers, and asserts the DDL is accepted. It then replays a **rich shared corpus** — joins (`INNER`/`LEFT`/`RIGHT` + a three-table chain), aggregates with `GROUP BY`/`HAVING`, `DISTINCT`/`COUNT(DISTINCT)`, `CASE`, `COALESCE`/`NULLIF`, scalar + correlated subqueries, `EXISTS`/`IN (subquery)`, derived tables, single/chained CTEs, `UNION`/`UNION ALL`, and window functions (ranking, `LAG`/`LEAD`, aggregate `OVER (…)` with a `ROWS` frame) — that is portable to both engines, plus **per-dialect corpora** for engine-specific features (Postgres: `FULL OUTER JOIN`, `ILIKE`, `RETURNING`, `::`, JSONB `->>`/`@>`, `EXTRACT`, `NULLS LAST`; MySQL: backtick identifiers, `IFNULL`, JSON path `->>`/`JSON_EXTRACT`/`JSON_CONTAINS`, `DATE_FORMAT`, `YEAR()`, `LIMIT offset, count`). A Docker-free `TestVapordbAcceptsSharedCorpus` proves the shared corpus is genuinely SQL vapordb parses and executes — so the full guarantee is "vapordb accepts it AND both real engines run it". A negative cross-check confirms a statement flagged by the `WithTarget` lint (backticks for Postgres, `ON CONFLICT` for MySQL) is genuinely rejected by the real database. This is portability-level validation ("does it run"), not semantic result parity.

  The module is deliberately nested with its own `go.mod` and a local `replace` directive to the core, so the testcontainers / `lib/pq` / `go-sql-driver/mysql` dependencies never touch the core `go.mod` / `go.sum` and `go test ./...` in the core never runs these Docker tests. Both targets share one table-driven runner; containers are started once per run (`TestMain`) and reused. A dedicated CI workflow (`.github/workflows/verify.yml`) runs it with Docker, separate from the core unit-test job. See `verify/README.md` for how to run it locally (`cd verify && go test ./...`, Docker required), including the concrete portability gaps it surfaced (Postgres two-arg `round()` needs a `numeric` argument; `GenerateDDL` emits no `PRIMARY KEY`/`UNIQUE`, so real `ON CONFLICT` upserts need a manually added constraint).

- **Split `executor.go` by pipeline stage** — the statement executor had grown to ~3,300 lines in a single file spanning the entire SELECT pipeline, DML, subqueries, expression evaluation, casts, and scalar functions. It is now split into cohesive same-package files — `select.go`, `join.go`, `aggregate.go`, `dml.go`, `subquery.go`, `eval.go`, `cast.go`, `func.go` — with `executor.go` retaining only the shared table-reference and row helpers plus UNION. This is a pure code-relocation refactor: no public API changed, no function signatures changed, and no test was modified (the white-box `package vapordb` test suite is unchanged and stays the behaviour oracle). The library remains a single flat package — the split is by file, not by sub-package, so the zero-friction single-import ergonomics are preserved.

- **Consolidated the pre-parse rewrite layer into `rewrite.go`** — the SQL-text rewriters that patch PostgreSQL / standard-SQL syntax into MySQL-dialect form before parsing (`rewriteDoubleQuotedIdents`, `rewriteJSONOps`, `rewriteILIKE`, `rewriteFilterAggregates`, `rewriteFullOuterJoins`, `rewriteAnyAll`) were scattered across `vapordb.go`, `upsert.go`, and `rewrite_filter.go`, and the order-sensitive chain was hand-inlined and duplicated across the `Query`, `Exec`, and `RETURNING` entry points. They now live in one `rewrite.go` with a documented pipeline and two orchestrators — `rewritePreParse` (shared base chain) and `rewriteDML` (base + `= ANY`/`<> ALL` + `ON CONFLICT`) — so the rewrite ordering is defined in exactly one place. Pure relocation: no behaviour or test changes.

- **Stabilised the wall-clock scaling guards** (`TestUpsertScalesLinearlyWithIndex`, `TestBatchCommitRemovesTxBottleneck`, and the join-scaling test). Per-item timing ratios at these scales are dominated by warmup / GC / scheduler noise — batching already removes most of the O(N²) term — so absolute thresholds flake under suite load no matter how they are tuned. The threshold assertions now fail only under `VAPORDB_PERF=1` (a dedicated perf job); the default `go test` run measures and logs them without ever flaking. A genuine asymptotic regression still shows up unmistakably in the logged growth ratios (~8x vs ~1x) and is enforced by the perf job.

### 2026-07-02

**Performance**

- **Hash join for equi-joins** — joins were previously executed as a nested loop over full-table scans (for every left row, scan every right row and re-evaluate the `ON` condition on a freshly merged row), making the read path `O(N²)` per join and `O(Nᵏ)` for a k-table chain. Any `col = col` join condition (single or `AND`-chained) now builds a hash table on the right input and probes it from the left, reducing each join to `O(N)`. The optimisation applies at every link of a multi-table join chain and to all join types (`INNER` / `LEFT` / `RIGHT` / `FULL OUTER`).

  **Mixed `ON` conditions are covered too.** A condition that mixes an equi-join with other predicates — e.g. `a.id = b.a_id AND b.deleted_at IS NULL` — is split into the equi term (used for hashing) plus a *residual* predicate that is evaluated only on the key-matched candidate pairs. This keeps such joins `O(N)` without rewriting the query, and because the residual is applied during matching (not as a post-join `WHERE`) it preserves outer-join semantics: a `LEFT JOIN` whose residual fails still null-pads the unmatched left row rather than dropping it.

  Correctness is preserved by design: hash keys mirror `Compare`'s equality semantics (the numeric family — `bool`/`int`/`float` — hashes together; `NULL` never matches), and any condition without a usable equi term — purely non-equi (`<`, `>`, `LIKE`), `OR`-joined, cross joins, or key values that need coercion (`DATE`, `JSON`, or a key position mixing numeric and string values) — transparently falls back to the original nested-loop join.

  Measured on a 3-table `COUNT(*)` over a join chain at 1,600 rows per table: **6.4 s → 7.5 ms (~850×)**, memory **11.4 GB → 3.8 MB**, allocations **30.8M → 37k**. Per-item cost, which previously roughly doubled every time N doubled (the nested-loop signature), is now flat. Mixed-`ON` joins that used to fall back to the nested loop (~2.7 s at 1,600 rows) now match this. See `stress_test.go` for the scaling benchmarks and regression guards.

### 2026-06-26

**Added**

- **`INSERT … SELECT`** — a table can now be populated from the result of a query in a single statement, instead of a pre-SELECT plus an N-row upsert loop. The SELECT's output columns map onto the INSERT target list **positionally** (names need not match), and the full SELECT pipeline is available: `WHERE`, `JOIN` (including `FULL OUTER JOIN`), `GROUP BY`, `DISTINCT`, `ORDER BY`, `LIMIT`, derived tables, `UNION` / `UNION ALL`, and correlated / scalar subqueries. It composes with `ON CONFLICT … DO NOTHING | DO UPDATE`, `RETURNING`, named parameters, and CTEs (`WITH … INSERT … SELECT`).

  ```sql
  INSERT INTO items (sku, name)
  SELECT sku, name FROM catalogue WHERE in_stock
  ON CONFLICT (sku) DO UPDATE SET name = EXCLUDED.name
  RETURNING sku, name
  ```

  The INSERT needs an explicit target column list and the SELECT must name each output column (a bare `*` is rejected because rows are unordered maps); two projected columns resolving to the same name must be aliased with `AS`. For a `UNION` source every branch must project the same ordered output names — a mismatch is reported as an error rather than silently inserting NULLs. Window functions in the projection are the one unsupported case (see Roadmap → Remaining).

**Fixed**

- **`WITH … <DML>` through `db.Exec` panicked and deadlocked the database** — `Exec` rebound its `db` variable to the temporary CTE database after `resolveCTEs`, so the deferred unlock released the *temp* DB's mutex (`panic: sync: Unlock of unlocked RWMutex`) while leaving the *real* DB's write lock held forever, hanging every subsequent call. `Exec` now executes against the temp DB without rebinding, so the deferred unlock always targets the lock that is actually held. This affected any CTE-prefixed statement run through `Exec`, independent of `INSERT … SELECT`.
- **`WITH … INSERT INTO <new table> SELECT …` silently lost its rows** — tables newly created by the main statement were written only to the throwaway CTE database and never propagated back. A new `commitCTEWrites` step now copies main-statement tables back into the real database after the write, while skipping the transient CTE tables so they never leak in as real tables.
- **`FULL OUTER JOIN` was rejected by `Exec` / `RETURNING`** — `rewriteFullOuterJoins` previously ran only in the `Query` SELECT path. It is now applied in the `Exec` and `RETURNING` rewrite chains too, so `FULL OUTER JOIN` works as an `INSERT … SELECT` source.

### 2026-06-10

**Added**

- **Streaming persistence** — two new `io`-based methods complement the file-based `Save` / `Load`:

  - `db.SaveTo(w io.Writer)` — serialise the whole database as JSON to any writer.
  - `db.LoadFrom(r io.Reader)` — restore the database from JSON on any reader (reads to EOF).

  These make it trivial to persist to gzip streams, HTTP request/response bodies, `bytes.Buffer`, S3 objects, or embedded `fs.FS` snapshots. Unlike `Save` / `Load`, the streaming variants have no associated file path and therefore do not enable the companion query log.

  ```go
  var buf bytes.Buffer
  db.SaveTo(&buf)

  db2 := vapordb.New()
  db2.LoadFrom(&buf)
  ```

### 2026-05-19

**Added**

- **`ILIKE` / `NOT ILIKE`** — PostgreSQL case-insensitive pattern matching is now supported. `col ILIKE '%foo%'` is transparently rewritten to `LOWER(col) LIKE LOWER('%foo%')` before parsing, so both sides of the comparison are lowercased. `NOT ILIKE` works the same way. Supports simple identifiers, qualified identifiers (`t.col`), backtick-quoted identifiers, named parameters (`:pat`), and positional placeholders (`?`, `$1`). String literals that happen to contain the word `ILIKE` are never touched. Regular case-sensitive `LIKE` is completely unaffected.

  ```sql
  SELECT * FROM users WHERE name ILIKE '%alice%'
  SELECT * FROM orders WHERE status NOT ILIKE 'cancel%'
  SELECT * FROM t WHERE t.email ILIKE :emailPat
  ```

### 2026-05-14

**Added**

- **Double-quoted identifier support** — standard-SQL / PostgreSQL double-quoted identifiers (e.g. `"name"`, `"type"`, `"status"`, `"value"`) are now automatically translated to MySQL backtick-quoted identifiers before parsing. This means queries copied directly from PostgreSQL or any SQL editor that quotes identifiers work without modification. Single-quoted string literals containing `"` characters are left untouched. The `""` escape inside a double-quoted identifier is preserved as a literal `"`. No config needed — the rewrite is transparent and applies to all entry points: `Query`, `Exec`, `QueryNamed`, `ExecNamed`, and the `database/sql` driver.

  ```sql
  -- These are now all equivalent:
  SELECT "name", "type" FROM "orders" WHERE "status" = 'open'
  SELECT `name`, `type` FROM `orders` WHERE `status` = 'open'
  SELECT  name,  type  FROM  orders  WHERE  status  = 'open'
  ```

### 2026-05-12

**Added**

- **Value-based `RANGE` frames** — `RANGE BETWEEN N PRECEDING AND N FOLLOWING` (and all mixed combinations: `UNBOUNDED PRECEDING AND N FOLLOWING`, `N PRECEDING AND UNBOUNDED FOLLOWING`, `CURRENT ROW AND N FOLLOWING`, `N PRECEDING AND CURRENT ROW`) are now computed by comparing the numeric ORDER BY column value of each row against the current row's value ± N. Works with `int64` and `float64` ORDER BY columns; `DESC` ordering flips the direction correctly. All aggregate and value window functions benefit automatically.

- **`IN (subquery)` full pipeline** — `IN` / `NOT IN` subqueries can now contain the complete SELECT pipeline: `GROUP BY`, `HAVING`, `ORDER BY`, `LIMIT`, `OFFSET`, `DISTINCT`, and `UNION` / `UNION ALL`. Correlated references to the outer row work inside all of these. Previously only a bare `WHERE`-filtered `SELECT` was supported.

- **`DATETIME(expr)` / `TIMESTAMP(expr)` function** — new scalar date function that parses a string or `KindDate` value into a full datetime, preserving the time-of-day component. Complements `DATE(expr)` which has always truncated to midnight.

- **`FILTER (WHERE …)` on aggregates** — `SUM(amount) FILTER (WHERE type = 'RESERVED')` and all other standard aggregate functions now accept a PostgreSQL-style `FILTER` clause. Rewritten to `AGG(CASE WHEN cond THEN expr END)` before parsing, so it composes freely with `GROUP BY`, `HAVING`, `COALESCE`, and named parameters.

- **`array_agg(col)`** — collects non-NULL column values into a `KindJSON` array. Returns `NULL` when all values are NULL. Works with `FILTER (WHERE …)` and `COALESCE(array_agg(…), '[]')`.

- **Partial `ON CONFLICT` target** — `ON CONFLICT (cols) WHERE predicate DO UPDATE SET …` (partial-index conflict target). The `WHERE` predicate is stripped before execution since vapordb uses value-equality rather than real indexes; the update still applies when a conflict on the specified columns is found.

- **Optimistic locking on upsert** — `ON CONFLICT (cols) DO UPDATE SET … WHERE condition` (trailing `WHERE` on the SET clause). The existing row is updated only when `condition` evaluates to true; otherwise the conflict is silently skipped (matching PostgreSQL semantics for `xmax` / version-column locking patterns).

- **`pq.Array` parameter binding** — `pq.Array([]T{…}).Value()` returns a PostgreSQL array literal string (`"{A001,A002}"`). Both the native API (`QueryNamed` / `ExecNamed`) and the `database/sql` driver now detect this format and expand it to a SQL comma-separated literal list for use inside `IN (…)`. Plain Go slices (`[]string`, `[]int64`, etc.) are also accepted as positional arguments to `sql.DB.QueryContext` via a new `driver.NamedValueChecker` implementation that converts them to the same format before `database/sql` would reject them as unsupported types.

- **`driver.RegisterAs(sqlDriverName string)`** — registers the vapordb SQL driver under an additional name so that sqlx (and other libraries that inspect the driver name) automatically selects the correct placeholder bind style. `RegisterAs("pgx")` makes sqlx use `$N` (DOLLAR) placeholders; `RegisterAs("mysql")` makes it use `?`. The call is idempotent and safe against duplicate-registration panics.

- **`database/sql` driver** (`github.com/flyingraptor/vapordb/driver`) — import as a side-effect to register vapordb under the `"vapordb"` driver name. Call `driver.Register(name, db)` to associate a `*DB` with a DSN string, then use standard `sql.Open("vapordb", name)`. Compatible with sqlx, bun, goqu, and any library that accepts a `*sql.DB`. Both `?` (MySQL/SQLite-style) and `$1`/`$2` (Postgres-style) positional parameters are substituted as SQL literals before the query reaches the engine. Column order in result sets is alphabetical — predictable for ORMs that match by column name.

  ```go
  import _ "github.com/flyingraptor/vapordb/driver"

  driver.Register("mydb", vdb)
  sqlDB, _ := sql.Open("vapordb", "mydb")
  sqlDB.Exec(`INSERT INTO users (id, name) VALUES (?, ?)`, 1, "Alice")
  ```

- **Transactions** (`db.Begin` / `tx.Commit` / `tx.Rollback`) — `db.Begin()` returns a `*Tx` with the full `Exec`, `Query`, `ExecNamed`, and `QueryNamed` API of `*DB`. A shallow copy-on-write snapshot of every table's row slice is captured at `Begin` time; `Rollback` atomically replaces the live tables with the snapshot, and `Commit` discards it. No MVCC — sufficient for fast in-process testing.

  ```go
  tx, _ := db.Begin()
  tx.Exec(`UPDATE accounts SET balance = balance - 99 WHERE id = 1`)
  tx.Exec(`INSERT INTO orders (id, amount) VALUES (42, 99)`)
  tx.Commit()   // or tx.Rollback() to undo both changes
  ```

- **`GenerateDDL(dialect)`** — `db.GenerateDDL("mysql")` / `db.GenerateDDL("postgres")` inspects the live schema and emits `CREATE TABLE` DDL for every table, ready to paste into a real database. Type mapping: `KindBool` → `TINYINT(1)` / `BOOLEAN`; `KindInt` → `BIGINT`; `KindFloat` → `DOUBLE` / `DOUBLE PRECISION`; `KindDate` → `DATETIME` / `TIMESTAMP`; `KindJSON` → `JSON` / `JSONB`. Enum-constrained columns become `ENUM(…)` in MySQL and `TEXT … CHECK (… IN (…))` in Postgres. Tables and columns appear in alphabetical order for deterministic, diff-friendly output.

- **`KindJSON` — first-class JSON type** — a new `KindJSON` value kind stores JSON documents as live Go values (`map[string]any` / `[]any`). JSON columns arise naturally from any of:
  - `json_parse('{"k":"v"}')` literal in SQL
  - `CAST(expr AS JSON)` / `CAST(expr AS JSONB)`
  - Go `map[string]any` / `[]any` struct fields via `InsertStruct` or `ExecNamed`

  JSON values round-trip through `Save` / `Load`. Supported SQL operations:

  | Expression | Meaning |
  |------------|---------|
  | `json_extract(doc, '$.key')` | Extract value at path |
  | `doc -> '$.key'` | Shorthand for `json_extract` |
  | `json_unquote(val)` | Return JSON scalar as plain string |
  | `doc ->> '$.key'` | Shorthand for `json_unquote(json_extract(…))` |
  | `json_contains(doc, candidate)` | True when doc contains all key-value pairs of candidate |
  | `doc @> expr` | PostgreSQL-style containment |
  | `doc <@ expr` | PostgreSQL-style "contained in" |
  | `json_array_length(arr)` | Number of elements in a JSON array |
  | `json_keys(obj)` | Keys of a JSON object as a JSON array |
  | `json_type(val)` | `'OBJECT'`, `'ARRAY'`, `'STRING'`, `'INTEGER'`, `'BOOLEAN'` |

  `ScanRows[T]` maps `KindJSON` values into `map[string]any`, `[]any`, and `any` struct fields automatically.

**Fixed**

- **`HAVING` aggregates not in `SELECT`** — `SELECT dept_id … GROUP BY dept_id HAVING COUNT(*) > 1` now works correctly when `COUNT(*)` is not in the `SELECT` list. Previously the aggregate resolved to `NULL` inside the HAVING predicate because `collectGroupAggs` did not recurse into `ComparisonExpr`, `AndExpr`, `OrExpr`, `NotExpr`, `IsExpr`, or `RangeCond` nodes. Affected both the main `execSelect` pipeline and all subquery paths.
- **`time.Time` named-parameter truncation** — `ExecNamed`, `ExecStruct`, and `InsertStruct` previously always emitted `DATE('…')` for `time.Time` parameters, silently dropping hours/minutes/seconds. They now emit `DATETIME('…')` when the time component is non-zero, and `DATE('…')` only for midnight-valued times.
- **`Save`/`Load` round-trip** — verified and fixed for every `Kind` (Null, Bool, Int, Float, String, Date with time component, JSON object, JSON array), `EnumSets`, and the `Locked` flag.

### 2026-05-11

**Added**

- **`FULL OUTER JOIN` / `FULL JOIN`** — returns all rows from both sides; matched rows appear once with both sides populated; unmatched left rows get `NULL` right columns; unmatched right rows get `NULL` left columns. Works with `WHERE`, `GROUP BY`, `ORDER BY`, CTEs, and chains of multiple joins.
- **`RIGHT JOIN` / `RIGHT OUTER JOIN`** — previously parsed but semantically treated as INNER JOIN; now correctly returns all right rows with `NULL` left columns for unmatched right rows.
- **Window frames** — `ROWS BETWEEN … AND …` and `RANGE BETWEEN … AND …` frame clauses parsed and applied to aggregate and value window functions. Supported bounds: `UNBOUNDED PRECEDING`, `N PRECEDING`, `CURRENT ROW`, `N FOLLOWING`, `UNBOUNDED FOLLOWING`. When ORDER BY is present in `OVER(…)` but no frame is written, the SQL standard default `RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW` is used (running aggregate). Without ORDER BY the default is the whole partition.
- **`LAG(col [, offset [, default]])`** / **`LEAD(col [, offset [, default]])`** — offset lookup in the sorted partition. Offset defaults to 1; the optional third argument is the default when no prior/next row exists. Work with `PARTITION BY`.
- **`FIRST_VALUE(col)`** / **`LAST_VALUE(col)`** / **`NTH_VALUE(col, n)`** — frame-based value functions.
- **`NTILE(n)`** — divides the ordered partition into `n` as-equal-as-possible buckets; returns the 1-based bucket number per row.
- **`CUME_DIST()`** — cumulative distribution: `(last_peer_position + 1) / partition_size`.
- **`PERCENT_RANK()`** — percent rank: `(rank − 1) / (partition_size − 1)`; first row in ordering is always `0.0`.
- **Scalar subqueries** — a `(SELECT col FROM …)` expression anywhere a value is expected: projected columns, `WHERE` / `HAVING` operands, the RHS of comparisons, and `ORDER BY`. Must project exactly one column; zero rows → `NULL`; two or more rows → error. Full inner SELECT pipeline supported. Correlated references to the outer row work.
- **`IN (subquery)` / `NOT IN (subquery)`** — correlated and uncorrelated subqueries on the RHS of `IN` / `NOT IN`. Empty subquery makes `IN` always false and `NOT IN` always true.
- **`LIKE … ESCAPE`** — literal `%` and `_` in patterns via an escape character.
- **`||` operator** — concatenate when either operand is a string, otherwise boolean OR.
- **`RETURNING` on upsert** — `INSERT … ON CONFLICT … RETURNING` returns one row per `VALUES` tuple.
- **Named parameters (extended)** — `QueryNamed` / `ExecNamed` now accept `time.Time`, `driver.Valuer`, `encoding.TextMarshaler`, and `fmt.Stringer` values (e.g. `net.IP`, `uuid.UUID`).
- **`CAST` / `CONVERT`** — typed coercions for common SQL targets.
- **Schema conflict policy** — unsafe column type changes error by default; opt in to the legacy wipe behaviour with `New(WithForceWipeOnSchemaConflict(true))` or per-call `WithWriteForceWipeOnSchemaConflict`.
- **Embedded structs** — `ScanRows`, `InsertStruct`, and named-parameter structs recurse anonymous embedded fields with `db` tags.

**Fixed**

- **Qualified column resolution in outer joins** — `resolveColumn` no longer lets the suffix fallback match a column from a different table when the reference has an explicit qualifier (e.g. `l.id` no longer resolves to `r.id` when the left table is empty).
- **Correlated subquery column shadowing** — inner table columns are qualified so bare unqualified references resolve to the inner table while outer-table–qualified references (e.g. `users.id`) resolve to the outer row even when both tables share a column name.
- **Window + `LIMIT`** — outer `LIMIT` / `OFFSET` apply after window evaluation so `COUNT(*) OVER()` reflects the full filtered set.
- **Correlated `EXISTS`** — single-table `FROM` with an explicit alias qualifies outer columns so inner `id` does not shadow `alias.id`.
- **`INSERT … RETURNING`** after a schema wipe returns the correct rows.

### 2026-04-27

**Added**

- **Query log.** After `db.Save(path)` or `db.Load(path)`, all subsequent `Exec` and `Query` calls are appended in real time to a companion JSON Lines file (e.g. `db.json` → `db_queries.jsonl`). Each entry contains `ts`, `op`, `sql`, `duration_ms`, `rows`, and `error`. Append-only; logging starts automatically once a path is known.
- **Schema locking.** `db.LockTable(name)` / `db.LockSchema()` freeze the schema of one or all tables. Locked tables reject INSERTs that would add columns, widen types, or cause unsafe type changes. `db.UnlockTable` / `db.UnlockSchema` re-enable evolution. Lock state persists through `Save` / `Load`.
- **Enum constraints.** `db.DeclareEnum(table, col, vals...)` registers an allowed-value constraint on a string column. Calling it again widens the set. Constraints persist through `Save` / `Load`.
- **Goroutine safety.** All public methods are safe for concurrent use. Pure SELECTs acquire a shared read lock; mutating calls acquire an exclusive write lock.

### 2026-04-25

**Added**

- **`RETURNING` clause.** Append `RETURNING col1, col2` or `RETURNING *` to any `INSERT`, `UPDATE`, or `DELETE`. INSERT returns inserted rows; UPDATE returns post-update state; DELETE returns pre-deletion state.
- **Window functions.** `ROW_NUMBER()`, `RANK()`, `DENSE_RANK()`, `COUNT`, `SUM`, `AVG`, `MIN`, `MAX` with `OVER([PARTITION BY …] [ORDER BY …])`. Frame support and additional functions (`LAG`, `LEAD`, `FIRST_VALUE`, etc.) added 2026-05-11.
- **CTEs (`WITH … AS (…) SELECT …`).** Multiple CTEs, CTEs referencing earlier CTEs, CTEs with `UNION`, and CTEs in `JOIN` / `EXISTS` all work.
- **`UNION` / `UNION ALL`.** Chains of three or more, mixed `UNION` / `UNION ALL`, top-level `ORDER BY` and `LIMIT` on the combined result.
- **Subqueries in `FROM` (derived tables).** `SELECT … FROM (SELECT …) AS sub` with `SELECT *`, outer `WHERE`, qualified `alias.col`, `JOIN` against derived tables, and nesting.
- **`SELECT EXISTS (subquery)`.** Correlated and uncorrelated; works in `WHERE`, `AND` / `OR` / `NOT`, and as a projected column.
- **`= ANY(…)` / `<> ALL(…)`.** Pre-processed to `IN` / `NOT IN`. Named slice parameters expand element-by-element.
- **UPSERT (`ON CONFLICT … DO UPDATE SET` / `DO NOTHING`).** Composite conflict keys, batch-value inserts, partial updates, constant expressions in SET, and `DO NOTHING` are all supported.
- **Named parameters.** `db.QueryNamed` / `db.ExecNamed` accept `map[string]any` or a struct with `db` tags.
- **Pointer and Stringer support in struct mapping.** `InsertStruct` dereferences pointers and calls `String()` on `fmt.Stringer` fields. `ScanRows` allocates pointer fields and uses `encoding.TextUnmarshaler` for custom types.
- **`driver.Valuer` / `sql.Scanner` support.** `InsertStruct` calls `Value()` on `driver.Valuer` fields; `ScanRows` calls `Scan(src)` on `sql.Scanner` fields.

### 2026-04-24

**Added**

- **Date support** (`KindDate`) backed by `time.Time`.
  - `DATE(expr)` parses a string literal into a date value.
  - Comparison operators (`>`, `<`, `>=`, `<=`, `=`, `<>`) against date columns; string literals are automatically coerced when one operand is `KindDate`.
  - `BETWEEN` / `NOT BETWEEN`, `ORDER BY`, `MIN`, `MAX`, `IN` on date columns.
  - Date functions: `NOW()`, `CURDATE()`, `DATE()`, `YEAR()`, `MONTH()`, `DAY()`, `HOUR()`, `MINUTE()`, `SECOND()`, `WEEKDAY()`, `DAYOFWEEK()`, `DATEDIFF()`, `TIMESTAMPDIFF()`, `DATE_FORMAT()`, `DATE_ADD()`, `DATE_SUB()`.
  - `time.Time` fields in structs round-trip through `InsertStruct` and `ScanRows` automatically.
  - `KindDate` values persist correctly through `Save` / `Load`.
  - Expression-only `SELECT` without a real table (e.g. `SELECT NOW()`) supported via `FROM DUAL`.
- **NULL propagation fix for `NOT BETWEEN`** A NULL left operand now correctly returns false for both `BETWEEN` and `NOT BETWEEN`, matching standard SQL semantics.

### Initial release

- In-memory SQL engine with automatic schema inference and safe type widening.
- `SELECT` with `WHERE`, `JOIN` (INNER, LEFT, RIGHT, FULL OUTER), `GROUP BY`, `HAVING`, `ORDER BY`, `LIMIT`, `OFFSET`, `DISTINCT`.
- Aggregates: `COUNT`, `SUM`, `AVG`, `MIN`, `MAX` (including `COUNT(DISTINCT …)`).
- Predicates: `=`, `<>`, `<`, `>`, `<=`, `>=`, `BETWEEN`, `IN`, `NOT IN`, `LIKE`, `NOT LIKE`, `IS NULL`, `IS NOT NULL`, `IS TRUE`, `IS FALSE`, `AND`, `OR`, `NOT`.
- Scalar functions: `UPPER`, `LOWER`, `LENGTH`, `CHAR_LENGTH`, `CONCAT`, `COALESCE`, `IFNULL`, `NULLIF`, `ABS`, `ROUND`, `FLOOR`, `CEIL`, `CAST`.
- `CASE` / `WHEN` / `ELSE` expressions and arithmetic operators.
- `INSERT`, `UPDATE`, `DELETE`.
- `db.InsertStruct` and `vapordb.ScanRows[T]` for struct-based data access via `db` tags.
- `db.Save` / `db.Load` for JSON persistence.
