# vapordb — Proposed Features & Fixes

---

## Bugs

### 1. `COUNT(*) OVER()` counts post-LIMIT rows

**File:** `vapordb.go` (`Query`), `executor.go` (`execSelect`)

Window functions are extracted before parsing, replaced with placeholders, and
applied by `applyWindowFuncs` after `execSelectStatement` returns. Because
`execSelect` applies `LIMIT` internally, `applyWindowFuncs` only sees the
already-truncated slice. `COUNT(*) OVER()` returns the page size, not the total
matched row count.

**Fix:** When window specs are present, strip `LIMIT`/`OFFSET` from the parsed
statement before executing, apply window functions on the full result, then
re-apply the limit afterwards.

---

### 2. Correlated EXISTS silently resolves column against wrong row

**File:** `executor.go` (`execSelectCorrelated`, `mergeRowsOuter`, `resolveColumn`)

When the outer `FROM` has a single table (e.g. `FROM regions r`), `isMultiTable`
is `false` so `qualifyRow` is not called — outer row keys are bare (`id`, `name`,
…). Inside `execSelectCorrelated`, `mergeRowsOuter` blends outer and inner rows.
If the inner table also has a column named `id`, `resolveColumn`'s last-resort
suffix search finds the inner key first. The correlated condition
`rc.region_id = r.id` silently resolves `r.id` to the inner table's `id`.

**Fix:** Always qualify row keys with `alias.` when the table carries an explicit
alias, regardless of `isMultiTable`.

---

### 3. `ScanRows` and `toParamMap` do not handle embedded structs

**File:** `mapping.go` (`ScanRows`), `named.go` (`toParamMap`)

Both build their field/tag maps using `rt.NumField()` over top-level fields only.
Embedded (anonymous) structs are skipped, so promoted `db`-tagged fields are
never scanned or bound.

```go
type regionRowWithCount struct {
    regionRow          // ID uuid.UUID `db:"id"`, Name string `db:"name"` — ignored
    TotalCount int `db:"total_count"`
}
```

**Fix:** Recurse into anonymous struct fields when building the tag index,
matching `database/sql` + `sqlx` behaviour.

---

### 4. `CAST()` is a no-op

**File:** `executor.go` (lines ~1724–1728)

`CAST(x AS INT)`, `CAST(x AS TEXT)`, etc. return the original value unchanged.
No type conversion is performed.

**Fix:** Implement actual coercions for the common cast targets: `INT` /
`INTEGER` / `BIGINT`, `FLOAT` / `DOUBLE`, `TEXT` / `CHAR` / `VARCHAR`,
`DATE` / `DATETIME` / `TIMESTAMP`, `BOOL` / `BOOLEAN`.

---

### 5. Destructive type-widening wipe on schema conflict

**File:** `infer.go` (lines ~49–55)

When an INSERT introduces a value whose inferred type conflicts with the existing
column type, the entire table's rows are silently deleted. This makes type
mistakes during test seeding catastrophically hard to debug.

**Fix:** Return an error on type conflict rather than wiping rows. Alternatively,
attempt implicit coercion and only error when it is impossible.

---

### 6. String concatenation operator `||` not supported

**File:** `executor.go` (binary expression evaluation)

The SQL standard and SQLite both use `||` for string concatenation. vapordb only
handles `+`. Expressions like `'%' || :search || '%'` in LIKE clauses fail.

**Fix:** Treat `||` as a string concatenation operator alongside `+`. This is
standard SQL and supported by PostgreSQL, SQLite, and DuckDB — `+` for concat
is a SQL Server / MySQL-ism and should be considered a bonus, not the default.

---

## Features

### 7. Transactions — `BEGIN` / `COMMIT` / `ROLLBACK`

All writes are immediate and unconditional. There is no way to group statements
into an atomic unit or to undo a partial write.

**Proposed API:**

```go
tx, err := db.Begin()
tx.ExecNamed(...)
tx.Commit()    // or tx.Rollback()
```

Internally a transaction could snapshot the affected tables' row slices on
`Begin` and restore them on `Rollback`. Full MVCC is not required; a copy-on-
write approach per transaction is sufficient for test use.

---

### 8. `INSERT ... SELECT`

**File:** `executor.go` (lines ~296–299 reject non-VALUES inserts)

```sql
INSERT INTO archive SELECT * FROM orders WHERE status = 'closed'
```

Currently unsupported; only `INSERT ... VALUES` is accepted.

---

### 9. `INTERSECT` and `EXCEPT` set operations

**File:** `executor.go` (lines ~229–285 handle UNION/UNION ALL only)

Standard SQL set operations that complement the already-supported `UNION` /
`UNION ALL`.

---

### 10. `FULL OUTER JOIN`

**File:** `executor.go` (`applyJoin`, line ~511)

`INNER`, `LEFT`, `RIGHT`, and implicit `CROSS` joins are supported. `FULL OUTER
JOIN` is not.

---

### 11. `IN (subquery)`

**File:** `executor.go` (lines ~1415–1433 handle value-list IN only)

```sql
WHERE region_id IN (SELECT id FROM regions WHERE name LIKE 'North%')
```

Currently only `IN (val1, val2, …)` literal lists are supported.

---

### 12. Window function frame specifications

**File:** `window.go`

`ROWS BETWEEN` / `RANGE BETWEEN` frame clauses are not parsed or applied. This
makes it impossible to implement running totals, moving averages, or
`FIRST_VALUE` / `LAST_VALUE` correctly.

Also missing: `LAG()`, `LEAD()`, `FIRST_VALUE()`, `LAST_VALUE()`,
`NTH_VALUE()`, `NTILE()`, `CUME_DIST()`, `PERCENT_RANK()`.

---

### 13. Missing string functions

The following commonly-used functions are absent:

| Function | Notes |
|----------|-------|
| `SUBSTRING()` / `SUBSTR()` | Extract part of a string |
| `TRIM()` / `LTRIM()` / `RTRIM()` | Strip whitespace or chars |
| `REPLACE()` | Replace occurrences |
| `INSTR()` / `POSITION()` | Find substring position |
| `LPAD()` / `RPAD()` | Pad to fixed width |
| `REPEAT()` | Repeat string N times |
| `REVERSE()` | Reverse a string |

---

### 14. Missing aggregate functions

| Function | Notes |
|----------|-------|
| `GROUP_CONCAT()` / `STRING_AGG()` | Concatenate strings per group |
| `STDDEV()` / `VARIANCE()` | Statistical aggregates |
| `COUNT(DISTINCT expr)` in window | Supported in GROUP BY, missing in OVER |

---

### 15. `DISTINCT` in non-COUNT aggregates

`COUNT(DISTINCT col)` works. `SUM(DISTINCT col)`, `AVG(DISTINCT col)` do not.

---

### 16. Scalar subqueries in `SELECT` and `WHERE`

```sql
SELECT name, (SELECT COUNT(*) FROM orders WHERE orders.user_id = users.id) AS order_count
FROM users
```

Scalar subqueries that return a single value are not supported outside of
`EXISTS`.

---

### 17. `RECURSIVE` CTEs

```sql
WITH RECURSIVE tree AS (
    SELECT id, parent_id FROM nodes WHERE parent_id IS NULL
    UNION ALL
    SELECT n.id, n.parent_id FROM nodes n JOIN tree ON tree.id = n.parent_id
)
SELECT * FROM tree
```

`WITH` is supported but recursion is not detected or executed.

---

### 18. Unique and primary key constraints

`DeclareEnum` is the only column constraint. There is no way to enforce
uniqueness on a column or combination of columns, so duplicate-key bugs go
undetected in tests.

**Proposed API:**

```go
db.DeclarePrimaryKey("users", "id")
db.DeclareUnique("users", "email")
```

---

### 19. `DEFAULT` column values

There is no way to declare a default value for a column. Omitted columns always
become NULL. This forces test fixtures to provide every column explicitly even
when sensible defaults exist in the real schema.

**Proposed API:**

```go
db.DeclareDefault("users", "created_at", time.Now)  // func() any
db.DeclareDefault("orders", "status", "pending")
```

---

### 20. `LIKE` with `ESCAPE` clause

```sql
WHERE path LIKE '50\%' ESCAPE '\'
```

There is no way to match a literal `%` or `_` in a LIKE pattern.

---

### 21. `RETURNING` on upsert (`ON CONFLICT ... RETURNING`)

`RETURNING` works on plain INSERT/UPDATE/DELETE. It does not work when combined
with `ON CONFLICT DO UPDATE`, making it hard to retrieve the final row state
after an upsert.

---

### 22. Native UUID type

**Files:** `named.go` (`anyToSQLLiteral`), `mapping.go` (`setStructField`)

`uuid.UUID` implements both `fmt.Stringer` and `encoding.TextUnmarshaler`.
`InsertStruct` handles it correctly via `structFieldToSQL` (calls `.String()`),
and `ScanRows` handles it correctly via `TextUnmarshaler`. But `QueryNamed`
fails because `anyToSQLLiteral` does not check for `fmt.Stringer` — it only
handles primitive Go kinds, so passing a `uuid.UUID` param returns
`unsupported param type uuid.UUID`.

**Fix:** In `anyToSQLLiteral`, before the `switch rv.Kind()`, check whether the
value implements `fmt.Stringer` (or `encoding.TextMarshaler`) and use the string
representation. This would make `uuid.UUID`, `net.IP`, and any other
Stringer-typed named params work without callers needing to call `.String()`
manually.

At the SQL level, add a first-class `UUID` kind alongside the existing
`KindInt`, `KindFloat`, `KindString`, `KindBool`, `KindDate`. UUID comparison,
storage, and index-friendly ordering could then be handled natively rather than
being silently string-compared.

---

### 23. `database/sql` driver + dialect configuration

vapordb has its own `Query` / `QueryNamed` / `ExecNamed` API. It does not
implement `database/sql/driver.Driver` + `driver.Conn`, so it cannot be used
anywhere a `*sql.DB` is expected — including with `sqlx`, query builders, or any
application code that accepts `*sql.DB`.

Beyond the driver interface, different SQL dialects have meaningful syntax
differences that affect portability:

| Feature | PostgreSQL | MySQL | SQLite |
|---------|-----------|-------|--------|
| String concat | `\|\|` | `CONCAT()` / `+` | `\|\|` |
| Param placeholder | `$1`, `$2` | `?` | `?` |
| Boolean literals | `TRUE` / `FALSE` | `1` / `0` | `1` / `0` |
| `RETURNING` | ✅ | ❌ | ✅ (3.35+) |
| `ON CONFLICT` | ✅ | `ON DUPLICATE KEY` | ✅ |
| `ILIKE` | ✅ | ❌ (case-insensitive by collation) | ❌ |
| `UUID` type | native | `CHAR(36)` | `TEXT` |
| `SERIAL` / `GENERATED` | ✅ | `AUTO_INCREMENT` | `ROWID` |

**Proposed:** A dialect option on `New` that configures parsing and evaluation
for the target database:

```go
db := vapordb.New(vapordb.WithDialect(vapordb.Postgres))
db := vapordb.New(vapordb.WithDialect(vapordb.MySQL))
db := vapordb.New(vapordb.WithDialect(vapordb.SQLite))
```

#### Implementation approach

The cleanest split is three layers, each dialect-aware:

**1. Pre-processing (rewrite layer)**
Before the SQL reaches the parser, a dialect-specific rewriter normalises
syntax that the underlying `sqlparser` cannot handle:
- Rewrite `$1`, `$2` positional placeholders → `:__p1__`, `:__p2__` named form
- Rewrite `?` positional placeholders similarly
- Rewrite `||` → `CONCAT(…)` if the parser does not support it natively
- Rewrite `ILIKE` → `LIKE` + case-fold both operands

This keeps the core parser and executor unchanged and dialect logic isolated in
one place (`dialect.go`).

**2. Evaluation (function / operator layer)**
A small dialect config struct passed through the executor controls:
- Whether `+` is numeric-only or also string concat
- Whether string comparisons are case-sensitive (`LIKE` vs `ILIKE`)
- Which aggregate/window functions are available
- Type affinity rules (e.g. Postgres `UUID` column vs SQLite `TEXT`)

**3. `database/sql` driver registration**
Each dialect registers itself as a named `database/sql` driver on `init()` so
callers can use the standard interface without changing application code:

```go
import _ "github.com/flyingraptor/vapordb/dialect/postgres"

db, _ := sql.Open("vapordb+postgres", "")
// db is a *sql.DB — works with sqlx, goqu, bun, etc.
```

The driver adapter translates between `database/sql/driver.Value` (limited to
`int64`, `float64`, `string`, `[]byte`, `time.Time`, `bool`, `nil`) and
vapordb's richer `Value` type, and routes `QueryContext` / `ExecContext` through
the existing vapordb engine.

#### Scope note
The dialect system does not need to be exhaustive on day one. A `Postgres` dialect
covering `||`, `$N` placeholders, `ILIKE`, and `RETURNING` covers the majority of
real-world usage and is enough to make vapordb a viable drop-in test double for
Postgres-targeting services.

---

### 24. Schema inspection API

There is no way to programmatically inspect the inferred schema at runtime. This
makes it difficult to write generic fixtures or assert that an expected column
exists.

**Proposed API:**

```go
db.Tables()                        // []string
db.Columns("regions")              // []ColumnInfo{Name, Kind}
db.HasTable("regions") bool
db.HasColumn("regions", "name") bool
```

---

### 25. `EXPLAIN` / query tracing

Even a minimal `EXPLAIN` that returns the sequence of operations (table scan,
join, filter, sort, limit) would make debugging unexpected query results much
faster. Alternatively, a structured query log with per-query timings.