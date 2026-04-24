# vapordb

In-memory SQL database for fast prototyping in Go - no setup, no schema, just queries

When building something new, the data model changes constantly. With a real database every field addition, rename, or type change requires a migration script, an ALTER TABLE, and a re-run of your seed data. That friction compounds quickly and slows you down at exactly the stage where you need to move fast.

vapordb removes that entirely. You just write code. Change a struct, add a column to an INSERT, and the schema updates itself. There is nothing to migrate, nothing to roll back, and no mismatch between your code and your database to debug. You stay focused on the logic and the shape of the data rather than the mechanics of keeping a schema in sync.

The result is full SQL with joins, aggregates, CASE, LIKE, BETWEEN. Enough to work with your data properly while you design. Once the data model stabilises and you are ready to commit to a real database, you write the CREATE TABLE and migrations once, with full knowledge of what you actually need.

```go
db := vapordb.New()
db.Exec(`INSERT INTO users (id, name, age) VALUES (1, 'Alice', 30)`)

rows, _ := db.Query(`SELECT name FROM users WHERE age > 25`)
```

## Features

- **Zero setup.** No CREATE TABLE, no migrations. Schema is inferred from the first INSERT.
- **Automatic schema evolution.** New columns are added on the fly. Safe type widening (e.g. `int` to `float`) happens automatically.
- **SQL you already know.** SELECT, INSERT, UPDATE, DELETE with WHERE, JOIN, GROUP BY, ORDER BY, LIMIT, HAVING.
- **Rich expressions.** Aggregates, scalar functions, CASE, BETWEEN, IN, LIKE, arithmetic, and more.
- **UPSERT.** `ON CONFLICT (col) DO UPDATE SET …` and `ON CONFLICT (col) DO NOTHING`. Composite conflict keys and batch-value inserts are supported.
- **Date support.** `KindDate` type backed by `time.Time`. Date literals, comparisons, BETWEEN, ORDER BY, and date functions (NOW, CURDATE, DATE, YEAR, MONTH, DAY, DATEDIFF, DATE_ADD, DATE_FORMAT, …). String literals auto-coerce when compared against date columns.
- **Named parameters.** `:param` placeholders in any SQL statement via `db.QueryNamed` / `db.ExecNamed`. Accepts `map[string]any` or a `db`-tagged struct.
- **Struct mapping.** Insert from structs and scan results back into typed slices via `db` tags. Pointer fields, `sql.NullString` / `sql.Null*`, `fmt.Stringer`, `encoding.TextUnmarshaler`, and custom `driver.Valuer` / `sql.Scanner` types all round-trip automatically.
- **Optional persistence.** Save the entire database to a JSON file and reload it later.

## Installation

```bash
go get github.com/flyingraptor/vapordb
```

## Quick Start

```go
package main

import (
    "fmt"
    "github.com/flyingraptor/vapordb"
)

func main() {
    db := vapordb.New()

    db.Exec(`INSERT INTO users (id, name, age) VALUES (1, 'Alice', 30)`)
    db.Exec(`INSERT INTO users (id, name, age) VALUES (2, 'Bob',   25)`)
    db.Exec(`INSERT INTO users (id, name, age) VALUES (3, 'Carol', 28)`)

    rows, _ := db.Query(`SELECT name, age FROM users WHERE age >= 28 ORDER BY age DESC`)
    for _, r := range rows {
        fmt.Printf("%s is %v\n", r["name"].V, r["age"].V)
    }
    // Carol is 28
    // Alice is 30
}
```

## Core API

| Method | Description |
|--------|-------------|
| `vapordb.New()` | Create a new empty database |
| `db.Exec(sql)` | Run INSERT, UPDATE, or DELETE |
| `db.Query(sql)` | Run SELECT, returns `[]Row` |
| `db.Save(path)` | Persist the database to a JSON file |
| `db.Load(path)` | Load a previously saved JSON file |
| `db.InsertStruct(table, v)` | Insert a struct using `db` field tags |
| `vapordb.ScanRows[T](rows)` | Scan `[]Row` into a typed slice |
| `db.QueryNamed(sql, params)` | SELECT with named `:param` placeholders |
| `db.ExecNamed(sql, params)` | INSERT/UPDATE/DELETE with named `:param` placeholders |

A `Row` is `map[string]Value`. Each `Value` has:
- `.V` is the underlying Go value (`int64`, `float64`, `string`, `bool`, `time.Time`, or `nil`)
- `.Kind` is one of `KindNull`, `KindBool`, `KindInt`, `KindFloat`, `KindString`, `KindDate`

## Schema Inference

You never define a schema. vapordb infers it from the data:

```go
db.Exec(`INSERT INTO events (id, name) VALUES (1, 'launch')`)
// schema: {id: Int, name: String}

db.Exec(`INSERT INTO events (id, name, score) VALUES (2, 'beta', 9.5)`)
// schema: {id: Int, name: String, score: Float}
// previous row gets score = NULL automatically
```

Type widening is safe and automatic (`bool` to `int` to `float`). Crossing into a different family (e.g. a column that was `int` now receives a `string`) wipes the table and starts fresh with the new type.

## Struct Mapping

Tag your structs with `db` and use the built-in helpers:

```go
type Product struct {
    ID    int     `db:"id"`
    Name  string  `db:"name"`
    Price float64 `db:"price"`
}

// insert
db.InsertStruct("products", Product{1, "Widget", 9.99})
db.InsertStruct("products", Product{2, "Gadget", 24.50})

// query back into typed slice
rows, _ := db.Query(`SELECT id, name, price FROM products ORDER BY price`)
products := vapordb.ScanRows[Product](rows)
```

NULL columns are mapped to the field's zero value (`0`, `""`, `false`).

### Pointer fields

Pointer fields map NULL to `nil` and non-NULL to an allocated value:

```go
type User struct {
    ID    int     `db:"id"`
    Name  string  `db:"name"`
    Bio   *string `db:"bio"`   // NULL when not set
}

bio := "Go enthusiast"
db.InsertStruct("users", User{ID: 1, Name: "Alice", Bio: &bio})
db.InsertStruct("users", User{ID: 2, Name: "Bob"})   // Bio is nil → NULL

rows, _ := db.Query(`SELECT id, name, bio FROM users ORDER BY id`)
users := vapordb.ScanRows[User](rows)
// users[0].Bio  →  &"Go enthusiast"
// users[1].Bio  →  nil
```

### sql.NullString and sql.Null* types

The standard `database/sql` nullable types work out of the box via the `driver.Valuer` / `sql.Scanner` interfaces:

```go
type Order struct {
    ID      int            `db:"id"`
    Note    sql.NullString `db:"note"`    // nullable string
    Qty     sql.NullInt64  `db:"qty"`     // nullable int
}

db.InsertStruct("orders", Order{
    ID:   1,
    Note: sql.NullString{String: "urgent", Valid: true},
    Qty:  sql.NullInt64{Int64: 3, Valid: true},
})
db.InsertStruct("orders", Order{
    ID:   2,
    Note: sql.NullString{Valid: false},   // → NULL
    Qty:  sql.NullInt64{Valid: false},    // → NULL
})

rows, _ := db.Query(`SELECT id, note, qty FROM orders ORDER BY id`)
orders := vapordb.ScanRows[Order](rows)
// orders[1].Note.Valid  →  false
```

### Custom types (driver.Valuer / sql.Scanner)

Any type implementing both interfaces round-trips automatically:

```go
type Status string

func (s Status) Value() (driver.Value, error) { return string(s), nil }
func (s *Status) Scan(src any) error {
    if v, ok := src.(string); ok { *s = Status(v) }
    return nil
}

type Task struct {
    ID     int    `db:"id"`
    Status Status `db:"status"`
}

db.InsertStruct("tasks", Task{ID: 1, Status: "open"})
tasks := vapordb.ScanRows[Task](mustQuery(db, `SELECT id, status FROM tasks`))
// tasks[0].Status  →  "open"
```

Types implementing `fmt.Stringer` and `encoding.TextUnmarshaler` (such as `net.IP` or `uuid.UUID`) also round-trip without any extra code.

## Named Parameters

Use `:name` placeholders instead of inlining values into SQL strings. Pass either a `map[string]any` or a struct with `db` tags.

```go
// map
rows, err := db.QueryNamed(
    `SELECT * FROM orders WHERE user_id = :uid AND status = :status`,
    map[string]any{"uid": 42, "status": "open"},
)

// struct
type Filter struct {
    MinAge int    `db:"min"`
    MaxAge int    `db:"max"`
}
rows, err = db.QueryNamed(
    `SELECT name FROM users WHERE age BETWEEN :min AND :max`,
    Filter{MinAge: 25, MaxAge: 35},
)

// insert
err = db.ExecNamed(
    `INSERT INTO users (id, name, age) VALUES (:id, :name, :age)`,
    map[string]any{"id": 5, "name": "Eve", "age": 28},
)
```

Single-quoted string literals in the SQL are never scanned for placeholders, so values like `WHERE note = ':not_a_param'` are safe. Single quotes inside string values are automatically escaped.

## Persistence

```go
// save at any point
db.Save("/var/data/myapp.json")

// load on next startup
db := vapordb.New()
if err := db.Load("/var/data/myapp.json"); err != nil && !errors.Is(err, os.ErrNotExist) {
    log.Fatal(err)
}
```

The JSON file contains the full schema and all rows for every table.

## SQL Reference

### SELECT

```sql
SELECT name, age FROM users
SELECT * FROM users WHERE age > 25
SELECT * FROM users ORDER BY age DESC LIMIT 10
SELECT * FROM users ORDER BY age DESC LIMIT 10 OFFSET 20
SELECT DISTINCT city FROM users
```

### Aggregates

```sql
SELECT COUNT(*), SUM(amount), AVG(score), MIN(age), MAX(age) FROM users
SELECT dept, COUNT(*) AS cnt FROM employees GROUP BY dept HAVING cnt > 5
SELECT COUNT(DISTINCT label) FROM products
```

### JOINs

```sql
SELECT u.name, o.product
FROM users u
INNER JOIN orders o ON u.id = o.user_id

SELECT u.name, o.product
FROM users u
LEFT JOIN orders o ON u.id = o.user_id

SELECT u.name, p.title, c.body
FROM users u
INNER JOIN posts p ON p.author_id = u.id
INNER JOIN comments c ON c.post_id = p.id
```

### WHERE predicates

```sql
WHERE age >= 18
WHERE age BETWEEN 18 AND 65
WHERE name IN ('Alice', 'Bob')
WHERE name NOT IN ('Alice', 'Bob')
WHERE name LIKE 'A%'
WHERE name LIKE '_lice'
WHERE score IS NULL
WHERE score IS NOT NULL
WHERE age > 18 AND score >= 50
WHERE age < 18 OR age > 65
WHERE NOT (age = 25)
```

### Scalar functions

```sql
UPPER(name), LOWER(name)
LENGTH(name), CHAR_LENGTH(name)
CONCAT(first, ' ', last)
COALESCE(score, 0)
IFNULL(score, 0)
NULLIF(score, 0)
ABS(balance)
ROUND(price, 2), FLOOR(price), CEIL(price)
CAST(age AS CHAR)
```

### Dates

Date columns are stored as `time.Time` with `KindDate`. Use the `DATE()` function to create date literals from strings, or insert `time.Time` values directly via `InsertStruct`.

String literals in comparisons against date columns are automatically coerced — `WHERE created_at > '2024-01-01'` works without wrapping in `DATE()`.

```sql
-- Insert using DATE() literal
INSERT INTO events (id, name, created_at) VALUES (1, 'launch', DATE('2024-01-15'))

-- Compare and filter
SELECT * FROM events WHERE created_at > '2024-01-01'
SELECT * FROM events WHERE created_at BETWEEN '2024-01-01' AND '2024-12-31'
SELECT * FROM events WHERE created_at IN ('2024-01-15', '2024-06-01')

-- Order by date
SELECT * FROM events ORDER BY created_at DESC

-- Aggregate
SELECT MIN(created_at), MAX(created_at) FROM events

-- Date functions
NOW()                              -- current datetime
CURDATE()                          -- today at midnight UTC
DATE(expr)                         -- truncate to date (strip time part)
YEAR(date), MONTH(date), DAY(date) -- extract parts
HOUR(dt), MINUTE(dt), SECOND(dt)
DATEDIFF(d1, d2)                   -- days from d2 to d1
DATE_ADD(date, INTERVAL 7 DAY)     -- add interval (units: SECOND MINUTE HOUR DAY WEEK MONTH YEAR)
DATE_SUB(date, INTERVAL 1 MONTH)
DATE_FORMAT(date, '%Y-%m-%d')      -- format with MySQL specifiers

-- Filter by extracted part
SELECT * FROM events WHERE YEAR(created_at) = 2024
SELECT * FROM events WHERE MONTH(created_at) = 12
```

**Struct mapping** — tag `time.Time` fields with `db` and they round-trip automatically:

```go
type Event struct {
    ID        int       `db:"id"`
    Name      string    `db:"name"`
    CreatedAt time.Time `db:"created_at"`
}

db.InsertStruct("events", Event{ID: 1, Name: "launch", CreatedAt: time.Now()})

rows, _ := db.Query(`SELECT * FROM events WHERE created_at > '2024-01-01'`)
events := vapordb.ScanRows[Event](rows)
```

### CASE

```sql
CASE
    WHEN score >= 90 THEN 'A'
    WHEN score >= 70 THEN 'B'
    ELSE 'C'
END
```

### INSERT / UPDATE / DELETE

```sql
INSERT INTO users (id, name, age) VALUES (1, 'Alice', 30)

UPDATE users SET age = 31 WHERE name = 'Alice'

DELETE FROM users WHERE age < 18
```

### UPSERT

PostgreSQL-style `ON CONFLICT` is fully supported. Conflict detection is done by value-equality on the specified column(s); no unique index is required.

```sql
-- Update specific columns when a row with the same id already exists.
INSERT INTO users (id, name, score)
VALUES (1, 'Alice', 99)
ON CONFLICT (id) DO UPDATE SET name = EXCLUDED.name, score = EXCLUDED.score

-- Update only one column; leave others untouched.
INSERT INTO products (sku, name, price)
VALUES ('A1', 'Widget', 14.99)
ON CONFLICT (sku) DO UPDATE SET price = EXCLUDED.price

-- Composite conflict key (both columns must match).
INSERT INTO scores (user_id, game, score)
VALUES (7, 'chess', 200)
ON CONFLICT (user_id, game) DO UPDATE SET score = EXCLUDED.score

-- Use a constant in the SET clause instead of EXCLUDED.
INSERT INTO tasks (id, status) VALUES (3, 'done')
ON CONFLICT (id) DO UPDATE SET status = 'archived'

-- Skip the insert silently if a conflict is found.
INSERT INTO users (id, name) VALUES (1, 'Bob')
ON CONFLICT (id) DO NOTHING
```

In Go:

```go
db.Exec(`
    INSERT INTO users (id, name, score) VALUES (1, 'Alice', 42)
    ON CONFLICT (id) DO UPDATE SET name = EXCLUDED.name, score = EXCLUDED.score
`)
```

`EXCLUDED.col` refers to the value that was in the incoming row for that column, matching PostgreSQL semantics.

## Use Cases

**Microservice with lightweight local state**

Load once at startup, query in handlers, save after mutations. No external database required.

```go
var (
    db *vapordb.DB
    mu sync.RWMutex
)

func main() {
    db = vapordb.New()
    db.Load("state.json")
    http.HandleFunc("/users", handleUsers)
    http.ListenAndServe(":8080", nil)
}

func handleUsers(w http.ResponseWriter, r *http.Request) {
    mu.RLock()
    rows, _ := db.Query(`SELECT id, name FROM users`)
    mu.RUnlock()
    // ...
}
```

**CLI tools and scripts**

Query, filter, and transform tabular data without spinning up a database.

**Testing**

Seed an in-memory database per test. Fast, isolated, no cleanup needed.

```go
func TestPricing(t *testing.T) {
    db := vapordb.New()
    db.Exec(`INSERT INTO products (id, price) VALUES (1, 9.99)`)
    rows, _ := db.Query(`SELECT price FROM products WHERE id = 1`)
    // assert...
}
```

**Prototyping**

Sketch out a data model and queries before committing to a real database schema.

## Limitations

- No transactions or rollback
- No indexes. All queries do a full table scan.
- No foreign key constraints. Model relations with JOINs.
- No concurrent write safety. Use a `sync.RWMutex` if sharing across goroutines.
- MySQL SQL dialect (via `github.com/xwb1989/sqlparser`)

## Roadmap

- **`= ANY(…)` / `<> ALL(…)` array operators** `IN` and `NOT IN` with literal lists already work. This item covers the PostgreSQL-dialect syntax `WHERE col = ANY(array)` / `WHERE col <> ALL(array)`, evaluated using the same underlying `IN` / `NOT IN` logic so batch-ID queries like `WHERE group_id = ANY(:group_ids)` work without rewriting.
- **`SELECT EXISTS (subquery)`** Evaluate a correlated or uncorrelated subquery in the EXISTS position, returning a bool. Needed for existence-check queries.
- **Subqueries in `FROM`** `SELECT * FROM (SELECT …) AS sub` — derived tables. A stepping stone toward CTEs and more expressive queries.
- **`UNION` / `UNION ALL`** Combining result sets from multiple SELECTs. Common for reporting and fan-out queries.
- **CTEs (`WITH … AS (…) SELECT …`)** Nearly every complex query in a real codebase uses them for readability and reuse. Also a prerequisite for recursive queries.
- **Window functions** `COUNT(*) OVER()` and similar for pagination total-count patterns. Low priority; can be worked around with a separate `COUNT(*)` query.

## Changelog

### 2026-04-25

**Added**

- **UPSERT (`ON CONFLICT … DO UPDATE SET` / `DO NOTHING`).** PostgreSQL-style upsert is pre-processed and translated to the MySQL ON DUPLICATE KEY UPDATE form the parser understands. Conflict detection scans for rows matching on the specified column(s); on a hit the SET assignments are applied in place. Composite conflict keys, batch-value inserts, partial updates (updating only some columns), constant expressions in the SET clause, and the silent-skip variant (`DO NOTHING`) are all supported.
- **Named parameters** `db.QueryNamed(sql, params)` and `db.ExecNamed(sql, params)` accept a `map[string]any` or a struct with `db` tags. `:param` placeholders in the SQL are replaced with properly escaped literals. String literals inside single quotes are never scanned, and single quotes in values are escaped automatically.
- **Pointer and Stringer support in struct mapping.** `InsertStruct` now dereferences pointer fields (`*string`, `*int`, `*float64`, …) and nil pointers become NULL. Types implementing `fmt.Stringer` (e.g. `net.IP`, `uuid.UUID`) are stored using their `String()` output. `ScanRows` allocates pointer fields when the column is non-NULL and uses `encoding.TextUnmarshaler` to reconstruct custom types from their stored string form.
- **`driver.Valuer` / `sql.Scanner` support.** `InsertStruct` calls `Value()` on any field implementing `database/sql/driver.Valuer` (e.g. `sql.NullString`, `sql.NullInt64`, custom types) and converts the result to a SQL literal. `ScanRows` calls `Scan(src)` on any field implementing `database/sql.Scanner`, passing the raw stored value. This makes `sql.Null*` types and any custom type following the standard database interface contract work automatically.

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
- `SELECT` with `WHERE`, `JOIN` (INNER, LEFT), `GROUP BY`, `HAVING`, `ORDER BY`, `LIMIT`, `OFFSET`, `DISTINCT`.
- Aggregates: `COUNT`, `SUM`, `AVG`, `MIN`, `MAX` (including `COUNT(DISTINCT …)`).
- Predicates: `=`, `<>`, `<`, `>`, `<=`, `>=`, `BETWEEN`, `IN`, `NOT IN`, `LIKE`, `NOT LIKE`, `IS NULL`, `IS NOT NULL`, `IS TRUE`, `IS FALSE`, `AND`, `OR`, `NOT`.
- Scalar functions: `UPPER`, `LOWER`, `LENGTH`, `CHAR_LENGTH`, `CONCAT`, `COALESCE`, `IFNULL`, `NULLIF`, `ABS`, `ROUND`, `FLOOR`, `CEIL`, `CAST`.
- `CASE` / `WHEN` / `ELSE` expressions and arithmetic operators.
- `INSERT`, `UPDATE`, `DELETE`.
- `db.InsertStruct` and `vapordb.ScanRows[T]` for struct-based data access via `db` tags.
- `db.Save` / `db.Load` for JSON persistence.
