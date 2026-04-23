# vapordb

In-memory SQL database for fast prototyping in Go - no setup, no schema, just queries

When building something new, the data model changes constantly. With a real database every field addition, rename, or type change requires a migration script, an ALTER TABLE, and a re-run of your seed data. That friction compounds quickly and slows you down at exactly the stage where you need to move fast.

vapordb removes that entirely. You just write code. Change a struct, add a column to an INSERT, and the schema updates itself. There is nothing to migrate, nothing to roll back, and no mismatch between your code and your database to debug. You stay focused on the logic and the shape of the data rather than the mechanics of keeping a schema in sync.

Once the data model stabilises and you are ready to commit to a real database, you write the CREATE TABLE and migrations once, with full knowledge of what you actually need. The result is a cleaner schema with fewer columns you will never use and fewer constraints you will need to undo later.

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
- **Optional persistence.** Save the entire database to a JSON file and reload it later.
- **Struct mapping.** Insert from structs and scan results back into typed slices via `db` tags.

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

A `Row` is `map[string]Value`. Each `Value` has:
- `.V` is the underlying Go value (`int64`, `float64`, `string`, `bool`, or `nil`)
- `.Kind` is one of `KindNull`, `KindBool`, `KindInt`, `KindFloat`, `KindString`

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
