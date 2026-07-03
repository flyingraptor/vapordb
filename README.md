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

![vapordb architecture](vapordb_diagram.png)

## Documentation

- **[FEATURES.md](FEATURES.md)** — the full feature list, struct mapping, named parameters, persistence, target-database declaration, and the complete SQL reference with examples.
- **[LIMITATIONS.md](LIMITATIONS.md)** — known limitations and remaining roadmap items.
- **[CHANGELOG.md](CHANGELOG.md)** — release history and the completed roadmap.
- **[verify/README.md](verify/README.md)** — how vapordb's generated DDL and SQL corpus are validated against real PostgreSQL and MySQL.

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

Schema is inferred from your first INSERT and evolves automatically as new
columns or wider types arrive — there is no `CREATE TABLE` and no migration. See
[FEATURES.md](FEATURES.md) for the full capabilities.

## Core API

| Method | Description |
|--------|-------------|
| `vapordb.New()` | Create a new empty database |
| `vapordb.New(vapordb.WithForceWipeOnSchemaConflict(true))` | Opt in to the legacy behaviour of wiping a table’s rows when an INSERT would change an existing column’s type family (default is to return an error). Per-call overrides use `vapordb.WithWriteForceWipeOnSchemaConflict` on `Query`, `Exec`, `QueryNamed`, and `ExecNamed`. |
| `db.Exec(sql)` | Run INSERT, UPDATE, or DELETE |
| `db.Query(sql)` | Run SELECT, returns `[]Row` |
| `db.Save(path)` | Persist the database to a JSON file |
| `db.Load(path)` | Load a previously saved JSON file |
| `db.SaveTo(w)` | Persist the database as JSON to any `io.Writer` |
| `db.LoadFrom(r)` | Load the database from JSON on any `io.Reader` |
| `db.InsertStruct(table, v)` | Insert a struct using `db` field tags |
| `vapordb.ScanRows[T](rows)` | Scan `[]Row` into a typed slice |
| `db.QueryNamed(sql, params)` | SELECT with named `:param` placeholders |
| `db.ExecNamed(sql, params)` | INSERT/UPDATE/DELETE with named `:param` placeholders |
| `db.DeclareEnum(table, col, vals...)` | Restrict a column to a fixed set of string values |
| `db.LockSchema()` | Freeze every table's schema at once |
| `db.UnlockSchema()` | Thaw every table's schema at once |
| `db.LockTable(name)` | Freeze a single table's schema |
| `db.UnlockTable(name)` | Thaw a single table's schema |

A `Row` is `map[string]Value`. Each `Value` has:
- `.V` is the underlying Go value (`int64`, `float64`, `string`, `bool`, `time.Time`, JSON as `map[string]any` / `[]any`, or `nil`)
- `.Kind` is one of `KindNull`, `KindBool`, `KindInt`, `KindFloat`, `KindString`, `KindDate`, `KindJSON`

## Declaring a target database (optional)

If you already know which real database you'll migrate to, declare it up front
and vapordb will flag SQL that would not port cleanly — a warn-only lint that
never changes execution or results. The default (`TargetGeneric`) does no
checking, so this is fully opt-in.

```go
db := vapordb.New(vapordb.WithTarget(vapordb.TargetPostgres)) // or TargetMySQL

db.Exec(`SELECT * FROM users WHERE id = ANY(1, 2, 3)`)
for _, w := range db.PortabilityWarnings() {
    fmt.Println(w.Message)
}
```

With a target set, `db.GenerateDDL("")` also uses the target's dialect. See
[FEATURES.md](FEATURES.md#declaring-a-target-database) for the full list of
flagged constructs and live-warning callbacks.
