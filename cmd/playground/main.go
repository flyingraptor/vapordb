package main

import (
	"errors"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"runtime"
	"strings"

	"github.com/flyingraptor/vapordb"
)

// saveFile sits next to main.go so the data travels with the source.
var saveFile = func() string {
	_, file, _, _ := runtime.Caller(0)
	return filepath.Join(filepath.Dir(file), "data.json")
}()

func main() {
	db := vapordb.New()

	// ── load existing state, or seed fresh data on first run ─────────────────
	if err := db.Load(saveFile); err != nil {
		if errors.Is(err, os.ErrNotExist) {
			fmt.Println("no save file found — seeding fresh data")
			seed(db)
		} else {
			log.Fatal(err)
		}
	} else {
		fmt.Printf("loaded existing database from %s\n", saveFile)
	}

	// ── queries ───────────────────────────────────────────────────────────────
	section("Basic SELECT with WHERE + ORDER BY")
	query(db, `SELECT name, age FROM users WHERE age >= 28 ORDER BY age ASC, name ASC`)

	section("Aggregates")
	query(db, `SELECT COUNT(*) AS total_users, AVG(age) AS avg_age, MAX(score) AS top_score FROM users`)

	section("GROUP BY age")
	query(db, `SELECT age, COUNT(*) AS cnt FROM users GROUP BY age ORDER BY age`)

	section("JOIN users → orders (spend per user)")
	query(db, `
		SELECT u.name, COUNT(*) AS orders, SUM(o.amount) AS spent
		FROM users u
		INNER JOIN orders o ON u.id = o.user_id
		GROUP BY u.name
		ORDER BY spent DESC
	`)

	section("LEFT JOIN — users with no orders")
	query(db, `
		SELECT u.name, o.product
		FROM users u
		LEFT JOIN orders o ON u.id = o.user_id
		WHERE o.product IS NULL
	`)

	section("BETWEEN + IN")
	query(db, `SELECT name, age FROM users WHERE age BETWEEN 25 AND 30 AND name IN ('Bob', 'Alice', 'Eve') ORDER BY name`)

	section("LIKE")
	query(db, `SELECT name FROM users WHERE name LIKE '%e%' ORDER BY name`)

	section("Scalar functions")
	query(db, `
		SELECT
			name,
			UPPER(name)                   AS up,
			LENGTH(name)                  AS len,
			COALESCE(score, 0)            AS score,
			ROUND(COALESCE(score, 0), 1)  AS rounded
		FROM users
		ORDER BY name
	`)

	section("CASE expression")
	query(db, `
		SELECT name, score,
			CASE
				WHEN score >= 90 THEN 'A'
				WHEN score >= 70 THEN 'B'
				WHEN score IS NULL THEN 'N/A'
				ELSE 'C'
			END AS grade
		FROM users
		ORDER BY name
	`)

	// ── struct mapping via db.InsertStruct + vapordb.ScanRows ────────────────
	section("Struct mapping (InsertStruct + ScanRows)")

	// Insert via struct — no hand-written SQL needed.
	tmpDB := vapordb.New()

	type Product struct {
		ID    int     `db:"id"`
		Name  string  `db:"name"`
		Price float64 `db:"price"`
	}
	tmpDB.InsertStruct("products", Product{1, "Widget", 9.99})
	tmpDB.InsertStruct("products", Product{2, "Gadget", 24.50})

	// Query back into a typed slice.
	rows, _ := tmpDB.Query(`SELECT id, name, price FROM products ORDER BY id`)
	products := vapordb.ScanRows[Product](rows)
	for _, p := range products {
		fmt.Printf("  Product{ID:%d Name:%-8s Price:%.2f}\n", p.ID, p.Name, p.Price)
	}

	// ScanRows works for any struct — try it with User too.
	userRows, _ := db.Query(`SELECT id, name, age, score FROM users ORDER BY id`)
	users := vapordb.ScanRows[User](userRows)
	for _, u := range users {
		fmt.Printf("  User{ID:%d Name:%-6s Age:%d Score:%.1f}\n", u.ID, u.Name, u.Age, u.Score)
	}

	// ── mutations — try adding your own below ─────────────────────────────────
	section("UPDATE + DELETE")
	exec(db, `UPDATE users SET score = 50.0 WHERE score IS NULL`)
	exec(db, `DELETE FROM orders WHERE amount < 5`)
	query(db, `SELECT name, score FROM users ORDER BY name`)
	query(db, `SELECT id, product, amount FROM orders ORDER BY id`)

	// ── save state for next run ───────────────────────────────────────────────
	if err := db.Save(saveFile); err != nil {
		log.Fatal(err)
	}
	fmt.Printf("\nsaved to %s — run again to load it back\n", saveFile)
}

// seed inserts the initial rows on first run.
func seed(db *vapordb.DB) {
	exec(db, `INSERT INTO users (id, name, age, score) VALUES (1, 'Alice',  30, 88.5)`)
	exec(db, `INSERT INTO users (id, name, age, score) VALUES (2, 'Bob',    25, 72.0)`)
	exec(db, `INSERT INTO users (id, name, age, score) VALUES (3, 'Carol',  28, 95.0)`)
	exec(db, `INSERT INTO users (id, name, age, score) VALUES (4, 'Dave',   35, NULL)`)
	exec(db, `INSERT INTO users (id, name, age, score) VALUES (5, 'Eve',    28, 60.0)`)

	exec(db, `INSERT INTO orders (id, user_id, product, amount) VALUES (1, 1, 'book',   12.99)`)
	exec(db, `INSERT INTO orders (id, user_id, product, amount) VALUES (2, 1, 'pen',     2.50)`)
	exec(db, `INSERT INTO orders (id, user_id, product, amount) VALUES (3, 2, 'laptop', 999.00)`)
	exec(db, `INSERT INTO orders (id, user_id, product, amount) VALUES (4, 3, 'book',   12.99)`)
	exec(db, `INSERT INTO orders (id, user_id, product, amount) VALUES (5, 3, 'phone',  499.00)`)
	exec(db, `INSERT INTO orders (id, user_id, product, amount) VALUES (6, 5, 'pen',     2.50)`)
}

// ── helpers ───────────────────────────────────────────────────────────────────

func exec(db *vapordb.DB, sql string) {
	if err := db.Exec(strings.TrimSpace(sql)); err != nil {
		log.Fatalf("exec error: %v\nsql: %s", err, sql)
	}
}

func query(db *vapordb.DB, sql string) {
	rows, err := db.Query(strings.TrimSpace(sql))
	if err != nil {
		log.Fatalf("query error: %v\nsql: %s", err, sql)
	}
	if len(rows) == 0 {
		fmt.Println("  (no rows)")
		return
	}

	// column order: preserve insertion order via first row scan.
	// Since Row is a map we collect keys and sort for stable output.
	keys := orderedKeys(rows)
	header := strings.Join(keys, "\t")
	fmt.Println(" ", header)
	fmt.Println(" ", strings.Repeat("-", len(header)+len(keys)*4))
	for _, r := range rows {
		parts := make([]string, len(keys))
		for i, k := range keys {
			v := r[k]
			if v.Kind == vapordb.KindNull {
				parts[i] = "NULL"
			} else {
				parts[i] = fmt.Sprintf("%v", v.V)
			}
		}
		fmt.Println(" ", strings.Join(parts, "\t"))
	}
	fmt.Println()
}

func section(title string) {
	fmt.Printf("\n╔═  %s\n", title)
}

func orderedKeys(rows []vapordb.Row) []string {
	seen := map[string]bool{}
	var keys []string
	for _, r := range rows {
		for k := range r {
			if !seen[k] {
				seen[k] = true
				keys = append(keys, k)
			}
		}
	}
	// stable sort so output doesn't change between runs
	for i := 1; i < len(keys); i++ {
		for j := i; j > 0 && keys[j] < keys[j-1]; j-- {
			keys[j], keys[j-1] = keys[j-1], keys[j]
		}
	}
	return keys
}
