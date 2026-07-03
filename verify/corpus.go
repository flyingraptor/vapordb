// Package verify holds internal, Docker-backed validation for vapordb. It is a
// separate Go module (github.com/flyingraptor/vapordb/verify) so the heavy
// testcontainers and database-driver dependencies never leak into the core
// module's go.mod.
//
// The goal is portability-level validation only: it proves that the DDL emitted
// by vapordb's GenerateDDL, plus a curated corpus of the SQL vapordb accepts,
// actually parse and execute on real PostgreSQL and MySQL. It deliberately does
// NOT assert semantic result parity — only "does it run".
//
// The corpus is layered:
//
//   - sharedSeed / sharedReads / sharedWrites — statements that vapordb accepts
//     AND that are portable to BOTH PostgreSQL and MySQL. These are the strongest
//     cross-check: vapordb parses them (asserted Docker-free in
//     TestVapordbAcceptsSharedCorpus) and both real engines run them.
//   - postgresCorpus / mysqlCorpus — statements exercising features that are
//     specific to one dialect (JSON operators, FULL OUTER JOIN, ILIKE,
//     RETURNING, DATE_FORMAT, backtick identifiers, …), run only against the
//     matching real database.
package verify

import "github.com/flyingraptor/vapordb"

// buildCorpusDB constructs an in-memory vapordb database whose inferred schema
// exercises every Value Kind (int, float, bool, string, date, JSON) plus an
// enum-constrained column, across three tables that can be joined in a chain
// (orders → users → regions). The resulting schema is what GenerateDDL is asked
// to emit for the real databases.
func buildCorpusDB() (*vapordb.DB, error) {
	db := vapordb.New()

	// tier is enum-constrained: MySQL renders it as ENUM(...), Postgres as
	// TEXT with a CHECK (... IN (...)) constraint.
	db.DeclareEnum("users", "tier", "bronze", "silver", "gold")

	// One representative row per table is enough to infer the full schema.
	// active → KindBool, age/id/region_id → KindInt, score/amount → KindFloat,
	// name/note/tier → KindString, created_at → KindDate, prefs → KindJSON.
	seed := []string{
		`INSERT INTO regions (id, name) VALUES (1, 'North')`,
		`INSERT INTO users (id, name, age, score, active, created_at, prefs, tier, region_id)
			VALUES (1, 'Alice', 30, 9.5, true, DATE('2024-01-15'), json_parse('{"theme":"dark"}'), 'gold', 1)`,
		`INSERT INTO orders (id, user_id, amount, note)
			VALUES (1, 1, 19.99, 'first order')`,
	}
	for _, s := range seed {
		if err := db.Exec(s); err != nil {
			return nil, err
		}
	}
	return db, nil
}

// tableNames lists the tables buildCorpusDB creates, in a drop-safe order (no
// foreign keys exist, but keep dependents first for clarity).
func tableNames() []string { return []string{"orders", "users", "regions"} }

// sharedSeed inserts rows the shared read/write corpus and the per-dialect
// corpora operate on. All literals are ANSI-portable: boolean `true` (accepted
// by Postgres BOOLEAN and by MySQL as 1), a bare JSON string literal (coerces
// into JSONB on Postgres, validates against a JSON column on MySQL), and a
// datetime string literal (coerces into TIMESTAMP / DATETIME).
func sharedSeed() []string {
	return []string{
		`INSERT INTO regions (id, name) VALUES (10, 'East')`,
		`INSERT INTO regions (id, name) VALUES (20, 'West')`,
		`INSERT INTO users (id, name, age, score, active, created_at, prefs, tier, region_id)
			VALUES (100, 'Zoe', 41, 7.25, true, '2024-02-02 10:00:00', '{"theme":"light"}', 'silver', 10)`,
		`INSERT INTO users (id, name, age, score, active, created_at, prefs, tier, region_id)
			VALUES (200, 'Yan', 17, 3.5, false, '2024-03-03 08:30:00', '{"theme":"auto"}', 'bronze', 20)`,
		`INSERT INTO users (id, name, age, score, active, created_at, prefs, tier, region_id)
			VALUES (300, 'Xena', 52, 8.0, true, '2024-04-04 12:00:00', '{"theme":"dark"}', 'gold', 10)`,
		`INSERT INTO orders (id, user_id, amount, note) VALUES (100, 100, 42.50, 'second order')`,
		`INSERT INTO orders (id, user_id, amount, note) VALUES (200, 100, 5.00, 'third order')`,
		`INSERT INTO orders (id, user_id, amount, note) VALUES (300, 300, 100.00, '')`,
		`INSERT INTO orders (id, user_id, amount, note) VALUES (400, 200, 12.00, NULL)`,
	}
}

// vapordbSeed mirrors sharedSeed's rows but uses vapordb-native wrappers for the
// two specially-typed columns: json_parse(...) so prefs is inferred as KindJSON,
// and DATE(...) / DATETIME(...) so created_at is inferred as KindDate. Real
// PostgreSQL and MySQL have no such functions and instead coerce plain string
// literals into their JSON / timestamp columns (see sharedSeed), so the seed
// INSERTs necessarily differ for these columns only. Everything else — the
// complex reads and the writes — is genuinely shared. Used exclusively by the
// Docker-free TestVapordbAcceptsSharedCorpus.
func vapordbSeed() []string {
	return []string{
		`INSERT INTO regions (id, name) VALUES (10, 'East')`,
		`INSERT INTO regions (id, name) VALUES (20, 'West')`,
		`INSERT INTO users (id, name, age, score, active, created_at, prefs, tier, region_id)
			VALUES (100, 'Zoe', 41, 7.25, true, DATETIME('2024-02-02 10:00:00'), json_parse('{"theme":"light"}'), 'silver', 10)`,
		`INSERT INTO users (id, name, age, score, active, created_at, prefs, tier, region_id)
			VALUES (200, 'Yan', 17, 3.5, false, DATETIME('2024-03-03 08:30:00'), json_parse('{"theme":"auto"}'), 'bronze', 20)`,
		`INSERT INTO users (id, name, age, score, active, created_at, prefs, tier, region_id)
			VALUES (300, 'Xena', 52, 8.0, true, DATETIME('2024-04-04 12:00:00'), json_parse('{"theme":"dark"}'), 'gold', 10)`,
		`INSERT INTO orders (id, user_id, amount, note) VALUES (100, 100, 42.50, 'second order')`,
		`INSERT INTO orders (id, user_id, amount, note) VALUES (200, 100, 5.00, 'third order')`,
		`INSERT INTO orders (id, user_id, amount, note) VALUES (300, 300, 100.00, '')`,
		`INSERT INTO orders (id, user_id, amount, note) VALUES (400, 200, 12.00, NULL)`,
	}
}

// sharedReads is the heart of the coverage: complex SELECTs that vapordb accepts
// and that run unchanged on both PostgreSQL 16 and MySQL 8. It covers joins
// (INNER / LEFT / RIGHT, multi-table chain), aggregates + GROUP BY / HAVING,
// DISTINCT and COUNT(DISTINCT), CASE, COALESCE / NULLIF, BETWEEN / IN / LIKE,
// scalar + correlated subqueries, EXISTS / NOT EXISTS, IN / NOT IN (subquery),
// derived tables, CTEs (single + chained), UNION / UNION ALL, and window
// functions (ranking, offset, aggregate, explicit ROWS frame, whole-partition).
func sharedReads() []string {
	return []string{
		// filtering / ordering / limit-offset
		`SELECT id, name, age FROM users WHERE age >= 18 ORDER BY id`,
		`SELECT id FROM users WHERE age BETWEEN 18 AND 65 AND tier IN ('gold', 'silver') ORDER BY id`,
		`SELECT id FROM users WHERE name LIKE '%a%' ORDER BY id`,
		`SELECT id FROM users ORDER BY id LIMIT 2 OFFSET 1`,
		`SELECT id, tier, score FROM users ORDER BY tier ASC, score DESC`,

		// aggregates / grouping
		`SELECT count(*) AS c, avg(score) AS a, min(age) AS mn, max(age) AS mx, sum(score) AS s FROM users`,
		`SELECT tier, count(*) AS n FROM users GROUP BY tier HAVING count(*) >= 1 ORDER BY tier`,
		`SELECT count(DISTINCT tier) AS distinct_tiers FROM users`,
		`SELECT DISTINCT tier FROM users ORDER BY tier`,

		// joins
		`SELECT u.name, o.amount FROM users u INNER JOIN orders o ON u.id = o.user_id ORDER BY o.id`,
		`SELECT u.name, o.amount FROM users u LEFT JOIN orders o ON o.user_id = u.id ORDER BY u.id, o.id`,
		`SELECT u.name, o.amount FROM users u RIGHT JOIN orders o ON o.user_id = u.id ORDER BY o.id`,
		`SELECT u.name, r.name AS region, o.amount
			FROM users u
			INNER JOIN regions r ON r.id = u.region_id
			INNER JOIN orders o ON o.user_id = u.id
			ORDER BY o.id`,
		`SELECT u.tier, sum(o.amount) AS spent
			FROM users u INNER JOIN orders o ON o.user_id = u.id
			GROUP BY u.tier ORDER BY u.tier`,

		// expressions
		`SELECT id, CASE WHEN score >= 8 THEN 'high' WHEN score >= 5 THEN 'mid' ELSE 'low' END AS band FROM users ORDER BY id`,
		`SELECT id, COALESCE(NULLIF(note, ''), 'none') AS n FROM orders ORDER BY id`,
		// round(v, s) with a scale needs numeric input on Postgres (its
		// two-arg round is not defined for double precision), so cast first —
		// CAST(x AS DECIMAL(p,s)) is portable to both engines and vapordb.
		`SELECT id, upper(name) AS un, lower(name) AS ln, char_length(name) AS len,
			round(CAST(score AS DECIMAL(10, 2)), 1) AS r, floor(score) AS f, ceil(score) AS c, abs(score) AS a
			FROM users ORDER BY id`,
		`SELECT concat(name, '-', tier) AS label FROM users ORDER BY id`,

		// subqueries
		`SELECT u.name, (SELECT count(*) FROM orders o WHERE o.user_id = u.id) AS cnt FROM users u ORDER BY u.id`,
		`SELECT name FROM users u WHERE (SELECT count(*) FROM orders o WHERE o.user_id = u.id) > 0 ORDER BY id`,
		`SELECT name FROM users u WHERE EXISTS (SELECT 1 FROM orders o WHERE o.user_id = u.id) ORDER BY id`,
		`SELECT name FROM users u WHERE NOT EXISTS (SELECT 1 FROM orders o WHERE o.user_id = u.id) ORDER BY id`,
		`SELECT name FROM users WHERE id IN (SELECT user_id FROM orders) ORDER BY id`,
		`SELECT name FROM users WHERE id NOT IN (SELECT user_id FROM orders WHERE amount > 50) ORDER BY id`,

		// derived tables
		`SELECT sub.tier, sub.total FROM (SELECT tier, sum(score) AS total FROM users GROUP BY tier) AS sub WHERE sub.total > 0 ORDER BY sub.tier`,
		`SELECT u.name, agg.total
			FROM users u
			INNER JOIN (SELECT user_id, sum(amount) AS total FROM orders GROUP BY user_id) AS agg
			ON agg.user_id = u.id
			ORDER BY agg.total DESC`,

		// CTEs
		`WITH active_users AS (SELECT id, name FROM users WHERE active = true) SELECT name FROM active_users ORDER BY id`,
		`WITH totals AS (SELECT user_id, sum(amount) AS t FROM orders GROUP BY user_id),
			big AS (SELECT user_id FROM totals WHERE t >= 40)
			SELECT u.name FROM users u INNER JOIN big b ON b.user_id = u.id ORDER BY u.id`,

		// set operations
		`SELECT id FROM users WHERE tier = 'gold' UNION SELECT user_id FROM orders ORDER BY id`,
		`SELECT id FROM users UNION ALL SELECT user_id FROM orders ORDER BY id LIMIT 10`,

		// window functions
		`SELECT id, tier, score, ROW_NUMBER() OVER (PARTITION BY tier ORDER BY score DESC) AS rn FROM users ORDER BY tier, rn`,
		`SELECT id, score, RANK() OVER (ORDER BY score DESC) AS rnk, DENSE_RANK() OVER (ORDER BY score DESC) AS drnk FROM users ORDER BY rnk, id`,
		`SELECT id, amount, sum(amount) OVER (ORDER BY id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running FROM orders ORDER BY id`,
		`SELECT id, score, LAG(score) OVER (ORDER BY id) AS prev, LEAD(score) OVER (ORDER BY id) AS nxt FROM users ORDER BY id`,
		`SELECT id, count(*) OVER () AS total, avg(score) OVER (PARTITION BY tier) AS tier_avg FROM users ORDER BY id`,
	}
}

// sharedWrites mutates the seeded data and must run last (after sharedReads and
// the per-dialect corpora, which read the seeded rows). Portable to both.
func sharedWrites() []string {
	return []string{
		`UPDATE users SET age = age + 1 WHERE id = 100`,
		`UPDATE users SET score = score * 2 WHERE tier = 'bronze'`,
		`UPDATE orders SET note = 'backfilled' WHERE note IS NULL`,
		`DELETE FROM orders WHERE amount < 6`,
		`DELETE FROM users WHERE id = 200`,
	}
}

// postgresCorpus exercises PostgreSQL-specific features vapordb supports or
// emits: FULL OUTER JOIN, ILIKE, RETURNING, the :: cast, JSONB arrow (->>) and
// containment (@>) operators, EXTRACT, and NULLS LAST ordering. Run only against
// real Postgres, after sharedSeed + sharedReads and before sharedWrites.
func postgresCorpus() []string {
	return []string{
		`SELECT u.id AS uid, o.id AS oid FROM users u FULL OUTER JOIN orders o ON o.user_id = u.id ORDER BY uid, oid`,
		`SELECT id FROM users WHERE name ILIKE '%a%' ORDER BY id`,
		`INSERT INTO regions (id, name) VALUES (99, 'Central') RETURNING id, name`,
		`SELECT id, prefs->>'theme' AS theme FROM users WHERE prefs->>'theme' = 'dark' ORDER BY id`,
		`SELECT id FROM users WHERE prefs @> '{"theme":"dark"}' ORDER BY id`,
		`SELECT id, score::integer AS si FROM users ORDER BY id`,
		`SELECT id, EXTRACT(YEAR FROM created_at) AS yr FROM users ORDER BY id`,
		`SELECT id, note FROM orders ORDER BY note NULLS LAST, id`,
	}
}

// mysqlCorpus exercises MySQL-specific features vapordb supports or emits:
// backtick identifiers, IFNULL, JSON path arrow (->> / JSON_EXTRACT / JSON_CONTAINS),
// DATE_FORMAT, YEAR(), and the LIMIT offset, count form. Run only against real
// MySQL, after sharedSeed + sharedReads and before sharedWrites.
func mysqlCorpus() []string {
	return []string{
		"SELECT `id`, `name` FROM `users` ORDER BY `id`",
		`SELECT id, IFNULL(note, 'none') AS n FROM orders ORDER BY id`,
		`SELECT id, prefs->>'$.theme' AS theme FROM users ORDER BY id`,
		`SELECT id, JSON_EXTRACT(prefs, '$.theme') AS theme FROM users ORDER BY id`,
		`SELECT id FROM users WHERE JSON_CONTAINS(prefs, '"dark"', '$.theme') ORDER BY id`,
		`SELECT id, DATE_FORMAT(created_at, '%Y-%m') AS ym FROM users ORDER BY id`,
		`SELECT id, YEAR(created_at) AS yr FROM users ORDER BY id`,
		`SELECT id FROM users ORDER BY id LIMIT 1, 2`,
	}
}
