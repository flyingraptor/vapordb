package vapordb

// Tests for JOIN semantics and FK-style relational queries.
// No FK constraints exist in vapordb — relationships are resolved at query
// time with JOIN conditions, exactly as described in PROMPT.md.

import "testing"

// ─── fixtures ────────────────────────────────────────────────────────────────

// blogDB builds a small blog dataset:
//
//	authors  (id, name, country)
//	posts    (id, author_id, title, views)
//	comments (id, post_id, body)
//	tags     (id, name)
//	post_tags (post_id, tag_id)   ← junction table (many-to-many)
func blogDB(t *testing.T) *DB {
	t.Helper()
	db := New()

	// authors
	mustExec(t, db, `INSERT INTO authors (id, name, country) VALUES (1, 'Alice', 'US')`)
	mustExec(t, db, `INSERT INTO authors (id, name, country) VALUES (2, 'Bob',   'UK')`)
	mustExec(t, db, `INSERT INTO authors (id, name, country) VALUES (3, 'Carol', 'US')`)

	// posts — Carol has no posts (tests LEFT JOIN "missing FK")
	mustExec(t, db, `INSERT INTO posts (id, author_id, title, views) VALUES (10, 1, 'Go Tips',    1000)`)
	mustExec(t, db, `INSERT INTO posts (id, author_id, title, views) VALUES (11, 1, 'SQL Tricks', 500)`)
	mustExec(t, db, `INSERT INTO posts (id, author_id, title, views) VALUES (12, 2, 'UK Weather', 200)`)

	// comments — post 12 has no comments
	mustExec(t, db, `INSERT INTO comments (id, post_id, body) VALUES (100, 10, 'Great post!')`)
	mustExec(t, db, `INSERT INTO comments (id, post_id, body) VALUES (101, 10, 'Very helpful')`)
	mustExec(t, db, `INSERT INTO comments (id, post_id, body) VALUES (102, 11, 'Nice')`)

	// tags
	mustExec(t, db, `INSERT INTO tags (id, name) VALUES (1, 'go')`)
	mustExec(t, db, `INSERT INTO tags (id, name) VALUES (2, 'sql')`)
	mustExec(t, db, `INSERT INTO tags (id, name) VALUES (3, 'weather')`)

	// post_tags (junction)
	mustExec(t, db, `INSERT INTO post_tags (post_id, tag_id) VALUES (10, 1)`)
	mustExec(t, db, `INSERT INTO post_tags (post_id, tag_id) VALUES (10, 2)`)
	mustExec(t, db, `INSERT INTO post_tags (post_id, tag_id) VALUES (11, 2)`)
	mustExec(t, db, `INSERT INTO post_tags (post_id, tag_id) VALUES (12, 3)`)

	return db
}

// ─── basic FK-style join ──────────────────────────────────────────────────────

// Verify every post is joined to its author via author_id = authors.id.
func TestJoinPostToAuthor(t *testing.T) {
	db := blogDB(t)

	rows := mustQuery(t, db, `
		SELECT a.name, p.title
		FROM authors a
		JOIN posts p ON a.id = p.author_id
		ORDER BY p.id ASC
	`)
	if len(rows) != 3 {
		t.Fatalf("expected 3 (post, author) pairs, got %d: %v", len(rows), rows)
	}
	want := []struct{ name, title string }{
		{"Alice", "Go Tips"},
		{"Alice", "SQL Tricks"},
		{"Bob", "UK Weather"},
	}
	for i, w := range want {
		if rows[i]["name"] != strVal(w.name) || rows[i]["title"] != strVal(w.title) {
			t.Errorf("row %d: want {%s, %s}, got %v", i, w.name, w.title, rows[i])
		}
	}
}

// ─── three-table JOIN chain ───────────────────────────────────────────────────

// comments → posts → authors: find the author name for every comment.
func TestThreeTableJoinChain(t *testing.T) {
	db := blogDB(t)

	rows := mustQuery(t, db, `
		SELECT a.name, c.body
		FROM comments c
		JOIN posts    p ON c.post_id  = p.id
		JOIN authors  a ON p.author_id = a.id
		ORDER BY c.id ASC
	`)
	if len(rows) != 3 {
		t.Fatalf("expected 3 comment rows, got %d: %v", len(rows), rows)
	}
	// All three comments belong to Alice's posts (10 and 11).
	for _, r := range rows {
		if r["name"] != strVal("Alice") {
			t.Errorf("expected author Alice, got %v", r["name"])
		}
	}
}

// ─── LEFT JOIN — author with no posts ────────────────────────────────────────

// Carol has no posts; she should still appear with NULL post fields.
func TestLeftJoinAuthorNoPosts(t *testing.T) {
	db := blogDB(t)

	rows := mustQuery(t, db, `
		SELECT a.name, p.title
		FROM authors a
		LEFT JOIN posts p ON a.id = p.author_id
		ORDER BY a.id ASC, p.id ASC
	`)
	// Alice × 2, Bob × 1, Carol × 1 (null)
	if len(rows) != 4 {
		t.Fatalf("expected 4 rows, got %d: %v", len(rows), rows)
	}
	carolRow := rows[3]
	if carolRow["name"] != strVal("Carol") {
		t.Errorf("expected Carol in last row, got %v", carolRow["name"])
	}
	if carolRow["title"].Kind != KindNull {
		t.Errorf("expected NULL title for Carol, got %v", carolRow["title"])
	}
}

// ─── LEFT JOIN + IS NULL — find authors with no posts ────────────────────────

// The classic "anti-join" pattern: LEFT JOIN + WHERE right.pk IS NULL.
func TestLeftJoinIsNullAntijoin(t *testing.T) {
	db := blogDB(t)

	rows := mustQuery(t, db, `
		SELECT a.name
		FROM authors a
		LEFT JOIN posts p ON a.id = p.author_id
		WHERE p.id IS NULL
	`)
	if len(rows) != 1 {
		t.Fatalf("expected 1 author with no posts, got %d: %v", len(rows), rows)
	}
	if rows[0]["name"] != strVal("Carol") {
		t.Errorf("expected Carol, got %v", rows[0]["name"])
	}
}

// ─── JOIN + GROUP BY + COUNT ──────────────────────────────────────────────────

// Count posts per author.
func TestJoinGroupByCount(t *testing.T) {
	db := blogDB(t)

	rows := mustQuery(t, db, `
		SELECT a.name, COUNT(*) AS post_count
		FROM authors a
		JOIN posts p ON a.id = p.author_id
		GROUP BY a.name
		ORDER BY a.name ASC
	`)
	if len(rows) != 2 {
		t.Fatalf("expected 2 groups (Alice, Bob), got %d: %v", len(rows), rows)
	}
	if rows[0]["name"] != strVal("Alice") || rows[0]["post_count"] != intVal(2) {
		t.Errorf("Alice: expected post_count=2, got %v", rows[0])
	}
	if rows[1]["name"] != strVal("Bob") || rows[1]["post_count"] != intVal(1) {
		t.Errorf("Bob: expected post_count=1, got %v", rows[1])
	}
}

// ─── JOIN + GROUP BY + SUM ────────────────────────────────────────────────────

// Total views per author.
func TestJoinGroupBySum(t *testing.T) {
	db := blogDB(t)

	rows := mustQuery(t, db, `
		SELECT a.name, SUM(p.views) AS total_views
		FROM authors a
		JOIN posts p ON a.id = p.author_id
		GROUP BY a.name
		ORDER BY total_views DESC
	`)
	if len(rows) != 2 {
		t.Fatalf("expected 2 rows, got %d", len(rows))
	}
	// Alice: 1000+500=1500, Bob: 200
	if rows[0]["name"] != strVal("Alice") || rows[0]["total_views"] != intVal(1500) {
		t.Errorf("row 0: want Alice/1500, got %v", rows[0])
	}
	if rows[1]["name"] != strVal("Bob") || rows[1]["total_views"] != intVal(200) {
		t.Errorf("row 1: want Bob/200, got %v", rows[1])
	}
}

// ─── JOIN + GROUP BY + HAVING ─────────────────────────────────────────────────

// Authors with more than one post.
func TestJoinGroupByHaving(t *testing.T) {
	db := blogDB(t)

	rows := mustQuery(t, db, `
		SELECT a.name, COUNT(*) AS cnt
		FROM authors a
		JOIN posts p ON a.id = p.author_id
		GROUP BY a.name
		HAVING COUNT(*) > 1
	`)
	if len(rows) != 1 {
		t.Fatalf("expected 1 author with >1 post, got %d: %v", len(rows), rows)
	}
	if rows[0]["name"] != strVal("Alice") {
		t.Errorf("expected Alice, got %v", rows[0]["name"])
	}
}

// ─── many-to-many via junction table ─────────────────────────────────────────

// Find every tag name attached to post 10.
func TestManyToManyJunctionJoin(t *testing.T) {
	db := blogDB(t)

	rows := mustQuery(t, db, `
		SELECT t.name
		FROM post_tags pt
		JOIN tags t ON pt.tag_id = t.id
		WHERE pt.post_id = 10
		ORDER BY t.name ASC
	`)
	if len(rows) != 2 {
		t.Fatalf("expected 2 tags for post 10, got %d: %v", len(rows), rows)
	}
	if rows[0]["name"] != strVal("go") || rows[1]["name"] != strVal("sql") {
		t.Errorf("unexpected tags: %v", rows)
	}
}

// Find all posts that carry the "sql" tag, with author name.
func TestManyToManyThreeTableJoin(t *testing.T) {
	db := blogDB(t)

	rows := mustQuery(t, db, `
		SELECT a.name, p.title, tg.name AS tag
		FROM posts p
		JOIN authors  a  ON p.author_id = a.id
		JOIN post_tags pt ON pt.post_id  = p.id
		JOIN tags     tg ON tg.id        = pt.tag_id
		WHERE tg.name = 'sql'
		ORDER BY p.id ASC
	`)
	if len(rows) != 2 {
		t.Fatalf("expected 2 sql-tagged posts, got %d: %v", len(rows), rows)
	}
	if rows[0]["title"] != strVal("Go Tips") || rows[1]["title"] != strVal("SQL Tricks") {
		t.Errorf("unexpected titles: %v", rows)
	}
}

// ─── self-join (hierarchy / reporting) ───────────────────────────────────────

// employees (id, name, manager_id) — classic self-referential FK pattern.
func TestSelfJoin(t *testing.T) {
	db := New()
	mustExec(t, db, `INSERT INTO employees (id, name, manager_id) VALUES (1, 'CEO',   0)`)
	mustExec(t, db, `INSERT INTO employees (id, name, manager_id) VALUES (2, 'Alice', 1)`)
	mustExec(t, db, `INSERT INTO employees (id, name, manager_id) VALUES (3, 'Bob',   1)`)
	mustExec(t, db, `INSERT INTO employees (id, name, manager_id) VALUES (4, 'Carol', 2)`)

	rows := mustQuery(t, db, `
		SELECT e.name AS employee, m.name AS manager
		FROM employees e
		JOIN employees m ON e.manager_id = m.id
		ORDER BY e.id ASC
	`)
	// CEO (manager_id=0) has no match → 3 rows for Alice, Bob, Carol
	if len(rows) != 3 {
		t.Fatalf("expected 3 rows, got %d: %v", len(rows), rows)
	}
	wantMgr := map[string]string{
		"Alice": "CEO",
		"Bob":   "CEO",
		"Carol": "Alice",
	}
	for _, r := range rows {
		emp, _ := r["employee"].V.(string)
		mgr, _ := r["manager"].V.(string)
		if wantMgr[emp] != mgr {
			t.Errorf("employee %s: expected manager %s, got %s", emp, wantMgr[emp], mgr)
		}
	}
}

// ─── JOIN with non-equality condition ────────────────────────────────────────

// Find all (author, post) pairs where the post has more views than
// ANY post the author made — effectively a cross-product filtered by >.
// Simpler version: find posts with views above the author's minimum post.
func TestJoinNonEqualityCondition(t *testing.T) {
	db := New()
	mustExec(t, db, `INSERT INTO authors (id, name) VALUES (1, 'Alice')`)
	mustExec(t, db, `INSERT INTO posts (id, author_id, views) VALUES (1, 1, 100)`)
	mustExec(t, db, `INSERT INTO posts (id, author_id, views) VALUES (2, 1, 200)`)
	mustExec(t, db, `INSERT INTO posts (id, author_id, views) VALUES (3, 1, 300)`)

	// Self-join posts: find pairs (a, b) where a.views < b.views (same author)
	rows := mustQuery(t, db, `
		SELECT p1.id AS low_id, p2.id AS high_id
		FROM posts p1
		JOIN posts p2 ON p1.author_id = p2.author_id AND p1.views < p2.views
		ORDER BY p1.id ASC, p2.id ASC
	`)
	// (1,2),(1,3),(2,3) → 3 pairs
	if len(rows) != 3 {
		t.Fatalf("expected 3 view-ordering pairs, got %d: %v", len(rows), rows)
	}
}

// ─── implicit (comma) join ────────────────────────────────────────────────────

// FROM a, b WHERE a.id = b.fk is equivalent to INNER JOIN ON.
func TestImplicitCommaJoin(t *testing.T) {
	db := New()
	mustExec(t, db, `INSERT INTO users (id, name) VALUES (1, 'Alice')`)
	mustExec(t, db, `INSERT INTO users (id, name) VALUES (2, 'Bob')`)
	mustExec(t, db, `INSERT INTO orders (id, user_id, total) VALUES (10, 1, 50)`)
	mustExec(t, db, `INSERT INTO orders (id, user_id, total) VALUES (11, 2, 75)`)

	rows := mustQuery(t, db, `
		SELECT users.name, orders.total
		FROM users, orders
		WHERE users.id = orders.user_id
		ORDER BY orders.total ASC
	`)
	if len(rows) != 2 {
		t.Fatalf("expected 2 rows, got %d: %v", len(rows), rows)
	}
	if rows[0]["name"] != strVal("Alice") || rows[0]["total"] != intVal(50) {
		t.Errorf("row 0: want Alice/50, got %v", rows[0])
	}
	if rows[1]["name"] != strVal("Bob") || rows[1]["total"] != intVal(75) {
		t.Errorf("row 1: want Bob/75, got %v", rows[1])
	}
}

// ─── JOIN with LIMIT ─────────────────────────────────────────────────────────

func TestJoinWithLimit(t *testing.T) {
	db := blogDB(t)

	rows := mustQuery(t, db, `
		SELECT a.name, p.title
		FROM authors a
		JOIN posts p ON a.id = p.author_id
		ORDER BY p.views DESC
		LIMIT 2
	`)
	if len(rows) != 2 {
		t.Fatalf("expected 2 rows, got %d", len(rows))
	}
	// Top 2 by views: Go Tips (1000), SQL Tricks (500)
	if rows[0]["title"] != strVal("Go Tips") {
		t.Errorf("expected Go Tips first, got %v", rows[0]["title"])
	}
}

// ─── one-to-many: comments per post ──────────────────────────────────────────

func TestOneToManyCommentCount(t *testing.T) {
	db := blogDB(t)

	rows := mustQuery(t, db, `
		SELECT p.title, COUNT(*) AS comment_count
		FROM posts p
		JOIN comments c ON c.post_id = p.id
		GROUP BY p.title
		ORDER BY comment_count DESC
	`)
	// post 12 (UK Weather) has 0 comments → excluded from INNER JOIN
	if len(rows) != 2 {
		t.Fatalf("expected 2 posts with comments, got %d: %v", len(rows), rows)
	}
	if rows[0]["title"] != strVal("Go Tips") || rows[0]["comment_count"] != intVal(2) {
		t.Errorf("expected Go Tips / 2 comments, got %v", rows[0])
	}
	if rows[1]["title"] != strVal("SQL Tricks") || rows[1]["comment_count"] != intVal(1) {
		t.Errorf("expected SQL Tricks / 1 comment, got %v", rows[1])
	}
}

// ─── LEFT JOIN to include posts with zero comments ────────────────────────────

func TestLeftJoinZeroComments(t *testing.T) {
	db := blogDB(t)

	rows := mustQuery(t, db, `
		SELECT p.title, COUNT(*) AS comment_count
		FROM posts p
		LEFT JOIN comments c ON c.post_id = p.id
		GROUP BY p.title
		ORDER BY p.id ASC
	`)
	// All 3 posts should appear; UK Weather gets COUNT(*) of 1 because the
	// NULL right row still counts as a group member for COUNT(*).
	if len(rows) != 3 {
		t.Fatalf("expected 3 rows (all posts), got %d: %v", len(rows), rows)
	}
}

// ─── JOIN result fed into WHERE on joined column ──────────────────────────────

// Only posts from US-based authors.
func TestJoinWhereOnJoinedColumn(t *testing.T) {
	db := blogDB(t)

	rows := mustQuery(t, db, `
		SELECT p.title
		FROM posts p
		JOIN authors a ON p.author_id = a.id
		WHERE a.country = 'US'
		ORDER BY p.id ASC
	`)
	// Only Alice is US (posts 10, 11). Carol has no posts.
	if len(rows) != 2 {
		t.Fatalf("expected 2 US-author posts, got %d: %v", len(rows), rows)
	}
	if rows[0]["title"] != strVal("Go Tips") || rows[1]["title"] != strVal("SQL Tricks") {
		t.Errorf("unexpected titles: %v", rows)
	}
}

// ─── SELECT * across join (star expansion) ───────────────────────────────────

func TestJoinStarExpansion(t *testing.T) {
	db := New()
	mustExec(t, db, `INSERT INTO cats (id, name)      VALUES (1, 'Mimi')`)
	mustExec(t, db, `INSERT INTO toys (id, cat_id, toy) VALUES (10, 1, 'ball')`)

	rows := mustQuery(t, db, `
		SELECT *
		FROM cats c
		JOIN toys t ON c.id = t.cat_id
	`)
	if len(rows) != 1 {
		t.Fatalf("expected 1 joined row, got %d", len(rows))
	}
	// Both tables' columns should be present.
	r := rows[0]
	if r["name"] != strVal("Mimi") {
		t.Errorf("expected name=Mimi, got %v", r["name"])
	}
	if r["toy"] != strVal("ball") {
		t.Errorf("expected toy=ball, got %v", r["toy"])
	}
}
