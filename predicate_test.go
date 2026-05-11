package vapordb

// Tests for every WHERE-clause predicate and operator vapordb supports.

import "testing"

// ─── shared fixture ───────────────────────────────────────────────────────────

// predDB sets up a small table covering int, float, string and NULL values.
//
//	things (id, n, score, label)
//	  1  → n=1,  score=1.5,  label='apple'
//	  2  → n=5,  score=5.0,  label='banana'
//	  3  → n=10, score=10.5, label='cherry'
//	  4  → n=15, score=NULL, label=NULL
//	  5  → n=20, score=20.0, label='apple'
func predDB(t *testing.T) *DB {
	t.Helper()
	db := New()
	mustExec(t, db, `INSERT INTO things (id, n, score, label) VALUES (1,  1,  1.5,  'apple')`)
	mustExec(t, db, `INSERT INTO things (id, n, score, label) VALUES (2,  5,  5.0,  'banana')`)
	mustExec(t, db, `INSERT INTO things (id, n, score, label) VALUES (3,  10, 10.5, 'cherry')`)
	mustExec(t, db, `INSERT INTO things (id, n, score, label) VALUES (4,  15, NULL, NULL)`)
	mustExec(t, db, `INSERT INTO things (id, n, score, label) VALUES (5,  20, 20.0, 'apple')`)
	return db
}

func ids(rows []Row) []int64 {
	out := make([]int64, len(rows))
	for i, r := range rows {
		out[i], _ = r["id"].V.(int64)
	}
	return out
}

// ─── equality / inequality ────────────────────────────────────────────────────

func TestEqualInt(t *testing.T) {
	db := predDB(t)
	rows := mustQuery(t, db, `SELECT id FROM things WHERE n = 5`)
	if len(rows) != 1 || rows[0]["id"] != intVal(2) {
		t.Errorf("= int: want [2], got %v", ids(rows))
	}
}

func TestEqualString(t *testing.T) {
	db := predDB(t)
	rows := mustQuery(t, db, `SELECT id FROM things WHERE label = 'apple' ORDER BY id`)
	if len(rows) != 2 || rows[0]["id"] != intVal(1) || rows[1]["id"] != intVal(5) {
		t.Errorf("= string: want [1 5], got %v", ids(rows))
	}
}

func TestNotEqual(t *testing.T) {
	db := predDB(t)
	// != and <> should both work
	rows := mustQuery(t, db, `SELECT id FROM things WHERE n != 5 AND label IS NOT NULL ORDER BY id`)
	// excludes id=2 (n=5) and id=4 (label IS NULL) → 1,3,5
	if len(rows) != 3 {
		t.Errorf("!=: want 3 rows, got %d: %v", len(rows), ids(rows))
	}
}

func TestLessThan(t *testing.T) {
	db := predDB(t)
	rows := mustQuery(t, db, `SELECT id FROM things WHERE n < 10 ORDER BY id`)
	if got := ids(rows); len(got) != 2 || got[0] != 1 || got[1] != 2 {
		t.Errorf("< 10: want [1 2], got %v", got)
	}
}

func TestLessThanOrEqual(t *testing.T) {
	db := predDB(t)
	rows := mustQuery(t, db, `SELECT id FROM things WHERE n <= 10 ORDER BY id`)
	if got := ids(rows); len(got) != 3 || got[2] != 3 {
		t.Errorf("<= 10: want [1 2 3], got %v", got)
	}
}

func TestGreaterThan(t *testing.T) {
	db := predDB(t)
	rows := mustQuery(t, db, `SELECT id FROM things WHERE n > 10 ORDER BY id`)
	if got := ids(rows); len(got) != 2 || got[0] != 4 || got[1] != 5 {
		t.Errorf("> 10: want [4 5], got %v", got)
	}
}

func TestGreaterThanOrEqual(t *testing.T) {
	db := predDB(t)
	rows := mustQuery(t, db, `SELECT id FROM things WHERE n >= 10 ORDER BY id`)
	if got := ids(rows); len(got) != 3 || got[0] != 3 {
		t.Errorf(">= 10: want [3 4 5], got %v", got)
	}
}

func TestCompareFloat(t *testing.T) {
	db := predDB(t)
	rows := mustQuery(t, db, `SELECT id FROM things WHERE score > 5.0 ORDER BY id`)
	// score > 5.0: 10.5(id=3), 20.0(id=5); id=4 is NULL → skipped
	if got := ids(rows); len(got) != 2 || got[0] != 3 || got[1] != 5 {
		t.Errorf("score > 5.0: want [3 5], got %v", got)
	}
}

// ─── NULL propagation in comparisons ─────────────────────────────────────────

// NULL compared with anything using =, <, > etc. must return false (not an error).
func TestNullComparisonReturnsFalse(t *testing.T) {
	db := predDB(t)

	// score IS NULL for id=4; comparing NULL > 0 must NOT match
	rows := mustQuery(t, db, `SELECT id FROM things WHERE score > 0 ORDER BY id`)
	for _, r := range rows {
		if r["id"] == intVal(4) {
			t.Error("NULL > 0 should not match (id=4 appeared in results)")
		}
	}

	// NULL = NULL must also be false (use IS NULL for that)
	rows = mustQuery(t, db, `SELECT id FROM things WHERE score = NULL`)
	if len(rows) != 0 {
		t.Errorf("score = NULL should match 0 rows, got %d", len(rows))
	}
}

// ─── BETWEEN / NOT BETWEEN ───────────────────────────────────────────────────

func TestBetweenInclusive(t *testing.T) {
	db := predDB(t)
	rows := mustQuery(t, db, `SELECT id FROM things WHERE n BETWEEN 5 AND 15 ORDER BY id`)
	// inclusive: n=5,10,15 → ids 2,3,4
	if got := ids(rows); len(got) != 3 || got[0] != 2 || got[2] != 4 {
		t.Errorf("BETWEEN 5 AND 15: want [2 3 4], got %v", got)
	}
}

func TestBetweenEdgeLow(t *testing.T) {
	db := predDB(t)
	rows := mustQuery(t, db, `SELECT id FROM things WHERE n BETWEEN 1 AND 1`)
	if len(rows) != 1 || rows[0]["id"] != intVal(1) {
		t.Errorf("BETWEEN low edge: want [1], got %v", ids(rows))
	}
}

func TestBetweenEdgeHigh(t *testing.T) {
	db := predDB(t)
	rows := mustQuery(t, db, `SELECT id FROM things WHERE n BETWEEN 20 AND 20`)
	if len(rows) != 1 || rows[0]["id"] != intVal(5) {
		t.Errorf("BETWEEN high edge: want [5], got %v", ids(rows))
	}
}

func TestNotBetween(t *testing.T) {
	db := predDB(t)
	rows := mustQuery(t, db, `SELECT id FROM things WHERE n NOT BETWEEN 5 AND 15 ORDER BY id`)
	// n=1(id=1) and n=20(id=5) are outside [5,15]
	if got := ids(rows); len(got) != 2 || got[0] != 1 || got[1] != 5 {
		t.Errorf("NOT BETWEEN: want [1 5], got %v", got)
	}
}

func TestBetweenFloat(t *testing.T) {
	db := predDB(t)
	rows := mustQuery(t, db, `SELECT id FROM things WHERE score BETWEEN 5.0 AND 10.5 ORDER BY id`)
	// score: 5.0(id=2), 10.5(id=3); NULL(id=4) excluded
	if got := ids(rows); len(got) != 2 || got[0] != 2 || got[1] != 3 {
		t.Errorf("BETWEEN float: want [2 3], got %v", got)
	}
}

func TestBetweenStrings(t *testing.T) {
	db := predDB(t)
	rows := mustQuery(t, db, `SELECT id FROM things WHERE label BETWEEN 'apple' AND 'cherry' ORDER BY id`)
	// lexicographic: apple ≤ banana ≤ cherry; NULL excluded → ids 1,2,3,5
	if got := ids(rows); len(got) != 4 {
		t.Errorf("BETWEEN strings: want 4 rows, got %v", got)
	}
}

// ─── IN / NOT IN ─────────────────────────────────────────────────────────────

func TestInIntegers(t *testing.T) {
	db := predDB(t)
	rows := mustQuery(t, db, `SELECT id FROM things WHERE n IN (1, 10, 20) ORDER BY id`)
	if got := ids(rows); len(got) != 3 || got[0] != 1 || got[1] != 3 || got[2] != 5 {
		t.Errorf("IN ints: want [1 3 5], got %v", got)
	}
}

func TestInStrings(t *testing.T) {
	db := predDB(t)
	rows := mustQuery(t, db, `SELECT id FROM things WHERE label IN ('apple', 'cherry') ORDER BY id`)
	// ids 1,3,5 (id=4 label IS NULL → not matched)
	if got := ids(rows); len(got) != 3 || got[0] != 1 || got[1] != 3 || got[2] != 5 {
		t.Errorf("IN strings: want [1 3 5], got %v", got)
	}
}

func TestNotIn(t *testing.T) {
	db := predDB(t)
	rows := mustQuery(t, db, `SELECT id FROM things WHERE n NOT IN (5, 15, 20) AND n IS NOT NULL ORDER BY id`)
	// n=1(id=1), n=10(id=3) remain
	if got := ids(rows); len(got) != 2 || got[0] != 1 || got[1] != 3 {
		t.Errorf("NOT IN: want [1 3], got %v", got)
	}
}

func TestInSingleValue(t *testing.T) {
	db := predDB(t)
	rows := mustQuery(t, db, `SELECT id FROM things WHERE n IN (5)`)
	if len(rows) != 1 || rows[0]["id"] != intVal(2) {
		t.Errorf("IN single: want [2], got %v", ids(rows))
	}
}

func TestInNoMatch(t *testing.T) {
	db := predDB(t)
	rows := mustQuery(t, db, `SELECT id FROM things WHERE n IN (99, 100)`)
	if len(rows) != 0 {
		t.Errorf("IN no match: want [], got %v", ids(rows))
	}
}

// ─── LIKE / NOT LIKE ─────────────────────────────────────────────────────────

func TestLikePercentPrefix(t *testing.T) {
	db := predDB(t)
	rows := mustQuery(t, db, `SELECT id FROM things WHERE label LIKE 'a%' ORDER BY id`)
	// apple × 2 (ids 1,5)
	if got := ids(rows); len(got) != 2 || got[0] != 1 || got[1] != 5 {
		t.Errorf("LIKE 'a%%': want [1 5], got %v", got)
	}
}

func TestLikePercentSuffix(t *testing.T) {
	db := predDB(t)
	rows := mustQuery(t, db, `SELECT id FROM things WHERE label LIKE '%e' ORDER BY id`)
	// apple(ids 1,5), orange not present; cherry ends with y; banana ends in a
	// Only 'apple' ends with 'e' → ids 1,5
	if got := ids(rows); len(got) != 2 {
		t.Errorf("LIKE '%%e': want 2 rows, got %v", got)
	}
}

func TestLikePercentBoth(t *testing.T) {
	db := predDB(t)
	rows := mustQuery(t, db, `SELECT id FROM things WHERE label LIKE '%an%' ORDER BY id`)
	// 'banana' contains 'an' → id=2
	if got := ids(rows); len(got) != 1 || got[0] != 2 {
		t.Errorf("LIKE '%%an%%': want [2], got %v", got)
	}
}

func TestLikeUnderscore(t *testing.T) {
	db := predDB(t)
	// '_pple' matches 'apple' (any single char + 'pple')
	rows := mustQuery(t, db, `SELECT id FROM things WHERE label LIKE '_pple' ORDER BY id`)
	if got := ids(rows); len(got) != 2 || got[0] != 1 || got[1] != 5 {
		t.Errorf("LIKE '_pple': want [1 5], got %v", got)
	}
}

func TestLikeUnderscoreMiddle(t *testing.T) {
	db := predDB(t)
	// 'b_nana' matches 'banana'
	rows := mustQuery(t, db, `SELECT id FROM things WHERE label LIKE 'b_nana'`)
	if len(rows) != 1 || rows[0]["id"] != intVal(2) {
		t.Errorf("LIKE 'b_nana': want [2], got %v", ids(rows))
	}
}

func TestLikeExactMatch(t *testing.T) {
	db := predDB(t)
	rows := mustQuery(t, db, `SELECT id FROM things WHERE label LIKE 'cherry'`)
	if len(rows) != 1 || rows[0]["id"] != intVal(3) {
		t.Errorf("LIKE exact: want [3], got %v", ids(rows))
	}
}

func TestLikeNoMatch(t *testing.T) {
	db := predDB(t)
	rows := mustQuery(t, db, `SELECT id FROM things WHERE label LIKE 'z%'`)
	if len(rows) != 0 {
		t.Errorf("LIKE no match: want [], got %v", ids(rows))
	}
}

func TestLikePercentAlone(t *testing.T) {
	db := predDB(t)
	// '%' matches everything (including empty string)
	rows := mustQuery(t, db, `SELECT id FROM things WHERE label LIKE '%' ORDER BY id`)
	// NULL label (id=4) is NOT matched — LIKE on NULL returns false
	if got := ids(rows); len(got) != 4 {
		t.Errorf("LIKE '%%': want 4 non-null rows, got %v", got)
	}
}

func TestNotLike(t *testing.T) {
	db := predDB(t)
	rows := mustQuery(t, db, `SELECT id FROM things WHERE label NOT LIKE 'a%' AND label IS NOT NULL ORDER BY id`)
	// banana(id=2), cherry(id=3)
	if got := ids(rows); len(got) != 2 || got[0] != 2 || got[1] != 3 {
		t.Errorf("NOT LIKE 'a%%': want [2 3], got %v", got)
	}
}

func TestLikeMatchEscapeUnit(t *testing.T) {
	if !LikeMatchEscape(`50#%`, `50%`, '#') {
		t.Fatal("50 hash-percent should match fifty percent")
	}
	if LikeMatchEscape(`50#%`, `50x`, '#') {
		t.Fatal("50 hash-percent should not match 50x")
	}
	if !LikeMatchEscape(`a#%`, `a%`, '#') {
		t.Fatal("a hash-percent should match a percent only")
	}
	if LikeMatchEscape(`a#%`, `a%b`, '#') {
		t.Fatal("a hash-percent should not match a percent b")
	}
	if !LikeMatchEscape(`a#_b`, `a_b`, '#') {
		t.Fatal("literal underscore via #_")
	}
	if LikeMatchEscape(`50#`, `50`, '#') {
		t.Fatal("trailing lone escape should not match")
	}
}

func TestLikeEscapeLiteralPercent(t *testing.T) {
	db := New()
	mustExec(t, db, `INSERT INTO paths (p) VALUES ('50%')`)
	mustExec(t, db, `INSERT INTO paths (p) VALUES ('50x')`)
	mustExec(t, db, `INSERT INTO paths (p) VALUES ('150%')`)
	rows := mustQuery(t, db, `SELECT p FROM paths WHERE p LIKE '50#%' ESCAPE '#' ORDER BY p`)
	if len(rows) != 1 || rows[0]["p"] != strVal("50%") {
		t.Fatalf("want one row p=50 percent, got %v", rows)
	}
}

func TestLikeEscapeLiteralUnderscore(t *testing.T) {
	db := New()
	mustExec(t, db, `INSERT INTO paths (p) VALUES ('x_y')`)
	mustExec(t, db, `INSERT INTO paths (p) VALUES ('xy')`)
	rows := mustQuery(t, db, `SELECT p FROM paths WHERE p LIKE 'x|_y' ESCAPE '|'`)
	if len(rows) != 1 || rows[0]["p"] != strVal("x_y") {
		t.Fatalf("want x_y, got %v", rows)
	}
}

func TestNotLikeEscape(t *testing.T) {
	db := New()
	mustExec(t, db, `INSERT INTO paths (p) VALUES ('a%b')`)
	mustExec(t, db, `INSERT INTO paths (p) VALUES ('acb')`)
	rows := mustQuery(t, db, `SELECT p FROM paths WHERE p NOT LIKE 'a#%b' ESCAPE '#' ORDER BY p`)
	if len(rows) != 1 || rows[0]["p"] != strVal("acb") {
		t.Fatalf("want acb only, got %v", rows)
	}
}

func TestLikeEscapeEmptyDisables(t *testing.T) {
	db := New()
	_, err := db.Query(`SELECT 1 AS n FROM DUAL WHERE 'hello' LIKE 'h_llo' ESCAPE ''`)
	if err != nil {
		t.Fatal(err)
	}
}

func TestLikeEscapeInvalidLength(t *testing.T) {
	db := New()
	_, err := db.Query(`SELECT 1 AS n FROM DUAL WHERE 'a' LIKE 'a' ESCAPE '##'`)
	if err == nil {
		t.Fatal("expected error for multi-rune ESCAPE")
	}
}

// ─── AND / OR / NOT ───────────────────────────────────────────────────────────

func TestAnd(t *testing.T) {
	db := predDB(t)
	rows := mustQuery(t, db, `SELECT id FROM things WHERE n >= 5 AND n <= 15 ORDER BY id`)
	if got := ids(rows); len(got) != 3 || got[0] != 2 || got[2] != 4 {
		t.Errorf("AND: want [2 3 4], got %v", got)
	}
}

func TestOr(t *testing.T) {
	db := predDB(t)
	rows := mustQuery(t, db, `SELECT id FROM things WHERE n = 1 OR n = 20 ORDER BY id`)
	if got := ids(rows); len(got) != 2 || got[0] != 1 || got[1] != 5 {
		t.Errorf("OR: want [1 5], got %v", got)
	}
}

func TestNot(t *testing.T) {
	db := predDB(t)
	rows := mustQuery(t, db, `SELECT id FROM things WHERE NOT (n = 1) AND label IS NOT NULL ORDER BY id`)
	// n ≠ 1, label non-null → ids 2,3,5
	if got := ids(rows); len(got) != 3 || got[0] != 2 || got[2] != 5 {
		t.Errorf("NOT: want [2 3 5], got %v", got)
	}
}

func TestAndShortCircuit(t *testing.T) {
	// False AND anything → false, no error even if right side would be weird
	db := New()
	mustExec(t, db, `INSERT INTO t (v) VALUES (1)`)
	rows := mustQuery(t, db, `SELECT v FROM t WHERE 1 = 0 AND v > 0`)
	if len(rows) != 0 {
		t.Errorf("false AND: expected no rows, got %d", len(rows))
	}
}

func TestOrShortCircuit(t *testing.T) {
	db := New()
	mustExec(t, db, `INSERT INTO t (v) VALUES (1)`)
	rows := mustQuery(t, db, `SELECT v FROM t WHERE 1 = 1 OR v = 99`)
	if len(rows) != 1 {
		t.Errorf("true OR: expected 1 row, got %d", len(rows))
	}
}

func TestComplexAndOr(t *testing.T) {
	db := predDB(t)
	// (n < 5 OR n > 15) AND label IS NOT NULL
	// n < 5: id=1 (n=1); n > 15: id=5 (n=20); both have non-null labels
	rows := mustQuery(t, db, `
		SELECT id FROM things
		WHERE (n < 5 OR n > 15) AND label IS NOT NULL
		ORDER BY id
	`)
	if got := ids(rows); len(got) != 2 || got[0] != 1 || got[1] != 5 {
		t.Errorf("(n<5 OR n>15) AND non-null: want [1 5], got %v", got)
	}
}

func TestThreeWayAnd(t *testing.T) {
	db := predDB(t)
	rows := mustQuery(t, db, `SELECT id FROM things WHERE n > 1 AND n < 20 AND label IS NOT NULL ORDER BY id`)
	// n in (5,10), label non-null → ids 2,3
	if got := ids(rows); len(got) != 2 || got[0] != 2 || got[1] != 3 {
		t.Errorf("3-way AND: want [2 3], got %v", got)
	}
}

// ─── IS TRUE / IS FALSE ───────────────────────────────────────────────────────

func TestIsTrue(t *testing.T) {
	db := New()
	mustExec(t, db, `INSERT INTO t (flag) VALUES (1)`)
	mustExec(t, db, `INSERT INTO t (flag) VALUES (0)`)
	mustExec(t, db, `INSERT INTO t (flag) VALUES (NULL)`)

	rows := mustQuery(t, db, `SELECT flag FROM t WHERE flag IS TRUE`)
	if len(rows) != 1 || rows[0]["flag"] != intVal(1) {
		t.Errorf("IS TRUE: expected [1], got %v", rows)
	}
}

func TestIsFalse(t *testing.T) {
	db := New()
	mustExec(t, db, `INSERT INTO t (flag) VALUES (1)`)
	mustExec(t, db, `INSERT INTO t (flag) VALUES (0)`)

	rows := mustQuery(t, db, `SELECT flag FROM t WHERE flag IS FALSE`)
	if len(rows) != 1 || rows[0]["flag"] != intVal(0) {
		t.Errorf("IS FALSE: expected [0], got %v", rows)
	}
}

// ─── ORDER BY with multiple columns ──────────────────────────────────────────

func TestOrderByMultipleColumns(t *testing.T) {
	db := New()
	mustExec(t, db, `INSERT INTO t (a, b) VALUES (1, 3)`)
	mustExec(t, db, `INSERT INTO t (a, b) VALUES (1, 1)`)
	mustExec(t, db, `INSERT INTO t (a, b) VALUES (2, 2)`)
	mustExec(t, db, `INSERT INTO t (a, b) VALUES (1, 2)`)

	rows := mustQuery(t, db, `SELECT a, b FROM t ORDER BY a ASC, b DESC`)
	// a=1: b=3,2,1 then a=2: b=2
	expected := [][2]int64{{1, 3}, {1, 2}, {1, 1}, {2, 2}}
	for i, e := range expected {
		if rows[i]["a"] != intVal(e[0]) || rows[i]["b"] != intVal(e[1]) {
			t.Errorf("row %d: want {a=%d,b=%d}, got %v", i, e[0], e[1], rows[i])
		}
	}
}

func TestOrderByNullsFirst(t *testing.T) {
	db := New()
	mustExec(t, db, `INSERT INTO t (v) VALUES (5)`)
	mustExec(t, db, `INSERT INTO t (v) VALUES (NULL)`)
	mustExec(t, db, `INSERT INTO t (v) VALUES (3)`)

	rows := mustQuery(t, db, `SELECT v FROM t ORDER BY v ASC`)
	// NULL sorts first (treated as smallest)
	if rows[0]["v"].Kind != KindNull {
		t.Errorf("ORDER BY ASC: expected NULL first, got %v", rows[0]["v"])
	}
	if rows[1]["v"] != intVal(3) || rows[2]["v"] != intVal(5) {
		t.Errorf("ORDER BY ASC: expected [NULL,3,5], got %v %v %v", rows[0]["v"], rows[1]["v"], rows[2]["v"])
	}
}

func TestOrderByDesc(t *testing.T) {
	db := New()
	mustExec(t, db, `INSERT INTO t (v) VALUES (5)`)
	mustExec(t, db, `INSERT INTO t (v) VALUES (NULL)`)
	mustExec(t, db, `INSERT INTO t (v) VALUES (3)`)

	rows := mustQuery(t, db, `SELECT v FROM t ORDER BY v DESC`)
	// DESC reverses: 5, 3, NULL
	if rows[0]["v"] != intVal(5) || rows[1]["v"] != intVal(3) {
		t.Errorf("ORDER BY DESC: expected [5,3,NULL], got %v %v %v", rows[0]["v"], rows[1]["v"], rows[2]["v"])
	}
	if rows[2]["v"].Kind != KindNull {
		t.Errorf("ORDER BY DESC: expected NULL last, got %v", rows[2]["v"])
	}
}

// ─── WHERE with UPDATE ────────────────────────────────────────────────────────

func TestUpdateWithGreaterThan(t *testing.T) {
	db := predDB(t)
	mustExec(t, db, `UPDATE things SET n = 0 WHERE n > 10`)
	// ids 4,5 had n > 10
	rows := mustQuery(t, db, `SELECT id FROM things WHERE n = 0 ORDER BY id`)
	if got := ids(rows); len(got) != 2 || got[0] != 4 || got[1] != 5 {
		t.Errorf("UPDATE > 10: want ids [4 5] zeroed, got %v", got)
	}
}

func TestUpdateWithBetween(t *testing.T) {
	db := predDB(t)
	mustExec(t, db, `UPDATE things SET label = 'mid' WHERE n BETWEEN 5 AND 15`)
	rows := mustQuery(t, db, `SELECT id FROM things WHERE label = 'mid' ORDER BY id`)
	if got := ids(rows); len(got) != 3 || got[0] != 2 || got[2] != 4 {
		t.Errorf("UPDATE BETWEEN: want ids [2 3 4], got %v", got)
	}
}

func TestUpdateWithIn(t *testing.T) {
	db := predDB(t)
	mustExec(t, db, `UPDATE things SET n = 99 WHERE id IN (1, 3, 5)`)
	rows := mustQuery(t, db, `SELECT id FROM things WHERE n = 99 ORDER BY id`)
	if got := ids(rows); len(got) != 3 || got[0] != 1 || got[2] != 5 {
		t.Errorf("UPDATE IN: want ids [1 3 5], got %v", got)
	}
}

// ─── WHERE with DELETE ────────────────────────────────────────────────────────

func TestDeleteWithLike(t *testing.T) {
	db := predDB(t)
	mustExec(t, db, `DELETE FROM things WHERE label LIKE 'a%'`)
	rows := mustQuery(t, db, `SELECT id FROM things ORDER BY id`)
	for _, r := range rows {
		if r["id"] == intVal(1) || r["id"] == intVal(5) {
			t.Errorf("DELETE LIKE: id=1 and id=5 (label='apple') should be gone")
		}
	}
	if len(rows) != 3 {
		t.Errorf("DELETE LIKE: expected 3 remaining rows, got %d", len(rows))
	}
}

func TestDeleteWithBetween(t *testing.T) {
	db := predDB(t)
	mustExec(t, db, `DELETE FROM things WHERE n BETWEEN 5 AND 15`)
	rows := mustQuery(t, db, `SELECT id FROM things ORDER BY id`)
	// ids 2,3,4 removed → 1,5 remain
	if got := ids(rows); len(got) != 2 || got[0] != 1 || got[1] != 5 {
		t.Errorf("DELETE BETWEEN: want ids [1 5], got %v", got)
	}
}

func TestDeleteWithNotIn(t *testing.T) {
	db := predDB(t)
	// keep only ids 1 and 5
	mustExec(t, db, `DELETE FROM things WHERE id NOT IN (1, 5)`)
	rows := mustQuery(t, db, `SELECT id FROM things ORDER BY id`)
	if got := ids(rows); len(got) != 2 || got[0] != 1 || got[1] != 5 {
		t.Errorf("DELETE NOT IN: want ids [1 5], got %v", got)
	}
}

// ─── compound WHERE in SELECT ─────────────────────────────────────────────────

func TestWhereInAndBetween(t *testing.T) {
	db := predDB(t)
	// n IN (1,5,10) AND n BETWEEN 4 AND 12 → only n=5(id=2) and n=10(id=3)
	rows := mustQuery(t, db, `
		SELECT id FROM things
		WHERE n IN (1, 5, 10) AND n BETWEEN 4 AND 12
		ORDER BY id
	`)
	if got := ids(rows); len(got) != 2 || got[0] != 2 || got[1] != 3 {
		t.Errorf("IN AND BETWEEN: want [2 3], got %v", got)
	}
}

func TestWhereOrWithLike(t *testing.T) {
	db := predDB(t)
	// label LIKE 'a%' OR n > 18
	rows := mustQuery(t, db, `
		SELECT id FROM things
		WHERE label LIKE 'a%' OR n > 18
		ORDER BY id
	`)
	// apple(ids 1,5) match LIKE, n>18: id=5 (n=20) — union → 1,5
	if got := ids(rows); len(got) != 2 || got[0] != 1 || got[1] != 5 {
		t.Errorf("LIKE OR >: want [1 5], got %v", got)
	}
}

func TestWhereNotBetweenAndNotNull(t *testing.T) {
	db := predDB(t)
	rows := mustQuery(t, db, `
		SELECT id FROM things
		WHERE n NOT BETWEEN 5 AND 15 AND label IS NOT NULL
		ORDER BY id
	`)
	// NOT BETWEEN: n=1(id=1), n=20(id=5); both have labels → [1,5]
	if got := ids(rows); len(got) != 2 || got[0] != 1 || got[1] != 5 {
		t.Errorf("NOT BETWEEN AND NOT NULL: want [1 5], got %v", got)
	}
}
