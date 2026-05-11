package vapordb

// Tests for SQL functions and aggregate expressions.

import (
	"math"
	"testing"
)

// ─── helpers ─────────────────────────────────────────────────────────────────

func floatVal(f float64) Value { return Value{Kind: KindFloat, V: f} }
func boolVal(b bool) Value    { return Value{Kind: KindBool, V: b} }

func floatClose(a, b Value, eps float64) bool {
	if a.Kind != KindFloat || b.Kind != KindFloat {
		return false
	}
	return math.Abs(a.V.(float64)-b.V.(float64)) < eps
}

// numDB builds a simple numeric table with some NULLs.
//
//	nums (id, v)
//	  1 → 10
//	  2 → 20
//	  3 → 30
//	  4 → NULL
func numDB(t *testing.T) *DB {
	t.Helper()
	db := New()
	mustExec(t, db, `INSERT INTO nums (id, v) VALUES (1, 10)`)
	mustExec(t, db, `INSERT INTO nums (id, v) VALUES (2, 20)`)
	mustExec(t, db, `INSERT INTO nums (id, v) VALUES (3, 30)`)
	mustExec(t, db, `INSERT INTO nums (id, v) VALUES (4, NULL)`)
	return db
}

// ─── COUNT ───────────────────────────────────────────────────────────────────

func TestCountStar(t *testing.T) {
	db := numDB(t)
	rows := mustQuery(t, db, `SELECT COUNT(*) AS n FROM nums`)
	if rows[0]["n"] != intVal(4) {
		t.Errorf("COUNT(*): expected 4, got %v", rows[0]["n"])
	}
}

func TestCountColumn_SkipsNulls(t *testing.T) {
	db := numDB(t)
	rows := mustQuery(t, db, `SELECT COUNT(v) AS n FROM nums`)
	// id=4 has NULL v → only 3 non-null values
	if rows[0]["n"] != intVal(3) {
		t.Errorf("COUNT(v): expected 3 (NULLs skipped), got %v", rows[0]["n"])
	}
}

func TestCountDistinct(t *testing.T) {
	db := New()
	mustExec(t, db, `INSERT INTO t (x) VALUES (1)`)
	mustExec(t, db, `INSERT INTO t (x) VALUES (1)`)
	mustExec(t, db, `INSERT INTO t (x) VALUES (2)`)
	rows := mustQuery(t, db, `SELECT COUNT(DISTINCT x) AS n FROM t`)
	if rows[0]["n"] != intVal(2) {
		t.Errorf("COUNT(DISTINCT): expected 2, got %v", rows[0]["n"])
	}
}

func TestCountGroupBy(t *testing.T) {
	db := New()
	mustExec(t, db, `INSERT INTO t (cat, val) VALUES ('a', 1)`)
	mustExec(t, db, `INSERT INTO t (cat, val) VALUES ('a', 2)`)
	mustExec(t, db, `INSERT INTO t (cat, val) VALUES ('b', 3)`)

	rows := mustQuery(t, db, `SELECT cat, COUNT(*) AS n FROM t GROUP BY cat ORDER BY cat ASC`)
	if len(rows) != 2 {
		t.Fatalf("expected 2 groups, got %d", len(rows))
	}
	if rows[0]["n"] != intVal(2) || rows[1]["n"] != intVal(1) {
		t.Errorf("unexpected counts: %v %v", rows[0], rows[1])
	}
}

// ─── SUM ─────────────────────────────────────────────────────────────────────

func TestSumBasic(t *testing.T) {
	db := numDB(t)
	rows := mustQuery(t, db, `SELECT SUM(v) AS s FROM nums`)
	// NULLs are skipped → 10+20+30 = 60
	if rows[0]["s"] != intVal(60) {
		t.Errorf("SUM: expected 60, got %v", rows[0]["s"])
	}
}

func TestSumAllNull(t *testing.T) {
	db := New()
	mustExec(t, db, `INSERT INTO t (v) VALUES (NULL)`)
	mustExec(t, db, `INSERT INTO t (v) VALUES (NULL)`)
	rows := mustQuery(t, db, `SELECT SUM(v) AS s FROM t`)
	if rows[0]["s"].Kind != KindNull {
		t.Errorf("SUM of all NULLs: expected NULL, got %v", rows[0]["s"])
	}
}

func TestSumGroupBy(t *testing.T) {
	db := New()
	mustExec(t, db, `INSERT INTO sales (region, amount) VALUES ('east', 100)`)
	mustExec(t, db, `INSERT INTO sales (region, amount) VALUES ('east', 150)`)
	mustExec(t, db, `INSERT INTO sales (region, amount) VALUES ('west', 200)`)

	rows := mustQuery(t, db, `SELECT region, SUM(amount) AS total FROM sales GROUP BY region ORDER BY region ASC`)
	if rows[0]["total"] != intVal(250) {
		t.Errorf("east: expected 250, got %v", rows[0]["total"])
	}
	if rows[1]["total"] != intVal(200) {
		t.Errorf("west: expected 200, got %v", rows[1]["total"])
	}
}

// ─── AVG ─────────────────────────────────────────────────────────────────────

func TestAvgBasic(t *testing.T) {
	db := numDB(t)
	rows := mustQuery(t, db, `SELECT AVG(v) AS a FROM nums`)
	// NULLs excluded: (10+20+30)/3 = 20.0
	if !floatClose(rows[0]["a"], floatVal(20.0), 1e-9) {
		t.Errorf("AVG: expected 20.0, got %v", rows[0]["a"])
	}
}

func TestAvgAllNull(t *testing.T) {
	db := New()
	mustExec(t, db, `INSERT INTO t (v) VALUES (NULL)`)
	rows := mustQuery(t, db, `SELECT AVG(v) AS a FROM t`)
	if rows[0]["a"].Kind != KindNull {
		t.Errorf("AVG of all NULLs: expected NULL, got %v", rows[0]["a"])
	}
}

func TestAvgGroupBy(t *testing.T) {
	db := New()
	mustExec(t, db, `INSERT INTO t (cat, v) VALUES ('x', 10)`)
	mustExec(t, db, `INSERT INTO t (cat, v) VALUES ('x', 30)`)
	mustExec(t, db, `INSERT INTO t (cat, v) VALUES ('y', 5)`)

	rows := mustQuery(t, db, `SELECT cat, AVG(v) AS a FROM t GROUP BY cat ORDER BY cat ASC`)
	if !floatClose(rows[0]["a"], floatVal(20.0), 1e-9) {
		t.Errorf("x avg: expected 20.0, got %v", rows[0]["a"])
	}
	if !floatClose(rows[1]["a"], floatVal(5.0), 1e-9) {
		t.Errorf("y avg: expected 5.0, got %v", rows[1]["a"])
	}
}

// ─── MIN / MAX ────────────────────────────────────────────────────────────────

func TestMinMax(t *testing.T) {
	db := numDB(t)
	rows := mustQuery(t, db, `SELECT MIN(v) AS lo, MAX(v) AS hi FROM nums`)
	if rows[0]["lo"] != intVal(10) {
		t.Errorf("MIN: expected 10, got %v", rows[0]["lo"])
	}
	if rows[0]["hi"] != intVal(30) {
		t.Errorf("MAX: expected 30, got %v", rows[0]["hi"])
	}
}

func TestMinMaxStrings(t *testing.T) {
	db := New()
	mustExec(t, db, `INSERT INTO t (name) VALUES ('Charlie')`)
	mustExec(t, db, `INSERT INTO t (name) VALUES ('Alice')`)
	mustExec(t, db, `INSERT INTO t (name) VALUES ('Bob')`)

	rows := mustQuery(t, db, `SELECT MIN(name) AS lo, MAX(name) AS hi FROM t`)
	if rows[0]["lo"] != strVal("Alice") {
		t.Errorf("MIN(string): expected Alice, got %v", rows[0]["lo"])
	}
	if rows[0]["hi"] != strVal("Charlie") {
		t.Errorf("MAX(string): expected Charlie, got %v", rows[0]["hi"])
	}
}

func TestMinMaxAllNull(t *testing.T) {
	db := New()
	mustExec(t, db, `INSERT INTO t (v) VALUES (NULL)`)
	rows := mustQuery(t, db, `SELECT MIN(v) AS lo, MAX(v) AS hi FROM t`)
	if rows[0]["lo"].Kind != KindNull || rows[0]["hi"].Kind != KindNull {
		t.Errorf("MIN/MAX of all NULLs: expected NULL, got %v %v", rows[0]["lo"], rows[0]["hi"])
	}
}

func TestMinMaxGroupBy(t *testing.T) {
	db := New()
	mustExec(t, db, `INSERT INTO scores (player, score) VALUES ('alice', 80)`)
	mustExec(t, db, `INSERT INTO scores (player, score) VALUES ('alice', 95)`)
	mustExec(t, db, `INSERT INTO scores (player, score) VALUES ('bob',   70)`)

	rows := mustQuery(t, db, `
		SELECT player, MIN(score) AS best_worst, MAX(score) AS best
		FROM scores
		GROUP BY player
		ORDER BY player ASC
	`)
	if rows[0]["best"] != intVal(95) {
		t.Errorf("alice MAX: expected 95, got %v", rows[0]["best"])
	}
	if rows[0]["best_worst"] != intVal(80) {
		t.Errorf("alice MIN: expected 80, got %v", rows[0]["best_worst"])
	}
}

// ─── HAVING with multiple aggregate conditions ────────────────────────────────

func TestHavingWithSumAndCount(t *testing.T) {
	db := New()
	mustExec(t, db, `INSERT INTO orders (cust, amount) VALUES ('alice', 100)`)
	mustExec(t, db, `INSERT INTO orders (cust, amount) VALUES ('alice', 200)`)
	mustExec(t, db, `INSERT INTO orders (cust, amount) VALUES ('bob',   50)`)
	mustExec(t, db, `INSERT INTO orders (cust, amount) VALUES ('carol', 500)`)

	// Customers with total > 100 AND at least 2 orders
	rows := mustQuery(t, db, `
		SELECT cust, SUM(amount) AS total, COUNT(*) AS cnt
		FROM orders
		GROUP BY cust
		HAVING SUM(amount) > 100 AND COUNT(*) >= 2
	`)
	if len(rows) != 1 {
		t.Fatalf("expected 1 customer, got %d: %v", len(rows), rows)
	}
	if rows[0]["cust"] != strVal("alice") {
		t.Errorf("expected alice, got %v", rows[0]["cust"])
	}
}

// ─── COALESCE ─────────────────────────────────────────────────────────────────

func TestCoalesceReturnsFirstNonNull(t *testing.T) {
	db := New()
	mustExec(t, db, `INSERT INTO t (a, b, c) VALUES (NULL, NULL, 42)`)
	mustExec(t, db, `INSERT INTO t (a, b, c) VALUES (NULL, 7,    99)`)
	mustExec(t, db, `INSERT INTO t (a, b, c) VALUES (1,    2,    3)`)

	// ORDER BY c ASC → rows in order c=3, c=42, c=99
	rows := mustQuery(t, db, `SELECT COALESCE(a, b, c) AS v FROM t ORDER BY c ASC`)
	// c=3:  a=1       → COALESCE returns 1
	// c=42: a=NULL, b=NULL → COALESCE falls through to c=42
	// c=99: a=NULL, b=7    → COALESCE returns 7
	if rows[0]["v"] != intVal(1) {
		t.Errorf("row 0 (c=3): expected 1, got %v", rows[0]["v"])
	}
	if rows[1]["v"] != intVal(42) {
		t.Errorf("row 1 (c=42): expected 42, got %v", rows[1]["v"])
	}
	if rows[2]["v"] != intVal(7) {
		t.Errorf("row 2 (c=99): expected 7, got %v", rows[2]["v"])
	}
}

func TestCoalesceAllNull(t *testing.T) {
	db := New()
	mustExec(t, db, `INSERT INTO t (a) VALUES (NULL)`)
	rows := mustQuery(t, db, `SELECT COALESCE(a, NULL) AS v FROM t`)
	if rows[0]["v"].Kind != KindNull {
		t.Errorf("COALESCE(NULL, NULL): expected NULL, got %v", rows[0]["v"])
	}
}

func TestCoalesceInSelect(t *testing.T) {
	db := New()
	mustExec(t, db, `INSERT INTO users (id, nickname, username) VALUES (1, NULL, 'alice')`)
	mustExec(t, db, `INSERT INTO users (id, nickname, username) VALUES (2, 'Bob', 'bob123')`)

	rows := mustQuery(t, db, `SELECT COALESCE(nickname, username) AS display FROM users ORDER BY id ASC`)
	if rows[0]["display"] != strVal("alice") {
		t.Errorf("row 0: expected alice (fallback to username), got %v", rows[0]["display"])
	}
	if rows[1]["display"] != strVal("Bob") {
		t.Errorf("row 1: expected Bob (nickname), got %v", rows[1]["display"])
	}
}

// ─── IFNULL ──────────────────────────────────────────────────────────────────

func TestIfNull(t *testing.T) {
	db := New()
	mustExec(t, db, `INSERT INTO t (v) VALUES (NULL)`)
	mustExec(t, db, `INSERT INTO t (v) VALUES (5)`)

	rows := mustQuery(t, db, `SELECT IFNULL(v, 0) AS v FROM t ORDER BY v ASC`)
	if rows[0]["v"] != intVal(0) {
		t.Errorf("IFNULL(NULL,0): expected 0, got %v", rows[0]["v"])
	}
	if rows[1]["v"] != intVal(5) {
		t.Errorf("IFNULL(5,0): expected 5, got %v", rows[1]["v"])
	}
}

// ─── NULLIF ──────────────────────────────────────────────────────────────────

func TestNullIf(t *testing.T) {
	db := New()
	mustExec(t, db, `INSERT INTO t (v) VALUES (0)`)
	mustExec(t, db, `INSERT INTO t (v) VALUES (5)`)

	// NULLIF(v, 0): turns 0 into NULL, leaves 5 alone
	rows := mustQuery(t, db, `SELECT NULLIF(v, 0) AS v FROM t ORDER BY v ASC`)
	nullRow := rows[0]
	if nullRow["v"].Kind != KindNull {
		t.Errorf("NULLIF(0,0): expected NULL, got %v", nullRow["v"])
	}
}

// ─── UPPER / LOWER ───────────────────────────────────────────────────────────

func TestUpperLower(t *testing.T) {
	db := New()
	mustExec(t, db, `INSERT INTO t (name) VALUES ('Hello World')`)

	rows := mustQuery(t, db, `SELECT UPPER(name) AS u, LOWER(name) AS l FROM t`)
	if rows[0]["u"] != strVal("HELLO WORLD") {
		t.Errorf("UPPER: expected 'HELLO WORLD', got %v", rows[0]["u"])
	}
	if rows[0]["l"] != strVal("hello world") {
		t.Errorf("LOWER: expected 'hello world', got %v", rows[0]["l"])
	}
}

func TestUpperLowerNull(t *testing.T) {
	db := New()
	mustExec(t, db, `INSERT INTO t (name) VALUES (NULL)`)
	rows := mustQuery(t, db, `SELECT UPPER(name) AS u, LOWER(name) AS l FROM t`)
	if rows[0]["u"].Kind != KindNull || rows[0]["l"].Kind != KindNull {
		t.Errorf("UPPER/LOWER(NULL): expected NULL, got %v %v", rows[0]["u"], rows[0]["l"])
	}
}

// ─── LENGTH ──────────────────────────────────────────────────────────────────

func TestLength(t *testing.T) {
	db := New()
	mustExec(t, db, `INSERT INTO t (s) VALUES ('hello')`)
	mustExec(t, db, `INSERT INTO t (s) VALUES ('')`)
	mustExec(t, db, `INSERT INTO t (s) VALUES (NULL)`)

	rows := mustQuery(t, db, `SELECT LENGTH(s) AS n FROM t ORDER BY n ASC`)
	if rows[0]["n"].Kind != KindNull {
		t.Errorf("LENGTH(NULL): expected NULL, got %v", rows[0]["n"])
	}
	if rows[1]["n"] != intVal(0) {
		t.Errorf("LENGTH(''): expected 0, got %v", rows[1]["n"])
	}
	if rows[2]["n"] != intVal(5) {
		t.Errorf("LENGTH('hello'): expected 5, got %v", rows[2]["n"])
	}
}

// ─── CONCAT ──────────────────────────────────────────────────────────────────

func TestConcat(t *testing.T) {
	db := New()
	mustExec(t, db, `INSERT INTO t (first, last) VALUES ('John', 'Doe')`)

	rows := mustQuery(t, db, `SELECT CONCAT(first, ' ', last) AS full_name FROM t`)
	if rows[0]["full_name"] != strVal("John Doe") {
		t.Errorf("CONCAT: expected 'John Doe', got %v", rows[0]["full_name"])
	}
}

func TestConcatNullPropagates(t *testing.T) {
	db := New()
	mustExec(t, db, `INSERT INTO t (a, b) VALUES ('hello', NULL)`)

	rows := mustQuery(t, db, `SELECT CONCAT(a, b) AS v FROM t`)
	if rows[0]["v"].Kind != KindNull {
		t.Errorf("CONCAT with NULL: expected NULL, got %v", rows[0]["v"])
	}
}

// ─── ABS ─────────────────────────────────────────────────────────────────────

func TestAbs(t *testing.T) {
	db := New()
	mustExec(t, db, `INSERT INTO t (v) VALUES (-5)`)
	mustExec(t, db, `INSERT INTO t (v) VALUES (3)`)
	mustExec(t, db, `INSERT INTO t (v) VALUES (0)`)

	rows := mustQuery(t, db, `SELECT ABS(v) AS a FROM t ORDER BY v ASC`)
	if rows[0]["a"] != intVal(5) {
		t.Errorf("ABS(-5): expected 5, got %v", rows[0]["a"])
	}
	if rows[1]["a"] != intVal(0) {
		t.Errorf("ABS(0): expected 0, got %v", rows[1]["a"])
	}
	if rows[2]["a"] != intVal(3) {
		t.Errorf("ABS(3): expected 3, got %v", rows[2]["a"])
	}
}

// ─── ROUND / FLOOR / CEIL ────────────────────────────────────────────────────

func TestRoundFloorCeil(t *testing.T) {
	db := New()
	mustExec(t, db, `INSERT INTO t (v) VALUES (3.7)`)

	rows := mustQuery(t, db, `SELECT ROUND(v) AS r, FLOOR(v) AS f, CEIL(v) AS c FROM t`)
	if rows[0]["r"] != intVal(4) {
		t.Errorf("ROUND(3.7): expected 4, got %v", rows[0]["r"])
	}
	if rows[0]["f"] != intVal(3) {
		t.Errorf("FLOOR(3.7): expected 3, got %v", rows[0]["f"])
	}
	if rows[0]["c"] != intVal(4) {
		t.Errorf("CEIL(3.7): expected 4, got %v", rows[0]["c"])
	}
}

func TestRoundDecimals(t *testing.T) {
	db := New()
	mustExec(t, db, `INSERT INTO t (v) VALUES (3.14159)`)

	rows := mustQuery(t, db, `SELECT ROUND(v, 2) AS r FROM t`)
	if !floatClose(rows[0]["r"], floatVal(3.14), 1e-9) {
		t.Errorf("ROUND(3.14159, 2): expected 3.14, got %v", rows[0]["r"])
	}
}

// ─── arithmetic expressions ───────────────────────────────────────────────────

func TestArithmeticInSelect(t *testing.T) {
	db := New()
	mustExec(t, db, `INSERT INTO t (price, qty) VALUES (10, 3)`)
	mustExec(t, db, `INSERT INTO t (price, qty) VALUES (5,  7)`)

	rows := mustQuery(t, db, `SELECT price * qty AS revenue FROM t ORDER BY revenue DESC`)
	if rows[0]["revenue"] != intVal(35) {
		t.Errorf("row 0: expected 35, got %v", rows[0]["revenue"])
	}
	if rows[1]["revenue"] != intVal(30) {
		t.Errorf("row 1: expected 30, got %v", rows[1]["revenue"])
	}
}

func TestArithmeticDivision(t *testing.T) {
	db := New()
	mustExec(t, db, `INSERT INTO t (a, b) VALUES (10, 4)`)

	rows := mustQuery(t, db, `SELECT a / b AS v FROM t`)
	if !floatClose(rows[0]["v"], floatVal(2.5), 1e-9) {
		t.Errorf("10/4: expected 2.5, got %v", rows[0]["v"])
	}
}

func TestArithmeticModulo(t *testing.T) {
	db := New()
	mustExec(t, db, `INSERT INTO t (v) VALUES (10)`)

	rows := mustQuery(t, db, `SELECT v % 3 AS r FROM t`)
	if rows[0]["r"] != intVal(1) {
		t.Errorf("10 %% 3: expected 1, got %v", rows[0]["r"])
	}
}

func TestDivisionByZeroIsNull(t *testing.T) {
	db := New()
	mustExec(t, db, `INSERT INTO t (v) VALUES (5)`)

	rows := mustQuery(t, db, `SELECT v / 0 AS r FROM t`)
	if rows[0]["r"].Kind != KindNull {
		t.Errorf("x/0: expected NULL, got %v", rows[0]["r"])
	}
}

// ─── aggregate + arithmetic together ─────────────────────────────────────────

func TestSumExpression(t *testing.T) {
	db := New()
	mustExec(t, db, `INSERT INTO t (price, qty) VALUES (10, 2)`)
	mustExec(t, db, `INSERT INTO t (price, qty) VALUES (5,  4)`)

	rows := mustQuery(t, db, `SELECT SUM(price * qty) AS revenue FROM t`)
	if rows[0]["revenue"] != intVal(40) {
		t.Errorf("SUM(price*qty): expected 40, got %v", rows[0]["revenue"])
	}
}

// ─── COALESCE + SUM: null-safe sum ───────────────────────────────────────────

func TestCoalesceWithSum(t *testing.T) {
	db := New()
	mustExec(t, db, `INSERT INTO t (v) VALUES (10)`)
	mustExec(t, db, `INSERT INTO t (v) VALUES (NULL)`)
	mustExec(t, db, `INSERT INTO t (v) VALUES (5)`)

	// Replace NULL with 0 before summing.
	rows := mustQuery(t, db, `SELECT SUM(COALESCE(v, 0)) AS s FROM t`)
	if rows[0]["s"] != intVal(15) {
		t.Errorf("SUM(COALESCE(v,0)): expected 15, got %v", rows[0]["s"])
	}
}

// ─── CASE WHEN ───────────────────────────────────────────────────────────────

func TestCaseSearched(t *testing.T) {
	db := New()
	mustExec(t, db, `INSERT INTO t (score) VALUES (95)`)
	mustExec(t, db, `INSERT INTO t (score) VALUES (75)`)
	mustExec(t, db, `INSERT INTO t (score) VALUES (55)`)
	mustExec(t, db, `INSERT INTO t (score) VALUES (40)`)

	rows := mustQuery(t, db, `
		SELECT score,
		       CASE
		           WHEN score >= 90 THEN 'A'
		           WHEN score >= 70 THEN 'B'
		           WHEN score >= 50 THEN 'C'
		           ELSE 'F'
		       END AS grade
		FROM t ORDER BY score DESC
	`)
	want := []string{"A", "B", "C", "F"}
	for i, g := range want {
		if rows[i]["grade"] != strVal(g) {
			t.Errorf("row %d (score=%v): expected %s, got %v", i, rows[i]["score"], g, rows[i]["grade"])
		}
	}
}

func TestCaseSimple(t *testing.T) {
	db := New()
	mustExec(t, db, `INSERT INTO t (status) VALUES ('active')`)
	mustExec(t, db, `INSERT INTO t (status) VALUES ('inactive')`)
	mustExec(t, db, `INSERT INTO t (status) VALUES ('pending')`)

	rows := mustQuery(t, db, `
		SELECT status,
		       CASE status
		           WHEN 'active'   THEN 1
		           WHEN 'inactive' THEN 0
		           ELSE -1
		       END AS code
		FROM t ORDER BY code DESC
	`)
	codes := []int64{1, 0, -1}
	for i, c := range codes {
		if rows[i]["code"] != intVal(c) {
			t.Errorf("row %d: expected code=%d, got %v", i, c, rows[i]["code"])
		}
	}
}

func TestCaseNoElseReturnsNull(t *testing.T) {
	db := New()
	mustExec(t, db, `INSERT INTO t (v) VALUES (99)`)

	rows := mustQuery(t, db, `SELECT CASE WHEN v = 1 THEN 'one' END AS r FROM t`)
	if rows[0]["r"].Kind != KindNull {
		t.Errorf("CASE with no matching WHEN and no ELSE: expected NULL, got %v", rows[0]["r"])
	}
}

// ─── IS NULL / IS NOT NULL ───────────────────────────────────────────────────

func TestIsNullIsNotNull(t *testing.T) {
	db := numDB(t)

	nullRows := mustQuery(t, db, `SELECT id FROM nums WHERE v IS NULL`)
	if len(nullRows) != 1 || nullRows[0]["id"] != intVal(4) {
		t.Errorf("IS NULL: expected id=4, got %v", nullRows)
	}

	notNullRows := mustQuery(t, db, `SELECT COUNT(*) AS n FROM nums WHERE v IS NOT NULL`)
	if notNullRows[0]["n"] != intVal(3) {
		t.Errorf("IS NOT NULL COUNT: expected 3, got %v", notNullRows[0]["n"])
	}
}

// ─── unary minus ─────────────────────────────────────────────────────────────

func TestUnaryMinus(t *testing.T) {
	db := New()
	mustExec(t, db, `INSERT INTO t (v) VALUES (7)`)

	rows := mustQuery(t, db, `SELECT -v AS neg FROM t`)
	if rows[0]["neg"] != intVal(-7) {
		t.Errorf("-v: expected -7, got %v", rows[0]["neg"])
	}
}

// ─── `||` pipe (parser OrExpr): string concat when a string is involved, else OR ─

func TestPipeOrStringConcatChained(t *testing.T) {
	db := New()
	rows := mustQuery(t, db, `SELECT '%'||'x'||'%' AS p FROM DUAL`)
	if rows[0]["p"] != strVal("%x%") {
		t.Fatalf("want %%x%%, got %v", rows[0]["p"])
	}
}

func TestPipeOrLikePattern(t *testing.T) {
	db := New()
	mustExec(t, db, `INSERT INTO t (name) VALUES ('hello')`)
	mustExec(t, db, `INSERT INTO t (name) VALUES ('goodbye')`)
	// Parentheses keep the pattern as one expression; without them this parser
	// treats `like a||b` like `like a or b` at the LIKE precedence boundary.
	rows := mustQuery(t, db, `SELECT name FROM t WHERE name LIKE ('%'||'hell'||'%')`)
	if len(rows) != 1 || rows[0]["name"] != strVal("hello") {
		t.Fatalf("want one row hello, got %+v", rows)
	}
}

func TestPipeOrNumericBooleanOr(t *testing.T) {
	db := New()
	rows := mustQuery(t, db, `SELECT 0||1 AS a, 1||0 AS b, 0||0 AS c FROM DUAL`)
	if rows[0]["a"] != boolVal(true) {
		t.Fatalf("0||1: want true, got %v", rows[0]["a"])
	}
	if rows[0]["b"] != boolVal(true) {
		t.Fatalf("1||0: want true, got %v", rows[0]["b"])
	}
	if rows[0]["c"] != boolVal(false) {
		t.Fatalf("0||0: want false, got %v", rows[0]["c"])
	}
}

func TestPipeOrNullWithString(t *testing.T) {
	db := New()
	rows := mustQuery(t, db, `SELECT NULL||'x' AS p, 'x'||NULL AS q FROM DUAL`)
	if rows[0]["p"].Kind != KindNull || rows[0]["q"].Kind != KindNull {
		t.Fatalf("want NULL NULL, got %v %v", rows[0]["p"], rows[0]["q"])
	}
}

func TestPipeOrBothStringsConcat(t *testing.T) {
	db := New()
	rows := mustQuery(t, db, `SELECT 'a'||'b' AS p FROM DUAL`)
	if rows[0]["p"] != strVal("ab") {
		t.Fatalf("want ab, got %v", rows[0]["p"])
	}
}

func TestPipeOrWhereNumericTruth(t *testing.T) {
	db := New()
	rows := mustQuery(t, db, `SELECT 1 AS n FROM DUAL WHERE 0||1`)
	if len(rows) != 1 {
		t.Fatalf("WHERE 0||1: want one row, got %d", len(rows))
	}
}

// ─── combined: COALESCE + CASE + aggregate in one query ───────────────────────

func TestCombinedFunctions(t *testing.T) {
	db := New()
	mustExec(t, db, `INSERT INTO products (name, price, discount) VALUES ('alpha', 100, 10)`)
	mustExec(t, db, `INSERT INTO products (name, price, discount) VALUES ('beta',  200, NULL)`)
	mustExec(t, db, `INSERT INTO products (name, price, discount) VALUES ('gamma', 50,  5)`)

	// Effective price = price - COALESCE(discount, 0)
	// Label = CASE WHEN effective < 100 THEN 'cheap' ELSE 'normal' END
	rows := mustQuery(t, db, `
		SELECT
		    name,
		    price - COALESCE(discount, 0) AS effective,
		    CASE WHEN price - COALESCE(discount, 0) < 100 THEN 'cheap' ELSE 'normal' END AS label
		FROM products
		ORDER BY effective ASC
	`)
	if len(rows) != 3 {
		t.Fatalf("expected 3 rows, got %d", len(rows))
	}

	// gamma: 50-5=45 cheap, alpha: 100-10=90 cheap, beta: 200-0=200 normal
	wantEffective := []int64{45, 90, 200}
	wantLabels := []string{"cheap", "cheap", "normal"}
	for i := range rows {
		if rows[i]["effective"] != intVal(wantEffective[i]) {
			t.Errorf("row %d: effective want %d got %v", i, wantEffective[i], rows[i]["effective"])
		}
		if rows[i]["label"] != strVal(wantLabels[i]) {
			t.Errorf("row %d: label want %s got %v", i, wantLabels[i], rows[i]["label"])
		}
	}
}
