package vapordb

import (
	"testing"
	"time"
)

func TestCastStringToSignedInt(t *testing.T) {
	db := New()
	rows := mustQuery(t, db, `SELECT CAST('42' AS SIGNED) AS n FROM DUAL`)
	if rows[0]["n"] != intVal(42) {
		t.Fatalf("want 42, got %v", rows[0]["n"])
	}
}

func TestCastFloatTruncatesToSignedInt(t *testing.T) {
	db := New()
	rows := mustQuery(t, db, `SELECT CAST(3.7 AS SIGNED) AS n FROM DUAL`)
	if rows[0]["n"] != intVal(3) {
		t.Fatalf("want 3, got %v", rows[0]["n"])
	}
}

func TestCastNegativeToUnsignedZero(t *testing.T) {
	db := New()
	rows := mustQuery(t, db, `SELECT CAST(-1 AS UNSIGNED) AS n FROM DUAL`)
	if rows[0]["n"] != intVal(0) {
		t.Fatalf("want 0, got %v", rows[0]["n"])
	}
}

func TestCastIntToCharString(t *testing.T) {
	db := New()
	rows := mustQuery(t, db, `SELECT CAST(7 AS CHAR) AS s FROM DUAL`)
	if rows[0]["s"] != strVal("7") {
		t.Fatalf("want '7', got %v", rows[0]["s"])
	}
}

func TestCastStringToDate(t *testing.T) {
	db := New()
	rows := mustQuery(t, db, `SELECT CAST('2020-03-15' AS DATE) AS d FROM DUAL`)
	v := rows[0]["d"]
	if v.Kind != KindDate {
		t.Fatalf("want KindDate, got %v", v.Kind)
	}
	got := v.V.(time.Time).UTC()
	want := time.Date(2020, 3, 15, 0, 0, 0, 0, time.UTC)
	if !got.Equal(want) {
		t.Fatalf("want %v, got %v", want, got)
	}
}

func TestCastToBool(t *testing.T) {
	db := New()
	// Parser accepts SIGNED for BOOLEAN-like coercions in MySQL dialect.
	rows := mustQuery(t, db, `SELECT CAST(0 AS SIGNED) AS a, CAST(1 AS SIGNED) AS b FROM DUAL`)
	if rows[0]["a"] != intVal(0) || rows[0]["b"] != intVal(1) {
		t.Fatalf("want 0 and 1, got %v %v", rows[0]["a"], rows[0]["b"])
	}
}

func TestCastStringToDecimalFloat(t *testing.T) {
	db := New()
	rows := mustQuery(t, db, `SELECT CAST('2.5' AS DECIMAL(10,4)) AS x FROM DUAL`)
	if rows[0]["x"] != floatVal(2.5) {
		t.Fatalf("want 2.5, got %v", rows[0]["x"])
	}
}

func TestCastNullStaysNull(t *testing.T) {
	db := New()
	rows := mustQuery(t, db, `SELECT CAST(NULL AS SIGNED) AS n FROM DUAL`)
	if rows[0]["n"].Kind != KindNull {
		t.Fatalf("want NULL, got %v", rows[0]["n"])
	}
}

func TestCastInWhere(t *testing.T) {
	db := New()
	mustExec(t, db, `INSERT INTO t (id, v) VALUES (1, '10'), (2, '20')`)
	rows := mustQuery(t, db, `SELECT id FROM t WHERE CAST(v AS SIGNED) > 15 ORDER BY id`)
	if len(rows) != 1 || rows[0]["id"] != intVal(2) {
		t.Fatalf("want id=2, got %v", rows)
	}
}

func TestCastUnsupportedTypeError(t *testing.T) {
	db := New()
	_, err := db.Query(`SELECT CAST('x' AS JSON) AS j FROM DUAL`)
	if err == nil {
		t.Fatal("expected error for unsupported CAST target")
	}
}
