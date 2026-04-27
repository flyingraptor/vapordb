package vapordb

import (
	"bufio"
	"encoding/json"
	"os"
	"strings"
	"testing"
)

// readLogEntries parses a JSON Lines query-log file into a slice of maps.
func readLogEntries(t *testing.T, path string) []map[string]any {
	t.Helper()
	f, err := os.Open(path)
	if err != nil {
		t.Fatalf("open log: %v", err)
	}
	defer f.Close()
	var entries []map[string]any
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		var m map[string]any
		if err := json.Unmarshal(sc.Bytes(), &m); err != nil {
			t.Fatalf("parse log line: %v", err)
		}
		entries = append(entries, m)
	}
	return entries
}

func TestQueryLog_NoLogBeforeSave(t *testing.T) {
	db := New()
	mustExec(t, db, `INSERT INTO t (v) VALUES (1)`)
	// No Save called — logPath is nil, nothing written to disk.
	if db.logPath.Load() != nil {
		t.Fatalf("expected nil logPath before Save")
	}
}

func TestQueryLog_LogPathDerivedFromSavePath(t *testing.T) {
	dir := t.TempDir()
	path := dir + "/db.json"
	db := New()
	mustExec(t, db, `INSERT INTO t (v) VALUES (1)`)
	if err := db.Save(path); err != nil {
		t.Fatal(err)
	}
	expected := dir + "/db_queries.jsonl"
	p := db.logPath.Load()
	if p == nil || *p != expected {
		t.Fatalf("expected log path %q, got %v", expected, p)
	}
}

func TestQueryLog_ExecWritesEntry(t *testing.T) {
	dir := t.TempDir()
	db := New()
	mustExec(t, db, `INSERT INTO t (v) VALUES (1)`)
	if err := db.Save(dir + "/db.json"); err != nil {
		t.Fatal(err)
	}

	mustExec(t, db, `INSERT INTO t (v) VALUES (2)`)
	mustExec(t, db, `INSERT INTO t (v) VALUES (3)`)

	entries := readLogEntries(t, dir+"/db_queries.jsonl")
	if len(entries) != 2 {
		t.Fatalf("expected 2 log entries, got %d", len(entries))
	}
	for _, e := range entries {
		if e["op"] != "exec" {
			t.Errorf("expected op=exec, got %v", e["op"])
		}
		if _, ok := e["ts"]; !ok {
			t.Error("missing ts field")
		}
		if _, ok := e["duration_ms"]; !ok {
			t.Error("missing duration_ms field")
		}
	}
}

func TestQueryLog_QueryWritesEntry(t *testing.T) {
	dir := t.TempDir()
	db := New()
	mustExec(t, db, `INSERT INTO t (v) VALUES (1)`)
	mustExec(t, db, `INSERT INTO t (v) VALUES (2)`)
	if err := db.Save(dir + "/db.json"); err != nil {
		t.Fatal(err)
	}

	rows := mustQuery(t, db, `SELECT v FROM t`)
	if len(rows) != 2 {
		t.Fatalf("unexpected row count %d", len(rows))
	}

	entries := readLogEntries(t, dir+"/db_queries.jsonl")
	if len(entries) != 1 {
		t.Fatalf("expected 1 log entry, got %d", len(entries))
	}
	e := entries[0]
	if e["op"] != "query" {
		t.Errorf("expected op=query, got %v", e["op"])
	}
	if e["rows"].(float64) != 2 {
		t.Errorf("expected rows=2, got %v", e["rows"])
	}
}

func TestQueryLog_ErrorsAreLogged(t *testing.T) {
	dir := t.TempDir()
	db := New()
	mustExec(t, db, `INSERT INTO t (v) VALUES (1)`)
	db.LockTable("t")
	if err := db.Save(dir + "/db.json"); err != nil {
		t.Fatal(err)
	}

	_ = db.Exec(`INSERT INTO t (v, newcol) VALUES (2, 'x')`) // should fail

	entries := readLogEntries(t, dir+"/db_queries.jsonl")
	if len(entries) != 1 {
		t.Fatalf("expected 1 entry, got %d", len(entries))
	}
	errField, ok := entries[0]["error"]
	if !ok || !strings.Contains(errField.(string), "schema-locked") {
		t.Errorf("expected error field with schema-locked, got %v", errField)
	}
}

func TestQueryLog_LoadEnablesLogging(t *testing.T) {
	dir := t.TempDir()
	path := dir + "/db.json"

	// First DB: save a snapshot.
	db := New()
	mustExec(t, db, `INSERT INTO t (v) VALUES (1)`)
	if err := db.Save(path); err != nil {
		t.Fatal(err)
	}

	// Second DB: load and run a query — should be logged.
	db2 := New()
	if err := db2.Load(path); err != nil {
		t.Fatal(err)
	}
	mustQuery(t, db2, `SELECT v FROM t`)

	entries := readLogEntries(t, dir+"/db_queries.jsonl")
	if len(entries) != 1 {
		t.Fatalf("expected 1 log entry after Load, got %d", len(entries))
	}
}

func TestQueryLog_AppendsBetweenSaves(t *testing.T) {
	dir := t.TempDir()
	path := dir + "/db.json"
	db := New()
	mustExec(t, db, `INSERT INTO t (v) VALUES (1)`)
	if err := db.Save(path); err != nil { // enables logging from here on
		t.Fatal(err)
	}

	mustExec(t, db, `INSERT INTO t (v) VALUES (2)`) // entry 1
	mustExec(t, db, `INSERT INTO t (v) VALUES (3)`) // entry 2
	if err := db.Save(path); err != nil {
		t.Fatal(err)
	}
	mustExec(t, db, `INSERT INTO t (v) VALUES (4)`) // entry 3

	entries := readLogEntries(t, dir+"/db_queries.jsonl")
	if len(entries) != 3 {
		t.Fatalf("expected 3 log entries, got %d", len(entries))
	}
}
