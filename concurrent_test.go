package vapordb

import (
	"fmt"
	"sync"
	"testing"
)

// Each test in this file is designed to be run with -race so the detector can
// catch any missing synchronisation. They are also valid correctness tests on
// their own (row counts, absence of panics, etc.).

// TestConcurrentReads verifies that many goroutines can SELECT simultaneously
// without corrupting each other's results.
func TestConcurrentReads(t *testing.T) {
	db := New()
	for i := 0; i < 100; i++ {
		mustExec(t, db, fmt.Sprintf(`INSERT INTO items (id, val) VALUES (%d, %d)`, i, i*10))
	}

	const readers = 20
	var wg sync.WaitGroup
	wg.Add(readers)
	for r := 0; r < readers; r++ {
		go func() {
			defer wg.Done()
			rows, err := db.Query(`SELECT id, val FROM items WHERE val >= 0 ORDER BY id`)
			if err != nil {
				t.Errorf("concurrent Query: %v", err)
				return
			}
			if len(rows) != 100 {
				t.Errorf("expected 100 rows, got %d", len(rows))
			}
		}()
	}
	wg.Wait()
}

// TestConcurrentWrites verifies that concurrent INSERTs from many goroutines
// all land without panic or lost rows.
func TestConcurrentWrites(t *testing.T) {
	db := New()
	const writers = 50

	var wg sync.WaitGroup
	wg.Add(writers)
	for w := 0; w < writers; w++ {
		w := w
		go func() {
			defer wg.Done()
			if err := db.Exec(fmt.Sprintf(`INSERT INTO counter (n) VALUES (%d)`, w)); err != nil {
				t.Errorf("concurrent Exec: %v", err)
			}
		}()
	}
	wg.Wait()

	rows := mustQuery(t, db, `SELECT n FROM counter`)
	if len(rows) != writers {
		t.Errorf("expected %d rows after concurrent inserts, got %d", writers, len(rows))
	}
}

// TestConcurrentReadWrite exercises simultaneous readers and writers on the
// same table. Readers must never see a torn / partial state.
func TestConcurrentReadWrite(t *testing.T) {
	db := New()
	// Seed one row so readers always have something to scan.
	mustExec(t, db, `INSERT INTO events (id, msg) VALUES (0, 'seed')`)

	const (
		writers = 10
		readers = 10
		inserts = 5
	)

	var wg sync.WaitGroup
	wg.Add(writers + readers)

	for w := 0; w < writers; w++ {
		w := w
		go func() {
			defer wg.Done()
			for i := 0; i < inserts; i++ {
				id := w*inserts + i + 1
				if err := db.Exec(fmt.Sprintf(`INSERT INTO events (id, msg) VALUES (%d, 'w%d-i%d')`, id, w, i)); err != nil {
					t.Errorf("writer Exec: %v", err)
				}
			}
		}()
	}

	for r := 0; r < readers; r++ {
		go func() {
			defer wg.Done()
			for i := 0; i < inserts; i++ {
				if _, err := db.Query(`SELECT id, msg FROM events ORDER BY id`); err != nil {
					t.Errorf("reader Query: %v", err)
				}
			}
		}()
	}

	wg.Wait()

	// After all goroutines finish every INSERT must be present.
	rows := mustQuery(t, db, `SELECT id FROM events`)
	want := writers*inserts + 1 // +1 for the seed row
	if len(rows) != want {
		t.Errorf("expected %d rows, got %d", want, len(rows))
	}
}

// TestConcurrentReturning ensures that concurrent DML+RETURNING calls (which
// take the write lock) do not race with each other or with plain SELECTs.
func TestConcurrentReturning(t *testing.T) {
	db := New()

	const ops = 30
	var wg sync.WaitGroup
	wg.Add(ops * 2)

	// Half goroutines: INSERT … RETURNING
	for i := 0; i < ops; i++ {
		i := i
		go func() {
			defer wg.Done()
			_, err := db.Query(fmt.Sprintf(
				`INSERT INTO log (id, note) VALUES (%d, 'entry') RETURNING id`, i,
			))
			if err != nil {
				t.Errorf("RETURNING Query: %v", err)
			}
		}()
	}

	// Half goroutines: plain SELECT
	for i := 0; i < ops; i++ {
		go func() {
			defer wg.Done()
			if _, err := db.Query(`SELECT id, note FROM log`); err != nil {
				t.Errorf("plain Query: %v", err)
			}
		}()
	}

	wg.Wait()

	rows := mustQuery(t, db, `SELECT id FROM log`)
	if len(rows) != ops {
		t.Errorf("expected %d rows, got %d", ops, len(rows))
	}
}

// TestConcurrentSave ensures Save can run alongside reads without a data race.
func TestConcurrentSave(t *testing.T) {
	db := New()
	for i := 0; i < 20; i++ {
		mustExec(t, db, fmt.Sprintf(`INSERT INTO snap (id) VALUES (%d)`, i))
	}

	path := t.TempDir() + "/snap.json"
	const goroutines = 10
	var wg sync.WaitGroup
	wg.Add(goroutines * 2)

	for i := 0; i < goroutines; i++ {
		go func() {
			defer wg.Done()
			if err := db.Save(path); err != nil {
				t.Errorf("Save: %v", err)
			}
		}()
		go func() {
			defer wg.Done()
			if _, err := db.Query(`SELECT id FROM snap`); err != nil {
				t.Errorf("Query during Save: %v", err)
			}
		}()
	}

	wg.Wait()
}
