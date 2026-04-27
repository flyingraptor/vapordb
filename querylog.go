package vapordb

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"time"
)

// logEntry is one line in the query log (JSON Lines format).
type logEntry struct {
	Ts         string  `json:"ts"`
	Op         string  `json:"op"`
	SQL        string  `json:"sql"`
	DurationMs float64 `json:"duration_ms"`
	Rows       int     `json:"rows,omitempty"`
	Error      string  `json:"error,omitempty"`
}

// logPathFor derives the query-log file path from the JSON snapshot path.
//
//	"db.json"          → "db_queries.jsonl"
//	"/data/snap.json"  → "/data/snap_queries.jsonl"
func logPathFor(snapshotPath string) string {
	ext := filepath.Ext(snapshotPath)
	base := strings.TrimSuffix(snapshotPath, ext)
	return base + "_queries.jsonl"
}

// appendQueryLog writes one entry to the query log file at logPath.
// If logPath is empty the call is a no-op. Errors are silently discarded
// so a log write failure never interrupts the caller.
func appendQueryLog(logPath, op, sql string, rows int, dur time.Duration, execErr error) {
	if logPath == "" {
		return
	}
	e := logEntry{
		Ts:         time.Now().UTC().Format(time.RFC3339Nano),
		Op:         op,
		SQL:        sql,
		DurationMs: float64(dur.Microseconds()) / 1000.0,
		Rows:       rows,
	}
	if execErr != nil {
		e.Error = execErr.Error()
	}
	data, err := json.Marshal(e)
	if err != nil {
		return
	}
	data = append(data, '\n')
	f, err := os.OpenFile(logPath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o644)
	if err != nil {
		return
	}
	defer f.Close()
	_, _ = f.Write(data)
}
