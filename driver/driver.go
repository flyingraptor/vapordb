// Package driver registers vapordb as a database/sql driver under the name
// "vapordb". Import it as a side effect to enable sql.Open:
//
//	import _ "github.com/flyingraptor/vapordb/driver"
//
//	db.Register("mydb", vdb)
//	sqlDB, _ := sql.Open("vapordb", "mydb")
//
// Positional parameters (? MySQL/SQLite-style or $1/$2 Postgres-style) are
// rewritten to SQL literals before being passed to the vapordb engine.
// Column order in result sets is alphabetical; ORMs that match by column name
// (sqlx, bun, …) work transparently.
package driver

import (
	"database/sql"
	sqld "database/sql/driver"
	"encoding/json"
	"fmt"
	"io"
	"reflect"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/flyingraptor/vapordb"
)

func init() {
	sql.Register("vapordb", &Driver{})
}

// registry maps DSN name strings to *vapordb.DB instances.
var registry sync.Map

// aliasRegistered tracks SQL driver names registered via RegisterAs so we
// don't panic on repeated calls with the same name.
var aliasRegistered sync.Map

// Register associates name with db so that sql.Open("vapordb", name) returns a
// connection backed by db. Use an empty string as name for the default instance.
func Register(name string, db *vapordb.DB) {
	registry.Store(name, db)
}

// Unregister removes a previously registered database.
func Unregister(name string) {
	registry.Delete(name)
}

// RegisterAs registers the vapordb SQL driver under an additional driver name.
// Its primary use is sqlx integration: sqlx infers the placeholder bind style
// from the name passed to sql.Open / sqlx.Open, so registering under a known
// name lets sqlx choose the correct style automatically.
//
// Common patterns:
//
//	// Option A — register once (e.g. in TestMain), then open like a real driver:
//	driver.RegisterAs("pgx")               // sqlx treats this as $N (DOLLAR) style
//	db := sqlx.MustOpen("pgx", "mydb")
//
//	// Option B — no registration needed; override the bind style after Open:
//	sqlDB, _ := sql.Open("vapordb", "mydb")
//	db := sqlx.NewDb(sqlDB, "postgres")    // explicit $N style, no conflict risk
//
// RegisterAs is idempotent: repeated calls with the same name are silently
// ignored. If another driver has already claimed sqlDriverName, the call is
// also silently ignored so it is safe to call even when lib/pq or pgx may be
// imported in the same binary.
func RegisterAs(sqlDriverName string) {
	if _, alreadyOurs := aliasRegistered.LoadOrStore(sqlDriverName, struct{}{}); alreadyOurs {
		return
	}
	// sql.Register panics on duplicate; swallow the panic if another driver
	// already owns the name (e.g. pgx imported alongside vapordb in tests).
	func() {
		defer func() { recover() }() //nolint:errcheck
		sql.Register(sqlDriverName, &Driver{})
	}()
}

// ── Driver ────────────────────────────────────────────────────────────────────

// Driver implements database/sql/driver.Driver.
type Driver struct{}

func (d *Driver) Open(name string) (sqld.Conn, error) {
	v, ok := registry.Load(name)
	if !ok {
		return nil, fmt.Errorf("vapordb: no database registered under %q; call driver.Register first", name)
	}
	return &conn{db: v.(*vapordb.DB)}, nil
}

// ── Conn ─────────────────────────────────────────────────────────────────────

type conn struct {
	db *vapordb.DB
	tx *vapordb.Tx
}

func (c *conn) Prepare(query string) (sqld.Stmt, error) {
	return &stmt{conn: c, query: query}, nil
}

func (c *conn) Close() error { return nil }

func (c *conn) Begin() (sqld.Tx, error) {
	if c.tx != nil {
		return nil, fmt.Errorf("vapordb: transaction already active")
	}
	tx, err := c.db.Begin()
	if err != nil {
		return nil, err
	}
	c.tx = tx
	return &txWrap{conn: c, tx: tx}, nil
}

func (c *conn) execSQL(q string) error {
	if c.tx != nil {
		return c.tx.Exec(q)
	}
	return c.db.Exec(q)
}

func (c *conn) querySQL(q string) ([]vapordb.Row, error) {
	if c.tx != nil {
		return c.tx.Query(q)
	}
	return c.db.Query(q)
}

// ── Tx ───────────────────────────────────────────────────────────────────────

type txWrap struct {
	conn *conn
	tx   *vapordb.Tx
}

func (t *txWrap) Commit() error {
	t.conn.tx = nil
	return t.tx.Commit()
}

func (t *txWrap) Rollback() error {
	t.conn.tx = nil
	return t.tx.Rollback()
}

// ── Stmt ─────────────────────────────────────────────────────────────────────

type stmt struct {
	conn  *conn
	query string
}

func (s *stmt) Close() error { return nil }

// NumInput returns the number of positional placeholders (? or $N) in the
// query. Returns -1 when the query uses no positional placeholders (so the
// driver accepts any number of args, including zero).
func (s *stmt) NumInput() int {
	return countPlaceholders(s.query)
}

func (s *stmt) Exec(args []sqld.Value) (sqld.Result, error) {
	q, err := bindArgs(s.query, args)
	if err != nil {
		return nil, err
	}
	if err := s.conn.execSQL(q); err != nil {
		return nil, err
	}
	return sqld.RowsAffected(0), nil
}

func (s *stmt) Query(args []sqld.Value) (sqld.Rows, error) {
	q, err := bindArgs(s.query, args)
	if err != nil {
		return nil, err
	}
	rows, err := s.conn.querySQL(q)
	if err != nil {
		return nil, err
	}
	return newDriverRows(rows), nil
}

// ── Rows ─────────────────────────────────────────────────────────────────────

type driverRows struct {
	cols []string
	data []vapordb.Row
	idx  int
}

func newDriverRows(data []vapordb.Row) *driverRows {
	r := &driverRows{data: data}
	if len(data) > 0 {
		cols := make([]string, 0, len(data[0]))
		for k := range data[0] {
			cols = append(cols, k)
		}
		sort.Strings(cols)
		r.cols = cols
	}
	return r
}

func (r *driverRows) Columns() []string {
	if r.cols == nil {
		return []string{}
	}
	return r.cols
}

func (r *driverRows) Close() error { return nil }

func (r *driverRows) Next(dest []sqld.Value) error {
	if r.idx >= len(r.data) {
		return io.EOF
	}
	row := r.data[r.idx]
	r.idx++
	for i, col := range r.cols {
		dest[i] = vaporValueToDriver(row[col])
	}
	return nil
}

// ── parameter binding ─────────────────────────────────────────────────────────

var (
	posPlaceholderRE = regexp.MustCompile(`\?`)
	dollarPlaceholderRE = regexp.MustCompile(`\$(\d+)`)
)

// countPlaceholders returns the number of positional placeholders in query.
// Returns -1 when none are found (named params or no params).
func countPlaceholders(query string) int {
	// Check for $N style first
	matches := dollarPlaceholderRE.FindAllString(query, -1)
	if len(matches) > 0 {
		max := 0
		for _, m := range matches {
			n, _ := strconv.Atoi(m[1:])
			if n > max {
				max = n
			}
		}
		return max
	}
	// Count ? placeholders (outside of string literals)
	count := 0
	inStr := false
	for i := 0; i < len(query); i++ {
		ch := query[i]
		if ch == '\'' {
			if inStr {
				// Check for escaped quote ''
				if i+1 < len(query) && query[i+1] == '\'' {
					i++
				} else {
					inStr = false
				}
			} else {
				inStr = true
			}
			continue
		}
		if !inStr && ch == '?' {
			count++
		}
	}
	if count == 0 {
		return -1
	}
	return count
}

// bindArgs substitutes positional placeholders (? or $N) with SQL literal
// representations of the provided driver.Value arguments.
func bindArgs(query string, args []sqld.Value) (string, error) {
	if len(args) == 0 {
		return query, nil
	}

	// Detect Postgres $N style
	if dollarPlaceholderRE.MatchString(query) {
		return bindDollarArgs(query, args)
	}

	// MySQL ? style: replace in order, skipping inside string literals
	return bindQMarkArgs(query, args)
}

func bindDollarArgs(query string, args []sqld.Value) (string, error) {
	return dollarPlaceholderRE.ReplaceAllStringFunc(query, func(m string) string {
		n, _ := strconv.Atoi(m[1:])
		if n < 1 || n > len(args) {
			return m
		}
		return driverValueToSQL(args[n-1])
	}), nil
}

func bindQMarkArgs(query string, args []sqld.Value) (string, error) {
	var sb strings.Builder
	argIdx := 0
	inStr := false
	i := 0
	for i < len(query) {
		ch := query[i]
		if ch == '\'' {
			if inStr {
				sb.WriteByte(ch)
				i++
				if i < len(query) && query[i] == '\'' {
					sb.WriteByte(query[i])
					i++
				} else {
					inStr = false
				}
			} else {
				inStr = true
				sb.WriteByte(ch)
				i++
			}
			continue
		}
		if !inStr && ch == '?' {
			if argIdx >= len(args) {
				return "", fmt.Errorf("vapordb driver: not enough arguments for query (got %d, need more)", len(args))
			}
			sb.WriteString(driverValueToSQL(args[argIdx]))
			argIdx++
			i++
			continue
		}
		sb.WriteByte(ch)
		i++
	}
	return sb.String(), nil
}

// driverValueToSQL converts a driver.Value to its SQL literal representation.
func driverValueToSQL(v sqld.Value) string {
	switch x := v.(type) {
	case nil:
		return "NULL"
	case bool:
		if x {
			return "1"
		}
		return "0"
	case int64:
		return strconv.FormatInt(x, 10)
	case float64:
		return strconv.FormatFloat(x, 'f', -1, 64)
	case string:
		// Detect PostgreSQL array literals produced by pq.Array(slice).Value()
		// or by CheckNamedValue below:  {elem1,elem2,...} → 'elem1', 'elem2', ...
		if expanded, ok := driverExpandPGArray(x); ok {
			return expanded
		}
		return "'" + strings.ReplaceAll(x, "'", "''") + "'"
	case []byte:
		return "'" + strings.ReplaceAll(string(x), "'", "''") + "'"
	case time.Time:
		if x.IsZero() {
			return "NULL"
		}
		return fmt.Sprintf("DATE('%s')", x.UTC().Format("2006-01-02 15:04:05"))
	default:
		return "'" + strings.ReplaceAll(fmt.Sprintf("%v", x), "'", "''") + "'"
	}
}

// ── PostgreSQL array / slice parameter support ────────────────────────────────

// CheckNamedValue implements driver.NamedValueChecker on stmt.
// It is called by database/sql before default type conversion and lets us
// intercept plain Go slices (e.g. []string, []int64) that are not valid
// driver.Value types, converting them to PostgreSQL array literal strings
// so driverValueToSQL can expand them properly.
//
// []byte is skipped — it is a valid driver.Value and should pass through.
func (s *stmt) CheckNamedValue(nv *sqld.NamedValue) error {
	rv := reflect.ValueOf(nv.Value)
	for rv.Kind() == reflect.Ptr {
		if rv.IsNil() {
			nv.Value = nil
			return nil
		}
		rv = rv.Elem()
	}
	if rv.Kind() != reflect.Slice && rv.Kind() != reflect.Array {
		return sqld.ErrSkip // use standard database/sql conversion
	}
	// []byte is a valid driver.Value — leave it alone.
	if rv.Type().Elem().Kind() == reflect.Uint8 {
		return sqld.ErrSkip
	}
	// Convert to PostgreSQL array literal: {elem1,elem2,...}
	nv.Value = driverSliceToPGArray(rv)
	return nil
}

// driverSliceToPGArray converts a Go slice (reflected) into a PostgreSQL
// array literal string like {A001,A002} so that driverValueToSQL can expand
// it to 'A001', 'A002' for use inside IN (…).
func driverSliceToPGArray(rv reflect.Value) string {
	if rv.Len() == 0 {
		return "{}"
	}
	parts := make([]string, rv.Len())
	for i := 0; i < rv.Len(); i++ {
		parts[i] = fmt.Sprintf("%v", rv.Index(i).Interface())
	}
	return "{" + strings.Join(parts, ",") + "}"
}

// driverExpandPGArray detects a PostgreSQL array literal "{elem,...}" and
// expands it to a SQL comma-separated list of literals suitable for IN (…).
// Returns ("NULL", true) for an empty array {}.
// Returns ("", false) if s is not a PostgreSQL array literal.
func driverExpandPGArray(s string) (string, bool) {
	if len(s) < 2 || s[0] != '{' || s[len(s)-1] != '}' {
		return "", false
	}
	inner := s[1 : len(s)-1]
	if inner == "" {
		return "NULL", true
	}
	elems := driverParsePGArrayElems(inner)
	lits := make([]string, len(elems))
	for i, e := range elems {
		if strings.EqualFold(e, "NULL") {
			lits[i] = "NULL"
		} else if _, err := strconv.ParseInt(e, 10, 64); err == nil {
			lits[i] = e
		} else if _, err := strconv.ParseFloat(e, 64); err == nil {
			lits[i] = e
		} else {
			lits[i] = "'" + strings.ReplaceAll(e, "'", "''") + "'"
		}
	}
	return strings.Join(lits, ", "), true
}

// driverParsePGArrayElems splits the inner content of a PostgreSQL array
// literal by commas, handling double-quoted elements.
func driverParsePGArrayElems(s string) []string {
	var elems []string
	var cur strings.Builder
	i := 0
	for i < len(s) {
		switch s[i] {
		case '"':
			i++
			for i < len(s) && s[i] != '"' {
				if s[i] == '\\' {
					i++
					if i < len(s) {
						cur.WriteByte(s[i])
						i++
					}
				} else {
					cur.WriteByte(s[i])
					i++
				}
			}
			if i < len(s) {
				i++
			}
		case ',':
			elems = append(elems, cur.String())
			cur.Reset()
			i++
		default:
			cur.WriteByte(s[i])
			i++
		}
	}
	elems = append(elems, cur.String())
	return elems
}

// marshalJSON wraps json.Marshal for use in this package.
func marshalJSON(v any) ([]byte, error) {
	return json.Marshal(v)
}

// vaporValueToDriver converts a vapordb Value to a driver.Value.
func vaporValueToDriver(v vapordb.Value) sqld.Value {
	switch v.Kind {
	case vapordb.KindNull:
		return nil
	case vapordb.KindBool:
		return v.V.(bool)
	case vapordb.KindInt:
		return v.V.(int64)
	case vapordb.KindFloat:
		return v.V.(float64)
	case vapordb.KindString:
		return v.V.(string)
	case vapordb.KindDate:
		return v.V.(time.Time)
	case vapordb.KindJSON:
		// Return JSON as a string so standard sql.Scanner can handle it.
		b, err := marshalJSON(v.V)
		if err != nil {
			return nil
		}
		return string(b)
	default:
		return nil
	}
}
