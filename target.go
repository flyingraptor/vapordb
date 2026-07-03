package vapordb

import "regexp"

// Target names the real database a user intends to migrate to once their data
// model stabilises. Declaring it early (via WithTarget) turns vapordb from a
// permissive sandbox into a dialect-aware one: statements are checked for
// constructs that would not port to the target, and GenerateDDL defaults to the
// target's dialect. The permissive prototyping core (schema inference, type
// widening) is deliberately left unchanged — the target lens only concerns the
// portability of the SQL you write, not the schema mechanics.
type Target int

const (
	// TargetGeneric is the default: no target declared, no portability checks
	// (identical to vapordb's historical behaviour).
	TargetGeneric Target = iota
	// TargetPostgres flags MySQL-specific syntax that PostgreSQL rejects.
	TargetPostgres
	// TargetMySQL flags PostgreSQL-specific syntax that MySQL rejects.
	TargetMySQL
)

// String returns a human-readable target name.
func (t Target) String() string {
	switch t {
	case TargetPostgres:
		return "postgres"
	case TargetMySQL:
		return "mysql"
	default:
		return "generic"
	}
}

// ddlDialect maps a Target to the dialect string GenerateDDL understands.
// ok is false for TargetGeneric (no DDL dialect implied).
func (t Target) ddlDialect() (dialect string, ok bool) {
	switch t {
	case TargetPostgres:
		return "postgres", true
	case TargetMySQL:
		return "mysql", true
	default:
		return "", false
	}
}

// PortabilityWarning describes one construct in a statement that is unlikely to
// port cleanly to the declared Target.
type PortabilityWarning struct {
	SQL     string // the statement that triggered the warning
	Target  Target // the declared target it was checked against
	Message string // what is non-portable and the target's equivalent
}

// WithTarget declares the database the user intends to migrate to. When set to
// a value other than [TargetGeneric], every [DB.Query] / [DB.Exec] (and their
// named variants) is checked for non-portable SQL, and [DB.GenerateDDL] may be
// called with an empty dialect to use the target's dialect. The default is
// [TargetGeneric] (no checking), so this option is fully backward compatible.
func WithTarget(t Target) Option {
	return func(db *DB) {
		db.target = t
	}
}

// WithPortabilityWarner registers a callback invoked synchronously for every
// [PortabilityWarning] produced while the target is not [TargetGeneric]. It is
// optional: warnings are always accumulated and retrievable via
// [DB.PortabilityWarnings] regardless of whether a warner is set.
func WithPortabilityWarner(fn func(PortabilityWarning)) Option {
	return func(db *DB) {
		db.portWarner = fn
	}
}

// PortabilityWarnings returns a copy of every [PortabilityWarning] accumulated
// so far. It is safe for concurrent use.
func (db *DB) PortabilityWarnings() []PortabilityWarning {
	db.warnMu.Lock()
	defer db.warnMu.Unlock()
	if len(db.warnings) == 0 {
		return nil
	}
	out := make([]PortabilityWarning, len(db.warnings))
	copy(out, db.warnings)
	return out
}

// ClearPortabilityWarnings discards all accumulated portability warnings.
func (db *DB) ClearPortabilityWarnings() {
	db.warnMu.Lock()
	defer db.warnMu.Unlock()
	db.warnings = nil
}

// recordPortabilityWarnings lints sql against the declared target and, for each
// issue found, appends a PortabilityWarning and invokes the optional warner.
// It is a no-op when the target is TargetGeneric. Callers may hold db.mu; a
// dedicated warnMu guards the warnings slice so this never deadlocks.
func (db *DB) recordPortabilityWarnings(sql string) {
	if db.target == TargetGeneric {
		return
	}
	msgs := lintPortability(db.target, sql)
	if len(msgs) == 0 {
		return
	}
	warns := make([]PortabilityWarning, len(msgs))
	for i, m := range msgs {
		warns[i] = PortabilityWarning{SQL: sql, Target: db.target, Message: m}
	}
	db.warnMu.Lock()
	db.warnings = append(db.warnings, warns...)
	db.warnMu.Unlock()

	if db.portWarner != nil {
		for _, w := range warns {
			db.portWarner(w)
		}
	}
}

// lintRule is one portability check: a pattern (matched against SQL with string
// literals masked out) and the message emitted when it matches.
type lintRule struct {
	re  *regexp.Regexp
	msg string
}

// postgresLintRules flag MySQL-specific constructs that PostgreSQL rejects.
var postgresLintRules = []lintRule{
	{regexp.MustCompile("`"), "backtick-quoted identifiers are MySQL-specific; PostgreSQL uses double quotes (\"col\")"},
	{regexp.MustCompile(`(?i)\bON\s+DUPLICATE\s+KEY\b`), "ON DUPLICATE KEY UPDATE is MySQL-specific; PostgreSQL uses INSERT ... ON CONFLICT (...) DO UPDATE"},
	{regexp.MustCompile(`(?i)\bIFNULL\s*\(`), "IFNULL() is MySQL-specific; PostgreSQL uses COALESCE()"},
	{regexp.MustCompile(`(?i)\b(RLIKE|REGEXP)\b`), "RLIKE / REGEXP is MySQL-specific; PostgreSQL uses the ~ operator (or regexp_like)"},
	{regexp.MustCompile(`(?i)\bLIMIT\s+\d+\s*,\s*\d+`), "LIMIT offset, count is MySQL-specific; PostgreSQL uses LIMIT count OFFSET offset"},
	{regexp.MustCompile(`(?i)\bDATE_FORMAT\s*\(`), "DATE_FORMAT() is MySQL-specific; PostgreSQL uses to_char()"},
}

// mysqlLintRules flag PostgreSQL-specific constructs that MySQL rejects.
var mysqlLintRules = []lintRule{
	{regexp.MustCompile(`"`), "double-quoted identifiers are PostgreSQL/standard-SQL; MySQL uses backticks (`col`) unless ANSI_QUOTES is enabled"},
	{regexp.MustCompile(`(?i)\bON\s+CONFLICT\b`), "ON CONFLICT is PostgreSQL-specific; MySQL uses INSERT ... ON DUPLICATE KEY UPDATE"},
	{regexp.MustCompile(`(?i)\bILIKE\b`), "ILIKE is PostgreSQL-specific; MySQL uses LOWER(col) LIKE LOWER(pattern)"},
	{regexp.MustCompile(`::`), "the :: cast operator is PostgreSQL-specific; MySQL uses CAST(x AS type)"},
	{regexp.MustCompile(`@>|<@`), "the @> / <@ JSON containment operators are PostgreSQL-specific; MySQL uses JSON_CONTAINS()"},
	{regexp.MustCompile(`(?i)=\s*ANY\s*\(`), "= ANY(...) is PostgreSQL-specific; MySQL uses IN (...)"},
	{regexp.MustCompile(`(?i)(<>|!=)\s*ALL\s*\(`), "<> ALL(...) is PostgreSQL-specific; MySQL uses NOT IN (...)"},
	{regexp.MustCompile(`(?i)\bNULLS\s+(FIRST|LAST)\b`), "NULLS FIRST / NULLS LAST is PostgreSQL/standard-SQL; MySQL does not support it"},
	{regexp.MustCompile(`(?i)\bRETURNING\b`), "RETURNING is PostgreSQL-specific; MySQL (< 8.0.19, non-MariaDB) does not support it"},
}

// lintPortability returns one message per non-portable construct found in sql
// for the given target. String-literal contents are masked before matching so
// that values like '@>' or 'ON CONFLICT' inside a string never trigger a
// warning. Order of messages follows the rule order; duplicates are collapsed.
func lintPortability(target Target, sql string) []string {
	var rules []lintRule
	switch target {
	case TargetPostgres:
		rules = postgresLintRules
	case TargetMySQL:
		rules = mysqlLintRules
	default:
		return nil
	}

	masked := maskStringLiterals(sql)
	var out []string
	for _, r := range rules {
		if r.re.MatchString(masked) {
			out = append(out, r.msg)
		}
	}
	return out
}

// maskStringLiterals returns a copy of sql with the contents of every
// single-quoted string literal replaced by spaces (quotes and length
// preserved), so pattern matching for dialect syntax never fires on values
// inside string literals. The ” escape sequence is handled.
func maskStringLiterals(sql string) string {
	b := []byte(sql)
	n := len(b)
	i := 0
	for i < n {
		if b[i] != '\'' {
			i++
			continue
		}
		i++ // past opening quote
		for i < n {
			if b[i] == '\'' {
				if i+1 < n && b[i+1] == '\'' {
					b[i], b[i+1] = ' ', ' ' // '' escape — blank both, stay in literal
					i += 2
					continue
				}
				break // closing quote
			}
			b[i] = ' '
			i++
		}
		if i < n {
			i++ // past closing quote
		}
	}
	return string(b)
}
