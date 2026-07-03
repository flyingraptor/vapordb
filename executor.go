package vapordb

import (
	"fmt"
	"math"
	"sort"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/xwb1989/sqlparser"
)

// ─── TABLE REFERENCE HELPERS ─────────────────────────────────────────────────

type tableRef struct {
	name          string // actual table name (or alias for derived tables)
	alias         string // alias used to qualify column names in the row
	subRows       []Row  // non-nil for derived tables (subquery in FROM)
	explicitAlias bool   // true when AS … was used; qualify keys so alias.col resolves in correlated subqueries
}

type joinDesc struct {
	right     tableRef
	joinType  string         // "join", "left join", "right join", etc.
	condition sqlparser.Expr // nil → cross / implicit join
}

// rowsForRef returns the rows for a tableRef, whether it's a real table or a
// derived table produced by a subquery in FROM. Rows are qualified with the
// ref alias so multi-table queries can use the alias.col notation.
func rowsForRef(db *DB, ref tableRef, qualify bool) []Row {
	var raw []Row
	if ref.subRows != nil {
		raw = ref.subRows
	} else if tbl := db.Tables[ref.name]; tbl != nil {
		raw = tbl.Rows
	}
	if len(raw) == 0 {
		return nil
	}
	out := make([]Row, len(raw))
	for i, r := range raw {
		if qualify {
			out[i] = qualifyRow(r, ref.alias)
		} else {
			out[i] = copyRow(r)
		}
	}
	return out
}

func extractTableRef(db *DB, ate *sqlparser.AliasedTableExpr) (tableRef, error) {
	switch expr := ate.Expr.(type) {
	case sqlparser.TableName:
		name := expr.Name.String()
		alias := name
		explicit := false
		if !ate.As.IsEmpty() {
			alias = ate.As.String()
			explicit = true
		}
		return tableRef{name: name, alias: alias, explicitAlias: explicit}, nil

	case *sqlparser.Subquery:
		// Derived table: execute the inner SELECT now so its result rows can
		// be used as a virtual table for the rest of the outer query.
		inner, ok := expr.Select.(*sqlparser.Select)
		if !ok {
			return tableRef{}, fmt.Errorf("derived table: unsupported subquery type %T", expr.Select)
		}
		alias := ate.As.String()
		if alias == "" {
			return tableRef{}, fmt.Errorf("derived table subquery must have an alias (AS …)")
		}
		rows, err := execSelect(db, inner)
		if err != nil {
			return tableRef{}, fmt.Errorf("derived table %q: %w", alias, err)
		}
		return tableRef{name: alias, alias: alias, subRows: rows, explicitAlias: true}, nil

	default:
		return tableRef{}, fmt.Errorf("unsupported FROM expression type: %T", ate.Expr)
	}
}

// walkTableExpr recursively flattens a TableExpr into ordered refs and join
// descriptors. joins[i] describes how refs[i+1] is joined to the left side.
func walkTableExpr(db *DB, te sqlparser.TableExpr) ([]tableRef, []joinDesc, error) {
	switch t := te.(type) {
	case *sqlparser.AliasedTableExpr:
		ref, err := extractTableRef(db, t)
		if err != nil {
			return nil, nil, err
		}
		return []tableRef{ref}, nil, nil

	case *sqlparser.JoinTableExpr:
		leftRefs, leftJoins, err := walkTableExpr(db, t.LeftExpr)
		if err != nil {
			return nil, nil, err
		}
		rightRefs, rightJoins, err := walkTableExpr(db, t.RightExpr)
		if err != nil {
			return nil, nil, err
		}
		if len(rightRefs) == 0 {
			return leftRefs, leftJoins, nil
		}
		jt := strings.ToLower(t.Join)
		if jt == "straight_join" {
			// STRAIGHT_JOIN is our sentinel for FULL OUTER JOIN (rewritten by
			// rewriteFullOuterJoins before parsing because the MySQL-dialect
			// parser has no FULL OUTER JOIN production).
			jt = "full join"
		}
		jd := joinDesc{
			right:     rightRefs[0],
			joinType:  jt,
			condition: t.Condition.On,
		}
		allJoins := append(leftJoins, jd)
		allJoins = append(allJoins, rightJoins...)
		return append(leftRefs, rightRefs...), allJoins, nil

	case *sqlparser.ParenTableExpr:
		var allRefs []tableRef
		var allJoins []joinDesc
		for _, inner := range t.Exprs {
			refs, joins, err := walkTableExpr(db, inner)
			if err != nil {
				return nil, nil, err
			}
			allRefs = append(allRefs, refs...)
			allJoins = append(allJoins, joins...)
		}
		return allRefs, allJoins, nil

	default:
		return nil, nil, fmt.Errorf("unsupported FROM expression type: %T", te)
	}
}

// extractFromClause collects all table refs and join descriptors from the
// FROM clause, treating comma-separated tables as implicit cross joins.
func extractFromClause(db *DB, from sqlparser.TableExprs) ([]tableRef, []joinDesc, error) {
	var allRefs []tableRef
	var allJoins []joinDesc

	for _, te := range from {
		refs, joins, err := walkTableExpr(db, te)
		if err != nil {
			return nil, nil, err
		}
		if len(allRefs) > 0 && len(refs) > 0 {
			// Comma-separated table → implicit cross join.
			allJoins = append(allJoins, joinDesc{right: refs[0], joinType: "cross join"})
			allJoins = append(allJoins, joins...)
		} else {
			allJoins = append(allJoins, joins...)
		}
		allRefs = append(allRefs, refs...)
	}
	return allRefs, allJoins, nil
}

// ─── ROW UTILITIES ───────────────────────────────────────────────────────────

func copyRow(r Row) Row {
	out := make(Row, len(r))
	for k, v := range r {
		out[k] = v
	}
	return out
}

// qualifyRow prefixes every column key with "alias.".
func qualifyRow(r Row, alias string) Row {
	out := make(Row, len(r))
	for k, v := range r {
		out[alias+"."+k] = v
	}
	return out
}

func mergeRows(a, b Row) Row {
	out := make(Row, len(a)+len(b))
	for k, v := range a {
		out[k] = v
	}
	for k, v := range b {
		out[k] = v
	}
	return out
}

// nullRowForTable builds a row of NULLs for a table's schema, qualified with alias.
// For derived tables (subRows != nil) it infers columns from the first result row.
func nullRowForTable(db *DB, ref tableRef) Row {
	out := make(Row)
	if ref.subRows != nil {
		if len(ref.subRows) > 0 {
			for col := range ref.subRows[0] {
				out[ref.alias+"."+col] = Null
			}
		}
		return out
	}
	if tbl := db.Tables[ref.name]; tbl != nil {
		for col := range tbl.Schema {
			out[ref.alias+"."+col] = Null
		}
	}
	return out
}

// rowKey produces a deterministic string fingerprint for deduplication.
func rowKey(row Row) string {
	keys := make([]string, 0, len(row))
	for k := range row {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	var sb strings.Builder
	for _, k := range keys {
		sb.WriteString(k)
		sb.WriteByte('=')
		fmt.Fprintf(&sb, "%T:%v", row[k].V, row[k].V)
		sb.WriteByte(';')
	}
	return sb.String()
}

// ─── UNION / UNION ALL ───────────────────────────────────────────────────────

// execUnion evaluates a UNION or UNION ALL statement.
// Chain structure is left-associative: A UNION B UNION C → (A UNION B) UNION C.
// ORDER BY and LIMIT are only applied once, at the outermost level.
func execUnion(db *DB, stmt *sqlparser.Union) ([]Row, error) {
	rows, err := execUnionNode(db, stmt)
	if err != nil {
		return nil, err
	}
	// Top-level ORDER BY.
	if len(stmt.OrderBy) > 0 {
		if err := sortRows(db, rows, stmt.OrderBy); err != nil {
			return nil, err
		}
	}
	// Top-level LIMIT.
	if stmt.Limit != nil {
		rows, err = applyLimit(db, rows, stmt.Limit)
		if err != nil {
			return nil, err
		}
	}
	return rows, nil
}

// execUnionNode recursively collects rows from a UNION tree, applying
// deduplication at each UNION (distinct) node but not at UNION ALL nodes.
func execUnionNode(db *DB, stmt *sqlparser.Union) ([]Row, error) {
	// Left branch: may itself be a Union (chained) or a plain Select.
	var leftRows []Row
	var err error
	switch l := stmt.Left.(type) {
	case *sqlparser.Select:
		leftRows, err = execSelect(db, l)
	case *sqlparser.Union:
		leftRows, err = execUnionNode(db, l)
	default:
		return nil, fmt.Errorf("UNION: unsupported left side %T", stmt.Left)
	}
	if err != nil {
		return nil, err
	}

	// Right branch is always a plain Select per the grammar.
	rightSel, ok := stmt.Right.(*sqlparser.Select)
	if !ok {
		return nil, fmt.Errorf("UNION: unsupported right side %T", stmt.Right)
	}
	rightRows, err := execSelect(db, rightSel)
	if err != nil {
		return nil, err
	}

	combined := append(leftRows, rightRows...)

	if strings.EqualFold(stmt.Type, "union all") {
		return combined, nil
	}
	// UNION (distinct) — remove duplicate rows.
	return distinctRows(combined), nil
}

// ─── INSERT / UPSERT ─────────────────────────────────────────────────────────

// execInsert handles plain INSERT and the two upsert variants:
//
//   - conflictCols non-nil + stmt.OnDup non-nil  →  update existing row on match
//   - conflictCols non-nil + doNothing true       →  skip insert on conflict
//
// The insert source may be a VALUES list or a SELECT/UNION (INSERT … SELECT);
// see insertSourceRows for how each source is materialised into target rows.
//
// When affectedRowIdx is non-nil, each inserted or upsert-updated row's table
// index is appended (in source-row order), so INSERT … RETURNING can include
// ON CONFLICT DO UPDATE results. DO NOTHING conflicts append nothing. Skipped
// when nil (e.g. plain Exec).
func execInsert(db *DB, stmt *sqlparser.Insert, conflictCols []string, doNothing bool, forceWipeOnSchemaConflict bool, trackSchemaWipe *bool, affectedRowIdx *[]int, upsertWhere string) error {
	tableName := stmt.Table.Name.String()

	if len(stmt.Columns) == 0 {
		return fmt.Errorf("INSERT requires an explicit column list")
	}

	cols := make([]string, len(stmt.Columns))
	for i, c := range stmt.Columns {
		cols[i] = c.Lowered()
	}

	// Build the incoming rows from either a VALUES list or a SELECT/UNION
	// source (INSERT … SELECT). The SELECT is materialised up front so that
	// reading from and writing to the same table in one statement is safe.
	incoming, err := insertSourceRows(db, stmt, cols)
	if err != nil {
		return err
	}

	for _, row := range incoming {
		// Validate enum constraints before touching the schema.
		if existing := db.Tables[tableName]; existing != nil {
			if err := validateEnum(existing, row); err != nil {
				return err
			}
		}

		// ── Upsert path ──────────────────────────────────────────────────────
		if len(conflictCols) > 0 {
			w, err := UpsertSchema(db, tableName, row, forceWipeOnSchemaConflict)
			if err != nil {
				return err
			}
			if trackSchemaWipe != nil && w {
				*trackSchemaWipe = true
			}
			tbl := db.Tables[tableName]
		if idx := findConflict(tbl, conflictCols, row); idx >= 0 {
			// Conflict found.
			if doNothing {
				continue // skip this row silently
			}
			// Gap 4: optimistic-lock predicate — evaluate against the existing row.
			// If the predicate is false the update is silently skipped (matching
			// PostgreSQL's "no rows updated" semantics for failed lock checks).
			if upsertWhere != "" {
				ok, err := evalUpsertWhere(db, upsertWhere, tbl.Rows[idx])
				if err != nil {
					return fmt.Errorf("evaluating upsert WHERE predicate: %w", err)
				}
				if !ok {
					continue
				}
			}
			// Apply ON DUPLICATE KEY UPDATE assignments.
			changed, err := applyOnDup(db, tbl.Rows[idx], row, stmt.OnDup)
			if err != nil {
				return err
			}
			// Drop only the indexes whose key columns the update touched.
			tbl.invalidateConflictIdxForCols(changed)
			// Re-validate after the update assignments.
			if err := validateEnum(tbl, tbl.Rows[idx]); err != nil {
				return err
			}
			if affectedRowIdx != nil {
				*affectedRowIdx = append(*affectedRowIdx, idx)
			}
			continue
		}
		}
		// ── Normal insert ────────────────────────────────────────────────────
		w, err := UpsertSchema(db, tableName, row, forceWipeOnSchemaConflict)
		if err != nil {
			return err
		}
		if trackSchemaWipe != nil && w {
			*trackSchemaWipe = true
		}
		tbl := db.Tables[tableName]
		for col := range tbl.Schema {
			if _, exists := row[col]; !exists {
				row[col] = Null
			}
		}
		tbl.Rows = append(tbl.Rows, row)
		tbl.indexAppendRow(len(tbl.Rows)-1, row)
		if affectedRowIdx != nil {
			*affectedRowIdx = append(*affectedRowIdx, len(tbl.Rows)-1)
		}
	}
	return nil
}

// insertSourceRows materialises the rows an INSERT will apply, from either a
// VALUES list or a SELECT/UNION source (INSERT … SELECT). Each returned Row is
// keyed by the INSERT's target column list (cols). For a SELECT source the
// SELECT's output columns are mapped positionally onto cols.
func insertSourceRows(db *DB, stmt *sqlparser.Insert, cols []string) ([]Row, error) {
	switch src := stmt.Rows.(type) {
	case sqlparser.Values:
		rows := make([]Row, 0, len(src))
		for _, valTuple := range src {
			if len(valTuple) != len(cols) {
				return nil, fmt.Errorf("column/value count mismatch: %d columns vs %d values",
					len(cols), len(valTuple))
			}
			row := make(Row, len(cols))
			for i, expr := range valTuple {
				val, err := evalExpr(db, expr, Row{})
				if err != nil {
					return nil, fmt.Errorf("evaluating value for column %q: %w", cols[i], err)
				}
				row[cols[i]] = val
			}
			rows = append(rows, row)
		}
		return rows, nil

	case sqlparser.SelectStatement:
		return insertSelectRows(db, src, cols)

	default:
		return nil, fmt.Errorf("unsupported INSERT source type: %T", stmt.Rows)
	}
}

// insertSelectRows runs a SELECT/UNION source and maps each result row onto the
// INSERT target columns positionally: target column i receives the value of the
// SELECT's i-th output column. The SELECT must project an explicit column list
// (no bare "*" / "tbl.*") so that output columns are ordered and countable, and
// the output-column count must match the target-column count.
func insertSelectRows(db *DB, sel sqlparser.SelectStatement, cols []string) ([]Row, error) {
	names, err := selectOutputColumns(sel)
	if err != nil {
		return nil, err
	}
	if len(names) != len(cols) {
		return nil, fmt.Errorf("column/value count mismatch: %d columns vs %d selected columns",
			len(cols), len(names))
	}
	// Reject duplicate output names: the row representation is a map, so two
	// columns that resolve to the same output key cannot be read back
	// positionally without data loss. Ask the user to disambiguate with AS.
	seen := make(map[string]struct{}, len(names))
	for _, n := range names {
		if _, dup := seen[n]; dup {
			return nil, fmt.Errorf("INSERT … SELECT: duplicate output column %q; add an alias (AS …) to disambiguate", n)
		}
		seen[n] = struct{}{}
	}

	resultRows, err := execSelectStatement(db, asStatement(sel))
	if err != nil {
		return nil, err
	}

	out := make([]Row, 0, len(resultRows))
	for _, rr := range resultRows {
		row := make(Row, len(cols))
		for i, target := range cols {
			if v, ok := rr[names[i]]; ok {
				row[target] = v
			} else {
				row[target] = Null
			}
		}
		out = append(out, row)
	}
	return out, nil
}

// asStatement unwraps a ParenSelect to the concrete SELECT/UNION statement that
// execSelectStatement understands.
func asStatement(sel sqlparser.SelectStatement) sqlparser.Statement {
	if ps, ok := sel.(*sqlparser.ParenSelect); ok {
		return asStatement(ps.Select)
	}
	return sel
}

// selectOutputColumns returns the ordered output-column names of a SELECT/UNION
// source, derived from its projection list using the same naming rule as
// projectRow (so the names match the keys present in the executed result rows).
// A UNION takes its column names from the left-most SELECT, matching SQL
// semantics. Bare "*" / "tbl.*" projections are rejected because the row
// representation is an unordered map, so a deterministic positional mapping is
// not possible.
func selectOutputColumns(sel sqlparser.SelectStatement) ([]string, error) {
	switch s := sel.(type) {
	case *sqlparser.Select:
		names := make([]string, 0, len(s.SelectExprs))
		for _, se := range s.SelectExprs {
			ae, ok := se.(*sqlparser.AliasedExpr)
			if !ok {
				return nil, fmt.Errorf("INSERT … SELECT requires an explicit column list in the SELECT; '*' is not supported, name each column instead")
			}
			names = append(names, outputKey(ae))
		}
		if len(names) == 0 {
			return nil, fmt.Errorf("INSERT … SELECT: the SELECT has no output columns")
		}
		return names, nil
	case *sqlparser.Union:
		// Every UNION branch must project the same ordered output names so the
		// positional mapping is unambiguous: result rows are keyed by each
		// branch's own output names, so divergent names would silently insert
		// NULLs for the mismatched branch. Require explicit AS aliases instead.
		left, err := selectOutputColumns(s.Left)
		if err != nil {
			return nil, err
		}
		right, err := selectOutputColumns(s.Right)
		if err != nil {
			return nil, err
		}
		if len(left) != len(right) {
			return nil, fmt.Errorf("INSERT … SELECT: UNION branches project different column counts (%d vs %d)", len(left), len(right))
		}
		for i := range left {
			if left[i] != right[i] {
				return nil, fmt.Errorf("INSERT … SELECT: UNION branches have mismatched output column %q vs %q; give both the same name with AS", left[i], right[i])
			}
		}
		return left, nil
	case *sqlparser.ParenSelect:
		return selectOutputColumns(s.Select)
	default:
		return nil, fmt.Errorf("INSERT … SELECT: unsupported SELECT type %T", sel)
	}
}

// findConflict returns the index of the first row in tbl whose values for all
// conflictCols match the incoming row, or -1 if no conflict exists.
//
// It uses a cached conflict-key index (O(1) lookup) when the key values are
// encodable, falling back to a linear scan for Date / JSON keys so the match
// semantics remain identical to a direct value comparison.
func findConflict(tbl *Table, conflictCols []string, incoming Row) int {
	if tbl == nil {
		return -1
	}
	if idx, used := tbl.lookupConflict(conflictCols, incoming); used {
		return idx
	}
	// Fallback: linear scan (non-encodable key values, e.g. Date / JSON).
	for i, existing := range tbl.Rows {
		match := true
		for _, col := range conflictCols {
			if existing[col] != incoming[col] {
				match = false
				break
			}
		}
		if match {
			return i
		}
	}
	return -1
}

// applyOnDup updates target in-place using the OnDup assignment list from the
// parsed INSERT statement. Each assignment is either VALUES(col) — meaning
// "use the value from the incoming row" — or any other expression evaluated
// against the incoming row as context.
// applyOnDup updates target in place and returns the names of the columns it
// wrote, so the caller can invalidate only the conflict indexes those columns
// participate in (indexes on untouched columns stay valid).
func applyOnDup(db *DB, target Row, incoming Row, onDup sqlparser.OnDup) ([]string, error) {
	changed := make([]string, 0, len(onDup))
	for _, upd := range onDup {
		colName := upd.Name.Name.Lowered()
		var newVal Value
		switch expr := upd.Expr.(type) {
		case *sqlparser.ValuesFuncExpr:
			// VALUES(col) → take the value from the incoming row.
			ref := expr.Name.Name.Lowered()
			v, ok := incoming[ref]
			if !ok {
				v = Null
			}
			newVal = v
		default:
			// General expression evaluated in the context of the incoming row.
			var err error
			newVal, err = evalExpr(db, expr, incoming)
			if err != nil {
				return nil, fmt.Errorf("ON CONFLICT update expr for %q: %w", colName, err)
			}
		}
		target[colName] = newVal
		changed = append(changed, colName)
	}
	return changed, nil
}

// evalUpsertWhere parses a bare SQL predicate string and evaluates it against
// existingRow. It is used to implement the optimistic-lock WHERE clause of
// ON CONFLICT DO UPDATE … WHERE <pred>.
func evalUpsertWhere(db *DB, pred string, existingRow Row) (bool, error) {
	stmt, err := sqlparser.Parse("SELECT 1 WHERE " + pred)
	if err != nil {
		return false, fmt.Errorf("invalid upsert WHERE predicate %q: %w", pred, err)
	}
	sel, ok := stmt.(*sqlparser.Select)
	if !ok || sel.Where == nil {
		return false, fmt.Errorf("could not extract WHERE expression from upsert predicate: %s", pred)
	}
	return evalBoolWithDB(db, sel.Where.Expr, existingRow)
}

// ─── SELECT ──────────────────────────────────────────────────────────────────

func execSelect(db *DB, stmt *sqlparser.Select) ([]Row, error) {
	refs, joins, err := extractFromClause(db, stmt.From)
	if err != nil {
		return nil, err
	}
	if len(refs) == 0 {
		return nil, fmt.Errorf("no tables in FROM clause")
	}

	isMultiTable := len(refs) > 1
	qualifyFirst := isMultiTable || refs[0].explicitAlias

	// Build initial rows from the first table (real or derived).
	firstRef := refs[0]
	var rows []Row
	if firstRef.name == "dual" {
		// MySQL's implicit dummy table: SELECT expr (no real FROM clause).
		rows = []Row{{}}
	} else {
		rows = rowsForRef(db, firstRef, qualifyFirst)
	}

	// Apply joins.
	for _, jd := range joins {
		rows, err = applyJoin(db, rows, jd)
		if err != nil {
			return nil, err
		}
	}

	// WHERE.
	if stmt.Where != nil {
		rows, err = applyWhere(db, rows, stmt.Where)
		if err != nil {
			return nil, err
		}
	}

	// GROUP BY / aggregates.
	// Pass HAVING expression so aggregates referenced only in HAVING (e.g.
	// HAVING COUNT(*) > 1 when SELECT has no COUNT) are pre-computed per group.
	hasAgg := selectHasAggregates(stmt.SelectExprs)
	if len(stmt.GroupBy) > 0 || hasAgg {
		var havingExpr sqlparser.Expr
		if stmt.Having != nil {
			havingExpr = stmt.Having.Expr
		}
		rows, err = applyGroupBy(db, rows, stmt.GroupBy, stmt.SelectExprs, havingExpr)
		if err != nil {
			return nil, err
		}
		if stmt.Having != nil {
			rows, err = applyWhere(db, rows, stmt.Having)
			if err != nil {
				return nil, err
			}
		}
	}

	// ORDER BY.
	// Enrich each working row with the SELECT-expression aliases so that
	// ORDER BY can reference computed columns like `ORDER BY revenue` when
	// `revenue` is defined as `price * qty AS revenue` in the SELECT list.
	if len(stmt.OrderBy) > 0 {
		rows, err = enrichWithAliases(db, rows, stmt.SelectExprs)
		if err != nil {
			return nil, err
		}
		if err = sortRows(db, rows, stmt.OrderBy); err != nil {
			return nil, err
		}
	}

	// LIMIT.
	if stmt.Limit != nil {
		rows, err = applyLimit(db, rows, stmt.Limit)
		if err != nil {
			return nil, err
		}
	}

	// Project columns.
	projected, err := projectRows(db, rows, stmt.SelectExprs, isMultiTable)
	if err != nil {
		return nil, err
	}

	// DISTINCT.
	if stmt.Distinct == sqlparser.DistinctStr {
		projected = distinctRows(projected)
	}

	return projected, nil
}

// nullRowLike returns a row whose keys mirror the first row in rows, with every
// value set to NULL. Used to generate null-padded left-side rows for RIGHT JOIN
// and FULL OUTER JOIN when a right row has no matching left partner.
func nullRowLike(rows []Row) Row {
	if len(rows) == 0 {
		return Row{}
	}
	result := make(Row, len(rows[0]))
	for k := range rows[0] {
		result[k] = Null
	}
	return result
}

func applyJoin(db *DB, leftRows []Row, jd joinDesc) ([]Row, error) {
	rightRows := rowsForRef(db, jd.right, true)

	// Fast path: when the ON condition contains at least one equi-join term
	// (a = b), build a hash table on the right side and probe it from the left,
	// turning the O(L×R) nested loop into O(L+R). A mixed condition such as
	// `a.id = b.a_id AND b.deleted_at IS NULL` is split into equi keys (used for
	// hashing) plus a residual predicate applied only to the key-matched
	// candidate pairs, so it stays O(N) too. Falls back to the nested loop for
	// cross joins, purely non-equi conditions, empty inputs, or key types the
	// hash cannot represent with Compare's exact equality semantics (dates,
	// JSON, mixed numeric/string families).
	if jd.condition != nil && len(leftRows) > 0 && len(rightRows) > 0 {
		if keys, residual, ok := splitJoinCondition(jd.condition, leftRows[0], rightRows[0], jd.right.alias); ok {
			result, done, err := hashJoin(db, leftRows, rightRows, jd, keys, residual)
			if err != nil {
				return nil, err
			}
			if done {
				return result, nil
			}
		}
	}

	return nestedLoopJoin(db, leftRows, rightRows, jd)
}

// nestedLoopJoin is the general join algorithm: for every left row it scans
// every right row and evaluates the join condition on the merged row. It
// handles any condition (including non-equi and cross joins) and all join
// types, at the cost of O(L×R) work. applyJoin uses it as the fallback when the
// hash join cannot apply.
func nestedLoopJoin(db *DB, leftRows, rightRows []Row, jd joinDesc) ([]Row, error) {
	isLeft := strings.Contains(jd.joinType, "left")
	isRight := strings.Contains(jd.joinType, "right")
	isFull := jd.joinType == "full join"

	var result []Row
	// rightMatched tracks which right rows participated in at least one match;
	// used to emit null-padded rows for unmatched right rows in RIGHT / FULL joins.
	rightMatched := make([]bool, len(rightRows))

	for _, lr := range leftRows {
		matched := false
		for ri, rr := range rightRows {
			merged := mergeRows(lr, rr)
			if jd.condition == nil {
				result = append(result, merged)
				rightMatched[ri] = true
				matched = true
			} else {
				ok, err := evalBoolWithDB(db, jd.condition, merged)
				if err != nil {
					return nil, err
				}
				if ok {
					result = append(result, merged)
					rightMatched[ri] = true
					matched = true
				}
			}
		}
		// LEFT / FULL: unmatched left row → keep with NULLs for right columns.
		if (isLeft || isFull) && !matched {
			result = append(result, mergeRows(lr, nullRowForTable(db, jd.right)))
		}
	}

	// RIGHT / FULL: unmatched right rows → keep with NULLs for left columns.
	if isRight || isFull {
		nullLeft := nullRowLike(leftRows)
		for ri, rr := range rightRows {
			if !rightMatched[ri] {
				result = append(result, mergeRows(nullLeft, rr))
			}
		}
	}

	return result, nil
}

// joinKeyPair describes one equality in an equi-join condition, split so that
// `left` is evaluated against left-side rows and `right` against right-side rows.
type joinKeyPair struct {
	left  *sqlparser.ColName
	right *sqlparser.ColName
}

// splitJoinCondition separates a join ON condition into equi-join key pairs
// (col = col across the two inputs) and a residual predicate (every other
// AND-ed term). The hash join hashes on the key pairs and applies the residual
// only to the key-matched candidate pairs, so a mixed condition such as
//
//	a.id = b.a_id AND b.deleted_at IS NULL
//
// still runs in O(N): `a.id = b.a_id` becomes the hash key and
// `b.deleted_at IS NULL` becomes the residual. Applying the residual during
// matching (rather than as a post-join WHERE) preserves ON semantics for outer
// joins, where a residual failure means the row is unmatched and gets
// null-padded — not dropped.
//
// ok is false when there is no usable cross-side equi-join term (a pure
// non-equi / OR / cross-table-filter condition), so the caller falls back to
// the nested loop. Terms joined by OR are never split — an OR expression as a
// whole becomes residual, and if it is the entire condition ok is false.
func splitJoinCondition(cond sqlparser.Expr, leftSample, rightSample Row, rightAlias string) (keys []joinKeyPair, residual sqlparser.Expr, ok bool) {
	switch e := cond.(type) {
	case *sqlparser.ParenExpr:
		return splitJoinCondition(e.Expr, leftSample, rightSample, rightAlias)

	case *sqlparser.AndExpr:
		lk, lr, _ := splitJoinCondition(e.Left, leftSample, rightSample, rightAlias)
		rk, rr, _ := splitJoinCondition(e.Right, leftSample, rightSample, rightAlias)
		keys = append(lk, rk...)
		residual = andExprs(lr, rr)
		return keys, residual, len(keys) > 0

	case *sqlparser.ComparisonExpr:
		if e.Operator == sqlparser.EqualStr {
			lcol, lok := e.Left.(*sqlparser.ColName)
			rcol, rok := e.Right.(*sqlparser.ColName)
			if lok && rok {
				ls := colSide(lcol, leftSample, rightSample, rightAlias)
				rs := colSide(rcol, leftSample, rightSample, rightAlias)
				switch {
				case ls == sideLeft && rs == sideRight:
					return []joinKeyPair{{left: lcol, right: rcol}}, nil, true
				case ls == sideRight && rs == sideLeft:
					return []joinKeyPair{{left: rcol, right: lcol}}, nil, true
				}
			}
		}
		// Any equality that is not a clean cross-side column pair (literal
		// operand, same-side columns, function operands) is a residual predicate.
		return nil, cond, false

	default:
		// Any other predicate (IS NULL, IN, BETWEEN, non-equi comparison, OR, …)
		// is evaluated as a residual on the merged candidate rows.
		return nil, cond, false
	}
}

// andExprs combines two optional predicates with AND, dropping nil operands.
func andExprs(a, b sqlparser.Expr) sqlparser.Expr {
	switch {
	case a == nil:
		return b
	case b == nil:
		return a
	default:
		return &sqlparser.AndExpr{Left: a, Right: b}
	}
}

type joinSide int

const (
	sideAmbiguous joinSide = iota
	sideLeft
	sideRight
)

// colSide decides whether a join-condition column refers to the left or right
// input. It prefers unambiguous presence in exactly one side's row; when a
// column resolves on both sides (e.g. a self-join) it disambiguates by matching
// the column qualifier against the right table's alias.
func colSide(c *sqlparser.ColName, leftSample, rightSample Row, rightAlias string) joinSide {
	_, inLeft := resolveColumn(c, leftSample)
	_, inRight := resolveColumn(c, rightSample)
	switch {
	case inRight && !inLeft:
		return sideRight
	case inLeft && !inRight:
		return sideLeft
	case inLeft && inRight:
		q := c.Qualifier.Name.String()
		if q == rightAlias {
			return sideRight
		}
		if q != "" {
			return sideLeft
		}
		return sideAmbiguous
	default:
		return sideAmbiguous
	}
}

// hashJoin performs an equi-join by hashing the right input and probing from
// the left. When residual is non-nil it is evaluated on each key-matched
// candidate pair (the merged row) and only pairs that satisfy it count as a
// match — this keeps mixed ON conditions O(N) while preserving outer-join
// null-padding semantics.
//
// It returns done=false (and a nil result) if it encounters a key value it
// cannot represent with the same equality semantics as the nested loop
// (KindDate / KindJSON, or a key position that mixes numeric and string values
// across rows); the caller then falls back to nestedLoopJoin.
//
// Row output order matches the nested loop: left rows in order, each paired
// with its matching right rows in right-row order, LEFT/FULL null-padding
// interleaved per left row, and RIGHT/FULL null-padding appended at the end.
func hashJoin(db *DB, leftRows, rightRows []Row, jd joinDesc, keys []joinKeyPair, residual sqlparser.Expr) ([]Row, bool, error) {
	isLeft := strings.Contains(jd.joinType, "left")
	isRight := strings.Contains(jd.joinType, "right")
	isFull := jd.joinType == "full join"

	// families[i] records whether key position i has been seen as numeric or
	// string; a later value of the other family means the two columns can match
	// under Compare in ways a family-tagged hash key would miss, so we bail.
	families := make([]keyFamily, len(keys))

	// Build the hash table on the right input.
	index := make(map[string][]int, len(rightRows))
	for ri, rr := range rightRows {
		key, matchable, supported := buildJoinKey(rr, keys, false, families)
		if !supported {
			return nil, false, nil
		}
		if !matchable {
			continue // NULL key: never matches (unmatched for RIGHT/FULL padding)
		}
		index[key] = append(index[key], ri)
	}

	var result []Row
	rightMatched := make([]bool, len(rightRows))

	for _, lr := range leftRows {
		key, matchable, supported := buildJoinKey(lr, keys, true, families)
		if !supported {
			return nil, false, nil
		}
		matched := false
		if matchable {
			for _, ri := range index[key] {
				merged := mergeRows(lr, rightRows[ri])
				if residual != nil {
					pass, err := evalBoolWithDB(db, residual, merged)
					if err != nil {
						return nil, false, err
					}
					if !pass {
						continue
					}
				}
				result = append(result, merged)
				rightMatched[ri] = true
				matched = true
			}
		}
		if (isLeft || isFull) && !matched {
			result = append(result, mergeRows(lr, nullRowForTable(db, jd.right)))
		}
	}

	if isRight || isFull {
		nullLeft := nullRowLike(leftRows)
		for ri, rr := range rightRows {
			if !rightMatched[ri] {
				result = append(result, mergeRows(nullLeft, rr))
			}
		}
	}

	return result, true, nil
}

type keyFamily int8

const (
	familyUnset keyFamily = iota
	familyNumeric
	familyString
)

// buildJoinKey builds the composite hash key for one row across all key
// positions. useLeft selects the left or right column of each pair. It returns
// matchable=false when any key value is NULL (which can never satisfy an
// equi-join), and supported=false when a value's type cannot be hashed with
// Compare-identical semantics, signalling the caller to fall back.
func buildJoinKey(row Row, keys []joinKeyPair, useLeft bool, families []keyFamily) (key string, matchable, supported bool) {
	var sb strings.Builder
	for i, kp := range keys {
		col := kp.right
		if useLeft {
			col = kp.left
		}
		v, _ := resolveColumn(col, row)
		switch v.Kind {
		case KindNull:
			return "", false, true
		case KindBool, KindInt, KindFloat:
			if families[i] == familyString {
				return "", false, false
			}
			families[i] = familyNumeric
			// Canonical numeric form matches Compare's numeric-family branch,
			// so int 1, float 1.0, and bool true all hash together.
			sb.WriteString("n:")
			sb.WriteString(strconv.FormatFloat(numericFloat(v), 'g', -1, 64))
		case KindString:
			if families[i] == familyNumeric {
				return "", false, false
			}
			families[i] = familyString
			sb.WriteString("s:")
			sb.WriteString(v.V.(string))
		default:
			// KindDate / KindJSON: Compare applies coercions the hash cannot
			// reproduce; fall back to the nested loop.
			return "", false, false
		}
		sb.WriteByte('\x01')
	}
	return sb.String(), true, true
}

func applyWhere(db *DB, rows []Row, where *sqlparser.Where) ([]Row, error) {
	result := make([]Row, 0, len(rows))
	for _, row := range rows {
		ok, err := evalBoolWithDB(db, where.Expr, row)
		if err != nil {
			return nil, err
		}
		if ok {
			result = append(result, row)
		}
	}
	return result, nil
}

// applyGroupBy groups rows by groupBy keys, applies aggregate functions from
// selectExprs (and optionally a HAVING expr so that HAVING aggregates not
// present in SELECT are still pre-computed), and returns one projected row per
// group. Callers should pass a non-nil having when they intend to filter the
// result with applyWhere / evalBoolWithDB afterwards.
func applyGroupBy(db *DB, rows []Row, groupBy sqlparser.GroupBy, selectExprs sqlparser.SelectExprs, having ...sqlparser.Expr) ([]Row, error) {
	if len(groupBy) == 0 {
		// No GROUP BY but aggregates present → treat all rows as one group.
		out, err := computeGroup(db, rows, selectExprs, having...)
		if err != nil {
			return nil, err
		}
		return []Row{out}, nil
	}

	groupMap := make(map[string][]Row)
	var groupOrder []string
	for _, row := range rows {
		key, err := computeGroupKey(db, row, groupBy)
		if err != nil {
			return nil, err
		}
		if _, exists := groupMap[key]; !exists {
			groupOrder = append(groupOrder, key)
		}
		groupMap[key] = append(groupMap[key], row)
	}

	result := make([]Row, 0, len(groupOrder))
	for _, key := range groupOrder {
		out, err := computeGroup(db, groupMap[key], selectExprs, having...)
		if err != nil {
			return nil, err
		}
		result = append(result, out)
	}
	return result, nil
}

func computeGroupKey(db *DB, row Row, groupBy sqlparser.GroupBy) (string, error) {
	parts := make([]string, 0, len(groupBy))
	for _, expr := range groupBy {
		val, err := evalExpr(db, expr, row)
		if err != nil {
			return "", err
		}
		parts = append(parts, fmt.Sprintf("%T\x00%v", val.V, val.V))
	}
	return strings.Join(parts, "\x01"), nil
}

func computeGroup(db *DB, rows []Row, selectExprs sqlparser.SelectExprs, extraExprs ...sqlparser.Expr) (Row, error) {
	firstRow := Row{}
	if len(rows) > 0 {
		firstRow = rows[0]
	}

	// Build an "aggRow" that contains all non-NULL aggregate sub-expressions
	// pre-computed and stored under their canonical SQL string keys.
	// evalExpr already knows to look up aggregate functions in the row
	// so COALESCE(SUM(…), 0) and HAVING COUNT(*) > 1 work automatically
	// once the aggregate result is in the row.
	aggRow := precomputeGroupAggs(db, rows, selectExprs, firstRow, extraExprs...)

	out := make(Row)
	for _, se := range selectExprs {
		switch s := se.(type) {
		case *sqlparser.StarExpr:
			for k, v := range firstRow {
				out[k] = v
			}
		case *sqlparser.AliasedExpr:
			outKey := outputKey(s)
			var val Value
			var err error
			if fe, ok := s.Expr.(*sqlparser.FuncExpr); ok && isAggFunc(fe.Name.Lowered()) {
				val, err = evalAggFunc(db, fe, rows)
			} else {
				// Use aggRow so that aggregate sub-expressions nested inside
				// scalar wrappers (COALESCE, IFNULL, arithmetic, …) resolve
				// to their pre-computed group values.
				val, err = evalExpr(db, s.Expr, aggRow)
			}
			if err != nil {
				return nil, err
			}
			out[outKey] = val
			// Also store by canonical key so HAVING can look it up.
			canonical := strings.ToLower(sqlparser.String(s.Expr))
			if canonical != outKey {
				out[canonical] = val
			}
		}
	}

	// Copy pre-computed aggregate values (e.g. "sum(case when …)") into the
	// output row so that the subsequent projectRows call can resolve aggregate
	// sub-expressions that are wrapped inside scalar functions like COALESCE.
	for k, v := range aggRow {
		if _, exists := out[k]; !exists {
			if _, inFirstRow := firstRow[k]; !inFirstRow {
				out[k] = v
			}
		}
	}

	return out, nil
}

// precomputeGroupAggs recursively collects every aggregate FuncExpr that
// appears inside selectExprs (and optional extra expressions such as a HAVING
// clause), evaluates each against rows, and stores the result in a copy of
// firstRow keyed by the canonical SQL string. evalExpr will find these values
// when it encounters the same aggregate function expression.
func precomputeGroupAggs(db *DB, rows []Row, selectExprs sqlparser.SelectExprs, firstRow Row, extraExprs ...sqlparser.Expr) Row {
	aggRow := make(Row, len(firstRow)+8)
	for k, v := range firstRow {
		aggRow[k] = v
	}
	for _, se := range selectExprs {
		if ae, ok := se.(*sqlparser.AliasedExpr); ok {
			collectGroupAggs(db, ae.Expr, rows, aggRow)
		}
	}
	for _, expr := range extraExprs {
		if expr != nil {
			collectGroupAggs(db, expr, rows, aggRow)
		}
	}
	return aggRow
}

// collectGroupAggs walks expr, finds aggregate FuncExprs, evaluates each one,
// and stores the result in aggRow under the canonical SQL string key.
func collectGroupAggs(db *DB, expr sqlparser.Expr, rows []Row, aggRow Row) {
	if expr == nil {
		return
	}
	switch e := expr.(type) {
	case *sqlparser.FuncExpr:
		if isAggFunc(e.Name.Lowered()) {
			key := strings.ToLower(sqlparser.String(e))
			if _, already := aggRow[key]; !already {
				if v, err := evalAggFunc(db, e, rows); err == nil {
					aggRow[key] = v
				}
			}
			return // don't recurse into aggregate arguments
		}
		for _, arg := range e.Exprs {
			if ae, ok := arg.(*sqlparser.AliasedExpr); ok {
				collectGroupAggs(db, ae.Expr, rows, aggRow)
			}
		}
	case *sqlparser.BinaryExpr:
		collectGroupAggs(db, e.Left, rows, aggRow)
		collectGroupAggs(db, e.Right, rows, aggRow)
	case *sqlparser.ComparisonExpr:
		collectGroupAggs(db, e.Left, rows, aggRow)
		collectGroupAggs(db, e.Right, rows, aggRow)
	case *sqlparser.AndExpr:
		collectGroupAggs(db, e.Left, rows, aggRow)
		collectGroupAggs(db, e.Right, rows, aggRow)
	case *sqlparser.OrExpr:
		collectGroupAggs(db, e.Left, rows, aggRow)
		collectGroupAggs(db, e.Right, rows, aggRow)
	case *sqlparser.NotExpr:
		collectGroupAggs(db, e.Expr, rows, aggRow)
	case *sqlparser.ParenExpr:
		collectGroupAggs(db, e.Expr, rows, aggRow)
	case *sqlparser.CaseExpr:
		for _, when := range e.Whens {
			collectGroupAggs(db, when.Cond, rows, aggRow)
			collectGroupAggs(db, when.Val, rows, aggRow)
		}
		if e.Else != nil {
			collectGroupAggs(db, e.Else, rows, aggRow)
		}
	case *sqlparser.IsExpr:
		collectGroupAggs(db, e.Expr, rows, aggRow)
	case *sqlparser.RangeCond:
		collectGroupAggs(db, e.Left, rows, aggRow)
		collectGroupAggs(db, e.From, rows, aggRow)
		collectGroupAggs(db, e.To, rows, aggRow)
	}
}

func evalAggFunc(db *DB, fe *sqlparser.FuncExpr, rows []Row) (Value, error) {
	switch fe.Name.Lowered() {
	case "count":
		return aggCount(db, fe, rows)
	case "sum":
		return aggSum(db, fe, rows)
	case "avg":
		return aggAvg(db, fe, rows)
	case "min":
		return aggMin(db, fe, rows)
	case "max":
		return aggMax(db, fe, rows)
	case "array_agg":
		return aggArrayAgg(db, fe, rows)
	}
	return Null, fmt.Errorf("unknown aggregate function: %s", fe.Name.Lowered())
}

func aggCount(db *DB, fe *sqlparser.FuncExpr, rows []Row) (Value, error) {
	if aggIsStar(fe) {
		return Value{Kind: KindInt, V: int64(len(rows))}, nil
	}
	argExpr, err := aggArgExpr(fe)
	if err != nil {
		return Null, err
	}
	if fe.Distinct {
		seen := make(map[string]bool)
		for _, row := range rows {
			v, err := evalExpr(db, argExpr, row)
			if err != nil {
				return Null, err
			}
			if v.Kind == KindNull {
				continue
			}
			key := fmt.Sprintf("%T\x00%v", v.V, v.V)
			seen[key] = true
		}
		return Value{Kind: KindInt, V: int64(len(seen))}, nil
	}
	count := int64(0)
	for _, row := range rows {
		v, err := evalExpr(db, argExpr, row)
		if err != nil {
			return Null, err
		}
		if v.Kind != KindNull {
			count++
		}
	}
	return Value{Kind: KindInt, V: count}, nil
}

func aggSum(db *DB, fe *sqlparser.FuncExpr, rows []Row) (Value, error) {
	argExpr, err := aggArgExpr(fe)
	if err != nil {
		return Null, err
	}
	sum := float64(0)
	allInt := true
	any := false
	for _, row := range rows {
		v, err := evalExpr(db, argExpr, row)
		if err != nil {
			return Null, err
		}
		if v.Kind == KindNull {
			continue
		}
		if v.Kind != KindInt {
			allInt = false
		}
		sum += numericFloat(v)
		any = true
	}
	if !any {
		return Null, nil
	}
	if allInt {
		return Value{Kind: KindInt, V: int64(sum)}, nil
	}
	return Value{Kind: KindFloat, V: sum}, nil
}

func aggAvg(db *DB, fe *sqlparser.FuncExpr, rows []Row) (Value, error) {
	argExpr, err := aggArgExpr(fe)
	if err != nil {
		return Null, err
	}
	sum, count := float64(0), 0
	for _, row := range rows {
		v, err := evalExpr(db, argExpr, row)
		if err != nil {
			return Null, err
		}
		if v.Kind == KindNull {
			continue
		}
		sum += numericFloat(v)
		count++
	}
	if count == 0 {
		return Null, nil
	}
	return Value{Kind: KindFloat, V: sum / float64(count)}, nil
}

func aggMin(db *DB, fe *sqlparser.FuncExpr, rows []Row) (Value, error) {
	argExpr, err := aggArgExpr(fe)
	if err != nil {
		return Null, err
	}
	var minVal *Value
	for _, row := range rows {
		v, err := evalExpr(db, argExpr, row)
		if err != nil {
			return Null, err
		}
		if v.Kind == KindNull {
			continue
		}
		if minVal == nil || Compare(v, *minVal) < 0 {
			cp := v
			minVal = &cp
		}
	}
	if minVal == nil {
		return Null, nil
	}
	return *minVal, nil
}

func aggMax(db *DB, fe *sqlparser.FuncExpr, rows []Row) (Value, error) {
	argExpr, err := aggArgExpr(fe)
	if err != nil {
		return Null, err
	}
	var maxVal *Value
	for _, row := range rows {
		v, err := evalExpr(db, argExpr, row)
		if err != nil {
			return Null, err
		}
		if v.Kind == KindNull {
			continue
		}
		if maxVal == nil || Compare(v, *maxVal) > 0 {
			cp := v
			maxVal = &cp
		}
	}
	if maxVal == nil {
		return Null, nil
	}
	return *maxVal, nil
}

// aggArrayAgg collects all non-NULL values of its argument into a JSON array
// (KindJSON with []any underlying type). NULL values are silently skipped.
// When no rows produce a non-NULL value, NULL is returned (PostgreSQL semantics).
func aggArrayAgg(db *DB, fe *sqlparser.FuncExpr, rows []Row) (Value, error) {
	argExpr, err := aggArgExpr(fe)
	if err != nil {
		return Null, err
	}
	var arr []any
	for _, row := range rows {
		v, err := evalExpr(db, argExpr, row)
		if err != nil {
			return Null, err
		}
		if v.Kind == KindNull {
			continue
		}
		arr = append(arr, v.V)
	}
	if arr == nil {
		return Null, nil
	}
	return Value{Kind: KindJSON, V: arr}, nil
}

func aggIsStar(fe *sqlparser.FuncExpr) bool {
	if len(fe.Exprs) != 1 {
		return false
	}
	_, ok := fe.Exprs[0].(*sqlparser.StarExpr)
	return ok
}

func aggArgExpr(fe *sqlparser.FuncExpr) (sqlparser.Expr, error) {
	if len(fe.Exprs) == 0 {
		return nil, fmt.Errorf("aggregate %s() requires an argument", fe.Name.Lowered())
	}
	ae, ok := fe.Exprs[0].(*sqlparser.AliasedExpr)
	if !ok {
		return nil, fmt.Errorf("unsupported argument type %T in aggregate", fe.Exprs[0])
	}
	return ae.Expr, nil
}

func isAggFunc(name string) bool {
	switch name {
	case "count", "sum", "avg", "min", "max", "array_agg":
		return true
	}
	return false
}

func selectHasAggregates(exprs sqlparser.SelectExprs) bool {
	for _, se := range exprs {
		ae, ok := se.(*sqlparser.AliasedExpr)
		if !ok {
			continue
		}
		if exprContainsAgg(ae.Expr) {
			return true
		}
	}
	return false
}

func exprContainsAgg(expr sqlparser.Expr) bool {
	switch e := expr.(type) {
	case *sqlparser.FuncExpr:
		if isAggFunc(e.Name.Lowered()) {
			return true
		}
		// Non-aggregate function: recurse into its arguments.
		for _, arg := range e.Exprs {
			if ae, ok := arg.(*sqlparser.AliasedExpr); ok {
				if exprContainsAgg(ae.Expr) {
					return true
				}
			}
		}
		return false
	case *sqlparser.BinaryExpr:
		return exprContainsAgg(e.Left) || exprContainsAgg(e.Right)
	case *sqlparser.ParenExpr:
		return exprContainsAgg(e.Expr)
	}
	return false
}

func sortRows(db *DB, rows []Row, orderBy sqlparser.OrderBy) error {
	var sortErr error
	sort.SliceStable(rows, func(i, j int) bool {
		if sortErr != nil {
			return false
		}
		for _, order := range orderBy {
			a, err := evalExpr(db, order.Expr, rows[i])
			if err != nil {
				sortErr = err
				return false
			}
			b, err := evalExpr(db, order.Expr, rows[j])
			if err != nil {
				sortErr = err
				return false
			}
			cmp := Compare(a, b)
			if order.Direction == sqlparser.DescScr {
				cmp = -cmp
			}
			if cmp != 0 {
				return cmp < 0
			}
		}
		return false
	})
	return sortErr
}

func applyLimit(db *DB, rows []Row, limit *sqlparser.Limit) ([]Row, error) {
	offset := 0
	if limit.Offset != nil {
		v, err := evalExpr(nil, limit.Offset, Row{})
		if err != nil {
			return nil, err
		}
		if v.Kind == KindInt {
			offset = int(v.V.(int64))
		}
	}
	rowcount := len(rows)
	if limit.Rowcount != nil {
		v, err := evalExpr(nil, limit.Rowcount, Row{})
		if err != nil {
			return nil, err
		}
		if v.Kind == KindInt {
			rowcount = int(v.V.(int64))
		}
	}
	if offset >= len(rows) {
		return []Row{}, nil
	}
	end := offset + rowcount
	if end > len(rows) {
		end = len(rows)
	}
	return rows[offset:end], nil
}

func projectRows(db *DB, rows []Row, selectExprs sqlparser.SelectExprs, isJoin bool) ([]Row, error) {
	result := make([]Row, 0, len(rows))
	for _, row := range rows {
		out, err := projectRow(db, row, selectExprs, isJoin)
		if err != nil {
			return nil, err
		}
		result = append(result, out)
	}
	return result, nil
}

func projectRow(db *DB, row Row, selectExprs sqlparser.SelectExprs, isJoin bool) (Row, error) {
	out := make(Row)
	for _, se := range selectExprs {
		switch s := se.(type) {
		case *sqlparser.StarExpr:
			if s.TableName.IsEmpty() {
				// * → all columns, stripping the alias prefix.
				for k, v := range row {
					if parts := strings.SplitN(k, ".", 2); len(parts) == 2 {
						out[parts[1]] = v
					} else {
						out[k] = v
					}
				}
			} else {
				// alias.* → columns from that table alias.
				prefix := s.TableName.Name.String() + "."
				for k, v := range row {
					if strings.HasPrefix(k, prefix) {
						out[k[len(prefix):]] = v
					}
				}
			}
		case *sqlparser.AliasedExpr:
			key := outputKey(s)
			val, err := evalExprWithDB(db, s.Expr, row)
			if err != nil {
				return nil, err
			}
			out[key] = val
		}
	}
	return out, nil
}

func outputKey(ae *sqlparser.AliasedExpr) string {
	if !ae.As.IsEmpty() {
		return ae.As.Lowered()
	}
	switch e := ae.Expr.(type) {
	case *sqlparser.ColName:
		return e.Name.Lowered()
	default:
		return strings.ToLower(sqlparser.String(e))
	}
}

// enrichWithAliases adds SELECT-expression alias values to each working row so
// that ORDER BY can reference them by alias name before final projection.
// Existing keys are never overwritten, so real table columns take precedence.
func enrichWithAliases(db *DB, rows []Row, selectExprs sqlparser.SelectExprs) ([]Row, error) {
	result := make([]Row, len(rows))
	for i, row := range rows {
		enriched := copyRow(row)
		for _, se := range selectExprs {
			ae, ok := se.(*sqlparser.AliasedExpr)
			if !ok {
				continue
			}
			key := outputKey(ae)
			if _, exists := enriched[key]; exists {
				continue // don't shadow a real column
			}
			val, err := evalExprWithDB(db, ae.Expr, row)
			if err != nil {
				return nil, err
			}
			enriched[key] = val
		}
		result[i] = enriched
	}
	return result, nil
}

func distinctRows(rows []Row) []Row {
	seen := make(map[string]bool, len(rows))
	result := make([]Row, 0, len(rows))
	for _, row := range rows {
		k := rowKey(row)
		if !seen[k] {
			seen[k] = true
			result = append(result, row)
		}
	}
	return result
}

// ─── UPDATE ──────────────────────────────────────────────────────────────────

func execUpdate(db *DB, stmt *sqlparser.Update) error {
	if len(stmt.TableExprs) == 0 {
		return fmt.Errorf("UPDATE requires a table name")
	}
	ate, ok := stmt.TableExprs[0].(*sqlparser.AliasedTableExpr)
	if !ok {
		return fmt.Errorf("unsupported UPDATE table expression type: %T", stmt.TableExprs[0])
	}
	tn, ok := ate.Expr.(sqlparser.TableName)
	if !ok {
		return fmt.Errorf("unsupported UPDATE table expression")
	}
	tableName := tn.Name.String()

	tbl := db.Tables[tableName]
	if tbl == nil {
		return nil
	}
	// UPDATE can change any column value; drop cached conflict indexes so a
	// later upsert rebuilds them from the mutated rows.
	tbl.invalidateConflictIdx()

	for i, row := range tbl.Rows {
		if stmt.Where != nil {
			match, err := evalBoolWithDB(db, stmt.Where.Expr, row)
			if err != nil {
				return err
			}
			if !match {
				continue
			}
		}
		for _, upd := range stmt.Exprs {
			col := upd.Name.Name.Lowered()
			val, err := evalExpr(db, upd.Expr, row)
			if err != nil {
				return fmt.Errorf("evaluating SET %s: %w", col, err)
			}
			if err := validateEnumColumn(tbl, col, val); err != nil {
				return err
			}
			tbl.Rows[i][col] = val
			// Evolve schema (no wipe on UPDATE — just widen or accept new type).
			if val.Kind != KindNull {
				if existing, has := tbl.Schema[col]; !has || val.Kind > existing {
					tbl.Schema[col] = val.Kind
				} else if !has {
					tbl.Schema[col] = val.Kind
				}
			}
		}
	}
	return nil
}

// ─── DELETE ──────────────────────────────────────────────────────────────────

func execDelete(db *DB, stmt *sqlparser.Delete) error {
	if len(stmt.TableExprs) == 0 {
		return fmt.Errorf("DELETE requires a table name")
	}
	ate, ok := stmt.TableExprs[0].(*sqlparser.AliasedTableExpr)
	if !ok {
		return fmt.Errorf("unsupported DELETE table expression type: %T", stmt.TableExprs[0])
	}
	tn, ok := ate.Expr.(sqlparser.TableName)
	if !ok {
		return fmt.Errorf("unsupported DELETE table expression")
	}
	tableName := tn.Name.String()

	tbl := db.Tables[tableName]
	if tbl == nil {
		return nil
	}
	// DELETE removes rows and shifts indices; drop cached conflict indexes.
	tbl.invalidateConflictIdx()

	if stmt.Where == nil {
		tbl.Rows = make([]Row, 0)
		return nil
	}

	kept := make([]Row, 0, len(tbl.Rows))
	for _, row := range tbl.Rows {
		match, err := evalBoolWithDB(db, stmt.Where.Expr, row)
		if err != nil {
			return err
		}
		if !match {
			kept = append(kept, row)
		}
	}
	tbl.Rows = kept
	return nil
}

// ─── EXISTS / CORRELATED SUBQUERY ────────────────────────────────────────────

// correlatedSubqueryFromWhere runs inner FROM / JOIN / WHERE with outerRow
// merged into the predicate row (inner columns win on key conflicts). Returns
// surviving inner rows (not projected).
func correlatedSubqueryFromWhere(db *DB, inner *sqlparser.Select, outerRow Row) ([]Row, error) {
	refs, joins, err := extractFromClause(db, inner.From)
	if err != nil {
		return nil, err
	}
	if len(refs) == 0 {
		return nil, fmt.Errorf("subquery: no tables in FROM clause")
	}

	// Always qualify the first table's columns (e.g. "orders.id") so that
	// outer-row columns with the same bare name (e.g. "id" from users) are not
	// shadowed when the two rows are merged for correlated predicate evaluation.
	// resolveColumn's suffix-search fallback still finds "orders.user_id" when
	// a predicate uses the bare name "user_id".
	rows := rowsForRef(db, refs[0], true)
	for _, jd := range joins {
		if rows, err = applyJoin(db, rows, jd); err != nil {
			return nil, err
		}
	}
	if inner.Where != nil {
		qualifiedOuter := qualifyOuterRow(db, outerRow)
		filtered := rows[:0]
		for _, r := range rows {
			merged := mergeRowsOuter(qualifiedOuter, r)
			ok, err := evalBoolWithDB(db, inner.Where.Expr, merged)
			if err != nil {
				return nil, err
			}
			if ok {
				filtered = append(filtered, r)
			}
		}
		rows = filtered
	}
	return rows, nil
}

// execSelectCorrelated runs an EXISTS subquery, merging outerRow as a fallback
// so that correlated column references (e.g. users.id in an inner WHERE) resolve
// correctly against the outer driving row.
func execSelectCorrelated(db *DB, ex *sqlparser.ExistsExpr, outerRow Row) ([]Row, error) {
	inner, ok := ex.Subquery.Select.(*sqlparser.Select)
	if !ok {
		return nil, fmt.Errorf("EXISTS: unsupported subquery type %T", ex.Subquery.Select)
	}
	return correlatedSubqueryFromWhere(db, inner, outerRow)
}

// mergeRowsOuter builds a merged row for correlated predicate evaluation.
//
// Priority rules (highest to lowest):
//  1. Inner qualified keys  (e.g. "orders.user_id" — never overwritten)
//  2. Inner bare keys       (e.g. "user_id" derived from "orders.user_id")
//  3. Outer qualified keys  (e.g. "users.id"  — added so predicates like
//                            `users.id = …` resolve to the outer value)
//  4. Outer bare keys       (e.g. "id" from outer — added last, only when
//                            no inner bare key with the same name is present)
//
// This ensures that unqualified column references in the inner WHERE (e.g.
// bare `name`) resolve to the inner table, not the outer row, while
// qualified outer references (e.g. `users.id`) still find the outer value.
func mergeRowsOuter(outer, inner Row) Row {
	result := copyRow(inner) // step 1: all inner keys (qualified)

	// Step 2: bare versions of inner's qualified keys.
	for k, v := range inner {
		if dot := strings.IndexByte(k, '.'); dot > 0 {
			bare := k[dot+1:]
			if _, exists := result[bare]; !exists {
				result[bare] = v
			}
		}
	}

	// Steps 3 & 4: outer keys, only where not already present.
	for k, v := range outer {
		if _, exists := result[k]; !exists {
			result[k] = v
		}
	}
	return result
}

// qualifyOuterRow adds qualified copies (tablename.col) of bare keys in row by
// matching the row's column set against the DB schema. This allows correlated
// predicates that reference the outer table by name (e.g. `users.id`) to find
// the correct outer value even when the outer query stored the row with bare
// keys. Called only when the row has no qualified keys (single-table outer).
// Access to db.Tables is NOT lock-guarded here because this is always called
// from within a query that already holds db.mu.RLock.
func qualifyOuterRow(db *DB, row Row) Row {
	if len(row) == 0 {
		return row
	}
	// If row already has qualified keys (join / alias outer), return as-is.
	for k := range row {
		if strings.Contains(k, ".") {
			return row
		}
	}
	// Find the table whose schema is the largest subset of the row's bare keys.
	// Using subset-fit (rather than exact-fit) handles enriched rows where
	// computed aliases (e.g. ORDER BY enrichment) have added extra columns.
	tableName := ""
	bestLen := 0
	for name, tbl := range db.Tables {
		if len(tbl.Schema) > len(row) {
			continue // schema has more columns than the row → can't match
		}
		match := true
		for k := range tbl.Schema {
			if _, ok := row[k]; !ok {
				match = false
				break
			}
		}
		if match && len(tbl.Schema) > bestLen {
			tableName = name
			bestLen = len(tbl.Schema)
		}
	}
	if tableName == "" {
		return row
	}
	// Build a copy that has both bare keys and tablename.col keys.
	out := make(Row, len(row)*2)
	for k, v := range row {
		out[k] = v
		out[tableName+"."+k] = v
	}
	return out
}

// execSelectForIn runs a subquery on the RHS of IN / NOT IN with outerRow merged
// for correlation, returning one projected row per surviving inner row.
// Supports the full SELECT pipeline: GROUP BY, HAVING, ORDER BY, LIMIT, DISTINCT, UNION.
func execSelectForIn(db *DB, sub *sqlparser.Subquery, outerRow Row) ([]Row, error) {
	switch sel := sub.Select.(type) {
	case *sqlparser.Select:
		return execSelectForInSelect(db, sel, outerRow)
	case *sqlparser.Union:
		// For correlated UNION subqueries, run each branch with correlation then
		// re-apply the top-level ORDER BY / LIMIT using the existing execUnion
		// helpers. Correlation is handled inside execSelectForInSelect per branch.
		return execSelectForInUnion(db, sel, outerRow)
	default:
		return nil, fmt.Errorf("IN (subquery): unsupported subquery type %T", sub.Select)
	}
}

// execSelectForInSelect runs a single SELECT as an IN subquery, supporting the
// full pipeline: correlated WHERE, GROUP BY / aggregates, HAVING, ORDER BY,
// LIMIT, DISTINCT. Must project exactly one column.
func execSelectForInSelect(db *DB, inner *sqlparser.Select, outerRow Row) ([]Row, error) {
	if len(inner.SelectExprs) != 1 {
		return nil, fmt.Errorf("IN (subquery): inner SELECT must have exactly one column")
	}
	if _, ok := inner.SelectExprs[0].(*sqlparser.StarExpr); ok {
		return nil, fmt.Errorf("IN (subquery): use a single explicit column instead of SELECT *")
	}

	// FROM + JOINs + correlated WHERE.
	rows, err := correlatedSubqueryFromWhere(db, inner, outerRow)
	if err != nil {
		return nil, err
	}

	// GROUP BY / aggregates.
	if len(inner.GroupBy) > 0 || selectHasAggregates(inner.SelectExprs) {
		var havingExpr sqlparser.Expr
		if inner.Having != nil {
			havingExpr = inner.Having.Expr
		}
		rows, err = applyGroupBy(db, rows, inner.GroupBy, inner.SelectExprs, havingExpr)
		if err != nil {
			return nil, err
		}
	}

	// HAVING.
	if inner.Having != nil {
		qualifiedOuter := qualifyOuterRow(db, outerRow)
		filtered := rows[:0]
		for _, r := range rows {
			merged := mergeRowsOuter(qualifiedOuter, r)
			ok, herr := evalBoolWithDB(db, inner.Having.Expr, merged)
			if herr != nil {
				return nil, herr
			}
			if ok {
				filtered = append(filtered, r)
			}
		}
		rows = filtered
	}

	// ORDER BY.
	if len(inner.OrderBy) > 0 {
		rows, err = enrichWithAliases(db, rows, inner.SelectExprs)
		if err != nil {
			return nil, err
		}
		if err = sortRows(db, rows, inner.OrderBy); err != nil {
			return nil, err
		}
	}

	// LIMIT.
	if inner.Limit != nil {
		rows, err = applyLimit(db, rows, inner.Limit)
		if err != nil {
			return nil, err
		}
	}

	// Project.
	qualifiedOuter := qualifyOuterRow(db, outerRow)
	out := make([]Row, 0, len(rows))
	for _, r := range rows {
		merged := mergeRowsOuter(qualifiedOuter, r)
		pr, err := projectRow(db, merged, inner.SelectExprs, true)
		if err != nil {
			return nil, err
		}
		out = append(out, pr)
	}

	// DISTINCT.
	if inner.Distinct == sqlparser.DistinctStr {
		out = distinctRows(out)
	}

	return out, nil
}

// execSelectForInUnion handles UNION / UNION ALL inside an IN (subquery).
func execSelectForInUnion(db *DB, stmt *sqlparser.Union, outerRow Row) ([]Row, error) {
	rows, err := execSelectForInUnionNode(db, stmt, outerRow)
	if err != nil {
		return nil, err
	}
	if len(stmt.OrderBy) > 0 {
		if err := sortRows(db, rows, stmt.OrderBy); err != nil {
			return nil, err
		}
	}
	if stmt.Limit != nil {
		rows, err = applyLimit(db, rows, stmt.Limit)
		if err != nil {
			return nil, err
		}
	}
	return rows, nil
}

func execSelectForInUnionNode(db *DB, stmt *sqlparser.Union, outerRow Row) ([]Row, error) {
	var leftRows []Row
	var err error
	switch l := stmt.Left.(type) {
	case *sqlparser.Select:
		leftRows, err = execSelectForInSelect(db, l, outerRow)
	case *sqlparser.Union:
		leftRows, err = execSelectForInUnionNode(db, l, outerRow)
	default:
		return nil, fmt.Errorf("IN (subquery) UNION: unsupported left side %T", stmt.Left)
	}
	if err != nil {
		return nil, err
	}
	rightSel, ok := stmt.Right.(*sqlparser.Select)
	if !ok {
		return nil, fmt.Errorf("IN (subquery) UNION: unsupported right side %T", stmt.Right)
	}
	rightRows, err := execSelectForInSelect(db, rightSel, outerRow)
	if err != nil {
		return nil, err
	}
	combined := append(leftRows, rightRows...)
	if strings.EqualFold(stmt.Type, "union all") {
		return combined, nil
	}
	return distinctRows(combined), nil
}

// projectedSingleColumn returns the sole value from a one-column projected row.
func projectedSingleColumn(row Row) (Value, error) {
	if len(row) != 1 {
		return Null, fmt.Errorf("subquery: expected one projected column, got %d", len(row))
	}
	for _, v := range row {
		return v, nil
	}
	return Null, nil
}

// execScalarSubquery runs a scalar subquery (one column, zero or one row) with
// outerRow merged as a correlation fallback. Supports the full SELECT pipeline:
// FROM, JOINs, correlated WHERE, GROUP BY, HAVING, ORDER BY, LIMIT, DISTINCT.
// Returns NULL if the subquery produces no rows; errors if it produces 2 or more.
func execScalarSubquery(db *DB, inner *sqlparser.Select, outerRow Row) (Value, error) {
	if len(inner.SelectExprs) != 1 {
		return Null, fmt.Errorf("scalar subquery must SELECT exactly one column, got %d", len(inner.SelectExprs))
	}
	if _, ok := inner.SelectExprs[0].(*sqlparser.StarExpr); ok {
		return Null, fmt.Errorf("scalar subquery: use a single explicit column instead of SELECT *")
	}

	// FROM + JOINs + correlated WHERE.
	// correlatedSubqueryFromWhere always qualifies inner-table column keys
	// (e.g. "orders.id") so that bare outer keys (e.g. "id" from users) are
	// never shadowed during merge-based correlation evaluation.
	rows, err := correlatedSubqueryFromWhere(db, inner, outerRow)
	if err != nil {
		return Null, err
	}

	// GROUP BY / aggregates (e.g. SELECT COUNT(*) FROM …).
	hasAgg := selectHasAggregates(inner.SelectExprs)
	if len(inner.GroupBy) > 0 || hasAgg {
		var havingExpr sqlparser.Expr
		if inner.Having != nil {
			havingExpr = inner.Having.Expr
		}
		rows, err = applyGroupBy(db, rows, inner.GroupBy, inner.SelectExprs, havingExpr)
		if err != nil {
			return Null, err
		}
		if inner.Having != nil {
			filtered := rows[:0]
			for _, r := range rows {
				merged := mergeRowsOuter(outerRow, r)
				ok, herr := evalBoolWithDB(db, inner.Having.Expr, merged)
				if herr != nil {
					return Null, herr
				}
				if ok {
					filtered = append(filtered, r)
				}
			}
			rows = filtered
		}
	}

	// ORDER BY.
	if len(inner.OrderBy) > 0 {
		rows, err = enrichWithAliases(db, rows, inner.SelectExprs)
		if err != nil {
			return Null, err
		}
		if err = sortRows(db, rows, inner.OrderBy); err != nil {
			return Null, err
		}
	}

	// LIMIT.
	if inner.Limit != nil {
		rows, err = applyLimit(db, rows, inner.Limit)
		if err != nil {
			return Null, err
		}
	}

	// Project — inner rows may have qualified keys; projectRow resolves via
	// resolveColumn's suffix-search fallback so bare column names still work.
	projected, err := projectRows(db, rows, inner.SelectExprs, true)
	if err != nil {
		return Null, err
	}

	if inner.Distinct == sqlparser.DistinctStr {
		projected = distinctRows(projected)
	}

	switch len(projected) {
	case 0:
		return Null, nil
	case 1:
		return projectedSingleColumn(projected[0])
	default:
		return Null, fmt.Errorf("scalar subquery returned more than one row")
	}
}

// evalExprWithDB evaluates an expression with database access for EXISTS and IN (subquery).
func evalExprWithDB(db *DB, expr sqlparser.Expr, row Row) (Value, error) {
	return evalExpr(db, expr, row)
}

// evalBoolWithDB evaluates a boolean expression, handling ExistsExpr at any
// depth inside AND / OR / NOT / parentheses. Falls through to evalBool for
// subtrees that contain no EXISTS.
func evalBoolWithDB(db *DB, expr sqlparser.Expr, row Row) (bool, error) {
	switch e := expr.(type) {
	case *sqlparser.ExistsExpr:
		rows, err := execSelectCorrelated(db, e, row)
		return len(rows) > 0, err
	case *sqlparser.AndExpr:
		left, err := evalBoolWithDB(db, e.Left, row)
		if err != nil || !left {
			return left, err
		}
		return evalBoolWithDB(db, e.Right, row)
	case *sqlparser.OrExpr:
		v, err := evalOrPipe(db, e, row)
		if err != nil {
			return false, err
		}
		return isTruthy(v), nil
	case *sqlparser.NotExpr:
		v, err := evalBoolWithDB(db, e.Expr, row)
		return !v, err
	case *sqlparser.ParenExpr:
		return evalBoolWithDB(db, e.Expr, row)
	}
	return evalBool(db, expr, row)
}

// ─── EXPRESSION EVALUATION ───────────────────────────────────────────────────

// evalBool evaluates an expression and returns its boolean truth value.
func evalBool(db *DB, expr sqlparser.Expr, row Row) (bool, error) {
	val, err := evalExpr(db, expr, row)
	if err != nil {
		return false, err
	}
	return isTruthy(val), nil
}

func isTruthy(v Value) bool {
	switch v.Kind {
	case KindNull:
		return false
	case KindBool:
		return v.V.(bool)
	case KindInt:
		return v.V.(int64) != 0
	case KindFloat:
		return v.V.(float64) != 0
	case KindString:
		return v.V.(string) != ""
	case KindDate:
		if t, ok := v.V.(time.Time); ok {
			return !t.IsZero()
		}
	case KindJSON:
		return v.V != nil
	}
	return false
}

// evalExpr evaluates a SQL expression against a working row and returns a Value.
func evalExpr(db *DB, expr sqlparser.Expr, row Row) (Value, error) {
	if row == nil {
		row = Row{}
	}
	switch e := expr.(type) {
	case *sqlparser.NullVal:
		return Null, nil

	case sqlparser.BoolVal:
		return Value{Kind: KindBool, V: bool(e)}, nil

	case *sqlparser.SQLVal:
		return parseSQLVal(e)

	case *sqlparser.ColName:
		v, _ := resolveColumn(e, row)
		return v, nil

	case *sqlparser.ExistsExpr:
		rows, err := execSelectCorrelated(db, e, row)
		if err != nil {
			return Null, err
		}
		return Value{Kind: KindBool, V: len(rows) > 0}, nil

	case *sqlparser.Subquery:
		// Scalar subquery: (SELECT expr FROM …) — must produce 0 or 1 rows.
		inner, ok := e.Select.(*sqlparser.Select)
		if !ok {
			return Null, fmt.Errorf("scalar subquery: UNION is not supported")
		}
		return execScalarSubquery(db, inner, row)

	case *sqlparser.ComparisonExpr:
		return evalComparison(db, e, row)

	case *sqlparser.AndExpr:
		left, err := evalExpr(db, e.Left, row)
		if err != nil {
			return Null, err
		}
		if !isTruthy(left) {
			return Value{Kind: KindBool, V: false}, nil
		}
		right, err := evalExpr(db, e.Right, row)
		if err != nil {
			return Null, err
		}
		return Value{Kind: KindBool, V: isTruthy(right)}, nil

	case *sqlparser.OrExpr:
		return evalOrPipe(db, e, row)

	case *sqlparser.NotExpr:
		val, err := evalExpr(db, e.Expr, row)
		if err != nil {
			return Null, err
		}
		return Value{Kind: KindBool, V: !isTruthy(val)}, nil

	case *sqlparser.IsExpr:
		return evalIsExpr(db, e, row)

	case *sqlparser.ParenExpr:
		return evalExpr(db, e.Expr, row)

	case *sqlparser.BinaryExpr:
		return evalBinaryArith(db, e, row)

	case *sqlparser.UnaryExpr:
		val, err := evalExpr(db, e.Expr, row)
		if err != nil {
			return Null, err
		}
		switch e.Operator {
		case "-":
			switch x := val.V.(type) {
			case int64:
				return Value{Kind: KindInt, V: -x}, nil
			case float64:
				return Value{Kind: KindFloat, V: -x}, nil
			}
		case "!":
			return Value{Kind: KindBool, V: !isTruthy(val)}, nil
		}
		return val, nil

	case *sqlparser.FuncExpr:
		// If the aggregate was already computed and stored in the row (HAVING context),
		// return the pre-computed value instead of re-evaluating.
		if isAggFunc(e.Name.Lowered()) {
			key := strings.ToLower(sqlparser.String(e))
			if v, ok := row[key]; ok {
				return v, nil
			}
			return Null, nil
		}
		return evalScalarFunc(db, e, row)

	case *sqlparser.RangeCond:
		return evalRangeCond(db, e, row)

	case *sqlparser.CaseExpr:
		return evalCaseExpr(db, e, row)

	case *sqlparser.IntervalExpr:
		// INTERVAL n UNIT — evaluated to a string "n UNIT" consumed by DATE_ADD/DATE_SUB.
		v, err := evalExpr(db, e.Expr, row)
		if err != nil {
			return Null, err
		}
		return Value{Kind: KindString, V: valueString(v) + " " + strings.ToUpper(e.Unit)}, nil

	case *sqlparser.ConvertExpr:
		return evalConvertExpr(db, e, row)

	case *sqlparser.ConvertUsingExpr:
		v, err := evalExpr(db, e.Expr, row)
		if err != nil {
			return Null, err
		}
		if v.Kind == KindNull {
			return Null, nil
		}
		return Value{Kind: KindString, V: valueString(v)}, nil

	case sqlparser.ValTuple:
		// ValTuples appear as the RHS of IN; they should not be evaluated standalone.
		return Null, fmt.Errorf("unexpected ValTuple in scalar context")

	default:
		return Null, fmt.Errorf("unsupported expression type: %T", expr)
	}
}

func evalConvertExpr(db *DB, e *sqlparser.ConvertExpr, row Row) (Value, error) {
	v, err := evalExpr(db, e.Expr, row)
	if err != nil {
		return Null, err
	}
	if v.Kind == KindNull || e.Type == nil {
		return v, nil
	}
	return castValueToConvertType(v, e.Type)
}

// castValueToConvertType implements CAST(expr AS type) / CONVERT(expr, type)
// for the common targets used in portable SQL.
func castValueToConvertType(v Value, ct *sqlparser.ConvertType) (Value, error) {
	raw := strings.ToLower(strings.TrimSpace(ct.Type))

	if strings.Contains(raw, "unsigned") {
		return castSQLInt(v, true)
	}
	if strings.Contains(raw, "bool") {
		return castSQLBool(v), nil
	}
	if strings.Contains(raw, "double") || strings.Contains(raw, "float") || raw == "real" ||
		strings.HasPrefix(raw, "decimal") || strings.HasPrefix(raw, "numeric") {
		return castSQLFloat(v)
	}
	if strings.Contains(raw, "char") || raw == "text" || strings.HasPrefix(raw, "nchar") ||
		strings.HasPrefix(raw, "nvarchar") || strings.HasPrefix(raw, "binary") {
		return Value{Kind: KindString, V: valueString(v)}, nil
	}
	if strings.Contains(raw, "datetime") || strings.Contains(raw, "timestamp") {
		return castSQLDateTime(v)
	}
	if raw == "date" || strings.HasPrefix(raw, "date(") {
		return castSQLDate(v)
	}
	if raw == "time" || strings.HasPrefix(raw, "time(") {
		return castSQLTime(v)
	}
	if strings.Contains(raw, "int") || strings.Contains(raw, "signed") || raw == "integer" ||
		raw == "bigint" || raw == "smallint" || raw == "tinyint" {
		return castSQLInt(v, false)
	}
	if raw == "json" || raw == "jsonb" {
		if v.Kind == KindJSON {
			return v, nil
		}
		if v.Kind == KindString {
			jv, err := parseJSONValue(v.V.(string))
			if err != nil {
				return Null, fmt.Errorf("CAST … AS JSON: %w", err)
			}
			return jv, nil
		}
		if v.Kind == KindNull {
			return Null, nil
		}
		return Null, fmt.Errorf("CAST/CONVERT: cannot convert %v to JSON", v.Kind)
	}
	return Null, fmt.Errorf("CAST/CONVERT: unsupported type %q", ct.Type)
}

func castSQLInt(v Value, unsigned bool) (Value, error) {
	if v.Kind == KindNull {
		return Null, nil
	}
	var n int64
	switch v.Kind {
	case KindInt:
		n = v.V.(int64)
	case KindFloat:
		n = int64(v.V.(float64))
	case KindBool:
		if v.V.(bool) {
			n = 1
		}
	case KindString:
		s := strings.TrimSpace(v.V.(string))
		if unsigned {
			u, err := strconv.ParseUint(s, 10, 64)
			if err != nil {
				n = 0
			} else {
				n = int64(u)
			}
		} else {
			var err error
			n, err = strconv.ParseInt(s, 10, 64)
			if err != nil {
				n = 0
			}
		}
	case KindDate:
		t := v.V.(time.Time).UTC()
		n = int64(t.Year())*10000 + int64(t.Month())*100 + int64(t.Day())
	default:
		n = 0
	}
	if unsigned && n < 0 {
		n = 0
	}
	return Value{Kind: KindInt, V: n}, nil
}

func castSQLFloat(v Value) (Value, error) {
	if v.Kind == KindNull {
		return Null, nil
	}
	switch v.Kind {
	case KindFloat:
		return v, nil
	case KindInt:
		return Value{Kind: KindFloat, V: float64(v.V.(int64))}, nil
	case KindBool:
		if v.V.(bool) {
			return Value{Kind: KindFloat, V: 1}, nil
		}
		return Value{Kind: KindFloat, V: 0}, nil
	case KindString:
		f, err := strconv.ParseFloat(strings.TrimSpace(v.V.(string)), 64)
		if err != nil {
			return Value{Kind: KindFloat, V: 0}, nil
		}
		return Value{Kind: KindFloat, V: f}, nil
	case KindDate:
		return Value{Kind: KindFloat, V: float64(v.V.(time.Time).Unix())}, nil
	default:
		return Value{Kind: KindFloat, V: 0}, nil
	}
}

func castSQLBool(v Value) Value {
	if v.Kind == KindNull {
		return Null
	}
	return Value{Kind: KindBool, V: isTruthy(v)}
}

func castSQLDate(v Value) (Value, error) {
	if v.Kind == KindNull {
		return Null, nil
	}
	switch v.Kind {
	case KindDate:
		t := v.V.(time.Time).UTC()
		trunc := time.Date(t.Year(), t.Month(), t.Day(), 0, 0, 0, 0, time.UTC)
		return Value{Kind: KindDate, V: trunc}, nil
	case KindString:
		if t, ok := tryParseDate(strings.TrimSpace(v.V.(string))); ok {
			utc := t.UTC()
			trunc := time.Date(utc.Year(), utc.Month(), utc.Day(), 0, 0, 0, 0, time.UTC)
			return Value{Kind: KindDate, V: trunc}, nil
		}
		return Null, nil
	default:
		return Null, nil
	}
}

func castSQLDateTime(v Value) (Value, error) {
	if v.Kind == KindNull {
		return Null, nil
	}
	switch v.Kind {
	case KindDate:
		return v, nil
	case KindString:
		if t, ok := tryParseDate(strings.TrimSpace(v.V.(string))); ok {
			return Value{Kind: KindDate, V: t.UTC()}, nil
		}
		return Null, nil
	default:
		return Null, nil
	}
}

func castSQLTime(v Value) (Value, error) {
	if v.Kind == KindNull {
		return Null, nil
	}
	clock := func(t time.Time) Value {
		u := t.UTC()
		ref := time.Date(2000, 1, 1, u.Hour(), u.Minute(), u.Second(), u.Nanosecond(), time.UTC)
		return Value{Kind: KindDate, V: ref}
	}
	switch v.Kind {
	case KindString:
		s := strings.TrimSpace(v.V.(string))
		for _, f := range []string{"15:04:05", "15:04"} {
			if t, err := time.Parse(f, s); err == nil {
				return clock(t), nil
			}
		}
		if t, ok := tryParseDate(s); ok {
			return clock(t), nil
		}
		return Null, nil
	case KindDate:
		return clock(v.V.(time.Time)), nil
	default:
		return Null, nil
	}
}

// resolveColumn looks up a column in the working row, handling qualified names.
func resolveColumn(col *sqlparser.ColName, row Row) (Value, bool) {
	qualifier := col.Qualifier.Name.String()
	name := col.Name.Lowered()

	if qualifier != "" {
		// Exact qualified lookup: "alias.col"
		if v, ok := row[qualifier+"."+name]; ok {
			return v, true
		}
		// Bare name fallback (column stored without alias, e.g. in a derived table).
		if v, ok := row[name]; ok {
			return v, true
		}
		// Suffix search only for a qualified reference: accept keys where the
		// qualifier part itself matches (handles "tableName.col" when written as
		// "alias.col" where alias == tableName). Do NOT accept keys from other
		// tables — that would shadow e.g. a null-padded left row with a value
		// from the right row in an outer join.
		suffix := "." + qualifier + "." + name // e.g. ".employees.id" for "e.id"
		for k, v := range row {
			if strings.HasSuffix(k, suffix) {
				return v, true
			}
		}
		return Null, false
	}

	// Unqualified reference: check bare name then any "alias.col" key.
	if v, ok := row[name]; ok {
		return v, true
	}
	suffix := "." + name
	for k, v := range row {
		if strings.HasSuffix(k, suffix) {
			return v, true
		}
	}
	return Null, false
}

func parseSQLVal(sv *sqlparser.SQLVal) (Value, error) {
	switch sv.Type {
	case sqlparser.IntVal:
		n, err := strconv.ParseInt(string(sv.Val), 10, 64)
		if err != nil {
			f, err2 := strconv.ParseFloat(string(sv.Val), 64)
			if err2 != nil {
				return Null, err
			}
			return Value{Kind: KindInt, V: int64(f)}, nil
		}
		return Value{Kind: KindInt, V: n}, nil

	case sqlparser.FloatVal:
		f, err := strconv.ParseFloat(string(sv.Val), 64)
		if err != nil {
			return Null, err
		}
		return Value{Kind: KindFloat, V: f}, nil

	case sqlparser.StrVal:
		return Value{Kind: KindString, V: string(sv.Val)}, nil

	case sqlparser.HexVal:
		n, err := strconv.ParseInt(string(sv.Val), 16, 64)
		if err != nil {
			return Value{Kind: KindString, V: string(sv.Val)}, nil
		}
		return Value{Kind: KindInt, V: n}, nil

	case sqlparser.BitVal:
		n, err := strconv.ParseInt(string(sv.Val), 2, 64)
		if err != nil {
			return Null, err
		}
		return Value{Kind: KindInt, V: n}, nil

	default:
		return Value{Kind: KindString, V: string(sv.Val)}, nil
	}
}

func evalComparison(db *DB, e *sqlparser.ComparisonExpr, row Row) (Value, error) {
	op := e.Operator

	// IN / NOT IN
	if op == "in" || op == "not in" {
		left, err := evalExpr(db, e.Left, row)
		if err != nil {
			return Null, err
		}
		switch rhs := e.Right.(type) {
		case sqlparser.ValTuple:
			for _, item := range rhs {
				val, err := evalExpr(db, item, row)
				if err != nil {
					return Null, err
				}
				if Equal(left, val) {
					return Value{Kind: KindBool, V: op == "in"}, nil
				}
			}
			return Value{Kind: KindBool, V: op == "not in"}, nil
		case *sqlparser.Subquery:
			subRows, err := execSelectForIn(db, rhs, row)
			if err != nil {
				return Null, err
			}
			for _, pr := range subRows {
				val, err := projectedSingleColumn(pr)
				if err != nil {
					return Null, err
				}
				if Equal(left, val) {
					return Value{Kind: KindBool, V: op == "in"}, nil
				}
			}
			return Value{Kind: KindBool, V: op == "not in"}, nil
		default:
			return Null, fmt.Errorf("expected value list or subquery for IN, got %T", e.Right)
		}
	}

	left, err := evalExpr(db, e.Left, row)
	if err != nil {
		return Null, err
	}
	right, err := evalExpr(db, e.Right, row)
	if err != nil {
		return Null, err
	}

	// LIKE / NOT LIKE — NULL on either side propagates to false.
	if op == "like" || op == "not like" {
		if left.Kind == KindNull || right.Kind == KindNull {
			return Value{Kind: KindBool, V: false}, nil
		}
		pat := valueString(right)
		val := valueString(left)
		var match bool
		if e.Escape != nil {
			escVal, err := evalExpr(db, e.Escape, row)
			if err != nil {
				return Null, err
			}
			if escVal.Kind == KindNull {
				return Value{Kind: KindBool, V: false}, nil
			}
			escStr := valueString(escVal)
			if escStr == "" {
				// ESCAPE '' — treat as no escape character (PostgreSQL-compatible).
				match = LikeMatch(pat, val)
			} else if utf8.RuneCountInString(escStr) != 1 {
				return Null, fmt.Errorf("LIKE ESCAPE must be exactly one character")
			} else {
				escR, _ := utf8.DecodeRuneInString(escStr)
				match = LikeMatchEscape(pat, val, escR)
			}
		} else {
			match = LikeMatch(pat, val)
		}
		if op == "not like" {
			match = !match
		}
		return Value{Kind: KindBool, V: match}, nil
	}

	// NULL propagation for comparison operators
	if left.Kind == KindNull || right.Kind == KindNull {
		return Value{Kind: KindBool, V: false}, nil
	}

	cmp := Compare(left, right)
	var result bool
	switch op {
	case "=", "<=>":
		result = cmp == 0
	case "!=", "<>":
		result = cmp != 0
	case "<":
		result = cmp < 0
	case ">":
		result = cmp > 0
	case "<=":
		result = cmp <= 0
	case ">=":
		result = cmp >= 0
	default:
		return Null, fmt.Errorf("unsupported comparison operator: %s", op)
	}
	return Value{Kind: KindBool, V: result}, nil
}

func evalIsExpr(db *DB, e *sqlparser.IsExpr, row Row) (Value, error) {
	val, err := evalExpr(db, e.Expr, row)
	if err != nil {
		return Null, err
	}
	var result bool
	switch e.Operator {
	case "is null":
		result = val.Kind == KindNull
	case "is not null":
		result = val.Kind != KindNull
	case "is true":
		result = isTruthy(val)
	case "is false":
		result = !isTruthy(val)
	case "is not true":
		result = !isTruthy(val)
	case "is not false":
		result = isTruthy(val)
	default:
		return Null, fmt.Errorf("unsupported IS expression: %s", e.Operator)
	}
	return Value{Kind: KindBool, V: result}, nil
}

// combinePipeOr implements PostgreSQL-style ||: concatenate when either operand
// is a string; otherwise boolean OR (short-circuit truthiness on the left).
func combinePipeOr(left, right Value) Value {
	if left.Kind == KindNull || right.Kind == KindNull {
		return Null
	}
	if left.Kind == KindString || right.Kind == KindString {
		return Value{Kind: KindString, V: valueString(left) + valueString(right)}
	}
	if isTruthy(left) {
		return Value{Kind: KindBool, V: true}
	}
	return Value{Kind: KindBool, V: isTruthy(right)}
}

func evalOrPipe(db *DB, e *sqlparser.OrExpr, row Row) (Value, error) {
	left, err := evalExpr(db, e.Left, row)
	if err != nil {
		return Null, err
	}
	right, err := evalExpr(db, e.Right, row)
	if err != nil {
		return Null, err
	}
	return combinePipeOr(left, right), nil
}

func evalBinaryArith(db *DB, e *sqlparser.BinaryExpr, row Row) (Value, error) {
	left, err := evalExpr(db, e.Left, row)
	if err != nil {
		return Null, err
	}
	right, err := evalExpr(db, e.Right, row)
	if err != nil {
		return Null, err
	}
	if left.Kind == KindNull || right.Kind == KindNull {
		return Null, nil
	}

	// Parser may emit `||` as BinaryExpr in some contexts; match OrExpr / pipe rules.
	if e.Operator == "||" {
		return combinePipeOr(left, right), nil
	}

	// String concatenation with +
	if e.Operator == "+" && (left.Kind == KindString || right.Kind == KindString) {
		return Value{Kind: KindString, V: valueString(left) + valueString(right)}, nil
	}

	lf, rf := numericFloat(left), numericFloat(right)
	var result float64
	switch e.Operator {
	case "+":
		result = lf + rf
	case "-":
		result = lf - rf
	case "*":
		result = lf * rf
	case "/":
		if rf == 0 {
			return Null, nil
		}
		result = lf / rf
	case "div":
		if rf == 0 {
			return Null, nil
		}
		return Value{Kind: KindInt, V: int64(lf / rf)}, nil
	case "%":
		if rf == 0 {
			return Null, nil
		}
		return Value{Kind: KindInt, V: int64(lf) % int64(rf)}, nil
	default:
		return Null, fmt.Errorf("unsupported binary operator: %s", e.Operator)
	}
	if left.Kind == KindInt && right.Kind == KindInt && e.Operator != "/" {
		return Value{Kind: KindInt, V: int64(result)}, nil
	}
	return Value{Kind: KindFloat, V: result}, nil
}

func evalRangeCond(db *DB, e *sqlparser.RangeCond, row Row) (Value, error) {
	val, err := evalExpr(db, e.Left, row)
	if err != nil {
		return Null, err
	}
	// NULL on the tested value always produces false (unknown → not matched).
	if val.Kind == KindNull {
		return Value{Kind: KindBool, V: false}, nil
	}
	from, err := evalExpr(db, e.From, row)
	if err != nil {
		return Null, err
	}
	to, err := evalExpr(db, e.To, row)
	if err != nil {
		return Null, err
	}
	inRange := Compare(val, from) >= 0 && Compare(val, to) <= 0
	if e.Operator == "not between" {
		inRange = !inRange
	}
	return Value{Kind: KindBool, V: inRange}, nil
}

func evalCaseExpr(db *DB, e *sqlparser.CaseExpr, row Row) (Value, error) {
	var base *Value
	if e.Expr != nil {
		v, err := evalExpr(db, e.Expr, row)
		if err != nil {
			return Null, err
		}
		base = &v
	}
	for _, when := range e.Whens {
		cond, err := evalExpr(db, when.Cond, row)
		if err != nil {
			return Null, err
		}
		matched := false
		if base != nil {
			matched = Equal(*base, cond)
		} else {
			matched = isTruthy(cond)
		}
		if matched {
			return evalExpr(db, when.Val, row)
		}
	}
	if e.Else != nil {
		return evalExpr(db, e.Else, row)
	}
	return Null, nil
}

func evalScalarFunc(db *DB, e *sqlparser.FuncExpr, row Row) (Value, error) {
	name := e.Name.Lowered()

	args := make([]Value, 0, len(e.Exprs))
	for _, se := range e.Exprs {
		ae, ok := se.(*sqlparser.AliasedExpr)
		if !ok {
			continue
		}
		v, err := evalExpr(db, ae.Expr, row)
		if err != nil {
			return Null, err
		}
		args = append(args, v)
	}

	switch name {
	case "upper":
		if len(args) < 1 || args[0].Kind == KindNull {
			return Null, nil
		}
		return Value{Kind: KindString, V: strings.ToUpper(valueString(args[0]))}, nil

	case "lower":
		if len(args) < 1 || args[0].Kind == KindNull {
			return Null, nil
		}
		return Value{Kind: KindString, V: strings.ToLower(valueString(args[0]))}, nil

	case "length", "char_length", "character_length":
		if len(args) < 1 || args[0].Kind == KindNull {
			return Null, nil
		}
		return Value{Kind: KindInt, V: int64(len(valueString(args[0])))}, nil

	case "concat":
		var sb strings.Builder
		for _, a := range args {
			if a.Kind == KindNull {
				return Null, nil
			}
			sb.WriteString(valueString(a))
		}
		return Value{Kind: KindString, V: sb.String()}, nil

	case "coalesce":
		for _, a := range args {
			if a.Kind != KindNull {
				return a, nil
			}
		}
		return Null, nil

	case "ifnull", "nvl":
		if len(args) < 2 {
			return Null, nil
		}
		if args[0].Kind != KindNull {
			return args[0], nil
		}
		return args[1], nil

	case "nullif":
		if len(args) < 2 {
			return Null, nil
		}
		if Equal(args[0], args[1]) {
			return Null, nil
		}
		return args[0], nil

	case "abs":
		if len(args) < 1 || args[0].Kind == KindNull {
			return Null, nil
		}
		switch x := args[0].V.(type) {
		case int64:
			if x < 0 {
				return Value{Kind: KindInt, V: -x}, nil
			}
		case float64:
			if x < 0 {
				return Value{Kind: KindFloat, V: -x}, nil
			}
		}
		return args[0], nil

	case "round":
		if len(args) < 1 || args[0].Kind == KindNull {
			return Null, nil
		}
		f := numericFloat(args[0])
		if len(args) >= 2 {
			decimals := numericFloat(args[1])
			factor := math.Pow(10, decimals)
			return Value{Kind: KindFloat, V: math.Round(f*factor) / factor}, nil
		}
		return Value{Kind: KindInt, V: int64(math.Round(f))}, nil

	case "floor":
		if len(args) < 1 || args[0].Kind == KindNull {
			return Null, nil
		}
		return Value{Kind: KindInt, V: int64(math.Floor(numericFloat(args[0])))}, nil

	case "ceil", "ceiling":
		if len(args) < 1 || args[0].Kind == KindNull {
			return Null, nil
		}
		return Value{Kind: KindInt, V: int64(math.Ceil(numericFloat(args[0])))}, nil

	case "cast", "convert":
		if len(args) < 1 {
			return Null, nil
		}
		return args[0], nil

	// ── Date / time functions ────────────────────────────────────────────────

	case "now", "sysdate", "current_timestamp":
		return Value{Kind: KindDate, V: time.Now()}, nil

	case "curdate", "current_date":
		t := time.Now().UTC()
		return Value{Kind: KindDate, V: time.Date(t.Year(), t.Month(), t.Day(), 0, 0, 0, 0, time.UTC)}, nil

	case "curtime", "current_time":
		return Value{Kind: KindDate, V: time.Now()}, nil

	case "date":
		// DATE(expr) — truncate a datetime to date-only (midnight UTC) or parse
		// a string literal as a date.
		if len(args) < 1 || args[0].Kind == KindNull {
			return Null, nil
		}
		t := valueToTime(args[0])
		return Value{Kind: KindDate, V: time.Date(t.Year(), t.Month(), t.Day(), 0, 0, 0, 0, time.UTC)}, nil

	case "datetime", "timestamp":
		// DATETIME(expr) / TIMESTAMP(expr) — parse a string or date value as a
		// datetime, preserving the time-of-day component.
		if len(args) < 1 || args[0].Kind == KindNull {
			return Null, nil
		}
		t := valueToTime(args[0])
		return Value{Kind: KindDate, V: t.UTC()}, nil

	case "year":
		if len(args) < 1 || args[0].Kind == KindNull {
			return Null, nil
		}
		return Value{Kind: KindInt, V: int64(valueToTime(args[0]).Year())}, nil

	case "month":
		if len(args) < 1 || args[0].Kind == KindNull {
			return Null, nil
		}
		return Value{Kind: KindInt, V: int64(valueToTime(args[0]).Month())}, nil

	case "day", "dayofmonth":
		if len(args) < 1 || args[0].Kind == KindNull {
			return Null, nil
		}
		return Value{Kind: KindInt, V: int64(valueToTime(args[0]).Day())}, nil

	case "hour":
		if len(args) < 1 || args[0].Kind == KindNull {
			return Null, nil
		}
		return Value{Kind: KindInt, V: int64(valueToTime(args[0]).Hour())}, nil

	case "minute":
		if len(args) < 1 || args[0].Kind == KindNull {
			return Null, nil
		}
		return Value{Kind: KindInt, V: int64(valueToTime(args[0]).Minute())}, nil

	case "second":
		if len(args) < 1 || args[0].Kind == KindNull {
			return Null, nil
		}
		return Value{Kind: KindInt, V: int64(valueToTime(args[0]).Second())}, nil

	case "weekday":
		// Returns 0=Monday … 6=Sunday (MySQL convention).
		if len(args) < 1 || args[0].Kind == KindNull {
			return Null, nil
		}
		wd := valueToTime(args[0]).Weekday() // Sunday=0 in Go
		mysql := int64((int(wd) + 6) % 7)   // convert to Monday=0
		return Value{Kind: KindInt, V: mysql}, nil

	case "dayofweek":
		// Returns 1=Sunday … 7=Saturday (MySQL convention).
		if len(args) < 1 || args[0].Kind == KindNull {
			return Null, nil
		}
		return Value{Kind: KindInt, V: int64(valueToTime(args[0]).Weekday()) + 1}, nil

	case "datediff":
		// DATEDIFF(d1, d2) → number of days from d2 to d1.
		if len(args) < 2 {
			return Null, nil
		}
		if args[0].Kind == KindNull || args[1].Kind == KindNull {
			return Null, nil
		}
		t1 := valueToTime(args[0])
		t2 := valueToTime(args[1])
		d1 := time.Date(t1.Year(), t1.Month(), t1.Day(), 0, 0, 0, 0, time.UTC)
		d2 := time.Date(t2.Year(), t2.Month(), t2.Day(), 0, 0, 0, 0, time.UTC)
		days := int64(d1.Sub(d2).Hours() / 24)
		return Value{Kind: KindInt, V: days}, nil

	case "timestampdiff":
		// TIMESTAMPDIFF(unit, d1, d2) → integer difference in the given unit.
		// The unit is passed as a bare identifier (SECOND, MINUTE, HOUR, DAY, MONTH, YEAR).
		// sqlparser wraps it in a string value, so args[0] is a KindString.
		if len(args) < 3 {
			return Null, nil
		}
		if args[1].Kind == KindNull || args[2].Kind == KindNull {
			return Null, nil
		}
		unit := strings.ToUpper(valueString(args[0]))
		t1 := valueToTime(args[1])
		t2 := valueToTime(args[2])
		diff := t2.Sub(t1)
		var result int64
		switch unit {
		case "SECOND":
			result = int64(diff.Seconds())
		case "MINUTE":
			result = int64(diff.Minutes())
		case "HOUR":
			result = int64(diff.Hours())
		case "DAY":
			result = int64(diff.Hours() / 24)
		case "MONTH":
			years := t2.Year() - t1.Year()
			months := int(t2.Month()) - int(t1.Month())
			result = int64(years*12 + months)
		case "YEAR":
			result = int64(t2.Year() - t1.Year())
		default:
			return Null, fmt.Errorf("unsupported TIMESTAMPDIFF unit: %s", unit)
		}
		return Value{Kind: KindInt, V: result}, nil

	case "date_format":
		// DATE_FORMAT(date, format) — formats a date using MySQL format specifiers.
		if len(args) < 2 || args[0].Kind == KindNull || args[1].Kind == KindNull {
			return Null, nil
		}
		t := valueToTime(args[0])
		goFmt := mysqlFormatToGo(valueString(args[1]))
		return Value{Kind: KindString, V: t.UTC().Format(goFmt)}, nil

	case "date_add", "adddate":
		// DATE_ADD(date, INTERVAL n unit)
		// sqlparser passes the interval value as a string like "1 DAY".
		if len(args) < 2 || args[0].Kind == KindNull || args[1].Kind == KindNull {
			return Null, nil
		}
		t := valueToTime(args[0])
		result, err := applyInterval(t, valueString(args[1]), false)
		if err != nil {
			return Null, err
		}
		return Value{Kind: KindDate, V: result}, nil

	case "date_sub", "subdate":
		if len(args) < 2 || args[0].Kind == KindNull || args[1].Kind == KindNull {
			return Null, nil
		}
		t := valueToTime(args[0])
		result, err := applyInterval(t, valueString(args[1]), true)
		if err != nil {
			return Null, err
		}
		return Value{Kind: KindDate, V: result}, nil

	// ── JSON functions ────────────────────────────────────────────────────────

	case "json_extract":
		if len(args) < 2 {
			return Null, nil
		}
		path := valueString(args[1])
		return jsonExtract(args[0], path), nil

	case "json_unquote":
		if len(args) < 1 {
			return Null, nil
		}
		return jsonUnquote(args[0]), nil

	case "json_contains":
		if len(args) < 2 {
			return Null, nil
		}
		if jsonContainsCheck(args[0], args[1]) {
			return Value{Kind: KindBool, V: true}, nil
		}
		return Value{Kind: KindBool, V: false}, nil

	case "json_parse":
		if len(args) < 1 || args[0].Kind == KindNull {
			return Null, nil
		}
		v, err := parseJSONValue(valueString(args[0]))
		if err != nil {
			return Null, err
		}
		return v, nil

	case "json_array_length":
		if len(args) < 1 {
			return Null, nil
		}
		return jsonArrayLength(args[0]), nil

	case "json_keys":
		if len(args) < 1 {
			return Null, nil
		}
		return jsonKeys(args[0]), nil

	case "json_type":
		if len(args) < 1 || args[0].Kind == KindNull {
			return Null, nil
		}
		jv := args[0]
		if jv.Kind == KindJSON {
			switch jv.V.(type) {
			case map[string]any:
				return Value{Kind: KindString, V: "OBJECT"}, nil
			case []any:
				return Value{Kind: KindString, V: "ARRAY"}, nil
			default:
				return Value{Kind: KindString, V: "SCALAR"}, nil
			}
		}
		switch jv.Kind {
		case KindString:
			return Value{Kind: KindString, V: "STRING"}, nil
		case KindInt, KindFloat:
			return Value{Kind: KindString, V: "INTEGER"}, nil
		case KindBool:
			return Value{Kind: KindString, V: "BOOLEAN"}, nil
		case KindDate:
			return Value{Kind: KindString, V: "STRING"}, nil
		}
		return Null, nil
	}

	return Null, fmt.Errorf("unknown function: %s", name)
}

// mysqlFormatToGo converts a MySQL DATE_FORMAT format string to a Go time format.
func mysqlFormatToGo(format string) string {
	r := strings.NewReplacer(
		"%Y", "2006",
		"%y", "06",
		"%m", "01",
		"%c", "1",
		"%d", "02",
		"%e", "2",
		"%H", "15",
		"%h", "03",
		"%I", "03",
		"%i", "04",
		"%s", "05",
		"%S", "05",
		"%p", "PM",
		"%M", "January",
		"%b", "Jan",
		"%W", "Monday",
		"%a", "Mon",
		"%j", "002",
		"%%", "%",
	)
	return r.Replace(format)
}

// applyInterval adds or subtracts an interval expressed as "n UNIT" to t.
func applyInterval(t time.Time, interval string, subtract bool) (time.Time, error) {
	parts := strings.Fields(interval)
	if len(parts) < 2 {
		return t, fmt.Errorf("invalid interval: %q", interval)
	}
	n, err := strconv.Atoi(parts[0])
	if err != nil {
		return t, fmt.Errorf("invalid interval value: %q", parts[0])
	}
	if subtract {
		n = -n
	}
	unit := strings.ToUpper(parts[1])
	switch unit {
	case "SECOND":
		return t.Add(time.Duration(n) * time.Second), nil
	case "MINUTE":
		return t.Add(time.Duration(n) * time.Minute), nil
	case "HOUR":
		return t.Add(time.Duration(n) * time.Hour), nil
	case "DAY":
		return t.AddDate(0, 0, n), nil
	case "WEEK":
		return t.AddDate(0, 0, n*7), nil
	case "MONTH":
		return t.AddDate(0, n, 0), nil
	case "YEAR":
		return t.AddDate(n, 0, 0), nil
	default:
		return t, fmt.Errorf("unsupported interval unit: %s", unit)
	}
}
