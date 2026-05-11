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
// When affectedRowIdx is non-nil, each inserted or upsert-updated row's table
// index is appended (in VALUES order), so INSERT … RETURNING can include
// ON CONFLICT DO UPDATE results. DO NOTHING conflicts append nothing. Skipped
// when nil (e.g. plain Exec).
func execInsert(db *DB, stmt *sqlparser.Insert, conflictCols []string, doNothing bool, forceWipeOnSchemaConflict bool, trackSchemaWipe *bool, affectedRowIdx *[]int) error {
	tableName := stmt.Table.Name.String()

	values, ok := stmt.Rows.(sqlparser.Values)
	if !ok {
		return fmt.Errorf("only VALUES inserts are supported")
	}
	if len(stmt.Columns) == 0 {
		return fmt.Errorf("INSERT requires an explicit column list")
	}

	cols := make([]string, len(stmt.Columns))
	for i, c := range stmt.Columns {
		cols[i] = c.Lowered()
	}

	for _, valTuple := range values {
		if len(valTuple) != len(cols) {
			return fmt.Errorf("column/value count mismatch: %d columns vs %d values",
				len(cols), len(valTuple))
		}
		row := make(Row, len(cols))
		for i, expr := range valTuple {
			val, err := evalExpr(db, expr, Row{})
			if err != nil {
				return fmt.Errorf("evaluating value for column %q: %w", cols[i], err)
			}
			row[cols[i]] = val
		}

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
				// Apply ON DUPLICATE KEY UPDATE assignments.
				if err := applyOnDup(db, tbl.Rows[idx], row, stmt.OnDup); err != nil {
					return err
				}
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
		if affectedRowIdx != nil {
			*affectedRowIdx = append(*affectedRowIdx, len(tbl.Rows)-1)
		}
	}
	return nil
}

// findConflict returns the index of the first row in tbl whose values for all
// conflictCols match the incoming row, or -1 if no conflict exists.
func findConflict(tbl *Table, conflictCols []string, incoming Row) int {
	if tbl == nil {
		return -1
	}
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
func applyOnDup(db *DB, target Row, incoming Row, onDup sqlparser.OnDup) error {
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
				return fmt.Errorf("ON CONFLICT update expr for %q: %w", colName, err)
			}
		}
		target[colName] = newVal
	}
	return nil
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
	hasAgg := selectHasAggregates(stmt.SelectExprs)
	if len(stmt.GroupBy) > 0 || hasAgg {
		rows, err = applyGroupBy(db, rows, stmt.GroupBy, stmt.SelectExprs)
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

func applyGroupBy(db *DB, rows []Row, groupBy sqlparser.GroupBy, selectExprs sqlparser.SelectExprs) ([]Row, error) {
	if len(groupBy) == 0 {
		// No GROUP BY but aggregates present → treat all rows as one group.
		out, err := computeGroup(db, rows, selectExprs)
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
		out, err := computeGroup(db, groupMap[key], selectExprs)
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

func computeGroup(db *DB, rows []Row, selectExprs sqlparser.SelectExprs) (Row, error) {
	firstRow := Row{}
	if len(rows) > 0 {
		firstRow = rows[0]
	}
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
				val, err = evalExpr(db, s.Expr, firstRow)
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
	return out, nil
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
	case "count", "sum", "avg", "min", "max":
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
		return isAggFunc(e.Name.Lowered())
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
func execSelectForIn(db *DB, sub *sqlparser.Subquery, outerRow Row) ([]Row, error) {
	switch sel := sub.Select.(type) {
	case *sqlparser.Select:
		return execSelectForInSelect(db, sel, outerRow)
	case *sqlparser.Union:
		return nil, fmt.Errorf("IN (subquery): UNION is not supported yet")
	default:
		return nil, fmt.Errorf("IN (subquery): unsupported subquery type %T", sub.Select)
	}
}

func execSelectForInSelect(db *DB, inner *sqlparser.Select, outerRow Row) ([]Row, error) {
	if len(inner.GroupBy) > 0 || selectHasAggregates(inner.SelectExprs) {
		return nil, fmt.Errorf("IN (subquery): GROUP BY / aggregates are not supported yet")
	}
	if len(inner.OrderBy) > 0 || inner.Limit != nil {
		return nil, fmt.Errorf("IN (subquery): ORDER BY / LIMIT are not supported yet")
	}
	if inner.Distinct == sqlparser.DistinctStr {
		return nil, fmt.Errorf("IN (subquery): DISTINCT is not supported yet")
	}
	if inner.Having != nil {
		return nil, fmt.Errorf("IN (subquery): HAVING is not supported yet")
	}
	if len(inner.SelectExprs) != 1 {
		return nil, fmt.Errorf("IN (subquery): inner SELECT must have exactly one column")
	}
	if _, ok := inner.SelectExprs[0].(*sqlparser.StarExpr); ok {
		return nil, fmt.Errorf("IN (subquery): use a single explicit column instead of SELECT *")
	}
	refs, _, err := extractFromClause(db, inner.From)
	if err != nil {
		return nil, err
	}
	if len(refs) == 0 {
		return nil, fmt.Errorf("IN (subquery): no tables in FROM clause")
	}
	filtered, err := correlatedSubqueryFromWhere(db, inner, outerRow)
	if err != nil {
		return nil, err
	}
	// Use qualified outer row for projection too, consistent with the WHERE step.
	qualifiedOuter := qualifyOuterRow(db, outerRow)
	out := make([]Row, 0, len(filtered))
	for _, r := range filtered {
		merged := mergeRowsOuter(qualifiedOuter, r)
		pr, err := projectRow(db, merged, inner.SelectExprs, true)
		if err != nil {
			return nil, err
		}
		out = append(out, pr)
	}
	return out, nil
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
		rows, err = applyGroupBy(db, rows, inner.GroupBy, inner.SelectExprs)
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
