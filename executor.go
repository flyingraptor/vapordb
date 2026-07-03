package vapordb

import (
	"fmt"
	"sort"
	"strings"

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
