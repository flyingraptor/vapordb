package vapordb

import (
	"strconv"
	"strings"

	"github.com/xwb1989/sqlparser"
)

// JOIN execution: nested-loop and hash joins, join-key extraction.

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
