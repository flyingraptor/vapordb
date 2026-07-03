package vapordb

import (
	"strconv"
	"strings"
)

// conflictIndex maps a conflict-key string to the index of the first table row
// that carries it, for one specific set of conflict columns.
type conflictIndex struct {
	cols []string       // the conflict columns this index is keyed on
	m    map[string]int // conflict-key → first matching row index
}

func (ci *conflictIndex) hasCol(col string) bool {
	for _, c := range ci.cols {
		if c == col {
			return true
		}
	}
	return false
}

// conflictSig returns a stable signature string for a set of conflict columns,
// used as the cache key in Table.conflictIdx.
func conflictSig(cols []string) string {
	return strings.Join(cols, "\x00")
}

// conflictKeyPart encodes a single Value into a key fragment whose string
// equality exactly matches the Go value-equality (`==`) that the original
// linear findConflict used (existing[col] != incoming[col]). The Kind prefix
// keeps distinct kinds apart (e.g. int 1 vs float 1.0, which `==` treats as
// unequal). ok is false for kinds that cannot be encoded this way (Date, JSON),
// signalling the caller to fall back to a linear scan so behaviour is identical.
func conflictKeyPart(v Value) (string, bool) {
	switch v.Kind {
	case KindNull:
		return "z", true
	case KindBool:
		b, ok := v.V.(bool)
		if !ok {
			return "", false
		}
		if b {
			return "b1", true
		}
		return "b0", true
	case KindInt:
		n, ok := v.V.(int64)
		if !ok {
			return "", false
		}
		return "i" + strconv.FormatInt(n, 10), true
	case KindFloat:
		f, ok := v.V.(float64)
		if !ok {
			return "", false
		}
		return "f" + strconv.FormatFloat(f, 'g', -1, 64), true
	case KindString:
		s, ok := v.V.(string)
		if !ok {
			return "", false
		}
		return "s" + s, true
	default:
		// KindDate (time.Time equality is subtle) and KindJSON (uncomparable
		// map/slice) are not encoded; the caller uses the linear scan instead.
		return "", false
	}
}

// conflictKey builds the composite key for a row across cols. ok is false when
// any value's kind is not safely encodable, so the caller falls back to the
// linear scan (preserving the exact original comparison semantics).
func conflictKey(row Row, cols []string) (string, bool) {
	var sb strings.Builder
	for _, c := range cols {
		part, ok := conflictKeyPart(row[c])
		if !ok {
			return "", false
		}
		sb.WriteString(part)
		sb.WriteByte('\x01')
	}
	return sb.String(), true
}

// buildConflictIndex builds an index over the current rows for cols. It returns
// ok=false if any row holds a non-encodable value in a conflict column, in
// which case the index is not usable and callers must scan linearly.
func (t *Table) buildConflictIndex(cols []string) (*conflictIndex, bool) {
	m := make(map[string]int, len(t.Rows))
	for i, r := range t.Rows {
		key, ok := conflictKey(r, cols)
		if !ok {
			return nil, false
		}
		if _, exists := m[key]; !exists {
			m[key] = i // keep the earliest row (first-match semantics)
		}
	}
	return &conflictIndex{cols: append([]string(nil), cols...), m: m}, true
}

// lookupConflict returns the index of the first row conflicting with incoming
// on cols, using (and lazily building) the cached index. used=false means the
// index cannot answer the query (non-encodable key values) and the caller must
// fall back to a linear scan.
func (t *Table) lookupConflict(cols []string, incoming Row) (rowIdx int, used bool) {
	key, ok := conflictKey(incoming, cols)
	if !ok {
		return -1, false
	}
	sig := conflictSig(cols)
	ci := t.conflictIdx[sig]
	if ci == nil {
		built, ok := t.buildConflictIndex(cols)
		if !ok {
			return -1, false
		}
		if t.conflictIdx == nil {
			t.conflictIdx = make(map[string]*conflictIndex, 1)
		}
		t.conflictIdx[sig] = built
		ci = built
	}
	if i, found := ci.m[key]; found {
		return i, true
	}
	return -1, true
}

// indexAppendRow registers a newly appended row (at rowIdx) in every cached
// index. If the row cannot be encoded under an index's columns, that index is
// dropped (it would otherwise become incomplete); it will be rebuilt lazily.
func (t *Table) indexAppendRow(rowIdx int, row Row) {
	for sig, ci := range t.conflictIdx {
		key, ok := conflictKey(row, ci.cols)
		if !ok {
			delete(t.conflictIdx, sig)
			continue
		}
		if _, exists := ci.m[key]; !exists {
			ci.m[key] = rowIdx
		}
	}
}

// invalidateConflictIdx drops all cached indexes. Called by mutations that can
// move or remove rows (UPDATE, DELETE, schema wipe), after which incremental
// maintenance is no longer sound and a lazy rebuild is required.
func (t *Table) invalidateConflictIdx() {
	t.conflictIdx = nil
}

// invalidateConflictIdxForCols drops only the cached indexes whose columns
// overlap the changed columns. Used after an in-place upsert update so indexes
// on unaffected columns (the common case: updating non-key columns) survive.
func (t *Table) invalidateConflictIdxForCols(changed []string) {
	if t.conflictIdx == nil || len(changed) == 0 {
		return
	}
	for sig, ci := range t.conflictIdx {
		for _, c := range changed {
			if ci.hasCol(c) {
				delete(t.conflictIdx, sig)
				break
			}
		}
	}
}
