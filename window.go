package vapordb

import (
	"fmt"
	"regexp"
	"sort"
	"strconv"
	"strings"
)

// ── Frame bound constants ─────────────────────────────────────────────────────

const (
	winBoundUnboundedPreceding = iota
	winBoundPreceding
	winBoundCurrentRow
	winBoundFollowing
	winBoundUnboundedFollowing
)

// winBound describes one endpoint of a window frame.
type winBound struct {
	kind   int // winBound* constant
	offset int // rows distance for PRECEDING / FOLLOWING (ROWS mode)
}

// ── AST ───────────────────────────────────────────────────────────────────────

// winSpec is the parsed representation of one window function expression.
type winSpec struct {
	idx         int
	funcName    string     // lower-cased: row_number, rank, dense_rank, count, sum, …
	arg         string     // placeholder column name for the first argument, or ""
	partBy      []string   // placeholder column names from PARTITION BY
	orderBy     []winOrder // ORDER BY inside OVER(…)
	alias       string     // final column name in result rows
	placeholder string     // __win_N__ used during SQL execution
	helpers     []string   // extra placeholder columns injected into the query

	// LAG / LEAD
	lagOffset  int    // default 1
	lagDefault string // helper column for the default value; "" → NULL

	// NTH_VALUE
	nthN int // 1-based position

	// NTILE
	ntileN int // number of buckets (parsed from literal argument)

	// Explicit window frame
	hasFrame   bool
	frameRows  bool     // true=ROWS, false=RANGE
	frameStart winBound
	frameEnd   winBound
}

type winOrder struct {
	col  string
	desc bool
}

// overClause is the result of parsing an OVER(…) body.
type overClause struct {
	partBy     []string
	orderBy    []winOrder
	hasFrame   bool
	frameRows  bool
	frameStart winBound
	frameEnd   winBound
}

// winRE matches  FUNC(args) OVER(spec) [AS alias].
// [^()]* keeps us at one paren level; covers the common cases.
var winRE = regexp.MustCompile(
	`(?i)\b(ROW_NUMBER|RANK|DENSE_RANK|CUME_DIST|PERCENT_RANK|NTILE|COUNT|SUM|AVG|MIN|MAX|LAG|LEAD|FIRST_VALUE|LAST_VALUE|NTH_VALUE)\s*\(([^()]*)\)\s+OVER\s*\(([^()]*)\)(?:\s+AS\s+(\w+))?`,
)

// ── Extraction ────────────────────────────────────────────────────────────────

// extractWindowFuncs detects window function expressions in sql and:
//  1. Replaces each  fn(…) OVER(…) [AS alias]  with  0 AS __win_N__
//  2. Injects  col AS __win_N_hM__  helper expressions before FROM so that
//     ORDER BY / PARTITION BY / aggregate columns are available as projected columns.
//
// applyWindowFuncs must be called on the result rows to fill in computed values
// and remove helper columns.
func extractWindowFuncs(sql string) (string, []winSpec, error) {
	var specs []winSpec
	var extra []string
	idx := 0

	result := winRE.ReplaceAllStringFunc(sql, func(match string) string {
		sub := winRE.FindStringSubmatch(match)
		funcName := strings.ToLower(sub[1])
		rawArgs := strings.TrimSpace(sub[2])
		overContent := strings.TrimSpace(sub[3])
		alias := strings.ToLower(strings.TrimSpace(sub[4]))

		ph := fmt.Sprintf("__win_%d__", idx)
		if alias == "" {
			alias = ph
		}

		over := parseOverClause(overContent)
		argParts := splitWinArgs(rawArgs)

		var helpers []string
		argPH := ""
		lagOffset := 1
		lagDefault := ""
		nthN := 1
		ntileN := 1

		switch funcName {
		case "ntile":
			// NTILE(n) — literal integer, no helper column needed.
			if len(argParts) > 0 {
				if n, err := strconv.Atoi(argParts[0]); err == nil && n > 0 {
					ntileN = n
				}
			}

		case "cume_dist", "percent_rank":
			// No argument.

		case "lag", "lead":
			// LAG/LEAD(col [, offset [, default]])
			if len(argParts) > 0 {
				argPH = fmt.Sprintf("__win_%d_a__", idx)
				extra = append(extra, fmt.Sprintf("%s AS %s", argParts[0], argPH))
				helpers = append(helpers, argPH)
			}
			if len(argParts) >= 2 {
				if n, err := strconv.Atoi(argParts[1]); err == nil {
					lagOffset = n
				}
			}
			if len(argParts) >= 3 {
				lagDefault = fmt.Sprintf("__win_%d_d__", idx)
				extra = append(extra, fmt.Sprintf("%s AS %s", argParts[2], lagDefault))
				helpers = append(helpers, lagDefault)
			}

		case "nth_value":
			// NTH_VALUE(col, n)
			if len(argParts) > 0 {
				argPH = fmt.Sprintf("__win_%d_a__", idx)
				extra = append(extra, fmt.Sprintf("%s AS %s", argParts[0], argPH))
				helpers = append(helpers, argPH)
			}
			if len(argParts) >= 2 {
				if n, err := strconv.Atoi(argParts[1]); err == nil && n >= 1 {
					nthN = n
				}
			}

		default:
			// ROW_NUMBER, RANK, DENSE_RANK, COUNT, SUM, AVG, MIN, MAX,
			// FIRST_VALUE, LAST_VALUE
			rawArg := strings.ToLower(rawArgs)
			if rawArg != "" && rawArg != "*" {
				argPH = fmt.Sprintf("__win_%d_a__", idx)
				extra = append(extra, fmt.Sprintf("%s AS %s", rawArg, argPH))
				helpers = append(helpers, argPH)
			}
		}

		// PARTITION BY helpers.
		var resolvedPartBy []string
		for ci, p := range over.partBy {
			phCol := fmt.Sprintf("__win_%d_p%d__", idx, ci)
			extra = append(extra, fmt.Sprintf("%s AS %s", p, phCol))
			helpers = append(helpers, phCol)
			resolvedPartBy = append(resolvedPartBy, phCol)
		}

		// ORDER BY helpers.
		var resolvedOrderBy []winOrder
		for ci, o := range over.orderBy {
			phCol := fmt.Sprintf("__win_%d_k%d__", idx, ci)
			extra = append(extra, fmt.Sprintf("%s AS %s", o.col, phCol))
			helpers = append(helpers, phCol)
			resolvedOrderBy = append(resolvedOrderBy, winOrder{col: phCol, desc: o.desc})
		}

		specs = append(specs, winSpec{
			idx:         idx,
			funcName:    funcName,
			arg:         argPH,
			partBy:      resolvedPartBy,
			orderBy:     resolvedOrderBy,
			alias:       alias,
			placeholder: ph,
			helpers:     helpers,
			lagOffset:   lagOffset,
			lagDefault:  lagDefault,
			nthN:        nthN,
			ntileN:      ntileN,
			hasFrame:    over.hasFrame,
			frameRows:   over.frameRows,
			frameStart:  over.frameStart,
			frameEnd:    over.frameEnd,
		})
		idx++
		return fmt.Sprintf("0 AS %s", ph)
	})

	if len(extra) > 0 {
		result = injectBeforeFrom(result, extra)
	}
	return result, specs, nil
}

// splitWinArgs splits a comma-separated window function argument string.
func splitWinArgs(raw string) []string {
	parts := strings.Split(raw, ",")
	result := make([]string, 0, len(parts))
	for _, p := range parts {
		if t := strings.TrimSpace(p); t != "" {
			result = append(result, t)
		}
	}
	return result
}

// injectBeforeFrom inserts ", col1 AS ph1, col2 AS ph2 " immediately before the
// outermost FROM keyword in sql.
func injectBeforeFrom(sql string, cols []string) string {
	lower := strings.ToLower(sql)
	depth := 0
	i := 0
	for i < len(sql) {
		switch {
		case sql[i] == '(':
			depth++
			i++
		case sql[i] == ')':
			depth--
			i++
		case sql[i] == '\'' || sql[i] == '"':
			q := sql[i]
			i++
			for i < len(sql) && sql[i] != q {
				i++
			}
			i++
		case depth == 0 && i+4 <= len(sql) && lower[i:i+4] == "from":
			prevOK := i == 0 || !isWinAlphaNum(sql[i-1])
			nextOK := i+4 >= len(sql) || !isWinAlphaNum(sql[i+4])
			if prevOK && nextOK {
				return sql[:i] + ", " + strings.Join(cols, ", ") + " " + sql[i:]
			}
			i++
		default:
			i++
		}
	}
	return sql
}

func isWinAlphaNum(b byte) bool {
	return (b >= 'a' && b <= 'z') || (b >= 'A' && b <= 'Z') ||
		(b >= '0' && b <= '9') || b == '_'
}

// parseOverClause parses the body of OVER(…) into partition, order, and frame parts.
//
// Frame extraction strips ROWS BETWEEN … / RANGE BETWEEN … from the end of the
// clause before parsing PARTITION BY and ORDER BY, so ORDER BY columns are never
// contaminated by the frame keywords.
func parseOverClause(content string) overClause {
	upper := strings.ToUpper(content)
	var oc overClause

	// Strip frame spec (always appears after ORDER BY if present).
	if fi := strings.Index(upper, "ROWS BETWEEN"); fi >= 0 {
		oc.hasFrame = true
		oc.frameRows = true
		oc.frameStart, oc.frameEnd = parseFrameBounds(strings.TrimSpace(content[fi+12:]))
		content = strings.TrimSpace(content[:fi])
		upper = strings.ToUpper(content)
	} else if fi := strings.Index(upper, "RANGE BETWEEN"); fi >= 0 {
		oc.hasFrame = true
		oc.frameRows = false
		oc.frameStart, oc.frameEnd = parseFrameBounds(strings.TrimSpace(content[fi+13:]))
		content = strings.TrimSpace(content[:fi])
		upper = strings.ToUpper(content)
	}

	// PARTITION BY section.
	if pi := strings.Index(upper, "PARTITION BY"); pi >= 0 {
		rest := strings.TrimSpace(content[pi+12:])
		restUpper := strings.ToUpper(rest)
		end := strings.Index(restUpper, "ORDER BY")
		var partContent string
		if end >= 0 {
			partContent = rest[:end]
			content = rest[end:]
			upper = strings.ToUpper(content)
		} else {
			partContent = rest
			content = ""
			upper = ""
		}
		for _, col := range strings.Split(partContent, ",") {
			if c := strings.TrimSpace(strings.ToLower(col)); c != "" {
				oc.partBy = append(oc.partBy, c)
			}
		}
	}

	// ORDER BY section.
	if oi := strings.Index(upper, "ORDER BY"); oi >= 0 {
		rest := strings.TrimSpace(content[oi+8:])
		for _, part := range strings.Split(rest, ",") {
			part = strings.TrimSpace(part)
			if part == "" {
				continue
			}
			fields := strings.Fields(part)
			col := strings.ToLower(fields[0])
			desc := len(fields) > 1 && strings.EqualFold(fields[1], "DESC")
			oc.orderBy = append(oc.orderBy, winOrder{col: col, desc: desc})
		}
	}

	return oc
}

// parseFrameBounds parses "startBound AND endBound" from a frame clause body.
func parseFrameBounds(s string) (start, end winBound) {
	upper := strings.ToUpper(s)
	andIdx := strings.Index(upper, " AND ")
	if andIdx < 0 {
		start = parseSingleBound(s)
		end = winBound{kind: winBoundCurrentRow}
		return
	}
	start = parseSingleBound(strings.TrimSpace(s[:andIdx]))
	end = parseSingleBound(strings.TrimSpace(s[andIdx+5:]))
	return
}

// parseSingleBound parses one frame bound descriptor.
func parseSingleBound(s string) winBound {
	upper := strings.ToUpper(strings.TrimSpace(s))
	switch {
	case upper == "UNBOUNDED PRECEDING":
		return winBound{kind: winBoundUnboundedPreceding}
	case upper == "CURRENT ROW":
		return winBound{kind: winBoundCurrentRow}
	case upper == "UNBOUNDED FOLLOWING":
		return winBound{kind: winBoundUnboundedFollowing}
	case strings.HasSuffix(upper, " PRECEDING"):
		n, _ := strconv.Atoi(strings.TrimSuffix(upper, " PRECEDING"))
		return winBound{kind: winBoundPreceding, offset: n}
	case strings.HasSuffix(upper, " FOLLOWING"):
		n, _ := strconv.Atoi(strings.TrimSuffix(upper, " FOLLOWING"))
		return winBound{kind: winBoundFollowing, offset: n}
	}
	return winBound{kind: winBoundCurrentRow}
}

// ── Post-processing ───────────────────────────────────────────────────────────

// applyWindowFuncs computes each window function over the result rows, replaces
// placeholder columns with computed values, and removes helper columns.
func applyWindowFuncs(rows []Row, specs []winSpec) ([]Row, error) {
	for _, spec := range specs {
		if err := applyWinSpec(rows, spec); err != nil {
			return nil, fmt.Errorf("window function %s: %w", spec.funcName, err)
		}
	}
	return rows, nil
}

func applyWinSpec(rows []Row, spec winSpec) error {
	ph := spec.placeholder

	// Group row indices by partition key.
	partMap := make(map[string][]int)
	var partKeys []string
	for i, row := range rows {
		key := winPartKey(row, spec.partBy)
		if _, exists := partMap[key]; !exists {
			partKeys = append(partKeys, key)
		}
		partMap[key] = append(partMap[key], i)
	}

	for _, key := range partKeys {
		indices := partMap[key]

		// Sort the index slice by OVER ORDER BY (stable to preserve ties).
		if len(spec.orderBy) > 0 {
			sort.SliceStable(indices, func(a, b int) bool {
				return winLess(rows[indices[a]], rows[indices[b]], spec.orderBy)
			})
		}

		n := len(indices)

		// Determine effective frame for frame-based functions.
		// SQL standard defaults:
		//   • ORDER BY present, no explicit frame → RANGE UNBOUNDED PRECEDING TO CURRENT ROW
		//   • No ORDER BY, no explicit frame       → ROWS UNBOUNDED PRECEDING TO UNBOUNDED FOLLOWING (whole partition)
		frameRows := spec.frameRows
		frameStart := spec.frameStart
		frameEnd := spec.frameEnd
		if !spec.hasFrame {
			if len(spec.orderBy) > 0 {
				frameRows = false // RANGE
				frameStart = winBound{kind: winBoundUnboundedPreceding}
				frameEnd = winBound{kind: winBoundCurrentRow}
			} else {
				frameRows = true // ROWS
				frameStart = winBound{kind: winBoundUnboundedPreceding}
				frameEnd = winBound{kind: winBoundUnboundedFollowing}
			}
		}

		for pos, idx := range indices {
			var val Value

			switch spec.funcName {
			// ── Ranking functions (frame-independent) ────────────────────────
			case "row_number":
				val = Value{Kind: KindInt, V: int64(pos + 1)}
			case "rank":
				val = Value{Kind: KindInt, V: winRank(pos, indices, rows, spec.orderBy)}
			case "dense_rank":
				val = Value{Kind: KindInt, V: winDenseRank(pos, indices, rows, spec.orderBy)}
			case "cume_dist":
				val = Value{Kind: KindFloat, V: winCumeDist(pos, n, indices, rows, spec.orderBy)}
			case "percent_rank":
				val = Value{Kind: KindFloat, V: winPercentRank(pos, n, indices, rows, spec.orderBy)}
			case "ntile":
				val = Value{Kind: KindInt, V: winNtile(pos, n, spec.ntileN)}

			// ── Offset functions (frame-independent) ─────────────────────────
			case "lag":
				target := pos - spec.lagOffset
				if target >= 0 {
					val = winArg(rows[indices[target]], spec.arg)
				} else if spec.lagDefault != "" {
					val = winArg(rows[idx], spec.lagDefault)
				} else {
					val = Null
				}
			case "lead":
				target := pos + spec.lagOffset
				if target < n {
					val = winArg(rows[indices[target]], spec.arg)
				} else if spec.lagDefault != "" {
					val = winArg(rows[idx], spec.lagDefault)
				} else {
					val = Null
				}

			// ── Frame-based functions ─────────────────────────────────────────
			default:
				lo, hi := winFrameSlice(pos, n, frameRows, frameStart, frameEnd, indices, rows, spec.orderBy)
				frameIdx := indices[lo:hi]
				switch spec.funcName {
				case "count":
					val = Value{Kind: KindInt, V: int64(len(frameIdx))}
				case "sum":
					val = winSum(rows, frameIdx, spec.arg)
				case "avg":
					val = winAvg(rows, frameIdx, spec.arg)
				case "min":
					val = winMin(rows, frameIdx, spec.arg)
				case "max":
					val = winMax(rows, frameIdx, spec.arg)
				case "first_value":
					if len(frameIdx) > 0 {
						val = winArg(rows[frameIdx[0]], spec.arg)
					}
				case "last_value":
					if len(frameIdx) > 0 {
						val = winArg(rows[frameIdx[len(frameIdx)-1]], spec.arg)
					}
				case "nth_value":
					n1 := spec.nthN - 1 // convert to 0-based
					if n1 >= 0 && n1 < len(frameIdx) {
						val = winArg(rows[frameIdx[n1]], spec.arg)
					}
				default:
					val = Null
				}
			}

			rows[idx][ph] = val
		}
	}

	// Rename placeholder → final alias.
	if ph != spec.alias {
		for _, row := range rows {
			if v, ok := row[ph]; ok {
				row[spec.alias] = v
				delete(row, ph)
			}
		}
	}

	// Remove helper columns that were only needed for window computation.
	for _, h := range spec.helpers {
		for _, row := range rows {
			delete(row, h)
		}
	}

	return nil
}

// ── Frame computation ─────────────────────────────────────────────────────────

// winFrameSlice returns the [lo, hi) half-open range into the sorted partition
// indices slice for the frame at position pos.
func winFrameSlice(pos, n int, frameRows bool, start, end winBound, indices []int, rows []Row, orderBy []winOrder) (lo, hi int) {
	if frameRows {
		return winRowsFrame(pos, n, start, end)
	}
	return winRangeFrame(pos, n, indices, rows, start, end, orderBy)
}

// winRowsFrame computes the ROWS-mode frame: row offsets within the sorted partition.
func winRowsFrame(pos, n int, start, end winBound) (lo, hi int) {
	lo = winBoundRow(pos, n, start)
	hi = winBoundRow(pos, n, end) + 1 // inclusive end → exclusive upper bound
	if lo < 0 {
		lo = 0
	}
	if hi > n {
		hi = n
	}
	if hi < lo {
		hi = lo
	}
	return
}

// winBoundRow converts a frame bound to a row offset within the sorted partition.
func winBoundRow(pos, n int, b winBound) int {
	switch b.kind {
	case winBoundUnboundedPreceding:
		return 0
	case winBoundPreceding:
		return pos - b.offset
	case winBoundCurrentRow:
		return pos
	case winBoundFollowing:
		return pos + b.offset
	case winBoundUnboundedFollowing:
		return n - 1
	}
	return pos
}

// winRangeFrame computes the RANGE-mode frame, supporting:
//   - UNBOUNDED PRECEDING … UNBOUNDED FOLLOWING → whole partition
//   - UNBOUNDED PRECEDING … CURRENT ROW         → rows through last peer
//   - CURRENT ROW         … UNBOUNDED FOLLOWING → rows from first peer to end
func winRangeFrame(pos, n int, indices []int, rows []Row, start, end winBound, orderBy []winOrder) (lo, hi int) {
	switch {
	case start.kind == winBoundUnboundedPreceding && end.kind == winBoundUnboundedFollowing:
		return 0, n
	case start.kind == winBoundUnboundedPreceding && end.kind == winBoundCurrentRow:
		lo = 0
		hi = pos + 1
		for hi < n && winEqual(rows[indices[pos]], rows[indices[hi]], orderBy) {
			hi++
		}
	case start.kind == winBoundCurrentRow && end.kind == winBoundUnboundedFollowing:
		lo = pos
		for lo > 0 && winEqual(rows[indices[pos]], rows[indices[lo-1]], orderBy) {
			lo--
		}
		hi = n
	default:
		// Unsupported RANGE combination: fall back to current row only.
		lo, hi = pos, pos+1
	}
	return
}

// ── Aggregate helpers ─────────────────────────────────────────────────────────

func winPartKey(row Row, partBy []string) string {
	if len(partBy) == 0 {
		return ""
	}
	var sb strings.Builder
	for _, col := range partBy {
		fmt.Fprintf(&sb, "%v\x00", row[col].V)
	}
	return sb.String()
}

func winLess(a, b Row, orderBy []winOrder) bool {
	for _, o := range orderBy {
		cmp := Compare(a[o.col], b[o.col])
		if cmp != 0 {
			if o.desc {
				return cmp > 0
			}
			return cmp < 0
		}
	}
	return false
}

func winEqual(a, b Row, orderBy []winOrder) bool {
	for _, o := range orderBy {
		if Compare(a[o.col], b[o.col]) != 0 {
			return false
		}
	}
	return true
}

// winRank returns the 1-based RANK for position pos.
// Rows with equal ORDER BY values share a rank; gaps follow ties (1, 2, 2, 4).
func winRank(pos int, indices []int, rows []Row, orderBy []winOrder) int64 {
	for i := 0; i < pos; i++ {
		if winEqual(rows[indices[i]], rows[indices[pos]], orderBy) {
			return winRank(i, indices, rows, orderBy)
		}
	}
	return int64(pos + 1)
}

// winDenseRank returns the 1-based DENSE_RANK (no gaps after ties: 1, 2, 2, 3).
func winDenseRank(pos int, indices []int, rows []Row, orderBy []winOrder) int64 {
	rank := int64(1)
	for i := 1; i <= pos; i++ {
		if !winEqual(rows[indices[i-1]], rows[indices[i]], orderBy) {
			rank++
		}
	}
	return rank
}

// winCumeDist returns the cumulative distribution value for position pos.
// Value = (position of last peer + 1) / partition size.
func winCumeDist(pos, n int, indices []int, rows []Row, orderBy []winOrder) float64 {
	if n == 0 {
		return 0
	}
	lastPeer := pos
	for lastPeer+1 < n && winEqual(rows[indices[pos]], rows[indices[lastPeer+1]], orderBy) {
		lastPeer++
	}
	return float64(lastPeer+1) / float64(n)
}

// winPercentRank returns the percent rank: (rank - 1) / (n - 1).
// First row in ordering always returns 0.0.
func winPercentRank(pos, n int, indices []int, rows []Row, orderBy []winOrder) float64 {
	if n <= 1 {
		return 0
	}
	rank := winRank(pos, indices, rows, orderBy)
	return float64(rank-1) / float64(n-1)
}

// winNtile assigns a 1-based bucket number for a row at position pos.
// The partition of n rows is divided as evenly as possible into buckets tiles,
// with the first (n%buckets) tiles receiving one extra row each.
func winNtile(pos, n, buckets int) int64 {
	if buckets <= 0 || n == 0 {
		return 1
	}
	if buckets >= n {
		return int64(pos + 1)
	}
	base := n / buckets
	extra := n % buckets
	largeCutoff := extra * (base + 1)
	if pos < largeCutoff {
		return int64(pos/(base+1)) + 1
	}
	return int64(extra) + int64((pos-largeCutoff)/base) + 1
}

func winSum(rows []Row, indices []int, col string) Value {
	var intSum int64
	var floatSum float64
	hasFloat := false
	for _, idx := range indices {
		v := winArg(rows[idx], col)
		switch x := v.V.(type) {
		case int64:
			intSum += x
		case float64:
			floatSum += x
			hasFloat = true
		}
	}
	if hasFloat {
		return Value{Kind: KindFloat, V: floatSum + float64(intSum)}
	}
	return Value{Kind: KindInt, V: intSum}
}

func winAvg(rows []Row, indices []int, col string) Value {
	var sum float64
	count := 0
	for _, idx := range indices {
		v := winArg(rows[idx], col)
		switch x := v.V.(type) {
		case int64:
			sum += float64(x)
			count++
		case float64:
			sum += x
			count++
		}
	}
	if count == 0 {
		return Null
	}
	return Value{Kind: KindFloat, V: sum / float64(count)}
}

func winMin(rows []Row, indices []int, col string) Value {
	result := Null
	for _, idx := range indices {
		v := winArg(rows[idx], col)
		if v.Kind == KindNull {
			continue
		}
		if result.Kind == KindNull || Compare(v, result) < 0 {
			result = v
		}
	}
	return result
}

func winMax(rows []Row, indices []int, col string) Value {
	result := Null
	for _, idx := range indices {
		v := winArg(rows[idx], col)
		if v.Kind == KindNull {
			continue
		}
		if result.Kind == KindNull || Compare(v, result) > 0 {
			result = v
		}
	}
	return result
}

// winArg resolves a column value from a row, trying exact match then suffix match
// for qualified names (e.g. "t.salary" when looking for "salary").
func winArg(row Row, col string) Value {
	if col == "" {
		return Value{Kind: KindInt, V: int64(1)}
	}
	if v, ok := row[col]; ok {
		return v
	}
	suffix := "." + col
	for k, v := range row {
		if strings.HasSuffix(k, suffix) {
			return v
		}
	}
	return Null
}
