package vapordb

import (
	"fmt"
	"regexp"
	"sort"
	"strings"
)

// ── AST ───────────────────────────────────────────────────────────────────────

type winSpec struct {
	idx         int
	funcName    string     // lower-cased: row_number, rank, dense_rank, count, sum, avg, min, max
	arg         string     // placeholder column name for the argument, or "" for COUNT(*)
	partBy      []string   // placeholder column names derived from PARTITION BY
	orderBy     []winOrder // ORDER BY inside OVER(…), referencing placeholder column names
	alias       string     // final column name in result rows
	placeholder string     // __win_N__ used during SQL execution (value = 0 before post-process)
	helpers     []string   // extra placeholder columns injected into the query, to be removed after
}

type winOrder struct {
	col  string
	desc bool
}

// winRE matches:  FUNC([args]) OVER([spec]) [AS alias]
// [^()]* keeps us inside a single paren level (covers the vast majority of cases).
var winRE = regexp.MustCompile(
	`(?i)\b(ROW_NUMBER|RANK|DENSE_RANK|COUNT|SUM|AVG|MIN|MAX)\s*\(([^()]*)\)\s+OVER\s*\(([^()]*)\)(?:\s+AS\s+(\w+))?`,
)

// ── Extraction ────────────────────────────────────────────────────────────────

// extractWindowFuncs detects window function expressions in sql and:
//  1. Replaces each  fn(…) OVER(…) [AS alias]  with  0 AS __win_N__
//  2. Injects  col AS __win_N_hM__  helper expressions before FROM so that
//     ORDER BY / PARTITION BY / aggregate columns are available in projected rows.
//
// applyWindowFuncs must be called on the result rows to fill in computed values
// and clean up the helper columns.
func extractWindowFuncs(sql string) (string, []winSpec, error) {
	var specs []winSpec
	var extra []string // "col AS __win_N_hM__" injected into SELECT
	idx := 0

	result := winRE.ReplaceAllStringFunc(sql, func(match string) string {
		sub := winRE.FindStringSubmatch(match)
		funcName := strings.ToLower(sub[1])
		rawArg := strings.ToLower(strings.TrimSpace(sub[2]))
		overContent := strings.TrimSpace(sub[3])
		alias := strings.ToLower(strings.TrimSpace(sub[4]))

		ph := fmt.Sprintf("__win_%d__", idx)
		if alias == "" {
			alias = ph
		}

		rawPartBy, rawOrderBy := parseOverClause(overContent)

		var helpers []string

		// Helper for the aggregate argument column (e.g. salary in SUM(salary)).
		argPH := ""
		if rawArg != "" && rawArg != "*" {
			argPH = fmt.Sprintf("__win_%d_a__", idx)
			col := fmt.Sprintf("%s AS %s", rawArg, argPH)
			extra = append(extra, col)
			helpers = append(helpers, argPH)
		}

		// Helpers for PARTITION BY columns.
		var resolvedPartBy []string
		for ci, p := range rawPartBy {
			phCol := fmt.Sprintf("__win_%d_p%d__", idx, ci)
			extra = append(extra, fmt.Sprintf("%s AS %s", p, phCol))
			helpers = append(helpers, phCol)
			resolvedPartBy = append(resolvedPartBy, phCol)
		}

		// Helpers for ORDER BY columns.
		var resolvedOrderBy []winOrder
		for ci, o := range rawOrderBy {
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
		})
		idx++
		return fmt.Sprintf("0 AS %s", ph)
	})

	// Inject helper columns into the outermost SELECT list, right before FROM.
	if len(extra) > 0 {
		result = injectBeforeFrom(result, extra)
	}

	return result, specs, nil
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

// parseOverClause parses the body of OVER(…) into PARTITION BY and ORDER BY parts.
func parseOverClause(content string) (partBy []string, orderBy []winOrder) {
	upper := strings.ToUpper(content)

	// PARTITION BY section
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
				partBy = append(partBy, c)
			}
		}
	}

	// ORDER BY section
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
			orderBy = append(orderBy, winOrder{col: col, desc: desc})
		}
	}
	return
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

		// Sort the index slice by OVER ORDER BY (stable to preserve insertion order on ties).
		if len(spec.orderBy) > 0 {
			sort.SliceStable(indices, func(a, b int) bool {
				return winLess(rows[indices[a]], rows[indices[b]], spec.orderBy)
			})
		}

		n := len(indices)

		// Pre-compute a single aggregate value for the whole partition when applicable.
		var partVal *Value
		switch spec.funcName {
		case "count":
			v := Value{Kind: KindInt, V: int64(n)}
			partVal = &v
		case "sum":
			v := winSum(rows, indices, spec.arg)
			partVal = &v
		case "avg":
			v := winAvg(rows, indices, spec.arg)
			partVal = &v
		case "min":
			v := winMin(rows, indices, spec.arg)
			partVal = &v
		case "max":
			v := winMax(rows, indices, spec.arg)
			partVal = &v
		}

		// Write per-row values.
		for pos, idx := range indices {
			var val Value
			if partVal != nil {
				val = *partVal
			} else {
				switch spec.funcName {
				case "row_number":
					val = Value{Kind: KindInt, V: int64(pos + 1)}
				case "rank":
					val = Value{Kind: KindInt, V: winRank(pos, indices, rows, spec.orderBy)}
				case "dense_rank":
					val = Value{Kind: KindInt, V: winDenseRank(pos, indices, rows, spec.orderBy)}
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

// ── Helpers ───────────────────────────────────────────────────────────────────

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

// winRank returns the 1-based RANK for position pos. Rows with the same ORDER BY
// value get the same rank; gaps appear after ties (e.g. 1, 2, 2, 4).
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
