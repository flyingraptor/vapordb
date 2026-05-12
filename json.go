package vapordb

import (
	"encoding/json"
	"fmt"
	"reflect"
	"strconv"
	"strings"
)

// jsonExtract implements JSON_EXTRACT(doc, path).
// path must be a JSON path string like '$.key' or '$.key[0]'.
// If doc is a KindString value, it is parsed as JSON first.
func jsonExtract(doc Value, path string) Value {
	if doc.Kind == KindNull {
		return Null
	}
	var raw any
	if doc.Kind == KindJSON {
		raw = doc.V
	} else if doc.Kind == KindString {
		if err := json.Unmarshal([]byte(doc.V.(string)), &raw); err != nil {
			return Null
		}
	} else {
		return Null
	}
	result := navigateJSONPath(raw, path)
	if result == nil {
		return Null
	}
	return makeJSONValue(result)
}

// jsonUnquote implements JSON_UNQUOTE(val).
// For a KindString result, returns it as-is.
// For a KindJSON string scalar, returns the string without JSON encoding.
// For other types, returns the string representation.
func jsonUnquote(v Value) Value {
	if v.Kind == KindNull {
		return Null
	}
	if v.Kind == KindJSON {
		if s, ok := v.V.(string); ok {
			return Value{Kind: KindString, V: s}
		}
		return Value{Kind: KindString, V: valueString(v)}
	}
	return Value{Kind: KindString, V: valueString(v)}
}

// jsonContainsCheck implements JSON_CONTAINS(doc, candidate).
// Returns true when doc contains all elements / key-value pairs of candidate.
// String arguments are parsed as JSON automatically.
func jsonContainsCheck(doc, candidate Value) bool {
	a := ensureJSONAny(doc)
	b := ensureJSONAny(candidate)
	if a == nil || b == nil {
		return false
	}
	return jsonContainsVal(a, b)
}

// ensureJSONAny converts a Value to a raw Go JSON value.
func ensureJSONAny(v Value) any {
	if v.Kind == KindNull {
		return nil
	}
	if v.Kind == KindJSON {
		return v.V
	}
	if v.Kind == KindString {
		var out any
		if err := json.Unmarshal([]byte(v.V.(string)), &out); err != nil {
			return nil
		}
		return out
	}
	return nil
}

// jsonContainsVal checks whether a contains all elements of b (deep).
func jsonContainsVal(a, b any) bool {
	switch bv := b.(type) {
	case map[string]any:
		av, ok := a.(map[string]any)
		if !ok {
			return false
		}
		for k, bVal := range bv {
			aVal, exists := av[k]
			if !exists {
				return false
			}
			if !jsonContainsVal(aVal, bVal) {
				return false
			}
		}
		return true
	case []any:
		av, ok := a.([]any)
		if !ok {
			return false
		}
		for _, bElem := range bv {
			found := false
			for _, aElem := range av {
				if jsonContainsVal(aElem, bElem) {
					found = true
					break
				}
			}
			if !found {
				return false
			}
		}
		return true
	default:
		return reflect.DeepEqual(a, b)
	}
}

// navigateJSONPath extracts a value from a JSON document using a simplified
// path expression. Supported forms: $.key, $.key.nested, $.arr[0], $.arr[0].key.
func navigateJSONPath(doc any, path string) any {
	path = strings.TrimSpace(path)
	// Strip optional surrounding quotes that come from SQL string literals
	if len(path) >= 2 && path[0] == '\'' && path[len(path)-1] == '\'' {
		path = path[1 : len(path)-1]
	}
	if !strings.HasPrefix(path, "$") {
		return nil
	}
	return navigateSegments(doc, path[1:])
}

func navigateSegments(doc any, path string) any {
	if path == "" {
		return doc
	}
	if strings.HasPrefix(path, ".") {
		path = path[1:]
		i := 0
		for i < len(path) && path[i] != '.' && path[i] != '[' {
			i++
		}
		key := path[:i]
		rest := path[i:]
		m, ok := doc.(map[string]any)
		if !ok {
			return nil
		}
		val, exists := m[key]
		if !exists {
			return nil
		}
		return navigateSegments(val, rest)
	}
	if strings.HasPrefix(path, "[") {
		end := strings.Index(path, "]")
		if end < 0 {
			return nil
		}
		idxStr := path[1:end]
		rest := path[end+1:]
		idx, err := strconv.Atoi(idxStr)
		if err != nil {
			return nil
		}
		arr, ok := doc.([]any)
		if !ok {
			return nil
		}
		if idx < 0 || idx >= len(arr) {
			return nil
		}
		return navigateSegments(arr[idx], rest)
	}
	return nil
}

// makeJSONValue wraps a raw Go JSON value into a vapordb Value,
// keeping objects and arrays as KindJSON and promoting scalars to their
// native vapordb kinds.
func makeJSONValue(v any) Value {
	switch x := v.(type) {
	case nil:
		return Null
	case bool:
		return Value{Kind: KindBool, V: x}
	case float64:
		if x == float64(int64(x)) {
			return Value{Kind: KindInt, V: int64(x)}
		}
		return Value{Kind: KindFloat, V: x}
	case string:
		return Value{Kind: KindString, V: x}
	case map[string]any, []any:
		return Value{Kind: KindJSON, V: v}
	default:
		return Value{Kind: KindString, V: fmt.Sprintf("%v", v)}
	}
}

// parseJSONValue parses a JSON-encoded string into a KindJSON Value.
func parseJSONValue(s string) (Value, error) {
	var v any
	if err := json.Unmarshal([]byte(s), &v); err != nil {
		return Null, fmt.Errorf("json_parse: invalid JSON: %w", err)
	}
	return Value{Kind: KindJSON, V: v}, nil
}

// jsonArrayLength returns the length of a JSON array, or Null if the value is
// not a JSON array.
func jsonArrayLength(v Value) Value {
	var raw any
	switch v.Kind {
	case KindJSON:
		raw = v.V
	case KindString:
		if err := json.Unmarshal([]byte(v.V.(string)), &raw); err != nil {
			return Null
		}
	default:
		return Null
	}
	arr, ok := raw.([]any)
	if !ok {
		return Null
	}
	return Value{Kind: KindInt, V: int64(len(arr))}
}

// jsonKeys returns the keys of a JSON object as a JSON array, or Null.
func jsonKeys(v Value) Value {
	var raw any
	switch v.Kind {
	case KindJSON:
		raw = v.V
	case KindString:
		if err := json.Unmarshal([]byte(v.V.(string)), &raw); err != nil {
			return Null
		}
	default:
		return Null
	}
	m, ok := raw.(map[string]any)
	if !ok {
		return Null
	}
	keys := make([]any, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	return Value{Kind: KindJSON, V: keys}
}
