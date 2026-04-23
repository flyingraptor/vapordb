package vapordb

import (
	"encoding/json"
	"fmt"
	"strconv"
)

// Kind is the discriminant for the Value tagged union.
type Kind int

const (
	KindNull   Kind = 0
	KindBool   Kind = 1
	KindInt    Kind = 2 // stored as int64
	KindFloat  Kind = 3 // stored as float64
	KindString Kind = 4
)

// Value is a tagged union representing a single SQL value.
type Value struct {
	Kind Kind
	V    any // nil | bool | int64 | float64 | string
}

// Null is the canonical null Value.
var Null = Value{Kind: KindNull, V: nil}

// KindOf infers the Kind from a Go value.
func KindOf(v any) Kind {
	if v == nil {
		return KindNull
	}
	switch v.(type) {
	case bool:
		return KindBool
	case int, int8, int16, int32, int64,
		uint, uint8, uint16, uint32, uint64:
		return KindInt
	case float32, float64:
		return KindFloat
	case string:
		return KindString
	}
	return KindString
}

// MakeValue creates a Value from a Go value, normalizing numeric types.
func MakeValue(v any) Value {
	switch x := v.(type) {
	case nil:
		return Null
	case bool:
		return Value{Kind: KindBool, V: x}
	case int:
		return Value{Kind: KindInt, V: int64(x)}
	case int8:
		return Value{Kind: KindInt, V: int64(x)}
	case int16:
		return Value{Kind: KindInt, V: int64(x)}
	case int32:
		return Value{Kind: KindInt, V: int64(x)}
	case int64:
		return Value{Kind: KindInt, V: x}
	case uint:
		return Value{Kind: KindInt, V: int64(x)}
	case uint8:
		return Value{Kind: KindInt, V: int64(x)}
	case uint16:
		return Value{Kind: KindInt, V: int64(x)}
	case uint32:
		return Value{Kind: KindInt, V: int64(x)}
	case uint64:
		return Value{Kind: KindInt, V: int64(x)}
	case float32:
		return Value{Kind: KindFloat, V: float64(x)}
	case float64:
		return Value{Kind: KindFloat, V: x}
	case string:
		return Value{Kind: KindString, V: x}
	}
	return Value{Kind: KindString, V: fmt.Sprintf("%v", v)}
}

// Widen returns the wider (higher-rank) of two Kinds.
func Widen(a, b Kind) Kind {
	if b > a {
		return b
	}
	return a
}

// IsConflict returns true when the incoming type is incompatible with the
// existing schema type, triggering an unsafe table wipe.
//
// Rules (per spec):
//   - NULL never conflicts.
//   - Within the numeric family (Bool < Int < Float), a downgrade conflicts;
//     an upgrade is a safe widening.
//   - Any transition between the numeric family and String (in either
//     direction) is always an unsafe conflict — e.g. int→string and
//     string→float both wipe.
func IsConflict(existing, incoming Kind) bool {
	if existing == KindNull || incoming == KindNull {
		return false
	}
	if existing == incoming {
		return false
	}
	numericFamily := func(k Kind) bool {
		return k == KindBool || k == KindInt || k == KindFloat
	}
	existingNumeric := numericFamily(existing)
	incomingNumeric := numericFamily(incoming)

	// Crossing the numeric ↔ string boundary is always a conflict.
	if existingNumeric != incomingNumeric {
		return true
	}
	// Both numeric: a downgrade (incoming < existing) is a conflict.
	return incoming < existing
}

// Compare compares two Values. Returns -1, 0, or 1.
// NULLs sort before all other values.
func Compare(a, b Value) int {
	if a.Kind == KindNull && b.Kind == KindNull {
		return 0
	}
	if a.Kind == KindNull {
		return -1
	}
	if b.Kind == KindNull {
		return 1
	}
	if isNumericKind(a.Kind) && isNumericKind(b.Kind) {
		fa, fb := numericFloat(a), numericFloat(b)
		switch {
		case fa < fb:
			return -1
		case fa > fb:
			return 1
		default:
			return 0
		}
	}
	sa, sb := valueString(a), valueString(b)
	switch {
	case sa < sb:
		return -1
	case sa > sb:
		return 1
	default:
		return 0
	}
}

// Equal returns true if two Values are equal under SQL semantics.
func Equal(a, b Value) bool {
	return Compare(a, b) == 0
}

func isNumericKind(k Kind) bool {
	return k == KindBool || k == KindInt || k == KindFloat
}

func numericFloat(v Value) float64 {
	switch x := v.V.(type) {
	case bool:
		if x {
			return 1
		}
		return 0
	case int64:
		return float64(x)
	case float64:
		return x
	case string:
		if f, err := strconv.ParseFloat(x, 64); err == nil {
			return f
		}
	}
	return 0
}

func valueString(v Value) string {
	if v.V == nil {
		return ""
	}
	return fmt.Sprintf("%v", v.V)
}

// LikeMatch implements SQL LIKE pattern matching.
// % matches any sequence of characters; _ matches any single character.
func LikeMatch(pattern, value string) bool {
	return likeMatch([]rune(pattern), []rune(value))
}

func likeMatch(pattern, s []rune) bool {
	for len(pattern) > 0 {
		switch pattern[0] {
		case '%':
			// Consume consecutive % wildcards
			for len(pattern) > 0 && pattern[0] == '%' {
				pattern = pattern[1:]
			}
			if len(pattern) == 0 {
				return true
			}
			for i := 0; i <= len(s); i++ {
				if likeMatch(pattern, s[i:]) {
					return true
				}
			}
			return false
		case '_':
			if len(s) == 0 {
				return false
			}
			pattern = pattern[1:]
			s = s[1:]
		default:
			if len(s) == 0 || pattern[0] != s[0] {
				return false
			}
			pattern = pattern[1:]
			s = s[1:]
		}
	}
	return len(s) == 0
}

// ─── JSON MARSHAL / UNMARSHAL ────────────────────────────────────────────────

type valueWire struct {
	Kind Kind `json:"kind"`
	V    any  `json:"v"`
}

func (v Value) MarshalJSON() ([]byte, error) {
	return json.Marshal(valueWire{Kind: v.Kind, V: v.V})
}

func (v *Value) UnmarshalJSON(data []byte) error {
	var raw struct {
		Kind Kind            `json:"kind"`
		V    json.RawMessage `json:"v"`
	}
	if err := json.Unmarshal(data, &raw); err != nil {
		return err
	}
	v.Kind = raw.Kind
	if raw.V == nil || string(raw.V) == "null" {
		v.V = nil
		v.Kind = KindNull
		return nil
	}
	switch raw.Kind {
	case KindNull:
		v.V = nil
	case KindBool:
		var b bool
		if err := json.Unmarshal(raw.V, &b); err != nil {
			return err
		}
		v.V = b
	case KindInt:
		var n json.Number
		if err := json.Unmarshal(raw.V, &n); err != nil {
			return err
		}
		i, err := n.Int64()
		if err != nil {
			return fmt.Errorf("decoding int64: %w", err)
		}
		v.V = i
	case KindFloat:
		var f float64
		if err := json.Unmarshal(raw.V, &f); err != nil {
			return err
		}
		v.V = f
	case KindString:
		var s string
		if err := json.Unmarshal(raw.V, &s); err != nil {
			return err
		}
		v.V = s
	}
	return nil
}
