package vapordb

import (
	"encoding/json"
	"fmt"
	"strconv"
	"time"
)

// Kind is the discriminant for the Value tagged union.
type Kind int

const (
	KindNull   Kind = 0
	KindBool   Kind = 1
	KindInt    Kind = 2 // stored as int64
	KindFloat  Kind = 3 // stored as float64
	KindString Kind = 4
	KindDate   Kind = 5 // stored as time.Time
)

// Value is a tagged union representing a single SQL value.
type Value struct {
	Kind Kind
	V    any // nil | bool | int64 | float64 | string | time.Time
}

// dateFormats lists the formats tried when parsing a string as a date/datetime.
var dateFormats = []string{
	"2006-01-02",
	"2006-01-02 15:04:05",
	"2006-01-02T15:04:05",
	time.RFC3339,
	time.RFC3339Nano,
}

// tryParseDate tries to parse s as a date/datetime using known formats.
// Returns the parsed time and true on success.
func tryParseDate(s string) (time.Time, bool) {
	for _, f := range dateFormats {
		if t, err := time.Parse(f, s); err == nil {
			return t, true
		}
	}
	return time.Time{}, false
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
	case time.Time:
		return KindDate
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
	case time.Time:
		return Value{Kind: KindDate, V: x}
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
	// Dates are their own family; any transition to or from KindDate conflicts.
	if existing == KindDate || incoming == KindDate {
		return true
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
// When one operand is KindDate the other is coerced to time.Time so that
// string literals like '2024-01-15' compare naturally against date columns.
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
	if a.Kind == KindDate || b.Kind == KindDate {
		ta, tb := valueToTime(a), valueToTime(b)
		switch {
		case ta.Before(tb):
			return -1
		case ta.After(tb):
			return 1
		default:
			return 0
		}
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

// valueToTime extracts or parses a time.Time from a Value.
// Used when at least one operand in a comparison is KindDate.
func valueToTime(v Value) time.Time {
	if t, ok := v.V.(time.Time); ok {
		return t
	}
	if s, ok := v.V.(string); ok {
		if t, ok := tryParseDate(s); ok {
			return t
		}
	}
	return time.Time{}
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
	if t, ok := v.V.(time.Time); ok {
		if t.Hour() == 0 && t.Minute() == 0 && t.Second() == 0 && t.Nanosecond() == 0 {
			return t.UTC().Format("2006-01-02")
		}
		return t.UTC().Format("2006-01-02 15:04:05")
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
	wire := valueWire{Kind: v.Kind, V: v.V}
	if t, ok := v.V.(time.Time); ok {
		// Serialize as RFC3339 so the kind discriminant survives a round-trip.
		wire.V = t.UTC().Format(time.RFC3339Nano)
	}
	return json.Marshal(wire)
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
	case KindDate:
		var s string
		if err := json.Unmarshal(raw.V, &s); err != nil {
			return err
		}
		t, ok := tryParseDate(s)
		if !ok {
			return fmt.Errorf("decoding date: cannot parse %q", s)
		}
		v.V = t
	}
	return nil
}
