package vapordb

import "fmt"

// ─── ENUM VALIDATION ─────────────────────────────────────────────────────────

// validateEnum returns an error if any column in row violates an enum
// constraint recorded in tbl.EnumSets. NULL values are always accepted.
func validateEnum(tbl *Table, row Row) error {
	if len(tbl.EnumSets) == 0 {
		return nil
	}
	for col, allowed := range tbl.EnumSets {
		val, ok := row[col]
		if !ok || val.Kind == KindNull {
			continue
		}
		s := valueString(val)
		for _, a := range allowed {
			if a == s {
				goto next
			}
		}
		return fmt.Errorf("column %q: value %q is not in the enum set %v", col, s, allowed)
	next:
	}
	return nil
}

// validateEnumColumn returns an error if val violates the enum constraint on
// col in tbl. NULL values are always accepted.
func validateEnumColumn(tbl *Table, col string, val Value) error {
	if len(tbl.EnumSets) == 0 {
		return nil
	}
	allowed, ok := tbl.EnumSets[col]
	if !ok || val.Kind == KindNull {
		return nil
	}
	s := valueString(val)
	for _, a := range allowed {
		if a == s {
			return nil
		}
	}
	return fmt.Errorf("column %q: value %q is not in the enum set %v", col, s, allowed)
}

// ─── HELPERS ─────────────────────────────────────────────────────────────────

// mergeEnumValues returns a new slice containing all values from existing plus
// any values from incoming not already present, preserving order.
func mergeEnumValues(existing, incoming []string) []string {
	seen := make(map[string]bool, len(existing))
	result := make([]string, len(existing))
	copy(result, existing)
	for _, v := range existing {
		seen[v] = true
	}
	for _, v := range incoming {
		if !seen[v] {
			seen[v] = true
			result = append(result, v)
		}
	}
	return result
}
