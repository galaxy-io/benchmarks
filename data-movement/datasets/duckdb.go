package datasets

import "strings"

// duckDBString returns value as a DuckDB string literal. File paths come from
// the operating system and may contain apostrophes, so they must not be
// interpolated into generated scripts unescaped.
func duckDBString(value string) string {
	return "'" + strings.ReplaceAll(value, "'", "''") + "'"
}
