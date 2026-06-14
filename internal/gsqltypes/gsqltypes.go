// Package gsqltypes provides a minimal pure-Go TypeKind enum for the
// bigquery-emulator codebase. Its constants use the BigQuery type names
// (INT64 etc.), and their numeric values track the googlesql.TypeKind enum
// exposed by the googlesqlite driver, so a `*googlesqlite.ColumnType.Kind`
// can be cast directly to TypeKind.
//
// This enum is hand-kept as a verbatim mirror of googlesql.TypeKind so the
// pure-Go build avoids pulling in the cgo zetasql dependency that defines the
// canonical enum. The package was renamed from zsqltypes to gsqltypes, which
// dropped the "z" that pointed at zetasql, so this note records that origin.
package gsqltypes

// TypeKind matches googlesql.TypeKind integer values.
type TypeKind int

const (
	TYPE_UNKNOWN TypeKind = 0

	INT32  TypeKind = 2
	INT64  TypeKind = 3
	UINT32 TypeKind = 4
	UINT64 TypeKind = 5
	BOOL   TypeKind = 6
	FLOAT  TypeKind = 7
	DOUBLE TypeKind = 8
	STRING TypeKind = 9
	BYTES  TypeKind = 10
	DATE   TypeKind = 11
	// ENUM and PROTO sit at 16 and 19, leaving 12-15 unfilled, because the
	// values mirror googlesql.TypeKind verbatim. BigQuery never surfaces ENUM
	// or PROTO, so they exist only to keep this enum aligned with the driver.
	ENUM       TypeKind = 16
	ARRAY      TypeKind = 17
	STRUCT     TypeKind = 18
	PROTO      TypeKind = 19
	TIMESTAMP  TypeKind = 20
	TIME       TypeKind = 21
	DATETIME   TypeKind = 22
	GEOGRAPHY  TypeKind = 23
	NUMERIC    TypeKind = 24
	BIGNUMERIC TypeKind = 25
	// EXTENDED has no String() case and renders as TYPE_UNKNOWN because the
	// emulator does not support googlesql extended types.
	EXTENDED TypeKind = 26
	JSON     TypeKind = 27
	INTERVAL TypeKind = 28
)

func (k TypeKind) String() string {
	switch k {
	case INT32:
		return "INT32"
	case INT64:
		return "INT64"
	case UINT32:
		return "UINT32"
	case UINT64:
		return "UINT64"
	case BOOL:
		return "BOOL"
	case FLOAT:
		return "FLOAT"
	case DOUBLE:
		return "DOUBLE"
	case STRING:
		return "STRING"
	case BYTES:
		return "BYTES"
	case DATE:
		return "DATE"
	case ENUM:
		return "ENUM"
	case ARRAY:
		return "ARRAY"
	case STRUCT:
		return "STRUCT"
	case PROTO:
		return "PROTO"
	case TIMESTAMP:
		return "TIMESTAMP"
	case TIME:
		return "TIME"
	case DATETIME:
		return "DATETIME"
	case GEOGRAPHY:
		return "GEOGRAPHY"
	case NUMERIC:
		return "NUMERIC"
	case BIGNUMERIC:
		return "BIGNUMERIC"
	case JSON:
		return "JSON"
	case INTERVAL:
		return "INTERVAL"
	}
	return "TYPE_UNKNOWN"
}
