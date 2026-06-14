package server

import (
	"time"

	"github.com/parquet-go/parquet-go"

	"github.com/goccy/bigquery-emulator/types"
)

// ===== Fork: parquet temporal-load helpers =====
// Fork-added helpers for the parquet load path (zone-shift fix), kept grouped
// here so they stay separable from the upstream REST handlers for a future
// extraction or rebase. See UPSTREAMING.md.

// parquetLeafUnit captures the per-leaf parquet logical temporal scale needed
// to interpret the raw integer a TIMESTAMP/TIME column reconstructs into.
// DATE carries no unit (it is a day count) and TIMESTAMP/TIME default to
// nanoseconds when the writer omitted a unit, matching parquet-go's own
// AssignValue default.
type parquetLeafUnit struct {
	millis bool
	micros bool
	nanos  bool
}

// parquetLeafUnits maps each leaf column name in the parquet schema to its
// logical TimeUnit (when the leaf is a TIMESTAMP or TIME). Non-temporal leaves
// are omitted. The map is keyed by the leaf's field name, which matches the
// BigQuery column name the writer used.
func parquetLeafUnits(schema *parquet.Schema) map[string]parquetLeafUnit {
	units := map[string]parquetLeafUnit{}
	if schema == nil {
		return units
	}
	for _, f := range schema.Fields() {
		if !f.Leaf() {
			continue
		}
		lt := f.Type().LogicalType()
		if lt == nil {
			continue
		}
		switch {
		case lt.Timestamp != nil:
			units[f.Name()] = parquetLeafUnit{
				millis: lt.Timestamp.Unit.Millis != nil,
				micros: lt.Timestamp.Unit.Micros != nil,
				nanos:  lt.Timestamp.Unit.Nanos != nil,
			}
		case lt.Time != nil:
			units[f.Name()] = parquetLeafUnit{
				millis: lt.Time.Unit.Millis != nil,
				micros: lt.Time.Unit.Micros != nil,
				nanos:  lt.Time.Unit.Nanos != nil,
			}
		}
	}
	return units
}

// asInt64 coerces the numeric Go value parquet-go reconstructs for an INT32 or
// INT64 physical column into an int64. Reconstruct can widen DATE (INT32) to
// int32 or int64 depending on the path, so accept both.
func asInt64(v interface{}) (int64, bool) {
	switch n := v.(type) {
	case int64:
		return n, true
	case int32:
		return int64(n), true
	case int:
		return int64(n), true
	default:
		return 0, false
	}
}

// unitNanos returns the nanosecond multiplier for a parquet TimeUnit, defaulting
// to 1 (nanoseconds) when no unit was recorded, matching parquet-go.
func unitNanos(u parquetLeafUnit) int64 {
	switch {
	case u.millis:
		return 1e6
	case u.micros:
		return 1e3
	default:
		return 1
	}
}

// convertParquetTemporal rewrites the temporal cells of a reconstructed parquet
// row in place. parquet-go materializes a TIMESTAMP/DATE/TIME leaf into an
// interface{} destination as a raw integer (micros/millis/nanos since epoch, a
// day count, or time-of-day units), so without this step the integer would bind
// straight into a real temporal column. Each temporal cell becomes a UTC
// time.Time; AddTableData renders DATE/DATETIME/TIME (the civil, zoneless types)
// to a civil-form string at the bind seam. Cells that are nil or not the
// expected integer shape are left untouched.
func convertParquetTemporal(row map[string]interface{}, columns []*types.Column, units map[string]parquetLeafUnit) {
	for _, col := range columns {
		switch col.Type {
		case types.TIMESTAMP, types.DATETIME, types.DATE, types.TIME:
		default:
			continue
		}
		raw, ok := row[col.Name]
		if !ok || raw == nil {
			continue
		}
		n, ok := asInt64(raw)
		if !ok {
			continue
		}
		switch col.Type {
		case types.DATE:
			row[col.Name] = time.Unix(n*86400, 0).UTC()
		default:
			row[col.Name] = time.Unix(0, n*unitNanos(units[col.Name])).UTC()
		}
	}
}
