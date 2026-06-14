package server

import (
	"fmt"
	"strconv"
	"strings"
	"time"

	bigqueryv2 "google.golang.org/api/bigquery/v2"
)

// csvNullTokens are the textual values BigQuery's CSV autodetect treats as
// NULL; such values do not constrain a column's inferred type.
var csvNullTokens = map[string]struct{}{"": {}, "null": {}}

// normalizeSchemaFieldTypes upper-cases every field's type name (recursively
// for RECORD/STRUCT fields). BigQuery type names are case-insensitive and
// clients send mixed case: dbt's seed loader, for instance, emits lowercase
// "int64"/"string". The emulator's GoogleSQL analyzer only knows the canonical
// upper-case spellings (INT64, STRING, ...), so a lowercase type would be
// rejected as TYPE_UNKNOWN. Normalizing here keeps the analyzer happy without
// constraining the client. Blank types are left untouched so the caller can
// still detect "unspecified" and infer.
func normalizeSchemaFieldTypes(schema *bigqueryv2.TableSchema) {
	if schema == nil {
		return
	}
	for _, f := range schema.Fields {
		if f == nil {
			continue
		}
		if f.Type != "" {
			f.Type = strings.ToUpper(f.Type)
		}
		if len(f.Fields) > 0 {
			normalizeSchemaFieldTypes(&bigqueryv2.TableSchema{Fields: f.Fields})
		}
	}
}

// schemaHasUnspecifiedTypes reports whether a load schema carries at least one
// field whose type is blank. The Go BigQuery client omits an empty field type
// from the JSON it sends, so a field that arrives with a name but no type is
// the signal that the client expects type inference (this is what dbt's seed
// loads send when no column_types are configured). A nil schema is not
// "unspecified": that path is governed by the autodetect flag instead.
func schemaHasUnspecifiedTypes(schema *bigqueryv2.TableSchema) bool {
	if schema == nil {
		return false
	}
	for _, f := range schema.Fields {
		if f != nil && fieldTypeUnspecified(f.Type) {
			return true
		}
	}
	return false
}

// fieldTypeUnspecified reports whether a field type string carries no usable
// BigQuery type. Both the empty string (the wire form of an omitted type) and
// the analyzer's TYPE_UNKNOWN sentinel count as unspecified.
func fieldTypeUnspecified(t string) bool {
	return t == "" || strings.EqualFold(t, "TYPE_UNKNOWN")
}

// mergeInferredTypes returns a schema that keeps the client-supplied field
// names, order, and any explicitly set types, filling only the unspecified
// types from the inferred schema (matched by name, falling back to position).
// When the client supplied no schema, the inferred schema is returned as-is.
func mergeInferredTypes(provided, inferred *bigqueryv2.TableSchema) *bigqueryv2.TableSchema {
	if provided == nil || len(provided.Fields) == 0 {
		return inferred
	}
	if inferred == nil {
		return provided
	}
	byName := make(map[string]*bigqueryv2.TableFieldSchema, len(inferred.Fields))
	for _, f := range inferred.Fields {
		if f != nil {
			byName[f.Name] = f
		}
	}
	merged := make([]*bigqueryv2.TableFieldSchema, len(provided.Fields))
	for i, f := range provided.Fields {
		nf := *f
		if fieldTypeUnspecified(nf.Type) {
			if src, ok := byName[nf.Name]; ok {
				nf.Type = src.Type
			} else if i < len(inferred.Fields) && inferred.Fields[i] != nil {
				nf.Type = inferred.Fields[i].Type
			} else {
				nf.Type = "STRING"
			}
		}
		if nf.Mode == "" {
			nf.Mode = "NULLABLE"
		}
		merged[i] = &nf
	}
	return &bigqueryv2.TableSchema{Fields: merged}
}

func isCSVNull(s string) bool {
	_, ok := csvNullTokens[strings.ToLower(s)]
	return ok
}

func csvLooksBool(s string) bool {
	switch strings.ToLower(s) {
	case "true", "false":
		return true
	}
	return false
}

func csvLooksInt(s string) bool {
	_, err := strconv.ParseInt(s, 10, 64)
	return err == nil
}

func csvLooksFloat(s string) bool {
	_, err := strconv.ParseFloat(s, 64)
	return err == nil
}

func csvLooksDate(s string) bool {
	_, err := time.Parse("2006-01-02", s)
	return err == nil
}

func csvLooksTimestamp(s string) bool {
	for _, layout := range []string{
		"2006-01-02",
		"2006-01-02 15:04:05",
		"2006-01-02T15:04:05",
		"2006-01-02 15:04:05.999999999",
		time.RFC3339,
		time.RFC3339Nano,
	} {
		if _, err := time.Parse(layout, s); err == nil {
			return true
		}
	}
	return false
}

// inferCSVColumnType picks the narrowest BigQuery type that fits every
// non-null value in a column, in BigQuery's preference order
// (BOOL > INT64 > FLOAT64 > DATE > TIMESTAMP), and falls back to STRING. A
// column with no non-null values is typed STRING.
func inferCSVColumnType(values []string) string {
	allBool, allInt, allFloat, allDate, allTimestamp := true, true, true, true, true
	seen := false
	for _, v := range values {
		if isCSVNull(v) {
			continue
		}
		seen = true
		allBool = allBool && csvLooksBool(v)
		allInt = allInt && csvLooksInt(v)
		allFloat = allFloat && csvLooksFloat(v)
		allDate = allDate && csvLooksDate(v)
		allTimestamp = allTimestamp && csvLooksTimestamp(v)
	}
	switch {
	case !seen:
		return "STRING"
	case allBool:
		return "BOOLEAN"
	case allInt:
		return "INTEGER"
	case allFloat:
		return "FLOAT"
	case allDate:
		return "DATE"
	case allTimestamp:
		return "TIMESTAMP"
	default:
		return "STRING"
	}
}

// inferCSVSchema infers a table schema from CSV records, taking the first row
// as the header (column names) and the remaining rows as sample data. It backs
// the load job's autodetect option.
func inferCSVSchema(records [][]string) (*bigqueryv2.TableSchema, error) {
	if len(records) == 0 {
		return nil, fmt.Errorf("cannot autodetect schema: the CSV has no rows")
	}
	header := records[0]
	dataRows := records[1:]
	fields := make([]*bigqueryv2.TableFieldSchema, len(header))
	for i, name := range header {
		column := make([]string, 0, len(dataRows))
		for _, row := range dataRows {
			if i < len(row) {
				column = append(column, row[i])
			}
		}
		fields[i] = &bigqueryv2.TableFieldSchema{
			Name: name,
			Type: inferCSVColumnType(column),
			Mode: "NULLABLE",
		}
	}
	return &bigqueryv2.TableSchema{Fields: fields}, nil
}

func inferCSVSchemaWithoutHeader(records [][]string) (*bigqueryv2.TableSchema, error) {
	if len(records) == 0 {
		return nil, fmt.Errorf("cannot autodetect schema: the CSV has no rows")
	}
	columnCount := len(records[0])
	fields := make([]*bigqueryv2.TableFieldSchema, columnCount)
	for i := 0; i < columnCount; i++ {
		column := make([]string, 0, len(records))
		for _, row := range records {
			if i < len(row) {
				column = append(column, row[i])
			}
		}
		fields[i] = &bigqueryv2.TableFieldSchema{
			Name: fmt.Sprintf("string_field_%d", i),
			Type: inferCSVColumnType(column),
			Mode: "NULLABLE",
		}
	}
	return &bigqueryv2.TableSchema{Fields: fields}, nil
}
