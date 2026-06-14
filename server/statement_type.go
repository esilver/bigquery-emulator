package server

import "strings"

// BigQuery reports the kind of statement a query job ran through
// JobStatistics2.StatementType. dbt-bigquery and other clients branch on it:
// for SELECT and CREATE_TABLE_AS_SELECT they look up the destination table to
// read the result row count, so the value must be accurate. The emulator's
// SQL backend does not expose the analyzed statement kind, so we classify the
// statement text here. The set of returned values matches the BigQuery REST
// documentation for JobStatistics2.statementType.

// classifyStatementType returns the BigQuery statementType for a query string.
// It recognizes the DDL/DML statements dbt emits; anything that produces a
// result set or is otherwise unrecognized is reported as "SELECT", which is the
// value the emulator used unconditionally before this classifier existed.
func classifyStatementType(query string) string {
	toks := leadingKeywords(query, 4)
	if len(toks) == 0 {
		return "SELECT"
	}
	switch toks[0] {
	case "SELECT", "WITH", "FROM", "VALUES":
		return "SELECT"
	case "INSERT":
		return "INSERT"
	case "UPDATE":
		return "UPDATE"
	case "DELETE":
		return "DELETE"
	case "MERGE":
		return "MERGE"
	case "TRUNCATE":
		return "TRUNCATE_TABLE"
	case "DROP":
		switch nextKeyword(toks, 1) {
		case "SCHEMA", "DATABASE":
			return "DROP_SCHEMA"
		case "VIEW":
			return "DROP_VIEW"
		case "MATERIALIZED":
			return "DROP_MATERIALIZED_VIEW"
		case "FUNCTION":
			return "DROP_FUNCTION"
		case "PROCEDURE":
			return "DROP_PROCEDURE"
		default:
			return "DROP_TABLE"
		}
	case "ALTER":
		switch nextKeyword(toks, 1) {
		case "SCHEMA", "DATABASE":
			return "ALTER_SCHEMA"
		case "VIEW":
			return "ALTER_VIEW"
		default:
			return "ALTER_TABLE"
		}
	case "CREATE":
		return classifyCreate(toks)
	}
	return "SELECT"
}

// classifyCreate distinguishes the CREATE ... family. A CREATE TABLE that has
// an AS clause is reported as CREATE_TABLE_AS_SELECT (BigQuery treats CTAS
// distinctly because it produces a destination table with rows), while a bare
// CREATE TABLE is CREATE_TABLE.
func classifyCreate(toks []string) string {
	// Skip OR REPLACE and TEMP/TEMPORARY modifiers to find the object keyword.
	i := 1
	for i < len(toks) {
		switch toks[i] {
		case "OR", "REPLACE", "TEMP", "TEMPORARY", "TABLE_AS", "PUBLIC", "PRIVATE":
			i++
			continue
		}
		break
	}
	if i >= len(toks) {
		return "CREATE_TABLE"
	}
	switch toks[i] {
	case "SCHEMA", "DATABASE":
		return "CREATE_SCHEMA"
	case "VIEW":
		return "CREATE_VIEW"
	case "MATERIALIZED":
		return "CREATE_MATERIALIZED_VIEW"
	case "FUNCTION":
		return "CREATE_FUNCTION"
	case "PROCEDURE":
		return "CREATE_PROCEDURE"
	case "TABLE":
		return "CREATE_TABLE" // upgraded to *_AS_SELECT by isCreateTableAsSelect
	}
	return "CREATE_TABLE"
}

// leadingKeywords returns up to max uppercased leading word tokens of a SQL
// statement, skipping a leading line/block comment and any leading whitespace.
// Backticks, parentheses, and other punctuation terminate a token so
// "CREATE TABLE `x`.`y`" yields ["CREATE", "TABLE"].
func leadingKeywords(query string, max int) []string {
	s := stripLeadingComments(query)
	toks := make([]string, 0, max)
	var b strings.Builder
	flush := func() {
		if b.Len() > 0 {
			toks = append(toks, strings.ToUpper(b.String()))
			b.Reset()
		}
	}
	for _, r := range s {
		if isWordRune(r) {
			b.WriteRune(r)
			continue
		}
		flush()
		if len(toks) >= max {
			return toks
		}
	}
	flush()
	if len(toks) > max {
		toks = toks[:max]
	}
	return toks
}

func isWordRune(r rune) bool {
	return r == '_' ||
		(r >= 'a' && r <= 'z') ||
		(r >= 'A' && r <= 'Z') ||
		(r >= '0' && r <= '9')
}

// nextKeyword returns the token at index i, or "" if out of range.
func nextKeyword(toks []string, i int) string {
	if i < len(toks) {
		return toks[i]
	}
	return ""
}

// stripLeadingComments removes leading whitespace and any number of leading
// "-- ..." line comments or "/* ... */" block comments. dbt prepends a
// "/* {...} */" job comment to every statement, so the first real keyword is
// otherwise hidden.
func stripLeadingComments(query string) string {
	s := strings.TrimLeft(query, " \t\r\n")
	for {
		switch {
		case strings.HasPrefix(s, "--"):
			if idx := strings.IndexByte(s, '\n'); idx >= 0 {
				s = strings.TrimLeft(s[idx+1:], " \t\r\n")
			} else {
				return ""
			}
		case strings.HasPrefix(s, "/*"):
			if idx := strings.Index(s, "*/"); idx >= 0 {
				s = strings.TrimLeft(s[idx+2:], " \t\r\n")
			} else {
				return ""
			}
		default:
			return s
		}
	}
}

// schemaDDL describes a parsed CREATE/DROP SCHEMA statement. The emulator's
// SQL engine treats CREATE SCHEMA as a no-op and rejects DROP SCHEMA outright,
// so these are handled at the metadata layer instead of being sent to the
// engine.
type schemaDDL struct {
	isCreate    bool   // true for CREATE SCHEMA, false for DROP SCHEMA
	projectID   string // optional project qualifier; "" when unqualified
	datasetID   string
	ifNotExists bool // CREATE SCHEMA IF NOT EXISTS
	ifExists    bool // DROP SCHEMA IF EXISTS
}

// parseSchemaDDL recognizes "CREATE SCHEMA [IF NOT EXISTS] <name>" and
// "DROP SCHEMA [IF EXISTS] <name> [CASCADE|RESTRICT]" and extracts the dataset
// (and optional project) identifier. It returns ok=false for anything else.
// The name may be backtick-quoted and may be "project.dataset" or "dataset".
func parseSchemaDDL(query string) (schemaDDL, bool) {
	toks := leadingKeywords(query, 2)
	if len(toks) < 2 {
		return schemaDDL{}, false
	}
	var d schemaDDL
	switch {
	case toks[0] == "CREATE" && (toks[1] == "SCHEMA" || toks[1] == "DATABASE"):
		d.isCreate = true
	case toks[0] == "DROP" && (toks[1] == "SCHEMA" || toks[1] == "DATABASE"):
		d.isCreate = false
	default:
		return schemaDDL{}, false
	}

	rest := afterSchemaKeyword(query)
	upper := strings.ToUpper(rest)
	if d.isCreate {
		if strings.HasPrefix(upper, "IF NOT EXISTS") {
			d.ifNotExists = true
			rest = trimLeadingWords(rest, 3)
		}
	} else {
		if strings.HasPrefix(upper, "IF EXISTS") {
			d.ifExists = true
			rest = trimLeadingWords(rest, 2)
		}
	}

	name := firstIdentifier(rest)
	if name == "" {
		return schemaDDL{}, false
	}
	parts := splitQualifiedName(name)
	switch len(parts) {
	case 1:
		d.datasetID = parts[0]
	case 2:
		d.projectID = parts[0]
		d.datasetID = parts[1]
	default:
		// project.dataset is the deepest a schema name goes.
		d.projectID = parts[len(parts)-2]
		d.datasetID = parts[len(parts)-1]
	}
	if d.datasetID == "" {
		return schemaDDL{}, false
	}
	return d, true
}

// afterSchemaKeyword returns the statement text after the leading
// "CREATE|DROP SCHEMA|DATABASE" keywords, with leading whitespace trimmed.
func afterSchemaKeyword(query string) string {
	return trimLeadingWords(stripLeadingComments(query), 2)
}

// trimLeadingWords drops the first n whitespace-delimited words from s and
// returns the remainder with leading whitespace trimmed. Words are delimited by
// ASCII whitespace only, so a backtick-quoted identifier stays intact.
func trimLeadingWords(s string, n int) string {
	s = strings.TrimLeft(s, " \t\r\n")
	for ; n > 0; n-- {
		i := strings.IndexAny(s, " \t\r\n")
		if i < 0 {
			return ""
		}
		s = strings.TrimLeft(s[i:], " \t\r\n")
	}
	return s
}

// firstIdentifier returns the first (possibly dotted) identifier at the start
// of s. It handles three shapes BigQuery clients emit for a schema name:
//
//   - a single backtick-quoted name that may itself contain dots:
//     "`proj.dataset`"
//   - a dotted path of separately backtick-quoted segments:
//     "`proj`.`dataset`" (this is what dbt renders)
//   - a bare dotted path: "proj.dataset"
//
// Parsing stops at the first separator (whitespace, "(", ";") that is not
// inside backticks. Backticks are stripped from each segment and the segments
// are rejoined with ".", so every shape yields "proj.dataset".
func firstIdentifier(s string) string {
	s = strings.TrimLeft(s, " \t\r\n")
	if s == "" {
		return ""
	}
	var segments []string
	var b strings.Builder
	inQuote := false
	for _, r := range s {
		switch {
		case r == '`':
			inQuote = !inQuote
		case inQuote:
			b.WriteRune(r)
		case r == '.':
			segments = append(segments, b.String())
			b.Reset()
		case r == ' ' || r == '\t' || r == '\r' || r == '\n' || r == '(' || r == ';':
			segments = append(segments, b.String())
			return joinNonEmpty(segments)
		default:
			b.WriteRune(r)
		}
	}
	segments = append(segments, b.String())
	return joinNonEmpty(segments)
}

// joinNonEmpty joins identifier segments with "." after dropping empties so a
// trailing dot or blank segment does not produce "a." or ".b".
func joinNonEmpty(segments []string) string {
	parts := make([]string, 0, len(segments))
	for _, s := range segments {
		if s != "" {
			parts = append(parts, s)
		}
	}
	return strings.Join(parts, ".")
}

// splitQualifiedName splits a possibly-dotted identifier into its parts,
// stripping per-part backticks (e.g. "`proj`.`ds`" -> ["proj","ds"]).
func splitQualifiedName(name string) []string {
	raw := strings.Split(name, ".")
	parts := make([]string, 0, len(raw))
	for _, p := range raw {
		p = strings.Trim(p, "`")
		if p != "" {
			parts = append(parts, p)
		}
	}
	return parts
}

// isCreateTableAsSelect reports whether a CREATE TABLE statement has an AS
// <query> clause (CTAS) rather than a column-list definition. It scans for an
// "AS" keyword token that appears before any "(" - a column-list "CREATE TABLE
// t (a INT64)" has its "(" first, while "CREATE TABLE t AS SELECT ..." (and
// "CREATE TABLE t (a INT64) AS SELECT" is not valid) reaches AS first.
func isCreateTableAsSelect(query string) bool {
	s := stripLeadingComments(query)
	var b strings.Builder
	for _, r := range s {
		if isWordRune(r) {
			b.WriteRune(r)
			continue
		}
		word := strings.ToUpper(b.String())
		b.Reset()
		if word == "AS" {
			return true
		}
		if r == '(' {
			return false
		}
	}
	return strings.ToUpper(b.String()) == "AS"
}
