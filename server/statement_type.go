package server

import (
	"strings"

	"github.com/goccy/googlesqlite"
	bigqueryv2 "google.golang.org/api/bigquery/v2"

	internaltypes "github.com/goccy/bigquery-emulator/internal/types"
)

// statementTypeForQuery derives the BigQuery statementType
// (https://cloud.google.com/bigquery/docs/reference/rest/v2/Job#JobStatistics2)
// for a query job. The leading keywords of the statement give the verb and
// object; the ChangedCatalog the dialect layer reports after execution (nil
// for failed or dry-run jobs) refines CREATE TABLE vs CREATE TABLE AS SELECT.
//
// Clients branch on this value: dbt-bigquery in particular only calls
// get_table(job.destination) for SELECT, so DDL must not be reported as
// SELECT (issue #4).
func statementTypeForQuery(query string, cc *googlesqlite.ChangedCatalog) string {
	tokens := scanLeadingTokens(query, 16)
	if len(tokens) == 0 {
		return "SELECT"
	}
	switch tokens[0] {
	case "(", "SELECT", "WITH":
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
	case "ASSERT":
		return "ASSERT"
	case "CALL":
		return "CALL"
	case "EXPORT":
		return "EXPORT_DATA"
	case "LOAD":
		return "LOAD_DATA"
	case "BEGIN", "DECLARE", "SET", "IF", "WHILE", "LOOP", "FOR":
		return "SCRIPT"
	case "CREATE":
		obj, modifiers := ddlObject(tokens[1:])
		switch obj {
		case "SCHEMA":
			return "CREATE_SCHEMA"
		case "VIEW":
			if modifiers["MATERIALIZED"] {
				return "CREATE_MATERIALIZED_VIEW"
			}
			return "CREATE_VIEW"
		case "FUNCTION":
			if modifiers["TABLE"] {
				return "CREATE_TABLE_FUNCTION"
			}
			return "CREATE_FUNCTION"
		case "PROCEDURE":
			return "CREATE_PROCEDURE"
		case "TABLE":
			if modifiers["EXTERNAL"] {
				return "CREATE_EXTERNAL_TABLE"
			}
			if modifiers["SNAPSHOT"] {
				return "CREATE_SNAPSHOT_TABLE"
			}
			if isCreateTableAsSelect(query, tokens, cc) {
				return "CREATE_TABLE_AS_SELECT"
			}
			return "CREATE_TABLE"
		}
	case "DROP":
		obj, modifiers := ddlObject(tokens[1:])
		switch obj {
		case "SCHEMA":
			return "DROP_SCHEMA"
		case "VIEW":
			if modifiers["MATERIALIZED"] {
				return "DROP_MATERIALIZED_VIEW"
			}
			return "DROP_VIEW"
		case "FUNCTION":
			if modifiers["TABLE"] {
				return "DROP_TABLE_FUNCTION"
			}
			return "DROP_FUNCTION"
		case "PROCEDURE":
			return "DROP_PROCEDURE"
		case "TABLE":
			if modifiers["EXTERNAL"] {
				return "DROP_EXTERNAL_TABLE"
			}
			if modifiers["SNAPSHOT"] {
				return "DROP_SNAPSHOT_TABLE"
			}
			return "DROP_TABLE"
		}
	case "ALTER":
		obj, modifiers := ddlObject(tokens[1:])
		switch obj {
		case "SCHEMA":
			return "ALTER_SCHEMA"
		case "VIEW":
			if modifiers["MATERIALIZED"] {
				return "ALTER_MATERIALIZED_VIEW"
			}
			return "ALTER_VIEW"
		case "TABLE":
			return "ALTER_TABLE"
		}
	}
	// Unrecognized statements keep the historical SELECT default rather
	// than inventing a statement type the vocabulary does not contain.
	return "SELECT"
}

// ddlObject scans the tokens that follow a CREATE/DROP/ALTER verb and
// returns the object keyword (TABLE, VIEW, SCHEMA, FUNCTION, PROCEDURE)
// together with the set of modifier keywords that preceded it (OR REPLACE,
// TEMP, MATERIALIZED, EXTERNAL, SNAPSHOT, TABLE in "TABLE FUNCTION", ...).
func ddlObject(tokens []string) (string, map[string]bool) {
	modifiers := map[string]bool{}
	for i, tok := range tokens {
		switch tok {
		case "OR", "REPLACE", "TEMP", "TEMPORARY", "IF", "NOT", "EXISTS",
			"MATERIALIZED", "EXTERNAL", "SNAPSHOT", "AGGREGATE":
			modifiers[tok] = true
		case "TABLE":
			// "TABLE FUNCTION" declares a function, not a table.
			if i+1 < len(tokens) && tokens[i+1] == "FUNCTION" {
				modifiers["TABLE"] = true
				continue
			}
			return "TABLE", modifiers
		case "SCHEMA", "VIEW", "FUNCTION", "PROCEDURE":
			return tok, modifiers
		default:
			return "", modifiers
		}
	}
	return "", modifiers
}

// isCreateTableAsSelect decides between CREATE_TABLE and
// CREATE_TABLE_AS_SELECT. When the dialect layer reported the change it is
// authoritative: a CTAS spec carries the backing query, a plain CREATE
// TABLE does not. Otherwise (failed job, dry run) fall back to looking for
// a top-level AS keyword after the table name — the token scanner skips
// the column-definition list and OPTIONS(...) clause, both parenthesized,
// so a bare AS only appears for CTAS.
func isCreateTableAsSelect(query string, tokens []string, cc *googlesqlite.ChangedCatalog) bool {
	if cc != nil && cc.Table != nil {
		for _, spec := range cc.Table.Added {
			if !spec.IsView {
				return spec.Query != ""
			}
		}
	}
	for _, tok := range tokens {
		if tok == "AS" {
			return true
		}
	}
	return false
}

// queryJobStatistics builds the statistics.query block for a finished (or
// failed) query job. DDL statements additionally carry
// ddlOperationPerformed and ddlTargetTable, which real BigQuery populates
// and clients such as dbt read instead of a destination table.
func queryJobStatistics(query string, response *internaltypes.QueryResponse, totalBytes int64) *bigqueryv2.JobStatistics2 {
	var cc *googlesqlite.ChangedCatalog
	if response != nil {
		cc = response.ChangedCatalog
	}
	stmtType := statementTypeForQuery(query, cc)
	stats := &bigqueryv2.JobStatistics2{
		CacheHit:            false,
		StatementType:       stmtType,
		TotalBytesBilled:    totalBytes,
		TotalBytesProcessed: totalBytes,
	}
	switch {
	case strings.HasPrefix(stmtType, "CREATE_"):
		stats.DdlOperationPerformed = "CREATE"
		if tokens := scanLeadingTokens(query, 3); len(tokens) >= 3 && tokens[1] == "OR" && tokens[2] == "REPLACE" {
			stats.DdlOperationPerformed = "REPLACE"
		}
	case strings.HasPrefix(stmtType, "DROP_"):
		stats.DdlOperationPerformed = "DROP"
	case strings.HasPrefix(stmtType, "ALTER_"):
		stats.DdlOperationPerformed = "ALTER"
	}
	if stats.DdlOperationPerformed != "" {
		stats.DdlTargetTable = ddlTargetTable(cc)
	}
	return stats
}

// ddlTargetTable extracts the project.dataset.table reference of the DDL's
// target from the catalog change, when one was reported.
func ddlTargetTable(cc *googlesqlite.ChangedCatalog) *bigqueryv2.TableReference {
	if cc == nil || cc.Table == nil {
		return nil
	}
	for _, specs := range [][]*googlesqlite.TableSpec{cc.Table.Added, cc.Table.Updated, cc.Table.Deleted} {
		for _, spec := range specs {
			if len(spec.NamePath) == 3 {
				return &bigqueryv2.TableReference{
					ProjectId: spec.NamePath[0],
					DatasetId: spec.NamePath[1],
					TableId:   spec.NamePath[2],
				}
			}
		}
	}
	return nil
}

// scanLeadingTokens returns up to max uppercased keyword/identifier tokens
// from the start of a statement. Comments are skipped; quoted strings,
// backtick identifiers and balanced parenthesized groups (column lists,
// OPTIONS(...)) are skipped without contributing tokens, so the result is
// the statement's keyword skeleton.
func scanLeadingTokens(query string, max int) []string {
	var tokens []string
	s := query
	i, n := 0, len(s)
	for i < n && len(tokens) < max {
		c := s[i]
		switch {
		case c == ' ' || c == '\t' || c == '\n' || c == '\r' || c == ',' || c == ';' || c == '.':
			i++
		case c == '-' && i+1 < n && s[i+1] == '-':
			for i < n && s[i] != '\n' {
				i++
			}
		case c == '#':
			for i < n && s[i] != '\n' {
				i++
			}
		case c == '/' && i+1 < n && s[i+1] == '*':
			end := strings.Index(s[i+2:], "*/")
			if end < 0 {
				return tokens
			}
			i += 2 + end + 2
		case c == '`':
			end := strings.IndexByte(s[i+1:], '`')
			if end < 0 {
				return tokens
			}
			i += 1 + end + 1
		case c == '\'' || c == '"':
			i = skipQuoted(s, i)
		case c == '(':
			// A statement that *starts* with '(' is a parenthesized
			// query; otherwise the group (column list, OPTIONS, ...) is
			// skipped wholesale.
			if len(tokens) == 0 {
				return []string{"("}
			}
			depth := 0
			for i < n {
				switch s[i] {
				case '(':
					depth++
					i++
				case ')':
					depth--
					i++
				case '\'', '"':
					i = skipQuoted(s, i)
				case '`':
					end := strings.IndexByte(s[i+1:], '`')
					if end < 0 {
						return tokens
					}
					i += 1 + end + 1
				default:
					i++
				}
				if depth == 0 {
					break
				}
			}
		case isWordStart(c):
			j := i
			for j < n && isWordChar(s[j]) {
				j++
			}
			tokens = append(tokens, strings.ToUpper(s[i:j]))
			i = j
		default:
			i++
		}
	}
	return tokens
}

// skipQuoted advances past the quoted string starting at s[i] (a ' or "
// quote), honoring backslash escapes. It returns the index just after the
// closing quote, or len(s) when unterminated.
func skipQuoted(s string, i int) int {
	q := s[i]
	j := i + 1
	for j < len(s) {
		switch s[j] {
		case '\\':
			j += 2
		case q:
			return j + 1
		default:
			j++
		}
	}
	return j
}

func isWordStart(c byte) bool {
	return c == '_' || ('a' <= c && c <= 'z') || ('A' <= c && c <= 'Z')
}

func isWordChar(c byte) bool {
	return isWordStart(c) || ('0' <= c && c <= '9')
}
