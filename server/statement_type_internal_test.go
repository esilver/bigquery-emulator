package server

import (
	"strings"
	"testing"
)

// statementTypeForQuery is exercised end-to-end in dbt_issues_test.go; this
// covers the text-only fallback paths (no ChangedCatalog), in particular the
// dbt-style /* ... */ comment header and the parenthesized-group skipping.
func TestStatementTypeForQueryTextFallback(t *testing.T) {
	cases := []struct {
		query string
		want  string
	}{
		{`/* {"app": "dbt", "dbt_version": "1.7.0"} */ select 1 as x`, "SELECT"},
		{"-- comment\nWITH t AS (SELECT 1) SELECT * FROM t", "SELECT"},
		{"(SELECT 1)", "SELECT"},
		{"create schema if not exists `proj.ds`", "CREATE_SCHEMA"},
		{"create or replace view `p.d.v` as select 1", "CREATE_VIEW"},
		{"CREATE MATERIALIZED VIEW d.mv AS SELECT 1 AS x", "CREATE_MATERIALIZED_VIEW"},
		{"create table d.t (x int64, s struct<a string>) options(description='as if')", "CREATE_TABLE"},
		{"create or replace table `d`.`t` options(description=\"x\") as (select 1 as x)", "CREATE_TABLE_AS_SELECT"},
		{"create temp table t as select 1", "CREATE_TABLE_AS_SELECT"},
		{"create table if not exists d.t (x int64)", "CREATE_TABLE"},
		{"CREATE TABLE FUNCTION d.tf() AS (SELECT 1 AS x)", "CREATE_TABLE_FUNCTION"},
		{"create or replace function d.f(x int64) as (x + 1)", "CREATE_FUNCTION"},
		{"drop table if exists d.t", "DROP_TABLE"},
		{"drop view d.v", "DROP_VIEW"},
		{"drop schema d", "DROP_SCHEMA"},
		{"alter table d.t add column y string", "ALTER_TABLE"},
		{"alter view d.v set options(description='x')", "ALTER_VIEW"},
		{"insert into d.t (x) values (1)", "INSERT"},
		{"update d.t set x = 1 where true", "UPDATE"},
		{"delete from d.t where x = 1", "DELETE"},
		{"merge d.t using d.s on false when not matched then insert row", "MERGE"},
		{"truncate table d.t", "TRUNCATE_TABLE"},
		{"", "SELECT"},
	}
	for _, tc := range cases {
		if got := statementTypeForQuery(tc.query, nil); got != tc.want {
			t.Errorf("statementTypeForQuery(%q) = %q, want %q", tc.query, got, tc.want)
		}
	}
}

func TestSchemaDDLTargetPath(t *testing.T) {
	cases := []struct {
		query string
		want  string
	}{
		{"CREATE SCHEMA d2", "d2"},
		{"create schema if not exists D2", "D2"},
		{"CREATE SCHEMA proj.d2", "proj.d2"},
		{"CREATE SCHEMA `proj.d2`", "proj.d2"},
		{"CREATE SCHEMA `proj`.`d2`", "proj.d2"},
		{"CREATE SCHEMA proj.`d2`", "proj.d2"},
		{"-- comment\nCREATE SCHEMA /* x */ d2 OPTIONS(description='y')", "d2"},
		{"DROP SCHEMA d2", "d2"},
		{"DROP SCHEMA IF EXISTS d2 CASCADE", "d2"},
		{"ALTER SCHEMA d2 SET OPTIONS(description='y')", "d2"},
		{"CREATE TABLE d.t (x INT64)", ""},
		{"SELECT 1", ""},
		{"DROP TABLE d.t", ""},
	}
	for _, tc := range cases {
		got := strings.Join(schemaDDLTargetPath(tc.query), ".")
		if got != tc.want {
			t.Errorf("schemaDDLTargetPath(%q) = %q, want %q", tc.query, got, tc.want)
		}
	}
}
