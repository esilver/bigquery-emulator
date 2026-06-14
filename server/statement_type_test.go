package server

import "testing"

func TestClassifyStatementType(t *testing.T) {
	cases := []struct {
		query string
		want  string
	}{
		{"SELECT 1", "SELECT"},
		{"  select * from t", "SELECT"},
		{"WITH x AS (SELECT 1) SELECT * FROM x", "SELECT"},
		{"INSERT INTO `p.d.t` VALUES (1)", "INSERT"},
		{"UPDATE `p.d.t` SET a = 1 WHERE b = 2", "UPDATE"},
		{"DELETE FROM `p.d.t` WHERE a = 1", "DELETE"},
		{"MERGE INTO t USING s ON t.id = s.id", "MERGE"},
		{"TRUNCATE TABLE `p.d.t`", "TRUNCATE_TABLE"},
		{"CREATE SCHEMA `p.d`", "CREATE_SCHEMA"},
		{"CREATE SCHEMA IF NOT EXISTS `p.d`", "CREATE_SCHEMA"},
		{"create database foo", "CREATE_SCHEMA"},
		{"DROP SCHEMA `p.d`", "DROP_SCHEMA"},
		{"DROP SCHEMA IF EXISTS `p.d` CASCADE", "DROP_SCHEMA"},
		{"CREATE TABLE `p.d.t` (a INT64)", "CREATE_TABLE"},
		{"CREATE OR REPLACE TABLE `p.d.t` (a INT64)", "CREATE_TABLE"},
		{"CREATE TABLE `p.d.t` AS SELECT 1 AS a", "CREATE_TABLE_AS_SELECT"},
		{"CREATE OR REPLACE TABLE `p.d.t` AS SELECT 1 AS a", "CREATE_TABLE_AS_SELECT"},
		{"CREATE VIEW `p.d.v` AS SELECT 1", "CREATE_VIEW"},
		{"CREATE OR REPLACE VIEW `p.d.v` AS SELECT 1", "CREATE_VIEW"},
		{"CREATE MATERIALIZED VIEW `p.d.mv` AS SELECT 1", "CREATE_MATERIALIZED_VIEW"},
		{"DROP VIEW `p.d.v`", "DROP_VIEW"},
		{"DROP TABLE `p.d.t`", "DROP_TABLE"},
		{"ALTER TABLE `p.d.t` ADD COLUMN c INT64", "ALTER_TABLE"},
		{"CREATE FUNCTION f() AS (1)", "CREATE_FUNCTION"},
		// dbt prepends a job comment to every statement.
		{"/* {\"app\": \"dbt\"} */\nCREATE OR REPLACE VIEW `p.d.v` AS SELECT 1", "CREATE_VIEW"},
		{"-- a comment\nCREATE TABLE `p.d.t` AS SELECT 1", "CREATE_TABLE_AS_SELECT"},
		{"", "SELECT"},
	}
	for _, c := range cases {
		// CTAS upgrade is applied by the caller via isCreateTableAsSelect; fold
		// it in here so the test reflects the externally observed type.
		got := classifyStatementType(c.query)
		if got == "CREATE_TABLE" && isCreateTableAsSelect(c.query) {
			got = "CREATE_TABLE_AS_SELECT"
		}
		if got != c.want {
			t.Errorf("classifyStatementType(%q) = %q, want %q", c.query, got, c.want)
		}
	}
}

func TestParseSchemaDDL(t *testing.T) {
	cases := []struct {
		query       string
		wantOK      bool
		isCreate    bool
		project     string
		dataset     string
		ifNotExists bool
		ifExists    bool
	}{
		// dbt renders project.dataset with each segment backtick-quoted.
		{"create schema if not exists `test`.`jaffle_shop`", true, true, "test", "jaffle_shop", true, false},
		{"CREATE SCHEMA `test`.`ds`", true, true, "test", "ds", false, false},
		{"CREATE SCHEMA `ds`", true, true, "", "ds", false, false},
		{"create schema ds", true, true, "", "ds", false, false},
		// A single backtick-quoted name containing a dot.
		{"CREATE SCHEMA `test.jaffle_shop`", true, true, "test", "jaffle_shop", false, false},
		{"drop schema if exists `test`.`ds` cascade", true, false, "test", "ds", false, true},
		{"DROP SCHEMA `test`.`ds`", true, false, "test", "ds", false, false},
		{"create database `p`.`d`", true, true, "p", "d", false, false},
		// Not schema DDL.
		{"CREATE TABLE `p`.`d`.`t` (a INT64)", false, false, "", "", false, false},
		{"SELECT 1", false, false, "", "", false, false},
	}
	for _, c := range cases {
		got, ok := parseSchemaDDL(c.query)
		if ok != c.wantOK {
			t.Errorf("parseSchemaDDL(%q) ok=%v, want %v", c.query, ok, c.wantOK)
			continue
		}
		if !ok {
			continue
		}
		if got.isCreate != c.isCreate || got.projectID != c.project || got.datasetID != c.dataset ||
			got.ifNotExists != c.ifNotExists || got.ifExists != c.ifExists {
			t.Errorf("parseSchemaDDL(%q) = %+v, want isCreate=%v project=%q dataset=%q ifNotExists=%v ifExists=%v",
				c.query, got, c.isCreate, c.project, c.dataset, c.ifNotExists, c.ifExists)
		}
	}
}

func TestIsCreateTableAsSelect(t *testing.T) {
	yes := []string{
		"CREATE TABLE `p.d.t` AS SELECT 1",
		"CREATE OR REPLACE TABLE t AS (SELECT 1)",
		"create table t as select 1",
	}
	no := []string{
		"CREATE TABLE `p.d.t` (a INT64, b STRING)",
		"CREATE OR REPLACE TABLE t (a INT64)",
	}
	for _, q := range yes {
		if !isCreateTableAsSelect(q) {
			t.Errorf("isCreateTableAsSelect(%q) = false, want true", q)
		}
	}
	for _, q := range no {
		if isCreateTableAsSelect(q) {
			t.Errorf("isCreateTableAsSelect(%q) = true, want false", q)
		}
	}
}
