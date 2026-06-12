#!/usr/bin/env node

import { spawnSync } from "node:child_process";

const dbPath = process.env.SQLITE_DB || process.argv[2] || "/private/tmp/bqe-sqlite-data-fast/finance-emulator-sqlite.db";
const projectId = process.env.BQ_PROJECT_ID || "finance-emulator";
const datasetId = process.env.BQ_DATASET_ID || "dbt_test__audit";

const tablePrefix = `${projectId}_${datasetId}`;

function envelope(header, body) {
  return Buffer.from(JSON.stringify({ header, body })).toString("base64");
}

function encString(value) {
  return envelope("string", value);
}

function encDate(value) {
  return envelope("date", value);
}

function encArray(values) {
  return envelope("array", JSON.stringify(values));
}

function sqlString(value) {
  return `'${String(value).replaceAll("'", "''")}'`;
}

function quotedIdent(value) {
  return `"${String(value).replaceAll('"', '""')}"`;
}

function caseExpr(expr, values) {
  return `CASE ${expr}\n${values.map((value, index) => `  WHEN ${index} THEN ${sqlString(value)}`).join("\n")}\nEND`;
}

function tableMetadata(tableId, fields, rowCount) {
  const tableRef = { datasetId, projectId, tableId };
  return {
    creationTime: "1781230223",
    id: `${projectId}:${datasetId}.${tableId}`,
    kind: "bigquery#table",
    lastModifiedTime: "1781230223",
    numRows: String(rowCount),
    schema: { fields },
    selfLink: `http://0.0.0.0:9050/bigquery/v2/projects/${projectId}/datasets/${datasetId}/tables/${tableId}`,
    tableReference: tableRef,
    type: "TABLE",
  };
}

function field(name, type) {
  return { mode: "NULLABLE", name, type };
}

function sqlType(name, kind) {
  return {
    name,
    kind,
    signatureKind: 1,
    elementType: null,
    fieldTypes: null,
  };
}

function catalogColumn(name, type) {
  return {
    name,
    type,
    isNotNull: false,
  };
}

function catalogTableSpec(tableId, columns) {
  const now = new Date().toISOString();
  return {
    isTemp: false,
    isView: false,
    namePath: [projectId, datasetId, tableId],
    columns,
    primaryKey: null,
    createMode: 2,
    query: "",
    updatedAt: now,
    createdAt: now,
  };
}

const publisherFields = [
  field("publisher_id", "INTEGER"),
  field("publisher_name", "STRING"),
  field("tier", "STRING"),
  field("region", "STRING"),
];

const eventFields = [
  field("id", "INTEGER"),
  field("event_date", "DATE"),
  field("publisher_id", "INTEGER"),
  field("campaign_id", "INTEGER"),
  field("amount", "FLOAT"),
  field("category", "STRING"),
];

const int64Type = sqlType("INT64", 3);
const doubleType = sqlType("DOUBLE", 8);
const stringType = sqlType("STRING", 9);
const dateType = sqlType("DATE", 11);

const publisherCatalogColumns = [
  catalogColumn("publisher_id", int64Type),
  catalogColumn("publisher_name", stringType),
  catalogColumn("tier", stringType),
  catalogColumn("region", stringType),
];

const eventCatalogColumns = [
  catalogColumn("id", int64Type),
  catalogColumn("event_date", dateType),
  catalogColumn("publisher_id", int64Type),
  catalogColumn("campaign_id", int64Type),
  catalogColumn("amount", doubleType),
  catalogColumn("category", stringType),
];

const tableIds = ["publishers_1k", "events_100k", "events_1m"];
const encodedTableIds = tableIds.map(encString);
const encodedDatasetIds = [encString(datasetId)];

const dates = Array.from({ length: 30 }, (_, index) => {
  const d = new Date(Date.UTC(2026, 5, 1 + index));
  return encDate(d.toISOString().slice(0, 10));
});

const categories = Array.from({ length: 12 }, (_, index) => encString(`cat_${String(index).padStart(2, "0")}`));

const publisherRows = Array.from({ length: 1000 }, (_, index) => {
  const publisherId = index + 1;
  const tier = `tier_${(publisherId % 4) + 1}`;
  const region = publisherId % 4 === 0 ? "east" : publisherId % 4 === 2 ? "central" : "west";
  return `(${publisherId}, ${sqlString(encString(`publisher_${String(publisherId).padStart(4, "0")}`))}, ${sqlString(encString(tier))}, ${sqlString(encString(region))})`;
});

const tablesMetadataRows = [
  ["publishers_1k", publisherFields, 1000],
  ["events_100k", eventFields, 100000],
  ["events_1m", eventFields, 1000000],
].map(([tableId, fields, rowCount]) => {
  return [
    sqlString(encString(tableId)),
    sqlString(encString(projectId)),
    sqlString(encString(datasetId)),
    sqlString(encString(JSON.stringify(tableMetadata(tableId, fields, rowCount)))),
  ].join(", ");
});

const catalogRows = [
  ["publishers_1k", publisherCatalogColumns],
  ["events_100k", eventCatalogColumns],
  ["events_1m", eventCatalogColumns],
].map(([tableId, columns]) => {
  const catalogName = `${tablePrefix}_${tableId}`;
  return [
    sqlString(catalogName),
    sqlString("table"),
    sqlString(JSON.stringify(catalogTableSpec(tableId, columns))),
    sqlString(new Date().toISOString()),
    sqlString(new Date().toISOString()),
  ].join(", ");
});

const dateCase = caseExpr("(id % 30)", dates);
const categoryCase = caseExpr("(id % 12)", categories);

const sql = `
PRAGMA journal_mode = OFF;
PRAGMA synchronous = OFF;
PRAGMA temp_store = MEMORY;
PRAGMA locking_mode = EXCLUSIVE;

CREATE TABLE IF NOT EXISTS projects (
  id TEXT NOT NULL,
  datasetIDs TEXT,
  jobIDs TEXT,
  PRIMARY KEY (id)
);

CREATE TABLE IF NOT EXISTS datasets (
  id TEXT NOT NULL,
  projectID TEXT NOT NULL,
  tableIDs TEXT,
  modelIDs TEXT,
  routineIDs TEXT,
  metadata TEXT,
  PRIMARY KEY (projectID, id)
);

CREATE TABLE IF NOT EXISTS tables (
  id TEXT NOT NULL,
  projectID TEXT NOT NULL,
  datasetID TEXT NOT NULL,
  metadata TEXT,
  PRIMARY KEY (projectID, datasetID, id)
);

CREATE TABLE IF NOT EXISTS googlesqlite_catalog (
  name STRING NOT NULL PRIMARY KEY,
  kind STRING NOT NULL,
  spec STRING NOT NULL,
  updatedAt TIMESTAMP NOT NULL,
  createdAt TIMESTAMP NOT NULL
);

BEGIN;

INSERT OR REPLACE INTO projects (id, datasetIDs, jobIDs)
VALUES (
  ${sqlString(encString(projectId))},
  ${sqlString(encArray(encodedDatasetIds))},
  ${sqlString(encArray([]))}
);

INSERT OR REPLACE INTO datasets (id, projectID, tableIDs, modelIDs, routineIDs, metadata)
VALUES (
  ${sqlString(encString(datasetId))},
  ${sqlString(encString(projectId))},
  ${sqlString(encArray(encodedTableIds))},
  ${sqlString(encArray([]))},
  ${sqlString(encArray([]))},
  ${sqlString(encString(JSON.stringify({
    datasetReference: { datasetId, projectId },
    location: "US",
  })))}
);

INSERT OR REPLACE INTO tables (id, projectID, datasetID, metadata)
VALUES
  (${tablesMetadataRows.join("),\n  (")});

INSERT INTO googlesqlite_catalog (name, kind, spec, updatedAt, createdAt)
VALUES
  (${catalogRows.join("),\n  (")})
ON CONFLICT(name) DO UPDATE SET
  kind = excluded.kind,
  spec = excluded.spec,
  updatedAt = excluded.updatedAt;

DROP TABLE IF EXISTS ${quotedIdent(`${tablePrefix}_publishers_1k`)};
CREATE TABLE ${quotedIdent(`${tablePrefix}_publishers_1k`)} (
  publisher_id,
  publisher_name,
  tier,
  region
);

INSERT INTO ${quotedIdent(`${tablePrefix}_publishers_1k`)} (publisher_id, publisher_name, tier, region)
VALUES
  ${publisherRows.join(",\n  ")};

DROP TABLE IF EXISTS ${quotedIdent(`${tablePrefix}_events_100k`)};
CREATE TABLE ${quotedIdent(`${tablePrefix}_events_100k`)} (
  id,
  event_date,
  publisher_id,
  campaign_id,
  amount,
  category
);

WITH RECURSIVE seq(id) AS (
  VALUES(1)
  UNION ALL
  SELECT id + 1 FROM seq WHERE id < 100000
)
INSERT INTO ${quotedIdent(`${tablePrefix}_events_100k`)} (id, event_date, publisher_id, campaign_id, amount, category)
SELECT
  id,
  ${dateCase},
  (id % 1000) + 1,
  (id % 200) + 1,
  ((id % 10000) + 1) / 100.0,
  ${categoryCase}
FROM seq;

DROP TABLE IF EXISTS ${quotedIdent(`${tablePrefix}_events_1m`)};
CREATE TABLE ${quotedIdent(`${tablePrefix}_events_1m`)} (
  id,
  event_date,
  publisher_id,
  campaign_id,
  amount,
  category
);

WITH RECURSIVE seq(id) AS (
  VALUES(1)
  UNION ALL
  SELECT id + 1 FROM seq WHERE id < 1000000
)
INSERT INTO ${quotedIdent(`${tablePrefix}_events_1m`)} (id, event_date, publisher_id, campaign_id, amount, category)
SELECT
  id,
  ${dateCase},
  (id % 1000) + 1,
  (id % 200) + 1,
  ((id % 10000) + 1) / 100.0,
  ${categoryCase}
FROM seq;

COMMIT;

ANALYZE;
`;

const started = process.hrtime.bigint();
const result = spawnSync("sqlite3", [dbPath], {
  input: sql,
  encoding: "utf8",
  maxBuffer: 1024 * 1024 * 16,
});
const elapsedMs = Number(process.hrtime.bigint() - started) / 1_000_000;

if (result.error) {
  console.error(result.error.message);
  process.exit(1);
}
if (result.status !== 0) {
  process.stderr.write(result.stderr);
  process.exit(result.status ?? 1);
}

process.stdout.write(`Seeded ${dbPath} with ${datasetId}.{publishers_1k,events_100k,events_1m} in ${elapsedMs.toFixed(0)} ms\n`);
