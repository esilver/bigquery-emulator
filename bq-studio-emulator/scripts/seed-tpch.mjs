#!/usr/bin/env node
import fs from "node:fs";
import fsp from "node:fs/promises";
import path from "node:path";
import { randomUUID } from "node:crypto";
import { spawnSync, execFileSync } from "node:child_process";

const workDir = process.env.TPCH_WORKDIR || "/private/tmp/bq-studio-tpch";
const projectId = process.env.BQ_PROJECT_ID || "test";
const datasetId = process.env.BQ_TPCH_DATASET || "tpch";
const emulatorUrl = (process.env.BQ_DUCKDB_EMULATOR_URL || process.env.BQ_EMULATOR_URL || "http://127.0.0.1:9050").replace(/\/$/, "");

// Default 0.1 generates roughly 600k lineitem rows, which stays under the
// emulator multipart load ceiling. Bump TPCH_SCALE_FACTOR with care.
const scaleFactor = parsePositiveNumber(process.env.TPCH_SCALE_FACTOR, 0.1);
const prepareOnly = process.env.TPCH_PREPARE_ONLY === "1" || process.argv.includes("--prepare-only");
const forceGenerate = process.env.TPCH_FORCE_GENERATE === "1" || process.argv.includes("--force-generate");
const recreateTable = process.env.TPCH_RECREATE_TABLE !== "0";

// The 8 TPC-H tables produced by the duckdb tpch extension dbgen call.
const tpchTables = [
  "region",
  "nation",
  "supplier",
  "customer",
  "part",
  "partsupp",
  "orders",
  "lineitem"
];

function parsePositiveNumber(value, fallback) {
  const parsed = Number(value);
  return Number.isFinite(parsed) && parsed > 0 ? parsed : fallback;
}

function sqlString(value) {
  return `'${String(value).replaceAll("'", "''")}'`;
}

function quotedIdent(value) {
  return `"${String(value).replaceAll('"', '""')}"`;
}

function parquetPathFor(tableId) {
  return path.join(workDir, `${tableId}.parquet`);
}

function run(command, args, options = {}) {
  const result = spawnSync(command, args, {
    stdio: "inherit",
    ...options
  });
  if (result.error) throw result.error;
  if (result.status !== 0) {
    throw new Error(`${command} exited with status ${result.status}`);
  }
}

function duckdbJson(sql) {
  const stdout = execFileSync("duckdb", [":memory:", "-json", "-c", sql], {
    encoding: "utf8",
    maxBuffer: 256 * 1024 * 1024
  });
  const trimmed = stdout.trim();
  if (!trimmed) return [];
  return JSON.parse(trimmed);
}

async function exists(filePath) {
  try {
    await fsp.access(filePath);
    return true;
  } catch {
    return false;
  }
}

// Map a duckdb column type onto the closest BigQuery type. The duckdb tpch
// extension emits INTEGER/BIGINT identifiers, DECIMAL money columns, DOUBLE,
// VARCHAR text, and DATE columns, so we mirror the same casting discipline the
// nyc-taxi seeder uses for DECIMAL and temporal values.
function duckdbTypeToBigQuery(duckType) {
  const upper = String(duckType).toUpperCase();
  if (upper.startsWith("DECIMAL") || upper.startsWith("NUMERIC")) return "NUMERIC";
  if (upper.startsWith("VARCHAR") || upper.startsWith("CHAR") || upper === "TEXT" || upper === "STRING") return "STRING";
  if (upper === "DATE") return "DATE";
  if (upper === "TIMESTAMP" || upper.startsWith("TIMESTAMP")) return "STRING";
  if (upper === "DOUBLE" || upper === "FLOAT" || upper === "REAL") return "FLOAT";
  if (upper === "BOOLEAN" || upper === "BOOL") return "BOOLEAN";
  // BIGINT, INTEGER, HUGEINT, SMALLINT, TINYINT and their unsigned variants.
  if (upper.includes("INT")) return "INTEGER";
  return "STRING";
}

function generateTables() {
  const allPresent = tpchTables.every(tableId => fs.existsSync(parquetPathFor(tableId)));
  if (!forceGenerate && allPresent) {
    console.log(`Using existing TPC-H parquet files in ${workDir}`);
    return;
  }

  // Generate the full TPC-H dataset in memory once, then export every table to
  // its own parquet file so the REST loader can stream each independently.
  const exportStatements = tpchTables
    .map(tableId => `COPY ${quotedIdent(tableId)} TO ${sqlString(parquetPathFor(tableId))} (FORMAT PARQUET);`)
    .join("\n");

  const sql = `
INSTALL tpch;
LOAD tpch;
CALL dbgen(sf=${scaleFactor});
${exportStatements}
`;

  console.log(`Generating TPC-H data at scale factor ${scaleFactor} into ${workDir}`);
  run("duckdb", [":memory:", "-c", sql]);
  for (const tableId of tpchTables) {
    console.log(`Wrote ${parquetPathFor(tableId)}`);
  }
}

function schemaForTable(tableId) {
  const parquetPath = parquetPathFor(tableId);
  const rows = duckdbJson(`DESCRIBE SELECT * FROM read_parquet(${sqlString(parquetPath)});`);
  return rows.map(row => ({
    name: row.column_name,
    type: duckdbTypeToBigQuery(row.column_type),
    mode: "NULLABLE"
  }));
}

async function emulatorFetch(route, options = {}) {
  const response = await fetch(`${emulatorUrl}${route}`, options);
  const text = await response.text();
  let body = null;
  if (text) {
    try {
      body = JSON.parse(text);
    } catch {
      body = { raw: text };
    }
  }
  if (!response.ok) {
    const message = body?.error?.message || body?.error || body?.raw || response.statusText;
    const err = new Error(String(message));
    err.statusCode = response.status;
    err.details = body;
    throw err;
  }
  return body || {};
}

async function ensureDataset() {
  try {
    await emulatorFetch(`/bigquery/v2/projects/${encodeURIComponent(projectId)}/datasets/${encodeURIComponent(datasetId)}`);
    console.log(`Dataset exists: ${projectId}.${datasetId}`);
    return;
  } catch (error) {
    if (error.statusCode !== 404) throw error;
  }

  console.log(`Creating dataset ${projectId}.${datasetId}`);
  await emulatorFetch(`/bigquery/v2/projects/${encodeURIComponent(projectId)}/datasets`, {
    method: "POST",
    headers: { "content-type": "application/json; charset=utf-8" },
    body: JSON.stringify({
      datasetReference: { projectId, datasetId },
      location: "US"
    })
  });
}

async function dropExistingTable(tableId) {
  if (!recreateTable) return;
  try {
    await emulatorFetch(`/bigquery/v2/projects/${encodeURIComponent(projectId)}/datasets/${encodeURIComponent(datasetId)}/tables/${encodeURIComponent(tableId)}`, {
      method: "DELETE"
    });
    console.log(`Dropped existing table ${projectId}.${datasetId}.${tableId}`);
  } catch (error) {
    if (error.statusCode !== 404) throw error;
  }
}

async function loadTable(tableId) {
  const schemaFields = schemaForTable(tableId);
  const parquetPath = parquetPathFor(tableId);
  const metadata = {
    configuration: {
      load: {
        destinationTable: { projectId, datasetId, tableId },
        schema: { fields: schemaFields },
        sourceFormat: "PARQUET",
        writeDisposition: "WRITE_TRUNCATE"
      }
    }
  };
  const parquet = await fsp.readFile(parquetPath);
  const boundary = `codex-tpch-${randomUUID()}`;
  const body = Buffer.concat([
    Buffer.from(`--${boundary}\r\ncontent-type: application/json; charset=UTF-8\r\n\r\n${JSON.stringify(metadata)}\r\n`, "utf8"),
    Buffer.from(`--${boundary}\r\ncontent-type: application/octet-stream\r\n\r\n`, "utf8"),
    parquet,
    Buffer.from(`\r\n--${boundary}--\r\n`, "utf8")
  ]);

  console.log(`Loading ${parquetPath} into ${projectId}.${datasetId}.${tableId}`);
  const startedAt = performance.now();
  const result = await emulatorFetch(`/upload/bigquery/v2/projects/${encodeURIComponent(projectId)}/jobs?uploadType=multipart`, {
    method: "POST",
    headers: { "content-type": `multipart/related; boundary=${boundary}` },
    body
  });
  const durationMs = Math.round(performance.now() - startedAt);
  console.log(`Load job ${result.jobReference?.jobId || "(unknown)"} for ${tableId} finished in ${durationMs.toLocaleString()} ms`);
}

async function sanityQuery() {
  const query = `
SELECT
  COUNT(*) AS lineitem_rows,
  ROUND(SUM(l_extendedprice), 2) AS total_extendedprice,
  MIN(l_shipdate) AS first_shipdate,
  MAX(l_shipdate) AS last_shipdate
FROM \`${projectId}.${datasetId}.lineitem\`
`;
  const result = await emulatorFetch(`/bigquery/v2/projects/${encodeURIComponent(projectId)}/queries`, {
    method: "POST",
    headers: { "content-type": "application/json; charset=utf-8" },
    body: JSON.stringify({ query, useLegacySql: false, maxResults: 10 })
  });
  const values = result.rows?.[0]?.f?.map(cell => cell.v) || [];
  console.log(`Sanity: lineitem_rows=${values[0] || "?"}, total_extendedprice=${values[1] || "?"}, first_shipdate=${values[2] || "?"}, last_shipdate=${values[3] || "?"}`);
}

async function main() {
  console.log(`Target emulator: ${emulatorUrl}`);
  console.log(`Target dataset: ${projectId}.${datasetId} (TPC-H sf=${scaleFactor})`);
  await fsp.mkdir(workDir, { recursive: true });
  generateTables();

  if (prepareOnly) {
    console.log("Prepared local TPC-H parquet files only (--prepare-only).");
    return;
  }

  await ensureDataset();
  for (const tableId of tpchTables) {
    await dropExistingTable(tableId);
    await loadTable(tableId);
  }
  await sanityQuery();
}

main().catch(error => {
  console.error(error.message);
  if (error.details) console.error(JSON.stringify(error.details, null, 2));
  process.exit(1);
});
