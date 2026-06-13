#!/usr/bin/env node
import fs from "node:fs";
import fsp from "node:fs/promises";
import path from "node:path";
import { randomUUID } from "node:crypto";
import { spawnSync } from "node:child_process";
import { Readable } from "node:stream";
import { pipeline } from "node:stream/promises";

const DEFAULT_SOURCE_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-01.parquet";
const workDir = process.env.NYC_TAXI_WORKDIR || "/private/tmp/bq-studio-nyc-taxi";
const projectId = process.env.BQ_PROJECT_ID || "finance-emulator";
const datasetId = process.env.BQ_NYC_TAXI_DATASET || "nyc_taxi";
const tableId = process.env.BQ_NYC_TAXI_TABLE || "yellow_tripdata_2024_01";
const emulatorUrl = (process.env.BQ_DUCKDB_EMULATOR_URL || process.env.BQ_EMULATOR_URL || "http://127.0.0.1:9050").replace(/\/$/, "");
const sourceUrl = process.env.NYC_TAXI_PARQUET_URL || DEFAULT_SOURCE_URL;
const rowLimit = parsePositiveInteger(process.env.NYC_TAXI_ROW_LIMIT, 100000);
const prepareOnly = process.env.NYC_TAXI_PREPARE_ONLY === "1" || process.argv.includes("--prepare-only");
const forceDownload = process.env.NYC_TAXI_FORCE_DOWNLOAD === "1" || process.argv.includes("--force-download");
const forceCsv = process.env.NYC_TAXI_FORCE_CSV === "1" || process.argv.includes("--force-csv");
const recreateTable = process.env.NYC_TAXI_RECREATE_TABLE !== "0";

const parquetPath = path.join(workDir, path.basename(new URL(sourceUrl).pathname));
const csvPath = path.join(workDir, `${tableId}_${rowLimit}.csv`);

const schemaFields = [
  ["VendorID", "INTEGER"],
  ["tpep_pickup_datetime", "STRING"],
  ["tpep_dropoff_datetime", "STRING"],
  ["passenger_count", "INTEGER"],
  ["trip_distance", "FLOAT"],
  ["RatecodeID", "INTEGER"],
  ["store_and_fwd_flag", "STRING"],
  ["PULocationID", "INTEGER"],
  ["DOLocationID", "INTEGER"],
  ["payment_type", "INTEGER"],
  ["fare_amount", "FLOAT"],
  ["extra", "FLOAT"],
  ["mta_tax", "FLOAT"],
  ["tip_amount", "FLOAT"],
  ["tolls_amount", "FLOAT"],
  ["improvement_surcharge", "FLOAT"],
  ["total_amount", "FLOAT"],
  ["congestion_surcharge", "FLOAT"],
  ["Airport_fee", "FLOAT"]
].map(([name, type]) => ({ name, type, mode: "NULLABLE" }));

function parsePositiveInteger(value, fallback) {
  const parsed = Number(value);
  return Number.isInteger(parsed) && parsed > 0 ? parsed : fallback;
}

function sqlString(value) {
  return `'${String(value).replaceAll("'", "''")}'`;
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

async function exists(filePath) {
  try {
    await fsp.access(filePath);
    return true;
  } catch {
    return false;
  }
}

async function downloadParquet() {
  await fsp.mkdir(workDir, { recursive: true });
  if (!forceDownload && await exists(parquetPath)) {
    console.log(`Using existing ${parquetPath}`);
    return;
  }

  console.log(`Downloading ${sourceUrl}`);
  const response = await fetch(sourceUrl);
  if (!response.ok || !response.body) {
    throw new Error(`failed to download ${sourceUrl}: ${response.status} ${response.statusText}`);
  }
  await pipeline(Readable.fromWeb(response.body), fs.createWriteStream(parquetPath));
  console.log(`Wrote ${parquetPath}`);
}

function writeCsvSample() {
  if (!forceCsv && fs.existsSync(csvPath)) {
    console.log(`Using existing ${csvPath}`);
    return;
  }

  const sql = `
COPY (
  SELECT
    CAST(VendorID AS BIGINT) AS VendorID,
    strftime(CAST(tpep_pickup_datetime AS TIMESTAMP), '%Y-%m-%d %H:%M:%S') AS tpep_pickup_datetime,
    strftime(CAST(tpep_dropoff_datetime AS TIMESTAMP), '%Y-%m-%d %H:%M:%S') AS tpep_dropoff_datetime,
    CAST(passenger_count AS BIGINT) AS passenger_count,
    CAST(trip_distance AS DOUBLE) AS trip_distance,
    CAST(RatecodeID AS BIGINT) AS RatecodeID,
    store_and_fwd_flag,
    CAST(PULocationID AS BIGINT) AS PULocationID,
    CAST(DOLocationID AS BIGINT) AS DOLocationID,
    CAST(payment_type AS BIGINT) AS payment_type,
    CAST(fare_amount AS DOUBLE) AS fare_amount,
    CAST(extra AS DOUBLE) AS extra,
    CAST(mta_tax AS DOUBLE) AS mta_tax,
    CAST(tip_amount AS DOUBLE) AS tip_amount,
    CAST(tolls_amount AS DOUBLE) AS tolls_amount,
    CAST(improvement_surcharge AS DOUBLE) AS improvement_surcharge,
    CAST(total_amount AS DOUBLE) AS total_amount,
    CAST(congestion_surcharge AS DOUBLE) AS congestion_surcharge,
    CAST(Airport_fee AS DOUBLE) AS Airport_fee
  FROM read_parquet(${sqlString(parquetPath)})
  LIMIT ${rowLimit}
) TO ${sqlString(csvPath)} (HEADER, DELIMITER ',', NULL '');
`;

  console.log(`Creating ${rowLimit.toLocaleString()} row CSV sample at ${csvPath}`);
  run("duckdb", [":memory:", "-c", sql]);
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

async function dropExistingTable() {
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

async function loadCsv() {
  const metadata = {
    configuration: {
      load: {
        destinationTable: { projectId, datasetId, tableId },
        schema: { fields: schemaFields },
        sourceFormat: "CSV",
        skipLeadingRows: 1,
        writeDisposition: "WRITE_TRUNCATE"
      }
    }
  };
  const csv = await fsp.readFile(csvPath);
  const boundary = `codex-nyc-taxi-${randomUUID()}`;
  const body = Buffer.concat([
    Buffer.from(`--${boundary}\r\ncontent-type: application/json; charset=UTF-8\r\n\r\n${JSON.stringify(metadata)}\r\n`, "utf8"),
    Buffer.from(`--${boundary}\r\ncontent-type: text/csv\r\n\r\n`, "utf8"),
    csv,
    Buffer.from(`\r\n--${boundary}--\r\n`, "utf8")
  ]);

  console.log(`Loading ${csvPath} into ${projectId}.${datasetId}.${tableId}`);
  const startedAt = performance.now();
  const result = await emulatorFetch(`/upload/bigquery/v2/projects/${encodeURIComponent(projectId)}/jobs?uploadType=multipart`, {
    method: "POST",
    headers: { "content-type": `multipart/related; boundary=${boundary}` },
    body
  });
  const durationMs = Math.round(performance.now() - startedAt);
  console.log(`Load job ${result.jobReference?.jobId || "(unknown)"} finished in ${durationMs.toLocaleString()} ms`);
}

async function sanityQuery() {
  const query = `
SELECT
  COUNT(*) AS row_count,
  MIN(tpep_pickup_datetime) AS first_pickup,
  MAX(tpep_pickup_datetime) AS last_pickup,
  ROUND(SUM(total_amount), 2) AS total_amount
FROM \`${projectId}.${datasetId}.${tableId}\`
`;
  const result = await emulatorFetch(`/bigquery/v2/projects/${encodeURIComponent(projectId)}/queries`, {
    method: "POST",
    headers: { "content-type": "application/json; charset=utf-8" },
    body: JSON.stringify({ query, useLegacySql: false, maxResults: 10 })
  });
  const values = result.rows?.[0]?.f?.map(cell => cell.v) || [];
  console.log(`Sanity: rows=${values[0] || "?"}, first=${values[1] || "?"}, last=${values[2] || "?"}, total_amount=${values[3] || "?"}`);
}

async function main() {
  console.log(`Target emulator: ${emulatorUrl}`);
  console.log(`Target table: ${projectId}.${datasetId}.${tableId}`);
  await downloadParquet();
  writeCsvSample();

  if (prepareOnly) {
    console.log("Prepared local NYC Taxi files only (--prepare-only).");
    return;
  }

  await ensureDataset();
  await dropExistingTable();
  await loadCsv();
  await sanityQuery();
}

main().catch(error => {
  console.error(error.message);
  if (error.details) console.error(JSON.stringify(error.details, null, 2));
  process.exit(1);
});
