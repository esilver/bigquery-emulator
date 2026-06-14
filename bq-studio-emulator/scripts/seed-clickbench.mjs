#!/usr/bin/env node
import fs from "node:fs";
import fsp from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import { randomUUID } from "node:crypto";
import { spawnSync } from "node:child_process";
import { Readable } from "node:stream";
import { pipeline } from "node:stream/promises";
import { requireCli, waitForEmulator } from "./seed-utils.mjs";

// Canonical single-file ClickBench hits parquet published by ClickHouse.
const DEFAULT_SOURCE_URL = "https://datasets.clickhouse.com/hits_compatible/hits.parquet";
const workDir = process.env.CLICKBENCH_WORKDIR || path.join(os.tmpdir(), "bq-studio-clickbench");
const projectId = process.env.BQ_PROJECT_ID || "test";
const datasetId = process.env.BQ_CLICKBENCH_DATASET || "clickbench";
const tableId = process.env.BQ_CLICKBENCH_TABLE || "hits";
const emulatorUrl = (process.env.BQ_DUCKDB_EMULATOR_URL || process.env.BQ_EMULATOR_URL || "http://127.0.0.1:9050").replace(/\/$/, "");
const sourceUrl = process.env.CLICKBENCH_PARQUET_URL || DEFAULT_SOURCE_URL;

// The full hits dataset is ~100M rows (tens of GB). A multipart REST load of
// the whole file will exceed the emulator load ceiling, so by default we slice
// a bounded subset with the local duckdb CLI. Set CLICKBENCH_ROW_LIMIT to 0 to
// attempt loading the full file, which in practice needs the streaming load
// path rather than this single multipart upload.
const rowLimit = parseNonNegativeInteger(process.env.CLICKBENCH_ROW_LIMIT, 100000);
const prepareOnly = process.env.CLICKBENCH_PREPARE_ONLY === "1" || process.argv.includes("--prepare-only");
const forceDownload = process.env.CLICKBENCH_FORCE_DOWNLOAD === "1" || process.argv.includes("--force-download");
const forceSubset = process.env.CLICKBENCH_FORCE_SUBSET === "1" || process.argv.includes("--force-subset");
const recreateTable = process.env.CLICKBENCH_RECREATE_TABLE !== "0";

const sourceParquetPath = path.join(workDir, path.basename(new URL(sourceUrl).pathname));
const subsetParquetPath = path.join(workDir, `${tableId}_${rowLimit}.parquet`);

// ClickBench hits schema. Wide integer columns map to INTEGER, the EventTime /
// EventDate temporal pair is carried as STRING to mirror the nyc-taxi seeder's
// temporal handling, and the remaining text columns map to STRING.
const schemaFields = [
  ["WatchID", "INTEGER"],
  ["JavaEnable", "INTEGER"],
  ["Title", "STRING"],
  ["GoodEvent", "INTEGER"],
  ["EventTime", "STRING"],
  ["EventDate", "STRING"],
  ["CounterID", "INTEGER"],
  ["ClientIP", "INTEGER"],
  ["RegionID", "INTEGER"],
  ["UserID", "INTEGER"],
  ["CounterClass", "INTEGER"],
  ["OS", "INTEGER"],
  ["UserAgent", "INTEGER"],
  ["URL", "STRING"],
  ["Referer", "STRING"],
  ["IsRefresh", "INTEGER"],
  ["RefererCategoryID", "INTEGER"],
  ["RefererRegionID", "INTEGER"],
  ["URLCategoryID", "INTEGER"],
  ["URLRegionID", "INTEGER"],
  ["ResolutionWidth", "INTEGER"],
  ["ResolutionHeight", "INTEGER"],
  ["ResolutionDepth", "INTEGER"],
  ["FlashMajor", "INTEGER"],
  ["FlashMinor", "INTEGER"],
  ["FlashMinor2", "STRING"],
  ["NetMajor", "INTEGER"],
  ["NetMinor", "INTEGER"],
  ["UserAgentMajor", "INTEGER"],
  ["UserAgentMinor", "STRING"],
  ["CookieEnable", "INTEGER"],
  ["JavascriptEnable", "INTEGER"],
  ["IsMobile", "INTEGER"],
  ["MobilePhone", "INTEGER"],
  ["MobilePhoneModel", "STRING"],
  ["Params", "STRING"],
  ["IPNetworkID", "INTEGER"],
  ["TraficSourceID", "INTEGER"],
  ["SearchEngineID", "INTEGER"],
  ["SearchPhrase", "STRING"],
  ["AdvEngineID", "INTEGER"],
  ["IsArtifical", "INTEGER"],
  ["WindowClientWidth", "INTEGER"],
  ["WindowClientHeight", "INTEGER"],
  ["ClientTimeZone", "INTEGER"],
  ["ClientEventTime", "STRING"],
  ["SilverlightVersion1", "INTEGER"],
  ["SilverlightVersion2", "INTEGER"],
  ["SilverlightVersion3", "INTEGER"],
  ["SilverlightVersion4", "INTEGER"],
  ["PageCharset", "STRING"],
  ["CodeVersion", "INTEGER"],
  ["IsLink", "INTEGER"],
  ["IsDownload", "INTEGER"],
  ["IsNotBounce", "INTEGER"],
  ["FUniqID", "INTEGER"],
  ["OriginalURL", "STRING"],
  ["HID", "INTEGER"],
  ["IsOldCounter", "INTEGER"],
  ["IsEvent", "INTEGER"],
  ["IsParameter", "INTEGER"],
  ["DontCountHits", "INTEGER"],
  ["WithHash", "INTEGER"],
  ["HitColor", "STRING"],
  ["LocalEventTime", "STRING"],
  ["Age", "INTEGER"],
  ["Sex", "INTEGER"],
  ["Income", "INTEGER"],
  ["Interests", "INTEGER"],
  ["Robotness", "INTEGER"],
  ["RemoteIP", "INTEGER"],
  ["WindowName", "INTEGER"],
  ["OpenerName", "INTEGER"],
  ["HistoryLength", "INTEGER"],
  ["BrowserLanguage", "STRING"],
  ["BrowserCountry", "STRING"],
  ["SocialNetwork", "STRING"],
  ["SocialAction", "STRING"],
  ["HTTPError", "INTEGER"],
  ["SendTiming", "INTEGER"],
  ["DNSTiming", "INTEGER"],
  ["ConnectTiming", "INTEGER"],
  ["ResponseStartTiming", "INTEGER"],
  ["ResponseEndTiming", "INTEGER"],
  ["FetchTiming", "INTEGER"],
  ["SocialSourceNetworkID", "INTEGER"],
  ["SocialSourcePage", "STRING"],
  ["ParamPrice", "INTEGER"],
  ["ParamOrderID", "STRING"],
  ["ParamCurrency", "STRING"],
  ["ParamCurrencyID", "INTEGER"],
  ["OpenstatServiceName", "STRING"],
  ["OpenstatCampaignID", "STRING"],
  ["OpenstatAdID", "STRING"],
  ["OpenstatSourceID", "STRING"],
  ["UTMSource", "STRING"],
  ["UTMMedium", "STRING"],
  ["UTMCampaign", "STRING"],
  ["UTMContent", "STRING"],
  ["UTMTerm", "STRING"],
  ["FromTag", "STRING"],
  ["HasGCLID", "INTEGER"],
  ["RefererHash", "INTEGER"],
  ["URLHash", "INTEGER"],
  ["CLID", "INTEGER"]
].map(([name, type]) => ({ name, type, mode: "NULLABLE" }));

function parseNonNegativeInteger(value, fallback) {
  if (value === undefined || value === "") return fallback;
  const parsed = Number(value);
  return Number.isInteger(parsed) && parsed >= 0 ? parsed : fallback;
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
  if (!forceDownload && await exists(sourceParquetPath)) {
    console.log(`Using existing ${sourceParquetPath}`);
    return;
  }

  console.log(`Downloading ${sourceUrl}`);
  console.log("Note: the full ClickBench hits file is large (tens of GB), so this download can take a while.");
  const response = await fetch(sourceUrl);
  if (!response.ok || !response.body) {
    throw new Error(`failed to download ${sourceUrl}: ${response.status} ${response.statusText}`);
  }
  await pipeline(Readable.fromWeb(response.body), fs.createWriteStream(sourceParquetPath));
  console.log(`Wrote ${sourceParquetPath}`);
}

// Returns the parquet path that should be loaded. When CLICKBENCH_ROW_LIMIT is
// positive we carve a bounded subset with duckdb so the multipart REST load
// stays under the emulator load ceiling. A limit of 0 loads the source file
// directly, which generally requires the streaming load path.
function resolveLoadParquet() {
  if (rowLimit === 0) {
    console.log("CLICKBENCH_ROW_LIMIT=0, targeting the full hits file (streaming load recommended).");
    return sourceParquetPath;
  }

  if (!forceSubset && fs.existsSync(subsetParquetPath)) {
    console.log(`Using existing ${subsetParquetPath}`);
    return subsetParquetPath;
  }

  const sql = `
COPY (
  SELECT * FROM read_parquet(${sqlString(sourceParquetPath)})
  LIMIT ${rowLimit}
) TO ${sqlString(subsetParquetPath)} (FORMAT PARQUET);
`;
  console.log(`Creating ${rowLimit.toLocaleString()} row subset at ${subsetParquetPath}`);
  run("duckdb", [":memory:", "-c", sql]);
  return subsetParquetPath;
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

async function loadParquet(parquetPath) {
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
  const boundary = `bq-studio-clickbench-${randomUUID()}`;
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
  console.log(`Load job ${result.jobReference?.jobId || "(unknown)"} finished in ${durationMs.toLocaleString()} ms`);
}

async function sanityQuery() {
  const query = `
SELECT
  COUNT(*) AS row_count,
  COUNT(DISTINCT UserID) AS distinct_users,
  MIN(EventDate) AS first_event,
  MAX(EventDate) AS last_event
FROM \`${projectId}.${datasetId}.${tableId}\`
`;
  const result = await emulatorFetch(`/bigquery/v2/projects/${encodeURIComponent(projectId)}/queries`, {
    method: "POST",
    headers: { "content-type": "application/json; charset=utf-8" },
    body: JSON.stringify({ query, useLegacySql: false, maxResults: 10 })
  });
  const values = result.rows?.[0]?.f?.map(cell => cell.v) || [];
  console.log(`Sanity: rows=${values[0] || "?"}, distinct_users=${values[1] || "?"}, first_event=${values[2] || "?"}, last_event=${values[3] || "?"}`);
}

async function main() {
  console.log(`Target emulator: ${emulatorUrl}`);
  console.log(`Target table: ${projectId}.${datasetId}.${tableId}`);
  // The subset carve shells out to duckdb; a full-file load (limit 0) does not.
  if (rowLimit !== 0) requireCli("duckdb");
  await downloadParquet();
  const loadParquetPath = resolveLoadParquet();

  if (prepareOnly) {
    console.log("Prepared local ClickBench parquet files only (--prepare-only).");
    return;
  }

  await waitForEmulator(emulatorUrl, { projectId });
  await ensureDataset();
  await dropExistingTable();
  await loadParquet(loadParquetPath);
  await sanityQuery();
}

main().catch(error => {
  console.error(error.message);
  if (error.details) console.error(JSON.stringify(error.details, null, 2));
  process.exit(1);
});
