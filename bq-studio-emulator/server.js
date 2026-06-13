const http = require("node:http");
const path = require("node:path");
const fs = require("node:fs");
const fsp = require("node:fs/promises");
const { randomUUID } = require("node:crypto");

const PORT = Number(process.env.PORT || 5177);
const HOST = process.env.HOST || "127.0.0.1";
const PROJECT_ID = process.env.BQ_PROJECT_ID || "finance-emulator";
const DEFAULT_TARGET_ID = process.env.BQ_DEFAULT_TARGET || "duckdb";
const QUERY_MAX_RESULTS = parsePositiveInteger(process.env.BQ_STUDIO_QUERY_MAX_RESULTS, 1000);
const TARGETS = [
  {
    id: "duckdb",
    label: "DuckDB",
    emulatorUrl: (process.env.BQ_DUCKDB_EMULATOR_URL || process.env.BQ_EMULATOR_URL || "http://localhost:9050").replace(/\/$/, ""),
    projectId: process.env.BQ_DUCKDB_PROJECT_ID || PROJECT_ID,
    cancelMode: "interrupt"
  },
  {
    id: "sqlite",
    label: "SQLite",
    emulatorUrl: (process.env.BQ_SQLITE_EMULATOR_URL || "http://localhost:9051").replace(/\/$/, ""),
    projectId: process.env.BQ_SQLITE_PROJECT_ID || PROJECT_ID,
    cancelMode: "detach"
  }
];
const TARGET_BY_ID = new Map(TARGETS.map(target => [target.id, target]));
const PUBLIC_DIR = path.join(__dirname, "public");
const MAX_BODY_BYTES = 250 * 1024 * 1024;

const mimeTypes = {
  ".html": "text/html; charset=utf-8",
  ".css": "text/css; charset=utf-8",
  ".js": "text/javascript; charset=utf-8",
  ".json": "application/json; charset=utf-8",
  ".svg": "image/svg+xml",
  ".ico": "image/x-icon"
};

function parsePositiveInteger(value, fallback) {
  const parsed = Number(value);
  return Number.isInteger(parsed) && parsed > 0 ? parsed : fallback;
}

function sendJson(res, status, body) {
  const payload = JSON.stringify(body, null, 2);
  res.writeHead(status, {
    "content-type": "application/json; charset=utf-8",
    "cache-control": "no-store"
  });
  res.end(payload);
}

function sendError(res, status, message, details) {
  sendJson(res, status, { error: message, details });
}

function readBody(req, maxBytes = MAX_BODY_BYTES) {
  return new Promise((resolve, reject) => {
    const chunks = [];
    let size = 0;
    req.on("data", chunk => {
      size += chunk.length;
      if (size > maxBytes) {
        reject(Object.assign(new Error("request body too large"), { statusCode: 413 }));
        req.destroy();
        return;
      }
      chunks.push(chunk);
    });
    req.on("end", () => resolve(Buffer.concat(chunks)));
    req.on("error", reject);
  });
}

async function readJson(req) {
  const body = await readBody(req, 5 * 1024 * 1024);
  if (!body.length) return {};
  return JSON.parse(body.toString("utf8"));
}

async function emulatorFetch(route, options = {}) {
  const target = options.target || targetById(DEFAULT_TARGET_ID);
  const { target: _target, ...fetchOptions } = options;
  const response = await fetch(`${target.emulatorUrl}${route}`, fetchOptions);
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

function requestAbortSignal(req, res, target) {
  if (target.cancelMode !== "interrupt") return undefined;
  const controller = new AbortController();
  const abort = () => {
    if (!res.writableEnded) controller.abort();
  };
  req.on("aborted", abort);
  res.on("close", abort);
  return controller.signal;
}

function responseClosed(res) {
  return res.writableEnded || res.destroyed;
}

function targetById(id) {
  return TARGET_BY_ID.get(id) || TARGET_BY_ID.get(DEFAULT_TARGET_ID) || TARGETS[0];
}

function targetFromUrl(url) {
  return targetById(url.searchParams.get("target") || DEFAULT_TARGET_ID);
}

function normalizeTable(table, target = targetById(DEFAULT_TARGET_ID)) {
  const ref = table.tableReference || {};
  return {
    id: table.id || `${ref.projectId || target.projectId}:${ref.datasetId}.${ref.tableId}`,
    projectId: ref.projectId || target.projectId,
    datasetId: ref.datasetId,
    tableId: ref.tableId,
    type: table.type || "TABLE",
    creationTime: table.creationTime,
    lastModifiedTime: table.lastModifiedTime,
    numRows: table.numRows ? Number(table.numRows) : undefined,
    numBytes: table.numBytes ? Number(table.numBytes) : undefined,
    schema: table.schema || null,
    raw: table
  };
}

function normalizeDataset(dataset, target = targetById(DEFAULT_TARGET_ID)) {
  return {
    id: dataset.datasetReference?.datasetId || dataset.id,
    projectId: dataset.datasetReference?.projectId || target.projectId,
    location: dataset.location,
    raw: dataset
  };
}

function isGeneratedArtifactDataset(datasetId) {
  return /^bqjob_/i.test(datasetId)
    || /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i.test(datasetId);
}

function visibleDatasets(datasets, target) {
  return (datasets || [])
    .map(dataset => normalizeDataset(dataset, target))
    .filter(dataset => !isGeneratedArtifactDataset(dataset.id));
}

function normalizeQueryRows(response) {
  const fields = response.schema?.fields || [];
  const rows = (response.rows || []).map(row => {
    const object = {};
    fields.forEach((field, index) => {
      object[field.name] = normalizeQueryValue(field, row.f?.[index]?.v ?? null);
    });
    return object;
  });
  return {
    jobId: response.jobReference?.jobId,
    jobComplete: response.jobComplete !== false,
    totalRows: response.totalRows ? Number(response.totalRows) : rows.length,
    fields,
    rows,
    raw: response
  };
}

function normalizeQueryValue(field, value) {
  if (value === undefined || value === null) return null;

  if (field.mode === "REPEATED") {
    const itemField = { ...field, mode: "NULLABLE" };
    const items = Array.isArray(value) ? value : value.v;
    if (!Array.isArray(items)) return [];
    return items.map(item => normalizeQueryValue(itemField, item?.v ?? item));
  }

  if (field.type === "RECORD" || field.type === "STRUCT") {
    const cells = value?.f || value?.v?.f || [];
    const object = {};
    for (const [index, child] of (field.fields || []).entries()) {
      object[child.name] = normalizeQueryValue(child, cells[index]?.v ?? null);
    }
    return object;
  }

  return value;
}

function safeIdentifier(value, label) {
  if (!/^[A-Za-z0-9_]+$/.test(value || "")) {
    const err = new Error(`Invalid ${label}. Only letters, numbers, and underscores are supported.`);
    err.statusCode = 400;
    throw err;
  }
  return value;
}

function safeResourceIdentifier(value, label) {
  if (!/^[A-Za-z0-9_-]+$/.test(value || "")) {
    const err = new Error(`Invalid ${label}. Only letters, numbers, underscores, and dashes are supported.`);
    err.statusCode = 400;
    throw err;
  }
  return value;
}

function tableRef(target, dataset, table) {
  return `\`${target.projectId}.${safeResourceIdentifier(dataset, "dataset")}.${safeResourceIdentifier(table, "table")}\``;
}

async function runQuery(target, query, options = {}) {
  const startedAt = performance.now();
  const response = await emulatorFetch(`/bigquery/v2/projects/${encodeURIComponent(target.projectId)}/queries`, {
    target,
    method: "POST",
    signal: options.signal,
    headers: { "content-type": "application/json" },
    body: JSON.stringify({
      query,
      useLegacySql: false,
      maxResults: QUERY_MAX_RESULTS,
      useQueryCache: Boolean(options.useQueryCache)
    })
  });
  const durationMs = Math.round((performance.now() - startedAt) * 100) / 100;
  return { ...normalizeQueryRows(response), durationMs, rowLimit: QUERY_MAX_RESULTS };
}

function parseMultipart(buffer, contentType) {
  const match = /boundary=(?:"([^"]+)"|([^;]+))/i.exec(contentType || "");
  if (!match) {
    const err = new Error("Missing multipart boundary");
    err.statusCode = 400;
    throw err;
  }
  const boundary = `--${match[1] || match[2]}`;
  const text = buffer.toString("latin1");
  const parts = {};

  for (const section of text.split(boundary)) {
    const trimmed = section.replace(/^\r?\n/, "");
    if (!trimmed || trimmed === "--" || trimmed === "--\r\n") continue;
    const separator = trimmed.indexOf("\r\n\r\n");
    if (separator === -1) continue;

    const rawHeaders = trimmed.slice(0, separator);
    let rawBody = trimmed.slice(separator + 4);
    rawBody = rawBody.replace(/\r?\n--$/, "").replace(/\r?\n$/, "");
    const disposition = /content-disposition:\s*form-data;([^\r\n]+)/i.exec(rawHeaders)?.[1] || "";
    const name = /name="([^"]+)"/i.exec(disposition)?.[1];
    const filename = /filename="([^"]*)"/i.exec(disposition)?.[1];
    if (!name) continue;

    const data = Buffer.from(rawBody, "latin1");
    parts[name] = filename !== undefined
      ? { filename, data, value: data.toString("utf8") }
      : { data, value: data.toString("utf8") };
  }

  return parts;
}

function splitCsvLine(line) {
  const values = [];
  let current = "";
  let quoted = false;
  for (let i = 0; i < line.length; i += 1) {
    const char = line[i];
    const next = line[i + 1];
    if (char === '"' && quoted && next === '"') {
      current += '"';
      i += 1;
    } else if (char === '"') {
      quoted = !quoted;
    } else if (char === "," && !quoted) {
      values.push(current);
      current = "";
    } else {
      current += char;
    }
  }
  values.push(current);
  return values;
}

function splitCsvRecords(csvBuffer) {
  const text = csvBuffer.toString("utf8").replace(/^\uFEFF/, "");
  const records = [];
  let start = 0;
  let quoted = false;

  for (let i = 0; i < text.length; i += 1) {
    const char = text[i];
    const next = text[i + 1];
    if (char === '"' && quoted && next === '"') {
      i += 1;
    } else if (char === '"') {
      quoted = !quoted;
    } else if ((char === "\n" || char === "\r") && !quoted) {
      const record = text.slice(start, i);
      if (record.trim().length > 0) records.push(record);
      if (char === "\r" && next === "\n") i += 1;
      start = i + 1;
    }
  }

  const tail = text.slice(start);
  if (tail.trim().length > 0) records.push(tail);
  return records;
}

function isCsvFooterRecord(record) {
  const firstCell = (splitCsvLine(record)[0] || "").trim().toLowerCase();
  return firstCell === "grand total" || firstCell === "grand total:";
}

function prepareCsvUpload(csvBuffer, skipLeadingRows) {
  if (!skipLeadingRows) return { data: csvBuffer, skipLeadingRows: 0, trimmedFooterRows: 0 };
  const records = splitCsvRecords(csvBuffer).slice(skipLeadingRows - 1);
  let trimmedFooterRows = 0;
  while (records.length && isCsvFooterRecord(records[records.length - 1])) {
    records.pop();
    trimmedFooterRows += 1;
  }
  return {
    data: Buffer.from(records.length ? `${records.join("\n")}\n` : "", "utf8"),
    skipLeadingRows: 1,
    trimmedFooterRows
  };
}

function cleanCsvFieldName(header, index) {
  const cleaned = header.trim().replace(/[^A-Za-z0-9_]/g, "_").replace(/^_+|_+$/g, "");
  return cleaned || `field_${index + 1}`;
}

function uniqueCsvFieldNames(names) {
  const seen = new Map();
  return names.map(name => {
    const count = seen.get(name) || 0;
    seen.set(name, count + 1);
    return count ? `${name}_${count + 1}` : name;
  });
}

function pickCsvHeaderIndex(lines, skipLeadingRows) {
  if (!skipLeadingRows) return 0;
  const requestedIndex = Math.max(0, Math.min(lines.length - 1, skipLeadingRows - 1));
  const requestedWidth = splitCsvLine(lines[requestedIndex]).length;
  const candidates = lines.slice(0, Math.min(lines.length, 25)).map((line, index) => ({
    index,
    width: splitCsvLine(line).length
  }));
  const widest = candidates.reduce((best, candidate) => (
    candidate.width > best.width ? candidate : best
  ), { index: requestedIndex, width: requestedWidth });

  if (requestedWidth <= 2 && widest.width > requestedWidth) return widest.index;
  return requestedIndex;
}

function inferBigQueryType(values) {
  const nonEmpty = values.filter(value => value !== "");
  if (!nonEmpty.length) return "STRING";
  if (nonEmpty.every(value => /^-?\d+$/.test(value))) return "INTEGER";
  if (nonEmpty.every(value => /^-?(?:\d+\.\d+|\d+|\.\d+)$/.test(value))) return "FLOAT";
  if (nonEmpty.every(value => /^(true|false)$/i.test(value))) return "BOOLEAN";
  if (nonEmpty.every(value => /^\d{4}-\d{2}-\d{2}$/.test(value))) return "DATE";
  if (nonEmpty.every(value => /^\d{4}-\d{2}-\d{2}[T ][0-9:.+-]+Z?$/.test(value))) return "TIMESTAMP";
  return "STRING";
}

function inferSchema(csvBuffer, skipLeadingRows) {
  const lines = splitCsvRecords(csvBuffer);
  if (!lines.length) {
    const err = new Error("CSV file is empty");
    err.statusCode = 400;
    throw err;
  }
  const effectiveSkipLeadingRows = Math.max(0, Number(skipLeadingRows) || 0);
  const headerIndex = pickCsvHeaderIndex(lines, effectiveSkipLeadingRows);
  const headerValues = splitCsvLine(lines[headerIndex]);
  const headers = uniqueCsvFieldNames(
    effectiveSkipLeadingRows
      ? headerValues.map(cleanCsvFieldName)
      : headerValues.map((_, index) => `field_${index + 1}`)
  );
  const sampleStart = effectiveSkipLeadingRows ? headerIndex + 1 : 0;
  const sampleLines = lines.slice(sampleStart, sampleStart + 100);
  const columns = headers.map(() => []);
  for (const line of sampleLines) {
    const values = splitCsvLine(line);
    headers.forEach((_, index) => columns[index].push(values[index] || ""));
  }
  return {
    schema: {
      fields: headers.map((name, index) => ({ name, type: inferBigQueryType(columns[index]) }))
    },
    skipLeadingRows: effectiveSkipLeadingRows ? headerIndex + 1 : 0
  };
}

async function handleCsvLoad(req, res, target) {
  const body = await readBody(req);
  const parts = parseMultipart(body, req.headers["content-type"]);
  const file = parts.file;
  if (!file?.data?.length) {
    return sendError(res, 400, "Missing CSV file");
  }
  const dataset = safeIdentifier((parts.dataset?.value || "").trim(), "dataset");
  const table = safeIdentifier((parts.table?.value || "").trim(), "table");
  const writeDisposition = (parts.writeDisposition?.value || "WRITE_TRUNCATE").trim();
  let skipLeadingRows = Number(parts.skipLeadingRows?.value || 1);
  let schema;
  if (parts.schemaJson?.value) {
    schema = JSON.parse(parts.schemaJson.value);
  } else {
    const inferred = inferSchema(file.data, skipLeadingRows);
    schema = inferred.schema;
    skipLeadingRows = inferred.skipLeadingRows;
  }

  const upload = prepareCsvUpload(file.data, skipLeadingRows);
  const uploadCsv = upload.data;
  const uploadSkipLeadingRows = upload.skipLeadingRows;

  const metadata = {
    configuration: {
      load: {
        destinationTable: { projectId: target.projectId, datasetId: dataset, tableId: table },
        schema,
        sourceFormat: "CSV",
        skipLeadingRows: uploadSkipLeadingRows,
        writeDisposition
      }
    }
  };

  const boundary = `codex-bqstudio-${randomUUID()}`;
  const multipartBody = Buffer.concat([
    Buffer.from(`--${boundary}\r\ncontent-type: application/json; charset=UTF-8\r\n\r\n${JSON.stringify(metadata)}\r\n`, "utf8"),
    Buffer.from(`--${boundary}\r\ncontent-type: text/csv\r\n\r\n`, "utf8"),
    uploadCsv,
    Buffer.from(`\r\n--${boundary}--\r\n`, "utf8")
  ]);

  const startedAt = performance.now();
  const result = await emulatorFetch(`/upload/bigquery/v2/projects/${encodeURIComponent(target.projectId)}/jobs?uploadType=multipart`, {
    target,
    method: "POST",
    headers: { "content-type": `multipart/related; boundary=${boundary}` },
    body: multipartBody
  });
  const durationMs = Math.round((performance.now() - startedAt) * 100) / 100;
  sendJson(res, 200, {
    ok: true,
    durationMs,
    destination: `${target.projectId}:${dataset}.${table}`,
    schema,
    skipLeadingRows,
    uploadedSkipLeadingRows: uploadSkipLeadingRows,
    trimmedFooterRows: upload.trimmedFooterRows,
    jobId: result.jobReference?.jobId,
    raw: result
  });
}

function stats(samples) {
  const durations = samples.map(sample => sample.durationMs).sort((a, b) => a - b);
  const sum = durations.reduce((total, value) => total + value, 0);
  return {
    runs: durations.length,
    avgMs: Math.round((sum / durations.length) * 100) / 100,
    minMs: durations[0],
    maxMs: durations[durations.length - 1],
    p50Ms: durations[Math.floor((durations.length - 1) * 0.5)],
    p95Ms: durations[Math.floor((durations.length - 1) * 0.95)]
  };
}

async function handleBenchmark(req, res, target) {
  const body = await readJson(req);
  const dataset = safeIdentifier(body.dataset || "dbt_test__audit", "dataset");
  const table = safeIdentifier(body.table || "events_1m", "table");
  const runs = Math.max(1, Math.min(Number(body.runs || 3), 20));
  const ref = tableRef(target, dataset, table);
  const tableMeta = await emulatorFetch(`/bigquery/v2/projects/${encodeURIComponent(target.projectId)}/datasets/${encodeURIComponent(dataset)}/tables/${encodeURIComponent(table)}`, { target });
  const fields = tableMeta.schema?.fields || [];
  const names = new Set(fields.map(field => field.name));
  const amountField = names.has("amount") ? "amount" : fields.find(field => ["INTEGER", "FLOAT", "NUMERIC"].includes(field.type))?.name;
  const groupField = names.has("campaign_id") ? "campaign_id" : fields.find(field => ["STRING", "INTEGER"].includes(field.type))?.name;

  const querySet = [
    { name: "count", sql: `SELECT COUNT(*) AS row_count FROM ${ref}` }
  ];
  if (groupField && amountField) {
    querySet.push({
      name: "grouped",
      sql: `SELECT ${groupField}, COUNT(*) AS row_count, SUM(${amountField}) AS total_amount FROM ${ref} GROUP BY ${groupField} ORDER BY total_amount DESC LIMIT 100`
    });
  }
  if (names.has("publisher_id")) {
    try {
      await emulatorFetch(`/bigquery/v2/projects/${encodeURIComponent(target.projectId)}/datasets/${encodeURIComponent(dataset)}/tables/publishers_1k`, { target });
      querySet.push({
        name: "join",
        sql: `SELECT p.region, p.tier, COUNT(*) AS row_count${amountField ? `, SUM(e.${amountField}) AS total_amount` : ""} FROM ${ref} e JOIN \`${target.projectId}.${dataset}.publishers_1k\` p ON e.publisher_id = p.publisher_id GROUP BY p.region, p.tier ORDER BY row_count DESC`
      });
    } catch {
      // Dimension table is optional for generic benchmark runs.
    }
  }
  if (names.has("publisher_id") && names.has("event_date") && names.has("id") && amountField) {
    querySet.push({
      name: "window",
      sql: `SELECT COUNT(*) AS row_count, MAX(running_amount) AS max_running_amount FROM (SELECT id, publisher_id, SUM(${amountField}) OVER (PARTITION BY publisher_id ORDER BY event_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_amount FROM ${ref}) WHERE running_amount > 0`
    });
    querySet.push({
      name: "self_join",
      sql: `SELECT a.publisher_id, COUNT(*) AS pair_count, SUM(a.${amountField} + b.${amountField}) AS total_pair_amount FROM ${ref} a JOIN ${ref} b ON a.publisher_id = b.publisher_id AND a.event_date = b.event_date WHERE a.id < b.id AND a.id <= 200000 AND b.id <= 200000 GROUP BY a.publisher_id ORDER BY pair_count DESC LIMIT 50`
    });
  }

  const results = [];
  for (const query of querySet) {
    const samples = [];
    let error = null;
    for (let i = 0; i < runs; i += 1) {
      try {
        const result = await runQuery(target, query.sql);
        samples.push({
          run: i + 1,
          durationMs: result.durationMs,
          totalRows: result.totalRows,
          jobId: result.jobId
        });
      } catch (err) {
        error = { message: err.message, details: err.details };
        break;
      }
    }
    results.push({ ...query, samples, stats: samples.length ? stats(samples) : null, error });
  }

  sendJson(res, 200, {
    dataset,
    table,
    tableRows: tableMeta.numRows ? Number(tableMeta.numRows) : undefined,
    runs,
    results
  });
}

async function handleApi(req, res, url) {
  if (req.method === "GET" && url.pathname === "/api/targets") {
    return sendJson(res, 200, {
      defaultTarget: DEFAULT_TARGET_ID,
      targets: TARGETS.map(target => ({
        id: target.id,
        label: target.label,
        emulatorUrl: target.emulatorUrl,
        projectId: target.projectId,
        cancelMode: target.cancelMode
      }))
    });
  }

  const target = targetFromUrl(url);

  if (req.method === "GET" && url.pathname === "/api/health") {
    const startedAt = performance.now();
    const datasets = await emulatorFetch(`/bigquery/v2/projects/${encodeURIComponent(target.projectId)}/datasets`, { target });
    const visible = visibleDatasets(datasets.datasets, target);
    return sendJson(res, 200, {
      ok: true,
      targetId: target.id,
      targetLabel: target.label,
      emulatorUrl: target.emulatorUrl,
      projectId: target.projectId,
      datasetCount: visible.length,
      durationMs: Math.round((performance.now() - startedAt) * 100) / 100
    });
  }

  if (req.method === "GET" && url.pathname === "/api/datasets") {
    const data = await emulatorFetch(`/bigquery/v2/projects/${encodeURIComponent(target.projectId)}/datasets`, { target });
    return sendJson(res, 200, {
      datasets: visibleDatasets(data.datasets, target)
    });
  }

  if (req.method === "GET" && url.pathname === "/api/tables") {
    const dataset = safeResourceIdentifier(url.searchParams.get("dataset"), "dataset");
    const data = await emulatorFetch(`/bigquery/v2/projects/${encodeURIComponent(target.projectId)}/datasets/${encodeURIComponent(dataset)}/tables`, { target });
    return sendJson(res, 200, { dataset, tables: (data.tables || []).map(table => normalizeTable(table, target)) });
  }

  const schemaMatch = /^\/api\/tables\/([^/]+)\/([^/]+)\/schema$/.exec(url.pathname);
  if (req.method === "GET" && schemaMatch) {
    const dataset = safeResourceIdentifier(decodeURIComponent(schemaMatch[1]), "dataset");
    const table = safeResourceIdentifier(decodeURIComponent(schemaMatch[2]), "table");
    const data = await emulatorFetch(`/bigquery/v2/projects/${encodeURIComponent(target.projectId)}/datasets/${encodeURIComponent(dataset)}/tables/${encodeURIComponent(table)}`, { target });
    return sendJson(res, 200, normalizeTable(data, target));
  }

  const previewMatch = /^\/api\/tables\/([^/]+)\/([^/]+)\/preview$/.exec(url.pathname);
  if (req.method === "GET" && previewMatch) {
    const dataset = safeResourceIdentifier(decodeURIComponent(previewMatch[1]), "dataset");
    const table = safeResourceIdentifier(decodeURIComponent(previewMatch[2]), "table");
    const limit = Math.max(1, Math.min(Number(url.searchParams.get("limit") || 100), 1000));
    const result = await runQuery(target, `SELECT * FROM ${tableRef(target, dataset, table)} LIMIT ${limit}`, { signal: requestAbortSignal(req, res, target) });
    if (responseClosed(res)) return;
    return sendJson(res, 200, result);
  }

  if (req.method === "POST" && url.pathname === "/api/query") {
    const body = await readJson(req);
    if (!body.query?.trim()) return sendError(res, 400, "Query is required");
    const result = await runQuery(target, body.query, { useQueryCache: body.useQueryCache, signal: requestAbortSignal(req, res, target) });
    if (responseClosed(res)) return;
    return sendJson(res, 200, result);
  }

  if (req.method === "POST" && url.pathname === "/api/load/csv") {
    return handleCsvLoad(req, res, target);
  }

  if (req.method === "POST" && url.pathname === "/api/benchmark") {
    return handleBenchmark(req, res, target);
  }

  sendError(res, 404, "Unknown API route");
}

async function serveStatic(req, res, url) {
  let pathname = decodeURIComponent(url.pathname);
  if (pathname === "/") pathname = "/index.html";
  const filePath = path.normalize(path.join(PUBLIC_DIR, pathname));
  if (!filePath.startsWith(PUBLIC_DIR)) {
    return sendError(res, 403, "Forbidden");
  }
  try {
    const stat = await fsp.stat(filePath);
    if (!stat.isFile()) throw Object.assign(new Error("not found"), { code: "ENOENT" });
    const ext = path.extname(filePath);
    res.writeHead(200, {
      "content-type": mimeTypes[ext] || "application/octet-stream",
      "cache-control": "no-store"
    });
    fs.createReadStream(filePath).pipe(res);
  } catch (error) {
    if (error.code === "ENOENT") {
      return sendError(res, 404, "Not found");
    }
    throw error;
  }
}

const server = http.createServer(async (req, res) => {
  const url = new URL(req.url, `http://${req.headers.host || "localhost"}`);
  try {
    if (url.pathname.startsWith("/api/")) {
      await handleApi(req, res, url);
    } else {
      await serveStatic(req, res, url);
    }
  } catch (error) {
    if (error.name === "AbortError") {
      if (!responseClosed(res)) {
        sendError(res, 499, "Request canceled");
      }
      return;
    }
    if (responseClosed(res)) return;
    const status = error.statusCode || 500;
    sendError(res, status, error.message || "Internal server error", error.details);
  }
});

server.listen(PORT, HOST, () => {
  console.log(`BQ Studio Emulator listening on http://${HOST}:${PORT}`);
  console.log(`Proxying targets: ${TARGETS.map(target => `${target.id}=${target.projectId}@${target.emulatorUrl}`).join(", ")}`);
});
