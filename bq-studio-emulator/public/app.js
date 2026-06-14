const state = {
  health: null,
  targets: [],
  activeTarget: localStorage.getItem("bqStudioTarget") || "duckdb",
  datasets: [],
  tablesByDataset: new Map(),
  selectedDataset: null,
  expandedDataset: null,
  selectedTable: null,
  selectedSchema: null,
  activeQuery: null,
  loadGeneration: 0,
  history: JSON.parse(localStorage.getItem("bqStudioHistory") || "[]")
};

const el = id => document.getElementById(id);

const refs = {
  connectionText: el("connectionText"),
  targetSelect: el("targetSelect"),
  statusBadge: el("statusBadge"),
  cancelQueryBtn: el("cancelQueryBtn"),
  datasetSearch: el("datasetSearch"),
  datasetList: el("datasetList"),
  activeTableLabel: el("activeTableLabel"),
  queryMeta: el("queryMeta"),
  sqlEditor: el("sqlEditor"),
  queryError: el("queryError"),
  resultsGrid: el("resultsGrid"),
  resultSummary: el("resultSummary"),
  tableDetails: el("tableDetails"),
  tableStats: el("tableStats"),
  csvDataset: el("csvDataset"),
  csvTable: el("csvTable"),
  csvWriteDisposition: el("csvWriteDisposition"),
  csvSkipRows: el("csvSkipRows"),
  csvFile: el("csvFile"),
  schemaJson: el("schemaJson"),
  loadResult: el("loadResult"),
  benchDataset: el("benchDataset"),
  benchTable: el("benchTable"),
  benchRuns: el("benchRuns"),
  benchmarkOutput: el("benchmarkOutput"),
  historyList: el("historyList")
};

function pathWithTarget(path) {
  const url = new URL(path, window.location.origin);
  if (url.pathname !== "/api/targets") {
    url.searchParams.set("target", state.activeTarget);
  }
  return `${url.pathname}${url.search}`;
}

async function api(path, options = {}) {
  const response = await fetch(pathWithTarget(path), options);
  const text = await response.text();
  const body = text ? JSON.parse(text) : {};
  if (!response.ok) {
    const error = new Error(body.error || response.statusText);
    error.details = body.details;
    throw error;
  }
  return body;
}

async function loadTargets() {
  const data = await api("/api/targets");
  state.targets = data.targets || [];
  if (!state.targets.some(target => target.id === state.activeTarget)) {
    state.activeTarget = data.defaultTarget || state.targets[0]?.id || "duckdb";
    localStorage.setItem("bqStudioTarget", state.activeTarget);
  }
  renderTargetSelect();
}

function renderTargetSelect() {
  refs.targetSelect.innerHTML = state.targets.map(target => (
    `<option value="${escapeHtml(target.id)}">${escapeHtml(target.label)}</option>`
  )).join("");
  refs.targetSelect.value = state.activeTarget;
}

function activeTargetConfig() {
  return state.targets.find(target => target.id === state.activeTarget) || null;
}

function isStaleLoad(generation) {
  return generation !== state.loadGeneration;
}

function currentViewToken() {
  return {
    target: state.activeTarget,
    generation: state.loadGeneration
  };
}

function isStaleView(token) {
  return token.target !== state.activeTarget || token.generation !== state.loadGeneration;
}

function resetExplorerState() {
  state.health = null;
  state.datasets = [];
  state.tablesByDataset = new Map();
  state.selectedDataset = null;
  state.expandedDataset = null;
  state.selectedTable = null;
  state.selectedSchema = null;
  refs.activeTableLabel.textContent = "No table selected";
  refs.queryMeta.textContent = "";
  refs.tableStats.textContent = "";
  refs.tableDetails.className = "details-content empty-state";
  refs.tableDetails.textContent = "Select a table.";
  setResultsPlaceholder("No results", "Run a query to populate results.");
}

function setStatus(kind, text) {
  refs.statusBadge.className = `status ${kind}`;
  refs.statusBadge.textContent = text;
}

function showError(message, details) {
  refs.queryError.classList.remove("hidden");
  refs.queryError.textContent = details ? `${message}\n${JSON.stringify(details, null, 2)}` : message;
  refs.queryError.scrollTop = 0;
}

function clearError() {
  refs.queryError.classList.add("hidden");
  refs.queryError.textContent = "";
}

function setResultsPlaceholder(summary, message, stateClass = "") {
  refs.resultSummary.textContent = summary;
  refs.queryMeta.textContent = "";
  refs.resultsGrid.className = `grid-shell empty-state ${stateClass}`.trim();
  refs.resultsGrid.textContent = message;
}

function formatDuration(ms) {
  if (!Number.isFinite(ms)) return "";
  if (ms < 1000) return `${Math.max(0, Math.round(ms))} ms`;
  if (ms < 10000) return `${(ms / 1000).toFixed(1)} s`;
  return `${Math.round(ms / 1000)} s`;
}

function formatNumber(value) {
  if (value === undefined || value === null || Number.isNaN(Number(value))) return "";
  return Number(value).toLocaleString();
}

function saveHistory() {
  localStorage.setItem("bqStudioHistory", JSON.stringify(state.history.slice(0, 50)));
}

function addHistory(entry) {
  state.history.unshift({
    id: crypto.randomUUID(),
    createdAt: new Date().toISOString(),
    ...entry
  });
  state.history = state.history.slice(0, 50);
  saveHistory();
  renderHistory();
}

function setBusy(button, busyText) {
  const previous = button.textContent;
  button.disabled = true;
  button.textContent = busyText;
  return () => {
    button.disabled = false;
    button.textContent = previous;
  };
}

function startQueryProgress(startedAt) {
  const update = () => {
    const elapsed = formatDuration(Date.now() - startedAt);
    const prefix = state.activeQuery?.cancelRequested ? "Cancel requested" : "Running";
    refs.queryMeta.textContent = `${prefix} · ${elapsed}`;
    refs.resultSummary.textContent = `${prefix} · ${elapsed}`;
  };
  update();
  return window.setInterval(update, 250);
}

function showCancelButton() {
  const target = activeTargetConfig();
  refs.cancelQueryBtn.classList.remove("hidden");
  refs.cancelQueryBtn.disabled = false;
  refs.cancelQueryBtn.textContent = "Cancel";
  refs.cancelQueryBtn.title = target?.cancelMode === "detach" ? "Detach from running query" : "Cancel running query";
  return () => {
    refs.cancelQueryBtn.classList.add("hidden");
    refs.cancelQueryBtn.disabled = false;
    refs.cancelQueryBtn.textContent = "Cancel";
    refs.cancelQueryBtn.title = "Cancel running query";
  };
}

function cancelActiveQuery() {
  if (!state.activeQuery || state.activeQuery.cancelRequested) return;
  state.activeQuery.cancelRequested = true;
  refs.cancelQueryBtn.disabled = true;
  refs.cancelQueryBtn.textContent = "Canceling";
  const elapsed = formatDuration(Date.now() - state.activeQuery.startedAt);
  refs.queryMeta.textContent = `Cancel requested · ${elapsed}`;
  refs.resultSummary.textContent = `Cancel requested · ${elapsed}`;
  state.activeQuery.controller.abort();
}

function isAbortError(error) {
  return error?.name === "AbortError";
}

async function loadHealth(generation) {
  try {
    const health = await api("/api/health");
    if (isStaleLoad(generation)) return false;
    state.health = health;
    if (health.ok === false) {
      refs.connectionText.textContent = `${health.targetLabel || state.activeTarget}: unreachable at ${health.emulatorUrl}`;
      setStatus("error", "Offline");
      return true;
    }
    refs.connectionText.textContent = `${health.targetLabel || state.activeTarget}: ${health.projectId} at ${health.emulatorUrl}`;
    setStatus("ok", "Connected");
  } catch (error) {
    if (isStaleLoad(generation)) return false;
    refs.connectionText.textContent = "Connection unavailable";
    setStatus("error", "Offline");
  }
  return true;
}

async function loadDatasets(generation) {
  const data = await api("/api/datasets");
  if (isStaleLoad(generation)) return false;
  state.datasets = data.datasets || [];
  renderDatasets();
  return true;
}

function renderDatasets() {
  const search = refs.datasetSearch.value.trim().toLowerCase();
  const preferred = new Set([
    "dataset1",
    "nyc_taxi",
    "tpch",
    "clickbench"
  ]);
  const datasets = state.datasets
    .filter(dataset => !isGeneratedArtifactDataset(dataset.id))
    .filter(dataset => !search || dataset.id.toLowerCase().includes(search))
    .sort((a, b) => {
      const aPreferred = preferred.has(a.id) ? 0 : 1;
      const bPreferred = preferred.has(b.id) ? 0 : 1;
      return aPreferred - bPreferred || a.id.localeCompare(b.id);
    });

  refs.datasetList.innerHTML = "";
  if (!datasets.length) {
    refs.datasetList.className = "dataset-list empty-state";
    refs.datasetList.textContent = "No datasets";
    return;
  }
  refs.datasetList.className = "dataset-list";
  for (const dataset of datasets) {
    const wrapper = document.createElement("div");
    wrapper.className = "dataset-item";

    const button = document.createElement("button");
    const expanded = state.expandedDataset === dataset.id;
    button.className = `dataset-button ${state.selectedDataset === dataset.id || expanded ? "active" : ""}`;
    button.innerHTML = `<span class="dataset-name">${expanded ? "▾" : "▸"} ${escapeHtml(dataset.id)}</span><span class="count-pill">${dataset.location || ""}</span>`;
    button.addEventListener("click", () => toggleDataset(dataset.id));
    wrapper.appendChild(button);

    if (expanded) {
      const tableList = document.createElement("div");
      tableList.className = "table-list";
      const tables = state.tablesByDataset.get(dataset.id) || [];
      if (!tables.length) {
        tableList.innerHTML = `<div class="muted" style="padding: 7px 8px;">No tables</div>`;
      } else {
        for (const table of tables) {
          const tableButton = document.createElement("button");
          tableButton.className = `table-button ${state.selectedTable === table.tableId ? "active" : ""}`;
          tableButton.innerHTML = `<span class="table-name">${escapeHtml(table.tableId)}</span><span class="count-pill">${formatNumber(table.numRows) || table.type}</span>`;
          tableButton.addEventListener("click", () => selectTable(dataset.id, table.tableId));
          tableList.appendChild(tableButton);
        }
      }
      wrapper.appendChild(tableList);
    }
    refs.datasetList.appendChild(wrapper);
  }
}

function isGeneratedArtifactDataset(datasetId) {
  return /^bqjob_/i.test(datasetId)
    || /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i.test(datasetId);
}

async function toggleDataset(datasetId) {
  if (state.expandedDataset === datasetId) {
    state.expandedDataset = null;
    renderDatasets();
    return;
  }
  await selectDataset(datasetId);
}

async function selectDataset(datasetId, generation = state.loadGeneration) {
  if (isStaleLoad(generation)) return false;
  state.selectedDataset = datasetId;
  state.expandedDataset = datasetId;
  state.selectedTable = null;
  refs.csvDataset.value = datasetId;
  refs.benchDataset.value = datasetId;
  renderDatasets();
  try {
    const data = await api(`/api/tables?dataset=${encodeURIComponent(datasetId)}`);
    if (isStaleLoad(generation)) return false;
    state.tablesByDataset.set(datasetId, data.tables || []);
    renderDatasets();
  } catch (error) {
    if (isStaleLoad(generation)) return false;
    state.tablesByDataset.set(datasetId, []);
    renderDatasets();
    showError(error.message, error.details);
  }
  return true;
}

async function selectTable(datasetId, tableId, generation = state.loadGeneration) {
  if (isStaleLoad(generation)) return false;
  state.selectedDataset = datasetId;
  state.expandedDataset = datasetId;
  state.selectedTable = tableId;
  refs.activeTableLabel.textContent = `${datasetId}.${tableId}`;
  refs.csvDataset.value = datasetId;
  refs.csvTable.value = `${tableId}_copy`;
  refs.benchDataset.value = datasetId;
  refs.benchTable.value = tableId;
  refs.sqlEditor.value = `SELECT *
FROM \`${state.health?.projectId || "test"}.${datasetId}.${tableId}\`
LIMIT 100`;
  renderDatasets();
  try {
    const schema = await api(`/api/tables/${encodeURIComponent(datasetId)}/${encodeURIComponent(tableId)}/schema`);
    if (isStaleLoad(generation)) return false;
    state.selectedSchema = schema;
    renderTableDetails(schema);
  } catch (error) {
    if (isStaleLoad(generation)) return false;
    showError(error.message, error.details);
  }
  return true;
}

function renderTableDetails(table) {
  refs.tableStats.textContent = table.numRows !== undefined ? `${formatNumber(table.numRows)} rows` : "";
  const fields = table.schema?.fields || [];
  refs.tableDetails.className = "details-content";
  refs.tableDetails.innerHTML = `
    <dl class="metadata-list">
      <dt>Project</dt><dd>${escapeHtml(table.projectId || "")}</dd>
      <dt>Dataset</dt><dd>${escapeHtml(table.datasetId || "")}</dd>
      <dt>Table</dt><dd>${escapeHtml(table.tableId || "")}</dd>
      <dt>Type</dt><dd>${escapeHtml(table.type || "")}</dd>
      <dt>Rows</dt><dd>${formatNumber(table.numRows) || ""}</dd>
    </dl>
    <table class="schema-table">
      <thead><tr><th>Name</th><th>Type</th><th>Mode</th></tr></thead>
      <tbody>
        ${fields.map(field => `<tr><td>${escapeHtml(field.name)}</td><td>${escapeHtml(field.type)}</td><td>${escapeHtml(field.mode || "")}</td></tr>`).join("")}
      </tbody>
    </table>
  `;
}

async function runQuery(query = refs.sqlEditor.value, source = "manual") {
  if (state.activeQuery) return;
  clearError();
  const viewToken = currentViewToken();
  const controller = new AbortController();
  const restore = setBusy(el("runBtn"), "Running");
  const hideCancel = showCancelButton();
  const startedAt = Date.now();
  const cancelMode = activeTargetConfig()?.cancelMode || "interrupt";
  const activeQuery = { controller, startedAt, cancelRequested: false, cancelMode };
  state.activeQuery = activeQuery;
  setResultsPlaceholder("Running query...", "Running query...", "loading-state");
  const progressTimer = startQueryProgress(startedAt);
  try {
    const result = await api("/api/query", {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({ query }),
      signal: controller.signal
    });
    if (isStaleView(viewToken)) return;
    renderResults(result);
    addHistory({
      query,
      source,
      ok: true,
      durationMs: result.durationMs,
      totalRows: result.totalRows,
      jobId: result.jobId
    });
  } catch (error) {
    if (isStaleView(viewToken)) return;
    const durationMs = Date.now() - startedAt;
    if (isAbortError(error)) {
      const detached = cancelMode === "detach";
      setResultsPlaceholder(
        `${detached ? "Detached" : "Canceled"} · ${formatDuration(durationMs)}`,
        detached
          ? "UI request detached. SQLite keeps running the statement in the emulator so its connection stays usable."
          : "Query request canceled. Backends that honor request cancellation interrupt the running statement.",
        "canceled-state"
      );
    } else {
      setResultsPlaceholder(`Failed · ${formatDuration(durationMs)}`, "Query failed. See error details above.", "error-state");
      showError(error.message, error.details);
    }
    addHistory({
      query,
      source,
      ok: false,
      canceled: isAbortError(error),
      detached: isAbortError(error) && cancelMode === "detach",
      durationMs,
      error: error.message
    });
  } finally {
    window.clearInterval(progressTimer);
    if (state.activeQuery === activeQuery) state.activeQuery = null;
    hideCancel();
    restore();
  }
}

function renderResults(result) {
  const fields = result.fields || [];
  const rows = result.rows || [];
  const limited = result.rowLimit && result.totalRows > rows.length ? ` · limited to ${formatNumber(result.rowLimit)}` : "";
  refs.resultSummary.textContent = `${formatNumber(rows.length)} shown · ${formatNumber(result.totalRows)} total${limited} · ${result.durationMs} ms · ${result.jobId || ""}`;
  refs.queryMeta.textContent = result.jobId ? `Job ${result.jobId}` : "";

  if (!fields.length) {
    refs.resultsGrid.className = "grid-shell empty-state";
    refs.resultsGrid.textContent = "Query completed with no tabular result.";
    return;
  }

  refs.resultsGrid.className = "grid-shell";
  refs.resultsGrid.innerHTML = `
    <table class="data-grid">
      <thead><tr>${fields.map(field => `<th>${escapeHtml(field.name)}<span class="count-pill">${escapeHtml(field.type || "")}</span></th>`).join("")}</tr></thead>
      <tbody>
        ${rows.map(row => `<tr>${fields.map(field => {
          const rawValue = row[field.name];
          const value = formatCellValue(rawValue);
          if (isNestedCellValue(rawValue)) {
            return `<td class="nested-cell" title="${escapeHtml(value)}"><pre class="cell-scroll">${escapeHtml(value)}</pre></td>`;
          }
          return `<td title="${escapeHtml(value)}">${escapeHtml(value)}</td>`;
        }).join("")}</tr>`).join("")}
      </tbody>
    </table>
  `;
}

function formatCellValue(value) {
  if (value === undefined || value === null) return "";
  if (typeof value === "object") {
    try {
      return JSON.stringify(value, null, 2);
    } catch {
      return String(value);
    }
  }
  return String(value);
}

function isNestedCellValue(value) {
  return value !== null && typeof value === "object";
}

async function previewSelectedTable() {
  if (!state.selectedDataset || !state.selectedTable) return;
  clearError();
  const viewToken = currentViewToken();
  const dataset = state.selectedDataset;
  const table = state.selectedTable;
  const startedAt = Date.now();
  refs.queryMeta.textContent = "Running";
  setResultsPlaceholder("Loading preview...", "Loading preview...", "loading-state");
  try {
    const result = await api(`/api/tables/${encodeURIComponent(dataset)}/${encodeURIComponent(table)}/preview?limit=100`);
    if (isStaleView(viewToken)) return;
    renderResults(result);
    addHistory({
      query: `Preview ${dataset}.${table}`,
      source: "preview",
      ok: true,
      durationMs: result.durationMs,
      totalRows: result.totalRows,
      jobId: result.jobId
    });
  } catch (error) {
    if (isStaleView(viewToken)) return;
    setResultsPlaceholder(`Preview failed · ${Date.now() - startedAt} ms`, "Preview failed. See error details above.", "error-state");
    showError(error.message, error.details);
  }
}

function setCountQuery() {
  if (!state.selectedDataset || !state.selectedTable) return;
  refs.sqlEditor.value = `SELECT COUNT(*) AS row_count
FROM \`${state.health?.projectId || "test"}.${state.selectedDataset}.${state.selectedTable}\``;
}

function setAggregateQuery() {
  if (!state.selectedDataset || !state.selectedTable) return;
  const fields = state.selectedSchema?.schema?.fields || [];
  const amount = fields.find(field => ["amount", "revenue", "cost"].includes(field.name)) || fields.find(field => ["INTEGER", "FLOAT", "NUMERIC"].includes(field.type));
  const group = fields.find(field => ["campaign_id", "publisher_id", "category"].includes(field.name)) || fields.find(field => ["STRING", "INTEGER"].includes(field.type));
  if (!amount || !group) {
    setCountQuery();
    return;
  }
  refs.sqlEditor.value = `SELECT ${group.name}, COUNT(*) AS row_count, SUM(${amount.name}) AS total_${amount.name}
FROM \`${state.health?.projectId || "test"}.${state.selectedDataset}.${state.selectedTable}\`
GROUP BY ${group.name}
ORDER BY total_${amount.name} DESC
LIMIT 100`;
}

function toggleSqlLineComment(textarea) {
  const value = textarea.value;
  const selectionStart = textarea.selectionStart;
  const selectionEnd = textarea.selectionEnd;
  const lineStart = value.lastIndexOf("\n", selectionStart - 1) + 1;
  let lineEnd = value.indexOf("\n", selectionEnd);
  if (lineEnd === -1) lineEnd = value.length;
  if (selectionEnd > selectionStart && value[selectionEnd - 1] === "\n") {
    lineEnd = selectionEnd - 1;
  }

  const before = value.slice(0, lineStart);
  const block = value.slice(lineStart, lineEnd);
  const after = value.slice(lineEnd);
  const lines = block.split("\n");
  const nonEmptyLines = lines.filter(line => line.trim().length > 0);
  const shouldUncomment = nonEmptyLines.length > 0 && nonEmptyLines.every(line => /^\s*-- ?/.test(line));

  let deltaBeforeSelectionStart = 0;
  let deltaBeforeSelectionEnd = 0;
  let cursor = lineStart;
  const nextLines = lines.map(line => {
    let nextLine = line;
    let delta = 0;
    const commentMatch = /^(\s*)-- ?/.exec(line);
    if (shouldUncomment && commentMatch) {
      delta = -commentMatch[0].length + commentMatch[1].length;
      nextLine = commentMatch[1] + line.slice(commentMatch[0].length);
    } else if (!shouldUncomment && line.trim().length > 0) {
      const indent = /^\s*/.exec(line)[0];
      nextLine = `${indent}-- ${line.slice(indent.length)}`;
      delta = 3;
    }

    if (cursor < selectionStart) deltaBeforeSelectionStart += delta;
    if (cursor < selectionEnd) deltaBeforeSelectionEnd += delta;
    cursor += line.length + 1;
    return nextLine;
  });

  const nextBlock = nextLines.join("\n");
  textarea.value = before + nextBlock + after;

  if (selectionStart === selectionEnd) {
    const nextPosition = Math.max(lineStart, selectionStart + deltaBeforeSelectionStart);
    textarea.setSelectionRange(nextPosition, nextPosition);
  } else {
    textarea.setSelectionRange(
      Math.max(lineStart, selectionStart + deltaBeforeSelectionStart),
      Math.max(lineStart, selectionEnd + deltaBeforeSelectionEnd)
    );
  }
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
  const requestedWidth = parseCsvLine(lines[requestedIndex]).length;
  const candidates = lines.slice(0, Math.min(lines.length, 25)).map((line, index) => ({
    index,
    width: parseCsvLine(line).length
  }));
  const widest = candidates.reduce((best, candidate) => (
    candidate.width > best.width ? candidate : best
  ), { index: requestedIndex, width: requestedWidth });

  if (requestedWidth <= 2 && widest.width > requestedWidth) return widest.index;
  return requestedIndex;
}

function isCsvFooterLine(line) {
  const firstCell = (parseCsvLine(line)[0] || "").trim().toLowerCase();
  return firstCell === "grand total" || firstCell === "grand total:";
}

function inferSchemaFromText(text, requestedSkipLeadingRows = 1) {
  const lines = text.replace(/^\uFEFF/, "").split(/\r?\n/).filter(line => line.trim().length > 0);
  if (!lines.length) return { schema: { fields: [] }, skipLeadingRows: 0 };
  const skipLeadingRows = Math.max(0, Number(requestedSkipLeadingRows) || 0);
  const headerIndex = pickCsvHeaderIndex(lines, skipLeadingRows);
  const headerValues = parseCsvLine(lines[headerIndex]);
  const headers = uniqueCsvFieldNames(
    skipLeadingRows
      ? headerValues.map(cleanCsvFieldName)
      : headerValues.map((_, index) => `field_${index + 1}`)
  );
  const sampleStart = skipLeadingRows ? headerIndex + 1 : 0;
  const samples = lines
    .slice(sampleStart, sampleStart + 100)
    .filter(line => !isCsvFooterLine(line))
    .map(parseCsvLine);
  return {
    schema: {
      fields: headers.map((name, index) => ({
        name,
        type: inferType(samples.map(row => row[index] || ""))
      }))
    },
    skipLeadingRows: skipLeadingRows ? headerIndex + 1 : 0
  };
}

function parseCsvLine(line) {
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

function inferType(values) {
  const nonEmpty = values.filter(Boolean);
  if (!nonEmpty.length) return "STRING";
  if (nonEmpty.every(value => /^-?\d+$/.test(value))) return "INTEGER";
  if (nonEmpty.every(value => /^-?(?:\d+\.\d+|\d+|\.\d+)$/.test(value))) return "FLOAT";
  if (nonEmpty.every(value => /^(true|false)$/i.test(value))) return "BOOLEAN";
  if (nonEmpty.every(value => /^\d{4}-\d{2}-\d{2}$/.test(value))) return "DATE";
  if (nonEmpty.every(value => /^\d{4}-\d{2}-\d{2}[T ][0-9:.+-]+Z?$/.test(value))) return "TIMESTAMP";
  return "STRING";
}

async function inferCsvSchema() {
  const file = refs.csvFile.files[0];
  if (!file) return;
  const text = await file.slice(0, 1024 * 256).text();
  const inferred = inferSchemaFromText(text, refs.csvSkipRows.value);
  refs.csvSkipRows.value = String(inferred.skipLeadingRows);
  refs.schemaJson.value = JSON.stringify(inferred.schema, null, 2);
  refs.loadResult.className = "load-result muted";
  refs.loadResult.textContent = `Inferred ${inferred.schema.fields.length} fields from CSV record ${inferred.skipLeadingRows || 1}.`;
}

async function loadCsv(event) {
  event.preventDefault();
  const file = refs.csvFile.files[0];
  if (!file) {
    refs.loadResult.textContent = "Choose a CSV file.";
    return;
  }
  if (!refs.schemaJson.value.trim()) await inferCsvSchema();
  const restore = setBusy(el("loadCsvBtn"), "Loading");
  try {
    const form = new FormData();
    form.append("file", file);
    form.append("dataset", refs.csvDataset.value.trim());
    form.append("table", refs.csvTable.value.trim());
    form.append("writeDisposition", refs.csvWriteDisposition.value);
    form.append("skipLeadingRows", refs.csvSkipRows.value);
    form.append("schemaJson", refs.schemaJson.value.trim());
    const result = await api("/api/load/csv", { method: "POST", body: form });
    await selectDataset(refs.csvDataset.value.trim());
    await selectTable(refs.csvDataset.value.trim(), refs.csvTable.value.trim());
    const footerNote = result.trimmedFooterRows ? `\nTrimmed footer rows: ${result.trimmedFooterRows}` : "";
    refs.loadResult.textContent = `Loaded ${result.destination}\nJob: ${result.jobId}\nDuration: ${result.durationMs} ms${footerNote}`;
  } catch (error) {
    refs.loadResult.textContent = `${error.message}\n${error.details ? JSON.stringify(error.details, null, 2) : ""}`;
  } finally {
    restore();
  }
}

async function runBenchmark() {
  const restore = setBusy(el("runBenchmarkBtn"), "Running");
  refs.benchmarkOutput.className = "benchmark-output empty-state";
  refs.benchmarkOutput.textContent = "Running benchmark...";
  try {
    const result = await api("/api/benchmark", {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({
        dataset: refs.benchDataset.value.trim(),
        table: refs.benchTable.value.trim(),
        runs: Number(refs.benchRuns.value || 3)
      })
    });
    renderBenchmark(result);
  } catch (error) {
    refs.benchmarkOutput.className = "benchmark-output";
    refs.benchmarkOutput.innerHTML = `<div class="error-box">${escapeHtml(error.message)}</div>`;
  } finally {
    restore();
  }
}

function renderBenchmark(result) {
  refs.benchmarkOutput.className = "benchmark-output";
  refs.benchmarkOutput.innerHTML = `
    <div class="metadata-list">
      <dt>Table</dt><dd>${escapeHtml(result.dataset)}.${escapeHtml(result.table)}</dd>
      <dt>Rows</dt><dd>${formatNumber(result.tableRows) || ""}</dd>
      <dt>Runs</dt><dd>${result.runs}</dd>
    </div>
    ${result.results.map(item => `
      <section class="benchmark-block">
        <h3>${escapeHtml(item.name)}</h3>
        ${item.stats ? `
          <table class="data-grid">
            <thead><tr><th>avg</th><th>min</th><th>p50</th><th>p95</th><th>max</th><th>runs</th></tr></thead>
            <tbody><tr>
              <td>${item.stats.avgMs} ms</td>
              <td>${item.stats.minMs} ms</td>
              <td>${item.stats.p50Ms} ms</td>
              <td>${item.stats.p95Ms} ms</td>
              <td>${item.stats.maxMs} ms</td>
              <td>${item.stats.runs}</td>
            </tr></tbody>
          </table>
        ` : `<div class="error-box">${escapeHtml(item.error || "Benchmark query failed.")}</div>`}
      </section>
    `).join("")}
  `;
}

function renderHistory() {
  if (!state.history.length) {
    refs.historyList.className = "history-list empty-state";
    refs.historyList.textContent = "No query history.";
    return;
  }
  refs.historyList.className = "history-list";
  refs.historyList.innerHTML = state.history.map(item => `
    <article class="history-item">
      <div class="history-meta">
        <span>${item.ok ? "OK" : item.detached ? "Detached" : item.canceled ? "Canceled" : "Failed"} · ${Math.round(item.durationMs || 0)} ms · ${escapeHtml(item.source || "query")}</span>
        <span>${new Date(item.createdAt).toLocaleString()}</span>
      </div>
      <pre class="history-sql">${escapeHtml(item.query || item.error || "")}</pre>
      <button class="secondary-btn" data-history-id="${item.id}">Open</button>
    </article>
  `).join("");
  refs.historyList.querySelectorAll("[data-history-id]").forEach(button => {
    button.addEventListener("click", () => {
      const item = state.history.find(entry => entry.id === button.dataset.historyId);
      if (item?.query && !item.query.startsWith("Preview ")) {
        refs.sqlEditor.value = item.query;
        activateTab("query");
      }
    });
  });
}

function activateTab(name) {
  document.querySelectorAll(".tab").forEach(tab => tab.classList.toggle("active", tab.dataset.tab === name));
  document.querySelectorAll(".tab-panel").forEach(panel => panel.classList.remove("active"));
  el(`${name}Tab`).classList.add("active");
}

function escapeHtml(value) {
  return String(value)
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/"/g, "&quot;")
    .replace(/'/g, "&#039;");
}

function bindEvents() {
  document.querySelectorAll(".tab").forEach(tab => tab.addEventListener("click", () => activateTab(tab.dataset.tab)));
  refs.targetSelect.addEventListener("change", async () => {
    cancelActiveQuery();
    state.activeTarget = refs.targetSelect.value;
    localStorage.setItem("bqStudioTarget", state.activeTarget);
    resetExplorerState();
    clearError();
    await init();
  });
  el("refreshBtn").addEventListener("click", init);
  el("runBtn").addEventListener("click", () => runQuery());
  refs.cancelQueryBtn.addEventListener("click", cancelActiveQuery);
  refs.sqlEditor.addEventListener("keydown", event => {
    if ((event.metaKey || event.ctrlKey) && event.key === "Enter") {
      event.preventDefault();
      runQuery();
    } else if ((event.metaKey || event.ctrlKey) && event.key === "/") {
      event.preventDefault();
      toggleSqlLineComment(refs.sqlEditor);
    }
  });
  el("previewBtn").addEventListener("click", previewSelectedTable);
  el("sampleCountBtn").addEventListener("click", setCountQuery);
  el("sampleAggregateBtn").addEventListener("click", setAggregateQuery);
  refs.datasetSearch.addEventListener("input", renderDatasets);
  el("inferSchemaBtn").addEventListener("click", inferCsvSchema);
  refs.csvFile.addEventListener("change", inferCsvSchema);
  refs.csvSkipRows.addEventListener("change", inferCsvSchema);
  el("csvForm").addEventListener("submit", loadCsv);
  el("runBenchmarkBtn").addEventListener("click", runBenchmark);
  el("clearHistoryBtn").addEventListener("click", () => {
    state.history = [];
    saveHistory();
    renderHistory();
  });
}

async function init() {
  const generation = state.loadGeneration + 1;
  state.loadGeneration = generation;
  setStatus("pending", "Checking");
  await loadTargets();
  if (isStaleLoad(generation)) return;
  await loadHealth(generation);
  if (isStaleLoad(generation)) return;
  try {
    await loadDatasets(generation);
    if (isStaleLoad(generation)) return;
    const starter = state.datasets.find(dataset => dataset.id === "dataset1") || state.datasets[0];
    if (starter && !state.selectedDataset) {
      await selectDataset(starter.id, generation);
      if (isStaleLoad(generation)) return;
      const tables = state.tablesByDataset.get(starter.id) || [];
      const starterTable = tables.find(table => table.tableId === "table_a") || tables[0];
      if (starterTable) await selectTable(starter.id, starterTable.tableId, generation);
    }
  } catch (error) {
    if (isStaleLoad(generation)) return;
    showError(error.message, error.details);
  }
  if (isStaleLoad(generation)) return;
  renderHistory();
}

bindEvents();
init();
