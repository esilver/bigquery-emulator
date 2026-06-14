const test = require("node:test");
const assert = require("node:assert/strict");
const http = require("node:http");

// server.js reads its target URLs from the environment at module load, so the
// stub upstream is registered before requiring it. node --test gives this file
// its own process, so these assignments do not leak into other test files.
let upstreamRequests = [];
let upstreamHandler = () => ({ status: 200, body: {} });

const upstream = http.createServer((req, res) => {
  const chunks = [];
  req.on("data", chunk => chunks.push(chunk));
  req.on("end", () => {
    const body = Buffer.concat(chunks).toString("utf8");
    upstreamRequests.push({ method: req.method, url: req.url, body });
    let outcome;
    try {
      outcome = upstreamHandler(req, body) || { status: 200, body: {} };
    } catch (error) {
      outcome = { status: 500, body: { error: { message: error.message } } };
    }
    res.writeHead(outcome.status, { "content-type": "application/json" });
    res.end(JSON.stringify(outcome.body ?? {}));
  });
});

let proxy;
let baseUrl;

test.before(async () => {
  await new Promise(resolve => upstream.listen(0, "127.0.0.1", resolve));
  const upstreamUrl = `http://127.0.0.1:${upstream.address().port}`;
  process.env.BQ_DUCKDB_EMULATOR_URL = upstreamUrl;
  process.env.BQ_SQLITE_EMULATOR_URL = upstreamUrl;
  process.env.BQ_DEFAULT_TARGET = "duckdb";

  const { createServer } = require("../server");
  proxy = createServer();
  await new Promise(resolve => proxy.listen(0, "127.0.0.1", resolve));
  baseUrl = `http://127.0.0.1:${proxy.address().port}`;
});

test.after(async () => {
  await new Promise(resolve => proxy.close(resolve));
  await new Promise(resolve => upstream.close(resolve));
});

test.beforeEach(() => {
  upstreamRequests = [];
  upstreamHandler = () => ({ status: 200, body: {} });
});

async function call(path, options = {}) {
  const response = await fetch(`${baseUrl}${path}`, options);
  const text = await response.text();
  return { status: response.status, body: text ? JSON.parse(text) : {} };
}

function datasetsPayload() {
  return {
    datasets: [
      { datasetReference: { projectId: "test", datasetId: "dataset1" }, location: "US" },
      { datasetReference: { projectId: "test", datasetId: "bqjob_r999" }, location: "US" }
    ]
  };
}

test("targetFromUrl routes on the target query param and falls back to the default", () => {
  const { targetFromUrl } = require("../server");
  assert.equal(targetFromUrl(new URL("http://x/api/health?target=sqlite")).id, "sqlite");
  assert.equal(targetFromUrl(new URL("http://x/api/health?target=duckdb")).id, "duckdb");
  assert.equal(targetFromUrl(new URL("http://x/api/health")).id, "duckdb");
  assert.equal(targetFromUrl(new URL("http://x/api/health?target=bogus")).id, "duckdb");
});

test("/api/targets reports both backends with their cancel modes", async () => {
  const { status, body } = await call("/api/targets");
  assert.equal(status, 200);
  const byId = new Map(body.targets.map(target => [target.id, target]));
  assert.equal(byId.get("duckdb").cancelMode, "interrupt");
  assert.equal(byId.get("sqlite").cancelMode, "detach");
  assert.equal(body.defaultTarget, "duckdb");
});

test("/api/datasets filters generated artifact datasets through HTTP", async () => {
  upstreamHandler = () => ({ status: 200, body: datasetsPayload() });
  const { status, body } = await call("/api/datasets");
  assert.equal(status, 200);
  assert.deepEqual(body.datasets.map(dataset => dataset.id), ["dataset1"]);
});

test("/api/health returns ok:true with a numeric duration and filtered dataset count", async () => {
  upstreamHandler = () => ({ status: 200, body: datasetsPayload() });
  const { status, body } = await call("/api/health");
  assert.equal(status, 200);
  assert.equal(body.ok, true);
  assert.equal(body.datasetCount, 1);
  assert.equal(typeof body.durationMs, "number");
});

test("/api/health stays a 200 verdict with ok:false when the backend is unreachable", async () => {
  upstreamHandler = () => ({ status: 503, body: { error: { message: "backend down" } } });
  const { status, body } = await call("/api/health?target=sqlite");
  assert.equal(status, 200);
  assert.equal(body.ok, false);
  assert.equal(body.targetId, "sqlite");
  assert.ok(body.error);
});

test("/api/tables rejects an illegal dataset name with 400 while allowing dashes", async () => {
  upstreamHandler = () => ({ status: 200, body: { tables: [] } });
  const dotted = await call("/api/tables?dataset=bad.dataset");
  assert.equal(dotted.status, 400);
  assert.match(dotted.body.error, /Invalid dataset/);

  const dashed = await call("/api/tables?dataset=ok-dataset");
  assert.equal(dashed.status, 200);
});

test("/api/query rejects an empty body with 400", async () => {
  const { status, body } = await call("/api/query", {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify({ query: "  " })
  });
  assert.equal(status, 400);
  assert.equal(body.error, "Query is required");
});

test("/api/query forwards a clamped maxResults to the emulator and echoes the row limit", async () => {
  upstreamHandler = () => ({
    status: 200,
    body: {
      jobReference: { jobId: "job_rows" },
      totalRows: "1",
      schema: { fields: [{ name: "n", type: "INTEGER" }] },
      rows: [{ f: [{ v: "1" }] }]
    }
  });

  // A request above the hard cap is held at the cap both in the forwarded page
  // size and in the rowLimit the proxy reports back to the UI.
  const { status, body } = await call("/api/query", {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify({ query: "SELECT 1 AS n", maxResults: 999999 })
  });
  assert.equal(status, 200);
  assert.equal(body.rowLimit, 50000);

  const queryRequest = upstreamRequests.find(entry => /\/queries$/.test(entry.url));
  assert.ok(queryRequest, "expected a forwarded query request");
  assert.equal(JSON.parse(queryRequest.body).maxResults, 50000);
});

test("/api/query falls back to the default page size when maxResults is blank", async () => {
  upstreamHandler = () => ({
    status: 200,
    body: {
      jobReference: { jobId: "job_default" },
      totalRows: "1",
      schema: { fields: [{ name: "n", type: "INTEGER" }] },
      rows: [{ f: [{ v: "1" }] }]
    }
  });
  const { status, body } = await call("/api/query", {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify({ query: "SELECT 1 AS n" })
  });
  assert.equal(status, 200);
  assert.equal(body.rowLimit, 1000);
  const queryRequest = upstreamRequests.find(entry => /\/queries$/.test(entry.url));
  assert.equal(JSON.parse(queryRequest.body).maxResults, 1000);
});

test("/api/query/compare runs every backend and reports per-pane divergence", async () => {
  upstreamHandler = (req, body) => {
    const parsed = JSON.parse(body);
    if (/boom/.test(parsed.query)) {
      return { status: 400, body: { error: { message: "syntax error near boom" } } };
    }
    return {
      status: 200,
      body: {
        jobReference: { jobId: "job_ok" },
        totalRows: "1",
        schema: { fields: [{ name: "n", type: "INTEGER" }] },
        rows: [{ f: [{ v: "1" }] }]
      }
    };
  };
  const { status, body } = await call("/api/query/compare", {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify({ query: "SELECT 1 AS n" })
  });
  assert.equal(status, 200);
  assert.equal(body.results.length, 2);
  assert.ok(body.results.every(entry => entry.ok));
  assert.deepEqual(body.results.map(entry => entry.targetId).sort(), ["duckdb", "sqlite"]);
});

test("/api/query/compare returns ok:false for a failing pane and ok:true for the other", async () => {
  let calls = 0;
  upstreamHandler = () => {
    calls += 1;
    if (calls === 1) {
      return { status: 400, body: { error: { message: "dialect gap" } } };
    }
    return {
      status: 200,
      body: {
        jobReference: { jobId: "job_ok" },
        totalRows: "1",
        schema: { fields: [{ name: "n", type: "INTEGER" }] },
        rows: [{ f: [{ v: "1" }] }]
      }
    };
  };
  const { status, body } = await call("/api/query/compare", {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify({ query: "SELECT 1 AS n" })
  });
  assert.equal(status, 200);
  const okFlags = body.results.map(entry => entry.ok).sort();
  assert.deepEqual(okFlags, [false, true]);
  const failing = body.results.find(entry => !entry.ok);
  assert.equal(failing.durationMs, null);
  assert.ok(failing.error);
});

test("unknown API routes return 404", async () => {
  const { status, body } = await call("/api/does-not-exist");
  assert.equal(status, 404);
  assert.equal(body.error, "Unknown API route");
});
