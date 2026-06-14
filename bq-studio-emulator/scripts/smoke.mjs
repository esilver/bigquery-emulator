// Boot server.js against an in-process stub upstream and exercise the live
// route table: poll /api/health until ok:true, then run a SELECT 1 query and
// assert the decoded shape. Catches boot-time and route-wiring breakage that
// node --check cannot. Docker-free so it stays fast on every PR.

import http from "node:http";
import { spawn } from "node:child_process";
import { fileURLToPath } from "node:url";
import path from "node:path";

const here = path.dirname(fileURLToPath(import.meta.url));
const serverPath = path.join(here, "..", "server.js");

const datasetsPayload = {
  datasets: [{ datasetReference: { projectId: "test", datasetId: "dataset1" }, location: "US" }]
};

const selectOnePayload = {
  jobReference: { jobId: "smoke_job" },
  totalRows: "1",
  schema: { fields: [{ name: "n", type: "INTEGER" }] },
  rows: [{ f: [{ v: "1" }] }]
};

function startStub() {
  const stub = http.createServer((req, res) => {
    res.writeHead(200, { "content-type": "application/json" });
    if (/\/queries$/.test(req.url)) {
      res.end(JSON.stringify(selectOnePayload));
    } else {
      res.end(JSON.stringify(datasetsPayload));
    }
  });
  return new Promise(resolve => stub.listen(0, "127.0.0.1", () => resolve(stub)));
}

async function getJson(url, options) {
  const response = await fetch(url, options);
  const text = await response.text();
  return { status: response.status, body: text ? JSON.parse(text) : {} };
}

async function pollHealth(baseUrl, deadline) {
  for (;;) {
    try {
      const { body } = await getJson(`${baseUrl}/api/health`);
      if (body.ok === true) return body;
    } catch {
      // Server may still be binding the port; retry until the deadline.
    }
    if (Date.now() >= deadline) throw new Error("server did not report ok:true before timeout");
    await new Promise(resolve => setTimeout(resolve, 200));
  }
}

async function main() {
  const stub = await startStub();
  const upstreamUrl = `http://127.0.0.1:${stub.address().port}`;
  const port = 5188;
  const server = spawn(process.execPath, [serverPath], {
    env: {
      ...process.env,
      PORT: String(port),
      HOST: "127.0.0.1",
      BQ_DUCKDB_EMULATOR_URL: upstreamUrl,
      BQ_SQLITE_EMULATOR_URL: upstreamUrl
    },
    stdio: "inherit"
  });

  const baseUrl = `http://127.0.0.1:${port}`;
  let failure = null;
  try {
    await pollHealth(baseUrl, Date.now() + 15000);

    const query = await getJson(`${baseUrl}/api/query`, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({ query: "SELECT 1 AS n" })
    });
    if (query.status !== 200) throw new Error(`query returned ${query.status}`);
    if ((query.body.rows || []).length !== 1) throw new Error("expected exactly one row");
    if (query.body.rows[0].n !== "1") throw new Error(`expected n=1, got ${query.body.rows[0].n}`);
    if (typeof query.body.durationMs !== "number") throw new Error("missing numeric durationMs");
    if (typeof query.body.rowLimit !== "number") throw new Error("missing numeric rowLimit");

    console.log("smoke ok: health + SELECT 1 round-trip passed");
  } catch (error) {
    failure = error;
  } finally {
    server.kill("SIGTERM");
    stub.close();
  }

  if (failure) {
    console.error(`smoke failed: ${failure.message}`);
    process.exit(1);
  }
}

main().catch(error => {
  console.error(`smoke crashed: ${error.message}`);
  process.exit(1);
});
