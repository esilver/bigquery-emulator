// Shared helpers for the seeder scripts: a host-CLI preflight and an emulator
// readiness wait, so a first run fails with an actionable message instead of a
// bare ENOENT or ECONNREFUSED.

import { spawnSync } from "node:child_process";

const CLI_INSTALL_HINTS = {
  duckdb: "Install the DuckDB CLI: https://duckdb.org/docs/installation/ (macOS: brew install duckdb).",
  sqlite3: "Install the sqlite3 CLI (macOS: it ships with the OS; Debian/Ubuntu: apt-get install sqlite3)."
};

// Confirm a host CLI is callable before we shell out to it, turning a missing
// binary into a clear instruction rather than a raw ENOENT mid-run.
export function requireCli(command) {
  const probe = spawnSync(command, ["--version"], { stdio: "ignore" });
  if (probe.error) {
    const hint = CLI_INSTALL_HINTS[command] || `Install the ${command} CLI and ensure it is on your PATH.`;
    console.error(`Required CLI "${command}" was not found on your PATH.`);
    console.error(hint);
    process.exit(1);
  }
}

async function fetchDatasets(emulatorUrl, projectId) {
  const route = `/bigquery/v2/projects/${encodeURIComponent(projectId)}/datasets`;
  const response = await fetch(`${emulatorUrl}${route}`);
  if (!response.ok) {
    throw new Error(`emulator responded with ${response.status} ${response.statusText}`);
  }
  return response;
}

// Poll the datasets endpoint until the emulator answers, so seeding waits for a
// backend that is still starting instead of failing on the first request.
export async function waitForEmulator(emulatorUrl, options = {}) {
  const projectId = options.projectId || "test";
  const timeoutMs = options.timeoutMs || 30000;
  const intervalMs = options.intervalMs || 1000;
  const startCommand = options.startCommand || "docker compose up";
  const deadline = Date.now() + timeoutMs;
  let lastError = null;

  for (;;) {
    try {
      await fetchDatasets(emulatorUrl, projectId);
      return;
    } catch (error) {
      lastError = error;
    }
    if (Date.now() >= deadline) break;
    await new Promise(resolve => setTimeout(resolve, intervalMs));
  }

  console.error(`Emulator not reachable at ${emulatorUrl} after ${Math.round(timeoutMs / 1000)}s.`);
  console.error(`Start it with \`${startCommand}\`, then re-run this seeder.`);
  if (lastError) console.error(`Last error: ${lastError.message}`);
  process.exit(1);
}
