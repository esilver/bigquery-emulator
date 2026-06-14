# BQ Studio Emulator

A local BigQuery Studio-style workbench for the BigQuery emulator. It gives you
an explorer, a SQL editor with results grid, a CSV loader, a benchmark tab, and
query history, all served by a zero-dependency Node proxy.

The workbench fronts two emulator backends at once: the DuckDB-backed fork
(`ghcr.io/esilver/bigquery-emulator`) and the upstream SQLite-backed build
(`ghcr.io/goccy/bigquery-emulator`). Pick a backend in the top bar and the
explorer, editor, and benchmark tab all retarget it, so you can run the same
query against both engines and compare results and timings. DuckDB loads Parquet
directly, runs the TPC-H and ClickBench corpora, and is markedly faster on bulk
load, which the benchmark tab makes visible side by side.

## Quick start

Bring up both emulators and the UI with one command. The bundled sample
(`test.dataset1.table_a`) loads into both backends on startup, so the landing
query returns rows on either engine right away.

```bash
cd bq-studio-emulator
docker compose up
# open http://127.0.0.1:5177
```

Services and ports (published to `127.0.0.1` only, this is a local workbench):

- UI: `http://127.0.0.1:5177`
- DuckDB emulator: `http://127.0.0.1:9050`
- SQLite emulator: `http://127.0.0.1:9051`
- Project: `test`

## Prerequisites

- `docker compose` for the quick-start path.
- For the manual path and the seeders: Node `>=20`.
- The `nyc-taxi`, `tpch`, and `clickbench` seeders shell out to the `duckdb`
  CLI (macOS: `brew install duckdb`). The `sqlite` seeder shells out to the
  `sqlite3` CLI (ships with macOS, `apt-get install sqlite3` on Debian/Ubuntu).
  Each seeder checks for its CLI on startup and prints an install hint if it is
  missing.

## Sample corpora

Each seeder loads a dataset into a running emulator and prints a sanity query at
the end. The seeders wait for the emulator to answer before loading, so you can
start them right after `docker compose up`. Override `BQ_DUCKDB_EMULATOR_URL`
(or `BQ_SQLITE_EMULATOR_URL` for the SQLite storage seeder) to target a
different address.

### NYC Taxi (DuckDB backend)

```bash
NYC_TAXI_ROW_LIMIT=100000 npm run seed:nyc-taxi
```

Downloads the official TLC Yellow Taxi January 2024 Parquet file into a working
directory (overridable via `NYC_TAXI_WORKDIR`), carves a CSV sample with the
local `duckdb` CLI, and loads it into `test.nyc_taxi.yellow_tripdata_2024_01`.
The pickup/dropoff datetime fields load as strings to sidestep the emulator's
current CSV DATETIME binding behavior. Set `NYC_TAXI_PREPARE_ONLY=1` to prepare
the local files without contacting the emulator.

### TPC-H (DuckDB backend)

```bash
TPCH_SCALE_FACTOR=0.1 npm run seed:tpch
```

Generates the 8 TPC-H tables with the duckdb `tpch` extension (installed on
demand), exports each to Parquet, and loads all 8 into the `test.tpch` dataset.
Scale factor `0.1` produces roughly 600k `lineitem` rows, which stays under the
emulator multipart load ceiling. Raise `TPCH_SCALE_FACTOR` with care. Set
`TPCH_PREPARE_ONLY=1` to generate the Parquet files without loading.

### ClickBench (DuckDB backend)

```bash
CLICKBENCH_ROW_LIMIT=100000 npm run seed:clickbench
```

Downloads the canonical ClickBench `hits` Parquet, carves a bounded subset with
`duckdb`, and loads it into `test.clickbench.hits`. The full file is tens of GB,
so the default loads a 100k-row subset over the single multipart upload. Set
`CLICKBENCH_ROW_LIMIT=0` to target the full file, which in practice needs the
streaming load path rather than a single multipart upload. Set
`CLICKBENCH_PREPARE_ONLY=1` to prepare the local files without loading.

### SQLite storage seed (SQLite backend)

```bash
npm run seed:sqlite -- <path-to-sqlite-db>
```

Writes a synthetic `sample` dataset (`publishers_1k`, `events_100k`,
`events_1m`) directly into the upstream emulator's SQLite storage file with the
`sqlite3` CLI. The path defaults to a file under the OS temp dir and is
overridable via `SQLITE_DB`.

## Advanced: run the proxy manually

If you already have one or both emulators running, start the UI on its own and
point it at them with environment variables:

```bash
PORT=5177 \
BQ_DUCKDB_EMULATOR_URL=http://localhost:9050 \
BQ_SQLITE_EMULATOR_URL=http://localhost:9051 \
BQ_PROJECT_ID=test \
BQ_STUDIO_QUERY_MAX_RESULTS=1000 \
npm start
```

`GET /api/health?target=duckdb` (or `target=sqlite`) returns HTTP 200 with an
`ok` flag for the named backend, so a script can poll a single endpoint to learn
whether each emulator is reachable.

## Development

```bash
npm run check   # syntax-checks server.js, public/app.js, every scripts/*.mjs, then runs the tests
npm test        # node:test unit tests for the proxy
```
