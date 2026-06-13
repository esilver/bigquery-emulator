# BQ Studio Emulator

Local BigQuery Studio-style workbench for the BigQuery emulator.

```bash
cd bq-studio-emulator
npm start
```

Defaults:

- UI: `http://127.0.0.1:5177`
- DuckDB emulator: `http://localhost:9050`
- SQLite emulator: `http://localhost:9051`
- Project: `finance-emulator`

Override with:

```bash
PORT=5177 \
BQ_DUCKDB_EMULATOR_URL=http://localhost:9050 \
BQ_SQLITE_EMULATOR_URL=http://localhost:9051 \
BQ_PROJECT_ID=finance-emulator \
BQ_STUDIO_QUERY_MAX_RESULTS=1000 \
npm start
```

Seed the upstream SQLite emulator database directly:

```bash
node scripts/seed-sqlite-storage.mjs /private/tmp/bqe-sqlite-data-fast/finance-emulator-sqlite.db
```

Seed a bounded NYC Taxi sample into the DuckDB-backed emulator:

```bash
NYC_TAXI_ROW_LIMIT=100000 npm run seed:nyc-taxi
```

This downloads the official TLC Yellow Taxi January 2024 Parquet file into
`/private/tmp/bq-studio-nyc-taxi`, creates a CSV sample with the local DuckDB
CLI, and loads it into `finance-emulator.nyc_taxi.yellow_tripdata_2024_01`.
The pickup/dropoff datetime fields are loaded as strings to avoid the emulator's
current CSV DATETIME binding issue. Use `NYC_TAXI_PREPARE_ONLY=1` to prepare the
local files without contacting the emulator.
