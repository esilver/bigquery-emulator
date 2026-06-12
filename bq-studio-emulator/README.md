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
npm start
```

Seed the upstream SQLite emulator database directly:

```bash
node scripts/seed-sqlite-storage.mjs /private/tmp/bqe-sqlite-data-fast/finance-emulator-sqlite.db
```
