# Fork delta map

A fork of [goccy/bigquery-emulator](https://github.com/goccy/bigquery-emulator) that swaps the SQL backend to the DuckDB-backed [esilver/googlesqlite](https://github.com/esilver/googlesqlite) engine, runs pure-Go (`CGO_ENABLED=0`), and publishes its own image and release artifacts under `esilver/`. The Go module path stays `github.com/goccy/bigquery-emulator`, so import paths are unchanged.

Each section below names a file that diverges from upstream with a one-line why, to help a maintainer follow the divergence or rebase onto upstream.

## SQL backend swap

- `go.mod` - two `replace` directives repoint the SQL engine to the fork's builds:
  - `github.com/goccy/googlesqlite => github.com/esilver/googlesqlite` - the DuckDB-backed GoogleSQL engine, replacing upstream's SQLite-backed one.
  - `github.com/goccy/go-googlesql => github.com/esilver/go-googlesql` - the matching parser/runtime fork, pinned alongside it.
  - The DuckDB backend is reachable only through these directives, so a plain `go install` by module path still yields the upstream SQLite build (called out in the README Install note).

## Parquet temporal-load handling (zone-shift fix)

- `server/handler.go` - `convertParquetTemporal`, invoked from the parquet load path, rewrites the temporal cells of a reconstructed row. parquet-go materializes TIMESTAMP/DATE/TIME leaves into `interface{}` as raw integers (epoch micros/millis/nanos, a day count, or time-of-day units), so this converts each to a UTC `time.Time` before the bind seam, ensuring a real temporal value reaches the column.

- `internal/contentdata/repository.go` - civil-form bind for the zoneless types (`formatCivilDate` / `formatCivilDateTime` / `formatCivilTime`). A `time.Time` binds directly into TIMESTAMP, but DATE, DATETIME, and TIME are civil (zoneless). The value layer encodes a `time.Time` as a zoned TIMESTAMP, so binding into one of those columns would apply a local-zone shift (e.g. a midnight-UTC DATE rolling back a day west of UTC). These render the value to a civil-form UTC string so the cast is purely textual and zone-safe.

## External tables implemented

- `server/handler.go` - `handleExternalTable`, routed from `tablesInsertHandler` when `ExternalDataConfiguration` is set. It translates the config into a load job, runs it through the existing GCS load pipeline to create and populate a backing table, then records the config on the table metadata so it round-trips on `tables.get`/`tables.list`. Snapshot-at-create semantics: the backing table is populated once, so later queries read that snapshot.
- `server/server_test.go` - `TestExternalTable` covers creating and querying an external table over CSV from a fake GCS server.
- `docs/feature-support.md` - the entry is 🟡 (partial), upgraded from upstream's unimplemented status.

## Pure-Go build and fork-owned publishing

Enabled by the pure-Go SQL backend, which lets the whole binary build with `CGO_ENABLED=0`.

- `Dockerfile` - fully static `CGO_ENABLED=0` binary, cross-compiled for the target platform from the build platform so multi-arch builds need no QEMU.
- `.github/workflows/build.yml` - publishes the image to `ghcr.io/${{ github.repository }}` (the fork's namespace) as a single multi-arch manifest (`linux/amd64`, `linux/arm64`) with a SLSA build-provenance attestation.
- `.github/workflows/release.yml` - GoReleaser publishes prebuilt binaries and `deb`/`rpm`/`apk` packages (`--parallelism 1` to bound runner memory), with a build-provenance attestation over the artifacts.
- `.goreleaser.yml` - packaging metadata (homepage/maintainer) identifies the fork. Build targets match upstream.
- `README.md` - install/run instructions point at the fork's image, releases page, and `gh attestation verify --repo esilver/bigquery-emulator`, while keeping the upstream `go install github.com/goccy/...` path and a pointer to the upstream SQLite-backed image.

## BQ Studio side-by-side workbench

- `bq-studio-emulator/` - a local BigQuery Studio-style workbench (`server.js`, `scripts/`, `public/`) wired to two emulator processes at once: the DuckDB-backed build (default `http://localhost:9050`) and the upstream SQLite-backed build (default `http://localhost:9051`), for side-by-side comparison. Includes seed helpers for the SQLite storage DB and a bounded NYC Taxi sample for the DuckDB-backed emulator.

## Out of the core image

The workbench (`bq-studio-emulator/`), its host-side seeders, and the benchmark corpora (TPC-H, ClickBench) are satellite artifacts, kept out of the container image (via `.dockerignore`) and the Go module graph so the core image stays a single static binary on Debian. The one in-image seeding path is the load API: `server.YAMLSource` / `--data-from-yaml`, plus the bundled `sample.yaml` the quickstart uses. Larger datasets and benchmark corpora load over that same path from host-side scripts against a running emulator, keeping them out of the image bytes and the reviewable Go diff.
