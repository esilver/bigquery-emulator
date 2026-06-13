# Fork delta map

This repository is a fork of [goccy/bigquery-emulator](https://github.com/goccy/bigquery-emulator). It swaps the SQL backend to the DuckDB-backed [esilver/googlesqlite](https://github.com/esilver/googlesqlite) engine, runs pure-Go (`CGO_ENABLED=0`), and publishes its own image and release artifacts under `esilver/`. The Go module path stays `github.com/goccy/bigquery-emulator` so import paths are unchanged.

This file lists the areas that diverge from upstream and a one-line why for each, to help a maintainer grok the divergence or rebase onto upstream.

## SQL backend swap

- `go.mod` - two `replace` directives repoint the SQL engine to the fork's builds:
  - `github.com/goccy/googlesqlite => github.com/esilver/googlesqlite` - the DuckDB-backed GoogleSQL engine that replaces upstream's SQLite-backed one.
  - `github.com/goccy/go-googlesql => github.com/esilver/go-googlesql` - matching GoogleSQL parser/runtime fork pinned alongside it.
  - The DuckDB backend is reachable only through these `replace` directives, so a plain `go install` by module path still yields the upstream SQLite build (this is called out in the README Install note).

## Parquet temporal-load handling (zone-shift fix)

- `server/handler.go` - `convertParquetTemporal` (around line 535) rewrites the temporal cells of a reconstructed parquet row. parquet-go materializes TIMESTAMP/DATE/TIME leaves into `interface{}` as raw integers (micros/millis/nanos since epoch, a day count, or time-of-day units), so this step converts each to a UTC `time.Time` before the bind seam instead of letting a raw integer bind into a real temporal column. It is invoked from the parquet load path (around line 759).

- `internal/contentdata/repository.go` - civil-form bind for the zoneless types (around lines 657-675, helpers `formatCivilDate` / `formatCivilDateTime` / `formatCivilTime` around lines 688-708). A `time.Time` binds directly into TIMESTAMP, but DATE, DATETIME and TIME are civil (zoneless). The value layer encodes a `time.Time` as a zoned TIMESTAMP, so binding it into one of those columns would apply a local-zone shift (for example a midnight-UTC DATE rolling back a day west of UTC). These render the value to a civil-form string in UTC so the cast into the column is purely textual and zone-safe.

## External tables implemented

- `server/handler.go` - `handleExternalTable` (around line 3298), routed from `tablesInsertHandler` when `ExternalDataConfiguration` is set (around line 3250). It translates the external data configuration into a load job, routes it through the existing GCS load pipeline to create and populate a backing table, then records the external configuration on the table metadata so it round-trips on `tables.get`/`tables.list`. This is snapshot-at-create semantics, not live-per-query reads.
- `server/server_test.go` - `TestExternalTable` (around line 3645) covers creating and querying an external table over CSV from a fake GCS server.
- Reflected in `docs/feature-support.md` as 🟡 (partial) rather than upstream's unimplemented status.

## Pure-Go build and fork-owned publishing

- `Dockerfile` - builds a fully static binary with `CGO_ENABLED=0`, cross-compiling for the target platform from the build platform so multi-arch image builds need no QEMU. This is possible because the swapped-in SQL backend is pure Go.
- `.github/workflows/build.yml` - publishes the container image to `ghcr.io/${{ github.repository }}` (the fork's GHCR namespace) as a single multi-arch manifest (`linux/amd64`, `linux/arm64`) and attaches a SLSA build-provenance attestation.
- `.github/workflows/release.yml` - runs GoReleaser to publish prebuilt binaries and `deb`/`rpm`/`apk` packages, with `--parallelism 1` to keep memory bounded on the runner, plus a build-provenance attestation over the artifacts.
- `.goreleaser.yml` - packaging metadata (homepage/maintainer) identifies the fork; build targets match upstream.
- `README.md` - install/run instructions point at the fork's image (`ghcr.io/esilver/bigquery-emulator`), the fork's releases page, and `gh attestation verify --repo esilver/bigquery-emulator`, while intentionally keeping the upstream `go install github.com/goccy/...` path and an explicit pointer to the upstream SQLite-backed image.

## BQ Studio side-by-side workbench

- `bq-studio-emulator/` - a local BigQuery Studio-style workbench (`server.js`, `scripts/`, `public/`) added by the fork. It is wired to talk to two emulator processes at once: the DuckDB-backed build (default `http://localhost:9050`) and the upstream SQLite-backed build (default `http://localhost:9051`), so the two backends can be compared side by side. Includes seed helpers for the SQLite storage DB and for a bounded NYC Taxi sample loaded into the DuckDB-backed emulator.
