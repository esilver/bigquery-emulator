VERSION ?= latest
REVISION := $(shell git rev-parse --short HEAD)

# The SQL backend is pure Go (DuckDB transpiled to Go — module
# github.com/esilver/duckdb-go-pure), so the emulator builds without cgo and
# links a fully static binary on every platform. The -gcflags entry keeps the
# compiler's per-package memory bounded on the ~30 generated engine packages
# ('-c=1', serial function compilation); since duckdb-go-pure v0.3.0 the
# engine compiles fully optimized — no '-N'/'-l'. First build takes about a
# minute on a multicore box, cached afterwards.
emulator/build:
	CGO_ENABLED=0 go build \
		-gcflags='github.com/esilver/duckdb-go-pure/internal/duckdbcore/...=-c=1' \
		-o bigquery-emulator \
		-ldflags='-s -w -X main.version=${VERSION} -X main.revision=${REVISION}' \
		./cmd/bigquery-emulator

# The Dockerfile cross-compiles via the BuildKit platform args
# ($BUILDPLATFORM/$TARGETOS/$TARGETARCH), so it requires buildx.
docker/build:
	docker buildx build --load -t bigquery-emulator . \
		--build-arg VERSION=${VERSION} --build-arg REVISION=${REVISION}

# Build the multi-arch image exactly as CI does. Without --push buildx
# keeps the result in the build cache; add --push to publish a manifest.
docker/build/multiarch:
	docker buildx build --platform linux/amd64,linux/arm64 -t bigquery-emulator . \
		--build-arg VERSION=${VERSION} --build-arg REVISION=${REVISION}

# Run the multi-language client conformance suite (Python/Ruby/PHP/Node.js/bq/Java).
# Requires a running Docker daemon; skips automatically when none is found.
test/e2e:
	go test -count=1 -timeout 30m -v ./test/e2e/...
