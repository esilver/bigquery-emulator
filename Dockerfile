ARG GO_VERSION=1.26.2
ARG DEBIAN_VERSION=bookworm

FROM --platform=$BUILDPLATFORM golang:${GO_VERSION}-${DEBIAN_VERSION} AS builder

# Provided by buildx for every target platform.
ARG TARGETOS
ARG TARGETARCH

WORKDIR /build
# Copy the dependency manifests first to leverage the Docker layer cache.
COPY go.mod go.sum ./
RUN go mod download

COPY . ./

ARG VERSION
ARG REVISION

# The SQL backend is pure Go; build a fully static binary without cgo.
# The builder stage runs on the native build platform and cross-compiles
# for the requested target, so multi-arch image builds need no QEMU.
RUN CGO_ENABLED=0 GOOS=${TARGETOS} GOARCH=${TARGETARCH} go build -o /go/bin/bigquery-emulator \
    -ldflags "-s -w -X main.version=${VERSION} -X main.revision=${REVISION}" \
    ./cmd/bigquery-emulator

FROM debian:${DEBIAN_VERSION}

COPY --from=builder /go/bin/bigquery-emulator /bin/bigquery-emulator

WORKDIR /work

# Bundle the tiny sample dataset so the quickstart returns a real query
# result with one command (see README quickstart, --data-from-yaml).
COPY --from=builder /build/server/testdata/data.yaml /work/sample.yaml

ENTRYPOINT ["/bin/bigquery-emulator"]
