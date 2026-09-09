# Multi-stage build shared by every binary in cmd/. Select one with --build-arg BIN=agent.
#
# Base images are pinned by digest, not just tag: a tag can be re-pointed at new
# content, so digest pinning is what makes a build reproducible and keeps a
# compromised upstream tag from silently entering the image.

# golang:1.27-alpine
FROM golang@sha256:cf6fca6641884b8433441b2b0652976f975e1d0fdd26d177eaaf8596087f3125 AS build

WORKDIR /src

# Dependencies resolve in their own layer so source edits do not re-download the
# module cache on every build.
COPY go.mod go.sum ./
RUN --mount=type=cache,target=/go/pkg/mod go mod download

COPY . .

ARG BIN=collector
ARG VERSION=dev
ARG COMMIT=none
ARG BUILD_DATE=unknown
ARG TARGETOS
ARG TARGETARCH

# CGO off yields a static binary, which is what lets the runtime stage be
# distroless/static with no libc at all.
# -trimpath strips local filesystem paths; -s -w drops the symbol table and DWARF.
RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    CGO_ENABLED=0 GOOS=${TARGETOS:-linux} GOARCH=${TARGETARCH} \
    go build -trimpath \
      -ldflags "-s -w \
        -X github.com/jamespolk/go-log-aggregator/internal/version.Version=${VERSION} \
        -X github.com/jamespolk/go-log-aggregator/internal/version.Commit=${COMMIT} \
        -X github.com/jamespolk/go-log-aggregator/internal/version.BuildDate=${BUILD_DATE}" \
      -o /out/app ./cmd/${BIN}

# An empty state directory, staged here only so the runtime stage can COPY it with
# the right ownership. Docker initializes a fresh named volume from the image's
# contents at that path, ownership included, so creating it here as uid 65532 is
# what makes the volume writable by a nonroot process. Without it the agent's
# volume mounts root-owned and the agent crash-loops on mkdir with EACCES, which
# is not something distroless can fix at runtime: it has no shell to chown with.
RUN mkdir -p /state

# gcr.io/distroless/static-debian12:nonroot
# No shell, no package manager, no libc: nothing for an attacker to pivot into,
# and nothing to patch on a CVE treadmill. Runs as uid 65532.
FROM gcr.io/distroless/static-debian12@sha256:afa5c872c891853ca7fcf1f12c3edb23f7eeef36189728842dd51042ff57f7ab

COPY --from=build /out/app /app

# The agent persists its checkpoint and spool here, and both must survive a
# container restart for the restart half of phase 3's exit criteria to mean
# anything. The collector never writes here; an empty directory in its image is
# harmless, and keeping one Dockerfile for every binary is worth more than
# avoiding it.
COPY --from=build --chown=65532:65532 /state /var/lib/logagg

USER 65532:65532
WORKDIR /

EXPOSE 8080 9090 9095 9096 7946

ENTRYPOINT ["/app"]
