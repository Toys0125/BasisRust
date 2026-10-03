# syntax=docker/dockerfile:1.7
#
# Canonical BasisRust server image.
#
# This is the single source of truth for the published image
# `ghcr.io/toys0125/basisrust-server`. `.github/workflows/build-docker-server.yml`
# builds it from the static musl binaries attached to a GitHub release by passing
# `ARTIFACT_DIR=<dir>` naming an unpacked release payload inside the build context.
# Running `docker build` directly from a source checkout with no `ARTIFACT_DIR`
# compiles the server in the Rust stage instead, which is what keeps
# `docker compose build` in docker-compose-server.yml working.
#
# Why musl on alpine rather than glibc on ubuntu:
#   - Size. The image is ~14 MB of base plus the ~10 MB binary. On ubuntu:24.04 the base
#     alone was ~85 MB, giving a ~98 MB image; on alpine:3.20 it is ~22 MB, and the
#     compressed pull drops from ~37 MB to ~9 MB.
#   - Portability. The binaries are statically linked (no PT_INTERP, no DT_NEEDED), so
#     one artifact runs on glibc and musl distros alike. The glibc builds were not just
#     bigger but fragile: they link GLIBC_2.39 because build.yml compiles on ubuntu-24.04,
#     which makes them refuse to load on older bases (debian:bookworm-slim is GLIBC_2.36)
#     and fail outright on a musl-only distro such as alpine.
#   - The usual musl objection, its weaker malloc, does not apply: the server sets
#     mimalloc as its Rust #[global_allocator], so the C allocator is never used.
#
# Deliberately NOT stripped. `strip --strip-all` would save ~2.3 MB but removes all
# ~13k FUNC symbols, and with no separate debuginfo they cannot be recovered, which would
# break perf symbolisation and backtraces.

ARG RUST_IMAGE=rust:1-alpine
ARG RUNTIME_IMAGE=alpine:3.20

FROM ${RUST_IMAGE} AS build

# mimalloc compiles C, so a C toolchain is required even though the result is a static musl
# binary. CI takes a different route: build.yml installs musl-tools on the ubuntu runners,
# which also yield static musl binaries and avoid shipping a compiler in this stage.
RUN apk add --no-cache build-base musl-dev

WORKDIR /src
COPY . /src

# Build-context-relative directory holding an unpacked release payload, i.e. a
# directory containing `basis-server-console`. Leave empty to compile from source.
ARG ARTIFACT_DIR=

RUN set -eux; \
    mkdir -p /out; \
    if [ -n "${ARTIFACT_DIR}" ] && [ -x "/src/${ARTIFACT_DIR}/basis-server-console" ]; then \
        echo "Staging prebuilt server binary from ${ARTIFACT_DIR}"; \
        cp "/src/${ARTIFACT_DIR}/basis-server-console" /out/basis-server-console; \
    else \
        echo "No prebuilt binary at '${ARTIFACT_DIR}'; compiling from source"; \
        cargo build --release -p basis-server-console; \
        cp "/src/target/release/basis-server-console" /out/basis-server-console; \
    fi; \
    chmod 0755 /out/basis-server-console; \
    # The runtime base has no glibc at all, so a dynamically linked binary would fail at
    # exec with no useful message. Fail here instead. readelf is the authoritative check:
    # musl's own ldd prints a loader path even for a static-pie binary.
    if readelf -l /out/basis-server-console 2>/dev/null | grep -q INTERP; then \
        echo "::error::staged binary is dynamically linked; the alpine runtime needs a static musl build"; \
        exit 1; \
    fi; \
    if readelf -d /out/basis-server-console 2>/dev/null | grep -q NEEDED; then \
        echo "::error::staged binary has shared library dependencies:"; \
        readelf -d /out/basis-server-console | grep NEEDED; \
        exit 1; \
    fi; \
    echo "Static check passed: no PT_INTERP, no DT_NEEDED"

# ---------------------------------------------------------------------------
# Runtime
# ---------------------------------------------------------------------------

FROM ${RUNTIME_IMAGE} AS runtime

ARG BUILD_VERSION=unknown
ARG VCS_REF=unknown
ARG BUILD_DATE=unknown

LABEL org.opencontainers.image.title="BasisRust Server" \
      org.opencontainers.image.description="Basis-compatible dedicated server, Rust rewrite" \
      org.opencontainers.image.source="https://github.com/Toys0125/BasisRust" \
      org.opencontainers.image.url="https://github.com/Toys0125/BasisRust" \
      org.opencontainers.image.documentation="https://github.com/Toys0125/BasisRust#readme" \
      org.opencontainers.image.version="${BUILD_VERSION}" \
      org.opencontainers.image.revision="${VCS_REF}" \
      org.opencontainers.image.created="${BUILD_DATE}" \
      org.opencontainers.image.licenses="MIT"

# curl backs the HEALTHCHECK below; ca-certificates is there for deployments that enable
# outgoing TLS. libgcc-s1 is deliberately absent: the binary is fully static.
RUN apk add --no-cache ca-certificates curl

# Unprivileged runtime user. UID/GID are fixed and exposed as build args so a host
# volume can be chowned to match (docker-compose-server.yml binds ./config and
# ./logs, which the server rewrites in place).
ARG UID=10001
ARG GID=10001
RUN addgroup -g "${GID}" basis \
    && adduser -D -u "${UID}" -G basis -h /app -s /sbin/nologin basis

WORKDIR /app

COPY --from=build /out/basis-server-console /usr/local/bin/basis-server-console
COPY --chmod=0755 docker-entrypoint.sh /usr/local/bin/docker-entrypoint.sh

# Everything the server reads or writes lives under /app because the entrypoint
# passes --base-dir /app: config/, logs/, initialresources/ and defaultlibrary/.
# config/config.xml is deliberately not baked in; the server writes a
# schema-correct default on first boot, so the image never ships a stale config
# that a newer release would have to migrate.
RUN set -eux; \
    mkdir -p /app/config /app/logs /app/initialresources /app/defaultlibrary; \
    chown -R "${UID}:${GID}" /app

# Drop privileges for real. The server only writes under /app, and the entrypoint execs
# it as PID 1, so there is no setuid helper involved.
USER ${UID}:${GID}

# Only genuine *behavioural* defaults belong here. Server settings such as SetPort and
# Password are deliberately NOT set as ENV, for two reasons:
#   1. config.xml already defaults them, so the image gains nothing and would only
#      duplicate a schema that the release pipeline owns.
#   2. A non-empty ENV value is indistinguishable from one the operator set, which would
#      stop docker-entrypoint.sh from honouring the legacy `Port` name. Shipping
#      SetPort=4296 here silently pinned every deployment to 4296 even when a Basis
#      compose file asked for something else.
#
# The health variables are the exception: HEALTHCHECK below is evaluated by the daemon
# in a shell that never runs the entrypoint, so it cannot see anything the entrypoint
# exports and does need real values.
#
# EnableConsole=false because the headless image has no TTY. Set it to true to get the
# interactive /players, /status, /config, /perm commands (run with `docker run -it`).
# BASIS_STATUS_INTERVAL_SECS makes the server print a status line every 60s so that
# `docker logs` is useful without attaching to a console.
ENV BASIS_BASE_DIR=/app \
    EnableConsole=false \
    HealthCheckHost=127.0.0.1 \
    HealthCheckPort=10666 \
    HealthPath=/health \
    BASIS_STATUS_INTERVAL_SECS=60

# Gameplay traffic is UDP. 10666/tcp serves the JSON /health endpoint.
EXPOSE 4296/udp
EXPOSE 10666/tcp

# Shell form so the ENV values above are expanded when the check runs. These are always
# set by the ENV block, so the URL cannot be malformed.
HEALTHCHECK --interval=30s --timeout=5s --start-period=20s --retries=3 \
    CMD curl -fsS "http://${HealthCheckHost}:${HealthCheckPort}${HealthPath}" || exit 1

ENTRYPOINT ["/usr/local/bin/docker-entrypoint.sh"]
