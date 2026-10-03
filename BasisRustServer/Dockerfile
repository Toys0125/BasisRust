# syntax=docker/dockerfile:1.7
#
# Canonical BasisRust server image.
#
# This is the single source of truth for the published image
# `ghcr.io/toys0125/basisrust-server`. `.github/workflows/build-docker-server.yml`
# builds it from the prebuilt binaries attached to a GitHub release by passing
# `ARTIFACT_DIR=<dir>` naming an unpacked release payload inside the build context.
# Running `docker build` directly from a source checkout with no `ARTIFACT_DIR`
# compiles the server in the Rust stage instead, which is what keeps
# `docker compose build` in docker-compose-server.yml working.
#
# Why the runtime base is ubuntu:24.04 and must not be downgraded to a slimmer base:
# the release binaries are built by .github/workflows/build.yml on ubuntu-24.04 and are
# dynamically linked (PT_INTERP=/lib64/ld-linux-x86-64.so.2; DT_NEEDED=libc.so.6, libm.so.6,
# libgcc_s.so.1) against GLIBC_2.39. An older base such as debian:bookworm-slim (GLIBC_2.36)
# makes the dynamic loader report
#   libc.so.6: weak version `GLIBC_2.39' not found (required by .../basis-server-console)
# at every start. That particular requirement happens to be weak-versioned so the process
# still boots, which makes it a nasty failure to notice; any strong-versioned reference
# would abort instead. Only the glibc version floor matters here, not the dependency count.

ARG RUST_IMAGE=rust:1-bookworm
ARG RUNTIME_IMAGE=ubuntu:24.04

FROM ${RUST_IMAGE} AS build

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
    # The release binaries are built on ubuntu-24.04, so GLIBC_2.39 is the expected
    # floor. A source build can legitimately differ in either direction, so only report
    # a mismatch rather than asserting one is worse than the other.
    glibc_required="$(readelf --version-info /out/basis-server-console \
        | grep -o 'GLIBC_[0-9.]*' | sed 's/GLIBC_//' | sort -V -u -r | head -n1)"; \
    if [ "${glibc_required:-none}" != "2.39" ]; then \
        echo "::warning::binary needs GLIBC_${glibc_required:-none} but the runtime base provides 2.39; confirm RUNTIME_IMAGE still satisfies it"; \
    fi

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

# libgcc-s1 backs the libgcc_s.so.1 the binary needs; curl backs HEALTHCHECK.
# ca-certificates is there for deployments that enable outgoing TLS.
RUN set -eux; \
    apt-get update; \
    DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends \
        ca-certificates \
        curl \
        libgcc-s1; \
    rm -rf /var/lib/apt/lists/*

# Unprivileged runtime user. UID/GID are fixed and exposed as build args so a host
# volume can be chowned to match (docker-compose-server.yml binds ./config and
# ./logs, which the server rewrites in place).
ARG UID=10001
ARG GID=10001
RUN set -eux; \
    groupadd --gid "${GID}" basis; \
    useradd --uid "${UID}" --gid "${GID}" --home-dir /app --no-create-home \
        --shell /usr/sbin/nologin basis

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
