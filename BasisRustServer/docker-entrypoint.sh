#!/bin/sh
# Entrypoint for the BasisRust server image.
#
# The server is configured entirely through environment variables named after the
# config.xml fields (see ServerConfig::process_environment_overrides), so no argument
# plumbing is needed here. The only work this script does is translate the variable
# names used by upstream Basis (Unity) headless compose files into the names this
# server understands, so an existing Basis deployment can swap the image and keep its
# environment block. Native variables always win; the legacy ones are only used as a
# fallback.

set -eu

# Upstream Basis uses `Port`; this server uses `SetPort`.
if [ -n "${Port:-}" ] && [ -z "${SetPort:-}" ]; then
    SetPort="$Port"
    export SetPort
fi

# Upstream Basis uses `Ip`; this server calls the same setting `IPv4Address`.
#
# Note this only changes the bind address when OverrideAutoDiscoveryOfIpv is also enabled;
# by default the server binds the wildcard address, which is what a container needs in
# order to be reachable from outside. The mapping is kept so that a Basis deployment
# carrying the variable alongside that flag behaves the same as it did before.
if [ -n "${Ip:-}" ] && [ -z "${IPv4Address:-}" ]; then
    IPv4Address="$Ip"
    export IPv4Address
fi

# --base-dir decides where config/, logs/, initialresources/ and defaultlibrary/ live.
# It must be writable, because the server rewrites config/config.xml in place when it
# migrates the config version.
BASIS_BASE_DIR="${BASIS_BASE_DIR:-/app}"
mkdir -p "$BASIS_BASE_DIR/config"

# --config is relative to --base-dir. The file does not need to exist: the server writes
# a schema-correct default on first boot.
#
# `exec` hands PID 1 and all signals (SIGTERM in particular, which the server treats as
# a shutdown request) straight to the server. There are no child processes to reap, so
# no init shim is required.
exec /usr/local/bin/basis-server-console \
    --base-dir "$BASIS_BASE_DIR" \
    --config config/config.xml \
    "$@"
