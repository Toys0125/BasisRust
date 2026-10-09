"""Shared server launch and readiness helpers for script-data benchmarks."""

from __future__ import annotations

import hashlib
import pathlib
import re
import shutil
import xml.etree.ElementTree as ET


TRANSPORT_FIELDS = (
    "UseNativeSockets", "NatPunchEnabled", "PingInterval", "DisconnectTimeout",
    "SimulatePacketLoss", "SimulateLatency", "SimulationPacketLossChance",
    "SimulationMinLatency", "SimulationMaxLatency", "ReconnectDelay",
    "MaxConnectAttempts", "ReuseAddresss", "DontRoute", "Ipv6Enabled",
    "MtuOverride", "MtuDiscovery", "DisconnectOnUnreachable", "AllowPeerAddressChange",
)


def _sha256(path: pathlib.Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def prepare_server(kind, binary, base_dir, fixture, port, health_port):
    """Prepare an isolated Rust or C# server and return (command, config, metadata)."""
    kind = str(kind).lower()
    binary = pathlib.Path(binary).resolve()
    base_dir = pathlib.Path(base_dir).resolve()
    fixture = pathlib.Path(fixture).resolve()
    if kind not in ("rust", "csharp"):
        raise ValueError("kind must be 'rust' or 'csharp'")
    if not binary.is_file() or not fixture.is_file():
        raise FileNotFoundError(binary if not binary.is_file() else fixture)

    metadata = {"kind": kind, "binary": str(binary), "binary_sha256": _sha256(binary)}
    if kind == "rust":
        config = base_dir / "config" / "config.xml"
        config.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(fixture, config)
        command = [str(binary), "--base-dir", str(base_dir), "--port", str(port),
                   "--no-console", "--health-host", "127.0.0.1", "--health-port", str(health_port)]
        metadata["config_sha256"] = _sha256(config)
        return command, config, metadata

    # The C# console resolves config/config.xml beside its apphost and expects the
    # published runtime's assemblies and native libraries beside that apphost.
    shutil.copytree(binary.parent, base_dir, dirs_exist_ok=True,
                    ignore=shutil.ignore_patterns("config", "logs"))
    # These files hold prior-run configuration, moderation state and logs. Keep
    # published resources, but start each benchmark with only its own config.
    shutil.rmtree(base_dir / "config", ignore_errors=True)
    shutil.rmtree(base_dir / "logs", ignore_errors=True)
    config = base_dir / "config" / "config.xml"
    config.parent.mkdir(parents=True, exist_ok=True)
    shutil.copyfile(fixture, config)
    tree = ET.parse(config)
    root = tree.getroot()
    restriction = root.find("BasisUserRestrictionMode")
    if restriction is not None and restriction.text == "None":
        restriction.text = "Normal"
    for name, value in (("SetPort", str(port)), ("HealthCheckHost", "127.0.0.1"),
                        ("HealthCheckPort", str(health_port))):
        node = root.find(name)
        if node is None:
            node = ET.SubElement(root, name)
        node.text = value
    tree.write(config, encoding="utf-8", xml_declaration=True)

    # Current C# builds load LiteNetLib options from a sidecar. Translate only
    # fields present in the shared fixture; the schema owns the remaining defaults.
    transport = ET.Element("LNLTransportConfig")
    for name in TRANSPORT_FIELDS:
        node = root.find(name)
        if node is not None:
            output_name = "IPv6Enabled" if name == "Ipv6Enabled" else name
            ET.SubElement(transport, output_name).text = node.text
    transport_path = config.parent / "transports" / "litenetlib.xml"
    transport_path.parent.mkdir(parents=True, exist_ok=True)
    ET.ElementTree(transport).write(transport_path, encoding="utf-8", xml_declaration=True)

    launch = base_dir / binary.name
    runtime_hashes = {}
    for name in ("BasisNetworkConsole.dll", "BasisNetworkConsole.deps.json",
                 "BasisNetworkConsole.runtimeconfig.json", "BasisNetworkServer.dll",
                 "BasisNetworkCore.dll"):
        path = base_dir / name
        if path.is_file():
            runtime_hashes[name] = _sha256(path)
    metadata.update({"config_sha256": _sha256(config), "transport_config": str(transport_path),
                     "transport_config_sha256": _sha256(transport_path),
                     "runtimeconfig": str(base_dir / (binary.stem + ".runtimeconfig.json")),
                     "runtime_file_sha256": runtime_hashes})
    return [str(launch)], config, metadata


def ready(status, kind):
    """Return whether the server health response says the listener is ready."""
    if kind == "csharp":
        return status.get("ready") is True and status.get("listening") is True and "visitors" in status
    return status.get("status") == "healthy"


def population(status, kind, client_log_text=""):
    """Return a conservative authenticated population estimate for readiness."""
    if kind == "csharp":
        visitors = status.get("visitors")
        if not isinstance(visitors, int):
            return 0
        joins = set(re.findall(r"client (\d+) connected as remote peer", client_log_text or ""))
        return min(visitors, len(joins))
    players = status.get("players_online", 0)
    return players if isinstance(players, int) else 0
