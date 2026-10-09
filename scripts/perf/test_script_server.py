"""Focused checks for shared script benchmark server launch helpers."""

import pathlib
import tempfile
import unittest
import xml.etree.ElementTree as ET

import script_server


FIXTURE = pathlib.Path(__file__).resolve().parents[2] / "docs/performance/fixtures/avatar-1500-server.xml"


class ScriptServerTests(unittest.TestCase):
    def test_rust_launch_uses_isolated_base_dir_and_ports(self):
        with tempfile.TemporaryDirectory() as temp:
            root = pathlib.Path(temp)
            binary = root / "basis-server"
            binary.write_bytes(b"rust-binary")
            command, config, metadata = script_server.prepare_server(
                "rust", binary, root / "run", FIXTURE, 1234, 2345)
            self.assertEqual(command[command.index("--port") + 1], "1234")
            self.assertEqual(command[command.index("--health-port") + 1], "2345")
            self.assertTrue(config.is_file())
            self.assertEqual(metadata["kind"], "rust")

    def test_csharp_copies_runtime_translates_config_and_sidecar(self):
        with tempfile.TemporaryDirectory() as temp:
            root = pathlib.Path(temp)
            publish = root / "publish"
            publish.mkdir()
            apphost = publish / "BasisNetworkConsole"
            apphost.write_bytes(b"apphost")
            (publish / "BasisNetworkConsole.dll").write_bytes(b"assembly")
            (publish / "BasisNetworkConsole.runtimeconfig.json").write_text("{}")
            (publish / "BasisNetworkConsole.deps.json").write_text("{}")
            (publish / "BasisNetworkServer.dll").write_bytes(b"server assembly")
            (publish / "BasisNetworkCore.dll").write_bytes(b"core assembly")
            (publish / "config").mkdir()
            (publish / "config" / "permissions.xml").write_text("stale permissions")
            (publish / "logs").mkdir()
            (publish / "logs" / "old.log").write_text("prior run")
            (publish / "defaultlibrary").mkdir()
            (publish / "defaultlibrary" / "published.dat").write_text("published resource")
            (publish / "initialresources").mkdir()
            (publish / "initialresources" / "published.dat").write_text("published resource")
            runtime = root / "run"
            command, config, metadata = script_server.prepare_server(
                "csharp", apphost, runtime, FIXTURE, 1234, 2345)
            self.assertEqual(command, [str(runtime / apphost.name)])
            self.assertEqual((runtime / "BasisNetworkConsole.dll").read_bytes(), b"assembly")
            self.assertFalse((runtime / "config" / "permissions.xml").exists())
            self.assertFalse((runtime / "logs" / "old.log").exists())
            self.assertTrue((runtime / "defaultlibrary" / "published.dat").is_file())
            self.assertTrue((runtime / "initialresources" / "published.dat").is_file())
            values = {node.tag: node.text for node in ET.parse(config).getroot()}
            self.assertEqual(values["SetPort"], "1234")
            self.assertEqual(values["HealthCheckPort"], "2345")
            self.assertEqual(values["HealthCheckHost"], "127.0.0.1")
            transport = ET.parse(metadata["transport_config"]).getroot()
            self.assertIsNotNone(transport.find("IPv6Enabled"))
            self.assertEqual(metadata["kind"], "csharp")
            self.assertEqual(set(metadata["runtime_file_sha256"]), {
                "BasisNetworkConsole.dll", "BasisNetworkConsole.deps.json",
                "BasisNetworkConsole.runtimeconfig.json", "BasisNetworkServer.dll",
                "BasisNetworkCore.dll"})

    def test_readiness_and_population_require_authenticated_join_logs_for_csharp(self):
        csharp = {"ready": True, "listening": True, "visitors": 3}
        rust = {"status": "healthy", "players_online": 3}
        self.assertTrue(script_server.ready(csharp, "csharp"))
        self.assertFalse(script_server.ready({"ready": True, "visitors": 3}, "csharp"))
        self.assertTrue(script_server.ready(rust, "rust"))
        log = "client 1 connected as remote peer\nclient 2 connected as remote peer\n"
        self.assertEqual(script_server.population(csharp, "csharp", log), 2)
        self.assertEqual(script_server.population(rust, "rust"), 3)


if __name__ == "__main__":
    unittest.main()
