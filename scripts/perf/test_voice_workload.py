"""Regression checks for the receive population used by the voice suite."""
import importlib.util
import pathlib
import unittest

SPEC = importlib.util.spec_from_file_location(
    "voice_workload", pathlib.Path(__file__).with_name("run-windows-voice-workload.py"))
RUNNER = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(RUNNER)
AVATAR_SPEC = importlib.util.spec_from_file_location(
    "avatar_workload", pathlib.Path(__file__).with_name("run-windows-avatar-workload.py"))
AVATAR = importlib.util.module_from_spec(AVATAR_SPEC)
AVATAR_SPEC.loader.exec_module(AVATAR)


class ReceivePopulationTests(unittest.TestCase):
    def command(self, platform, speakers):
        return RUNNER.workload_command(
            "rtk", pathlib.Path("binaries"), pathlib.Path("capture"), pathlib.Path("audio"),
            "measurement", 1000, speakers, 45, 120, platform)

    def test_linux_suite_disables_both_voice_dropping_receive_paths(self):
        for speakers in (0, 10, 100):
            with self.subTest(speakers=speakers):
                command = self.command("posix", speakers)
                self.assertIn("--client-voice-all-clients", command)
                self.assertIn("--no-client-shared-receive", command)
                self.assertEqual(command[command.index("--client") + 1], str(pathlib.Path("binaries") / "client"))

    def test_windows_uses_same_population_without_linux_fallback(self):
        for speakers in (0, 10, 100):
            with self.subTest(speakers=speakers):
                command = self.command("nt", speakers)
                self.assertIn("--client-voice-all-clients", command)
                self.assertNotIn("--no-client-shared-receive", command)
                self.assertEqual(command[command.index("--client") + 1], str(pathlib.Path("binaries") / "client.exe"))
                self.assertEqual("--voice-audio-folder" in command, speakers > 0)

    def test_standalone_linux_voice_forces_unfiltered_receive_over_inherited_settings(self):
        env = {"BASIS_CLIENT_SHARED_RECEIVE": "1", "BASIS_CLIENT_LOAD_SINK_FILTER": "1"}
        env.update(AVATAR.client_receive_overrides(True, False, False, "posix"))
        self.assertEqual(env["BASIS_CLIENT_SHARED_RECEIVE"], "0")
        self.assertEqual(env["BASIS_CLIENT_LOAD_SINK_FILTER"], "0")

    def test_avatar_only_keeps_explicit_receive_options(self):
        self.assertEqual(AVATAR.client_receive_overrides(False, False, False, "posix"), {})
        self.assertEqual(AVATAR.client_receive_overrides(False, True, True, "posix"), {
            "BASIS_CLIENT_SHARED_RECEIVE": "0", "BASIS_CLIENT_LOAD_SINK_FILTER": "0"})


if __name__ == "__main__":
    unittest.main()
