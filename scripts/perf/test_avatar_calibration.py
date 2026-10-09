import argparse
import importlib.util
import io
import math
import pathlib
import unittest
from contextlib import redirect_stderr
from unittest.mock import patch


SCRIPT = pathlib.Path(__file__).with_name("tune-avatar-settings.py")
SPEC = importlib.util.spec_from_file_location("tune_avatar_calibration", SCRIPT)
CLI = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(CLI)


class ClientSelectionTests(unittest.TestCase):
    def test_omitted_clients_resolves_to_fixed_250_default(self):
        args = CLI.parser().parse_args(["--output", "unused"])
        self.assertIsNone(args.clients)
        self.assertFalse(args.auto_calibrate)
        CLI.resolve_client_selection(args)
        self.assertEqual(args.clients, 250)

    def test_explicit_fixed_clients_are_preserved(self):
        for clients in (250, 2000):
            with self.subTest(clients=clients):
                args = CLI.parser().parse_args(["--output", "unused", "--clients", str(clients)])
                CLI.resolve_client_selection(args)
                self.assertEqual(args.clients, clients)
                self.assertFalse(args.auto_calibrate)

    def test_default_workload_does_not_launch_calibration_pilots(self):
        args = CLI.parser().parse_args(["--output", "unused"])
        CLI.resolve_client_selection(args)
        with patch.object(CLI, "run_workload") as run:
            selection = CLI.calibrate_clients(args, {}, {}, pathlib.Path("unused"), 65535)
        run.assert_not_called()
        self.assertEqual(selection["mode"], "fixed_clients")
        self.assertEqual(selection["selected_clients"], 250)
        self.assertEqual(selection["attempts"], [])

    def test_calibration_is_explicit_opt_in(self):
        args = CLI.parser().parse_args(["--output", "unused", "--auto-calibrate", "--network-capacity-mbps", "2500"])
        CLI.resolve_client_selection(args)
        self.assertTrue(args.auto_calibrate)
        self.assertIsNone(args.clients)
        self.assertEqual(args.network_capacity_mbps, 2500)

    def test_explicit_clients_conflict_with_auto_calibration(self):
        with redirect_stderr(io.StringIO()), self.assertRaises(SystemExit):
            CLI.parser().parse_args(["--output", "unused", "--clients", "250", "--auto-calibrate"])
        args = argparse.Namespace(clients=250, auto_calibrate=True)
        with self.assertRaisesRegex(ValueError, "cannot be combined"):
            CLI.resolve_client_selection(args)


class CalibrationRateTests(unittest.TestCase):
    def test_counter_interpolation_uses_recorded_boundaries(self):
        samples = [
            {"monotonic_seconds": 1, "health": {"extended": {"rawUdp": {"bytesOut": 100}}}},
            {"monotonic_seconds": 3, "health": {"extended": {"rawUdp": {"bytesOut": 300}}}},
        ]
        self.assertEqual(CLI.interpolate_health_counter(samples, 2, "rawUdp", "bytesOut"), 200)
        with self.assertRaisesRegex(ValueError, "bracket"):
            CLI.interpolate_health_counter(samples, 4, "rawUdp", "bytesOut")

    def test_rate_uses_recorded_non_ten_second_interval(self):
        self.assertEqual(CLI.calibration_tx_mbps(8_000_000, 100.0, 108.0), 8.0)

    def test_nonpositive_or_nonfinite_interval_is_rejected(self):
        for start, end in ((1.0, 1.0), (2.0, 1.0), (0.0, math.nan), (0.0, math.inf)):
            with self.subTest(start=start, end=end), self.assertRaisesRegex(ValueError, "interval"):
                CLI.calibration_tx_mbps(1_000_000, start, end)

    def test_nonpositive_or_nonfinite_byte_delta_is_rejected(self):
        for byte_delta in (0, -1, math.nan, math.inf):
            with self.subTest(byte_delta=byte_delta), self.assertRaisesRegex(ValueError, "TX bytes"):
                CLI.calibration_tx_mbps(byte_delta, 1.0, 2.0)


if __name__ == "__main__":
    unittest.main()
