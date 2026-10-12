"""Native macOS process counter parsing and sampler selection."""
import unittest
from unittest.mock import patch
from avatar_benchmark import macos_metrics, native_sampler


class MacCounters(unittest.TestCase):
    def test_bsd_time_and_rss_units(self):
        for clock, seconds in [('0:03.25', 3.25), ('12:03.25', 723.25),
                               ('1:12:03.25', 4323.25), ('2-01:12:03.25', 177123.25)]:
            with patch('avatar_benchmark.subprocess.check_output', return_value=clock + ' 4096\n'):
                self.assertEqual(macos_metrics(42), {'cpu_seconds': seconds, 'rss_bytes': 4194304})

    def test_missing_process_and_invalid_counters_fail(self):
        for value in ['', 'garbage 4096', '0:00.25 -1']:
            with patch('avatar_benchmark.subprocess.check_output', return_value=value):
                with self.assertRaises((RuntimeError, ValueError)):
                    macos_metrics(42)

    def test_darwin_uses_native_sampler(self):
        with patch('avatar_benchmark.platform.system', return_value='Darwin'):
            self.assertIs(native_sampler(), macos_metrics)
