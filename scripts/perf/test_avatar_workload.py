"""Avatar benchmark gates require actual application and sustained senders."""
import csv
import importlib.util
import pathlib
import tempfile
import unittest

spec = importlib.util.spec_from_file_location('avatar_workload', pathlib.Path(__file__).with_name('run-avatar-workload.py'))
runner = importlib.util.module_from_spec(spec)
spec.loader.exec_module(runner)


class AvatarCaptureTests(unittest.TestCase):
    def capture(self, path, corrupt=False, stalled=False):
        observer = {'window_started': 'true', 'window_ms': '10000', 'near_peers': '1',
                    'missing_expected_peers': '0', 'stale_peers_500ms': '0',
                    'decode_errors': '1' if corrupt else '0', 'unapplied_deltas': '0',
                    'malformed_items': '0', 'applied_full_items': '100', 'applied_delta_items': '0'}
        with (path / 'observer.csv').open('w', newline='') as file:
            writer = csv.writer(file); writer.writerow(['metric', 'value']); writer.writerows(observer.items())
        with (path / 'observer.sender.csv').open('w', newline='') as file:
            writer = csv.DictWriter(file, fieldnames=['socket_sent_full', 'socket_sent_delta', 'send_errors', 'connected_at_end'])
            writer.writeheader()
            writer.writerows([{'socket_sent_full': 0 if stalled else 500, 'socket_sent_delta': 0,
                               'send_errors': 0, 'connected_at_end': 'true'}] * 2)
        return [{'elapsed_seconds': 10, 'server_cpu_seconds': 5, 'client_cpu_seconds': 2,
                 'server_rss_bytes': 1024, 'client_rss_bytes': 2048}]

    def test_valid_capture_reports_application_and_cpu(self):
        with tempfile.TemporaryDirectory() as directory:
            path = pathlib.Path(directory)
            result = runner.summarize_capture(path, 2, 10, self.capture(path), 0, 0)
            self.assertTrue(result['valid'])
            self.assertEqual(result['observer_items_per_second'], 10)
            self.assertEqual(result['processes']['server']['cpu_core_equivalents'], .5)

    def test_corruption_and_stalled_senders_fail(self):
        for corrupt, stalled in [(True, False), (False, True)]:
            with tempfile.TemporaryDirectory() as directory:
                path = pathlib.Path(directory)
                result = runner.summarize_capture(path, 2, 10, self.capture(path, corrupt, stalled), 0, 0)
                self.assertFalse(result['valid'])


if __name__ == '__main__':
    unittest.main()
