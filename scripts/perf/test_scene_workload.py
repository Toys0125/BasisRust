"""Validity checks must reject benchmarks that appear faster by dropping work."""
import importlib.util
import pathlib
import unittest

spec = importlib.util.spec_from_file_location('scene_workload', pathlib.Path(__file__).with_name('run-scene-workload.py'))
runner = importlib.util.module_from_spec(spec)
spec.loader.exec_module(runner)


class SceneSummaryTests(unittest.TestCase):
    def metrics(self):
        return {'expected_observer_messages': '100', 'received_messages': '100',
                'window_seconds': '2', 'received_bytes': '12800', 'observed_senders': '2',
                'sent_messages': '150', 'send_errors': '0', 'backpressure_skips': '0', 'malformed_messages': '0'}

    def test_complete_delivery_reports_throughput(self):
        result = runner.summarize(self.metrics(), 3, .95)
        self.assertTrue(result['valid'])
        self.assertEqual(result['observer_messages_per_second'], 50)
        self.assertEqual(result['observer_payload_bytes_per_second'], 6400)

    def test_skipped_sends_invalidate_requested_workload(self):
        self.assertFalse(runner.summarize(self.metrics(), 3, .95, 20)['valid'])
        self.assertTrue(runner.summarize(self.metrics(), 3, .95, 40)['valid'])

    def test_missing_work_or_corruption_invalidates_result(self):
        for key, value in [('received_messages', '90'), ('received_messages', '101'),
                           ('expected_observer_messages', '0'), ('observed_senders', '1'),
                           ('send_errors', '1'), ('backpressure_skips', '1'),
                           ('malformed_messages', '1'), ('window_seconds', '0')]:
            with self.subTest(key=key, value=value):
                metrics = self.metrics(); metrics[key] = value
                self.assertFalse(runner.summarize(metrics, 3, .95)['valid'])


if __name__ == '__main__':
    unittest.main()
