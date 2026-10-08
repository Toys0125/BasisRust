import importlib.util
import csv
import json
import pathlib
import tempfile
import unittest
from unittest.mock import patch


ANALYZER_PATH = pathlib.Path(__file__).with_name("analyze-windows-voice.py")
spec = importlib.util.spec_from_file_location("analyze_windows_voice", ANALYZER_PATH)
analyzer = importlib.util.module_from_spec(spec)
spec.loader.exec_module(analyzer)


class AnalyzeReceiptMetricsTests(unittest.TestCase):
    def setUp(self):
        self.voice = {"sent_packets": 100, "received_packets": 100}

    def test_new_capture_uses_all_client_unique_counts_and_checks_ordering(self):
        rows = [
            {"received_unique_packets": "99", "duplicate_packets": "1", "reordered_packets": "0",
             "wrapped_sequence_packets": "2", "ambiguous_sequence_packets": "0", "unique_count_available": "true"},
            {"received_unique_packets": "0", "duplicate_packets": "0", "reordered_packets": "0",
             "wrapped_sequence_packets": "0", "ambiguous_sequence_packets": "0", "unique_count_available": "true"},
        ]
        metrics, checks, ratio = analyzer.analyze_receipt_metrics(self.voice, rows, clients=2)
        self.assertEqual(metrics["voice_raw_receipt_to_expected_fanout_ratio"], 1.0)
        self.assertEqual(metrics["voice_unique_receipt_to_expected_fanout_ratio"], 0.99)
        self.assertEqual(metrics["voice_wrapped_sequence_packets"], 2)
        self.assertEqual(ratio, 0.99)
        self.assertTrue(checks["voice_unique_receipt_metrics_available"])
        self.assertFalse(checks["zero_voice_duplicate_packets"])
        self.assertTrue(checks["zero_voice_reordered_packets"])
        capacity = analyzer.voice_capacity_checks(1.0, ratio, 20, checks)
        self.assertFalse(capacity["zero_voice_duplicates"])
        self.assertFalse(all(capacity.values()))

    def test_new_capture_ambiguity_disables_unique_ratio(self):
        rows = [
            {"received_unique_packets": "99", "duplicate_packets": "0", "reordered_packets": "0",
             "wrapped_sequence_packets": "0", "ambiguous_sequence_packets": "1", "unique_count_available": "false"},
            {"received_unique_packets": "0", "duplicate_packets": "0", "reordered_packets": "0",
             "wrapped_sequence_packets": "0", "ambiguous_sequence_packets": "0", "unique_count_available": "true"},
        ]
        metrics, checks, ratio = analyzer.analyze_receipt_metrics(self.voice, rows, clients=2)
        self.assertEqual(metrics["voice_raw_receipt_to_expected_fanout_ratio"], 1.0)
        self.assertIsNone(metrics["voice_unique_receipt_to_expected_fanout_ratio"])
        self.assertIsNone(ratio)
        self.assertFalse(checks["voice_unique_receipt_metrics_available"])
        self.assertFalse(checks["zero_voice_ambiguous_sequences"])

    def test_old_capture_keeps_raw_metric_but_cannot_pass_unique_screen(self):
        old_rows = [{"received_packets": "100"}, {"received_packets": "0"}]
        metrics, checks, ratio = analyzer.analyze_receipt_metrics(self.voice, old_rows, clients=2)
        self.assertEqual(metrics["voice_raw_receipt_to_expected_fanout_ratio"], 1.0)
        self.assertEqual(metrics["voice_receipt_to_expected_fanout_ratio"], 1.0)
        self.assertIsNone(metrics["voice_unique_received_packets"])
        self.assertIsNone(ratio)
        self.assertFalse(checks["voice_unique_receipt_metrics_available"])
        self.assertFalse(checks["zero_voice_duplicate_packets"])
        capacity = analyzer.voice_capacity_checks(1.0, ratio, 20, checks)
        self.assertFalse(capacity["at_least_99pct_unique_expected_fanout_received"])
        self.assertFalse(capacity["unique_receipt_metrics_available"])

    def test_incomplete_new_capture_cannot_claim_all_client_availability(self):
        rows = [{"received_unique_packets": "100", "duplicate_packets": "0", "reordered_packets": "0",
                 "wrapped_sequence_packets": "0", "ambiguous_sequence_packets": "0", "unique_count_available": "true"}]
        metrics, checks, ratio = analyzer.analyze_receipt_metrics(self.voice, rows, clients=2)
        self.assertEqual(metrics["voice_unique_received_packets"], 100)
        self.assertIsNone(ratio)
        self.assertFalse(checks["voice_unique_receipt_metrics_available"])

    def test_workload_validation_failure_vetoes_capacity_pass(self):
        rows = [
            {"received_unique_packets": "99", "duplicate_packets": "0", "reordered_packets": "0",
             "wrapped_sequence_packets": "2", "ambiguous_sequence_packets": "0", "unique_count_available": "true"},
            {"received_unique_packets": "0", "duplicate_packets": "0", "reordered_packets": "0",
             "wrapped_sequence_packets": "0", "ambiguous_sequence_packets": "0", "unique_count_available": "true"},
        ]
        _, checks, ratio = analyzer.analyze_receipt_metrics(self.voice, rows, clients=2)
        capacity = analyzer.voice_capacity_checks(1.0, ratio, 20, checks, workload_validation_passed=False)
        self.assertTrue(capacity["at_least_99pct_unique_expected_fanout_received"])
        self.assertFalse(capacity["workload_validation_passed"])
        self.assertFalse(all(capacity.values()))

    def test_quality_failures_do_not_invalidate_well_formed_capture(self):
        with tempfile.TemporaryDirectory() as temporary:
            capture = pathlib.Path(temporary)
            (capture / "workload.json").write_text(json.dumps({
                "clients": 2, "voice": True, "voice_speaker_percent": 100,
                "measurement_window_seconds": 1, "client_exit_code": 0, "server_exit_code": 0,
            }))
            self._csv(capture / "observer.csv", [
                ["window_started", "true"], ["window_ms", "1000"], ["near_peers", "1"],
                ["missing_expected_peers", "0"], ["stale_peers_500ms", "0"], ["decode_errors", "0"],
                ["unapplied_deltas", "0"], ["malformed_items", "0"], ["non_newer_sequences", "0"],
                ["applied_full_items", "1"], ["applied_delta_items", "1"], ["gap_p50_ms", "20"],
                ["gap_p95_ms", "20"],
            ])
            process_fields = ["unix_seconds", "players_online", "active_states"]
            for label in ("server", "client"):
                process_fields += [f"{label}_{kind}_seconds" for kind in ("cpu", "kernel_cpu", "user_cpu")]
                process_fields += [f"{label}_{kind}_bytes" for kind in ("working_set", "commit_charge")]
            self._csv(capture / "process-metrics.csv", [
                process_fields,
                ["0", "2", "2", "0", "0", "0", "0", "100", "200", "0", "0", "0", "0"],
                ["1", "2", "2", "1", "0.5", "0.5", "1", "100", "200", "1", "0.5", "0.5", "1"],
            ])
            health_record = {
                "unix_seconds": 0,
                "extended": {
                    "rawUdp": {"bytesOut": 0, "bytesIn": 0, "packetsOut": 0, "packetsIn": 0, "wouldBlock": 0},
                    "appMessages": {"protocolErrors": 0},
                },
                "transport": {"nonReliableDroppedDatagrams": 0},
            }
            health_end = json.loads(json.dumps(health_record))
            health_end["unix_seconds"] = 1
            (capture / "health.jsonl").write_text(json.dumps(health_record) + "\n" + json.dumps(health_end) + "\n")
            self._csv(capture / "observer.sender.csv", [
                ["connected_at_end", "send_errors", "socket_sent_full", "socket_sent_delta"],
                ["true", "0", "1", "0"], ["true", "0", "1", "0"],
            ])
            self._csv(capture / "voice.csv", [
                ["received_packets", "sent_packets", "received_unique_packets", "duplicate_packets", "reordered_packets",
                 "wrapped_sequence_packets", "ambiguous_sequence_packets", "unique_count_available"],
                ["50", "50", "50", "0", "1", "0", "0", "true"],
                ["50", "50", "49", "0", "0", "0", "1", "false"],
            ])
            self._csv(capture / "voice.summary.csv", [
                ["metric", "value"], ["window_ms", "1000"], ["sent_packets", "100"], ["received_packets", "100"],
                ["sent_opus_bytes", "1000"], ["received_opus_bytes", "1000"], ["send_errors", "0"],
                ["skipped_packets", "0"], ["malformed_packets", "0"], ["self_received_packets", "0"],
            ])
            self._csv(capture / "voice.peers.csv", [
                ["remote_peer_id", "packets", "forward_missing_mod256", "duplicates", "reordered_or_ambiguous", "max_gap_ms"],
                ["1", "100", "0", "0", "0", "20"],
            ])
            self._csv(capture / "voice.gaps.csv", [["gap_floor_ms", "count"], ["20", "100"]])

            with patch.object(analyzer, "validate_samples", return_value={"valid": True}):
                result = analyzer.summarize(capture, capture)

        self.assertTrue(result["valid"])
        self.assertTrue(result["checks"]["all_clients_unique_receipt_records_complete"])
        self.assertTrue(result["capacity_checks"]["workload_validation_passed"])
        self.assertFalse(result["capacity_pass"])
        self.assertFalse(result["voice_delivery_checks"]["zero_voice_reordered_packets"])
        self.assertFalse(result["voice_delivery_checks"]["zero_voice_ambiguous_sequences"])
        self.assertFalse(result["voice_delivery_checks"]["voice_unique_receipt_metrics_available"])

    @staticmethod
    def _csv(path, rows):
        with path.open("w", newline="", encoding="utf-8") as stream:
            csv.writer(stream).writerows(rows)


if __name__ == "__main__":
    unittest.main()
