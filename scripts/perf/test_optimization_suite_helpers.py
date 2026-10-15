"""Regression coverage for Linux optimization suite workload gates."""

import json
import importlib.util
from pathlib import Path
import tempfile
import unittest

from optimization_suite_helpers import summarize_profile_workload

SUMMARY_PATH = Path(__file__).with_name('summarize_windows_avatar.py')
SPEC = importlib.util.spec_from_file_location('summarize_windows_avatar', SUMMARY_PATH)
SUMMARIZER = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(SUMMARIZER)


def make_capture(path, valid):
    """Write a tiny complete capture that exercises the real workload gates."""
    (path / 'workload.json').write_text(json.dumps({
        'server_ip': '127.0.0.1', 'server_kind': 'rust', 'clients': 1,
        'client_exit_code': 0, 'server_exit_code': 0,
    }))
    observer = {
        'window_started': 'true' if valid else 'false', 'window_ms': '60000',
        'near_peers': '0', 'applied_full_items': '100', 'applied_delta_items': '0',
        'gap_p50_ms': '10', 'gap_p95_ms': '20',
        'missing_expected_peers': '0', 'stale_peers_500ms': '0', 'decode_errors': '0',
        'unapplied_deltas': '0', 'malformed_items': '0', 'non_newer_sequences': '0',
        'discontinuities': '0', 'sequence_ambiguities': '0', 'sequence_resyncs': '0',
        'sequence_order_unknown_gaps': '0',
    }
    (path / 'observer.csv').write_text(''.join(f'{key},{value}\n' for key, value in observer.items()))
    with (path / 'process-metrics.csv').open('w') as stream:
        stream.write('unix_seconds,server_cpu_seconds,server_kernel_cpu_seconds,server_user_cpu_seconds,client_cpu_seconds,client_kernel_cpu_seconds,client_user_cpu_seconds,server_working_set_bytes,server_commit_charge_bytes,client_working_set_bytes,client_commit_charge_bytes,players_online,active_states\n')
        for index in range(31):
            elapsed = index * 2
            stream.write(f'{elapsed},{elapsed},{elapsed/2},{elapsed/2},{elapsed/2},{elapsed/4},{elapsed/4},1048576,2097152,1048576,2097152,1,1\n')
    health = []
    for seconds in (0, 60):
        health.append({'unix_seconds': seconds, 'extended': {
            'rawUdp': {'bytesOut': seconds * 1000, 'packetsOut': seconds * 10, 'wouldBlock': 0},
            'avatarSync': {'inboundUpdates': seconds * 10,
                           'outboundLogicalAvatarSends': seconds * 20,
                           'receiverSlices': 1},
            'reliable': {'retransmits': 0}, 'appMessages': {'protocolErrors': 0},
        }})
    (path / 'health.jsonl').write_text(''.join(json.dumps(row) + '\n' for row in health))
    (path / 'server.log').write_text('')
    (path / 'observer.sender.csv').write_text(
        'connected_at_end,socket_sent_full,socket_sent_delta,send_errors\ntrue,3000,0,0\n')


class ProfileWorkloadGateTests(unittest.TestCase):
    def test_invalid_profile_workload_is_summarized_and_fails_suite(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            capture, output = root / 'capture', root / 'output'
            capture.mkdir()
            output.mkdir()
            make_capture(capture, valid=False)
            failures = []

            summarize_profile_workload(capture, output, SUMMARIZER, failures)

            self.assertEqual(failures, ['profile-validation'])
            result = json.loads((output / 'avatar-1500.summary.json').read_text())
            self.assertFalse(result['valid'])
            self.assertFalse(result['checks']['window_started'])

    def test_valid_profile_workload_passes_real_summary_gates(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            capture, output = root / 'capture', root / 'output'
            capture.mkdir()
            output.mkdir()
            make_capture(capture, valid=True)
            failures = []

            summarize_profile_workload(capture, output, SUMMARIZER, failures)

            self.assertEqual(failures, [])
            result = json.loads((output / 'avatar-1500.summary.json').read_text())
            self.assertTrue(result['valid'], result['checks'])

    def test_missing_profile_workload_fails_suite(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            capture, output = root / 'capture', root / 'output'
            capture.mkdir()
            output.mkdir()
            failures = []

            summarize_profile_workload(capture, output, SUMMARIZER, failures)

            self.assertEqual(failures, ['profile-missing-workload'])
            self.assertFalse((output / 'avatar-1500.summary.json').exists())

    def test_malformed_workload_becomes_validation_failure(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            capture, output = root / 'capture', root / 'output'
            capture.mkdir()
            output.mkdir()
            (capture / 'workload.json').write_text('{broken')
            failures = []

            summarize_profile_workload(capture, output, SUMMARIZER, failures)

            self.assertEqual(failures, ['profile-validation'])
            self.assertTrue((output / 'avatar-1500-validation-error.txt').is_file())
            self.assertFalse((output / 'avatar-1500.summary.json').exists())


if __name__ == '__main__':
    unittest.main()
