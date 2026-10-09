import csv
import json
import pathlib
import tempfile
import unittest
from unittest.mock import patch

from avatar_tuning import interpolate_health_counter, server_udp_metrics


def sample(time, bytes_in, bytes_out):
    return {
        'monotonic_seconds': time,
        'health': {'extended': {'rawUdp': {'bytesIn': bytes_in, 'bytesOut': bytes_out}}},
    }


class ServerTransmitMetricsTests(unittest.TestCase):
    def test_rates_interpolate_at_fixed_window_boundaries(self):
        samples = [sample(0, 100, 200), sample(10, 1_100_100, 2_000_200)]
        metrics = server_udp_metrics(samples, 2, 8, 6, 1000)
        self.assertAlmostEqual(metrics['server_receive_mbps'], 0.88)
        self.assertAlmostEqual(metrics['server_transmit_mbps'], 1.6)
        self.assertAlmostEqual(metrics['server_transmit_capacity_percent'], 0.16)

    def test_counter_interpolation_rejects_invalid_time_and_nonfinite_counter(self):
        with self.assertRaisesRegex(ValueError, 'nonpositive'):
            interpolate_health_counter([sample(1, 1, 1), sample(1, 2, 2)], 1, 'rawUdp', 'bytesOut')
        with self.assertRaisesRegex(ValueError, 'Invalid health counter'):
            interpolate_health_counter([sample(0, 1, float('nan')), sample(2, 2, 3)], 1, 'rawUdp', 'bytesOut')

    def test_window_rejects_interior_counter_reset_even_when_endpoints_recover(self):
        samples = [sample(0, 0, 0), sample(5, 100, 100), sample(10, 0, 0), sample(15, 200, 200)]
        with self.assertRaisesRegex(ValueError, 'counter reset'):
            server_udp_metrics(samples, 2, 13, 11, 1000)

    def test_invalid_window_or_capacity_is_rejected(self):
        samples = [sample(0, 0, 0), sample(2, 10, 10)]
        with self.assertRaisesRegex(ValueError, 'window'):
            server_udp_metrics(samples, 0, 2, 0, 1000)
        with self.assertRaisesRegex(ValueError, 'capacity'):
            server_udp_metrics(samples, 0, 2, 2, 0)

    def test_summarize_reports_rates_and_accepts_matching_capacity_provenance(self):
        with tempfile.TemporaryDirectory() as temporary:
            capture = pathlib.Path(temporary)
            workload = {'clients': 2, 'window_seconds': 6, 'network_capacity_mbps': 1000,
                        'rayon_threads': None, 'tokio_workers': None}
            meta = {'completed': True, 'error': None, 'window_start_monotonic': 2,
                    'window_end_monotonic': 8, 'workload': dict(workload), 'client_exit_code': 0,
                    'server_exit_code': 0, 'started_unix_seconds': 1, 'finished_unix_seconds': 2}
            (capture / 'run.json').write_text(json.dumps(meta))
            (capture / 'server.log').write_text('')
            obs = {'gap_p50_ms': '10', 'gap_p95_ms': '20', 'applied_full_items': '300',
                   'applied_delta_items': '300', 'window_ms': '6000', 'near_peers': '1',
                   'expected_near_peers': '1', 'window_started': 'true', 'near_segment': '0',
                   'near_segment_ms': '6000'}
            zero_keys = ('missing_expected_peers', 'stale_peers_500ms', 'decode_errors',
                         'unapplied_deltas', 'malformed_items', 'non_newer_sequences',
                         'discontinuities', 'sequence_ambiguities', 'sequence_resyncs',
                         'sequence_order_unknown_gaps')
            obs.update({key: '0' for key in zero_keys})
            with (capture / 'observer.csv').open('w', newline='') as stream:
                csv.writer(stream).writerows([['metric', 'value'], *obs.items(), ['observed_channel', 'x']])
            with (capture / 'observer.sender.csv').open('w', newline='') as stream:
                writer = csv.writer(stream)
                writer.writerow(['logical_client_index', 'socket_sent_full', 'socket_sent_delta',
                                 'generated_full', 'generated_delta', 'connected_at_end',
                                 'remote_peer_id_start', 'remote_peer_id_end', 'send_errors'])
                writer.writerow([0, 300, 0, 300, 0, 'true', '1', '1', 0])
                writer.writerow([1, 300, 0, 300, 0, 'true', '2', '2', 0])
            with (capture / 'server-pairs.global.csv').open('w', newline='') as stream:
                writer = csv.writer(stream)
                writer.writerow(['elapsed_ms', 'tick_count', 'tick_micros', 'build_micros',
                                 'flush_micros', 'outbound_logical_avatar_sends', 'inbound_updates'])
                writer.writerow([0, 0, 0, 0, 0, 0, 0])
                writer.writerow([6000, 60, 60000, 30000, 15000, 600, 900])
            samples = [
                {'monotonic_seconds': time, 'server': {'cpu_seconds': time, 'rss_bytes': 1_000_000},
                 'client': {'cpu_seconds': time, 'rss_bytes': 1_000_000},
                 'players_online': 2, 'active_states': 2,
                 'health': {'extended': {
                     'rawUdp': {'bytesIn': byte_in, 'bytesOut': byte_out, 'wouldBlock': 0},
                     'appMessages': {'protocolErrors': 0}, 'reliable': {'retransmits': 0}},
                     'transport': {'nonReliableDroppedDatagrams': 0}}}
                for time, byte_in, byte_out in ((0, 100, 200), (5, 550_100, 1_000_200),
                                                (10, 1_100_100, 2_000_200))]
            (capture / 'samples.jsonl').write_text(''.join(json.dumps(row) + '\n' for row in samples))
            frozen = {'server': {'sha256': 'same'}, 'client': {'sha256': 'same'},
                      'server_config': {'sha256': 'same'}, 'client_config': {'sha256': 'same'}}
            experiment = {'workload': workload, 'frozen': frozen, 'os_name': 'posix'}
            entry = {'name': 'case', 'lanes': 0, 'round': 0,
                     'server_settings': {'BASIS_AVATAR_FLUSH_LANES': '0'}, 'client_settings': {}}
            commands = {'server': ['/server'], 'client': ['/client', '--x', '/client-config'],
                        'server_environment': entry['server_settings'], 'client_environment': {}}
            (capture / 'commands.json').write_text(json.dumps(commands))
            with patch('avatar_tuning.observer_metrics', return_value=obs), \
                 patch('avatar_tuning.sha256', return_value='same'):
                result = __import__('avatar_tuning').summarize(capture, experiment, entry)

        self.assertTrue(result['checks']['fixed_workload_provenance'])
        self.assertTrue(result['checks']['setting_provenance'])
        self.assertAlmostEqual(result['metrics']['server_receive_mbps'], 0.88)
        self.assertAlmostEqual(result['metrics']['server_transmit_mbps'], 1.6)
        self.assertAlmostEqual(result['metrics']['server_transmit_capacity_percent'], 0.16)


if __name__ == '__main__':
    unittest.main()
