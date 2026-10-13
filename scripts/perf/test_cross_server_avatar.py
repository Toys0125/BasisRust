"""C# captures must distinguish missing telemetry from measured zero work."""
import argparse
import json
import pathlib
import tempfile
import unittest
from unittest import mock

from avatar_benchmark import mixed_client_options, population_matches, server_population
from cross_server_avatar import (csharp_health_metrics, saved_config_controls_match,
                                summarize_csharp, translated_config_matches)
from script_server import prepare_server


class ReadinessTests(unittest.TestCase):
    def test_csharp_requires_exact_visitors_and_authenticated_client_indices(self):
        log = '\n'.join(['client 0 connected as remote peer 5',
                         'client 1 connected as remote peer 6',
                         'client 1 connected as remote peer 6'])
        counts = server_population({'visitors': 2}, 'csharp', client_log=log)
        self.assertIsNone(counts['active_states'])
        self.assertTrue(population_matches(counts, 'csharp', 2))
        for visitors, altered in ((3, log), (2, log.replace('client 0', 'client 2')), (True, log)):
            self.assertFalse(population_matches(server_population({'visitors': visitors}, 'csharp', client_log=altered), 'csharp', 2))
        self.assertFalse(population_matches(server_population({'visitors': 2}, 'csharp'), 'csharp', 2))

    def test_rust_still_requires_avatar_state_count(self):
        counts = server_population({'players_online': 2}, 'rust', 'active_states=1\nactive_states=2')
        self.assertTrue(population_matches(counts, 'rust', 2))
        self.assertFalse(population_matches(server_population({'players_online': 2}, 'rust'), 'rust', 2))

    def test_mixed_flags_preserve_workload_and_shared_marker(self):
        args = argparse.Namespace(additional_avatar_bytes=128, scene_data_bytes=128,
                                  scene_data_interval_ms=50, scene_data_reliable=False)
        path = pathlib.Path('/capture')
        flags = mixed_client_options(args, path, path / 'observe-start.marker')
        self.assertEqual(flags, ['--additional-avatar-bytes', '128', '--scene-data-bytes', '128',
            '--scene-data-interval-ms', '50', '--observe-scene-csv', '/capture/scene.csv',
            '--scene-start-file', '/capture/observe-start.marker'])


class AnalysisTests(unittest.TestCase):
    def test_saved_controls_require_sidecar_and_exact_known_schema_omissions(self):
        with tempfile.TemporaryDirectory() as directory:
            path = pathlib.Path(directory)
            saved = path / 'server-base/config/transports/litenetlib.xml'
            saved.parent.mkdir(parents=True)
            xml = '<LNLTransportConfig><IPv6Enabled>true</IPv6Enabled><NatPunchEnabled>false</NatPunchEnabled></LNLTransportConfig>'
            (path / 'prepared-litenetlib.xml').write_text(xml)
            saved.write_text(xml.replace('</LNLTransportConfig>', '<MergeHoldMs>3</MergeHoldMs></LNLTransportConfig>'))
            changes = {'Ipv6Enabled': {'prepared': 'true', 'saved': None},
                       'HealthIncludeExtendedMetrics': {'prepared': 'true', 'saved': None}}
            self.assertTrue(saved_config_controls_match(path, changes))
            for extra in ({'EnableComputeOffload': {'prepared': 'false', 'saved': 'true'}},
                          {'HealthIncludeExtendedMetrics': {'prepared': 'false', 'saved': None}}):
                self.assertFalse(saved_config_controls_match(path, dict(changes, **extra)))
            saved.write_text(xml.replace('<NatPunchEnabled>false', '<NatPunchEnabled>true'))
            self.assertFalse(saved_config_controls_match(path, changes))

    def test_native_missing_is_none_and_measured_drop_is_failure(self):
        samples = [{'monotonic_seconds': i, 'health': {'sent': i * 1_000_000,
                    'recv': i * 100, 'droppedUnreliable': 0, 'droppedVoice': 0}} for i in range(3)]
        metrics, checks, unavailable = csharp_health_metrics(samples, .5, 1.5, 1, 1000)
        self.assertEqual(metrics['server_transmit_mbps'], 8)
        self.assertEqual(metrics['server_unreliable_drops_per_second'], 0)
        self.assertTrue(checks['zero_transport_drops'])
        self.assertIsNone(metrics['server_transmit_datagrams_per_second'])
        self.assertIsNone(checks['native_counter_packetsSent'])
        self.assertIn('native_counter_packetsSent', unavailable)
        samples[-1]['health']['droppedUnreliable'] = 1
        self.assertFalse(csharp_health_metrics(samples, .5, 1.5, 1, 1000)[1]['zero_transport_drops'])
        samples[1]['health'].pop('droppedUnreliable')
        metrics, checks, unavailable = csharp_health_metrics(samples, .5, 1.5, 1, 1000)
        self.assertIsNone(metrics['server_unreliable_drops_per_second'])
        self.assertIsNone(checks['zero_transport_drops'])

    def test_interior_reset_is_failure_even_if_final_counter_is_larger(self):
        samples = [{'monotonic_seconds': i, 'health': {'sent': v}} for i, v in enumerate((100, 200, 50, 400))]
        metrics, checks, _ = csharp_health_metrics(samples, .5, 2.5, 2, 1000)
        self.assertFalse(checks['native_counter_sent'])
        self.assertIsNone(metrics['server_transmit_mbps'])

    def test_translated_config_attests_source_and_preserved_prelaunch_copy(self):
        fixture = pathlib.Path(__file__).resolve().parents[2] / 'docs/performance/fixtures/avatar-cpu-only-server.xml'
        from avatar_tuning import sha256
        with tempfile.TemporaryDirectory() as directory:
            path = pathlib.Path(directory)
            publish = path / 'publish'
            publish.mkdir()
            binary = publish / 'BasisNetworkConsole'
            binary.write_bytes(b'apphost')
            _, config, metadata = prepare_server('csharp', binary, path / 'server-base', fixture, 1234, 2345)
            (path / 'prepared-config.xml').write_bytes(config.read_bytes())
            (path / 'prepared-litenetlib.xml').write_bytes(pathlib.Path(metadata['transport_config']).read_bytes())
            exp = {'frozen': {'server_config': {'path': str(fixture), 'sha256': sha256(fixture)}},
                   'workload': {'port': 1234, 'health_port': 2345}}
            commands = {'server_metadata': metadata}
            self.assertTrue(translated_config_matches(path, exp, commands))
            config.write_text('<Configuration/>')  # startup save cannot alter preserved evidence
            self.assertTrue(translated_config_matches(path, exp, commands))
            initial = path / 'prepared-config.xml'
            initial.write_text(initial.read_text().replace('<EnableComputeOffload>false', '<EnableComputeOffload>true'))
            self.assertFalse(translated_config_matches(path, exp, commands))

    def test_complete_analysis_retains_unavailable_checks_and_strict_sender_gate(self):
        with tempfile.TemporaryDirectory() as directory:
            path = pathlib.Path(directory)
            workload = {'clients': 2, 'window_seconds': 1, 'server_cpus': '2-9', 'client_cpus': '10-15'}
            meta = {'completed': True, 'error': None, 'server_kind': 'csharp', 'workload': workload,
                    'window_start_monotonic': .5, 'window_end_monotonic': 1.5,
                    'client_exit_code': 0, 'server_exit_code': 0,
                    'started_unix_seconds': 10, 'finished_unix_seconds': 20}
            (path / 'run.json').write_text(json.dumps(meta))
            samples = [{'monotonic_seconds': i, 'server': {'cpu_seconds': i, 'rss_bytes': 100},
                        'client': {'cpu_seconds': i, 'rss_bytes': 100}, 'players_online': 2,
                        'active_states': None, 'authenticated_client_indices': [0, 1],
                        'health': {'ready': True, 'listening': True, 'visitors': 2,
                                   'droppedUnreliable': 0, 'droppedVoice': 0}} for i in range(3)]
            (path / 'samples.jsonl').write_text('\n'.join(json.dumps(s) for s in samples))
            commands = {'server_environment': {}, 'client_environment': {}, 'csharp_environment': {},
                        'server_affinity': '2-9', 'client_affinity': '10-15',
                        'client': ['/client', '--config', '/client.xml'], 'server': ['/server'],
                        'server_metadata': {'runtime_file_sha256': {'BasisNetworkServer.dll': 'hash'}}}
            (path / 'commands.json').write_text(json.dumps(commands))
            obs = {'window_ms': '1000', 'window_started': 'true', 'gap_p50_ms': '10', 'gap_p95_ms': '20',
                   'applied_full_items': '10', 'applied_delta_items': '10', 'near_peers': '1',
                   'expected_near_peers': '1', 'near_segment': '0', 'near_segment_ms': '1000'}
            for key in ('missing_expected_peers', 'stale_peers_500ms', 'decode_errors', 'unapplied_deltas',
                        'malformed_items', 'non_newer_sequences', 'discontinuities', 'sequence_ambiguities',
                        'sequence_resyncs', 'sequence_order_unknown_gaps'):
                obs[key] = '0'
            senders = [{'logical_client_index': str(i), 'socket_sent_full': '50', 'socket_sent_delta': '0',
                        'generated_full': '50', 'generated_delta': '0', 'connected_at_end': 'true',
                        'remote_peer_id_start': str(i), 'remote_peer_id_end': str(i), 'send_errors': '0'} for i in range(2)]
            exp = {'workload': workload, 'os_name': 'posix', 'frozen': {
                'server': {'sha256': 'hash', 'runtime_file_sha256': {'BasisNetworkServer.dll': 'hash'}},
                'client': {'sha256': 'hash'}, 'client_config': {'sha256': 'hash'}}}
            entry = {'name': 'csharp-1', 'variant': 'csharp', 'round': 0, 'server_settings': {}, 'client_settings': {}}
            with mock.patch('cross_server_avatar.observer_metrics', return_value=obs), \
                 mock.patch('cross_server_avatar.csv_rows', return_value=senders), \
                 mock.patch('cross_server_avatar.sha256', return_value='hash'), \
                 mock.patch('cross_server_avatar.translated_config_matches', return_value=True), \
                 mock.patch('cross_server_avatar.saved_config_controls_match', return_value=True), \
                 mock.patch('cross_server_avatar.saved_config_changes', return_value={}):
                result = summarize_csharp(path, exp, entry)
                self.assertTrue(result['valid'])
                self.assertIsNone(result['checks']['zero_retransmits'])
                self.assertIsNone(result['metrics']['built_logical_avatar_work_per_second'])
                self.assertIn('zero_retransmits', result['unavailable_checks'])
                senders[1]['socket_sent_full'] = '1'
                result = summarize_csharp(path, exp, entry)
                self.assertFalse(result['valid'])
                self.assertFalse(result['checks']['sender_progress'])


if __name__ == '__main__':
    unittest.main()
