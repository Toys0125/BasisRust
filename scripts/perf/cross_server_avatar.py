"""Analyze C# mixed captures without inventing Rust-only server diagnostics."""
import json
import math
import pathlib
import statistics
import xml.etree.ElementTree as ET

from avatar_benchmark import MIXED_DEFAULTS, population_matches
from avatar_tuning import (csv_rows, interpolate, mixed_command_provenance,
                           mixed_scene_result, observer_metrics, sha256, xml_values)
from script_server import TRANSPORT_FIELDS


RUST_METRICS = ('built_logical_avatar_work_per_second', 'inbound_updates_per_second',
                'diagnostic_counter_seconds', 'tick_ms_per_tick', 'build_ms_per_tick',
                'flush_ms_per_tick')
RUST_CHECKS = ('counter_window', 'zero_tick_errors', 'zero_protocolErrors',
               'zero_wouldBlock', 'zero_retransmits', 'continuous_avatar_states')

# Absent in the frozen protocol-v55 C# configuration schema. Health metrics
# are emitted natively; this workload sends no persistent-data operations.
UNSUPPORTED_FIXTURE_FIELDS = {'HealthIncludeExtendedMetrics': 'true',
    'DisableReadUnlessAdminPersistentFlag': 'false',
    'DisableWriteUnlessAdminPersistentFlag': 'true'}


def health_counter(samples, boundary, key):
    """Interpolate a present, non-reset native C# cumulative counter."""
    for first, last in zip(samples, samples[1:]):
        start, end = float(first['monotonic_seconds']), float(last['monotonic_seconds'])
        if not math.isfinite(start) or not math.isfinite(end) or end <= start:
            raise ValueError('Health samples have a nonpositive monotonic interval')
        if start <= boundary <= end:
            a, b = float(first['health'][key]), float(last['health'][key])
            if not math.isfinite(a) or not math.isfinite(b) or a < 0 or b < a:
                raise ValueError('Invalid/reset native health counter: ' + key)
            return a + (b - a) * (boundary - start) / (end - start)
    raise ValueError('Health samples do not bracket the fixed measurement window')


def csharp_health_metrics(samples, start, end, window, capacity):
    """Absent counters remain None; existing counters retain resets and drops."""
    if not samples or not all(math.isfinite(v) and v > 0 for v in (window, capacity)):
        raise ValueError('Invalid health measurement window/capacity')
    metrics, checks, unavailable = {}, {}, {}
    fields = (('recv', 'server_receive_mbps', 8 / 1_000_000),
              ('sent', 'server_transmit_mbps', 8 / 1_000_000),
              ('packetsSent', 'server_transmit_datagrams_per_second', 1),
              ('packetsRecv', 'server_receive_datagrams_per_second', 1),
              ('droppedUnreliable', 'server_unreliable_drops_per_second', 1),
              ('droppedVoice', 'server_voice_drops_per_second', 1))
    for key, metric, factor in fields:
        if not all(key in s['health'] for s in samples):
            metrics[metric] = None
            checks['native_counter_' + key] = None
            unavailable['native_counter_' + key] = 'C# health field absent from at least one sample'
        else:
            # Inspect every interval, so an interior reset cannot hide behind a
            # larger final value or fall between the boundary interpolation pairs.
            values = [float(s['health'][key]) for s in samples]
            valid = all(math.isfinite(v) and v >= 0 for v in values) and all(b >= a for a, b in zip(values, values[1:]))
            checks['native_counter_' + key] = valid
            metrics[metric] = ((health_counter(samples, end, key) - health_counter(samples, start, key)) / window * factor) if valid else None
        if key in ('droppedUnreliable', 'droppedVoice'):
            gate = 'zero_transport_drops' if key == 'droppedUnreliable' else 'zero_voice_drops'
            checks[gate] = all(s['health'][key] == 0 for s in samples) if checks['native_counter_' + key] is not None else None
            if checks[gate] is None:
                unavailable[gate] = unavailable['native_counter_' + key]
    metrics['server_transmit_capacity_percent'] = (metrics['server_transmit_mbps'] / capacity * 100
                                                   if metrics['server_transmit_mbps'] is not None else None)
    for key in ('queuePerPeer', 'voiceQueuePerPeer'):
        metrics['server_' + key] = sorted({s['health'][key] for s in samples}) if all(key in s['health'] for s in samples) else None
    return metrics, checks, unavailable


def translated_config_matches(path, experiment, commands):
    """Attest allowed fixture translations independently of the runtime metadata."""
    fixture = pathlib.Path(experiment['frozen']['server_config']['path'])
    if sha256(fixture) != experiment['frozen']['server_config']['sha256']:
        return False
    expected = xml_values(fixture)
    if expected.get('BasisUserRestrictionMode') == 'None':
        expected['BasisUserRestrictionMode'] = 'Normal'
    expected.update(SetPort=str(experiment['workload']['port']), HealthCheckHost='127.0.0.1',
                    HealthCheckPort=str(experiment['workload']['health_port']))
    config = path / 'prepared-config.xml'
    if xml_values(config) != expected:
        return False
    metadata = commands['server_metadata']
    transport = path / 'prepared-litenetlib.xml'
    root = ET.parse(transport).getroot()
    expected_transport = {('IPv6Enabled' if k == 'Ipv6Enabled' else k): expected[k]
                          for k in TRANSPORT_FIELDS if k in expected}
    return (root.tag == 'LNLTransportConfig' and len(root) == len(expected_transport)
            and {n.tag: (n.text or '').strip() for n in root} == expected_transport
            and sha256(config) == metadata['config_sha256']
            and sha256(transport) == metadata['transport_config_sha256'])


def saved_config_changes(path):
    """Retain serialization changes; the parent can audit schema normalization."""
    prepared = xml_values(path / 'prepared-config.xml')
    actual = xml_values(path / 'server-base/config/config.xml')
    return {key: {'prepared': prepared.get(key), 'saved': actual.get(key)}
            for key in sorted(prepared.keys() | actual.keys()) if prepared.get(key) != actual.get(key)}


def saved_config_controls_match(path, changes):
    """Attest supplied controls after both C# serializers save their schemas."""
    def transport_values(file):
        root = ET.parse(file).getroot()
        if root.tag != 'LNLTransportConfig' or len({node.tag for node in root}) != len(root):
            raise ValueError('Expected transport XML with unique field names')
        return {node.tag: (node.text or '').strip() for node in root}
    prepared_transport = transport_values(path / 'prepared-litenetlib.xml')
    saved_transport = transport_values(path / 'server-base/config/transports/litenetlib.xml')
    if any(saved_transport.get(key) != value for key, value in prepared_transport.items()):
        return False
    for key, value in changes.items():
        if value['prepared'] is None:
            continue  # Retained C# defaults added by its serializer.
        if key in TRANSPORT_FIELDS and value['saved'] is None:
            sidecar_key = 'IPv6Enabled' if key == 'Ipv6Enabled' else key
            if prepared_transport.get(sidecar_key) == value['prepared']:
                continue
        if key in UNSUPPORTED_FIXTURE_FIELDS and value == {
                'prepared': UNSUPPORTED_FIXTURE_FIELDS[key], 'saved': None}:
            continue
        return False
    return True


def summarize_csharp(path, experiment, entry):
    """Retain common strict gates, raw observations and instrumentation limits."""
    path = pathlib.Path(path)
    meta = json.loads((path / 'run.json').read_text())
    if not meta['completed'] or meta['error']:
        raise ValueError('Run did not complete: ' + str(meta['error']))
    obs = observer_metrics(path / 'observer.csv')
    senders = csv_rows(path / 'observer.sender.csv')
    samples = [json.loads(line) for line in (path / 'samples.jsonl').read_text().splitlines()]
    if len(samples) < 2 or not senders:
        raise ValueError('Incomplete process/sender samples')
    workload = experiment['workload']
    clients, window = workload['clients'], workload['window_seconds']
    start, end = meta['window_start_monotonic'], meta['window_end_monotonic']
    if not math.isfinite(start) or not math.isfinite(end) or abs(end - start - window) > .001:
        raise ValueError('Invalid fixed measurement boundaries')
    seconds = int(obs['window_ms']) / 1000
    if seconds <= 0:
        raise ValueError('Empty avatar observer window')
    metrics = {'gap_p50_ms': float(obs['gap_p50_ms']), 'gap_p95_ms': float(obs['gap_p95_ms']),
               'observer_applied_items_per_second': (int(obs['applied_full_items']) + int(obs['applied_delta_items'])) / seconds}
    for role in ('server', 'client'):
        metrics[role + '_cpu_cores'] = (interpolate(samples, end, role) - interpolate(samples, start, role)) / window
        rss = [s[role]['rss_bytes'] / 2**20 for s in samples if start <= s['monotonic_seconds'] <= end]
        if not rss:
            raise ValueError('No RSS samples in window')
        metrics[role + '_rss_mean_mib'] = statistics.mean(rss)
        metrics[role + '_rss_peak_mib'] = max(rss)
    metrics['combined_cpu_cores'] = metrics['server_cpu_cores'] + metrics['client_cpu_cores']
    metrics['sender_socket_items_per_second'] = sum(int(r['socket_sent_full']) + int(r['socket_sent_delta']) for r in senders) / window
    metrics['sender_generated_items_per_second'] = sum(int(r['generated_full']) + int(r['generated_delta']) for r in senders) / window
    minimum = min(int(r['socket_sent_full']) + int(r['socket_sent_delta']) for r in senders)
    checks = {
        'completed': meta['completed'] and meta['error'] is None,
        'server_kind': meta.get('server_kind') == 'csharp',
        'fixed_workload_provenance': meta['workload'] == workload,
        'client_exit': meta['client_exit_code'] == 0,
        'server_exit': meta['server_exit_code'] in ((0, 3221225786) if experiment['os_name'] == 'nt' else (0, -2)),
        'observer_window': obs['window_started'] == 'true' and abs(int(obs['window_ms']) - window * 1000) <= 100,
        'coverage': int(obs['near_peers']) == clients - 1 and int(obs['expected_near_peers']) == clients - 1,
        'uninterrupted_observer': int(obs['near_segment']) == 0 and abs(int(obs['near_segment_ms']) - window * 1000) <= 100,
        'continuous_readiness': all(s['health'].get('ready') is True and s['health'].get('listening') is True
                                    and population_matches(s, 'csharp', clients) for s in samples),
        'sender_records': len(senders) == clients and {int(s['logical_client_index']) for s in senders} == set(range(clients)),
        'sender_connections': all(s['connected_at_end'] == 'true' and s['remote_peer_id_start'] == s['remote_peer_id_end'] and s['remote_peer_id_start'] != '' for s in senders),
        'sender_progress': minimum >= window * 50 * .9,
        'sender_errors': all(int(s['send_errors']) == 0 for s in senders),
        'positive_metrics': all(math.isfinite(v) and v > 0 for k, v in metrics.items() if 'rss' not in k),
    }
    for key in ('missing_expected_peers', 'stale_peers_500ms', 'decode_errors', 'unapplied_deltas',
                'malformed_items', 'non_newer_sequences', 'discontinuities', 'sequence_ambiguities',
                'sequence_resyncs', 'sequence_order_unknown_gaps'):
        checks['zero_' + key] = int(obs[key]) == 0
    native_metrics, native_checks, unavailable = csharp_health_metrics(samples, start, end, window,
                                                                     workload.get('network_capacity_mbps', 1000))
    metrics.update(native_metrics)
    checks.update(native_checks)
    for name in RUST_METRICS:
        metrics[name] = None
    for name in RUST_CHECKS:
        checks[name] = None
        unavailable[name] = 'Rust server diagnostic is not emitted by C#'
    commands = json.loads((path / 'commands.json').read_text())
    checks['control_provenance'] = (commands['server_environment'] == entry['server_settings']
                                    and commands['client_environment'] == entry['client_settings']
                                    and commands.get('csharp_environment', {}) == entry.get('csharp_settings', {})
                                    and commands['server_affinity'] == workload['server_cpus']
                                    and commands['client_affinity'] == workload['client_cpus'])
    checks['matching_config_copy'] = translated_config_matches(path, experiment, commands)
    config_changes = saved_config_changes(path)
    checks['saved_config_controls'] = saved_config_controls_match(path, config_changes)
    checks['client_config_provenance'] = sha256(pathlib.Path(commands['client'][2])) == experiment['frozen']['client_config']['sha256']
    for role in ('server', 'client'):
        checks['frozen_' + role] = sha256(pathlib.Path(commands[role][0])) == experiment['frozen'][role]['sha256']
    expected_runtime = experiment['frozen']['server'].get('runtime_file_sha256')
    checks['frozen_server_runtime'] = (bool(expected_runtime)
        and commands['server_metadata']['runtime_file_sha256'] == expected_runtime
        and all(sha256(path / 'server-base' / name) == digest for name, digest in expected_runtime.items()))
    if any(key in workload for key in MIXED_DEFAULTS):
        checks['mixed_command_provenance'] = mixed_command_provenance(commands['client'], workload, path)
    scene = None
    if workload.get('scene_data_bytes', 0):
        scene, scene_checks, scene_metrics = mixed_scene_result(path, workload)
        checks.update(scene_checks)
        metrics.update(scene_metrics)
    return {'name': entry['name'], 'variant': entry['variant'], 'round': entry['round'],
            'valid': all(v for v in checks.values() if v is not None),
            'checks': checks, 'unavailable_checks': unavailable,
            'unavailable_metrics': {k: 'Rust server diagnostic is not emitted by C#' for k in RUST_METRICS},
            'metrics': metrics, 'observer': obs, 'minimum_sender_socket_items': minimum,
            'saved_config_changes': config_changes,
            'unsupported_fixture_fields': {key: value for key, value in config_changes.items()
                                           if key in UNSUPPORTED_FIXTURE_FIELDS},
            'saved_config_sha256': sha256(path / 'server-base/config/config.xml'),
            **({'scene': scene} if scene is not None else {}),
            'started_unix_seconds': meta['started_unix_seconds'], 'finished_unix_seconds': meta['finished_unix_seconds'],
            'sender_errors': sum(int(r['send_errors']) for r in senders),
            'exits': {k: meta[k] for k in ('client_exit_code', 'server_exit_code')}}
