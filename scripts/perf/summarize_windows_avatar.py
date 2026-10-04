"""Validate/summarize a 60-second dense Windows loopback avatar capture.

Gates match the 2026-10-04 review: coverage at one observer, sender progress,
readiness samples and protocol/transport errors. End-of-window stale counts do
not assert that no earlier 500ms gaps occurred. CPU and UDP deltas use the
recorded sample endpoints, rather than assuming the counters span 60 seconds.
"""

import argparse
import csv
import json
import pathlib
import re
import statistics
import sys
from datetime import datetime


def rows(path):
    with path.open(newline='', encoding='utf-8-sig') as stream:
        return list(csv.DictReader(stream))


def summarize(path):
    meta = json.loads((path / 'workload.json').read_text())
    obs = {}
    with (path / 'observer.csv').open(newline='', encoding='utf-8-sig') as stream:
        for row in csv.reader(stream):
            if len(row) != 2 or row[0] == 'observed_channel':
                break
            if row[0] != 'metric':
                obs[row[0]] = row[1]
    process = rows(path / 'process-metrics.csv')
    health = [json.loads(line) for line in (path / 'health.jsonl').read_text().splitlines()]
    first, last = process[0], process[-1]
    elapsed = float(last['unix_seconds']) - float(first['unix_seconds'])
    metrics = {'process_counter_seconds': elapsed}
    for label in ('server', 'client'):
        for kind in ('cpu', 'kernel_cpu', 'user_cpu'):
            key = label + '_' + kind + '_seconds'
            metrics[label + '_' + kind + '_cores'] = (float(last[key]) - float(first[key])) / elapsed
        for kind in ('working_set', 'commit_charge'):
            metrics[label + '_' + kind + '_peak_mib'] = max(float(p[label + '_' + kind + '_bytes']) for p in process) / 2**20
    metrics['combined_cpu_cores'] = metrics['server_cpu_cores'] + metrics['client_cpu_cores']
    for key in ('gap_p50_ms', 'gap_p95_ms'):
        metrics[key] = float(obs[key])
    metrics['observer_items_per_second'] = (int(obs['applied_full_items']) + int(obs['applied_delta_items'])) / (int(obs['window_ms']) / 1000)
    seconds = health[-1]['unix_seconds'] - health[0]['unix_seconds']
    metrics['health_counter_seconds'] = seconds
    for key, output in [('bytesOut', 'udp_payload_mb_per_second'), ('packetsOut', 'udp_datagrams_per_second')]:
        delta = health[-1]['extended']['rawUdp'][key] - health[0]['extended']['rawUdp'][key]
        metrics[output] = delta / seconds / (1_000_000 if key == 'bytesOut' else 1)
    for key, output in [('inboundUpdates', 'inbound_updates_per_second'), ('outboundLogicalAvatarSends', 'logical_avatar_sends_per_second')]:
        metrics[output] = (health[-1]['extended']['avatarSync'][key] - health[0]['extended']['avatarSync'][key]) / seconds
    slices = [h['extended']['avatarSync']['receiverSlices'] for h in health]
    metrics['receiver_slices_median'] = statistics.median(slices)
    metrics['receiver_slices_range'] = [min(slices), max(slices)]
    # Status logs retain the adaptive scheduler's instantaneous state even
    # when per-pair diagnostics are off. Do not treat cumulative average
    # build/flush values in these logs as measurement-window deltas.
    status_samples = []
    status_time = None
    server_log = (path / 'server.log').read_text(errors='replace')
    for line in server_log.splitlines():
        if 'Server is running and healthy' in line:
            try:
                status_time = datetime.fromisoformat(line.split()[0].replace('Z', '+00:00')).timestamp()
            except ValueError:
                status_time = None
        if status_time is not None and float(first['unix_seconds']) <= status_time <= float(last['unix_seconds']) and line.startswith('Avatar timing:'):
            fields = dict(re.findall(r'(\w+)=([\d.]+)', line))
            status_samples.append({key: float(fields[key]) for key in ('smooth_tick_us', 'receiver_cycle_ms')})
    if status_samples:
        metrics['scheduler_status_samples'] = len(status_samples)
        for key in ('smooth_tick_us', 'receiver_cycle_ms'):
            values = [sample[key] for sample in status_samples]
            metrics['status_' + key + '_median'] = statistics.median(values)
            metrics['status_' + key + '_range'] = [min(values), max(values)]
    senders = rows(path / 'observer.sender.csv')
    checks = {
        'loopback': meta['server_ip'] == '127.0.0.1',
        'local_rust_server': meta['server_kind'] == 'rust',
        'window_started': obs.get('window_started') == 'true',
        'window_ms': int(obs['window_ms']) == 60000,
        'observer_peers': int(obs['near_peers']) == meta['clients'] - 1,
        'all_readiness_samples': all(int(p['players_online']) == meta['clients'] and int(p['active_states']) == meta['clients'] for p in process),
        'readiness_samples': len(process) >= 29,
        'sender_records': len(senders) == meta['clients'],
        'all_senders_connected': all(p['connected_at_end'] == 'true' for p in senders),
        'all_senders_progressed': all(int(p['socket_sent_full']) + int(p['socket_sent_delta']) >= 2900 for p in senders),
        'zero_sender_errors': all(int(p['send_errors']) == 0 for p in senders),
        'zero_avatar_tick_errors': 'avatar sync tick failed:' not in server_log,
        'client_exited_cleanly': meta['client_exit_code'] == 0,
        'server_harness_stop': meta['server_exit_code'] in (0, 3221225786),
        'process_counter_window': 55 <= elapsed <= 65,
        'health_counter_window': 55 <= seconds <= 65,
        'positive_process_cpu': all(metrics[label + '_cpu_cores'] > 0 for label in ('server', 'client')),
        'positive_delivery_counters': all(metrics[key] > 0 for key in ('observer_items_per_second', 'inbound_updates_per_second', 'logical_avatar_sends_per_second', 'udp_payload_mb_per_second', 'udp_datagrams_per_second')),
    }
    for key in ('missing_expected_peers', 'stale_peers_500ms', 'decode_errors', 'unapplied_deltas', 'malformed_items', 'non_newer_sequences'):
        checks['zero_' + key] = int(obs[key]) == 0
    for key in ('discontinuities', 'sequence_ambiguities', 'sequence_resyncs', 'sequence_order_unknown_gaps'):
        if key in obs:
            checks['zero_' + key] = int(obs[key]) == 0
    if 'near_segment_ms' in obs:
        checks['full_uninterrupted_segment'] = int(obs['near_segment_ms']) == 60000 and int(obs['near_segment']) == 0
    for key in ('wouldBlock',):
        checks['zero_' + key] = all(h['extended']['rawUdp'][key] == 0 for h in health)
    checks['zero_protocol_errors'] = all(h['extended']['appMessages']['protocolErrors'] == 0 for h in health)
    checks['zero_retransmits'] = all(h['extended']['reliable']['retransmits'] == 0 for h in health)
    if 'transport' in health[0]:
        checks['zero_non_reliable_drops'] = all(h['transport']['nonReliableDroppedDatagrams'] == 0 for h in health)
    diagnostic = path / 'server-pairs.global.csv'
    if diagnostic.exists():
        samples = rows(diagnostic)
        a, b = samples[0], samples[-1]
        ticks = int(b['tick_count']) - int(a['tick_count'])
        metrics['ticks_per_second'] = ticks / ((float(b['elapsed_ms']) - float(a['elapsed_ms'])) / 1000)
        for key in ('tick', 'build', 'flush'):
            field = key + '_micros'
            metrics[key + '_ms_per_tick'] = (int(b[field]) - int(a[field])) / ticks / 1000
    return {'name': path.name, 'metrics': metrics, 'observer': obs, 'valid': all(checks.values()), 'checks': checks,
            'readiness_samples': len(process), 'minimum_sender_sends': min(int(p['socket_sent_full']) + int(p['socket_sent_delta']) for p in senders), 'metadata': meta}


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('captures', nargs='+', type=pathlib.Path)
    arguments = parser.parse_args()
    valid = True
    for path in arguments.captures:
        result = summarize(path)
        print(json.dumps({'name': result['name'], 'valid': result['valid'], 'metrics': result['metrics'], 'failed_checks': [k for k, v in result['checks'].items() if not v]}, indent=2))
        valid &= result['valid']
    sys.exit(0 if valid else 1)
