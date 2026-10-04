"""Strict capture parsing and conservative, repeat-level settings comparisons."""
import csv
import hashlib
import json
import math
import os
import pathlib
import statistics
import xml.etree.ElementTree as ET


def sha256(path):
    digest = hashlib.sha256()
    with path.open('rb') as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b''):
            digest.update(chunk)
    return digest.hexdigest()


def xml_values(path):
    try:
        root = ET.parse(path).getroot()
    except ET.ParseError as exc:
        raise ValueError('Malformed configuration XML') from exc
    if root.tag != 'Configuration' or len({n.tag for n in root}) != len(root):
        raise ValueError('Expected Configuration XML with unique field names')
    return {node.tag: (node.text or '').strip() for node in root}


def observer_metrics(path):
    values = {}
    with path.open(newline='', encoding='utf-8-sig') as stream:
        for row in csv.reader(stream):
            if row and row[0] == 'observed_channel':
                break
            if len(row) != 2:
                raise ValueError('Malformed observer metric section')
            if row[0] == 'metric':
                continue
            if row[0] in values:
                raise ValueError('Duplicate observer metric: ' + row[0])
            values[row[0]] = row[1]
    return values


def csv_rows(path):
    with path.open(newline='', encoding='utf-8-sig') as stream:
        return list(csv.DictReader(stream))


def delta(first, last, key):
    value = float(last[key]) - float(first[key])
    if not math.isfinite(value) or value < 0:
        raise ValueError('Invalid/reset cumulative counter: ' + key)
    return value


def interpolate(samples, boundary, role):
    for a, b in zip(samples, samples[1:]):
        ta, tb = a['monotonic_seconds'], b['monotonic_seconds']
        if ta <= boundary <= tb:
            return a[role]['cpu_seconds'] + (b[role]['cpu_seconds'] - a[role]['cpu_seconds']) * (boundary - ta) / (tb - ta)
    raise ValueError('CPU samples do not bracket the fixed measurement window')


def summarize(path, experiment, entry):
    """Retain every gate and metric, including invalid/outlier runs."""
    meta = json.loads((path / 'run.json').read_text())
    if not meta['completed'] or meta['error']:
        raise ValueError('Run did not complete: ' + str(meta['error']))
    obs = observer_metrics(path / 'observer.csv')
    senders = csv_rows(path / 'observer.sender.csv')
    samples = [json.loads(line) for line in (path / 'samples.jsonl').read_text().splitlines()]
    counters = csv_rows(path / 'server-pairs.global.csv')
    clients, window = experiment['workload']['clients'], experiment['workload']['window_seconds']
    if len(samples) < 2 or len(counters) < 2 or not senders:
        raise ValueError('Incomplete process/counter/sender samples')
    a, b = counters[0], counters[-1]
    seconds = delta(a, b, 'elapsed_ms') / 1000
    if seconds <= 0:
        raise ValueError('Empty cumulative counter window')
    start, end = meta['window_start_monotonic'], meta['window_end_monotonic']
    metrics = {'gap_p50_ms': float(obs['gap_p50_ms']), 'gap_p95_ms': float(obs['gap_p95_ms']),
               'observer_applied_items_per_second': (int(obs['applied_full_items']) + int(obs['applied_delta_items'])) / (int(obs['window_ms']) / 1000),
               'built_logical_avatar_work_per_second': delta(a, b, 'outbound_logical_avatar_sends') / seconds,
               'inbound_updates_per_second': delta(a, b, 'inbound_updates') / seconds,
               'diagnostic_counter_seconds': seconds}
    ticks = delta(a, b, 'tick_count')
    if ticks <= 0:
        raise ValueError('No measured ticks')
    for key in ('tick', 'build', 'flush'):
        metrics[key + '_ms_per_tick'] = delta(a, b, key + '_micros') / ticks / 1000
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
        'fixed_workload_provenance': meta['workload'] == experiment['workload'],
        'client_exit': meta['client_exit_code'] == 0,
        'server_exit': meta['server_exit_code'] in ((0, 3221225786) if experiment['os_name'] == 'nt' else (0, -2)),
        'observer_window': obs['window_started'] == 'true' and abs(int(obs['window_ms']) - window * 1000) <= 100,
        'coverage': int(obs['near_peers']) == clients - 1 and int(obs['expected_near_peers']) == clients - 1,
        'uninterrupted_observer': int(obs['near_segment']) == 0 and abs(int(obs['near_segment_ms']) - window * 1000) <= 100,
        'continuous_readiness': all(s['players_online'] == s['active_states'] == clients for s in samples),
        'counter_window': abs(seconds - window) <= max(.5, window * .05),
        'sender_records': len(senders) == clients and {int(s['logical_client_index']) for s in senders} == set(range(clients)),
        'sender_connections': all(s['connected_at_end'] == 'true' and s['remote_peer_id_start'] == s['remote_peer_id_end'] and s['remote_peer_id_start'] != '' for s in senders),
        # 50 offered items/s from the fixed 20ms policy. Fail visibly on an
        # overloaded generator; do not reward configurations that drop input.
        'sender_progress': minimum >= window * 50 * .9,
        'sender_errors': all(int(s['send_errors']) == 0 for s in senders),
        'positive_metrics': all(math.isfinite(v) and v > 0 for k, v in metrics.items() if 'rss' not in k),
        'matching_config_copy': sha256(path / 'server-base/config/config.xml') == experiment['frozen']['server_config']['sha256'],
        'zero_tick_errors': 'avatar sync tick failed:' not in (path / 'server.log').read_text(errors='replace'),
    }
    for key in ('missing_expected_peers', 'stale_peers_500ms', 'decode_errors', 'unapplied_deltas',
                'malformed_items', 'non_newer_sequences', 'discontinuities', 'sequence_ambiguities',
                'sequence_resyncs', 'sequence_order_unknown_gaps'):
        checks['zero_' + key] = int(obs[key]) == 0
    for group, key in [('appMessages', 'protocolErrors'), ('rawUdp', 'wouldBlock'), ('reliable', 'retransmits')]:
        checks['zero_' + key] = all(s['health']['extended'][group][key] == 0 for s in samples)
    checks['zero_transport_drops'] = all(s['health']['transport']['nonReliableDroppedDatagrams'] == 0 for s in samples)
    # Settings comparisons REQUIRE one frozen server and different configs.
    commands = json.loads((path / 'commands.json').read_text())
    dynamic = {'BASIS_STATUS_INTERVAL_SECS', 'BASIS_AVATAR_DIAGNOSTIC_OBSERVER_ID', 'BASIS_AVATAR_DIAGNOSTIC_START_FILE',
               'BASIS_AVATAR_DIAGNOSTIC_CSV', 'BASIS_AVATAR_DIAGNOSTIC_WINDOW_SECS'}
    checks['setting_provenance'] = (
        {k: v for k, v in commands['server_environment'].items() if k not in dynamic} == entry['server_settings']
        and commands['client_environment'] == entry['client_settings']
        and entry['server_settings'].get('BASIS_AVATAR_FLUSH_LANES') == str(entry['lanes']))
    checks['client_config_provenance'] = sha256(pathlib.Path(commands['client'][2])) == experiment['frozen']['client_config']['sha256']
    for role in ('server', 'client'):
        checks['frozen_' + role] = sha256(pathlib.Path(commands[role][0])) == experiment['frozen'][role]['sha256']
    return {'name': entry['name'], 'lanes': entry['lanes'], 'round': entry['round'], 'valid': all(checks.values()),
            'checks': checks, 'metrics': metrics, 'observer': obs, 'minimum_sender_socket_items': minimum,
            'started_unix_seconds': meta['started_unix_seconds'], 'finished_unix_seconds': meta['finished_unix_seconds'],
            'sender_errors': sum(int(r['send_errors']) for r in senders), 'exits': {k: meta[k] for k in ('client_exit_code', 'server_exit_code')}}


def run_order(lanes, repeats):
    """Forward/reverse pairs; rotate each pair for multi-setting screens."""
    order = []
    for round_index in range(repeats):
        shift = (round_index // 2) % len(lanes)
        rotated = lanes[shift:] + lanes[:shift]
        if round_index % 2:
            rotated = list(reversed(rotated))
        order.extend((round_index, lane) for lane in rotated)
    return order


def validate_series(experiment):
    """Validate the distinct-settings path and frozen common controls."""
    lanes, repeats = experiment['lanes'], experiment['repeats']
    if (experiment['comparison_kind'] != 'settings' or len(lanes) < 2 or len(set(lanes)) != len(lanes)
            or any(type(lane) is not int or not 0 <= lane <= 8 for lane in lanes)
            or type(repeats) is not int or repeats < 2 or repeats % 2):
        raise ValueError('Invalid distinct settings comparison')
    expected = run_order(lanes, repeats)
    entries = experiment['runs']
    if len(entries) > len(expected) or len({e['name'] for e in entries}) != len(entries):
        raise ValueError('Duplicate/extra run entries')
    baseline = None
    for entry, (round_index, lane) in zip(entries, expected):
        if (entry['round'], entry['lanes']) != (round_index, lane):
            raise ValueError('Run order does not match the counterbalanced settings design')
        server = dict(entry['server_settings'])
        if server.pop('BASIS_AVATAR_FLUSH_LANES', None) != str(lane):
            raise ValueError('Invalid run setting provenance')
        controls = (server, entry['client_settings'])
        if baseline is not None and controls != baseline:
            raise ValueError('Unexpected worker/client setting difference within sweep')
        baseline = controls


def rank(runs, lanes, repeats):
    """Descriptive repeat-level dominance, never pooled-update significance."""
    if (len(runs) != len(lanes) * repeats or any(not r.get('valid') for r in runs)
            or any(a.get('finished_unix_seconds', 0) > b.get('started_unix_seconds', float('inf')) for a, b in zip(runs, runs[1:]))):
        return {'status': 'inconclusive', 'reason': 'Incomplete series or failed delivery/provenance gates; all evidence retained.', 'recommended_lanes': None}
    grouped = {lane: sorted((r for r in runs if r['lanes'] == lane), key=lambda r: r['round']) for lane in lanes}
    if any([r['round'] for r in rs] != list(range(repeats)) for rs in grouped.values()):
        raise ValueError('Missing/duplicate repeat identities')
    keys = list(runs[0]['metrics'])
    stats = {str(lane): {k: {'median': statistics.median(r['metrics'][k] for r in rs),
                            'min': min(r['metrics'][k] for r in rs), 'max': max(r['metrics'][k] for r in rs)} for k in keys}
             for lane, rs in grouped.items()}
    comparisons = []
    winners = []
    for candidate in lanes:
        dominated = []
        for control in lanes:
            if candidate == control:
                continue
            changes = [{k: 100 * (a['metrics'][k] / b['metrics'][k] - 1) for k in keys}
                       for a, b in zip(grouped[candidate], grouped[control])]
            matched_input = all(abs(c['inbound_updates_per_second']) <= 5 and abs(c['sender_socket_items_per_second']) <= 5 for c in changes)
            no_dropped_work = all(c['built_logical_avatar_work_per_second'] >= 0 and c['observer_applied_items_per_second'] >= 0 for c in changes)
            cadence = all(c['gap_p50_ms'] <= -3 and c['gap_p95_ms'] <= -3 for c in changes)
            efficiency = all(abs(c['gap_p50_ms']) <= 3 and abs(c['gap_p95_ms']) <= 3 and c['server_cpu_cores'] <= -5 for c in changes)
            win = matched_input and no_dropped_work and (cadence or efficiency)
            comparisons.append({'candidate': candidate, 'control': control, 'matched_input': matched_input,
                                'no_dropped_work': no_dropped_work, 'tradeoff': 'cadence' if cadence else 'CPU' if efficiency else 'inconclusive',
                                'dominates': win, 'paired_change_percent': changes})
            dominated.append(win)
        if all(dominated):
            winners.append(candidate)
    winner = winners[0] if len(winners) == 1 and repeats >= 2 else None
    return {'status': 'measured preference' if winner is not None else 'inconclusive', 'recommended_lanes': winner,
            'reason': 'Consistent repeat-level cadence/CPU preference with matched input and retained work.' if winner is not None else 'No unique setting clears every paired delivery/cadence/CPU comparison; ties and variable runs are inconclusive.',
            'statistics': stats, 'comparisons': comparisons,
            'limits': 'Descriptive min/median/max across process runs; not a confidence interval. One synthetic loopback workload and one observer do not establish a universal best setting.'}


def report(experiment, runs, ranking):
    lines = ['# Local avatar settings tuning', '', f"Mode: {experiment['mode']}; {experiment['repeats']} repeats/setting; {experiment['workload']['clients']} clients; warmup/window {experiment['workload']['warmup_seconds']}/{experiment['workload']['window_seconds']}s; platform default flush lanes: {experiment['platform_default_flush_lanes']}.", '',
             '| Run | lanes | valid | applied p50/p95 ms | built work/s | applied items/s | input/s | server/client CPU cores | server RSS peak MiB |',
             '|---|---:|---|---|---:|---:|---:|---|---:|']
    for r in runs:
        m = r.get('metrics', {})
        def value(k, fmt='.2f'):
            return format(m[k], fmt) if k in m else '—'
        lines.append(f"| {r['name']} | {r['lanes']} | {r.get('valid', False)} | {value('gap_p50_ms')}/{value('gap_p95_ms')} | {value('built_logical_avatar_work_per_second', '.0f')} | {value('observer_applied_items_per_second', '.0f')} | {value('inbound_updates_per_second', '.0f')} | {value('server_cpu_cores')}/{value('client_cpu_cores')} | {value('server_rss_peak_mib')} |")
        failed = [k for k, v in r.get('checks', {}).items() if not v]
        if r.get('error') or failed:
            lines.extend(['', f"{r['name']}: {r.get('error') or ', '.join(failed)}", ''])
    lines.extend(['', f"Result: **{ranking['status']}**. {ranking['reason']}", '',
                  'Applied gaps are intervals between applied updates at one observer, not end-to-end latency. Built logical work counts recipient avatar items before transport, not delivered items. CPU cores = process CPU seconds / wall seconds (1 core = 100%). RSS is sampled working set, not allocator usage.', '',
                  'All raw runs, errors, exit codes, readiness, sender progress, commands and provenance are retained. No outliers are removed. Min/median/max and per-round changes are in summary.json; repeats are process runs, not independent receiver updates.', '',
                  'A screen suggests settings to confirm; confirmation only strengthens evidence for this host/workload. Dense synthetic poses, no voice/P2P, shared loopback generator CPU, one observer, and platform defaults limit generalization.'])
    winner = ranking['recommended_lanes']
    if winner is not None:
        settings = {'BASIS_AVATAR_FLUSH_LANES': winner}
        for option, variable in [('rayon_threads', 'RAYON_NUM_THREADS'), ('tokio_workers', 'TOKIO_WORKER_THREADS')]:
            if experiment['workload'][option] is not None:
                settings[variable] = experiment['workload'][option]
        lines.extend(['', f'Measured preference: flush lanes {winner}. Apply manually only after considering the per-round CPU/cadence changes in summary.json; retain the tested affinity:', '', '```sh',
                      *[f'export {k}={v}' for k, v in settings.items()], '```', '', '```powershell',
                      *[f'$env:{k}="{v}"' for k, v in settings.items()], '```'])
        for c in ranking['comparisons']:
            if c['candidate'] == winner:
                means = {k: statistics.mean(p[k] for p in c['paired_change_percent']) for k in ('gap_p50_ms', 'gap_p95_ms', 'server_cpu_cores', 'built_logical_avatar_work_per_second')}
                lines.append(f"\nVersus lanes {c['control']}: mean paired p50 {means['gap_p50_ms']:+.1f}%, p95 {means['gap_p95_ms']:+.1f}%, server CPU {means['server_cpu_cores']:+.1f}%, built work {means['built_logical_avatar_work_per_second']:+.1f}% ({c['tradeoff']} preference).")
    lines.extend(['', 'Exact invocation (argument array; paths may contain spaces):', '', '```json', json.dumps(experiment['invocation']), '```', ''])
    return '\n'.join(lines)
