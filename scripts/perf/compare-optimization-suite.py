#!/usr/bin/env python3
"""Compare frozen ABBA suite captures and plot benchmark/perf heatmaps.

Input directories use <variant>-micro-<tag>, <variant>-live-<tag> and
<variant>-profile-<tag>. Logs and *.perf.csv come from perf stat; live captures
are produced by run-windows-avatar-workload.py (also supports Linux).
Plotting requires matplotlib; JSON analysis uses only the standard library.
"""

import argparse
import collections
import csv
import json
import pathlib
import re
import statistics
import subprocess


def med(values):
    return statistics.median(values)


def counter_metrics(path):
    result = {}
    for row in csv.reader(path.read_text().splitlines(), delimiter=';'):
        if len(row) > 2 and not row[0].startswith('#'):
            try:
                result[row[2]] = float(row[0])
            except ValueError:
                continue
    return result


def read_micro(path):
    results = {}
    for status in path.glob('*.status.json'):
        name = status.name.removesuffix('.status.json')
        status_record = json.loads(status.read_text())
        code = status_record['exit_code']
        if code or status_record.get('timed_out', False):
            raise ValueError(
                f'{path.name}/{name} failed: exit_code={code}, '
                f'timed_out={status_record.get("timed_out", False)}'
            )
        text = (path / f'{name}.log').read_text()
        counters = counter_metrics(path / f'{name}.perf.csv')
        if name.startswith(('registry-', 'reliable-')):
            rows = [json.loads(s.removeprefix('RESULT ')) for s in text.splitlines()
                    if s.startswith(('{', 'RESULT {'))]
            record = rows[-1]
            if name.startswith('registry-'):
                assert record['processed_operations'] == 32768
                assert record['client_pending_at_end'] == record['protocol_errors_delta'] == 0
                assert record['client_ack_errors'] == 0
                metrics = {'rate': record['operations_per_second'],
                           'p95_ms': record['batch_completion_p95_ms']}
            else:
                assert record['delivered'] == record['unique_received'] == record['messages']
                assert record['ordering_or_payload_errors'] == record['duplicates'] == 0
                metrics = {'completion_ms': record['receiver_completion_ms'],
                           'p95_ms': record['first_transmission_to_delivery_p95_ms']}
            results[name] = {'metrics': metrics, 'counters': counters, 'details': record}
        elif name == 'ordered_admission_microbench':
            samples = collections.defaultdict(list)
            seen_samples = collections.defaultdict(set)
            for label, sample, value in re.findall(r'case=(\w+) sample=(\d+) ns_per_reservation=([\d.]+)', text):
                samples[label].append(float(value))
                seen_samples[label].add(int(sample))
            if set(samples) != {'one_peer', 'sixty_four_peers'}:
                raise ValueError(f'{path.name}/{name}: expected admission cases '
                                 f'one_peer/sixty_four_peers, got {sorted(samples)}')
            for label, values in samples.items():
                if len(values) != 5 or sorted(seen_samples[label]) != [0, 1, 2, 3, 4]:
                    raise ValueError(f'{path.name}/{name}: admission case {label} has '
                                     f'samples {sorted(seen_samples[label])} (count={len(values)}), expected 0-4 '
                                     f'(truncated log cannot aggregate)')
                results[f'admission/{label}'] = {'metrics': {'ns': med(values)}, 'samples': values}
        elif name == 'peer_snapshot_microbench':
            for line in text.splitlines():
                if line.startswith('{"active_peers"'):
                    record = json.loads(line)
                    for strategy in record['strategies']:
                        if len(strategy['elapsed_ns']) != 6:
                            raise ValueError(f'{path.name}/{name}: snapshot '
                                             f'{record["active_peers"]}/{strategy["name"]} has '
                                             f'{len(strategy["elapsed_ns"])} samples, expected 6 '
                                             f'(truncated log cannot aggregate)')
                        values = [x / strategy['passes_per_sample'] for x in strategy['elapsed_ns']]
                        results[f'snapshot/{record["active_peers"]}/{strategy["name"]}'] = {
                            'metrics': {'ns': med(values)}, 'samples': values}
        elif name == 'bench_word_channel_read_against_bit_loop_reference':
            for label, value, unit in re.findall(r'(word|bit_loop)=([\d.]+)(ms|µs|ns|s)', text):
                milliseconds = float(value) * {'ms': 1, 'µs': .001, 'ns': .000001, 's': 1000}[unit]
                results[f'channel/{label}'] = {'metrics': {'completion_ms': milliseconds}}
        elif name == 'profile_receive_framing':
            scenario = None
            for line in text.splitlines():
                match = re.search(r'scenario=(\w+)', line)
                if match:
                    scenario = match[1]
                match = re.search(r'(merged|compact)\s*:\s*([\d.]+) ns/tick', line)
                if match:
                    results[f'framing/{scenario}/{match[1]}'] = {'metrics': {'ns': float(match[2])}}
        elif name == 'profile_750_clients_framing_cpu':
            for label, value in re.findall(r'(merged|compact)\s*:\s*([\d.]+) ms total', text):
                results[f'framing750/{label}'] = {'metrics': {'completion_ms': float(value)}}
        elif name == 'profile_gpu_distance_consumption':
            samples = collections.defaultdict(list)
            seen_rounds = collections.defaultdict(set)
            for phase, round_index, mode, value in re.findall(r'phase=([\d.]+) round=(\d+) mode=(\d+) pairs=3998000 elapsed_us=(\d+)', text):
                samples[f'gpu/{phase}/{mode}'].append(float(value) / 1000)
                seen_rounds[f'gpu/{phase}/{mode}'].add(int(round_index))
            for label, values in samples.items():
                if len(values) != 8 or sorted(seen_rounds[label]) != list(range(8)):
                    raise ValueError(f'{path.name}/{label}: profile_gpu_distance_consumption has '
                                     f'rounds {sorted(seen_rounds[label])}, expected 0-7 '
                                     f'(truncated log cannot aggregate)')
                results[label] = {'metrics': {'completion_ms': med(values)}, 'samples': values}
    if len(list(path.glob('*.status.json'))) != 12:
        raise ValueError(f'{path} does not contain all 12 micro cases')
    if len(results) != 37:
        raise ValueError(f'{path}: expected 37 parsed micro subcases, got {len(results)}')
    return results


def read_live(path):
    results = {}
    for summary in path.glob('*.summary.json'):
        record = json.loads(summary.read_text())
        case = summary.name.removesuffix('.summary.json')
        commands = [case] + ([case + '-voice-validation'] if case.startswith('voice-') else [])
        for command in commands:
            status_path = path / f'{command}.status.json'
            if not status_path.is_file():
                raise ValueError(f'{status_path}: command status is missing')
            status = json.loads(status_path.read_text())
            if status.get('exit_code') != 0 or status.get('timed_out', False):
                raise ValueError(f'{status_path}: command did not complete successfully')
        health = [json.loads(x) for x in (path / case / 'health.jsonl').read_text().splitlines()]
        retrans = health[-1]['extended']['reliable']['retransmits'] - health[0]['extended']['reliable']['retransmits']
        failed = [k for k, v in record['checks'].items() if not v]
        eligible = not [k for k in failed if k != 'zero_retransmits'] and retrans == 0
        result = {'metrics': record['metrics'], 'strict_valid': record['valid'],
                  'failed_checks': failed, 'measured_retransmits': retrans,
                  'setup_retransmits': health[0]['extended']['reliable']['retransmits'],
                  'eligible': eligible, 'minimum_sender_sends': record['minimum_sender_sends']}
        fields = ('clients', 'tokio_workers', 'server_rayon_threads_override', 'server_kind',
                  'server_avatar_diagnostics', 'warmup_seconds', 'measurement_window_seconds',
                  'server_port', 'health_port', 'server_ip', 'movement_interval_ms', 'jitter_percent',
                  'unity_frame_accumulator_fps', 'layout', 'pose', 'voice', 'p2p', 'server_profiling',
                  'client_shared_receive_disabled', 'voice_audio_folder', 'voice_speaker_percent',
                  'voice_frame_duration_ms', 'voice_reencode', 'expected_observer_peers',
                  'client_binary_sha256', 'server_config_fixture_sha256', 'client_config_fixture_sha256',
                  'server_config_local_copy_sha256')
        result['inputs'] = {key: record['metadata'][key] for key in fields}
        result['server_sha256'] = record['metadata']['server_binary_sha256']
        voice = path / case / 'validated-summary.json'
        if voice.exists():
            validated = json.loads(voice.read_text())
            result['voice'] = {key: validated[key] for key in
                               ('valid', 'capacity_pass', 'checks', 'capacity_checks', 'metrics')}
            result['eligible'] &= validated['valid'] and validated['capacity_pass']
        if case.startswith('voice-') and not voice.exists():
            raise ValueError(f'{voice} missing: run analyze-windows-voice.py')
        results[case] = result
    if len(results) != 4:
        raise ValueError(f'{path}: expected all four live cases, got {len(results)}')
    return results


def validate_trial_inputs(live, manifests, control, candidate, client_change_reason=None):
    """Allow only a declared client identity change; preserve every workload gate."""
    client_hashes = {}
    for variant, runs in live.items():
        manifest = manifests[variant]
        client_hash = manifest.get('client_sha256') or manifest['binaries'].get('client')
        if not client_hash:
            raise ValueError(f'{variant}: frozen client identity is missing')
        client_hashes[variant] = client_hash
        for run in runs:
            for case, record in run.items():
                if record['server_sha256'] != manifest['binaries']['server']:
                    raise ValueError(f'{variant}/{case}: server binary does not match frozen manifest')
                if record['inputs']['client_binary_sha256'] != client_hash:
                    raise ValueError(f'{variant}/{case}: client binary does not match frozen manifest')
    if client_change_reason and client_hashes[control] == client_hashes[candidate]:
        raise ValueError('Declared client change requires different frozen client identities')
    for case in live[control][0]:
        inputs = live[control][0][case]['inputs']
        expected = {k: v for k, v in inputs.items()
                    if not client_change_reason or k != 'client_binary_sha256'}
        for runs in live.values():
            for run in runs:
                actual = {k: v for k, v in run[case]['inputs'].items()
                          if not client_change_reason or k != 'client_binary_sha256'}
                if actual != expected:
                    raise ValueError(f'{case}: workload inputs differ across trials')


def validate_live_eligibility(live):
    """Reject any cohort containing a live trial that failed a workload gate."""
    failures = []
    for variant, runs in live.items():
        for trial_index, run in enumerate(runs):
            for case, record in run.items():
                if record.get('eligible') is not True:
                    failures.append(f'{variant}/trial[{trial_index}]/{case}')
    if failures:
        raise ValueError('ineligible live trials cannot be aggregated: ' + ', '.join(failures))


def validate_micro_manifests(captures, variant, tags, frozen_manifest):
    frozen_binaries = frozen_manifest.get('binaries', {})
    frozen_client = frozen_manifest.get('client_sha256') or frozen_binaries.get('client')
    signature = None
    for tag in tags:
        path = captures / f'{variant}-micro-{tag}' / 'manifest.json'
        if not path.is_file():
            raise ValueError(f'{path}: micro manifest is missing')
        manifest = json.loads(path.read_text())
        binaries = manifest.get('binaries')
        client = manifest.get('client_sha256')
        if not isinstance(binaries, dict) or not client:
            raise ValueError(f'{path}: binary map or client_sha256 is missing')
        current = (binaries, client)
        if signature is not None and current != signature:
            raise ValueError(f'{path}: micro binary identities differ across {variant} tags')
        signature = current
        for name, digest in binaries.items():
            expected = frozen_binaries.get(name)
            if expected is not None and digest != expected:
                raise ValueError(f'{path}: {name} hash differs from frozen {variant} manifest')
        if frozen_client is not None and client != frozen_client:
            raise ValueError(f'{path}: client hash differs from frozen {variant} manifest')


def validate_profile_manifest(path, variant, frozen_manifest, live_runs):
    if not path.is_file():
        raise ValueError(f'{path}: profile manifest is missing')
    manifest = json.loads(path.read_text())
    binaries = manifest.get('binaries', {})
    frozen_binaries = frozen_manifest.get('binaries', {})
    expected_server = frozen_binaries.get('server')
    expected_client = frozen_manifest.get('client_sha256') or frozen_binaries.get('client')
    live_servers = {run[case]['server_sha256'] for run in live_runs for case in run}
    live_clients = {run[case]['inputs']['client_binary_sha256'] for run in live_runs for case in run}
    if len(live_servers) != 1 or len(live_clients) != 1:
        raise ValueError(f'{variant}: live runs do not have single server/client identities')
    live_server = next(iter(live_servers))
    live_client = next(iter(live_clients))
    if expected_server is None:
        expected_server = live_server
    if expected_client is None:
        expected_client = live_client
    if expected_server != live_server or expected_client != live_client:
        raise ValueError(f'{variant}: frozen and live binary identities differ')
    if binaries.get('server') != live_server:
        raise ValueError(f'{path}: server hash differs from live/frozen {variant} identity')
    profile_client = manifest.get('client_sha256') or binaries.get('client')
    if profile_client != live_client:
        raise ValueError(f'{path}: client hash differs from live/frozen {variant} identity')


def validate_live_audio(captures, control, candidate, tags, manifests):
    """Require one identical voice corpus across every live tag and variant.

    Workload inputs carry only the audio folder path, so a content swap at
    the same path would otherwise compare different corpora. Every live
    capture manifest records audio_sha256 per clip; every manifest is
    required and every map must be nonempty and agree. Every supported
    complete optimization suite includes two voice cases, so all absent,
    partly absent, or empty audio maps raise. There is intentionally no
    audio-change exception for optimizations.
    """
    paths = {f'{variant}-live-{tag}': captures / f'{variant}-live-{tag}' / 'manifest.json'
             for variant in (control, candidate) for tag in tags}
    missing = sorted(key for key, path in paths.items() if not path.is_file())
    if missing:
        raise ValueError('live manifests are missing, cannot verify voice corpus identity: '
                         + ', '.join(missing))
    audio_maps = {}
    for key, path in paths.items():
        audio = json.loads(path.read_text()).get('audio_sha256')
        if not isinstance(audio, dict) or not audio:
            raise ValueError(f'{path}: audio_sha256 map is missing or empty')
        audio_maps[key] = audio
    reference = sorted(audio_maps)[0]
    for key in sorted(audio_maps)[1:]:
        if audio_maps[key] != audio_maps[reference]:
            raise ValueError(f'{key}: voice corpus audio hashes differ from {reference}')
    for variant in (control, candidate):
        frozen_audio = manifests[variant].get('audio_sha256')
        if frozen_audio and frozen_audio != audio_maps[reference]:
            raise ValueError(f'{variant}: frozen manifest audio hashes differ from live captures')
    return audio_maps[reference]


def _require_zero_lost_reports(profile_dir):
    """Mirror profile_samples lost-sample gate for both targets without perf."""
    for label in ('server', 'client'):
        report = profile_dir / f'{label}.report.txt'
        if not report.is_file():
            raise ValueError(f'{report}: perf report is missing; cannot verify lost samples')
        lost_matches = re.findall(r'^\s*#?\s*Total Lost Samples:\s*(\d+)\s*$',
                                  report.read_text(), re.MULTILINE)
        if len(lost_matches) != 1:
            raise ValueError(f'{report}: expected one parseable Total Lost Samples line')
        if int(lost_matches[0]) != 0:
            raise ValueError(f'{report}: profile lost {lost_matches[0]} samples')


def validate_profile_drivers(captures, control, candidate, profile_tags,
                             profile_target, manifests, live_runs,
                             live_audio_reference):
    """Verify paired profile inputs, status and lost samples without backfilling history."""
    dirs = {variant: captures / f'{variant}-profile-{tag}'
            for variant, tag in zip((control, candidate), profile_tags)}
    if not any(path.exists() for path in dirs.values()):
        return None
    for variant, path in dirs.items():
        if not path.exists():
            raise ValueError(f'{path}: paired profile capture is missing; '
                             f'both {control} and {candidate} profile dirs must exist')
        manifest_path = path / 'manifest.json'
        if not manifest_path.is_file():
            raise ValueError(f'{manifest_path}: profile manifest is missing')
        for label in ('server', 'client'):
            data_path = path / f'{label}.data'
            if not data_path.is_file() or data_path.stat().st_size == 0:
                raise ValueError(f'Incomplete profile capture: {data_path} is missing')
    loaded = {variant: json.loads((dirs[variant] / 'manifest.json').read_text())
              for variant in dirs}
    has_inputs = {variant: 'profile_inputs' in loaded[variant] for variant in dirs}
    if not any(has_inputs.values()):
        if any(m.get('audio_sha256') for m in loaded.values()):
            raise ValueError('voice profiles require profile_inputs; legacy support is avatar-only')
        for variant, path in dirs.items():
            status_path = path / 'suite-status.json'
            if not status_path.is_file():
                raise ValueError(f'{status_path}: legacy profile suite-status is missing; '
                                 f'historical default drivers must record failed_commands')
            try:
                status = json.loads(status_path.read_text())
            except (json.JSONDecodeError, UnicodeDecodeError) as error:
                raise ValueError(f'{status_path}: legacy profile suite-status is unreadable: {error}')
            if status.get('failed_commands') != []:
                raise ValueError(f'{status_path}: legacy profile capture failed: '
                                 f'{status.get("failed_commands")}')
        for path in dirs.values():
            _require_zero_lost_reports(path)
        return None
    if not all(has_inputs.values()):
        missing = sorted(variant for variant, present in has_inputs.items() if not present)
        raise ValueError('profile workload inputs differ across variants: '
                         f'missing profile_inputs on {", ".join(missing)}')
    required_inputs = {'clients', 'speakers', 'voice_speaker_percent', 'workers',
                       'rayon_threads', 'server_tokio_workers', 'movement_interval_ms', 'unity_frame_accumulator_fps',
                       'voice_frame_duration_ms', 'voice_reencode', 'server_config_fixture_sha256',
                       'server_port', 'health_port', 'warmup_seconds', 'measurement_window_seconds',
                       'no_server_avatar_diagnostics', 'no_client_shared_receive',
                       'client_voice_all_clients', 'server_voice_diagnostics'}
    for variant in dirs:
        inputs = loaded[variant].get('profile_inputs')
        if not isinstance(inputs, dict) or not inputs:
            raise ValueError(f'{dirs[variant] / "manifest.json"}: '
                             f'profile_inputs map is missing or empty')
        if not required_inputs.issubset(inputs):
            raise ValueError(f'{dirs[variant]}: required profile_inputs fields are missing')
    environments = {}
    for variant, manifest in loaded.items():
        environment = manifest.get('relevant_environment')
        if not isinstance(environment, dict):
            raise ValueError(f'{dirs[variant]}: profile worker environment is missing')
        if environment.get('TOKIO_WORKER_THREADS') != str(manifest['profile_inputs']['server_tokio_workers']):
            raise ValueError(f'{dirs[variant]}: profile Tokio worker environment differs from declared inputs')
        if environment.get('RAYON_NUM_THREADS') != str(manifest['profile_inputs']['rayon_threads']):
            raise ValueError(f'{dirs[variant]}: profile Rayon worker environment differs from declared inputs')
        environments[variant] = environment
    if environments[control] != environments[candidate]:
        raise ValueError('profile worker environments differ across variants')
    if loaded[control]['profile_inputs'] != loaded[candidate]['profile_inputs']:
        raise ValueError('profile workload inputs differ across variants')
    live_fixtures = {run[case]['inputs'].get('server_config_fixture_sha256')
                     for runs in live_runs.values() for run in runs for case in run}
    if len(live_fixtures) != 1 or next(iter(live_fixtures)) is None:
        raise ValueError('live runs do not have a single fixture identity')
    live_fixture = next(iter(live_fixtures))
    for variant in dirs:
        manifest = loaded[variant]
        inputs = manifest['profile_inputs']
        if manifest.get('fixture_sha256') != live_fixture:
            raise ValueError(f'{dirs[variant] / "manifest.json"}: '
                             f'fixture hash does not match live fixture')
        if inputs.get('server_config_fixture_sha256') != live_fixture:
            raise ValueError(f'{dirs[variant] / "manifest.json"}: '
                             f'profile_inputs fixture hash does not match live fixture')
        frozen_fixture = manifests[variant].get('fixture_sha256')
        if frozen_fixture is not None and frozen_fixture != live_fixture:
            raise ValueError(f'{variant}: frozen manifest fixture hash differs from live captures')
        audio = manifest.get('audio_sha256')
        if not isinstance(audio, dict) or not audio:
            raise ValueError(f'{dirs[variant] / "manifest.json"}: '
                             f'profile voice corpus audio_sha256 map is missing or empty')
    if loaded[control].get('audio_sha256') != loaded[candidate].get('audio_sha256'):
        raise ValueError('profile voice corpus audio hashes differ across variants')
    if loaded[control].get('audio_sha256') != live_audio_reference:
        raise ValueError('profile voice corpus audio hashes differ from live captures')
    for variant, path in dirs.items():
        status_path = path / 'driver-status.json'
        if not status_path.is_file():
            raise ValueError(f'{status_path}: profile driver-status is missing')
        try:
            status = json.loads(status_path.read_text())
        except (json.JSONDecodeError, UnicodeDecodeError) as error:
            raise ValueError(f'{status_path}: profile driver-status is unreadable: {error}')
        if status.get('capture_ok') is not True:
            raise ValueError(f'{status_path}: profile capture_ok is not True')
        harness = status.get('harness') or {}
        if harness.get('exit_code') != 0 or harness.get('timed_out') is not False:
            raise ValueError(f'{status_path}: profile harness did not complete successfully')
        for section in ('profilers', 'reports'):
            entries = status.get(section) or {}
            for label in ('server', 'client'):
                entry = entries.get(label) or {}
                if entry.get('ok') is not True or entry.get('exit_code') != 0:
                    raise ValueError(f'{status_path}: profile {section}[{label}] is not ok')
                if section == 'reports' and 'Total Lost Samples: 0' not in str(
                        entry.get('lost_samples_header')):
                    raise ValueError(f'{status_path}: profile reports[{label}] lost-sample header '
                                     f'is not zero')
        validation = status.get('validation') or {}
        if len(validation) != 2:
            raise ValueError(f'{status_path}: profile validation must cover avatar and voice')
        for label in ('avatar', 'voice'):
            entry = validation.get(label) or {}
            if entry.get('ok') is not True or entry.get('exit_code') != 0:
                raise ValueError(f'{status_path}: profile validation[{label}] is not ok')
        voice = validation.get('voice') or {}
        if voice.get('valid') is not True or voice.get('capacity_pass') is not True:
            raise ValueError(f'{status_path}: profile voice validation gates are not True')
        voice_csv = status.get('server_voice_csv') or {}
        if voice_csv.get('ok') is not True:
            raise ValueError(f'{status_path}: profile server voice csv is not ok')
    for path in dirs.values():
        _require_zero_lost_reports(path)
    return loaded[control]['profile_inputs']


def aggregate(records, control, candidate):
    result = {}
    cases = set(records[control][0])
    assert all(set(run) == cases for runs in records.values() for run in runs)
    for runs in records.values():
        for run in runs:
            for case, record in run.items():
                if 'eligible' in record and record['eligible'] is not True:
                    raise ValueError(f'{case}: ineligible trial cannot be aggregated; '
                                     f'raw captures are preserved, never filtered')
    for case in sorted(cases):
        metrics = set(records[control][0][case]['metrics'])
        result[case] = {}
        for key in sorted(metrics):
            values = {variant: [run[case]['metrics'][key] for run in runs]
                      for variant, runs in records.items()}
            if not all(isinstance(x, (int, float)) for group in values.values() for x in group):
                continue
            a, b = med(values[control]), med(values[candidate])
            result[case][key] = {'control': a, 'candidate': b,
                                'change_percent': (b / a - 1) * 100 if a else None,
                                'samples': values}
    return result


def profile_samples(path):
    report = path.with_suffix('.report.txt')
    if not report.is_file():
        raise ValueError(f'{report}: perf report is missing; cannot verify lost samples')
    lost_matches = re.findall(r'^\s*#?\s*Total Lost Samples:\s*(\d+)\s*$',
                              report.read_text(), re.MULTILINE)
    if len(lost_matches) != 1:
        raise ValueError(f'{report}: expected one parseable Total Lost Samples line')
    lost_samples = int(lost_matches[0])
    if lost_samples != 0:
        raise ValueError(f'{report}: profile lost {lost_samples} samples')

    export = path.with_suffix('.leaf-samples.txt')
    if not export.exists() or not export.stat().st_size:
        temporary = export.with_suffix('.tmp')
        try:
            with temporary.open('w') as stream:
                subprocess.run(['perf', 'script', '--ns', '--hide-call-graph', '-F', 'time,period,ip,sym',
                                '-i', str(path)], stdout=stream, check=True)
            temporary.replace(export)
        except Exception:
            temporary.unlink(missing_ok=True)
            raise
    samples = []
    for line in export.read_text().splitlines():
        match = re.match(r'\s*([\d.]+):\s+(\d+)\s+[0-9a-f]+\s+(.+)', line)
        if match:
            samples.append((float(match[1]), int(match[2]), match[3]))
    if not samples:
        raise ValueError(f'No perf samples parsed from {export}')
    first = min(t for t, _, _ in samples)
    totals = collections.Counter()
    bins = collections.defaultdict(collections.Counter)
    for t, period, symbol in samples:
        totals[symbol] += period
        bins[int(t - first)][symbol] += period
    total = sum(totals.values())
    return {'sample_count': len(samples), 'lost_samples': lost_samples, 'period_sum': total,
            'complete_seconds': int(max(t for t, _, _ in samples) - first),
            'shares': {s: v / total * 100 for s, v in totals.items()},
            'bins': {str(t): {s: n / sum(v.values()) * 100 for s, n in v.items()}
                     for t, v in bins.items()}}


def plots(summary, prefix):
    import numpy as np
    import matplotlib
    matplotlib.use('Agg')
    import matplotlib.pyplot as plt
    from matplotlib.colors import TwoSlopeNorm

    live = summary['live_aggregate']
    cpu_target = summary.get('profile_target', 'server')
    keys = ['gap_p95_ms', 'observer_items_per_second', f'{cpu_target}_cpu_cores', 'combined_cpu_cores']
    labels = ['Avatar p95\ngap', 'Applied\nitems/s', f'{cpu_target.title()}\nCPU', 'Combined\nCPU']
    cases = list(live)
    def percentage(value):
        return float('nan') if value is None else value
    matrix = [[percentage(live[case][key]['change_percent']) for key in keys] for case in cases]
    fig, ax = plt.subplots(figsize=(8, 3.7), constrained_layout=True)
    signed = [[v * (-1 if key == 'observer_items_per_second' else 1)
               for key, v in zip(keys, row)] for row in matrix]
    scale = max([5] + [abs(x) for row in signed for x in row if np.isfinite(x)])
    image = ax.imshow(signed, aspect='auto', cmap='RdYlGn_r', norm=TwoSlopeNorm(vcenter=0, vmin=-scale, vmax=scale))
    ax.set_xticks(range(len(keys)), labels)
    ax.set_yticks(range(len(cases)), cases)
    ax.set_title('Live workload changes · two trials per variant')
    for i, row in enumerate(matrix):
        for j, value in enumerate(row):
            label = f'{value:+.1f}%' if np.isfinite(value) else 'n/a'
            ax.text(j, i, label, ha='center', va='center')
    fig.colorbar(image, ax=ax, label='Normalized change (%)')
    ax.set_xlabel('Green improves; red worsens. Mean trial quantiles, not pooled quantiles.', fontsize=9)
    for extension in ('png', 'svg'):
        fig.savefig(str(prefix) + '-benchmarks.' + extension, dpi=160)
    plt.close(fig)

    micro = summary['micro_aggregate']
    cases = list(micro)
    matrix = []
    for case in cases:
        record = micro[case]
        key = next(key for key in ('ns', 'completion_ms', 'p95_ms') if key in record)
        row = [percentage(record[key]['change_percent']),
               percentage(record.get('rate', {}).get('change_percent')),
               float('nan')]
        if case.startswith(('registry-', 'reliable-')):
            values = {variant: [run[case]['counters']['task-clock'] for run in runs]
                      for variant, runs in summary['micro_runs'].items()}
            control, candidate = summary['variant_order']
            baseline = med(values[control])
            row[2] = (med(values[candidate]) / baseline - 1) * 100 if baseline else float('nan')
        matrix.append(row)
    signed = np.array(matrix) * [1, -1, 1]
    fig, ax = plt.subplots(figsize=(9, 12), constrained_layout=True)
    image = ax.imshow(np.ma.masked_invalid(signed), aspect='auto', cmap='RdYlGn_r',
                      norm=TwoSlopeNorm(vcenter=0, vmin=-20, vmax=20))
    labels = [case.replace('dc122ac_fresh_all_snapshot', 'fresh-all')
              .replace('filtered_reused_snapshot', 'active-reused')
              .replace('main_guard_scan', 'guard') for case in cases]
    ax.set_yticks(range(len(cases)), labels)
    ax.set_xticks(range(3), ['Time / p95', 'Requests/s', 'Process CPU'])
    ax.set_title(f'All {len(cases)} low-level subcases · two trials per variant')
    for i, row in enumerate(matrix):
        for j, value in enumerate(row):
            if np.isfinite(value):
                ax.text(j, i, f'{value:+.1f}%', ha='center', va='center', fontsize=9)
    fig.colorbar(image, ax=ax, label='Normalized change (%) · color saturates at ±20%')
    ax.set_xlabel('Process CPU includes setup and driver. Blank = unavailable.\nGreen improves; red worsens. Unchanged-code drift is not an optimization gain.', fontsize=9, labelpad=12)
    for extension in ('png', 'svg'):
        fig.savefig(str(prefix) + '-micro.' + extension, dpi=160)
    plt.close(fig)

    profiles = summary['profiles']
    if profiles:
        totals = collections.Counter()
        for record in profiles.values():
            totals.update(record['shares'])
        symbols = [s for s, _ in totals.most_common(14)]
        def label(symbol):
            if 'core::iter::adapters::map::Map' in symbol:
                return 'Iterator fold'
            if 'dashmap::DashMap' in symbol:
                return 'Peer map lookup'
            if 'alloc::vec::spec_from_iter' in symbol:
                return 'Vector collection'
            if 'ZSTD_compressBlock_fast_extDict' in symbol:
                return 'Zstd dictionary block'
            if 'ZSTD_compressBlock_fast_dictMatch' in symbol:
                return 'Zstd dictionary match'
            name = symbol.replace('basis_server_core::avatar_sync::AvatarSyncSystem::', '')
            name = name.replace('basis_server_core::avatar_sync::', '')
            name = name.replace('basis_client_core::avatar::PoseState::', '')
            name = name.replace('basis_client_core::avatar::', '').replace('basis_client_core::', '')
            name = name.replace('basis_protocol::avatar_delta::', '').replace('bytes::bytes::', '')
            return name if len(name) <= 28 else name[:25] + '…'
        names = [label(symbol) for symbol in symbols]
        fig, axes = plt.subplots(1, len(profiles), figsize=(8.5, 6), constrained_layout=True, sharey=True, squeeze=False)
        fig.suptitle(f'Native {summary.get("profile_target", "server")} user-cycle heatmap')
        axes = list(axes[0])
        for ax, (variant, record) in zip(axes, profiles.items()):
            # The last time bin may contain only a few milliseconds of samples.
            # Keep complete seconds so a partial-bin spike is not drawn as 1 s.
            duration = record['complete_seconds']
            matrix = [[record['bins'].get(str(t), {}).get(symbol, 0) for t in range(duration)] for symbol in symbols]
            image = ax.imshow(matrix, aspect='auto', cmap='inferno', vmin=0, vmax=35,
                              extent=(0, duration, len(symbols)-.5, -.5))
            ax.set_title(f'{variant} · {record["sample_count"]:,} samples')
            ax.set_xlabel('Seconds within 30-second recording')
            ax.set_yticks(range(len(symbols)), names)
        fig.colorbar(image, ax=list(axes), extend='max',
                     label='Estimated user-cycle share per second (%) · color saturates at 35%')
        for extension in ('png', 'svg'):
            fig.savefig(str(prefix) + '-profile.' + extension, dpi=160)
        plt.close(fig)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('captures', type=pathlib.Path)
    parser.add_argument('--control', default='baseline')
    parser.add_argument('--candidate', default='candidate')
    parser.add_argument('--tags', nargs='+', default=['abba1', 'abba2'])
    parser.add_argument('--micro-tags', nargs='+', help='separate adjacent confirmation tags for low-level trials')
    parser.add_argument('--output', type=pathlib.Path, required=True, help='output filename prefix')
    parser.add_argument('--no-plots', action='store_true')
    parser.add_argument('--profile-tags', nargs=2, default=['initial', 'final'])
    parser.add_argument('--profile-target', choices=['server', 'client'], default='server')
    parser.add_argument('--client-change-reason', help='explicitly declare a changed workload client; all other inputs must match')
    args = parser.parse_args()
    micro_tags = args.micro_tags or args.tags
    micro, live, profiles, manifests = {}, {}, {}, {}
    for variant in (args.control, args.candidate):
        manifest = args.captures / variant / 'manifest.json'
        if not manifest.exists():
            manifest = args.captures / f'{variant}-live-{args.tags[0]}' / 'manifest.json'
        manifests[variant] = json.loads(manifest.read_text())
        validate_micro_manifests(args.captures, variant, micro_tags, manifests[variant])
        micro[variant] = [read_micro(args.captures / f'{variant}-micro-{tag}') for tag in micro_tags]
        live[variant] = [read_live(args.captures / f'{variant}-live-{tag}') for tag in args.tags]
    validate_trial_inputs(live, manifests, args.control, args.candidate, args.client_change_reason)
    validate_live_eligibility(live)
    live_audio_reference = validate_live_audio(args.captures, args.control, args.candidate, args.tags, manifests)
    profile_pair_inputs = validate_profile_drivers(args.captures, args.control, args.candidate,
                                                   args.profile_tags, args.profile_target,
                                                   manifests, live, live_audio_reference)
    for variant, profile_tag in zip([args.control, args.candidate], args.profile_tags):
        data = args.captures / f'{variant}-profile-{profile_tag}' / f'{args.profile_target}.data'
        if data.exists():
            validate_profile_manifest(data.parent / 'manifest.json', variant,
                                      manifests[variant], live[variant])
            profiles[variant] = profile_samples(data)
        elif data.parent.exists():
            raise ValueError(f'Incomplete profile capture: {data} is missing')
    # Published summaries keep input identities without machine-local paths.
    for runs in micro.values():
        for run in runs:
            for record in run.values():
                if 'config' in record.get('details', {}):
                    record['details']['config'] = pathlib.Path(record['details']['config']).name
    for runs in live.values():
        for run in runs:
            for record in run.values():
                folder = record['inputs']['voice_audio_folder']
                if folder:
                    record['inputs']['voice_audio_folder'] = pathlib.Path(folder).name
    summary = {'micro_runs': micro, 'live_runs': live,
               'variant_order': [args.control, args.candidate],
               'trial_tags': {'live': args.tags, 'micro': micro_tags},
               'micro_command_counters': {
                   variant: [{file.stem.removesuffix('.perf'): counter_metrics(file)
                              for file in (args.captures / f'{variant}-micro-{tag}').glob('*.perf.csv')}
                             for tag in micro_tags]
                   for variant in (args.control, args.candidate)},
               'micro_aggregate': aggregate(micro, args.control, args.candidate),
               'live_aggregate': aggregate(live, args.control, args.candidate),
               'profiles': profiles, 'profile_target': args.profile_target,
               'profile_inputs': profile_pair_inputs,
               'client_change_reason': args.client_change_reason, 'manifests': manifests,
               'live_all_eligible': all(record['eligible'] for runs in live.values()
                                        for run in runs for record in run.values())}
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.with_suffix('.json').write_text(json.dumps(summary, indent=2) + '\n')
    if not args.no_plots:
        plots(summary, args.output)
    print(json.dumps({'live_all_eligible': summary['live_all_eligible'],
                      'micro_cases': len(summary['micro_aggregate']),
                      'live_cases': len(summary['live_aggregate'])}))


if __name__ == '__main__':
    main()
