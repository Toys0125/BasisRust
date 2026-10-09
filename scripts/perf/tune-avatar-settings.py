#!/usr/bin/env python3
"""Find measured flush-lane preferences using frozen local loopback workloads.

This is a SETTINGS comparison (one server binary, distinct validated settings),
separate from the Windows binary/revision crossover comparator.
"""
import argparse
import contextlib
import json
import math
import os
import pathlib
import platform
import re
import shutil
import signal
import socket
import subprocess
import sys
import tempfile

from avatar_benchmark import ROOT, cpu_list, relevant_env, run_workload, select_port, write_json
from avatar_tuning import csv_rows, rank, report, run_order, sha256, summarize, validate_series, xml_values

FIXTURES = ROOT / 'docs/performance/fixtures'
PRESETS = {'quick': (5, 15, 2), 'screen': (15, 60, 2), 'confirm': (30, 60, 4)}
CALIBRATION_WARMUP_SECONDS = 5
CALIBRATION_WINDOW_SECONDS = 10
DEFAULT_CLIENTS = 250


def lane_values(text):
    if not re.fullmatch(r'[0-8](?:,[0-8])+', text):
        raise argparse.ArgumentTypeError('Supply at least two different flush lanes in 0..8, e.g. 0,6')
    lanes = [int(n) for n in text.split(',')]
    if len(set(lanes)) != len(lanes):
        raise argparse.ArgumentTypeError('Flush settings must be different; duplicate lanes are not a settings comparison')
    return lanes


def parser():
    result = argparse.ArgumentParser(description=__doc__)
    extension = '.exe' if os.name == 'nt' else ''
    result.add_argument('--output', type=pathlib.Path, help='new capture directory (never overwrites)')
    result.add_argument('--analyze', type=pathlib.Path, help='read a completed/failed capture without running processes')
    result.add_argument('--server', type=pathlib.Path, default=ROOT / ('BasisRustServer/target/release/basis-server-console' + extension))
    result.add_argument('--client', type=pathlib.Path, default=ROOT / ('BasisRustClient/target/release/basis-rust-client' + extension))
    result.add_argument('--server-config', type=pathlib.Path, default=FIXTURES / 'avatar-cpu-only-server.xml')
    result.add_argument('--client-config', type=pathlib.Path, default=FIXTURES / 'avatar-1500-client.xml')
    result.add_argument('--lanes', type=lane_values, default=lane_values('0,6'), help='only sweep dimension; default 0,6; range 0..8')
    result.add_argument('--mode', choices=PRESETS, default='screen')
    workload = result.add_mutually_exclusive_group()
    workload.add_argument('--clients', type=int,
                          help=f'fixed offered user count; defaults to {DEFAULT_CLIENTS}')
    workload.add_argument('--auto-calibrate', action='store_true',
                          help='opt in to selecting a fixed population near --network-capacity-mbps')
    result.add_argument('--network-capacity-mbps', type=float, default=1000,
                        help='measured server TX target/capacity reference; default 1000 Mbps; auto calibration uses this target')
    result.add_argument('--warmup-seconds', type=int)
    result.add_argument('--window-seconds', type=int)
    result.add_argument('--repeats', type=int, help='even number >=2 per setting; default 2 screen/quick, 4 confirm')
    result.add_argument('--rayon-threads', type=int, help='fixed server workers; 0 = Rayon automatic, omitted = platform default')
    result.add_argument('--tokio-workers', type=int, help='fixed server Tokio workers; >=1, omitted = available parallelism')
    result.add_argument('--client-workers', type=int, default=4)
    result.add_argument('--port', type=int, default=0, help='UDP port; 0 selects once, then freezes it for all runs')
    result.add_argument('--health-port', type=int, default=0, help='loopback TCP port; 0 selects once')
    result.add_argument('--server-cpus', help='CPU IDs/ranges; Linux taskset or Windows single-group affinity')
    result.add_argument('--client-cpus', help='CPU IDs/ranges; keep fixed across settings')
    result.add_argument('--startup-timeout', type=int, default=30)
    result.add_argument('--ready-timeout', type=int, default=360)
    result.add_argument('--binary-build-note', default='unknown; local toolchain/source metadata does not attest binary origin', help='optional user-supplied binary build/revision description')
    return result


def validate(args):
    if platform.system() not in ('Linux', 'Windows'):
        raise ValueError('Only Linux /proc and Windows native process counters are supported')
    if args.output is None or args.output.exists():
        raise ValueError('--output must be a new directory')
    for index, key in enumerate(('warmup_seconds', 'window_seconds', 'repeats')):
        if getattr(args, key) is None:
            setattr(args, key, PRESETS[args.mode][index])
    resolve_client_selection(args)
    if (args.clients is not None and args.clients < 2) or args.warmup_seconds < 0 or args.window_seconds < 2 or not math.isfinite(args.network_capacity_mbps) or args.network_capacity_mbps <= 0:
        raise ValueError('Require clients >=2 when specified, warmup >=0, window >=2, and network target >0 Mbps')
    if args.repeats < 2 or args.repeats % 2:
        raise ValueError('Repeats must be even and >=2 to counterbalance run order')
    if args.rayon_threads is not None and args.rayon_threads < 0:
        raise ValueError('Rayon threads must be >=0')
    if args.client_workers < 1 or (args.tokio_workers is not None and args.tokio_workers < 1):
        raise ValueError('Tokio/client workers must be >=1')
    if args.startup_timeout < 1 or args.ready_timeout < 1:
        raise ValueError('Timeouts must be positive')
    if any(not 0 <= p <= 65535 for p in (args.port, args.health_port)):
        raise ValueError('Ports must be 0..65535')
    for key in ('server', 'client', 'server_config', 'client_config'):
        value = getattr(args, key).resolve()
        if not value.is_file():
            raise ValueError(f'{key} file missing: {value}')
        setattr(args, key, value)
    if args.server == args.client:
        raise ValueError('Server and client must be distinct executables')
    if b'BASIS_AVATAR_FLUSH_LANES' not in args.server.read_bytes():
        raise ValueError('Server lacks the flush-lane capability marker; build PR27 or later (this check is not binary attestation)')
    server, client = xml_values(args.server_config), xml_values(args.client_config)
    required = {'ConfigVersion': '14', 'HealthIncludeExtendedMetrics': 'true', 'HealthIncludeBSRProfiling': 'false',
                'EnableBSRProfiling': 'false', 'EnableConsole': 'false', 'HealthPath': '/health',
                'SimulatePacketLoss': 'false', 'SimulateLatency': 'false', 'ReuseAddresss': 'false',
                'Ipv6Enabled': 'true', 'OverrideAutoDiscoveryOfIpv': 'false', 'IPv6Address': '::1',
                'EnableComputeOffload': 'false', 'UseAuth': 'true', 'UseAuthIdentity': 'true'}
    for key, value in required.items():
        if server.get(key) != value:
            raise ValueError(f'Server fixture requires {key}={value} for this CPU-only loopback experiment')
    if client.get('VoiceEnabled') != 'false':
        raise ValueError('Client fixture requires VoiceEnabled=false')
    for key in ('Password', 'CompanyName', 'ProductName'):
        if not client.get(key) or client[key] != server.get(key):
            raise ValueError(f'Server/client fixture {key} must match and be nonempty')
    if args.clients is not None and int(server.get('PeerLimit', '0')) < args.clients:
        raise ValueError('Fixture PeerLimit is lower than requested clients')
    for cpus in (args.server_cpus, args.client_cpus):
        if cpus:
            cpu_list(cpus)
            if os.name != 'nt' and not shutil.which('taskset'):
                raise ValueError('Linux affinity requires taskset on PATH')
    args.output = args.output.resolve()


def resolve_client_selection(args):
    """Resolve omission to the compatible fixed default; calibration stays opt-in."""
    if args.auto_calibrate and args.clients is not None:
        raise ValueError('--auto-calibrate cannot be combined with --clients')
    if args.clients is None and not args.auto_calibrate:
        args.clients = DEFAULT_CLIENTS
    return args


def interpolate_health_counter(samples, boundary, group, key):
    """Interpolate one cumulative health counter at a monotonic boundary."""
    section = 'transport' if group == 'transport' else 'extended'

    def value(sample):
        data = sample['health'][section]
        if section == 'extended':
            data = data[group]
        return float(data[key])

    for first, last in zip(samples, samples[1:]):
        start, end = float(first['monotonic_seconds']), float(last['monotonic_seconds'])
        if start <= boundary <= end:
            if not math.isfinite(start) or not math.isfinite(end) or end <= start:
                raise ValueError('Health samples have a nonpositive monotonic interval')
            first_value, last_value = value(first), value(last)
            if not math.isfinite(first_value) or not math.isfinite(last_value):
                raise ValueError(f'Invalid health counter: {group}.{key}')
            return first_value + (last_value - first_value) * (boundary - start) / (end - start)
    raise ValueError('Health samples do not bracket the calibration measurement window')


def clean_environment(server_config, client_config):
    # Server supports PascalCase overrides, including fields absent in fixtures.
    source = ROOT / 'BasisRustServer/crates/basis-protocol/src/config.rs'
    overrides = set(re.findall(r'(?:override_field|override_string)!\(\s*"([^"]+)"', source.read_text()))
    overrides |= set(xml_values(server_config)) | set(xml_values(client_config))
    override_keys = {key.upper() for key in overrides}
    cleared = sorted(k for k in os.environ if k.upper() in override_keys or k.upper().startswith('BASIS_') or k.upper() in ('RAYON_NUM_THREADS', 'TOKIO_WORKER_THREADS'))
    return {k: v for k, v in os.environ.items() if k not in cleared}, cleared


def next_calibration_clients(current, target_mbps, measured_mbps, maximum):
    """Scale population toward target TX, assuming dense fanout grows quadratically."""
    if not math.isfinite(measured_mbps) or measured_mbps <= 0:
        raise ValueError('Calibration measured no positive server TX traffic')
    factor = math.sqrt(target_mbps / measured_mbps)
    factor = min(2.0, max(0.5, factor))
    candidate = min(maximum, max(2, round(current * factor)))
    if candidate == current and abs(measured_mbps / target_mbps - 1) > 0.10:
        candidate = min(maximum, current + 1) if measured_mbps < target_mbps else max(2, current - 1)
    return candidate


def calibration_tx_mbps(byte_delta, window_start_monotonic, window_end_monotonic):
    """Convert transmitted bytes to Mbps over the recorded monotonic interval."""
    elapsed = float(window_end_monotonic) - float(window_start_monotonic)
    byte_delta = float(byte_delta)
    if not math.isfinite(elapsed) or elapsed <= 0:
        raise ValueError('Calibration measurement interval must be finite and positive')
    if not math.isfinite(byte_delta) or byte_delta <= 0:
        raise ValueError('Calibration measured no positive server TX bytes')
    return byte_delta * 8 / elapsed / 1_000_000


def calibrate_clients(args, server_env, client_env, output, maximum):
    """Use short loopback runs to select a fixed population near target TX."""
    if not args.auto_calibrate:
        return {'mode': 'fixed_clients', 'selected_clients': args.clients, 'target_tx_mbps': args.network_capacity_mbps,
                'attempts': []}
    if maximum < 2:
        raise ValueError('Fixture PeerLimit must allow at least 2 clients for automatic calibration')

    target = args.network_capacity_mbps
    candidate = min(1000, maximum)
    original_warmup, original_window = args.warmup_seconds, args.window_seconds
    attempts = []
    calibration_root = output / 'calibration'
    calibration_root.mkdir()
    args.warmup_seconds, args.window_seconds = CALIBRATION_WARMUP_SECONDS, CALIBRATION_WINDOW_SECONDS
    try:
        for index in range(1, 5):
            args.clients = candidate
            run_dir = calibration_root / f'{index:02d}-clients-{candidate}'
            print(f"CALIBRATE {index}/4 ({candidate} clients toward {target:g} Mbps TX)", flush=True)
            run_workload(args, run_dir, dict(server_env, BASIS_AVATAR_FLUSH_LANES=str(args.lanes[0])), client_env)
            meta = json.loads((run_dir / 'run.json').read_text())
            if not meta['completed'] or meta['error'] is not None or meta['client_exit_code'] != 0 or meta['server_exit_code'] not in (0, -2, 3221225786):
                raise RuntimeError(f"Calibration run {index} did not complete cleanly; evidence retained in {run_dir}")
            samples = [json.loads(line) for line in (run_dir / 'samples.jsonl').read_text().splitlines()]
            begin = interpolate_health_counter(samples, meta['window_start_monotonic'], 'rawUdp', 'bytesOut')
            end = interpolate_health_counter(samples, meta['window_end_monotonic'], 'rawUdp', 'bytesOut')
            if end < begin:
                raise ValueError('rawUdp.bytesOut reset during calibration')
            measured = calibration_tx_mbps(end - begin, meta['window_start_monotonic'], meta['window_end_monotonic'])
            pilot_errors = {}
            for group, key in (('appMessages', 'protocolErrors'), ('rawUdp', 'wouldBlock'),
                               ('reliable', 'retransmits'), ('transport', 'nonReliableDroppedDatagrams')):
                first = interpolate_health_counter(samples, meta['window_start_monotonic'], group, key)
                last = interpolate_health_counter(samples, meta['window_end_monotonic'], group, key)
                if last < first:
                    raise ValueError(f'{group}.{key} reset during calibration')
                pilot_errors[key] = last - first
            senders = csv_rows(run_dir / 'observer.sender.csv')
            minimum_sent = min((int(row['socket_sent_full']) + int(row['socket_sent_delta']) for row in senders), default=0)
            attempt = {'run': run_dir.name, 'clients': candidate, 'server_tx_mbps': measured,
                       'sender_count': len(senders), 'minimum_sender_items': minimum_sent,
                       'measurement_window_errors': pilot_errors, 'complete': True}
            attempts.append(attempt)
            write_json(calibration_root / 'calibration.json', {'target_tx_mbps': target, 'tolerance_percent': 10,
                                                                'attempts': attempts, 'selected_clients': None})
            if len(senders) != candidate or any(int(row['send_errors']) for row in senders) or minimum_sent < 10 * 50 * .9:
                raise RuntimeError(f"Calibration sender workload failed at {candidate} clients; evidence retained in {run_dir}")
            if any(count > .01 for count in pilot_errors.values()):
                raise RuntimeError(f"Calibration had transport errors in its measured window at {candidate} clients; evidence retained in {run_dir}")
            if abs(measured / target - 1) <= .10:
                selection = {'mode': 'auto_server_tx', 'selected_clients': candidate, 'target_tx_mbps': target,
                             'tolerance_percent': 10, 'attempts': attempts}
                write_json(calibration_root / 'calibration.json', selection)
                print(f"CALIBRATED {candidate} clients at {measured:.1f} Mbps server TX", flush=True)
                return selection
            next_candidate = next_calibration_clients(candidate, target, measured, maximum)
            if next_candidate == candidate:
                raise RuntimeError(f"Cannot reach {target:g} Mbps TX within the fixture's {maximum}-client limit; latest rate {measured:.1f} Mbps")
            candidate = next_candidate
        raise RuntimeError(f"Could not reach {target:g} Mbps TX within four calibration attempts; set --clients explicitly or change --network-capacity-mbps")
    except BaseException as exc:
        write_json(calibration_root / 'calibration.json', {'target_tx_mbps': target, 'tolerance_percent': 10,
                                                            'attempts': attempts, 'error': f'{type(exc).__name__}: {exc}'})
        raise
    finally:
        args.warmup_seconds, args.window_seconds = original_warmup, original_window


@contextlib.contextmanager
def experiment_lock():
    """One tuning experiment at a time per host/user, including separate ports."""
    path = pathlib.Path(tempfile.gettempdir()) / 'basis-avatar-settings.lock'
    with path.open('a+b') as stream:
        if os.name == 'nt':
            import msvcrt
            if stream.tell() == 0:
                stream.write(b'0')
                stream.flush()
            stream.seek(0)
            try:
                msvcrt.locking(stream.fileno(), msvcrt.LK_NBLCK, 1)
            except OSError as exc:
                raise RuntimeError('Another settings experiment holds the host lock') from exc
            try:
                yield
            finally:
                stream.seek(0)
                msvcrt.locking(stream.fileno(), msvcrt.LK_UNLCK, 1)
        else:
            import fcntl
            try:
                fcntl.flock(stream, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except OSError as exc:
                raise RuntimeError('Another settings experiment holds the host lock') from exc
            yield
    # Keep the lock inode; removing it allows two experiments to lock different files.


def command_metadata(command):
    try:
        result = subprocess.run(command, cwd=ROOT, capture_output=True, text=True, timeout=10)
        return {'command': command, 'exit_code': result.returncode, 'stdout': result.stdout.strip(), 'stderr': result.stderr.strip()}
    except (OSError, subprocess.TimeoutExpired) as exc:
        return {'command': command, 'unavailable': type(exc).__name__}


def save_report(output, experiment, runs):
    validate_series(experiment)
    ranking = rank(runs, experiment['lanes'], experiment['repeats'])
    write_json(output / 'summary.json', {'schema_version': 2, 'ranking': ranking, 'runs': runs})
    markdown = report(experiment, runs, ranking)
    (output / 'report.md').write_text(markdown, encoding='utf-8')
    return ranking


def analyze(output):
    experiment = json.loads((output / 'experiment.json').read_text())
    validate_series(experiment)
    runs = []
    for entry in experiment['runs']:
        try:
            runs.append(summarize(output / entry['name'], experiment, entry))
        except (OSError, ValueError, KeyError, ZeroDivisionError, IndexError) as exc:
            runs.append({**entry, 'valid': False, 'error': f'{type(exc).__name__}: {exc}'})
    ranking = save_report(output, experiment, runs)
    print(f"{ranking['status']}: {ranking['reason']}\nReport: {output / 'report.md'}")
    return 0 if len(runs) == len(experiment['lanes']) * experiment['repeats'] and all(r.get('valid') for r in runs) else 1


def main(argv=None):
    cli = parser()
    args = cli.parse_args(argv)
    if args.analyze:
        return analyze(args.analyze.resolve())
    try:
        validate(args)
    except (ValueError, OSError) as exc:
        cli.error(str(exc))
    # SIGTERM/SIGBREAK must unwind ownership just like Ctrl-C.
    def interrupt(*_):
        # A second Ctrl-C/termination request must not interrupt tree cleanup.
        for signum in (signal.SIGINT, signal.SIGTERM):
            signal.signal(signum, signal.SIG_IGN)
        if os.name == 'nt':
            signal.signal(signal.SIGBREAK, signal.SIG_IGN)
        raise KeyboardInterrupt
    signal.signal(signal.SIGINT, interrupt)
    signal.signal(signal.SIGTERM, interrupt)
    if os.name == 'nt':
        signal.signal(signal.SIGBREAK, interrupt)
    runs = []
    with experiment_lock():
        args.port = select_port(socket.SOCK_DGRAM, args.port)
        args.health_port = select_port(socket.SOCK_STREAM, args.health_port)
        env, cleared = clean_environment(args.server_config, args.client_config)
        args.output.mkdir(parents=True)
        frozen = args.output / 'frozen'
        frozen.mkdir()
        artifacts = {}
        for key in ('server', 'client', 'server_config', 'client_config'):
            original = getattr(args, key)
            destination = frozen / (key + original.suffix)
            shutil.copy2(original, destination)
            artifacts[key] = {'original_path': str(original), 'path': str(destination), 'sha256': sha256(destination)}
            setattr(args, key, destination)
        tool_files = ['tune-avatar-settings.py', 'avatar_benchmark.py', 'avatar_tuning.py', 'run-windows-avatar-workload.py', 'windows_process_job.py']
        experiment = {'schema_version': 2, 'comparison_kind': 'settings', 'mode': args.mode,
                      'invocation': [sys.executable, str(pathlib.Path(__file__).resolve()), *(argv if argv is not None else sys.argv[1:])],
                      'lanes': args.lanes, 'repeats': args.repeats, 'frozen': artifacts, 'runs': [],
                      'intentional_setting_differences': ['BASIS_AVATAR_FLUSH_LANES'],
                      'cleared_environment_keys': cleared, 'binary_build_note_user_supplied': args.binary_build_note,
                      'platform': platform.platform(), 'os_name': os.name, 'machine': platform.machine(), 'processor': platform.processor(),
                      'logical_cpus': os.cpu_count(), 'python': sys.version,
                      'allowed_cpus': sorted(os.sched_getaffinity(0)) if hasattr(os, 'sched_getaffinity') else None,
                      'platform_default_flush_lanes': 6 if os.name == 'nt' else 0,
                      'platform_default_rayon': 'min(available parallelism,8)' if os.name == 'nt' else 'Rayon available parallelism',
                      'platform_default_tokio': 'available parallelism',
                      'workload': {k: getattr(args, k) for k in ('clients', 'network_capacity_mbps', 'warmup_seconds', 'window_seconds', 'rayon_threads', 'tokio_workers', 'client_workers', 'port', 'health_port', 'server_cpus', 'client_cpus', 'startup_timeout', 'ready_timeout')},
                      'workload_policy': '20ms movement, zero jitter/drift, 60FPS Unity-policy synthetic pose, dense all-near, no voice/P2P, CPU-only distances; one applied observer',
                      'source_revision': command_metadata(['git', 'rev-parse', 'HEAD']),
                      'source_status': command_metadata(['git', 'status', '--porcelain']),
                      'local_rustc_not_binary_attestation': command_metadata(['rustc', '-Vv']),
                      'local_cargo': command_metadata(['cargo', '-V']),
                      'tool_sha256': {f: sha256(ROOT / 'scripts/perf' / f) for f in tool_files}}
        server_env = dict(env)
        client_env = dict(env, BASIS_CLIENT_TOKIO_WORKERS=str(args.client_workers), BASIS_AVATAR_DIAGNOSTICS='true')
        if args.rayon_threads is not None:
            server_env['RAYON_NUM_THREADS'] = str(args.rayon_threads)
        if args.tokio_workers is not None:
            server_env['TOKIO_WORKER_THREADS'] = str(args.tokio_workers)
        try:
            maximum_clients = int(xml_values(args.server_config).get('PeerLimit', '0'))
            client_selection = calibrate_clients(args, server_env, client_env, args.output, maximum_clients)
        except (Exception, KeyboardInterrupt) as exc:
            experiment['error'] = f'{type(exc).__name__}: {exc}'
            experiment['workload']['clients'] = args.clients
            calibration_path = args.output / 'calibration' / 'calibration.json'
            try:
                calibration = json.loads(calibration_path.read_text())
            except (OSError, ValueError):
                calibration = {}
            experiment['client_selection'] = {
                'mode': 'auto_server_tx', 'selected_clients': None,
                'target_tx_mbps': args.network_capacity_mbps, 'tolerance_percent': 10,
                'attempts': calibration.get('attempts', []), 'error': experiment['error']}
            write_json(args.output / 'experiment.json', experiment)
            save_report(args.output, experiment, [])
            print(f"Calibration stopped: {experiment['error']}. Evidence retained: {args.output}", file=sys.stderr)
            return 130 if isinstance(exc, KeyboardInterrupt) else 1
        experiment['workload']['clients'] = args.clients
        experiment['client_selection'] = client_selection
        manifest_path = args.output / 'experiment.json'
        write_json(manifest_path, experiment)
        try:
            for i, (round_index, lane) in enumerate(run_order(args.lanes, args.repeats)):
                entry = {'name': f'{i + 1:02d}-lanes-{lane}-round-{round_index + 1}', 'lanes': lane, 'round': round_index,
                         'server_settings': dict(relevant_env(server_env), BASIS_AVATAR_FLUSH_LANES=str(lane)),
                         'client_settings': relevant_env(client_env)}
                experiment['runs'].append(entry)
                write_json(manifest_path, experiment)
                print(f"START {entry['name']} ({i + 1}/{len(args.lanes) * args.repeats})", flush=True)
                for artifact in artifacts.values():
                    if sha256(pathlib.Path(artifact['path'])) != artifact['sha256']:
                        raise RuntimeError('Frozen artifact changed; stopping')
                for filename, expected in experiment['tool_sha256'].items():
                    if sha256(ROOT / 'scripts/perf' / filename) != expected:
                        raise RuntimeError('Tool source changed during experiment; stopping')
                run_workload(args, args.output / entry['name'], dict(server_env, BASIS_AVATAR_FLUSH_LANES=str(lane)), client_env)
                result = summarize(args.output / entry['name'], experiment, entry)
                runs.append(result)
                write_json(args.output / entry['name'] / 'validated-summary.json', result)
                m = result['metrics']
                print(f"  valid={result['valid']} applied p50/p95={m['gap_p50_ms']:.2f}/{m['gap_p95_ms']:.2f}ms TX={m['server_transmit_mbps']:.1f}Mbps ({m['server_transmit_capacity_percent']:.1f}% target) built={m['built_logical_avatar_work_per_second']:.0f}/s CPU={m['server_cpu_cores']:.2f} cores", flush=True)
                save_report(args.output, experiment, runs)
                if not result['valid']:
                    raise RuntimeError('Run failed: ' + ', '.join(k for k, v in result['checks'].items() if not v))
        except (Exception, KeyboardInterrupt) as exc:
            if experiment['runs'] and len(runs) < len(experiment['runs']):
                runs.append({**experiment['runs'][-1], 'valid': False, 'error': f'{type(exc).__name__}: {exc}'})
            experiment['error'] = f'{type(exc).__name__}: {exc}'
            write_json(manifest_path, experiment)
            save_report(args.output, experiment, runs)
            print(f"Stopped: {experiment['error']}. Evidence retained: {args.output}", file=sys.stderr)
            return 130 if isinstance(exc, KeyboardInterrupt) else 1
        ranking = save_report(args.output, experiment, runs)
        print(f"{ranking['status']}: {ranking['reason']}\nReport: {args.output / 'report.md'}")
    return 0


if __name__ == '__main__':
    try:
        sys.exit(main())
    except (OSError, RuntimeError, ValueError, KeyError) as error:
        print(str(error), file=sys.stderr)
        sys.exit(1)
