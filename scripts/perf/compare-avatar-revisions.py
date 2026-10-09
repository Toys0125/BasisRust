#!/usr/bin/env python3
"""Compare frozen server revisions using one fixed Rust-client avatar workload."""
import argparse
import importlib.util
import json
import os
from pathlib import Path
import platform
import signal
import socket
import statistics
import sys
import shutil

from avatar_benchmark import ROOT, relevant_env, run_workload, select_port, write_json
from avatar_tuning import run_order, sha256, summarize

spec = importlib.util.spec_from_file_location('avatar_settings_cli', Path(__file__).with_name('tune-avatar-settings.py'))
settings = importlib.util.module_from_spec(spec)
spec.loader.exec_module(settings)


def aggregate(runs, repeats):
    expected = run_order(['baseline', 'candidate'], repeats)
    complete = (len(runs) == repeats * 2 and all(r.get('valid') for r in runs)
                and [(r.get('round'), r.get('variant')) for r in runs] == expected
                and all(a['finished_unix_seconds'] <= b['started_unix_seconds'] for a, b in zip(runs, runs[1:])))
    grouped = {v: [r for r in runs if r.get('variant') == v and r.get('metrics')] for v in ('baseline', 'candidate')}
    stats = {v: {k: {'median': statistics.median(r['metrics'][k] for r in rs),
                     'min': min(r['metrics'][k] for r in rs), 'max': max(r['metrics'][k] for r in rs)}
                 for k in rs[0]['metrics']} for v, rs in grouped.items() if rs}
    paired = []
    for round_index in range(repeats):
        pair = {r['variant']: r for r in runs if r.get('round') == round_index and r.get('metrics')}
        if set(pair) == {'baseline', 'candidate'}:
            paired.append({'round': round_index, 'candidate_change_percent': {
                k: (100 * (pair['candidate']['metrics'][k] / pair['baseline']['metrics'][k] - 1)
                    if pair['baseline']['metrics'][k] else None)
                for k in pair['baseline']['metrics']}})
    return {'complete_and_valid': complete, 'statistics': stats, 'paired_comparisons': paired,
            'interpretation': 'Descriptive process-level comparisons; no significance or speedup claim. Check input, retained work, applied cadence and errors together.'}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    for flag in ('baseline', 'candidate', 'client', 'build-manifest', 'output'):
        parser.add_argument('--' + flag, type=Path, required=True)
    parser.add_argument('--blocks', type=int, default=2)
    parser.add_argument('--clients', type=int, default=250)
    parser.add_argument('--warmup-seconds', type=int, default=45)
    parser.add_argument('--window-seconds', type=int, default=60)
    parser.add_argument('--server-cpus', default='2-9')
    parser.add_argument('--client-cpus', default='10-15')
    supplied = parser.parse_args()
    if platform.system() != 'Linux' or supplied.blocks < 1:
        parser.error('This coordinator requires Linux and at least one balanced block')
    # Reuse existing bounds, CPU-only fixture and worker validation. No settings
    # are swept: the same explicit Linux flush-lane value is used for both revisions.
    args = settings.parser().parse_args([
        '--server', str(supplied.baseline), '--client', str(supplied.client), '--output', str(supplied.output),
        '--clients', str(supplied.clients), '--warmup-seconds', str(supplied.warmup_seconds),
        '--window-seconds', str(supplied.window_seconds), '--repeats', str(supplied.blocks * 2),
        '--server-cpus', supplied.server_cpus, '--client-cpus', supplied.client_cpus,
        '--rayon-threads', '4', '--tokio-workers', '4', '--client-workers', '4'])
    settings.validate(args)
    if set(settings.cpu_list(args.server_cpus)) & set(settings.cpu_list(args.client_cpus)):
        parser.error('Server and client CPU sets must be disjoint')
    candidate = supplied.candidate.resolve()
    if not candidate.is_file() or sha256(candidate) == sha256(args.server):
        parser.error('Candidate must be an existing binary distinct from baseline')
    build = json.loads(supplied.build_manifest.read_text())
    if build['binary_sha256'] != {'baseline': sha256(args.server), 'candidate': sha256(candidate), 'client': sha256(args.client)}:
        parser.error('Build-manifest binary hashes do not match selected binaries')
    unused_cpus = os.sched_getaffinity(0) - set(settings.cpu_list(args.server_cpus)) - set(settings.cpu_list(args.client_cpus))
    if unused_cpus:
        os.sched_setaffinity(0, unused_cpus)

    def interrupt(*_):
        for signum in (signal.SIGINT, signal.SIGTERM):
            signal.signal(signum, signal.SIG_IGN)
        raise KeyboardInterrupt
    signal.signal(signal.SIGINT, interrupt)
    signal.signal(signal.SIGTERM, interrupt)
    runs = []
    with settings.experiment_lock():
        args.port = select_port(socket.SOCK_DGRAM, 0)
        args.health_port = select_port(socket.SOCK_STREAM, 0)
        env, cleared = settings.clean_environment(args.server_config, args.client_config)
        args.output.mkdir(parents=True)
        frozen_dir = args.output / 'frozen'
        frozen_dir.mkdir()
        artifacts = {}
        for key, source in {'baseline': args.server, 'candidate': candidate, 'client': args.client,
                            'server_config': args.server_config, 'client_config': args.client_config}.items():
            destination = frozen_dir / (key + source.suffix)
            shutil.copy2(source, destination)
            artifacts[key] = {'path': str(destination), 'sha256': sha256(destination)}
        args.client = Path(artifacts['client']['path'])
        args.server_config = Path(artifacts['server_config']['path'])
        args.client_config = Path(artifacts['client_config']['path'])
        server_env = dict(env, BASIS_AVATAR_FLUSH_LANES='0', RAYON_NUM_THREADS='4', TOKIO_WORKER_THREADS='4')
        client_env = dict(env, BASIS_CLIENT_TOKIO_WORKERS='4', BASIS_AVATAR_DIAGNOSTICS='true')
        tool_paths = [Path(__file__).resolve(), Path(settings.__file__), ROOT / 'scripts/perf/avatar_benchmark.py', ROOT / 'scripts/perf/avatar_tuning.py']
        tools = {str(p): sha256(p) for p in tool_paths}
        experiment = {'comparison_kind': 'revisions', 'os_name': os.name, 'repeats': args.repeats,
                      'coordinator_affinity': sorted(os.sched_getaffinity(0)),
                      'platform': platform.platform(), 'build': build, 'frozen': artifacts,
                      'cleared_environment_keys': cleared, 'tool_sha256': tools, 'runs': [],
                      'invocation': [sys.executable, str(Path(__file__).resolve()), *sys.argv[1:]],
                      'workload': {k: getattr(args, k) for k in ('clients', 'network_capacity_mbps', 'warmup_seconds', 'window_seconds', 'rayon_threads', 'tokio_workers', 'client_workers', 'port', 'health_port', 'server_cpus', 'client_cpus', 'startup_timeout', 'ready_timeout')}}

        def save():
            write_json(args.output / 'experiment.json', experiment)
            write_json(args.output / 'summary.json', {'experiment': experiment, 'runs': runs, 'comparison': aggregate(runs, args.repeats)})

        save()
        try:
            for i, (round_index, variant) in enumerate(run_order(['baseline', 'candidate'], args.repeats)):
                entry = {'name': f'{i + 1:02d}-{variant}', 'round': round_index, 'variant': variant,
                         'server_settings': relevant_env(server_env), 'client_settings': relevant_env(client_env)}
                experiment['runs'].append(entry)
                save()
                print(f'START {entry["name"]} ({i + 1}/{args.repeats * 2})', flush=True)
                for artifact in artifacts.values():
                    if sha256(Path(artifact['path'])) != artifact['sha256']:
                        raise RuntimeError('Frozen artifact changed')
                for path, expected in tools.items():
                    if sha256(Path(path)) != expected:
                        raise RuntimeError('Capture tool changed during experiment')
                args.server = Path(artifacts[variant]['path'])
                run_workload(args, args.output / entry['name'], server_env, client_env)
                view = dict(experiment, frozen=dict(artifacts, server=artifacts[variant]))
                result = summarize(args.output / entry['name'], view, entry)
                runs.append(result)
                write_json(args.output / entry['name'] / 'validated-summary.json', result)
                save()
                m = result['metrics']
                print(f'END {entry["name"]}: valid={result["valid"]} p50/p95={m["gap_p50_ms"]:.2f}/{m["gap_p95_ms"]:.2f}ms built={m["built_logical_avatar_work_per_second"]:.0f}/s server/client CPU={m["server_cpu_cores"]:.2f}/{m["client_cpu_cores"]:.2f}', flush=True)
                if not result['valid']:
                    raise RuntimeError('Failed gates: ' + ', '.join(k for k, v in result['checks'].items() if not v))
        except (Exception, KeyboardInterrupt) as exc:
            experiment['error'] = f'{type(exc).__name__}: {exc}'
            if len(runs) < len(experiment['runs']):
                runs.append({**experiment['runs'][-1], 'valid': False, 'error': experiment['error']})
            save()
            print(experiment['error'], file=sys.stderr)
            return 130 if isinstance(exc, KeyboardInterrupt) else 1
    print('Completed; all runs retained in ' + str(args.output / 'summary.json'))
    return 0


if __name__ == '__main__':
    sys.exit(main())
