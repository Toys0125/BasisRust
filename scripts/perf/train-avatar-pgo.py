#!/usr/bin/env python3
"""Collect two validated instrumented-server runs for the fixed avatar PGO experiment."""
import argparse
import importlib.util
import os
from pathlib import Path
import platform
import shutil
import signal
import socket

from avatar_benchmark import relevant_env, run_workload, select_port, write_json
from avatar_tuning import sha256, summarize

spec = importlib.util.spec_from_file_location('avatar_settings_cli', Path(__file__).with_name('tune-avatar-settings.py'))
settings = importlib.util.module_from_spec(spec)
spec.loader.exec_module(settings)


def main():
    cli = argparse.ArgumentParser(description=__doc__)
    cli.add_argument('--root', type=Path, required=True, help='fresh PGO build/profile capture root')
    cli.add_argument('--client', type=Path, required=True)
    supplied = cli.parse_args()
    if platform.system() != 'Linux':
        cli.error('This experiment requires Linux')
    root = supplied.root.resolve()
    profiles = root / 'profiles'
    if not profiles.is_dir() or list(profiles.glob('*.profraw')):
        cli.error('Create an empty profiles directory; old profiles must not be mixed into training')
    args = settings.parser().parse_args([
        '--server', str(root / 'build-generate-clean/x86_64-unknown-linux-gnu/release/basis-server-console'),
        '--client', str(supplied.client), '--output', str(root / 'training'), '--clients', '250',
        '--warmup-seconds', '45', '--window-seconds', '60', '--repeats', '2',
        '--server-cpus', '2-9', '--client-cpus', '10-15', '--rayon-threads', '4',
        '--tokio-workers', '4', '--client-workers', '4'])
    settings.validate(args)
    os.sched_setaffinity(0, {0, 1})

    def interrupt(*_):
        for sig in (signal.SIGINT, signal.SIGTERM):
            signal.signal(sig, signal.SIG_IGN)
        raise KeyboardInterrupt
    signal.signal(signal.SIGINT, interrupt)
    signal.signal(signal.SIGTERM, interrupt)
    with settings.experiment_lock():
        args.port = select_port(socket.SOCK_DGRAM, 0)
        args.health_port = select_port(socket.SOCK_STREAM, 0)
        env, cleared = settings.clean_environment(args.server_config, args.client_config)
        args.output.mkdir(parents=True)
        frozen = args.output / 'frozen'
        frozen.mkdir()
        artifacts = {}
        for key in ('server', 'client', 'server_config', 'client_config'):
            source = getattr(args, key)
            destination = frozen / (key + source.suffix)
            shutil.copy2(source, destination)
            artifacts[key] = {'path': str(destination), 'sha256': sha256(destination)}
            setattr(args, key, destination)
        server_env = dict(env, BASIS_AVATAR_FLUSH_LANES='0', RAYON_NUM_THREADS='4', TOKIO_WORKER_THREADS='4')
        client_env = dict(env, BASIS_CLIENT_TOKIO_WORKERS='4', BASIS_AVATAR_DIAGNOSTICS='true')
        experiment = {'comparison_kind': 'revisions', 'purpose': 'PGO training, excluded from evaluation',
                      'os_name': os.name, 'frozen': artifacts, 'cleared_environment_keys': cleared,
                      'training_tool_sha256': sha256(Path(__file__)), 'runs': [],
                      'workload': {k: getattr(args, k) for k in ('clients', 'warmup_seconds', 'window_seconds', 'rayon_threads', 'tokio_workers', 'client_workers', 'port', 'health_port', 'server_cpus', 'client_cpus', 'startup_timeout', 'ready_timeout')}}
        write_json(args.output / 'experiment.json', experiment)
        for index in range(2):
            entry = {'name': f'{index + 1:02d}-training', 'variant': 'instrumented', 'round': index,
                     'server_settings': relevant_env(server_env), 'client_settings': relevant_env(client_env),
                     'profile_pattern': str(profiles / f'training-{index + 1}-%m-%p.profraw')}
            experiment['runs'].append(entry)
            write_json(args.output / 'experiment.json', experiment)
            for artifact in artifacts.values():
                if sha256(Path(artifact['path'])) != artifact['sha256']:
                    raise RuntimeError('Frozen training artifact changed')
            print('START ' + entry['name'], flush=True)
            run_workload(args, args.output / entry['name'], dict(server_env, LLVM_PROFILE_FILE=entry['profile_pattern']), client_env)
            result = summarize(args.output / entry['name'], experiment, entry)
            raw = list(profiles.glob(f'training-{index + 1}-*.profraw'))
            result['raw_profiles'] = [{'path': str(p), 'bytes': p.stat().st_size, 'sha256': sha256(p)} for p in raw]
            write_json(args.output / entry['name'] / 'validated-summary.json', result)
            if not result['valid'] or not raw or any(p.stat().st_size == 0 for p in raw):
                raise RuntimeError('Training delivery gates failed or profile output missing; do not merge this cohort')
            print(f'END {entry["name"]}: valid; {len(raw)} nonempty raw profiles', flush=True)
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
