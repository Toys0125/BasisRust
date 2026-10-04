#!/usr/bin/env python3
"""Run sequential frozen-server crossover tests with one frozen Windows client.

Requires the 60-second dense loopback workload used by the Windows review.
Names must be unique, and output must be a new directory. All runs use matching
diagnostics, config, warmup, client workers and environment; delivery failure
stops the series. Raw captures and host CPU samples are retained for each run.
"""

import argparse
import csv
import ctypes
import hashlib
import json
import os
import pathlib
import signal
import subprocess
import sys
import time

from summarize_windows_avatar import summarize

ROOT = pathlib.Path(__file__).resolve().parents[2]


class FILETIME(ctypes.Structure):
    _fields_ = [('low', ctypes.c_uint32), ('high', ctypes.c_uint32)]


def host_cpu():
    idle, kernel, user = FILETIME(), FILETIME(), FILETIME()
    if not ctypes.windll.kernel32.GetSystemTimes(ctypes.byref(idle), ctypes.byref(kernel), ctypes.byref(user)):
        raise ctypes.WinError()
    seconds = lambda x: ((x.high << 32) | x.low) / 10_000_000
    return {'unix_seconds': time.time(), 'busy_seconds': seconds(kernel) + seconds(user) - seconds(idle)}


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--output', required=True, type=pathlib.Path)
    parser.add_argument('--client', required=True, type=pathlib.Path)
    parser.add_argument('--server-config', type=pathlib.Path, default=ROOT/'docs/performance/fixtures/avatar-cpu-only-server.xml')
    parser.add_argument('--clients', type=int, default=750)
    parser.add_argument('--diagnostics', action='store_true')
    parser.add_argument('--flush-lanes', type=int)
    parser.add_argument('--variants', nargs='+', required=True, help='ordered name=server.exe entries')
    args = parser.parse_args()
    if os.name != 'nt':
        parser.error('Windows process metrics are required')
    if args.output.exists():
        parser.error('output directory already exists')
    variants = []
    for variant in args.variants:
        if '=' not in variant:
            parser.error('each variant must be name=server.exe')
        name, binary = variant.split('=', 1)
        if not name or pathlib.Path(name).name != name or name in ('.', '..') or any(name == n for n, _ in variants):
            parser.error('run names must be unique directory names')
        binary = pathlib.Path(binary).resolve()
        if not binary.is_file():
            parser.error('server binary missing: '+str(binary))
        variants.append((name, binary))
    if not args.client.is_file() or not args.server_config.is_file():
        parser.error('client binary and server fixture must exist')
    if args.clients < 2:
        parser.error('clients must be at least 2')
    if args.flush_lanes is not None and not 0 <= args.flush_lanes <= 8:
        parser.error('flush lanes must be 0..8; 0 uses the existing Rayon scheduling')
    capture = args.output.resolve()
    capture.mkdir(parents=True)
    env = dict(os.environ)
    cleared = []
    for key in list(env):
        if key.startswith('BASIS_') or key in ('RAYON_NUM_THREADS', 'TOKIO_WORKER_THREADS', 'Password', 'EnableComputeOffload'):
            cleared.append(key)
            env.pop(key)
    if args.flush_lanes is not None:
        env['BASIS_AVATAR_FLUSH_LANES'] = str(args.flush_lanes)
    source_diff = subprocess.check_output(['rtk', 'proxy', 'git', 'diff', '--binary'], cwd=ROOT)
    (capture/'source.patch').write_bytes(source_diff)
    manifest = {'cleared_environment_keys': cleared, 'runs': [],
                'rustc': subprocess.check_output(['rtk', 'proxy', 'rustc', '-Vv'], text=True).strip(),
                'source_revision': subprocess.check_output(['rtk', 'proxy', 'git', 'rev-parse', 'HEAD'], cwd=ROOT, text=True).strip(),
                'client_sha256': hashlib.sha256(args.client.read_bytes()).hexdigest(),
                'server_fixture_sha256': hashlib.sha256(args.server_config.read_bytes()).hexdigest(),
                'client_fixture_sha256': hashlib.sha256((ROOT/'docs/performance/fixtures/avatar-1500-client.xml').read_bytes()).hexdigest(),
                'source_patch_sha256': hashlib.sha256(source_diff).hexdigest(),
                'loopback': True, 'no_affinity': True, 'flush_lanes': args.flush_lanes}
    manifest_path = capture / 'experiment.json'
    for name, binary in variants:
        output = capture / name
        command = ['rtk', 'proxy', sys.executable, str(ROOT/'scripts/perf/run-windows-avatar-workload.py'),
                   '--server', str(binary), '--client', str(args.client.resolve()),
                   '--server-config', str(args.server_config.resolve()),
                   '--output', str(output), '--clients', str(args.clients), '--workers', '4',
                   '--warmup-seconds', '45', '--window-seconds', '60']
        if not args.diagnostics:
            command.append('--no-server-avatar-diagnostics')
        entry = {'name': name, 'server': str(binary), 'server_sha256': hashlib.sha256(binary.read_bytes()).hexdigest(), 'command': command}
        manifest['runs'].append(entry)
        manifest_path.write_text(json.dumps(manifest, indent=2)+'\n')
        print('START '+name, flush=True)
        samples = []
        with (capture/(name+'-harness.log')).open('w') as log:
            process = subprocess.Popen(command, cwd=ROOT, env=env, stdout=log, stderr=subprocess.STDOUT,
                                       creationflags=subprocess.CREATE_NEW_PROCESS_GROUP)
            try:
                deadline = time.monotonic()+600
                while process.poll() is None:
                    if time.monotonic() > deadline:
                        raise RuntimeError('Harness exceeded 600 seconds')
                    samples.append(host_cpu())
                    time.sleep(2)
                if process.returncode:
                    raise RuntimeError('Harness failed: '+str(process.returncode))
            finally:
                (capture/(name+'-host-cpu.json')).write_text(json.dumps(samples, indent=2)+'\n')
                if process.poll() is None:
                    try:
                        process.send_signal(signal.CTRL_BREAK_EVENT)
                        process.wait(timeout=90)
                    except (OSError, subprocess.TimeoutExpired):
                        process.terminate()
                        process.wait(timeout=30)
        result = summarize(output)
        result['checks']['frozen_server_hash'] = result['metadata']['server_binary_sha256'] == entry['server_sha256']
        result['checks']['frozen_client_hash'] = result['metadata']['client_binary_sha256'] == manifest['client_sha256']
        result['checks']['matching_server_fixture'] = result['metadata']['server_config_fixture_sha256'] == manifest['server_fixture_sha256']
        result['checks']['matching_client_fixture'] = result['metadata']['client_config_fixture_sha256'] == manifest['client_fixture_sha256']
        result['valid'] = all(result['checks'].values())
        entry['valid'] = result['valid']
        result['command'] = command
        result['server_sha256'] = entry['server_sha256']
        # Align host samples with the process measurement window. The residual
        # includes other apps AND workload-related system/DPC CPU; it cannot
        # identify background load. Endpoints differ by up to two seconds.
        with (output/'process-metrics.csv').open(newline='') as stream:
            rows = list(csv.DictReader(stream))
        a, b = float(rows[0]['unix_seconds']), float(rows[-1]['unix_seconds'])
        start, end = min(samples,key=lambda s:abs(s['unix_seconds']-a)), min(samples,key=lambda s:abs(s['unix_seconds']-b))
        host_cores = (end['busy_seconds']-start['busy_seconds'])/(end['unix_seconds']-start['unix_seconds'])
        result['metrics']['host_cpu_cores'] = host_cores
        result['metrics']['host_cpu_minus_process_cpu_cores'] = host_cores-result['metrics']['combined_cpu_cores']
        (output/'validated-summary.json').write_text(json.dumps(result, indent=2)+'\n')
        manifest_path.write_text(json.dumps(manifest, indent=2)+'\n')
        print(json.dumps({'completed':name,'valid':result['valid'],'metrics':result['metrics'],
                          'failed_checks':[k for k,v in result['checks'].items() if not v]}),flush=True)
        if not result['valid']:
            raise RuntimeError('Delivery gate failed; stopping series')
    print('SERIES COMPLETE',flush=True)


if __name__ == '__main__':
    main()
