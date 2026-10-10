#!/usr/bin/env python3
"""Benchmark opaque prop/scene script relay using real local Rust clients/server."""
import argparse
import csv
import hashlib
import json
import os
import pathlib
import socket
import subprocess
import time
from avatar_tuning import scene_summary as summarize
from script_server import prepare_server, ready, population
from avatar_benchmark import ROOT, ProcessTree, health, native_sampler, relevant_env, select_port, write_json

FIXTURES = ROOT / 'docs/performance/fixtures'




def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--server', type=pathlib.Path, default=ROOT / 'BasisRustServer/target/release/basis-server-console')
    parser.add_argument('--server-kind', choices=['rust', 'csharp'], default='rust')
    parser.add_argument('--client', type=pathlib.Path, default=ROOT / 'BasisRustClient/target/release/basis-rust-client')
    parser.add_argument('--output', type=pathlib.Path, required=True)
    parser.add_argument('--clients', type=int, default=100)
    parser.add_argument('--payload-bytes', type=int, default=128)
    parser.add_argument('--interval-ms', type=int, default=50)
    parser.add_argument('--delivery', choices=['unreliable', 'reliable'], default='unreliable')
    parser.add_argument('--warmup-seconds', type=float, default=5)
    parser.add_argument('--window-seconds', type=float, default=30)
    parser.add_argument('--ready-timeout', type=float, default=120)
    parser.add_argument('--minimum-delivery', type=float, default=.95)
    parser.add_argument('--port', type=int, default=0)
    parser.add_argument('--health-port', type=int, default=0)
    args = parser.parse_args()
    if args.clients < 2 or not 24 <= args.payload_bytes <= 1024 or args.interval_ms <= 0:
        parser.error('clients >=2, payload-bytes 24..1024, and interval-ms >0 required')
    if args.window_seconds <= 0 or args.warmup_seconds < 0 or args.ready_timeout <= 0 or not 0 < args.minimum_delivery <= 1:
        parser.error('invalid timing or minimum-delivery')
    args.server, args.client, args.output = args.server.resolve(), args.client.resolve(), args.output.resolve()
    if not args.server.is_file() or not args.client.is_file():
        parser.error('build server/client or pass --server and --client')
    if args.output.exists():
        parser.error('output must be a new directory')
    args.port = select_port(socket.SOCK_DGRAM, args.port)
    args.health_port = select_port(socket.SOCK_STREAM, args.health_port)
    args.output.mkdir(parents=True)
    base = args.output / 'server-base'
    server_cmd, config, server_metadata = prepare_server(args.server_kind, args.server, base,
        FIXTURES / 'avatar-1500-server.xml', args.port, args.health_port)
    marker = args.output / 'scene-start.marker'
    client_cmd = [str(args.client), '--config', str(FIXTURES / 'avatar-1500-client.xml'), '--ip', '127.0.0.1', '--port', str(args.port), '--clients', str(args.clients), '--no-reconnect', '--no-movement', '--no-spread', '--connect-timeout-ms', '60000', '--connect-batch-size', '25', '--connect-batch-delay-ms', '250', '--quit-batch-delay-ms', '0', '--scene-data-bytes', str(args.payload_bytes), '--scene-data-interval-ms', str(args.interval_ms), '--observe-scene-csv', str(args.output / 'scene.csv'), '--scene-start-file', str(marker)]
    if args.delivery == 'reliable':
        client_cmd.append('--scene-data-reliable')
    server_env = dict(os.environ, EnableBSRProfiling='false', EnableConsole='false')
    client_env = dict(server_env, BASIS_CLIENT_TOKIO_WORKERS='4')
    write_json(args.output / 'commands.json', {'server': server_cmd, 'client': client_cmd, 'cwd': str(ROOT),
        'server_environment': relevant_env(server_env), 'client_environment': relevant_env(client_env)})
    sha = lambda p: hashlib.sha256(p.read_bytes()).hexdigest()
    write_json(args.output / 'workload.json', {
        **{k: str(v) if isinstance(v, pathlib.Path) else v for k, v in vars(args).items()},
        'server_metadata': server_metadata, 'server_sha256': sha(args.server), 'client_sha256': sha(args.client), 'server_config_sha256': sha(config), 'client_config_sha256': sha(FIXTURES / 'avatar-1500-client.xml'),
        'avatar_updates': False, 'fanout': 'broadcast to all other peers', 'script_execution': False, 'latency_clock': 'same-host wall clock; histogram upper bounds in microseconds'})
    owner = ProcessTree()
    result = {'valid': False, 'errors': ['run incomplete']}
    sampler = native_sampler()
    try:
        with (args.output / 'server.log').open('w') as sl, (args.output / 'client.log').open('w') as cl:
            server = owner.start(server_cmd, cwd=ROOT, env=server_env, stdout=sl, stderr=subprocess.STDOUT)
            deadline = time.monotonic() + args.ready_timeout
            while True:
                if server.poll() is not None:
                    raise RuntimeError(f'server exited: {server.returncode}')
                try:
                    if ready(health(args.health_port), args.server_kind):
                        break
                except (OSError, ValueError):
                    pass
                if time.monotonic() > deadline:
                    raise RuntimeError('server readiness deadline exceeded')
                time.sleep(.1)
            client = owner.start(client_cmd, cwd=ROOT, env=client_env, stdout=cl, stderr=subprocess.STDOUT, stdin=subprocess.PIPE)

            def check():
                if server.poll() is not None or client.poll() is not None:
                    raise RuntimeError('server/client exited unexpectedly')
                status = health(args.health_port)
                return population(status, args.server_kind, (args.output / 'client.log').read_text(errors='replace'))

            while check() != args.clients:
                if time.monotonic() > deadline:
                    raise RuntimeError('client authentication deadline exceeded')
                time.sleep(.1)
            deadline = time.monotonic() + args.warmup_seconds
            while time.monotonic() < deadline:
                if check() != args.clients:
                    raise RuntimeError('population dropped during warmup')
                time.sleep(min(.1, max(0, deadline - time.monotonic())))
            baseline = {'server': sampler(server.pid), 'client': sampler(client.pid)}
            started = time.monotonic()
            marker.write_text(str(time.time()) + '\n')
            deadline = started + args.window_seconds
            with (args.output / 'samples.csv').open('w', newline='') as file:
                fields = ['elapsed_seconds', 'players_online', 'server_cpu_seconds', 'client_cpu_seconds', 'server_rss_bytes', 'client_rss_bytes']
                writer = csv.DictWriter(file, fieldnames=fields); writer.writeheader()
                peak = {'server': 0, 'client': 0}
                while True:
                    count = check()
                    if count != args.clients:
                        raise RuntimeError(f'population dropped to {count}')
                    sample = {label: sampler(p.pid) for label, p in [('server', server), ('client', client)]}
                    elapsed = time.monotonic() - started
                    row = {'elapsed_seconds': elapsed, 'players_online': count}
                    for label in sample:
                        row[label + '_cpu_seconds'] = sample[label]['cpu_seconds'] - baseline[label]['cpu_seconds']
                        row[label + '_rss_bytes'] = sample[label]['rss_bytes']
                        peak[label] = max(peak[label], sample[label]['rss_bytes'])
                    writer.writerow(row); file.flush()
                    if time.monotonic() >= deadline:
                        break
                    time.sleep(min(.25, max(0, deadline - time.monotonic())))
            client.stdin.write(b'quit\n'); client.stdin.flush()
            client.wait(timeout=30)
            if client.returncode != 0:
                raise RuntimeError(f'client shutdown failed: {client.returncode}')
            with (args.output / 'scene.csv').open() as file:
                metrics = {r['metric']: r['value'] for r in csv.DictReader(file)}
            result = summarize(metrics, args.clients, args.minimum_delivery, args.interval_ms)
            result['processes'] = {label: {'cpu_core_equivalents': row[label + '_cpu_seconds'] / elapsed, 'peak_rss_bytes': peak[label]} for label in peak}
            owner.stop(server)
            if server.returncode != 0:
                raise RuntimeError(f'server shutdown failed: {server.returncode}')
    except Exception as error:
        result = {'valid': False, 'errors': [str(error)]}
    finally:
        owner.close()
        write_json(args.output / 'summary.json', result)
    print(json.dumps(result, indent=2))
    return 0 if result['valid'] else 1


if __name__ == '__main__':
    raise SystemExit(main())
