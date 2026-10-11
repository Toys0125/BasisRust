#!/usr/bin/env python3
"""Measure real image relays and cache downloads with authenticated loopback clients."""
import argparse
import csv
import hashlib
import json
import os
import pathlib
import platform
import socket
import subprocess
import time
import xml.etree.ElementTree as ET

from avatar_benchmark import ROOT, ProcessTree, native_sampler, select_port, write_json


def records(path):
    if not path.exists():
        return []
    result = []
    for line in path.read_text().splitlines():
        try:
            result.append(json.loads(line))
        except json.JSONDecodeError:
            pass  # Last record may still be being written.
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output', type=pathlib.Path, required=True)
    parser.add_argument('--image', type=pathlib.Path, required=True)
    parser.add_argument('--clients', type=int, default=500)
    parser.add_argument('--sharers', type=int, default=3)
    parser.add_argument('--cache-recipients', type=int, default=3)
    parser.add_argument('--live-timeout', type=float, default=240)
    parser.add_argument('--cache-timeout', type=float, default=60)
    parser.add_argument('--baseline-seconds', type=float, default=10)
    parser.add_argument('--settle-seconds', type=float, default=15)
    parser.add_argument('--server', type=pathlib.Path,
                        default=ROOT / 'BasisRustServer/target/release/examples/image-load-server')
    parser.add_argument('--client', type=pathlib.Path,
                        default=ROOT / 'BasisRustClient/target/release/examples/image-load-client')
    args = parser.parse_args()
    args.output = args.output.resolve()
    args.image = args.image.resolve()
    args.server = args.server.resolve()
    args.client = args.client.resolve()
    if args.output.exists():
        parser.error('output must be a new directory')
    if not all(p.is_file() for p in (args.image, args.server, args.client)):
        parser.error('image and release benchmark executables must exist')
    if args.clients <= args.sharers + args.cache_recipients or args.sharers != 3 or args.cache_recipients < 1:
        parser.error('exactly three sharers and positive cache-recipients are required; clients must exceed their sum')
    args.output.mkdir(parents=True)
    base = args.output / 'server-base'
    (base / 'config').mkdir(parents=True)
    config = base / 'config/config.xml'
    tree = ET.parse(ROOT / 'docs/performance/fixtures/avatar-1500-server.xml')
    port = select_port(socket.SOCK_DGRAM, 0)
    overrides = {
        'SetPort': str(port), 'OverrideAutoDiscoveryOfIpv': 'true',
        'IPv4Address': '127.0.0.1', 'HealthIncludeExtendedMetrics': 'true',
        'EnableComputeOffload': 'false', 'EnableConsole': 'false',
        'ImageCacheEnabled': 'true', 'ImageCacheMaxMegabytes': '512',
        'ImageCacheMinimumPerOwnerMegabytes': '32',
        'ImageShareEgressMegabitsPerSecond': '200',
        'ImageShareDownloadMegabitsPerSecond': '200',
        'ImageShareEgressEnforcementPercent': '150',
    }
    for name, value in overrides.items():
        node = tree.getroot().find(name)
        if node is None:
            node = ET.SubElement(tree.getroot(), name)
        node.text = value
    tree.write(config, encoding='utf-8', xml_declaration=True)
    server_telemetry = args.output / 'server.jsonl'
    client_telemetry = args.output / 'client.jsonl'
    live_marker, cache_marker = [args.output / (name + '.marker') for name in ('live', 'cache')]
    server_cmd = [str(args.server), str(config), str(base), str(server_telemetry)]
    client_cmd = [str(args.client), '--config', str(ROOT / 'docs/performance/fixtures/avatar-1500-client.xml'),
                  '--ip', '127.0.0.1', '--port', str(port), '--clients', str(args.clients),
                  '--image', str(args.image), '--output', str(client_telemetry),
                  '--live-start-file', str(live_marker), '--cache-start-file', str(cache_marker),
                  '--sharers', str(args.sharers), '--cache-recipients', str(args.cache_recipients),
                  '--egress-mbps', '200', '--workers', '4']
    # Avoid accidental server config environment overrides inherited from other tasks.
    field_names = {node.tag for node in tree.getroot()}
    env = {key: value for key, value in os.environ.items()
           if key not in field_names and not key.startswith('BASIS_')}
    env.update({'TOKIO_WORKER_THREADS': '16', 'BASIS_CLIENT_TOKIO_WORKERS': '4',
                'BASIS_CLIENT_SHARED_RECEIVE': 'false', 'BASIS_CLIENT_LINUX_DROP_BULK_UNRELIABLE': 'false'})
    sha = lambda path: hashlib.sha256(path.read_bytes()).hexdigest()
    changed = subprocess.check_output(['git', 'diff', 'HEAD', '--name-only', '-z'], cwd=ROOT)
    untracked = subprocess.check_output(['git', 'ls-files', '--others', '--exclude-standard', '-z'], cwd=ROOT)
    source_files = sorted({name.decode() for name in (changed + untracked).split(b'\0') if name})
    write_json(args.output / 'workload.json', {
        'server_revision': subprocess.check_output(['git', 'rev-parse', 'HEAD'], cwd=ROOT, text=True).strip(),
        'modified_source_sha256': {name: sha(ROOT / name) for name in source_files if (ROOT / name).is_file()},
        'clients': args.clients, 'sharers': args.sharers, 'cache_recipients': args.cache_recipients,
        'image_bytes': args.image.stat().st_size, 'image_sha256': sha(args.image),
        'server_sha256': sha(args.server), 'client_sha256': sha(args.client),
        'config_sha256': sha(config), 'server_command': server_cmd, 'client_command': client_cmd,
        'host': platform.platform(), 'logical_processors': os.cpu_count(),
        'image_settings': {key: value for key, value in overrides.items() if key.startswith('Image')},
        'avatar_updates': False, 'voice': False, 'network': 'localhost UDP; server and generator share host',
        'server_scope': 'ServerState core with production workers; benchmark-only telemetry wrapper',
        'environment': {key: env[key] for key in env if key.startswith(('TOKIO_', 'BASIS_CLIENT_'))},
    })
    owner = ProcessTree()
    sampler = native_sampler()
    samples, phases = [], {}
    server = client = None
    client_finished = False
    start = time.monotonic()
    result = {'valid': False, 'errors': ['run incomplete']}
    with (args.output / 'samples.csv').open('w', newline='') as sample_file:
        writer = None

        def sample(phase):
            nonlocal writer
            for label, process in [('server', server), ('client', client)]:
                if process and process.poll() is not None and not (label == 'client' and client_finished):
                    raise RuntimeError(f'{label} exited unexpectedly: {process.returncode}')
            rows = records(server_telemetry)
            row = dict(rows[-1]) if rows else {}
            row.update({'sample_elapsed_seconds': time.monotonic() - start, 'phase': phase})
            for label, process in [('server', server), ('client', client)]:
                counters = sampler(process.pid) if process and process.poll() is None else {
                    'cpu_seconds': samples[-1].get(label + '_cpu_seconds', 0) if samples else 0,
                    'rss_bytes': 0,
                }
                row.update({label + '_' + key: value for key, value in counters.items()})
            if row['server_rss_bytes'] + row['client_rss_bytes'] > 8 * 1024 ** 3:
                raise RuntimeError('benchmark process memory exceeded the 8 GiB guard')
            if rows:
                if writer is None:
                    writer = csv.DictWriter(sample_file, fieldnames=list(row))
                    writer.writeheader()
                writer.writerow(row)
                sample_file.flush()
                samples.append(row)
            return row

        def wait(phase, timeout, predicate):
            begun = time.monotonic()
            while True:
                row = sample(phase)
                client_rows = records(client_telemetry)
                if predicate(row, client_rows):
                    phases[phase] = {'seconds': time.monotonic() - begun}
                    return
                if time.monotonic() - begun > timeout:
                    raise RuntimeError(f'{phase} deadline exceeded')
                time.sleep(.5)

        def fixed(phase, seconds):
            begun = time.monotonic()
            wait(phase, seconds + 5, lambda _row, _clients: time.monotonic() - begun >= seconds)

        try:
            with (args.output / 'server.log').open('w') as sl, (args.output / 'client.log').open('w') as cl:
                server = owner.start(server_cmd, cwd=ROOT, env=env, stdout=sl, stderr=subprocess.STDOUT)
                wait('server_start', 30, lambda row, _clients: 'listen_address' in row)
                client = owner.start(client_cmd, cwd=ROOT, env=env, stdin=subprocess.PIPE,
                                     stdout=cl, stderr=subprocess.STDOUT)
                wait('connect', 180, lambda row, clients: row.get('players_online') == args.clients
                     and any(r.get('event') == 'ready' for r in clients))
                fixed('baseline', args.baseline_seconds)
                live_marker.write_text(str(time.time()))
                wait('live', args.live_timeout, lambda row, clients: bool(clients)
                     and clients[-1].get('uploads_done') == args.sharers
                     and clients[-1].get('live_completed', -1) == clients[-1].get('live_expected', -2)
                     and row.get('cache_complete') == args.sharers)
                fixed('between_phases', 3)
                cache_marker.write_text(str(time.time()))
                wait('cache', args.cache_timeout, lambda _row, clients: bool(clients)
                     and clients[-1].get('cache_completed', -1) == clients[-1].get('cache_expected', -2))
                fixed('settle', args.settle_seconds)
                owner.stop(client)
                client_finished = True
                fixed('disconnected_idle', 5)
                owner.stop(server)
                last = records(client_telemetry)[-1]
                errors = []
                for key in ('send_errors', 'malformed', 'integrity_errors', 'missing_chunks', 'fragment_errors'):
                    if last.get(key, 0):
                        errors.append(f'{key}={last[key]}')
                expected_pairs = args.sharers * (args.clients - 1)
                expected_chunks = expected_pairs * ((args.image.stat().st_size + 16383) // 16384)
                if len(last.get('pairs') or []) != expected_pairs or last.get('unique_chunks') != expected_chunks:
                    errors.append('all-recipient image/chunk coverage mismatch')
                if client.returncode != 0 or server.returncode != 0:
                    errors.append('nonzero process shutdown')
                if not samples or samples[-1].get('players_online') != 0:
                    errors.append('players remain after shutdown')
                if any(row.get('image_dropped_messages', 0) or row.get('protocol_errors', 0) for row in samples):
                    errors.append('server dropped image messages or recorded protocol errors')
                if any(row.get('players_online') != args.clients for row in samples
                       if row['phase'] in ('baseline', 'live', 'between_phases', 'cache', 'settle')):
                    errors.append('population changed during measured workload')
                result = {'valid': not errors, 'errors': errors, 'client': last}
        except Exception as error:
            result = {'valid': False, 'errors': [str(error)],
                      'client': records(client_telemetry)[-1:]}
        finally:
            owner.close()
            result['phases'] = phases
            phase_metrics = {}
            for phase in dict.fromkeys(row['phase'] for row in samples):
                group = [row for row in samples if row['phase'] == phase]
                first, last = group[0], group[-1]
                seconds = last['sample_elapsed_seconds'] - first['sample_elapsed_seconds']
                phase_metrics[phase] = {
                    'seconds_sampled': seconds,
                    'minimum_players': min(row.get('players_online', 0) for row in group),
                    'server_cpu_cores': (last['server_cpu_seconds'] - first['server_cpu_seconds']) / seconds if seconds else 0,
                    'client_cpu_cores': (last['client_cpu_seconds'] - first['client_cpu_seconds']) / seconds if seconds else 0,
                    'server_peak_rss_bytes': max(row['server_rss_bytes'] for row in group),
                    'client_peak_rss_bytes': max(row['client_rss_bytes'] for row in group),
                    'wire_egress_mbps': (last.get('wire_bytes_sent', 0) - first.get('wire_bytes_sent', 0)) * 8 / 1e6 / seconds if seconds else 0,
                    'end': last,
                }
            result['phase_metrics'] = phase_metrics
            write_json(args.output / 'summary.json', result)
    print(json.dumps({'valid': result['valid'], 'errors': result['errors'], 'phases': phases}, indent=2))
    return 0 if result['valid'] else 1


if __name__ == '__main__':
    raise SystemExit(main())
