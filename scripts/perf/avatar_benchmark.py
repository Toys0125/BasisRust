"""Process ownership and native counters for local avatar settings experiments."""
import csv
import importlib.util
import json
import os
import pathlib
import re
import signal
import socket
import subprocess
import time
import urllib.request

from script_server import prepare_server, ready as server_ready

ROOT = pathlib.Path(__file__).resolve().parents[2]
MIXED_DEFAULTS = {'additional_avatar_bytes': 0, 'scene_data_bytes': 0,
                  'scene_data_interval_ms': 50, 'scene_data_reliable': False}
WORKLOAD_FIELDS = ('clients', 'network_capacity_mbps', 'warmup_seconds', 'window_seconds',
                   'rayon_threads', 'tokio_workers', 'client_workers', 'port', 'health_port',
                   'server_cpus', 'client_cpus', 'startup_timeout', 'ready_timeout')


def add_mixed_options(parser):
    parser.add_argument('--additional-avatar-bytes', type=int, default=0)
    parser.add_argument('--scene-data-bytes', type=int, default=0, help='0 disables scene; otherwise 24..1024 bytes')
    parser.add_argument('--scene-data-interval-ms', type=int, default=50)
    parser.add_argument('--scene-data-reliable', action='store_true')


def validate_mixed_options(args):
    if not 0 <= args.additional_avatar_bytes <= 255:
        raise ValueError('Additional avatar bytes must be 0..255')
    if args.scene_data_bytes != 0 and not 24 <= args.scene_data_bytes <= 1024:
        raise ValueError('Scene data bytes must be 0 or 24..1024')
    if args.scene_data_interval_ms <= 0:
        raise ValueError('Scene data interval must be positive')
    if args.scene_data_reliable and not args.scene_data_bytes:
        raise ValueError('Reliable scene delivery requires enabled scene data')


def mixed_cli(args):
    flags = []
    for name, default in MIXED_DEFAULTS.items():
        value = getattr(args, name, default)
        if name == 'scene_data_reliable':
            if value:
                flags.append('--scene-data-reliable')
        else:
            flags.extend(['--' + name.replace('_', '-'), str(value)])
    return flags


def workload_metadata(args):
    return {**{k: getattr(args, k) for k in WORKLOAD_FIELDS},
            **{k: getattr(args, k, default) for k, default in MIXED_DEFAULTS.items()}}


def mixed_client_options(args, output, marker):
    flags = []
    if getattr(args, 'additional_avatar_bytes', 0):
        flags.extend(['--additional-avatar-bytes', str(args.additional_avatar_bytes)])
    if getattr(args, 'scene_data_bytes', 0):
        flags.extend(['--scene-data-bytes', str(args.scene_data_bytes),
                      '--scene-data-interval-ms', str(args.scene_data_interval_ms),
                      '--observe-scene-csv', str(output / 'scene.csv'),
                      '--scene-start-file', str(marker)])
        if args.scene_data_reliable:
            flags.append('--scene-data-reliable')
    return flags


def write_json(path, value):
    path.write_text(json.dumps(value, indent=2, allow_nan=False) + '\n', encoding='utf-8')


def select_port(kind, port):
    """Reject occupied UDP (dual stack) or loopback health TCP ports."""
    if os.name == 'nt' and port:
        # Windows wildcard binds can exclude an existing specific interface.
        # Check the actual loopback destinations as well as the wildcard.
        if kind == socket.SOCK_STREAM:
            with socket.socket(socket.AF_INET, kind) as live:
                live.settimeout(.5)
                if live.connect_ex(('127.0.0.1', port)) == 0:
                    raise OSError('Health port already has a live listener')
        else:
            for family, host in ((socket.AF_INET, '127.0.0.1'), (socket.AF_INET6, '::1')):
                with socket.socket(family, kind) as exact:
                    exact.setsockopt(socket.SOL_SOCKET, socket.SO_EXCLUSIVEADDRUSE, 1)
                    exact.bind((host, port))
    family = socket.AF_INET6 if kind == socket.SOCK_DGRAM else socket.AF_INET
    with socket.socket(family, kind) as probe:
        if family == socket.AF_INET6:
            probe.setsockopt(socket.IPPROTO_IPV6, socket.IPV6_V6ONLY, 0)
        if os.name != 'nt' and kind == socket.SOCK_STREAM:
            # Allow this sweep's closed TCP connections in TIME_WAIT, while
            # still rejecting any live listener (no SO_REUSEPORT).
            probe.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        if os.name == 'nt':
            probe.setsockopt(socket.SOL_SOCKET, socket.SO_EXCLUSIVEADDRUSE, 1)
        probe.bind(('::' if family == socket.AF_INET6 else '127.0.0.1', port))
        return probe.getsockname()[1]


def health(port):
    # Ignore HTTP proxy environment variables for this local-only experiment.
    opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))
    with opener.open(f'http://127.0.0.1:{port}/health', timeout=2) as response:
        return json.loads(response.read())


def linux_metrics(pid):
    stat = pathlib.Path(f'/proc/{pid}/stat').read_text().rsplit(')', 1)[1].split()
    ticks = os.sysconf('SC_CLK_TCK')
    return {'cpu_seconds': (int(stat[11]) + int(stat[12])) / ticks,
            'rss_bytes': int(stat[21]) * os.sysconf('SC_PAGE_SIZE')}


def native_sampler():
    if os.name != 'nt':
        return linux_metrics
    # Reuse the declared pointer-sized Win32 counter API from the native runner.
    spec = importlib.util.spec_from_file_location('windows_avatar_workload', ROOT / 'scripts/perf/run-windows-avatar-workload.py')
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)

    def sample(pid):
        counters = module.process_metrics(pid)
        if counters is None:
            raise RuntimeError(f'Cannot sample Windows process {pid}')
        return {'cpu_seconds': counters['cpu_seconds'], 'rss_bytes': counters['working_set_bytes']}
    return sample


class ProcessTree:
    """Own POSIX sessions or one private Windows kill-on-close Job Object."""
    def __init__(self):
        self.processes = []
        self.job = None
        if os.name == 'nt':
            from windows_process_job import WindowsProcessJob
            self.job = WindowsProcessJob()

    def start(self, command, cpus=None, **kwargs):
        if cpus and os.name != 'nt':
            command = ['taskset', '-c', cpus, *command]
        if self.job:
            process = self.job.start(command, affinity_mask=sum(1 << n for n in cpu_list(cpus)) if cpus else None,
                                     creationflags=subprocess.CREATE_NEW_PROCESS_GROUP, **kwargs)
        else:
            process = subprocess.Popen(command, start_new_session=True, **kwargs)
        self.processes.append(process)
        return process

    @staticmethod
    def stop(process):
        if process.poll() is not None:
            return
        if os.name == 'nt':
            process.send_signal(signal.CTRL_BREAK_EVENT)
        else:
            os.killpg(process.pid, signal.SIGINT)
        try:
            process.wait(timeout=15)
        except subprocess.TimeoutExpired:
            # The run will be invalidated if graceful shutdown did not succeed.
            process.kill()
            process.wait(timeout=5)

    def close(self):
        try:
            for process in reversed(self.processes):
                try:
                    self.stop(process)
                except (OSError, subprocess.TimeoutExpired):
                    pass
        finally:
            if self.job:
                self.job.close()
            else:
                # Include descendants remaining after the direct parent exited.
                for process in self.processes:
                    try:
                        os.killpg(process.pid, signal.SIGKILL)
                    except ProcessLookupError:
                        pass
            for process in self.processes:
                process.wait(timeout=5)
                if process.stdin:
                    process.stdin.close()
            self.processes.clear()


def cpu_list(text):
    values = set()
    for group in text.split(','):
        if not re.fullmatch(r'\d+(?:-\d+)?', group):
            raise ValueError('CPU lists must be comma-separated IDs/ranges, e.g. 0-3,6')
        ends = [int(n) for n in group.split('-')]
        a, b = ends[0], ends[-1]
        if b < a or b >= (os.cpu_count() or 1):
            raise ValueError('CPU list outside the logical processor range')
        values.update(range(a, b + 1))
    if os.name == 'nt' and (os.cpu_count() or 1) > 64:
        raise ValueError('Windows affinity supports a single processor group (<=64 CPUs)')
    if hasattr(os, 'sched_getaffinity') and not values <= os.sched_getaffinity(0):
        raise ValueError('CPU list contains processors outside this process\'s allowed affinity')
    return sorted(values)


def server_population(status, kind, server_log='', client_log=''):
    """Keep transport visitors, authenticated joins and avatar state distinct."""
    if kind == 'csharp':
        visitors = status.get('visitors')
        joins = {int(n) for n in re.findall(r'client (\d+) connected as remote peer', client_log)}
        return {'players_online': visitors if type(visitors) is int else -1,
                'active_states': None, 'authenticated_client_indices': sorted(joins)}
    counts = re.findall(r'active_states=(\d+)', server_log)
    return {'players_online': int(status.get('players_online', -1)),
            'active_states': int(counts[-1]) if counts else -1}


def population_matches(counts, kind, clients):
    if kind == 'csharp':
        return (counts['players_online'] == clients
                and counts['authenticated_client_indices'] == list(range(clients)))
    return counts['players_online'] == counts['active_states'] == clients


def run_workload(args, output, server_env, client_env):
    """Use the existing portable workload policy, with fresh owned processes."""
    output.mkdir()
    base = output / 'server-base'
    kind = getattr(args, 'server_kind', 'rust')
    if kind not in ('rust', 'csharp'):
        raise ValueError('server_kind must be rust or csharp')
    server_metadata = None
    marker = output / 'observe-start.marker'
    if kind == 'csharp':
        server_cmd, config, server_metadata = prepare_server(
            kind, args.server, base, args.server_config, args.port, args.health_port)
    else:
        (base / 'config').mkdir(parents=True)
        config = base / 'config/config.xml'
        config.write_bytes(args.server_config.read_bytes())
        server_cmd = [str(args.server), '--base-dir', str(base), '--port', str(args.port),
                      '--no-console', '--health-host', '127.0.0.1', '--health-port', str(args.health_port)]
    client_cmd = [str(args.client), '--config', str(args.client_config), '--ip', '127.0.0.1',
                  '--port', str(args.port), '--clients', str(args.clients), '--no-reconnect',
                  '--connect-timeout-ms', '60000', '--connect-batch-size', '25', '--connect-batch-delay-ms', '250',
                  '--movement-interval-ms', '20', '--movement-jitter-percent', '0', '--no-spread',
                  '--unity-avatar-policy', '--unity-frame-rate', '60', '--unity-pose-amplitude-degrees', '20',
                  '--observe-avatar-csv', str(output / 'observer.csv'), '--avatar-observe-radius', '40',
                  '--avatar-observe-expected-peers', str(args.clients - 1),
                  '--observe-avatar-start-file', str(marker), '--observe-avatar-window-secs', str(args.window_seconds)]
    client_cmd.extend(mixed_client_options(args, output, marker))
    if kind == 'rust':
        server_env = dict(server_env, BASIS_STATUS_INTERVAL_SECS='1', BASIS_AVATAR_DIAGNOSTIC_OBSERVER_ID='0',
                          BASIS_AVATAR_DIAGNOSTIC_START_FILE=str(marker),
                          BASIS_AVATAR_DIAGNOSTIC_CSV=str(output / 'server-pairs.csv'),
                          BASIS_AVATAR_DIAGNOSTIC_WINDOW_SECS=str(args.window_seconds))
    commands = {'server': server_cmd, 'client': client_cmd, 'cwd': str(ROOT),
                'executed_server': ['taskset', '-c', args.server_cpus, *server_cmd] if args.server_cpus and os.name != 'nt' else server_cmd,
                'executed_client': ['taskset', '-c', args.client_cpus, *client_cmd] if args.client_cpus and os.name != 'nt' else client_cmd,
                'server_affinity': args.server_cpus, 'client_affinity': args.client_cpus,
                'server_environment': relevant_env(server_env), 'client_environment': relevant_env(client_env)}
    if server_metadata is not None:
        commands['server_metadata'] = server_metadata
        commands['csharp_environment'] = {k: server_env[k] for k in ('EnableConsole', 'EnableBSRProfiling') if k in server_env}
        (output / 'prepared-config.xml').write_bytes(config.read_bytes())
        (output / 'prepared-litenetlib.xml').write_bytes(pathlib.Path(server_metadata['transport_config']).read_bytes())
    write_json(output / 'commands.json', commands)
    sampler = native_sampler()
    owner = ProcessTree()
    server = client = None
    meta = {'started_unix_seconds': time.time(), 'error': None, 'completed': False,
            'workload': workload_metadata(args)}
    if kind == 'csharp':
        meta['server_kind'] = kind
    if getattr(args, 'scene_data_bytes', 0):
        meta['scene_measurement'] = {'start': 'first scene tick after shared avatar marker',
                                    'end': 'client shutdown, including observer grace',
                                    'duration_source': 'scene.csv window_seconds',
                                    'cpu_duration_source': 'fixed run.json monotonic window'}
    try:
        # Re-probe before EVERY fresh run; never accept a prior server's health.
        select_port(socket.SOCK_DGRAM, args.port)
        select_port(socket.SOCK_STREAM, args.health_port)
        with (output / 'server.log').open('w', encoding='utf-8') as sl, (output / 'client.log').open('w', encoding='utf-8') as cl:
            server = owner.start(server_cmd, args.server_cpus, cwd=ROOT, env=server_env, stdout=sl, stderr=subprocess.STDOUT)
            write_json(output / 'processes.json', {'server': server.pid})

            def alive():
                for label, process in [('server', server), ('client', client)]:
                    if process and process.poll() is not None:
                        raise RuntimeError(f'{label} exited unexpectedly: {process.returncode}')

            deadline = time.monotonic() + args.startup_timeout
            while True:
                alive()
                try:
                    status = health(args.health_port)
                except (OSError, ValueError):
                    status = {}
                if server_ready(status, kind):
                    empty_count = status.get('visitors' if kind == 'csharp' else 'players_online', -1)
                    if type(empty_count) is not int or empty_count != 0:
                        raise RuntimeError('Health is not an empty fresh server')
                    break
                if time.monotonic() >= deadline:
                    raise RuntimeError('Server health readiness timed out')
                time.sleep(.2)
            client = owner.start(client_cmd, args.client_cpus, cwd=ROOT, env=client_env, stdin=subprocess.PIPE, stdout=cl, stderr=subprocess.STDOUT)
            write_json(output / 'processes.json', {'server': server.pid, 'client': client.pid})

            def population():
                alive()
                status = health(args.health_port)
                counts = server_population(status, kind,
                    (output / 'server.log').read_text(errors='replace') if kind == 'rust' else '',
                    (output / 'client.log').read_text(errors='replace') if kind == 'csharp' else '')
                return status, counts

            deadline = time.monotonic() + args.ready_timeout
            while True:
                try:
                    status, counts = population()
                except OSError:
                    counts = {'players_online': -1, 'active_states': -1, 'authenticated_client_indices': []}
                if population_matches(counts, kind, args.clients):
                    break
                if time.monotonic() >= deadline:
                    raise RuntimeError(f'Clients not ready: population={counts}, requested={args.clients}')
                time.sleep(.5)
            write_json(output / 'ready-health.json', status)
            # Readiness must hold during warmup too, with early exit detection.
            warm_end = time.monotonic() + args.warmup_seconds
            while time.monotonic() < warm_end:
                _, counts = population()
                if not population_matches(counts, kind, args.clients):
                    raise RuntimeError('Population dropped during warmup')
                time.sleep(min(.5, max(0, warm_end - time.monotonic())))

            with (output / 'samples.jsonl').open('w', encoding='utf-8') as stream:
                def sample():
                    status, counts = population()
                    row = {'monotonic_seconds': time.monotonic(), 'unix_seconds': time.time(),
                           'server': sampler(server.pid), 'client': sampler(client.pid),
                           **counts, 'health': status}
                    stream.write(json.dumps(row) + '\n')
                    stream.flush()
                    if not population_matches(counts, kind, args.clients):
                        raise RuntimeError('Population dropped during measurement')
                sample()
                start = time.monotonic()
                meta['window_start_monotonic'] = start
                meta['window_end_monotonic'] = start + args.window_seconds
                marker.write_text(f'{time.time():.6f}\n', encoding='utf-8')
                end = start + args.window_seconds
                while time.monotonic() < end:
                    time.sleep(min(.5, max(0, end - time.monotonic())))
                    sample()
                # Wait for all independent diagnostic windows to finish, bounded.
                deadline = time.monotonic() + 10
                while not (output / 'observer.sender.csv').exists():
                    alive()
                    if time.monotonic() >= deadline:
                        raise RuntimeError('Sender diagnostic window did not finish')
                    time.sleep(.1)
            # Observer timing starts with its first inbound avatar after the
            # marker, whereas sender timing starts from the marker itself.
            # Keep receiving after the fixed CPU/counter window so shutdown
            # does not truncate a late-starting observer under heavy load.
            meta['observer_shutdown_grace_seconds'] = 2
            grace_end = time.monotonic() + meta['observer_shutdown_grace_seconds']
            while time.monotonic() < grace_end:
                _, counts = population()
                if not population_matches(counts, kind, args.clients):
                    raise RuntimeError('Population dropped during observer shutdown grace')
                time.sleep(min(.2, max(0, grace_end - time.monotonic())))
            # The client supports a clean console stop on Windows and Linux.
            client.stdin.write(b'quit 100 0\n')
            client.stdin.flush()
            client.wait(timeout=30)
            owner.stop(server)
            meta['completed'] = True
    except BaseException as exc:
        meta['error'] = f'{type(exc).__name__}: {exc}'
        raise
    finally:
        try:
            owner.close()
        except BaseException as exc:
            meta['completed'] = False
            meta['error'] = f'Cleanup {type(exc).__name__}: {exc}'
            raise
        finally:
            meta.update(server_exit_code=server.poll() if server else None,
                        client_exit_code=client.poll() if client else None, finished_unix_seconds=time.time())
            write_json(output / 'run.json', meta)
    return meta


def relevant_env(env):
    return {key: value for key, value in sorted(env.items()) if key.startswith('BASIS_') or key in ('RAYON_NUM_THREADS', 'TOKIO_WORKER_THREADS')}
