"""Focused settings validation, sectioned parsing, ranking and tree cleanup tests."""
import argparse
import copy
import importlib.util
import json
import os
import pathlib
import signal
import socket
import subprocess
import sys
import tempfile
import time
import unittest
from unittest import mock

from avatar_benchmark import ProcessTree, cpu_list, select_port
from avatar_tuning import delta, interpolate, observer_metrics, rank, run_order, validate_series, xml_values

SPEC = importlib.util.spec_from_file_location('tune_avatar', pathlib.Path(__file__).with_name('tune-avatar-settings.py'))
CLI = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(CLI)


def measured_runs():
    runs = []
    for round_index in range(2):
        for lane in (0, 6):
            good = lane == 6
            runs.append({'lanes': lane, 'round': round_index, 'valid': True,
                         'metrics': {'gap_p50_ms': 90 if good else 100, 'gap_p95_ms': 180 if good else 200,
                                     'server_cpu_cores': 3 if good else 2, 'built_logical_avatar_work_per_second': 1100 if good else 1000,
                                     'observer_applied_items_per_second': 110 if good else 100,
                                     'inbound_updates_per_second': 500, 'sender_socket_items_per_second': 500}})
    return runs


class ValidationTests(unittest.TestCase):
    def test_lane_settings_distinct_and_bounded(self):
        self.assertEqual(CLI.lane_values('0,6,8'), [0, 6, 8])
        for text in ('-1,6', '0,9', '0,6,6', '6', '0, 6', '0,6,', '0,6.0'):
            with self.subTest(text=text), self.assertRaises(argparse.ArgumentTypeError):
                CLI.lane_values(text)

    def test_invalid_duration_and_worker_values(self):
        for flags in (['--clients', '1'], ['--repeats', '3'], ['--repeats', '0'],
                      ['--window-seconds', '1'], ['--warmup-seconds', '-1'], ['--tokio-workers', '0'],
                      ['--client-workers', '0'], ['--rayon-threads', '-1'], ['--port', '65536'], ['--ready-timeout', '0']):
            with tempfile.TemporaryDirectory() as directory:
                args = CLI.parser().parse_args(['--output', directory + '/new', *flags])
                with self.subTest(flags=flags), self.assertRaises(ValueError):
                    CLI.validate(args)

    def test_override_scrubbing_includes_absent_fields_and_windows_case(self):
        with mock.patch.dict(os.environ, {'Password': 'do-not-print', 'ENABLECOMPUTEOFFLOAD': 'true',
                                        'LocomotionPolicyGravity': '9', 'BASIS_AVATAR_FLUSH_LANES': '8',
                                        'RAYON_NUM_THREADS': '2', 'TOKIO_WORKER_THREADS': '2', 'PATH': '/unchanged'}, clear=True):
            env, cleared = CLI.clean_environment(CLI.FIXTURES / 'avatar-cpu-only-server.xml', CLI.FIXTURES / 'avatar-1500-client.xml')
            self.assertEqual(env, {'PATH': '/unchanged'})
            self.assertEqual(len(cleared), 6)
            self.assertNotIn('do-not-print', str(cleared))

    def test_config_duplicates_rejected(self):
        with tempfile.TemporaryDirectory() as directory:
            path = pathlib.Path(directory) / 'config.xml'
            path.write_text('<Configuration><Password>a</Password><Password>b</Password></Configuration>')
            with self.assertRaisesRegex(ValueError, 'unique'):
                xml_values(path)

    def test_cpu_ids_rejected(self):
        for value in ('', '2-1', '-1', 'x', '0,,1', str(os.cpu_count() or 1)):
            with self.subTest(value=value), self.assertRaises(ValueError):
                cpu_list(value)

    def test_occupied_ports_are_rejected(self):
        for kind in (socket.SOCK_STREAM, socket.SOCK_DGRAM):
            with socket.socket(socket.AF_INET, kind) as occupied:
                occupied.bind(('127.0.0.1', 0))
                if kind == socket.SOCK_STREAM:
                    occupied.listen()
                with self.subTest(kind=kind), self.assertRaises(OSError):
                    select_port(kind, occupied.getsockname()[1])

    def test_host_lock_rejects_second_owner(self):
        with CLI.experiment_lock():
            result = subprocess.run([sys.executable, '-c',
                                     "import importlib.util; s=importlib.util.spec_from_file_location('t', 'scripts/perf/tune-avatar-settings.py'); m=importlib.util.module_from_spec(s); s.loader.exec_module(m);\nwith m.experiment_lock(): pass"],
                                    cwd=CLI.ROOT, env=dict(os.environ, PYTHONPATH=str(pathlib.Path(__file__).parent.resolve())),
                                    capture_output=True, text=True, timeout=10)
        self.assertNotEqual(result.returncode, 0)
        self.assertIn('host lock', result.stderr)

    def test_settings_manifest_rejects_config_order_and_worker_mismatches(self):
        experiment = {'comparison_kind': 'settings', 'lanes': [0, 6], 'repeats': 2, 'runs': []}
        for index, (round_index, lane) in enumerate(run_order([0, 6], 2)):
            experiment['runs'].append({'name': str(index), 'round': round_index, 'lanes': lane,
                                       'server_settings': {'BASIS_AVATAR_FLUSH_LANES': str(lane), 'RAYON_NUM_THREADS': '4'},
                                       'client_settings': {'BASIS_CLIENT_TOKIO_WORKERS': '4'}})
        validate_series(experiment)
        for change in ('lane', 'worker', 'order'):
            bad = copy.deepcopy(experiment)
            if change == 'lane':
                bad['runs'][1]['server_settings']['BASIS_AVATAR_FLUSH_LANES'] = '0'
            elif change == 'worker':
                bad['runs'][1]['server_settings']['RAYON_NUM_THREADS'] = '8'
            else:
                bad['runs'][1]['round'] = 1
            with self.subTest(change=change), self.assertRaises(ValueError):
                validate_series(bad)

    @unittest.skipUnless(os.name == 'posix', 'Local executable fixture uses a POSIX shebang')
    def test_startup_failure_and_timeout_retain_evidence_and_release_ports(self):
        for body, expected in [('raise SystemExit(23)', 'exited unexpectedly: 23'),
                               ('import time; time.sleep(60)', 'readiness timed out')]:
            with tempfile.TemporaryDirectory() as directory:
                root = pathlib.Path(directory)
                server, client = root / 'server', root / 'client'
                server.write_text('#!' + sys.executable + '\n# BASIS_AVATAR_FLUSH_LANES\n' + body + '\n')
                client.write_text('#!' + sys.executable + '\nraise SystemExit(24)\n')
                server.chmod(0o700)
                client.chmod(0o700)
                output = root / 'capture'
                result = subprocess.run([sys.executable, '-B', str(CLI.ROOT / 'scripts/perf/tune-avatar-settings.py'),
                                         '--server', str(server), '--client', str(client), '--output', str(output),
                                         '--startup-timeout', '1', '--clients', '2'],
                                        capture_output=True, text=True, timeout=15)
                with self.subTest(body=body):
                    self.assertEqual(result.returncode, 1, result.stderr)
                    meta = json.loads((output / '01-lanes-0-round-1/run.json').read_text())
                    self.assertFalse(meta['completed'])
                    self.assertIn(expected, meta['error'])
                    self.assertTrue((output / 'report.md').is_file())
                    summary = json.loads((output / 'summary.json').read_text())
                    self.assertEqual(summary['ranking']['status'], 'inconclusive')
                    experiment = json.loads((output / 'experiment.json').read_text())
                    select_port(socket.SOCK_DGRAM, experiment['workload']['port'])
                    select_port(socket.SOCK_STREAM, experiment['workload']['health_port'])
                    pid = json.loads((output / '01-lanes-0-round-1/processes.json').read_text())['server']
                    with self.assertRaises(ProcessLookupError):
                        os.kill(pid, 0)



class ParsingTests(unittest.TestCase):
    def test_sectioned_observer_uses_global_metrics(self):
        with tempfile.TemporaryDirectory() as directory:
            path = pathlib.Path(directory) / 'observer.csv'
            path.write_text('metric,value\ngap_p50_ms,12.5\ngap_p95_ms,30.5\nobserved_channel,packets\n1,999\n\npeer_id,gap_p50_ms\n5,999\n')
            self.assertEqual(observer_metrics(path), {'gap_p50_ms': '12.5', 'gap_p95_ms': '30.5'})
            path.write_text('gap_p50_ms,1\ngap_p50_ms,2\n')
            with self.assertRaisesRegex(ValueError, 'Duplicate'):
                observer_metrics(path)

    def test_cumulative_counters_are_deltas_and_resets_fail(self):
        self.assertEqual(delta({'count': '100'}, {'count': '160'}, 'count'), 60)
        with self.assertRaises(ValueError):
            delta({'count': '100'}, {'count': '60'}, 'count')

    def test_cpu_fixed_window_interpolation_requires_brackets(self):
        samples = [{'monotonic_seconds': 0, 'server': {'cpu_seconds': 1}},
                   {'monotonic_seconds': 2, 'server': {'cpu_seconds': 5}}]
        self.assertEqual(interpolate(samples, 1, 'server'), 3)
        with self.assertRaisesRegex(ValueError, 'bracket'):
            interpolate(samples, 3, 'server')


class RankingTests(unittest.TestCase):
    def test_counterbalanced_order_and_rotation(self):
        self.assertEqual(run_order([0, 6], 2), [(0, 0), (0, 6), (1, 6), (1, 0)])
        self.assertEqual([lane for _, lane in run_order([0, 3, 6], 4)], [0, 3, 6, 6, 3, 0, 3, 6, 0, 0, 6, 3])

    def test_cadence_win_reports_cpu_tradeoff(self):
        result = rank(measured_runs(), [0, 6], 2)
        self.assertEqual(result['recommended_lanes'], 6)
        comparison = next(c for c in result['comparisons'] if c['candidate'] == 6)
        self.assertEqual(comparison['tradeoff'], 'cadence')
        self.assertEqual(comparison['paired_change_percent'][0]['server_cpu_cores'], 50)

    def test_ties_failures_variable_direction_and_dropped_work_inconclusive(self):
        for key, value in [('gap_p50_ms', 110), ('built_logical_avatar_work_per_second', 700),
                           ('built_logical_avatar_work_per_second', 990), ('observer_applied_items_per_second', 99),
                           ('observer_applied_items_per_second', 70), ('inbound_updates_per_second', 400),
                           ('sender_socket_items_per_second', 400)]:
            runs = measured_runs()
            runs[-1]['metrics'][key] = value
            with self.subTest(key=key):
                self.assertIsNone(rank(runs, [0, 6], 2)['recommended_lanes'])
        runs = measured_runs()
        runs[-1]['valid'] = False
        self.assertIsNone(rank(runs, [0, 6], 2)['recommended_lanes'])
        runs = measured_runs()
        for run in runs:
            run['metrics'] = copy.deepcopy(runs[0]['metrics'])
        self.assertEqual(rank(runs, [0, 6], 2)['status'], 'inconclusive')
        self.assertEqual(rank(runs[:-1], [0, 6], 2)['status'], 'inconclusive')

    def test_cpu_preference_with_tied_cadence(self):
        runs = measured_runs()
        for run in runs:
            if run['lanes'] == 6:
                run['metrics'].update(gap_p50_ms=100, gap_p95_ms=200, server_cpu_cores=1.5)
        self.assertEqual(rank(runs, [0, 6], 2)['recommended_lanes'], 6)

    def test_missing_repeat_identity_rejected(self):
        runs = measured_runs()
        runs[-1]['round'] = 0
        with self.assertRaisesRegex(ValueError, 'repeat'):
            rank(runs, [0, 6], 2)


@unittest.skipUnless(os.name == 'nt', 'Native Windows affinity requires Windows')
class WindowsAffinityTests(unittest.TestCase):
    def test_affinity_is_applied_before_child_code_runs(self):
        from windows_process_job import WindowsProcessJob
        code = """import ctypes,json
from ctypes import wintypes
api=ctypes.WinDLL('kernel32', use_last_error=True)
api.GetCurrentProcess.restype=wintypes.HANDLE
api.GetProcessAffinityMask.argtypes=[wintypes.HANDLE,ctypes.POINTER(ctypes.c_size_t),ctypes.POINTER(ctypes.c_size_t)]
mask,system=ctypes.c_size_t(),ctypes.c_size_t()
if not api.GetProcessAffinityMask(api.GetCurrentProcess(),ctypes.byref(mask),ctypes.byref(system)):
    raise ctypes.WinError(ctypes.get_last_error())
print(json.dumps(mask.value))
"""
        allowed = int(subprocess.check_output([sys.executable, '-c', code], text=True, timeout=5))
        selected = allowed & -allowed
        with WindowsProcessJob() as job:
            process = job.start([sys.executable, '-c', code], affinity_mask=selected,
                                stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
            output, errors = process.communicate(timeout=5)
            self.assertEqual(process.returncode, 0, errors)
            self.assertEqual(int(output), selected)


@unittest.skipUnless(os.name == 'posix', 'POSIX descendant ownership; native Windows Job Object tests cover Windows')
class CleanupTests(unittest.TestCase):
    def test_cleanup_after_parent_exit_kills_descendants_only_and_releases_port(self):
        with tempfile.TemporaryDirectory() as directory:
            root = pathlib.Path(directory)
            sentinel = subprocess.Popen([sys.executable, '-c', 'import time; time.sleep(60)'])
            owner = ProcessTree()
            child_code = "import pathlib,socket,time; s=socket.socket(socket.AF_INET,socket.SOCK_DGRAM); s.bind(('127.0.0.1',0)); pathlib.Path('port').write_text(str(s.getsockname()[1])); time.sleep(60)"
            parent_code = 'import subprocess,sys; subprocess.Popen([sys.executable,"-c",' + repr(child_code) + '])'
            try:
                parent = owner.start([sys.executable, '-c', parent_code], cwd=root, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
                parent.wait(timeout=5)
                deadline = time.monotonic() + 5
                while not (root / 'port').exists() and time.monotonic() < deadline:
                    time.sleep(.02)
                self.assertTrue((root / 'port').exists())
                port = int((root / 'port').read_text())
                with self.assertRaises(OSError):
                    select_port(socket.SOCK_DGRAM, port)
                owner.close()
                deadline = time.monotonic() + 5
                while True:
                    try:
                        select_port(socket.SOCK_DGRAM, port)
                        break
                    except OSError:
                        if time.monotonic() > deadline:
                            self.fail('Owned descendant still holds its UDP port')
                        time.sleep(.02)
                self.assertIsNone(sentinel.poll())
            finally:
                owner.close()
                sentinel.terminate()
                sentinel.wait(timeout=5)


if __name__ == '__main__':
    unittest.main()
