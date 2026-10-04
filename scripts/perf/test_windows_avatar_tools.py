"""Check comparison identity and Windows ownership using isolated local children.

Run with `rtk proxy python -B scripts/perf/test_windows_avatar_tools.py`.
Fixtures are retained in a new ignored captures directory for each invocation.
"""
import ctypes
import json
import os
import pathlib
import socket
import subprocess
import sys
import time
import unittest
import uuid

from windows_process_job import WindowsProcessJob

ROOT = pathlib.Path(__file__).resolve().parents[2]
CAPTURE = ROOT/'captures'/('avatar-tools-tests-'+uuid.uuid4().hex)
CAPTURE.mkdir(parents=True)


class ComparisonTests(unittest.TestCase):
    def compare(self, identical=False):
        """Exercise default labels with eight synthetic validated run summaries."""
        metadata = {
            'clients':750, 'tokio_workers':4, 'server_rayon_threads_override':None,
            'server_kind':'rust', 'server_avatar_diagnostics':False,
            'warmup_seconds':45, 'measurement_window_seconds':60,
            'server_ip':'127.0.0.1', 'movement_interval_ms':20, 'jitter_percent':0,
            'unity_frame_accumulator_fps':60, 'layout':'colocated', 'pose':'synthetic',
            'voice':False, 'p2p':False, 'server_profiling':False,
            'client_binary_sha256':'3'*64, 'server_config_fixture_sha256':'4'*64,
            'client_config_fixture_sha256':'5'*64, 'relevant_environment':{},
            'server_port':64142, 'health_port':64353,
        }
        runs = []
        counts = {'before':0, 'six':0}
        for index, label in enumerate(('before','six','six','before',
                                       'six','before','before','six')):
            counts[label] += 1
            candidate = label == 'six'
            # Deliberately invented measurements and hashes: test inputs only.
            metrics = {
                'gap_p50_ms':90 if candidate else 100,
                'gap_p95_ms':140 if candidate else 150,
                'observer_items_per_second':1100 if candidate else 1000,
                'combined_cpu_cores':5, 'server_cpu_cores':4, 'client_cpu_cores':1,
                'udp_payload_mb_per_second':100, 'udp_datagrams_per_second':10000,
                'logical_avatar_sends_per_second':1000000, 'host_cpu_cores':10,
            }
            server_hash = ('2' if candidate and not identical else '1')*64
            runs.append({'name':f'{label}-{counts[label]}', 'valid':True,
                         'server_sha256':server_hash, 'metrics':metrics,
                         'metadata':dict(metadata, server_binary_sha256=server_hash,
                                         started_unix_seconds=index*120,
                                         finished_unix_seconds=index*120+105)})
        directory = CAPTURE/('identical' if identical else 'distinct')
        directory.mkdir()
        manifest = {'runs':[{'name':run['name']} for run in runs]}
        for run in runs:
            (directory/run['name']).mkdir()
            (directory/run['name']/'validated-summary.json').write_text(json.dumps(run))
        (directory/'experiment.json').write_text(json.dumps(manifest))
        return subprocess.run(['rtk','proxy',sys.executable,'-B',
                               str(ROOT/'scripts/perf/compare-windows-avatar-crossover.py'),
                               str(directory),'--summary-only'],capture_output=True,text=True,timeout=20)

    def test_six_candidate_is_the_default(self):
        """The six-job label works without an explicit candidate argument."""
        result = self.compare()
        self.assertEqual(result.returncode,0,result.stderr)
        self.assertTrue(json.loads(result.stdout)['screen_recovered'])

    def test_identical_binaries_are_rejected(self):
        """A differently labelled repeat of one executable cannot claim recovery."""
        result = self.compare(identical=True)
        self.assertNotEqual(result.returncode,0)
        self.assertIn('different frozen server binaries',result.stderr)


@unittest.skipUnless(os.name == 'nt','Windows Job Objects require Windows')
class ProcessOwnershipTests(unittest.TestCase):
    def setUp(self):
        """Use a fresh retained fixture directory for each owned-process test."""
        self.directory = CAPTURE/self._testMethodName
        self.directory.mkdir()

    def wait_for(self,path):
        """Wait a bounded time for this fixture's readiness signal."""
        deadline = time.monotonic()+15
        while time.monotonic()<deadline:
            try:
                return json.loads(path.read_text())
            except (FileNotFoundError,json.JSONDecodeError):
                time.sleep(0.05)
        self.fail('Fixture readiness timed out: '+str(path))

    def alive(self,pid):
        """Inspect only a recorded fixture PID, without altering its process."""
        api = ctypes.WinDLL('kernel32',use_last_error=True)
        api.OpenProcess.argtypes = [ctypes.c_uint32,ctypes.c_int,ctypes.c_uint32]
        api.OpenProcess.restype = ctypes.c_void_p
        api.WaitForSingleObject.argtypes = [ctypes.c_void_p,ctypes.c_uint32]
        api.WaitForSingleObject.restype = ctypes.c_uint32
        api.CloseHandle.argtypes = [ctypes.c_void_p]
        handle = api.OpenProcess(0x00100000,False,pid)  # SYNCHRONIZE
        if not handle:
            return False
        try:
            return api.WaitForSingleObject(handle,0)==258  # WAIT_TIMEOUT
        finally:
            api.CloseHandle(handle)

    def exercise_cleanup(self,terminate_proxy):
        """Own an RTK proxy, Python worker and socket-owning grandchild."""
        child = self.directory/'child.py'
        worker = self.directory/'worker.py'
        ready = self.directory/'ready.json'
        child.write_text('import json,os,pathlib,socket,sys,time\n'
                         's=socket.socket(socket.AF_INET,socket.SOCK_DGRAM)\n'
                         's.bind(("127.0.0.1",0))\n'
                         'pathlib.Path(sys.argv[1]).write_text(json.dumps({"pid":os.getpid(),"port":s.getsockname()[1]}))\n'
                         'time.sleep(120)\n')
        worker.write_text('import json,os,pathlib,subprocess,sys,time\n'
                          'child=subprocess.Popen(["rtk","proxy",sys.executable,sys.argv[1],sys.argv[2]])\n'
                          'pathlib.Path(sys.argv[3]).write_text(json.dumps({"pid":os.getpid()}))\n'
                          'time.sleep(120)\n')
        outside_job = WindowsProcessJob()
        outside = None
        try:
            outside = outside_job.start(['rtk','proxy',sys.executable,'-c','import time; time.sleep(120)'],
                                        creationflags=subprocess.CREATE_NEW_PROCESS_GROUP)
            with WindowsProcessJob() as job:
                process = job.start(['rtk','proxy',sys.executable,str(worker),str(child),str(ready),
                                     str(self.directory/'worker.json')],
                                    creationflags=subprocess.CREATE_NEW_PROCESS_GROUP)
                descendant = self.wait_for(ready)
                worker_pid = self.wait_for(self.directory/'worker.json')['pid']
                if terminate_proxy:
                    process.terminate()
                    process.wait(timeout=10)
            process.wait(timeout=10)
            deadline = time.monotonic()+10
            while (self.alive(descendant['pid']) or self.alive(worker_pid)) and time.monotonic()<deadline:
                time.sleep(0.05)
            self.assertFalse(self.alive(descendant['pid']))
            self.assertFalse(self.alive(worker_pid))
            self.assertIsNone(outside.poll(),'Unrelated process must remain untouched')
            with socket.socket(socket.AF_INET,socket.SOCK_DGRAM) as probe:
                probe.bind(('127.0.0.1',descendant['port']))
        finally:
            # Independently own the sentinel; close neither job by searching PIDs.
            outside_job.close()
            if outside is not None:
                outside.wait(timeout=10)

    def test_job_close_kills_owned_descendants(self):
        """Closing ownership cleans descendants even when the proxy is still alive."""
        self.exercise_cleanup(False)

    def test_forced_proxy_exit_kills_owned_descendants(self):
        """A killed RTK proxy cannot strand its worker or socket-owning grandchild."""
        self.exercise_cleanup(True)

    def test_resume_failure_does_not_start_workload(self):
        """Launch errors terminate the suspended process before any child can run."""
        marker = self.directory/'started.json'
        with WindowsProcessJob() as job:
            def fail(_):
                raise RuntimeError('test resume failure')
            job._resume_primary_thread = fail
            with self.assertRaisesRegex(RuntimeError,'test resume failure'):
                job.start(['rtk','proxy',sys.executable,'-c',
                           'import pathlib,sys; pathlib.Path(sys.argv[1]).write_text("true")',str(marker)])
        self.assertFalse(marker.exists())


if __name__ == '__main__':
    print('Retained test fixtures:',CAPTURE,flush=True)
    unittest.main(verbosity=2)
