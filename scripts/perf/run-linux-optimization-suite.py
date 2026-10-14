#!/usr/bin/env python3
"""Run all frozen Linux optimization benchmark families, sequentially.

Build/freeze named executables in --binaries: server, registry, reliable,
transport (unit tests), protocol (unit tests), client-tests, core-gpu (GPU
unit tests). --client selects the SAME frozen workload client for controls
and candidates. Supply preencoded mono 48 kHz, 20 ms Opus clips with --audio.

micro: registry (3), fault transport (3), ignored micro/profilers (6).
live: 1,000/1,500 avatars; with audio, 250 clients/25 speakers and 100/100.
stress: 250 clients/250 speakers, retained separately from eligible trials.
profile: 1,500 avatars; 30 s, 199 Hz perf user-cycle/DWARF server+client.

No builds run here. Output must be new. Live windows are 20 s warmup/60 s
measurement, fixed ports 64142/64353, four client and 16 server/Rayon workers.
Counter scope for micro is whole process, including setup and the UDP driver.
Live summaries retain strict cumulative setup-retry checks; compare with
compare-optimization-suite.py for explicit measurement-window eligibility.
"""

import argparse
import importlib.util
import hashlib
import json
import os
import pathlib
import signal
import sys
import subprocess
import time
from optimization_suite_helpers import summarize_profile_workload

ROOT = pathlib.Path(__file__).resolve().parents[2]
parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument('suite', choices=['micro', 'live', 'stress', 'profile'])
parser.add_argument('--binaries', type=pathlib.Path, required=True)
parser.add_argument('--client', type=pathlib.Path, required=True, help='common frozen workload client')
parser.add_argument('--output', type=pathlib.Path, required=True)
parser.add_argument('--audio', type=pathlib.Path)
args = parser.parse_args()
bins = args.binaries.resolve()
out = args.output.resolve()
required = (['registry', 'reliable', 'transport', 'protocol', 'client-tests', 'core-gpu']
            if args.suite == 'micro' else ['server'])
missing = [name for name in required if not (bins/name).is_file()]
if missing:
    parser.error('missing frozen binaries: '+', '.join(missing))
if not args.client.is_file():
    parser.error('frozen workload client is missing')
if out.exists():
    parser.error(f'output already exists: {out}')
if args.suite in ['live', 'stress'] and args.audio and not args.audio.is_dir():
    parser.error('audio folder does not exist')
if args.suite in ['live', 'stress'] and args.audio and not any(
        path.is_file() for path in args.audio.glob('*.opus')):
    parser.error('audio folder contains no .opus files')
if args.suite == 'stress' and not args.audio:
    parser.error('stress suite requires --audio')
args.audio = args.audio.resolve() if args.audio else None
out.mkdir(parents=True)
failures = []
manifest = {
    'suite': args.suite,
    'binaries': {name: hashlib.sha256((bins/name).read_bytes()).hexdigest()
                 for name in required},
    'client_sha256': hashlib.sha256(args.client.read_bytes()).hexdigest(),
    'fixture_sha256': hashlib.sha256((ROOT/'docs/performance/fixtures/avatar-cpu-only-server.xml').read_bytes()).hexdigest(),
    'audio_sha256': {path.name: hashlib.sha256(path.read_bytes()).hexdigest()
                     for path in sorted(args.audio.glob('*.opus'))} if args.audio else {},
}
(out/'manifest.json').write_text(json.dumps(manifest, indent=2)+'\n')
env = dict(os.environ)
env.update(RAYON_NUM_THREADS='16', TOKIO_WORKER_THREADS='16')
for key in ['Password', 'BASIS_SERVER_PASSWORD', 'BASIS_AVATAR_DIAGNOSTICS', 'BASIS_VOICE_DIAGNOSTIC_CSV']:
    env.pop(key, None)

def run(name, cmd, timeout=600):
    print(name, flush=True)
    (out / f'{name}.command.json').write_text(json.dumps(cmd))
    with (out / f'{name}.log').open('w') as log:
        result = subprocess.Popen(cmd, env=env, stdout=log, stderr=subprocess.STDOUT)
        timed_out = False
        try:
            result.wait(timeout=timeout)
        except subprocess.TimeoutExpired:
            timed_out = True
            # Give workload harnesses time to stop their server/client groups.
            result.send_signal(signal.SIGINT)
            try:
                result.wait(timeout=30)
            except subprocess.TimeoutExpired:
                result.kill()
                result.wait()
    (out / f'{name}.status.json').write_text(json.dumps({'exit_code': result.returncode, 'timed_out': timed_out}))
    if result.returncode or timed_out:
        failures.append(name)
        print(f'{name}: FAILED ({result.returncode})', flush=True)

def perf(name, cmd):
    run(name, ['perf', 'stat', '-x', ';', '-e', 'task-clock,cycles,instructions,cache-references,cache-misses,context-switches', '-o', str(out / f'{name}.perf.csv'), '--', *map(str,cmd)])

if args.suite == 'micro':
    for peers,batch in [(1,1),(64,1),(64,64)]:
        perf(f'registry-{peers}-{batch}', [bins/'registry', '--config', ROOT/'docs/performance/fixtures/avatar-cpu-only-server.xml', '--peers', peers, '--ops', 32768//peers, '--batch', batch])
    for scenario in ['nofault','loss','reorder']:
        perf('reliable-'+scenario, [bins/'reliable', '--scenario',scenario,'--messages',512])
    for binary,test in [('transport','ordered_admission_microbench'),('transport','peer_snapshot_microbench'),('protocol','bench_word_channel_read_against_bit_loop_reference'),('client-tests','profile_receive_framing'),('client-tests','profile_750_clients_framing_cpu'),('core-gpu','profile_gpu_distance_consumption')]:
        perf(test,[bins/binary,test,'--ignored','--nocapture','--test-threads=1'])
elif args.suite in ['live', 'stress']:
    spec=importlib.util.spec_from_file_location('summary',ROOT/'scripts/perf/summarize_windows_avatar.py')
    module=importlib.util.module_from_spec(spec); spec.loader.exec_module(module)
    cases=[('avatar-1000',1000,0),('avatar-1500',1500,0)]
    if args.audio:
        cases += [('voice-10pct',250,10),('voice-100pct',100,100)]
    if args.suite == 'stress':
        cases = [('voice-100pct-250',250,100)]
    for name,clients,speakers in cases:
        capture=out/name
        cmd=['python3',str(ROOT/'scripts/perf/run-windows-avatar-workload.py'),'--server',str(bins/'server'),'--client',str(args.client.resolve()),'--server-config',str(ROOT/'docs/performance/fixtures/avatar-cpu-only-server.xml'),'--clients',str(clients),'--workers','4','--rayon-threads','16','--warmup-seconds','20','--window-seconds','60','--no-server-avatar-diagnostics','--port','64142','--health-port','64353','--output',str(capture)]
        if speakers:
            cmd += ['--voice-audio-folder',str(args.audio),'--voice-speaker-percent',str(speakers),'--no-voice-reencode','--no-client-shared-receive','--client-voice-all-clients']
        run(name,cmd)
        if (capture/'workload.json').exists():
            result=module.summarize(capture)
            (out/f'{name}.summary.json').write_text(json.dumps(result,indent=2))
            if not result['valid']:
                failures.append(name+'-validation')
            print(json.dumps({'case':name,'valid':result['valid'],'failed':[k for k,v in result['checks'].items() if not v],'metrics':result['metrics']}),flush=True)
            if speakers:
                run(name+'-voice-validation',['python3',str(ROOT/'scripts/perf/analyze-windows-voice.py'),str(capture),'--audio-folder',str(args.audio)])
        else:
            failures.append(name+'-missing-workload')
else:
    spec=importlib.util.spec_from_file_location('summary',ROOT/'scripts/perf/summarize_windows_avatar.py')
    module=importlib.util.module_from_spec(spec); spec.loader.exec_module(module)
    capture=out/'avatar-1500'
    cmd=['python3',str(ROOT/'scripts/perf/run-windows-avatar-workload.py'),'--server',str(bins/'server'),'--client',str(args.client.resolve()),'--server-config',str(ROOT/'docs/performance/fixtures/avatar-cpu-only-server.xml'),'--clients','1500','--workers','4','--rayon-threads','16','--warmup-seconds','20','--window-seconds','60','--no-server-avatar-diagnostics','--port','64142','--health-port','64353','--output',str(capture)]
    with (out/'harness.log').open('w') as log:
        harness=subprocess.Popen(cmd,env=env,stdout=log,stderr=subprocess.STDOUT)
        profiles=[]
        try:
            deadline=time.monotonic()+420
            while not (capture/'observe-start.marker').exists():
                if harness.poll() is not None or time.monotonic()>deadline:
                    raise RuntimeError('profile workload did not start measurement')
                time.sleep(.2)
            ids=json.loads((capture/'processes.json').read_text())
            for label in ['server','client']:
                profiles.append((label,subprocess.Popen(['perf','record','-q','-F','199','-e','cycles:u','--call-graph','dwarf,8192','-p',str(ids[label+'_pid']),'-o',str(out/(label+'.data')),'--','sleep','30'],stderr=log)))
            for label,p in profiles:
                if p.wait(timeout=60):
                    failures.append(label+'-perf-record')
            if harness.wait(timeout=100):
                failures.append('profile-harness')
        except Exception as error:
            failures.append('profile-workload')
            (out/'profile-error.txt').write_text(str(error)+'\n')
        finally:
            for _,p in profiles:
                if p.poll() is None:
                    p.send_signal(signal.SIGINT)
                    try:
                        p.wait(timeout=5)
                    except subprocess.TimeoutExpired:
                        p.kill()
                        p.wait()
            if harness.poll() is None:
                harness.send_signal(signal.SIGINT)
                try:
                    harness.wait(timeout=30)
                except subprocess.TimeoutExpired:
                    harness.kill()
                    harness.wait()
    summarize_profile_workload(capture, out, module, failures)
    for label in ['server','client']:
        data = out/(label+'.data')
        if label+'-perf-record' in failures or not data.is_file() or data.stat().st_size == 0:
            failures.append(label+'-perf-report')
            continue
        with (out/(label+'.report.txt')).open('w') as report:
            if subprocess.run(['perf','report','--stdio','--no-children','--percent-limit','0.5','-i',str(data)],stdout=report,stderr=subprocess.DEVNULL).returncode:
                failures.append(label+'-perf-report')

(out/'suite-status.json').write_text(json.dumps({'failed_commands':failures},indent=2)+'\n')
sys.exit(1 if failures else 0)
