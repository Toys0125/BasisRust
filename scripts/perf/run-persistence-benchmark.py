#!/usr/bin/env python3
"""Matched movement + occasional verified 16KiB persistence load, using frozen binaries.

Runs each backend with uploads disabled/enabled, interleaved over repeated samples.
PostgreSQL must be a dedicated local test instance; PGSERVICE/PG* are used by psql,
BASIS_BENCH_POSTGRES_URL by the server. Credentials are never recorded in artifacts.
"""
import argparse
import csv
import hashlib
import json
import os
from pathlib import Path
import platform
import socket
import subprocess
import time
import xml.etree.ElementTree as ET

from avatar_benchmark import ProcessTree, health, native_sampler, select_port

ROOT = Path(__file__).resolve().parents[2]
ERROR_FIELDS = ('not_found','denied','full','rate_limited','invalid','unavailable','busy','timed_out','not_connected','not_allowed','corrupt','malformed','send_errors','timeouts','canceled','stale','warmup_failures','pending','latency_overflow')


def write_json(path, value):
    path.write_text(json.dumps(value, indent=2, sort_keys=True)+'\n')


def sha256(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def avatar_metrics(path):
    result = {}
    with path.open() as file:
        for row in csv.reader(file):
            if len(row)!=2 or row[0]=='observed_channel': break
            if row[0]!='metric': result[row[0]]=row[1]
    return result


def active_states(status):
    return status.get('extended',{}).get('avatarSync',{}).get('activeStates',-1)


def healthy_population(status, clients):
    return status.get('status')=='healthy' and status.get('players_online')==clients and active_states(status)==clients


def summarize(samples, storage, avatar, clients, enabled, window, exits):
    checks = {'process_shutdown': exits==[0,0], 'full_population': bool(samples) and all(healthy_population(s['health'],clients) for s in samples),
              'full_storage_window': abs(float(storage['duration_secs'])-window)<0.01,
              'avatar_window': avatar.get('window_started')=='true' and int(avatar.get('window_ms',0))==window*1000,
              'avatar_coverage': int(avatar.get('near_peers',-1))==min(9,clients-1)}
    for field in ERROR_FIELDS: checks['zero_'+field]=int(storage[field])==0
    for field in ['missing_expected_peers','stale_peers_500ms','decode_errors','unapplied_deltas','malformed_items']:
        checks['avatar_zero_'+field]=int(avatar.get(field,-1))==0
    offered = int(storage['sets_sent'])
    checks['all_operations_verified'] = offered==int(storage['sets_ok'])==int(storage['gets_sent'])==int(storage['gets_ok'])==int(storage['verified'])
    checks['all_writers_verified'] = int(storage['writers'])==int(storage['verified_writers'])==(clients if enabled else 0)
    checks['offered_load'] = offered>=clients if enabled else offered==0
    nominal=clients*window/float(storage['interval_secs']) if enabled else 0
    checks['nominal_offer_rate']=offered>=int(nominal*.95) if enabled else offered==0
    elapsed = samples[-1]['monotonic']-samples[0]['monotonic']
    avatar_rate=(samples[-1]['health']['extended']['avatarSync'].get('inboundUpdates',0)-samples[0]['health']['extended']['avatarSync'].get('inboundUpdates',0))/elapsed
    checks['nominal_avatar_uplink_rate']=avatar_rate>=clients*50*.95
    result={'valid':all(checks.values()),'failed_checks':[key for key,value in checks.items() if not value],
            'storage':storage,'avatar':avatar,'writes_per_second':int(storage['verified'])/window,
            'write_payload_mib_per_second':int(storage['verified'])*16384/window/1048576,
            'sample_seconds':elapsed,'samples':len(samples),'avatar_uplink_updates_per_second':avatar_rate,'nominal_writes_per_second':nominal/window,'offered_load_ratio':offered/nominal if nominal else 1.0}
    for role in ['server','client','postgres']:
        if samples[0].get(role) is None: continue
        result[role+'_cpu_cores']=(samples[-1][role]['cpu_seconds']-samples[0][role]['cpu_seconds'])/elapsed
        result[role+'_rss_peak_mib']=max(s[role]['rss_bytes'] for s in samples)/1048576
    result['avatar_gap_p95_ms']=float(avatar.get('gap_p95_ms',0))
    first=samples[0]['health'].get('extended',{}).get('rawUdp',{})
    last=samples[-1]['health'].get('extended',{}).get('rawUdp',{})
    for field in ['bytesIn','bytesOut','packetsIn','packetsOut']:
        result['server_'+field+'_per_second']=(last.get(field,0)-first.get(field,0))/elapsed
    return result


def pg_query(sql):
    process=subprocess.run(['psql','-X','-w','-qAt','-v','ON_ERROR_STOP=1'],input=sql,text=True,capture_output=True)
    if process.returncode: raise RuntimeError('test PostgreSQL operation failed (connection details suppressed)')
    return process.stdout.strip()


def postgres_metrics(pid, sampler):
    # RSS is the sum across a dedicated PostgreSQL process tree and double-counts
    # shared pages. CPU is cumulative across live processes (connections stable in window).
    pids={pid}; pending=[pid]
    while pending:
        parent=pending.pop()
        try: children={int(child) for child in Path(f'/proc/{parent}/task/{parent}/children').read_text().split()}
        except FileNotFoundError: children=set()
        children-=pids; pids.update(children); pending.extend(children)
    values=[]
    for process in pids:
        try:
            counters=sampler(process)
            fields=Path(f'/proc/{process}/stat').read_text().rsplit(')',1)[1].split()
            # Include reaped child CPU so autovacuum/worker exits do not reset the total.
            counters['cpu_seconds']=sum(int(fields[index]) for index in [11,12,13,14])/os.sysconf('SC_CLK_TCK')
            values.append(counters)
        except (FileNotFoundError,ProcessLookupError): pass
    return {'cpu_seconds':sum(v['cpu_seconds'] for v in values),'rss_bytes':sum(v['rss_bytes'] for v in values),'pids':sorted(pids)}


def run_trial(args, output, backend, clients, enabled, repeat):
    output.mkdir()
    table='bench_'+hashlib.sha256(str(output).encode()).hexdigest()[:24]
    marker=output/'measurement.start'
    port=select_port(socket.SOCK_DGRAM,0)
    health_port=select_port(socket.SOCK_STREAM,0)
    base=output/'server-base'; (base/'config').mkdir(parents=True)
    tree=ET.parse(ROOT/'docs/performance/fixtures/avatar-cpu-only-server.xml')
    settings={'StorageEnabled':'true','StorageBackend':backend if backend in ('memory','file') else 'sql',
              'StorageConnectionString':'','StorageSqlTable':table,'StorageMaxBytesPerPlayer':'32768','StorageMaxTotalMegabytes':'1024',
              'OverrideAutoDiscoveryOfIpv':'true','IPv4Address':'127.0.0.1','Ipv6Enabled':'false',
              'EnableConsole':'false','HealthIncludeExtendedMetrics':'true'}
    for name,value in settings.items():
        node=tree.getroot().find(name)
        if node is None: node=ET.SubElement(tree.getroot(),name)
        node.text=value
    tree.write(base/'config/config.xml',encoding='utf-8',xml_declaration=True)
    client_config=output/'Config.xml'
    client_config.write_text('<Configuration><Password>avatar-bench-only</Password></Configuration>\n')
    env=dict(os.environ)
    for key in list(env):
        if key.startswith(('BASIS_AVATAR_DIAGNOSTIC','BASIS_VOICE_DIAGNOSTIC')) or key in ['BASIS_STORAGE_URL','BASIS_SERVER_PASSWORD','Password']: env.pop(key,None)
    server_env=dict(env,TOKIO_WORKER_THREADS=str(args.server_workers),RAYON_NUM_THREADS=str(args.server_workers),BASIS_STORAGE_URL=os.environ.get('BASIS_BENCH_POSTGRES_URL','') if backend=='postgres' else '',RUST_LOG='info',BASIS_STATUS_INTERVAL_SECS='60')
    # The fixture wins over same-named ServerConfig environment overrides.
    server_env.update({node.tag: node.text or '' for node in tree.getroot()})
    client_env=dict(env,BASIS_CLIENT_TOKIO_WORKERS=str(args.client_workers),BASIS_CLIENT_SHARED_RECEIVE='1',BASIS_CLIENT_SHARED_MAINTENANCE='1',RUST_LOG='info')
    server_cmd=[str(args.server),'--base-dir',str(base),'--port',str(port),'--no-console','--health-host','127.0.0.1','--health-port',str(health_port)]
    client_cmd=[str(args.client),'--config',str(client_config),'--ip','127.0.0.1','--port',str(port),'--clients',str(clients),'--no-reconnect',
                '--connect-timeout-ms','60000','--connect-batch-size','50','--connect-batch-delay-ms','100',
                '--spawn-group-size','10','--spawn-group-spacing','1000','--fixed-spawn-positions',
                '--movement-interval-ms','20','--movement-jitter-percent','0','--unity-avatar-policy',
                '--persistence-upload='+str(enabled).lower(),'--persistence-interval-secs',str(args.interval_seconds),
                '--observe-persistence-csv',str(output/'persistence.csv'),'--observe-persistence-start-file',str(marker),'--observe-persistence-window-secs',str(args.window_seconds),
                '--observe-avatar-csv',str(output/'avatar.csv'),'--observe-avatar-start-file',str(marker),'--observe-avatar-window-secs',str(args.window_seconds),
                '--avatar-observe-radius','40','--avatar-observe-expected-peers',str(min(9,clients-1)),
                '--quit-batch-size','100','--quit-batch-delay-ms','0']
    owner=ProcessTree(); samples=[]; sampler=native_sampler(); server=client=None
    metadata={'backend':backend,'clients':clients,'uploads_enabled':enabled,'repeat':repeat,'server_command':server_cmd,'client_command':client_cmd,
              'warmup_seconds':args.warmup_seconds,'window_seconds':args.window_seconds,'server_cpus':args.server_cpus,'client_cpus':args.client_cpus,'table':table}
    write_json(output/'run.json',metadata)
    try:
        with (output/'server.log').open('w') as server_log, (output/'client.log').open('w') as client_log:
            server=owner.start(server_cmd,cpus=args.server_cpus,cwd=ROOT,env=server_env,stdout=server_log,stderr=subprocess.STDOUT)
            deadline=time.monotonic()+90
            while True:
                if server.poll() is not None: raise RuntimeError('server exited during startup')
                try:
                    if health(health_port).get('status')=='healthy': break
                except (OSError,ValueError): pass
                if time.monotonic()>deadline: raise RuntimeError('server readiness timeout')
                time.sleep(.1)
            if backend=='postgres' and 'PostgreSql' not in (output/'server.log').read_text(): raise RuntimeError('PostgreSQL unexpectedly fell back')
            client=owner.start(client_cmd,cpus=args.client_cpus,cwd=output,env=client_env,stdin=subprocess.PIPE,stdout=client_log,stderr=subprocess.STDOUT)
            deadline=time.monotonic()+120
            while not healthy_population(health(health_port),clients):
                if client.poll() is not None: raise RuntimeError('client exited during setup')
                if time.monotonic()>deadline: raise RuntimeError('authenticated population readiness timeout')
                time.sleep(.2)
            until=time.monotonic()+args.warmup_seconds
            while time.monotonic()<until:
                if not healthy_population(health(health_port),clients): raise RuntimeError('population lost during warmup')
                time.sleep(min(.5,max(0,until-time.monotonic())))
            marker.touch()
            start=time.monotonic()
            while True:
                now=time.monotonic()
                samples.append({'monotonic':now,'server':sampler(server.pid),'client':sampler(client.pid),
                    'postgres':postgres_metrics(args.postgres_pid,sampler) if backend=='postgres' else None,'health':health(health_port)})
                if now-start>=args.window_seconds: break
                time.sleep(min(1,max(0,start+args.window_seconds-time.monotonic())))
            write_json(output/'samples.json',samples)
            # Let pre-deadline Set/Get chains finish; no new writes start after the window.
            time.sleep(args.drain_seconds)
            client.stdin.write(b'quit 100 0\n'); client.stdin.flush(); client.wait(timeout=30)
            owner.stop(server)
        storage=next(csv.DictReader((output/'persistence.csv').open()))
        result=summarize(samples,storage,avatar_metrics(output/'avatar.csv'),clients,enabled,args.window_seconds,[server.poll(),client.poll()])
        result.update(metadata)
        write_json(output/'summary.json',result)
        return result
    finally:
        owner.close()
        if backend=='postgres':
            usage='basis_storage_usage_'+hashlib.sha256(table.encode()).hexdigest()[:32]
            pg_query(f'DROP TABLE IF EXISTS {table} CASCADE; DROP TABLE IF EXISTS {usage}; DROP TABLE IF EXISTS {usage}_legacy; DROP FUNCTION IF EXISTS {usage}_fn();')


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--server',type=Path,required=True); parser.add_argument('--client',type=Path,required=True)
    parser.add_argument('--output',type=Path,required=True)
    parser.add_argument('--backends',nargs='+',choices=['memory','file','sqlite','postgres'],default=['memory','file','sqlite','postgres'])
    parser.add_argument('--clients',type=int,nargs='+',default=[100,1000]); parser.add_argument('--repeats',type=int,default=3)
    parser.add_argument('--warmup-seconds',type=int,default=20); parser.add_argument('--window-seconds',type=int,default=60)
    parser.add_argument('--interval-seconds',type=int,default=10); parser.add_argument('--drain-seconds',type=int,default=3)
    parser.add_argument('--server-workers',type=int,default=8); parser.add_argument('--client-workers',type=int,default=4)
    parser.add_argument('--server-cpus',default='0-7'); parser.add_argument('--client-cpus',default='8-11')
    parser.add_argument('--postgres-pid',type=int)
    args=parser.parse_args()
    for name in ['window_seconds','interval_seconds','repeats','server_workers','client_workers']:
        if getattr(args,name)<=0: parser.error(name+' must be positive')
    if any(count<2 for count in args.clients): parser.error('at least two clients are required')
    if 'postgres' in args.backends and (not os.environ.get('BASIS_BENCH_POSTGRES_URL') or not args.postgres_pid): parser.error('dedicated PostgreSQL URL and pid are required')
    args.server=args.server.resolve(); args.client=args.client.resolve(); args.output=args.output.resolve()
    if args.output.exists(): parser.error('output must be new')
    args.output.mkdir(parents=True)
    manifest={'server_sha256':sha256(args.server),'client_sha256':sha256(args.client),'source_head':subprocess.check_output(['git','rev-parse','HEAD'],cwd=ROOT,text=True).strip(),
              'source_diff_sha256':hashlib.sha256(subprocess.check_output(['git','diff','HEAD'],cwd=ROOT)).hexdigest(),
              'platform':platform.platform(),'cpu_model':next((line.split(':',1)[1].strip() for line in Path('/proc/cpuinfo').read_text().splitlines() if line.startswith('model name')),'unknown'),
              'affinity':sorted(os.sched_getaffinity(0)),'settings':{key:str(value) if isinstance(value,Path) else value for key,value in vars(args).items()},
              'postgres_placement':'same-host dedicated service, separate CPU affinity; CPU reported separately; summed database RSS double-counts shared pages',
              'latency_resolution_us':100}
    if 'postgres' in args.backends:
        manifest['postgres_affinity']=sorted(os.sched_getaffinity(args.postgres_pid))
        manifest['postgres_settings']=pg_query("SELECT json_build_object('version',version(),'fsync',current_setting('fsync'),'synchronous_commit',current_setting('synchronous_commit'),'shared_buffers',current_setting('shared_buffers')); ")
    write_json(args.output/'manifest.json',manifest)
    results=[]
    for count in args.clients:
        for repeat in range(args.repeats):
            backends=args.backends[repeat%len(args.backends):]+args.backends[:repeat%len(args.backends)]
            for backend in backends:
                for enabled in ([False,True] if repeat%2==0 else [True,False]):
                    label=f'{count}-{backend}-'+('writes' if enabled else 'idle')+f'-{repeat+1}'
                    print('START '+label,flush=True)
                    result=run_trial(args,args.output/label,backend,count,enabled,repeat+1)
                    results.append(result); write_json(args.output/'results.json',results)
                    print(json.dumps({'trial':label,'valid':result['valid'],'failed_checks':result['failed_checks'],'server_cpu_cores':round(result['server_cpu_cores'],3),'cycle_p95_ms':result['storage']['cycle_p95_ms'],'verified':result['storage']['verified'],'avatar_p95_ms':result['avatar_gap_p95_ms']}),flush=True)
                    if not result['valid']: raise RuntimeError('trial failed validity checks; stopping rather than accepting dropped work')
    print('COMPLETE '+str(args.output),flush=True)


if __name__=='__main__': main()
