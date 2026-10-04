#!/usr/bin/env python3
"""Read validated crossover results; compare balanced four-run blocks.

Adjacent pairs are descriptive, not independent samples of all receiver updates.
The acceptance screen requires complete coverage and non-worse primary delivery
metrics in every block. This is a workload recovery screen, not significance.
"""
import argparse
import json
import pathlib
import statistics

parser = argparse.ArgumentParser()
parser.add_argument('capture', type=pathlib.Path)
parser.add_argument('--control', default='before')
parser.add_argument('--candidate', default='four')
parser.add_argument('--summary-only', action='store_true')
args = parser.parse_args()
if not args.control or not args.candidate or args.control == args.candidate:
    parser.error('control and candidate must be distinct nonempty run-name prefixes')
manifest = json.loads((args.capture/'experiment.json').read_text())
runs = [json.loads((args.capture/r['name']/'validated-summary.json').read_text()) for r in manifest['runs']]
if len(runs) < 8 or len(runs) % 4:
    raise ValueError('Require at least two complete balanced four-run blocks')
def group(run):
    for label in (args.control, args.candidate):
        if run['name'].startswith(label+'-'):
            return label
    raise ValueError('Unknown variant: '+run['name'])
for label in (args.control, args.candidate):
    if len({r['server_sha256'] for r in runs if group(r)==label}) != 1:
        raise ValueError('Different frozen binaries within variant')
matching = ('clients','tokio_workers','server_rayon_threads_override','server_kind',
            'server_avatar_diagnostics','warmup_seconds','measurement_window_seconds',
            'server_ip','movement_interval_ms','jitter_percent','unity_frame_accumulator_fps',
            'layout','pose','voice','p2p','server_profiling','client_binary_sha256',
            'server_config_fixture_sha256','client_config_fixture_sha256','relevant_environment')
for key in matching:
    if any(r['metadata'][key] != runs[0]['metadata'][key] for r in runs):
        raise ValueError('Mismatched instrumentation/workload: '+key)
keys = ('gap_p50_ms','gap_p95_ms','observer_items_per_second','combined_cpu_cores',
        'server_cpu_cores','client_cpu_cores','udp_payload_mb_per_second',
        'udp_datagrams_per_second','logical_avatar_sends_per_second','host_cpu_cores')
def comparison(subset):
    means = {label:{key:statistics.mean(r['metrics'][key] for r in subset if group(r)==label) for key in keys}
             for label in (args.control,args.candidate)}
    change = {key:100*(means[args.candidate][key]/means[args.control][key]-1) for key in keys}
    recovered = (change['gap_p50_ms'] <= 0 and change['gap_p95_ms'] <= 0
                 and change['observer_items_per_second'] >= 0)
    return {'order':[group(r) for r in subset], 'means':means,'candidate_change_percent':change,
            'primary_delivery_recovered':recovered}
blocks=[]
pairs=[]
for offset in range(0,len(runs),4):
    block = runs[offset:offset+4]
    if any(sum(group(r)==label for r in block)!=2 for label in (args.control,args.candidate)):
        raise ValueError('Each four-run block must have two runs per variant')
    blocks.append(comparison(block))
    for pair in (block[:2],block[2:]):
        if group(pair[0])==group(pair[1]):
            raise ValueError('Adjacent order pairs must use different variants')
        pairs.append(comparison(pair))
result = {'capture':args.capture.name,'all_delivery_checks_passed':all(r['valid'] for r in runs),
          'server_port_consistent':len({r['metadata']['server_port'] for r in runs}) == 1,
          'health_port_consistent':len({r['metadata']['health_port'] for r in runs}) == 1,
          'sequential_runs':all(a['metadata']['finished_unix_seconds'] <= b['metadata']['started_unix_seconds']
                                for a,b in zip(runs,runs[1:])),
          'aggregate':comparison(runs),'blocks':blocks,'adjacent_pairs':pairs,
          'screen_recovered':all(r['valid'] for r in runs) and all(b['primary_delivery_recovered'] for b in blocks),
          'runs':runs}
result['screen_recovered'] &= (result['server_port_consistent'] and result['health_port_consistent']
                               and result['sequential_runs'])
if args.summary_only:
    del result['runs']
print(json.dumps(result,indent=2))
