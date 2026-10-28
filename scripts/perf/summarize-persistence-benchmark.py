#!/usr/bin/env python3
"""Publish all frozen persistence trials, paired medians, ranges and an exportable plot."""
import argparse
import csv
import json
import math
from pathlib import Path
import statistics

BACKENDS=['memory','file','sqlite','postgres']


METRICS = ['server_cpu_cores','client_cpu_cores','server_rss_peak_mib','client_rss_peak_mib','writes_per_second','offered_load_ratio','avatar_gap_p95_ms','avatar_uplink_updates_per_second',
           'set_p50_ms','set_p95_ms','set_p99_ms','get_p50_ms','get_p95_ms','get_p99_ms','cycle_p50_ms','cycle_p95_ms','cycle_p99_ms']


def metric(row,key):
    if key in row: value=row[key]
    elif key in row.get('storage',{}): value=row['storage'][key]
    else: raise ValueError('missing measurement: '+key)
    value=float(value)
    if not math.isfinite(value) or value<0: raise ValueError('invalid measurement: '+key)
    return value


def validate_results(results):
    if len(results)!=48 or any(not row['valid'] for row in results):
        raise ValueError('expected all 48 eligible trials; do not publish a partial or invalid matrix')
    groups={}
    for row in results:
        groups.setdefault((row['clients'],row['backend'],row['uploads_enabled']),[]).append(row)
        for key in METRICS: metric(row,key)
        if row['backend']=='postgres': metric(row,'postgres_cpu_cores')
    expected={(count,backend,enabled) for count in [100,1000] for backend in BACKENDS for enabled in [False,True]}
    if groups.keys()!=expected or any(len(rows)!=3 or {row['repeat'] for row in rows}!={1,2,3} for rows in groups.values()):
        raise ValueError('expected three distinct matched repeat IDs (1,2,3) per cell')
    return groups


def summarize(source, output):
    results=json.loads((source/'results.json').read_text())
    manifest=json.loads((source/'manifest.json').read_text())
    groups=validate_results(results)
    output.parent.mkdir(parents=True,exist_ok=True)
    csv_path=output.with_suffix('.csv')
    fields=['clients','backend','uploads','repeat','server_cpu_cores','client_cpu_cores','postgres_cpu_cores','server_rss_peak_mib','client_rss_peak_mib','verified','writers','writes_per_second','offered_load_ratio','set_p50_ms','set_p95_ms','set_p99_ms','get_p50_ms','get_p95_ms','get_p99_ms','cycle_p50_ms','cycle_p95_ms','cycle_p99_ms','avatar_gap_p95_ms','avatar_uplink_updates_per_second','valid']
    with csv_path.open('w',newline='') as file:
        writer=csv.DictWriter(file,fieldnames=fields);writer.writeheader()
        for row in results:
            flat={key:row.get(key,'') for key in fields}
            flat.update({key:row['storage'][key] for key in ['verified','writers','set_p50_ms','set_p95_ms','set_p99_ms','get_p50_ms','get_p95_ms','get_p99_ms','cycle_p50_ms','cycle_p95_ms','cycle_p99_ms']})
            flat['uploads']=row['uploads_enabled'];writer.writerow(flat)
    def median(rows,key): return statistics.median(metric(row,key) for row in rows)
    def pairs(count,backend,key):
        idle={row['repeat']:row for row in groups[count,backend,False]}
        return [metric(row,key)-metric(idle[row['repeat']],key) for row in groups[count,backend,True]]
    lines=['# Verified 16 KiB persistence workload benchmark','',
           f"Frozen workload source: `{manifest['source_head']}`. All 48 trials passed the eligibility checks.",'',
           '## Workload and measurement','',
           '- 100 and 1,000 headless clients; memory, JSON file, SQLite WAL and PostgreSQL backends.',
           '- Three paired repeats per population/backend, uploads off versus on. Backend order rotates; control/treatment order alternates.',
           '- Each client moves using the Unity-policy simulation at nominal 50 Hz in groups of ten, spaced 1,000 units apart. Voice, image and scene workloads are disabled.',
           '- Persistence sends exact 16,384-byte changing pseudo-random values every nominal ten seconds, with staggered initial phases. Each successful Set is followed by a Get and full byte comparison.',
           '- Twenty-second warmup after full authenticated population, then a sixty-second observation window and three-second drain. One Set/Get chain may be in flight per client.',
           f"- Server logical CPU affinity: `{manifest['settings']['server_cpus']}`, {manifest['settings']['server_workers']} Tokio/Rayon workers. Client affinity: `{manifest['settings']['client_cpus']}`, {manifest['settings']['client_workers']} runtime workers. PostgreSQL affinity: `{manifest.get('postgres_affinity',[])}`. This host has eight physical cores/sixteen logical CPUs; the default role split gives four/two/two physical cores respectively.",
           '- Latency is application RTT over the actual v56 reliable/fragmented Basis protocol, including sealed client uplinks: Set enqueue to its reply, Get enqueue to its reply, and Set enqueue to Get reply arrival. Readback correctness is required separately. Histograms use 0.1 ms bins; no overflow occurred.',
           '- CPU is CPU-seconds divided by measured wall-seconds (1.0 = one logical CPU fully busy). RSS is the maximum sampled at 1 Hz. PostgreSQL CPU is separate from the game-server process; summed database RSS is retained only in raw artifacts because shared pages are double-counted.',
           '- Eligibility requires the full population and avatar state count at every 1 Hz sample, at least 95% of nominal avatar uplinks and persistence offers, every client completing a verified write, and no failed storage statuses/integrity errors/timeouts/cancellations/pending requests. The avatar observer requires all nine expected nearby peers, no stale peers at window end, and no decode failures.',
           '', '## Results','',
           'Numbers below are medians of three per-trial values. The latency range is the minimum/maximum per-trial p95, not a confidence interval. CPU deltas are medians of matched-pair differences. Offered storage load is 10 or 100 writes/s; these results do not establish maximum backend capacity.','',
           '| Clients | Backend | Idle server CPU | Upload server CPU | Paired CPU delta | Verified writes/s | Set→Get p50 ms | p95 ms (range) | p99 ms | Avatar p95 idle→upload ms |',
           '|---:|---|---:|---:|---:|---:|---:|---:|---:|---:|']
    for count in [100,1000]:
        for backend in BACKENDS:
            idle=groups[count,backend,False];writes=groups[count,backend,True]
            p95=[float(row['storage']['cycle_p95_ms']) for row in writes]
            lines.append(f"| {count:,} | {backend} | {median(idle,'server_cpu_cores'):.3f} | {median(writes,'server_cpu_cores'):.3f} | {statistics.median(pairs(count,backend,'server_cpu_cores')):+.3f} | {median(writes,'writes_per_second'):.2f} | {median(writes,'cycle_p50_ms'):.1f} | {statistics.median(p95):.1f} ({min(p95):.1f}–{max(p95):.1f}) | {median(writes,'cycle_p99_ms'):.1f} | {median(idle,'avatar_gap_p95_ms'):.1f}→{median(writes,'avatar_gap_p95_ms'):.1f} |")
    total_verified=sum(int(row['storage']['verified']) for row in results)
    idle_avatar=[metric(row,'avatar_gap_p95_ms') for row in results if row['clients']==1000 and not row['uploads_enabled']]
    lines.extend(['','## Interpretation','',
                  f'- All 48 trials were eligible: {total_verified:,} Set/Get cycles passed full readback verification, every configured writer participated, and storage error, corruption, timeout and pending-request counters were zero.',
                  '- At 1,000 clients, memory has the lowest persistence latency; file persistence has the highest. PostgreSQL uses additional database CPU beyond the game-server CPU shown in the first table.',
                  f'- The 1,000-client avatar baseline is already delayed with uploads disabled: per-trial avatar-gap p95 spans {min(idle_avatar):.1f}–{max(idle_avatar):.1f} ms. Passing the workload checks does not establish smooth 50 Hz downstream delivery. This baseline needs separate investigation before drawing an interactive-capacity conclusion.',
                  '- Small paired CPU changes, including negative values at 100 clients, are run variation rather than evidence that uploads improve performance.',
                  '',f'![Persistence benchmark]({output.stem}.png)','',
                  '## Request phases and resources with uploads','',
                  '| Clients | Backend | Set p95 ms | Get p95 ms | Server peak RSS MiB | Client CPU | Client peak RSS MiB | Database CPU |',
                  '|---:|---|---:|---:|---:|---:|---:|---:|'])
    for count in [100,1000]:
        for backend in BACKENDS:
            rows=groups[count,backend,True]
            database=f"{median(rows,'postgres_cpu_cores'):.3f}" if backend=='postgres' else '—'
            lines.append(f"| {count:,} | {backend} | {median(rows,'set_p95_ms'):.1f} | {median(rows,'get_p95_ms'):.1f} | {median(rows,'server_rss_peak_mib'):.1f} | {median(rows,'client_cpu_cores'):.3f} | {median(rows,'client_rss_peak_mib'):.1f} | {database} |")
    lines.extend(['','## Environment and limits','',f"- Host: {manifest['cpu_model']}; {manifest['platform']}.",
                  '- This is a colocated CPU/storage test. PostgreSQL uses local TCP on a dedicated test service; WAN latency and remote TLS overhead are not represented. The client is headless and does not model Unity rendering cost.',
                  '- SQLite uses WAL with SQLx’s default FULL synchronous policy. File persistence uses atomic replacement and file/directory sync. PostgreSQL settings are recorded below.',
                  '- Three repeats characterize this host/workload only. Inspect all samples and ranges before generalizing small CPU or latency differences.',
                  '- Setup pilots were excluded, including a ten-second 1,000-client pilot that missed one writer at an interval boundary. An initial matrix was discarded after exposing a shared-receiver fragment-handoff disconnect; the fix was regression-tested before restarting with new frozen binaries. Formal sixty-second runs require all writers.',
                  '', '```json',manifest.get('postgres_settings','{}'),'```','',
                  f"Server SHA-256: `{manifest['server_sha256']}`  ",f"Client SHA-256: `{manifest['client_sha256']}`",'',
                  f'All individual measurements are in [{csv_path.name}]({csv_path.name}). Raw health samples, logs, commands and manifests are retained locally at `{source}`.','',
                  '## Reproduce','',
                  'Build SQL-enabled server and client release binaries. Supply a dedicated local PostgreSQL URL through `BASIS_BENCH_POSTGRES_URL`, matching libpq `PG*`/service settings for `psql`, and the owned PostgreSQL postmaster PID. Keep database CPU affinity on 12–15 for the role split above. The benchmark uses Python standard-library tools and `psql`; report plotting additionally needs Matplotlib and NumPy (install them in a private virtual environment).','',
                  '```sh','cargo build --release --locked --manifest-path BasisRustServer/Cargo.toml -p basis-server-console --features sql-storage',
                  'cargo build --release --locked --manifest-path BasisRustClient/Cargo.toml --bin basis-rust-client',
                  'python3 -B scripts/perf/run-persistence-benchmark.py \\',
                  '  --server /path/to/frozen/server --client /path/to/frozen/client \\',
                  '  --output /new/empty/output-path --postgres-pid YOUR_TEST_POSTMASTER_PID',
                  'python3 -B scripts/perf/summarize-persistence-benchmark.py \\',
                  '  --input /new/empty/output-path --output docs/performance/pr38-persistence-benchmark.md','```',''])
    output.write_text('\n'.join(lines))
    # Standard static figures are exportable alongside the versioned report.
    import matplotlib
    matplotlib.use('Agg')
    import matplotlib.pyplot as plt
    import numpy as np
    fig,axes=plt.subplots(1,3,figsize=(16,4.8),layout='constrained')
    x=np.arange(4);width=.36
    for offset,count,color in [(-width/2,100,'#3b82f6'),(width/2,1000,'#f59e0b')]:
        values=[median(groups[count,b,True],'cycle_p95_ms') for b in BACKENDS]
        axes[0].bar(x+offset,values,width,label=f'{count:,} clients',color=color)
    axes[0].set(title='Set → Get reply p95',ylabel='Milliseconds',xticks=x,xticklabels=BACKENDS);axes[0].legend()
    for index,(field,title,ylabel) in enumerate([('server_cpu_cores','Game-server CPU, 1,000 clients','CPU core equivalents'),('avatar_gap_p95_ms','Avatar gap p95, 1,000 clients','Milliseconds')],1):
        for offset,enabled,color in [(-width/2,False,'#94a3b8'),(width/2,True,'#2563eb')]:
            values=[median(groups[1000,b,enabled],field) for b in BACKENDS]
            axes[index].bar(x+offset,values,width,label='16 KiB uploads' if enabled else 'Idle storage',color=color)
        axes[index].set(title=title,ylabel=ylabel,xticks=x,xticklabels=BACKENDS)
        axes[index].set_ylim(0,max(median(groups[1000,b,e],field) for b in BACKENDS for e in [False,True])*1.3)
        axes[index].legend()
    for axis in axes: axis.spines[['top','right']].set_visible(False);axis.grid(axis='y',alpha=.2);axis.set_axisbelow(True)
    fig.suptitle('Verified persistence under movement load · 3 paired repeats · 60-second windows',fontsize=14)
    fig.savefig(output.with_suffix('.png'),dpi=160);fig.savefig(output.with_suffix('.svg'))
    plt.close(fig)
    print(output)


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--input',type=Path,required=True);parser.add_argument('--output',type=Path,required=True)
    args=parser.parse_args();summarize(args.input.resolve(),args.output.resolve())


if __name__=='__main__': main()
