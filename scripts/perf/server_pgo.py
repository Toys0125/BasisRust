#!/usr/bin/env python3
"""Build, train and strictly qualify a native Linux x86-64 PGO server.

Every population attempt owns fresh profiles and A/B observations. Invalid
training/evaluation is retained and retried at a lower fixed population; only a
fully valid attempt can produce a distributable. Smoke mode never packages.
"""
import argparse
import importlib.util
import io
import json
import os
from pathlib import Path
import platform
import shutil
import statistics
import subprocess
import tarfile

import avatar_benchmark
from avatar_benchmark import run_workload, select_port, workload_metadata, write_json
from avatar_tuning import run_order, sha256
from server_pgo_analysis import summarize_pgo
import socket

ROOT = Path(__file__).resolve().parents[2]
TARGET = 'x86_64-unknown-linux-gnu'
COMMON_FLAGS = '-Cllvm-args=-pgo-warn-missing-function'
INTEGRITY_CHECKS = ('fixed_workload_provenance',
                    'control_provenance', 'mixed_command_provenance', 'matching_config_copy',
                    'frozen_server', 'frozen_client', 'client_config_provenance')
PROFILE_DIAGNOSTICS = ('no profile data', 'profile data may be out of date', 'hash mismatch',
                       'profile data mismatch', 'missing profile', 'profile counter overflow',
                       'function control flow change detected')


def populations(clients, minimum):
    if minimum < 2 or clients < minimum:
        raise ValueError('Require 2 <= --minimum-clients <= --clients')
    result = [clients]
    while result[-1] > minimum:
        result.append(max(minimum, result[-1] // 2))
    return result


def require_valid(results):
    """Never promote diagnostics, missing checks, or empty/incomplete cohorts."""
    return bool(results) and all(r.get('valid') is True and r.get('checks')
                                and all(v is True for v in r['checks'].values()) for r in results)


def build_environment(target, flags):
    env = dict(os.environ)
    removed = []
    for key in list(env):
        if key in ('RUSTFLAGS', 'CARGO_ENCODED_RUSTFLAGS', 'LLVM_PROFILE_FILE', 'RUSTC',
                   'RUSTC_WRAPPER', 'RUSTC_WORKSPACE_WRAPPER', 'RUSTUP_TOOLCHAIN') or key.startswith('CARGO_PROFILE_') or (key.startswith('CARGO_TARGET_') and key.endswith('_RUSTFLAGS')):
            removed.append(key)
            del env[key]
    fixed = {'CARGO_TARGET_DIR': str(target), 'CARGO_INCREMENTAL': '0',
             'CARGO_BUILD_JOBS': '2', 'RUSTFLAGS': flags}
    env.update(fixed)
    return env, fixed, sorted(removed)


def command_output(command):
    return subprocess.check_output(command, cwd=ROOT, text=True).strip()


def snapshot(output):
    paths = ['BasisRustServer', 'BasisRustClient', 'scripts/perf', 'docs/performance/fixtures',
             '.cargo', 'rust-toolchain', 'rust-toolchain.toml']
    if subprocess.run(['git', 'diff', '--quiet', 'HEAD', '--', *paths], cwd=ROOT).returncode:
        raise ValueError('Commit server/client/benchmark/build-configuration changes before training')
    if command_output(['git', 'ls-files', '--others', '--exclude-standard', '--', *paths]):
        raise ValueError('Untracked server/client/build inputs must be committed before training')
    revision = command_output(['git', 'rev-parse', 'HEAD'])
    tracked = command_output(['git', 'ls-files', '--', *paths]).splitlines()
    source = output / 'source'
    source.mkdir()
    archive = subprocess.check_output(['git', 'archive', revision, '--', *tracked], cwd=ROOT)
    with tarfile.open(fileobj=io.BytesIO(archive)) as tar:
        tar.extractall(source, filter='data')
    return {'revision': revision, 'source_files': {str(p.relative_to(source)): sha256(p)
             for p in sorted(source.rglob('*')) if p.is_file()}}


def check_artifacts(artifacts):
    for artifact in artifacts.values():
        if sha256(Path(artifact['path'])) != artifact['sha256']:
            raise ValueError('Frozen artifact changed: ' + artifact['path'])


def host_controls():
    """Keep observable power/governor controls fixed across every fresh process."""
    roots = (Path('/sys/class/power_supply'), Path('/sys/devices/system/cpu'))
    files = [*roots[0].glob('*/online'), *roots[0].glob('*/status'),
             *roots[1].glob('cpu*/cpufreq/scaling_governor'),
             roots[1] / 'cpufreq/boost', roots[1] / 'intel_pstate/no_turbo']
    values = {str(p): p.read_text().strip() for p in sorted(files) if p.is_file()}
    if any(value == 'Discharging' for key, value in values.items() if key.endswith('/status')):
        raise ValueError('Capacity qualification requires external power')
    return values


class Pipeline:
    def __init__(self, options):
        self.options = options
        self.root = options.output.resolve()
        self.root.mkdir(parents=True, exist_ok=False)
        self.manifest = {'smoke_only': options.smoke, 'attempts': [], 'builds': {}}
        if any(c.isspace() for c in str(self.root)):
            raise ValueError('Output path must not contain whitespace (RUSTFLAGS paths)')
        self.manifest['source'] = snapshot(self.root)
        self.source = self.root / 'source'
        self.compiler = command_output(['rustc', '+' + options.toolchain, '-vV'])
        if f'host: {TARGET}' not in self.compiler:
            raise ValueError('PGO currently supports native Linux x86-64 GNU only')
        sysroot = Path(command_output(['rustc', '+' + options.toolchain, '--print', 'sysroot']))
        self.llvm = sysroot / 'lib/rustlib' / TARGET / 'bin/llvm-profdata'
        if not self.llvm.is_file():
            raise ValueError(f'Install matching LLVM tools: rustup component add llvm-tools-preview --toolchain {options.toolchain}')
        self.manifest.update(compiler=self.compiler, target=TARGET, common_flags=COMMON_FLAGS,
                             llvm_profdata={'path': str(self.llvm), 'sha256': sha256(self.llvm),
                                            'version': command_output([str(self.llvm), '--version'])})
        self.profiles = self.root / 'profiles'
        self.profiles.mkdir()
        spec = importlib.util.spec_from_file_location('pgo_settings', ROOT / 'scripts/perf/tune-avatar-settings.py')
        self.settings = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(self.settings)
        self.manifest['host_controls'] = host_controls()
        self.manifest['host'] = {'platform': platform.platform(), 'logical_cpus': os.cpu_count(),
                                 'server_cpus': options.server_cpus, 'client_cpus': options.client_cpus}
        self.manifest['controls'] = {str(p.relative_to(ROOT)): sha256(p)
                                     for directory in ('scripts/perf', 'docs/performance/fixtures')
                                     for p in (ROOT / directory).rglob('*')
                                     if p.is_file() and p.suffix in ('.py', '.xml')}
        self.save()

    def save(self):
        write_json(self.root / 'manifest.json', self.manifest)

    def verify_source(self):
        if host_controls() != self.manifest['host_controls']:
            raise ValueError('Host power/governor controls changed during qualification')
        for name, expected in self.manifest['source']['source_files'].items():
            if sha256(self.source / name) != expected:
                raise ValueError('Frozen build source changed: ' + name)
        for name, expected in self.manifest['controls'].items():
            if sha256(ROOT / name) != expected:
                raise ValueError('Benchmark controls changed: ' + name)
        if sha256(self.llvm) != self.manifest['llvm_profdata']['sha256']:
            raise ValueError('Matching LLVM tool changed')

    def build(self, name, stage, profile=None):
        self.verify_source()
        flags = COMMON_FLAGS
        if stage == 'generate':
            flags += ' -Cprofile-generate=' + str(self.profiles)
        elif stage == 'use':
            if not profile or sha256(Path(profile['path'])) != profile['sha256']:
                raise ValueError('Validated profile missing or changed')
            flags += ' -Cprofile-use=' + profile['path']
        target = self.root / ('build-' + name)
        if target.exists():
            raise ValueError('Build output already exists')
        env, fixed, removed = build_environment(target, flags)
        client = stage == 'client'
        package = 'basis-rust-client' if client else 'basis-server-console'
        workspace = 'BasisRustClient' if client else 'BasisRustServer'
        command = ['cargo', '+' + self.options.toolchain, 'build', '--locked', '--release',
                   '--target', TARGET, '--manifest-path', str(self.source / workspace / 'Cargo.toml'), '-p', package]
        record = {'command': command, 'environment': fixed, 'cleared_environment_keys': removed}
        self.manifest['builds'][name] = record
        self.save()
        print('BUILD ' + name, flush=True)
        log = self.root / ('build-' + name + '.log')
        with log.open('w') as out:
            record['exit_code'] = subprocess.run(command, cwd=self.source, env=env, stdout=out, stderr=subprocess.STDOUT).returncode
        record['log_sha256'] = sha256(log)
        diagnostics = [line for line in log.read_text().splitlines()
                       if any(word in line.lower() for word in PROFILE_DIAGNOSTICS)]
        record['profile_diagnostics'] = diagnostics
        self.save()
        if record['exit_code'] or diagnostics:
            raise ValueError('Build failed or emitted profile diagnostics; inspect ' + str(log))
        binary = target / TARGET / 'release' / package
        fingerprints = list((target / TARGET / 'release/.fingerprint').glob(package + '-*/bin-' + package + '.json'))
        if len(fingerprints) != 1:
            raise ValueError('Ambiguous Cargo fingerprint')
        fingerprint = json.loads(fingerprints[0].read_text())
        if fingerprint['rustflags'] != flags.split():
            raise ValueError('Cargo did not use the requested PGO flags')
        ordinary = self.manifest['builds'].get('ordinary', {}).get('cargo_fingerprint')
        if not client and ordinary and fingerprint['profile'] != ordinary['profile']:
            raise ValueError('Release settings differ between PGO stages')
        record.update(path=str(binary), sha256=sha256(binary), cargo_fingerprint=fingerprint)
        self.save()
        return {'path': str(binary), 'sha256': record['sha256']}

    def cohort(self, directory, clients, binaries, training=False):
        self.verify_source()
        args_list = ['--server', binaries['baseline']['path'], '--client', self.client['path'],
                     '--output', str(directory), '--clients', str(clients),
                     '--warmup-seconds', str(self.options.warmup_seconds), '--window-seconds', str(self.options.window_seconds),
                     '--repeats', '2', '--rayon-threads', '4', '--tokio-workers', '4', '--client-workers', '4',
                     '--additional-avatar-bytes', '128', '--scene-data-bytes', '128', '--scene-data-interval-ms', '200']
        for key in ('server_cpus', 'client_cpus'):
            if getattr(self.options, key):
                args_list += ['--' + key.replace('_', '-'), getattr(self.options, key)]
        args = self.settings.parser().parse_args(args_list)
        self.settings.validate(args)
        args.port = select_port(socket.SOCK_DGRAM, 0)
        args.health_port = select_port(socket.SOCK_STREAM, 0)
        directory.mkdir()
        artifacts = dict(binaries, client=self.client)
        for key in ('server_config', 'client_config'):
            source = getattr(args, key)
            destination = directory / source.name
            shutil.copy2(source, destination)
            setattr(args, key, destination)
            artifacts[key] = {'path': str(destination), 'sha256': sha256(destination)}
        env, removed = self.settings.clean_environment(args.server_config, args.client_config)
        env.pop('LLVM_PROFILE_FILE', None)
        server_env = dict(env, BASIS_AVATAR_FLUSH_LANES='0', RAYON_NUM_THREADS='4', TOKIO_WORKER_THREADS='4',
                          BASIS_SCENE_BATCH_MS='3', BASIS_SCENE_PEER_QUEUE_LIMIT='1024', MaxUploadBytesPerSecondPerPlayer='131072')
        client_env = dict(env, BASIS_CLIENT_TOKIO_WORKERS='4', BASIS_AVATAR_DIAGNOSTICS='true')
        experiment = {'comparison_kind': 'revisions', 'comparison_dimension': 'pgo', 'os_name': os.name,
                      'frozen': artifacts, 'workload': workload_metadata(args), 'runs': [],
                      'purpose': 'excluded profile training' if training else 'strict same-source PGO A/B',
                      'cleared_environment_keys': removed}
        results = []
        order = [(i, 'baseline') for i in range(2)] if training else run_order(['baseline', 'candidate'], self.options.evaluation_rounds)
        profile_directory = self.profiles / directory.parent.name
        if training:
            profile_directory.mkdir()
        for index, (round_index, variant) in enumerate(order):
            self.verify_source()
            check_artifacts(artifacts)
            entry = {'name': f'{index + 1:02d}-{variant}', 'variant': variant, 'round': round_index,
                     'server_settings': avatar_benchmark.relevant_env(server_env),
                     'client_settings': avatar_benchmark.relevant_env(client_env)}
            experiment['runs'].append(entry)
            write_json(directory / 'experiment.json', experiment)
            selected = dict(server_env)
            if training:
                selected['LLVM_PROFILE_FILE'] = str(profile_directory / f'training-{index + 1}-%m-%p.profraw')
                entry['profile_pattern'] = selected['LLVM_PROFILE_FILE']
            args.server = Path(artifacts[variant]['path'])
            run = directory / entry['name']
            print(f'RUN {clients} clients {directory.name}/{entry["name"]}', flush=True)
            try:
                run_workload(args, run, selected, client_env)
            except (RuntimeError, subprocess.TimeoutExpired) as error:
                retryable = isinstance(error, subprocess.TimeoutExpired) or str(error).startswith(
                    ('Clients not ready:', 'Population dropped during', 'Sender diagnostic window did not finish'))
                meta = json.loads((run / 'run.json').read_text())
                if not retryable or str(meta.get('error', '')).startswith('Cleanup '):
                    raise
                result = {'name': entry['name'], 'variant': variant, 'round': round_index,
                          'valid': False, 'checks': {'completed': False}, 'metrics': {},
                          'error': str(error)}
            else:
                try:
                    result = summarize_pgo(run, experiment, entry)
                except (KeyError, TypeError) as error:
                    raise ValueError('Missing or invalid native validation telemetry') from error
            if training:
                raw = sorted(profile_directory.glob(f'training-{index + 1}-*.profraw'))
                result['raw_profiles'] = [{'path': str(p), 'sha256': sha256(p), 'bytes': p.stat().st_size} for p in raw]
                result['checks']['nonempty_raw_profiles'] = bool(raw) and all(p.stat().st_size for p in raw)
                result['valid'] = all(result['checks'].values())
            results.append(result)
            self.manifest['attempts'][-1]['training' if training else 'evaluation'] = results
            self.save()
            write_json(directory / 'experiment.json', experiment)
            write_json(directory / 'summary.json', {'experiment': experiment, 'runs': results})
            failed = [key for key, value in result['checks'].items() if not value]
            print('RESULT ' + entry['name'] + ': ' + ('PASS' if result['valid'] else 'FAIL ' + ', '.join(failed)), flush=True)
            if not result.get('error') and not all(result['checks'][key] for key in INTEGRITY_CHECKS):
                raise ValueError('Experiment integrity failed; do not retry damaged provenance')
        check_artifacts(artifacts)
        self.verify_source()
        return results

    def merge(self, training):
        if not require_valid(training):
            raise ValueError('Diagnostic training cannot produce a usable profile')
        artifacts = [p for run in training for p in run['raw_profiles']]
        if not artifacts:
            raise ValueError('No training profiles')
        directory = Path(artifacts[0]['path']).parent
        if {Path(p['path']) for p in artifacts} != set(directory.glob('*.profraw')):
            raise ValueError('Unexpected/stale raw profiles in this attempt')
        if list(self.profiles.glob('*.profraw')):
            raise ValueError('Unexpected default profiling output')
        for p in artifacts:
            if not p['bytes'] or sha256(Path(p['path'])) != p['sha256']:
                raise ValueError('Training profile changed')
        output = directory / 'merged.profdata'
        if output.exists():
            raise ValueError('Profile already exists')
        self.verify_source()
        command = [str(self.llvm), 'merge', '-o', str(output), *[p['path'] for p in artifacts]]
        subprocess.run(command, check=True)
        with (directory / 'profile-summary.txt').open('w') as log:
            subprocess.run([str(self.llvm), 'show', '--detailed-summary', str(output)], stdout=log, stderr=subprocess.STDOUT, check=True)
        return {'path': str(output), 'sha256': sha256(output), 'raw_profiles': artifacts, 'merge_command': command}

    def run(self):
        with self.settings.experiment_lock():
            ordinary = self.build('ordinary', 'release')
            self.client = self.build('client', 'client')
            instrumented = self.build('instrumented', 'generate')
            for clients in populations(self.options.clients, self.options.minimum_clients):
                directory = self.root / f'clients-{clients}'
                directory.mkdir()
                attempt = {'clients': clients, 'qualified': False}
                self.manifest['attempts'].append(attempt)
                training = self.cohort(directory / 'training', clients, {'baseline': instrumented, 'candidate': instrumented}, True)
                attempt['training'] = training
                self.save()
                if not require_valid(training):
                    print(f'RETRY: {clients}-client training failed; retained without merging profiles', flush=True)
                    continue
                profile = self.merge(training)
                attempt['profile'] = profile
                pgo = self.build(f'pgo-{clients}', 'use', profile)
                evaluation = self.cohort(directory / 'evaluation', clients, {'baseline': ordinary, 'candidate': pgo})
                attempt['evaluation'] = evaluation
                attempt['qualified'] = require_valid(evaluation)
                self.save()
                if attempt['qualified']:
                    self.manifest['selected_clients'] = clients
                    self.manifest['validated_binary'] = pgo
                    self.save()
                    self.report()
                    if not self.options.smoke:
                        self.package(pgo)
                    return
                print(f'RETRY: {clients}-client A/B failed; no artifact promoted', flush=True)
            self.report()
            raise ValueError('No population passed every training and A/B gate; no distributable produced')

    def report(self):
        lines = ['# Server PGO qualification', '', f'Source: `{self.manifest["source"]["revision"]}`.',
                 f'Target: `{TARGET}`. Smoke only: `{self.options.smoke}`.',
                 f'Qualified client count: `{self.manifest.get("selected_clients", "none")}`.', '',
                 'Profiles and A/B results are separate at each client count. Training is excluded from evaluation.',
                 '20-ms avatar updates, Unity-policy 60 FPS poses, 128 B extra avatar, 128 B Scene at 5 Hz; CPU-only, voice off.', '',
                 '| Clients | Phase / process | Scene receipt | Scene p95 bound, ms | Avatar p95 gap, ms | CPU cores | Failed gates |',
                 '| --- | --- | ---: | ---: | ---: | ---: | --- |']
        if self.manifest.get('error'):
            lines += ['', 'Qualification aborted: ' + self.manifest['error'], '']
        for attempt in self.manifest['attempts']:
            for phase in ('training', 'evaluation'):
                for result in attempt.get(phase, []):
                    m = result['metrics']
                    failed = ', '.join(k for k, v in result['checks'].items() if not v) or 'none'
                    if not m:
                        lines.append(f'| {attempt["clients"]} | {phase}/{result["name"]} | - | - | - | - | {failed}: {result.get("error", "unavailable")} |')
                        continue
                    lines.append(f'| {attempt["clients"]} | {phase}/{result["name"]} | {m["scene_observer_delivery_ratio"]:.3%} | {m["scene_latency_p95_upper_ms"]:.3f} | {m["gap_p95_ms"]:.3f} | {m["server_cpu_cores"]:.3f} | {failed} |')
        for attempt in self.manifest['attempts']:
            if not attempt.get('evaluation'):
                continue
            lines += ['', f'## {attempt["clients"]}-client descriptive arm medians', '',
                      '| Arm | Strict passes | CPU cores | Avatar p95 gap, ms | Applied items/s | Built logical work/s | Upload B/client/s |',
                      '| --- | --- | ---: | ---: | ---: | ---: | ---: |']
            for variant in ('baseline', 'candidate'):
                runs = [r for r in attempt['evaluation'] if r['variant'] == variant]
                measured = [r for r in runs if r['metrics']]
                if not measured:
                    lines.append(f'| {variant} | 0/{len(runs)} | - | - | - | - | - |')
                    continue
                values = [statistics.median(r['metrics'][key] for r in measured) for key in
                          ('server_cpu_cores', 'gap_p95_ms', 'observer_applied_items_per_second',
                           'built_logical_avatar_work_per_second', 'upload_accepted_bytes_per_client_per_second')]
                lines.append(f'| {variant} | {sum(r["valid"] for r in runs)}/{len(runs)} | ' + ' | '.join(f'{v:.3f}' for v in values) + ' |')
        lines += ['', 'A lower-count pass does not certify higher-count capacity, 20 Hz Scene or image/photo workloads.',
                  'Loopback and one receiver observer limit conclusions; received latency excludes lost messages.',
                  'All raw measurements remain in the local output directory; no raw profiles/JSON are packaged.']
        (self.root / 'PGO-VALIDATION.md').write_text('\n'.join(lines) + '\n')

    def package(self, binary):
        selected = self.manifest.get('selected_clients')
        qualified = next((a for a in self.manifest['attempts'] if a['clients'] == selected), None)
        if (self.options.smoke or not qualified or not qualified.get('qualified')
                or not require_valid(qualified.get('training', []))
                or not require_valid(qualified.get('evaluation', []))
                or self.manifest.get('validated_binary') != binary):
            raise ValueError('Only a fully qualified non-smoke attempt may be packaged')
        self.verify_source()
        check_artifacts({'server': binary})
        dist = self.root / 'dist'
        dist.mkdir()
        staging = self.root / 'package'
        staging.mkdir()
        shutil.copy2(binary['path'], staging / 'basis-server-console')
        shutil.copy2(self.source / 'BasisRustServer/Config.xml', staging / 'Config.xml')
        shutil.copy2(self.root / 'PGO-VALIDATION.md', staging / 'PGO-VALIDATION.md')
        (staging / 'SHA256SUMS').write_text(''.join(f'{sha256(p)}  {p.name}\n' for p in sorted(staging.iterdir()) if p.is_file()))
        with tarfile.open(dist / 'BasisRust-server-linux-x64-pgo.tar.gz', 'w:gz') as archive:
            for path in sorted(staging.iterdir()):
                archive.add(path, arcname=path.name)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output', type=Path, required=True)
    parser.add_argument('--toolchain', default='1.99.0')
    parser.add_argument('--clients', type=int, default=750)
    parser.add_argument('--minimum-clients', type=int, default=250)
    parser.add_argument('--warmup-seconds', type=int, default=45)
    parser.add_argument('--window-seconds', type=int, default=60)
    parser.add_argument('--evaluation-rounds', type=int, default=4)
    parser.add_argument('--server-cpus')
    parser.add_argument('--client-cpus')
    parser.add_argument('--smoke', action='store_true', help='4-client short end-to-end CI check; never packages a distributable')
    args = parser.parse_args()
    if platform.system() != 'Linux':
        parser.error('PGO qualification currently requires native Linux x86-64 GNU')
    if args.smoke:
        args.clients = args.minimum_clients = 4
        args.warmup_seconds, args.window_seconds, args.evaluation_rounds = 3, 10, 2
    populations(args.clients, args.minimum_clients)
    if args.evaluation_rounds < 2 or args.evaluation_rounds % 2 or args.warmup_seconds < 0 or args.window_seconds < 2:
        parser.error('Require an even number of evaluation rounds >=2 and a window >=2 seconds')
    if bool(args.server_cpus) != bool(args.client_cpus):
        parser.error('Specify both CPU sets, or neither')
    if args.server_cpus and set(avatar_benchmark.cpu_list(args.server_cpus)) & set(avatar_benchmark.cpu_list(args.client_cpus)):
        parser.error('Server and client CPU sets must be disjoint')
    pipeline = None
    try:
        pipeline = Pipeline(args)
        pipeline.run()
    except (ValueError, RuntimeError, subprocess.CalledProcessError) as error:
        if pipeline:
            pipeline.manifest['error'] = str(error)
            pipeline.save()
            pipeline.report()
        parser.exit(1, str(error) + '\n')


if __name__ == '__main__':
    main()
