"""Test fail-closed promotion, fresh profiles and bounded fallback boundaries."""
import json
import os
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from server_pgo import Pipeline, build_environment, populations, require_valid, parse_options, native_target, executable_name, generation_flags, COMMON_FLAGS, server_flags


class PromotionTests(unittest.TestCase):
    def test_failed_or_unavailable_gate_cannot_be_promoted(self):
        for results in [[], [{'valid': False, 'checks': {'scene_delivery': False}}],
                        [{'valid': True, 'checks': {'zero_retransmits': False}}],
                        [{'valid': True, 'checks': {'counter': None}}], [{'valid': True, 'checks': {}}]]:
            self.assertFalse(require_valid(results))
        self.assertTrue(require_valid([{'valid': True, 'checks': {'zero_retransmits': True}}]))

    def test_failed_diagnostic_training_never_invokes_llvm_merge(self):
        pipeline = object.__new__(Pipeline)
        with patch('server_pgo.subprocess.run') as run:
            with self.assertRaises(ValueError):
                pipeline.merge([{'valid': False, 'checks': {'zero_retransmits': False}}])
            run.assert_not_called()

    def test_750_fallback_is_bounded_and_does_not_repeat_population(self):
        self.assertEqual(populations(750, 250), [750, 375, 250])
        self.assertEqual(populations(375, 250), [375, 250])
        self.assertEqual(populations(100, 100), [100])
        for clients, minimum in [(99, 99), (100, 101), (300, 0)]:
            with self.assertRaises(ValueError):
                populations(clients, minimum)

    @patch("server_pgo.platform.system", return_value="Linux")
    def test_default_and_smoke_populations_preserve_100_client_floor(self, _system):
        self.assertEqual(populations(300, 100), [300, 150, 100])
        for extra in [[], ['--smoke']]:
            args = parse_options(['--output', '/tmp/pgo-options-test', *extra])
            self.assertEqual((args.clients, args.minimum_clients), (300, 100))
        custom = parse_options(['--output', '/tmp/pgo-options-test', '--smoke',
                                '--clients', '150', '--minimum-clients', '100'])
        self.assertEqual((custom.clients, custom.minimum_clients), (150, 100))
        self.assertEqual((custom.warmup_seconds, custom.window_seconds,
                          custom.evaluation_rounds), (3, 10, 2))
        import contextlib
        import io
        with contextlib.redirect_stderr(io.StringIO()):
            with self.assertRaises(SystemExit):
                parse_options(['--output', '/tmp/pgo-options-test', '--smoke',
                               '--clients', '100', '--minimum-clients', '99'])

    def test_profiles_cannot_cross_native_targets(self):
        hosts = ['x86_64-unknown-linux-gnu', 'aarch64-unknown-linux-gnu',
                 'x86_64-pc-windows-msvc', 'aarch64-apple-darwin']
        for host in hosts:
            self.assertEqual(native_target(host), host)
            for other in hosts:
                if host != other:
                    with self.assertRaises(ValueError):
                        native_target(host, other)
        for arch in ['x86_64', 'aarch64']:
            host = arch + '-unknown-linux-gnu'
            self.assertEqual(native_target(host, arch + '-unknown-linux-musl'),
                             arch + '-unknown-linux-musl')
        self.assertEqual(executable_name('llvm-profdata', hosts[2]), 'llvm-profdata.exe')
        self.assertEqual(executable_name('llvm-profdata', hosts[3]), 'llvm-profdata')

    def test_package_exports_only_the_validated_pair_and_markdown(self):
        from types import SimpleNamespace
        from avatar_tuning import sha256
        import tarfile
        for target in ['x86_64-pc-windows-msvc', 'x86_64-unknown-linux-musl']:
            with tempfile.TemporaryDirectory() as tmp:
                root = Path(tmp)
                server = root / 'frozen-server'; server.write_bytes(b'validated PGO')
                client = root / 'frozen-client'; client.write_bytes(b'shared client')
                source = root / 'source/BasisRustServer'; source.mkdir(parents=True)
                (source / 'Config.xml').write_text('<Configuration/>')
                (root / 'PGO-VALIDATION.md').write_text('Valid test report')
                pipeline = object.__new__(Pipeline)
                pipeline.root = root; pipeline.source = root / 'source'; pipeline.target = target
                pipeline.host_target = target.replace('-musl', '-gnu')
                pipeline.options = SimpleNamespace(smoke=False)
                pipeline.verify_source = lambda: None
                binary = {'path': str(server), 'sha256': sha256(server)}
                pipeline.client = {'path': str(client), 'sha256': sha256(client)}
                good = [{'valid': True, 'checks': {'quality': True}}]
                pipeline.manifest = {'selected_clients': 100, 'validated_binary': binary,
                                     'attempts': [{'clients': 100, 'qualified': True,
                                                   'training': good, 'evaluation': good}]}
                pipeline.package(binary)
                release = root / 'release'
                self.assertEqual((release / executable_name('basis-server-console', target)).read_bytes(), server.read_bytes())
                self.assertEqual((release / executable_name('basis-rust-client', pipeline.host_target)).read_bytes(), client.read_bytes())
                for row in (release / 'SHA256SUMS').read_text().splitlines():
                    digest, name = row.split('  ',1)
                    self.assertEqual(sha256(release / name), digest)
                with tarfile.open(next((root / 'dist').glob('*.tar.gz'))) as archive:
                    self.assertEqual(set(archive.getnames()),
                                     {executable_name('basis-server-console', target),
                                      'Config.xml', 'PGO-VALIDATION.md', 'SHA256SUMS'})

    def test_macos_retention_is_matched_across_server_stages(self):
        for target in ['aarch64-apple-darwin', 'x86_64-unknown-linux-gnu',
                       'x86_64-pc-windows-msvc']:
            flags = generation_flags(target, Path('/tmp/fresh-profiles'))
            self.assertIn('-Cprofile-generate=/tmp/fresh-profiles', flags)
            self.assertEqual('-Clink-dead-code=yes' in flags, target == 'aarch64-apple-darwin')
            self.assertTrue(flags.startswith(server_flags(target)))
            self.assertEqual('-Clink-dead-code=yes' in server_flags(target), target == 'aarch64-apple-darwin')
        self.assertNotIn('link-dead-code', COMMON_FLAGS)

    def test_profile_diagnostic_inspection_never_changes_build_guard(self):
        from types import SimpleNamespace
        from avatar_tuning import sha256
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp); profile_path = root/'current.profdata'; profile_path.write_bytes(b'current')
            pipeline = object.__new__(Pipeline)
            pipeline.root=root; pipeline.source=root; pipeline.profiles=root/'profiles'
            pipeline.target=pipeline.host_target='aarch64-apple-darwin'
            pipeline.options=SimpleNamespace(toolchain='1.99.0')
            pipeline.manifest={'builds':{}}; pipeline.verify_source=lambda:None; pipeline.save=lambda:None
            def compile_warning(*args, **kwargs):
                kwargs['stdout'].write('warning: no profile data available for function _Rmissing Hash = 1 up to 0 count discarded\n')
                return SimpleNamespace(returncode=0)
            with patch('server_pgo.subprocess.run',side_effect=compile_warning), \
                 patch.object(pipeline,'explain_missing_profiles') as explain:
                with self.assertRaisesRegex(ValueError,'profile diagnostics'):
                    pipeline.build('candidate','use',{'path':str(profile_path),'sha256':sha256(profile_path)})
                explain.assert_called_once()
                self.assertTrue(pipeline.manifest['builds']['candidate']['profile_diagnostics'])

    def test_missing_profile_report_is_bounded_to_five_symbols(self):
        from types import SimpleNamespace
        with tempfile.TemporaryDirectory() as tmp:
            pipeline=object.__new__(Pipeline); pipeline.root=Path(tmp); pipeline.llvm=Path('/llvm-profdata')
            diagnostics=[f'no profile data available for function _Rmissing{i} Hash = 1' for i in range(20)]
            with patch('server_pgo.subprocess.run',return_value=SimpleNamespace(returncode=0)) as run:
                pipeline.explain_missing_profiles('candidate',diagnostics,{'path':'/current.profdata'})
                self.assertEqual(run.call_count,5)
                for call in run.call_args_list:
                    self.assertNotIn('--all-functions',call.args[0])
                    self.assertEqual(call.kwargs['timeout'],15)

    def test_inherited_flags_and_codegen_overrides_cannot_change_stages(self):
        with patch.dict(os.environ, {'CARGO_ENCODED_RUSTFLAGS': 'bad', 'RUSTFLAGS': 'bad',
                                     'LLVM_PROFILE_FILE': 'stale', 'RUSTC_WRAPPER': 'bad',
                                     'CARGO_PROFILE_RELEASE_LTO': 'false', 'CARGO_TARGET_X86_64_UNKNOWN_LINUX_GNU_RUSTFLAGS': 'bad'}):
            env, fixed, removed = build_environment(Path('/tmp/example'), '-Cprofile-use=/tmp/current')
            self.assertEqual(env['RUSTFLAGS'], fixed['RUSTFLAGS'])
            for key in ['CARGO_ENCODED_RUSTFLAGS', 'LLVM_PROFILE_FILE', 'RUSTC_WRAPPER',
                        'CARGO_PROFILE_RELEASE_LTO', 'CARGO_TARGET_X86_64_UNKNOWN_LINUX_GNU_RUSTFLAGS']:
                self.assertNotIn(key, env)
                self.assertIn(key, removed)

    def test_unexpected_raw_profile_is_not_merged(self):
        from avatar_tuning import sha256
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp); directory = root / 'clients-4'; directory.mkdir()
            raw = directory / 'training-1.profraw'; raw.write_bytes(b'fresh')
            (directory / 'stale.profraw').write_bytes(b'stale')
            pipeline = object.__new__(Pipeline); pipeline.profiles = root
            result = {'valid': True, 'checks': {'all': True}, 'raw_profiles': [{'path': str(raw), 'sha256': sha256(raw), 'bytes': 5}]}
            with patch('server_pgo.subprocess.run') as run:
                with self.assertRaisesRegex(ValueError, 'stale'):
                    pipeline.merge([result])
                run.assert_not_called()

    def test_failed_training_and_both_failed_arms_retry_without_promotion(self):
        from contextlib import nullcontext
        from types import SimpleNamespace
        from unittest.mock import Mock
        with tempfile.TemporaryDirectory() as tmp:
            pipeline = object.__new__(Pipeline)
            pipeline.root = Path(tmp)
            pipeline.options = SimpleNamespace(clients=750, minimum_clients=250, smoke=False)
            pipeline.settings = SimpleNamespace(experiment_lock=nullcontext)
            pipeline.manifest = {'attempts': []}
            pipeline.save = Mock()
            pipeline.report = Mock()
            pipeline.package = Mock()
            pipeline.build = Mock(side_effect=lambda name, stage, profile=None: {'path': name, 'sha256': name})
            pipeline.merge = Mock(return_value={'path': 'fresh-profile', 'sha256': 'hash'})
            good = {'valid': True, 'checks': {'scene_delivery': True}}
            bad = {'valid': False, 'checks': {'scene_delivery': False}}
            pipeline.cohort = Mock(side_effect=[[bad, bad], [good, good], [bad, bad],
                                               [good, good], [good, good]])
            pipeline.run()
            self.assertEqual([a['clients'] for a in pipeline.manifest['attempts']], [750, 375, 250])
            self.assertEqual([a['qualified'] for a in pipeline.manifest['attempts']], [False, False, True])
            self.assertEqual(pipeline.merge.call_count, 2)
            self.assertEqual(pipeline.manifest['selected_clients'], 250)
            pipeline.package.assert_called_once_with({'path': 'pgo-250', 'sha256': 'pgo-250'})
            self.assertNotIn('evaluation', pipeline.manifest['attempts'][0])

    def test_smoke_and_unqualified_attempt_cannot_publish(self):
        from types import SimpleNamespace
        with tempfile.TemporaryDirectory() as tmp:
            for smoke in (True, False):
                pipeline = object.__new__(Pipeline)
                pipeline.root = Path(tmp)
                pipeline.options = SimpleNamespace(smoke=smoke)
                pipeline.manifest = {'attempts': []}
                with self.assertRaisesRegex(ValueError, 'qualified non-smoke'):
                    pipeline.package({'path': 'diagnostic', 'sha256': 'hash'})
                self.assertFalse((pipeline.root / 'dist').exists())

    def test_changed_profile_is_rejected_before_build(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / 'merged.profdata'; path.write_bytes(b'changed')
            pipeline = object.__new__(Pipeline); pipeline.verify_source = lambda: None
            with self.assertRaisesRegex(ValueError, 'changed'):
                pipeline.build('pgo', 'use', {'path': str(path), 'sha256': 'previous'})


if __name__ == '__main__':
    unittest.main()
