"""Test fail-closed promotion, fresh profiles and bounded fallback boundaries."""
import json
import os
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from server_pgo import Pipeline, build_environment, populations, require_valid


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
        self.assertEqual(populations(4, 4), [4])
        for clients, minimum in [(1, 1), (4, 5), (4, 0)]:
            with self.assertRaises(ValueError):
                populations(clients, minimum)

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
