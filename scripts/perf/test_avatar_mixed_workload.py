"""Mixed workload options must remain matched from CLI through capture commands."""
import argparse
from pathlib import Path
import subprocess
import sys
import unittest

from avatar_benchmark import (MIXED_DEFAULTS, WORKLOAD_FIELDS, add_mixed_options,
                              mixed_cli, mixed_client_options, validate_mixed_options, workload_metadata)
from avatar_tuning import mixed_command_provenance


class MixedWorkloadTests(unittest.TestCase):
    def parse(self, flags=()):
        parser = argparse.ArgumentParser()
        add_mixed_options(parser)
        return parser.parse_args(flags)

    def test_defaults_disable_extra_traffic_and_leave_client_options_empty(self):
        args = self.parse()
        self.assertEqual(vars(args), MIXED_DEFAULTS)
        self.assertEqual(mixed_client_options(args, Path('/run'), Path('/run/observe-start.marker')), [])
        validate_mixed_options(args)

    def test_bounds_reject_invalid_payloads_cadence_and_disabled_reliable_scene(self):
        for flags in (['--additional-avatar-bytes', '-1'], ['--additional-avatar-bytes', '256'],
                      ['--scene-data-bytes', '-1'], ['--scene-data-bytes', '23'],
                      ['--scene-data-bytes', '1025'], ['--scene-data-interval-ms', '0'],
                      ['--scene-data-reliable']):
            with self.subTest(flags=flags), self.assertRaises(ValueError):
                validate_mixed_options(self.parse(flags))
        for size in (24, 128, 1024):
            validate_mixed_options(self.parse(['--scene-data-bytes', str(size), '--additional-avatar-bytes', '255']))

    def test_forwarding_retains_all_options_and_frozen_metadata(self):
        args = self.parse(['--additional-avatar-bytes', '128', '--scene-data-bytes', '128',
                           '--scene-data-interval-ms', '50', '--scene-data-reliable'])
        self.assertEqual(vars(self.parse(mixed_cli(args))), vars(args))
        for field in WORKLOAD_FIELDS:
            setattr(args, field, 1)
        workload = workload_metadata(args)
        self.assertEqual({k: workload[k] for k in MIXED_DEFAULTS}, {k: getattr(args, k) for k in MIXED_DEFAULTS})
        path = Path('/capture/run')
        marker = path / 'observe-start.marker'
        command = ['client', '--observe-avatar-start-file', str(marker), *mixed_client_options(args, path, marker)]
        self.assertTrue(mixed_command_provenance(command, workload, path))
        for flag, value in (('--additional-avatar-bytes', '0'), ('--scene-data-bytes', '24'),
                            ('--scene-data-interval-ms', '100'), ('--scene-start-file', '/other-marker'),
                            ('--observe-scene-csv', '/other-csv')):
            changed = list(command)
            changed[changed.index(flag) + 1] = value
            with self.subTest(flag=flag):
                self.assertFalse(mixed_command_provenance(changed, workload, path))
        self.assertFalse(mixed_command_provenance([v for v in command if v != '--scene-data-reliable'], workload, path))
        with self.assertRaisesRegex(ValueError, 'Duplicate'):
            mixed_command_provenance([*command, '--scene-data-bytes', '128'], workload, path)

    def test_old_manifest_without_mixed_fields_is_compatible(self):
        self.assertTrue(mixed_command_provenance(['client'], {}, Path('/run')))

    def test_all_three_entry_points_advertise_the_same_options(self):
        for script in ('tune-avatar-settings.py', 'compare-avatar-revisions.py', 'train-avatar-pgo.py'):
            output = subprocess.check_output([sys.executable, '-B', str(Path(__file__).with_name(script)), '--help'], text=True)
            with self.subTest(script=script):
                for key in MIXED_DEFAULTS:
                    self.assertIn('--' + key.replace('_', '-'), output)


if __name__ == '__main__':
    unittest.main()
