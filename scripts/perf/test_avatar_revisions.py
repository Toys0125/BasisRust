"""Capture aggregation must retain failures and reject unmatched/overlapping runs."""
import copy
import importlib.util
import json
from pathlib import Path
import unittest

spec = importlib.util.spec_from_file_location('avatar_revisions', Path(__file__).with_name('compare-avatar-revisions.py'))
cli = importlib.util.module_from_spec(spec)
spec.loader.exec_module(cli)


def runs():
    return [{'name': str(i), 'round': r, 'variant': v, 'valid': True,
             'started_unix_seconds': i * 10, 'finished_unix_seconds': i * 10 + 9,
             'metrics': {'server_cpu_cores': 3 if v == 'baseline' else 2.9,
                         'built_logical_avatar_work_per_second': 100 if v == 'baseline' else 90}}
            for i, (r, v) in enumerate(cli.run_order(['baseline', 'candidate'], 2))]


class RevisionAggregationTests(unittest.TestCase):
    def test_reduced_work_remains_visible_alongside_lower_cpu(self):
        result = cli.aggregate(runs(), 2)
        self.assertTrue(result['complete_and_valid'])
        for pair in result['paired_comparisons']:
            self.assertLess(pair['candidate_change_percent']['server_cpu_cores'], 0)
            self.assertLess(pair['candidate_change_percent']['built_logical_avatar_work_per_second'], 0)
        self.assertNotIn('winner', result)

    def test_zero_baseline_retains_undefined_change_and_serializes(self):
        rows = runs()
        rows[0]['metrics']['server_cpu_cores'] = 0
        rows[0]['valid'] = False
        result = cli.aggregate(rows, 2)
        self.assertFalse(result['complete_and_valid'])
        self.assertIsNone(result['paired_comparisons'][0]['candidate_change_percent']['server_cpu_cores'])
        json.dumps(result, allow_nan=False)

    def test_invalid_incomplete_duplicate_and_overlapping_runs(self):
        rows = runs()
        bad = copy.deepcopy(rows); bad[0]['valid'] = False
        duplicate = copy.deepcopy(rows); duplicate[1]['variant'] = 'baseline'
        overlap = copy.deepcopy(rows); overlap[1]['started_unix_seconds'] = 0
        for case in (rows[:-1], bad, duplicate, overlap):
            with self.subTest(case=case):
                self.assertFalse(cli.aggregate(case, 2)['complete_and_valid'])


if __name__ == '__main__':
    unittest.main()
