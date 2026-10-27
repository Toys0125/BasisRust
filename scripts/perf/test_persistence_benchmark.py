import importlib.util
from pathlib import Path
import unittest

spec=importlib.util.spec_from_file_location('persistence_benchmark',Path(__file__).with_name('run-persistence-benchmark.py'))
bench=importlib.util.module_from_spec(spec)
spec.loader.exec_module(bench)


class PersistenceBenchmarkTests(unittest.TestCase):
    def fixture(self, enabled=True):
        health={'status':'healthy','players_online':100,'extended':{'avatarSync':{'activeStates':100}}}
        samples=[{'monotonic':0,'health':health,'server':{'cpu_seconds':1,'rss_bytes':1048576},'client':{'cpu_seconds':2,'rss_bytes':2097152}},
                 {'monotonic':60,'health':health,'server':{'cpu_seconds':61,'rss_bytes':2097152},'client':{'cpu_seconds':32,'rss_bytes':2097152}}]
        samples[0]['health']={'status':'healthy','players_online':100,'extended':{'avatarSync':{'activeStates':100,'inboundUpdates':0}}}
        samples[1]['health']={'status':'healthy','players_online':100,'extended':{'avatarSync':{'activeStates':100,'inboundUpdates':300000}}}
        row={field:'0' for field in bench.ERROR_FIELDS}
        row.update(duration_secs='60',interval_secs='10',sets_sent='600' if enabled else '0',sets_ok='600' if enabled else '0',gets_sent='600' if enabled else '0',gets_ok='600' if enabled else '0',verified='600' if enabled else '0',writers='100' if enabled else '0',verified_writers='100' if enabled else '0')
        avatar={field:'0' for field in ['missing_expected_peers','stale_peers_500ms','decode_errors','unapplied_deltas','malformed_items']}
        avatar.update(window_started='true',window_ms='60000',near_peers='9',gap_p95_ms='20.1')
        return samples,row,avatar

    def test_valid_matched_control_and_verified_write_load(self):
        for enabled in [False,True]:
            result=bench.summarize(*self.fixture(enabled),100,enabled,60,[0,0])
            self.assertTrue(result['valid'],result['failed_checks'])
            self.assertEqual(result['server_cpu_cores'],1)
            self.assertEqual(result['client_cpu_cores'],0.5)
            self.assertEqual(result['writes_per_second'],10 if enabled else 0)

    def test_fast_failed_or_unverified_work_is_never_accepted(self):
        for field in ['corrupt','full','timeouts','canceled','pending','warmup_failures','latency_overflow']:
            samples,row,avatar=self.fixture()
            row[field]='1'
            self.assertFalse(bench.summarize(samples,row,avatar,100,True,60,[0,0])['valid'],field)
        samples,row,avatar=self.fixture()
        row['verified']='599'
        self.assertFalse(bench.summarize(samples,row,avatar,100,True,60,[0,0])['valid'])
        samples,row,avatar=self.fixture()
        samples[-1]=dict(samples[-1],health={'status':'healthy','players_online':99,'extended':{'avatarSync':{'activeStates':99,'inboundUpdates':300000}}})
        self.assertFalse(bench.summarize(samples,row,avatar,100,True,60,[0,0])['valid'])
        samples,row,avatar=self.fixture()
        avatar['missing_expected_peers']='1'
        self.assertFalse(bench.summarize(samples,row,avatar,100,True,60,[0,0])['valid'])


class PublicationValidationTests(unittest.TestCase):
    def test_duplicate_repeats_missing_and_nonfinite_metrics_are_rejected(self):
        spec=importlib.util.spec_from_file_location('persistence_summary',Path(__file__).with_name('summarize-persistence-benchmark.py'))
        report=importlib.util.module_from_spec(spec);spec.loader.exec_module(report)
        def fixture():
            return [dict({key:1 for key in report.METRICS},clients=count,backend=backend,uploads_enabled=enabled,repeat=repeat,valid=True,postgres_cpu_cores=1)
                    for count in [100,1000] for backend in report.BACKENDS for enabled in [False,True] for repeat in [1,2,3]]
        self.assertEqual(len(report.validate_results(fixture())),16)
        rows=fixture();rows[1]['repeat']=1
        with self.assertRaises(ValueError):report.validate_results(rows)
        rows=fixture();del rows[0]['server_cpu_cores']
        with self.assertRaises(ValueError):report.validate_results(rows)
        rows=fixture();rows[0]['cycle_p95_ms']=float('nan')
        with self.assertRaises(ValueError):report.validate_results(rows)


if __name__=='__main__': unittest.main()
