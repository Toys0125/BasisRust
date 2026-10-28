import importlib.util
import contextlib
import io
import json
from pathlib import Path
import tempfile
import unittest
from unittest import mock

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
    def setUp(self):
        spec=importlib.util.spec_from_file_location('persistence_summary',Path(__file__).with_name('summarize-persistence-benchmark.py'))
        self.report=importlib.util.module_from_spec(spec);spec.loader.exec_module(self.report)

    def publication_fixture(self):
        settings=dict(clients=[100,1000],backends=self.report.BACKENDS,repeats=3,
                      warmup_seconds=7,window_seconds=120,interval_seconds=30,drain_seconds=5,
                      server_cpus='0-3',server_workers=4,client_cpus='4-5',client_workers=2)
        manifest=dict(settings=settings,source_head='test',server_sha256='server',client_sha256='client',
                      cpu_model='test CPU',platform='test OS',affinity=list(range(12)),postgres_affinity=[6,7])
        rows=[]
        for count in [100,1000]:
            for backend in self.report.BACKENDS:
                for enabled in [False,True]:
                    for repeat in [1,2,3]:
                        row=dict({key:1 for key in self.report.METRICS},clients=count,backend=backend,
                                 uploads_enabled=enabled,repeat=repeat,valid=True,postgres_cpu_cores=1,
                                 **{key:settings[key] for key in ['warmup_seconds','window_seconds','drain_seconds','server_cpus','client_cpus','server_workers','client_workers']})
                        row['storage']={key:1 for key in self.report.METRICS if key.startswith(('set_','get_','cycle_'))}
                        row['storage'].update(interval_secs='30',verified='400' if enabled else '0',writers=str(count) if enabled else '0')
                        rows.append(row)
        return manifest,rows

    def test_custom_timings_and_affinity_are_reported_and_reproduced(self):
        manifest,rows=self.publication_fixture()
        with tempfile.TemporaryDirectory() as directory:
            source=Path(directory);output=source/'report.md'
            (source/'manifest.json').write_text(json.dumps(manifest))
            (source/'results.json').write_text(json.dumps(rows))
            with mock.patch.object(self.report,'plot_results') as plot,contextlib.redirect_stdout(io.StringIO()):
                self.report.summarize(source,output)
            text=output.read_text()
            for expected in ['every nominal 30 seconds','7-second warmup','120-second observation window and 5-second drain',
                             '3.33333 writes/s (100 clients), 33.3333 writes/s (1,000 clients)',
                             'Server logical CPU affinity: `0-3`, 4','Client affinity: `4-5`, 2',
                             'PostgreSQL affinity: `[6, 7]`','Host process allowed logical CPUs: `[0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11]`',
                             '--warmup-seconds 7 --window-seconds 120','--interval-seconds 30 --drain-seconds 5',
                             '--server-cpus 0-3 --server-workers 4','--client-cpus 4-5 --client-workers 2']:
                self.assertIn(expected,text)
            for stale in ['ten seconds','sixty-second','eight physical','four/two/two','12–15']:
                self.assertNotIn(stale,text)
            self.assertEqual(plot.call_args.args[2],120)

    def test_mixed_or_unsupported_settings_are_rejected_before_publication(self):
        for field in ['warmup_seconds','window_seconds','drain_seconds','server_cpus','client_cpus','server_workers','client_workers']:
            manifest,rows=self.publication_fixture();rows[0][field]='mismatch'
            with self.assertRaisesRegex(ValueError,'trial disagrees with manifest: '+field):
                self.report.validate_settings(manifest,rows)
        manifest,rows=self.publication_fixture();rows[0]['storage']['interval_secs']='10'
        with self.assertRaisesRegex(ValueError,'interval_seconds'):
            self.report.validate_settings(manifest,rows)
        for field in ['window_seconds','interval_seconds','server_workers','client_workers']:
            manifest,rows=self.publication_fixture();manifest['settings'][field]=0
            with self.assertRaisesRegex(ValueError,'invalid saved setting: '+field):
                self.report.validate_settings(manifest,rows)
        manifest,rows=self.publication_fixture();manifest['settings']['clients']=[100]
        with self.assertRaisesRegex(ValueError,'full 100/1,000-client'):
            self.report.validate_settings(manifest,rows)

    def test_legacy_settings_are_disclosed_and_partial_metadata_is_rejected(self):
        manifest,rows=self.publication_fixture()
        del rows[0]['server_workers']
        with self.assertRaisesRegex(ValueError,'incomplete per-trial setting: server_workers'):
            self.report.validate_settings(manifest,rows)
        for row in rows:
            row.pop('server_workers',None);row.pop('client_workers');row.pop('drain_seconds')
        self.report.validate_settings(manifest,rows)
        with tempfile.TemporaryDirectory() as directory:
            source=Path(directory);output=source/'report.md'
            (source/'manifest.json').write_text(json.dumps(manifest))
            (source/'results.json').write_text(json.dumps(rows))
            with mock.patch.object(self.report,'plot_results'),contextlib.redirect_stdout(io.StringIO()):
                self.report.summarize(source,output)
            self.assertIn('Legacy trial records omit `server_workers, client_workers, drain_seconds`',output.read_text())
            self.assertIn('not independently cross-checked per trial',output.read_text())

    def test_duplicate_repeats_missing_and_nonfinite_metrics_are_rejected(self):
        report=self.report
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
