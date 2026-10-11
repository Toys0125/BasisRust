import copy, pathlib, sys, unittest
sys.path.insert(0,str(pathlib.Path.cwd()/'scripts/perf'))

from server_pgo_analysis import upload_metrics, UPLOAD_COUNTERS
class UploadIntegrity(unittest.TestCase):
    def samples(self):
        return [{'monotonic_seconds':t,'health':{'extended':{'uploadBudget':dict(
            {key:0 for key in UPLOAD_COUNTERS},bytesPerSecondPerPlayer=131072,burstSeconds=2,
            acceptedMessages=100*t,acceptedBytes=2000*t)}}} for t in (0,1,2)]
    def test_matched_window(self):
        metrics,checks,deltas=upload_metrics(self.samples(),.5,1.5,131072,750)
        self.assertTrue(all(checks.values()))
        self.assertEqual(deltas['acceptedBytes'],2000)
        lower_metrics, _, _ = upload_metrics(self.samples(),.5,1.5,131072,250)
        self.assertEqual(lower_metrics['upload_accepted_bytes_per_client_per_second'],8)
    def test_dropped_input_disqualifies(self):
        samples=self.samples(); samples[-1]['health']['extended']['uploadBudget']['rejectedMessages']=1
        self.assertFalse(upload_metrics(samples,.5,1.5,131072,750)[1]['zero_upload_rejections'])
    def test_wrong_config_missing_reset_unbracketed(self):
        mutations=[lambda s:s[-1]['health']['extended']['uploadBudget'].update(bytesPerSecondPerPlayer=0),
                   lambda s:s[-1]['health']['extended'].pop('uploadBudget'),
                   lambda s:s[-1]['health']['extended']['uploadBudget'].update(acceptedBytes=0)]
        for mutate in mutations:
            samples=self.samples(); mutate(samples)
            with self.assertRaises((ValueError,KeyError)): upload_metrics(samples,.5,1.5,131072,750)
        with self.assertRaises(ValueError):upload_metrics(self.samples(),0,3,131072,750)
if __name__=='__main__': unittest.main()
