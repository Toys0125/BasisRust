import json,pathlib,sys,tempfile,unittest
from unittest.mock import patch
sys.path.insert(0,str(pathlib.Path.cwd()/'scripts/perf'))
from server_pgo_analysis import summarize_pgo as summarize
class FinalIntegrity(unittest.TestCase):
    def run_case(self,variant,images='zero'):
        exp={'frozen':{'baseline':{'sha256':'old'},'candidate':{'sha256':'new'}}}
        data={'health':{'extended':{}}}
        if images!='missing':data['health']['extended']['images']={key:0 for key in ['egressDroppedMessages','egressDroppedBytes','cachedImages','completeImages','cacheBytes']}
        if images=='dropped':data['health']['extended']['images']['egressDroppedMessages']=1
        with tempfile.TemporaryDirectory() as tmp:
            p=pathlib.Path(tmp);(p/'samples.jsonl').write_text(json.dumps(data)+'\n')
            with patch('server_pgo_analysis.upload_summarize',return_value={'checks':{'original_gate':True},'valid':True}) as inner:
                result=summarize(p,exp,{'variant':variant})
                chosen=inner.call_args[0][1]
                self.assertEqual(chosen['frozen']['server'],exp['frozen'][variant])
                return result
    def test_correct_binary_and_available_candidate(self):self.assertTrue(self.run_case('candidate')['image_observations']['available'])
    def test_baseline_also_requires_native_image_telemetry(self):
        with self.assertRaises(KeyError):self.run_case('baseline','missing')
    def test_baseline_image_drop_disqualifies(self):self.assertFalse(self.run_case('baseline','dropped')['valid'])
    def test_image_drop_disqualifies(self):self.assertFalse(self.run_case('candidate','dropped')['valid'])
    def test_missing_candidate_telemetry_rejected(self):
        with self.assertRaises(KeyError):self.run_case('candidate','missing')
if __name__=='__main__':unittest.main()
