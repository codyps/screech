import copy
from pathlib import Path
import sys
import unittest
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'scripts'))
from check_jetstream_targets import compare


class JetStreamTargets(unittest.TestCase):
    def data(self, scenario='consume-one'):
        rows=[]
        for server in ['screech','nats-main']:
            for trial in range(3):
                rows.append(dict(server=server,scenario=scenario,payload_bytes=128,subscribers=1,publishes=1000,trial=trial,checked=True,publishes_per_second=100,p99_us=5,cycle_p99_us=10,durable_p99_us=10))
        return {'rows':rows}
    def test_exact_boundaries_and_correct_latency_scope(self):
        data=self.data()
        for r in data['rows']:
            if r['server']=='screech':r.update(publishes_per_second=80,cycle_p99_us=20,p99_us=999)
        result=compare(data,[('consume-one',128,1,1000)],'screech')[0]
        self.assertTrue(result['passed']);self.assertEqual(result['latency_metric'],'cycle_p99_us')
        data['rows'][0]['cycle_p99_us']=21;data['rows'][1]['cycle_p99_us']=21
        self.assertFalse(compare(data,[('consume-one',128,1,1000)],'screech')[0]['passed'])
    def test_core_on_stream_uses_durable_latency(self):
        data=self.data('core-stream')
        for r in data['rows']:
            if r['server']=='screech':r.update(durable_p99_us=21,p99_us=1)
        self.assertFalse(compare(data,[('core-stream',128,1,1000)],'screech')[0]['passed'])
    def test_missing_unchecked_duplicate_and_invalid_data_fail_closed(self):
        for mutation in ['missing','unchecked','duplicate','nan','wrong-count','missing-cycle']:
            data=self.data()
            if mutation=='missing':data['rows'].pop()
            elif mutation=='unchecked':data['rows'][0]['checked']=False
            elif mutation=='duplicate':data['rows'][1]['trial']=0
            elif mutation=='nan':data['rows'][0]['publishes_per_second']=float('nan')
            elif mutation=='wrong-count':data['rows'][0]['publishes']=999
            else:del data['rows'][0]['cycle_p99_us']
            with self.subTest(mutation=mutation), self.assertRaises(ValueError):compare(data,[('consume-one',128,1,1000)],'screech')
