import copy
from pathlib import Path
import sys
import unittest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]/'scripts'))
from benchmark import JETSTREAM_CASES
from check_cluster_targets import compare_cluster


class ClusterTargets(unittest.TestCase):
    def data(self, voters=3):
        rows=[]
        for server in (f'screech-cluster-{voters}', f'nats-cluster-{voters}-always'):
            for scenario,size,subs,_ in JETSTREAM_CASES:
                for trial in range(3):
                    own=server.startswith('screech')
                    rows.append(dict(server=server,scenario=scenario,payload_bytes=size,subscribers=subs,
                        publishes=1000,trial=trial,checked=True,voters=voters,stream_replicas=voters,
                        publishes_per_second=120 if own else 100,p99_us=8 if own else 10,
                        cycle_p99_us=8 if own else 10,durable_p99_us=8 if own else 10))
        return dict(environment=dict(voters=voters),rows=rows)

    def test_boundaries_and_every_cluster_workload(self):
        for voters in (3,5):
            rows=compare_cluster(self.data(voters),voters)
            self.assertEqual(len(rows),14)
            self.assertTrue(all(row['passed'] for row in rows))
            self.assertEqual(next(r for r in rows if r['scenario']=='consume-one')['latency_metric'],'cycle_p99_us')
            self.assertEqual(next(r for r in rows if r['scenario']=='core-stream')['latency_metric'],'durable_p99_us')

    def test_throughput_and_latency_cannot_hide_each_other(self):
        for throughput, latency in ((119.9,1),(1000,8.1)):
            data=self.data()
            for row in data['rows']:
                if row['server'].startswith('screech'):
                    row.update(publishes_per_second=throughput,p99_us=latency,
                               cycle_p99_us=latency,durable_p99_us=latency)
            self.assertFalse(any(row['passed'] for row in compare_cluster(data,3)))

    def test_missing_trials_wrong_voters_and_weaker_modes_are_rejected(self):
        base=self.data()
        for kind in ('missing','replicas','environment','mode','unchecked','duplicate','mixed-count'):
            data=copy.deepcopy(base)
            if kind=='missing': data['rows'].pop()
            elif kind=='replicas': data['rows'][0]['stream_replicas']=1
            elif kind=='environment': data['environment']['voters']=5
            elif kind=='mode': data['rows'][-1]['server']='nats-default'
            elif kind=='unchecked': data['rows'][0]['checked']=False
            elif kind=='duplicate': data['rows'][1]['trial']=0
            else: data['rows'][0]['publishes']=2000
            with self.subTest(kind=kind), self.assertRaises(ValueError): compare_cluster(data,3)
