import json
from pathlib import Path
import tempfile
import types
import unittest
from unittest.mock import patch
import sys
SCRIPTS = Path(__file__).resolve().parents[1]/'scripts'
sys.path.insert(0, str(SCRIPTS))
import benchmark
import render_benchmarks as site
from benchmark_cluster import BenchmarkCluster

class BenchmarkSiteTests(unittest.TestCase):
    def fixture(self, directory):
        p=Path(directory)/'input.json'
        value={'environment':{'cpu':'test'},'rows':[{'checked':True,'server':'screech','scenario':'durable','payload_bytes':128,'subscribers':1,'publishes':10,'deliveries':10,'seconds':1,'deliveries_per_second':10,'trial':0}]}
        p.write_text(json.dumps(value));return p,value
    def test_generated_site_round_trips_injected_text_as_data(self):
        with tempfile.TemporaryDirectory() as tmp:
            p,value=self.fixture(tmp);out=Path(tmp)/'site';label='</script><script>alert(1)</script>'
            result=site.build([(label,p)],out,revision='abc')
            text=(out/'data.js').read_text()
            self.assertNotIn('</script>',text)
            self.assertEqual(json.loads(text.removeprefix('window.BENCHMARK_DATA = ').strip().removesuffix(';'))['campaigns'][0]['label'],label)
            self.assertEqual(json.loads((out/result['campaigns'][0]['download']).read_text()),value)
            for name in ['index.html','dashboard.js','dashboard.css','.nojekyll']:self.assertTrue((out/name).exists())
    def test_unchecked_invalid_and_duplicate_trials_rejected_before_writing(self):
        with tempfile.TemporaryDirectory() as tmp:
            p,value=self.fixture(tmp)
            for mutate in [lambda v:v['rows'][0].update(checked=False),lambda v:v['rows'][0].update(seconds=0),lambda v:v['rows'][0].update(p50_us=float('nan')),lambda v:v['rows'].append(v['rows'][0].copy()),lambda v:v['rows'][0].pop('trial')]:
                v=json.loads(json.dumps(value));mutate(v);p.write_text(json.dumps(v));out=Path(tmp)/'site'
                with self.assertRaises(ValueError):site.build([('bad',p)],out)
                self.assertFalse(out.exists())
    def test_sync_modes_reach_the_actual_server_config(self):
        with tempfile.TemporaryDirectory() as tmp:
            for label,expected in [('nats-main','sync_interval: "always"'),('nats-default',None),('nats-sync-10ms','sync_interval: "10ms"')]:
                with patch.object(benchmark.subprocess,'Popen',return_value=types.SimpleNamespace(poll=lambda:None)),patch.object(benchmark,'Client',return_value=types.SimpleNamespace(close=lambda:None)):
                    _,_,log,_=benchmark.start('/nats',label,True,Path(tmp))
                    log.close()
                config=(Path(tmp)/'nats.conf').read_text()
                if expected:self.assertIn(expected,config)
                else:self.assertNotIn('sync_interval',config)
    def test_scaling_case_topology_is_required_before_publication(self):
        with tempfile.TemporaryDirectory() as tmp:
            p,value=self.fixture(tmp)
            for scenario,expected in site.CORE_TOPOLOGIES.items():
                row=value['rows'][0]
                row.update(scenario=scenario,core_subjects=expected[0],unused_wildcards=expected[1],verified_unused_wildcards=expected[1],core_window=1024)
                p.write_text(json.dumps(value));site.read_campaign(p,'valid')
                for key in ['core_subjects','unused_wildcards','verified_unused_wildcards','core_window']:
                    broken=json.loads(json.dumps(value));broken['rows'][0].pop(key);p.write_text(json.dumps(broken))
                    with self.assertRaisesRegex(ValueError,'topology'):site.read_campaign(p,'invalid')
                row['core_subjects']=1 if expected[0]!=1 else 8192
                p.write_text(json.dumps(value))
                with self.assertRaisesRegex(ValueError,'topology'):site.read_campaign(p,'mislabeled')

    def test_invalid_sync_interval_is_rejected_before_launch(self):
        for interval in ['"\nmalicious', '-1s', '1ms;bad', 'always', '0', 'bad']:
            with self.assertRaises(ValueError):benchmark.main(types.SimpleNamespace(trials=3,nats_sync_interval=[interval]))

    def test_cluster_rows_cannot_claim_wrong_replication(self):
        with tempfile.TemporaryDirectory() as tmp:
            p,value=self.fixture(tmp)
            for voters in (3,5):
                value['environment']['voters']=voters
                for server in (f'screech-cluster-{voters}',f'nats-cluster-{voters}-always'):
                    value['rows'][0].update(server=server,voters=voters,stream_replicas=voters)
                    p.write_text(json.dumps(value));site.read_campaign(p,'valid')
                    for key in ('voters','stream_replicas'):
                        broken=json.loads(json.dumps(value));broken['rows'][0][key]=1;p.write_text(json.dumps(broken))
                        with self.assertRaisesRegex(ValueError,'replication topology'):site.read_campaign(p,'invalid')

    def test_cluster_start_failure_reaps_already_started_voters(self):
        with tempfile.TemporaryDirectory() as tmp:
            cluster=BenchmarkCluster('/server',True,3,Path(tmp))
            from unittest.mock import Mock
            process=Mock();process.poll.return_value=None
            with patch('benchmark_cluster.subprocess.Popen',side_effect=[process,OSError('launch failed')]):
                with self.assertRaisesRegex(OSError,'launch failed'):cluster.start()
            process.kill.assert_called_once();process.wait.assert_called_once()
            self.assertTrue(all(log.closed for log in cluster.logs))

    def test_nats_readiness_requires_all_replicas_current(self):
        from unittest.mock import Mock
        with tempfile.TemporaryDirectory() as tmp:
            cluster=BenchmarkCluster('/server',False,3,Path(tmp))
            clients=[Mock(),Mock()]
            def info(current):
                return {'config':{'num_replicas':3},'cluster':{'leader':'n1','replicas':[{'current':True},{'current':current}]}}
            clients[0].request.side_effect=[info(True),info(False)]
            clients[1].request.side_effect=[info(False),info(True),{'success':True}]
            with patch('benchmark_cluster.Client',side_effect=clients),patch('benchmark_cluster.time.sleep') as sleep:
                cluster.wait_ready()
            sleep.assert_called_once()
            for client in clients:client.close.assert_called_once()
            self.assertEqual(clients[1].request.call_args[0][0],'$JS.API.STREAM.DELETE.BENCH_READY')

    def test_usage_sums_voters_and_preserves_missing_samples(self):
        processes=[types.SimpleNamespace(pid=1),types.SimpleNamespace(pid=2)]
        with patch.object(benchmark,'usage',side_effect=[dict(rss_kib=10,cpu_seconds=2),dict(rss_kib=30,cpu_seconds=3)]):
            self.assertEqual(benchmark.total_usage(processes),dict(rss_kib=40,cpu_seconds=5))
        with patch.object(benchmark,'usage',side_effect=[dict(rss_kib=10,cpu_seconds=2),dict(rss_kib=None,cpu_seconds=None)]):
            self.assertEqual(benchmark.total_usage(processes),dict(rss_kib=None,cpu_seconds=None))

    def test_screech_readiness_requires_every_voter_applied(self):
        import sqlite3
        with tempfile.TemporaryDirectory() as tmp:
            cluster=BenchmarkCluster('/server',True,3,Path(tmp))
            for i,commit in enumerate((10,10,9),1):
                with sqlite3.connect(Path(tmp)/str(i)/'db') as db:
                    db.execute('create table cluster_meta(key text primary key,value blob)')
                    db.execute('insert into cluster_meta values(?,?)',('view',commit.to_bytes(8,'little')))
            self.assertFalse(cluster.all_applied(10))
            with sqlite3.connect(Path(tmp)/'3/db') as db:
                db.execute("update cluster_meta set value=? where key='view'",((10).to_bytes(8,'little'),))
            self.assertTrue(cluster.all_applied(10))

    def test_cluster_campaign_rejects_weaker_durability_before_launch(self):
        defaults=dict(trials=1,nats_sync_interval=[],cluster_messages=1000,cluster_voters=3,
                      screech_unsafe_ack=False,screech_before=None,nats_default=False)
        for option in (dict(screech_unsafe_ack=True),dict(screech_before='/old'),
                       dict(nats_default=True),dict(nats_sync_interval=['10ms'])):
            with self.assertRaisesRegex(ValueError,'strict Screech and NATS always'):
                benchmark.main(types.SimpleNamespace(**(defaults | option)))

    def test_screech_readiness_waits_for_a_fixed_committed_target(self):
        from unittest.mock import Mock,call
        with tempfile.TemporaryDirectory() as tmp:
            cluster=BenchmarkCluster('/server',True,3,Path(tmp))
            client=Mock();client.request.side_effect=[{'voters':[1,2,3]}, {'config':{'num_replicas':3}},
                {'commit_index':7}, {'success':True}]
            with patch('benchmark_cluster.Client',return_value=client),patch.object(cluster,'all_applied',side_effect=[False,True]) as applied,patch('benchmark_cluster.time.sleep'):
                cluster.wait_ready()
            self.assertEqual(applied.call_args_list,[call(7),call(7)])
            self.assertEqual(client.request.call_count,4)

if __name__=='__main__':unittest.main()
