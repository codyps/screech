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
                row.update(scenario=scenario,core_subjects=expected[0],unused_wildcards=expected[1],core_window=1024)
                p.write_text(json.dumps(value));site.read_campaign(p,'valid')
                for key in ['core_subjects','unused_wildcards','core_window']:
                    broken=json.loads(json.dumps(value));broken['rows'][0].pop(key);p.write_text(json.dumps(broken))
                    with self.assertRaisesRegex(ValueError,'topology'):site.read_campaign(p,'invalid')
                row['core_subjects']=1 if expected[0]!=1 else 8192
                p.write_text(json.dumps(value))
                with self.assertRaisesRegex(ValueError,'topology'):site.read_campaign(p,'mislabeled')

    def test_invalid_sync_interval_is_rejected_before_launch(self):
        for interval in ['"\nmalicious', '-1s', '1ms;bad', 'always', '0', 'bad']:
            with self.assertRaises(ValueError):benchmark.main(types.SimpleNamespace(trials=3,nats_sync_interval=[interval]))

if __name__=='__main__':unittest.main()
