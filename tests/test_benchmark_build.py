import copy
import json
from pathlib import Path
import sys
import tempfile
import unittest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'scripts'))
from benchmark_build import BINARIES, campaigns, create, verify


class BenchmarkBuild(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        for name in BINARIES.values():
            (self.root / name).write_bytes(name.encode())
        self.build = create(self.root, 'source', 'upstream', 'rustc test')

    def write_campaign(self, name, attempt='1', **updates):
        environment = copy.deepcopy(self.build)
        environment.update(ci_run_id='run', ci_run_attempt=attempt, **updates)
        path = self.root / name
        path.write_text(json.dumps(dict(environment=environment)))
        return path

    def test_downloaded_build_detects_changed_binary_or_source(self):
        self.assertEqual(verify(self.root, 'source'), self.build)
        with self.assertRaisesRegex(ValueError, 'source revision'):
            verify(self.root, 'another-source')
        (self.root / 'nats-server').write_bytes(b'other upstream build')
        with self.assertRaisesRegex(ValueError, 'nats-server'):
            verify(self.root, 'source')

    def test_separate_topologies_share_one_build_and_attempt(self):
        paths = [self.write_campaign(f'{name}.json') for name in ('standard', 'concurrent', 'three', 'five')]
        self.assertEqual(campaigns(self.build, paths, 'run'), ('run', '1'))
        # A complete new measurement attempt remains valid; replacing only
        # one topology is rejected by the separate partial-retry regression.
        paths = [self.write_campaign(f'{name}.json', attempt='2')
                 for name in ('standard', 'concurrent', 'three', 'five')]
        self.assertEqual(campaigns(self.build, paths, 'run'), ('run', '2'))

    def test_mixed_binary_upstream_source_or_toolchain_is_rejected(self):
        for field in (*BINARIES, 'revision', 'nats_commit', 'rustc'):
            with self.subTest(field=field):
                path = self.write_campaign('mixed.json', **{field: 'other'})
                with self.assertRaisesRegex(ValueError, field):
                    campaigns(self.build, [path], 'run')

    def test_partial_measurement_retry_cannot_replace_one_topology(self):
        three = self.write_campaign('three.json')
        five = self.write_campaign('five.json', attempt='2')
        with self.assertRaisesRegex(ValueError, 'different attempts'):
            campaigns(self.build, [three, five], 'run')

    def test_missing_or_other_workflow_provenance_is_rejected(self):
        path = self.write_campaign('campaign.json')
        with self.assertRaisesRegex(ValueError, 'workflow provenance'):
            campaigns(self.build, [path], 'another-run')
        for attempt in ('', '0', 'unknown'):
            with self.subTest(attempt=attempt):
                path = self.write_campaign('campaign.json', attempt=attempt)
                with self.assertRaisesRegex(ValueError, 'workflow provenance'):
                    campaigns(self.build, [path], 'run')
        with self.assertRaisesRegex(ValueError, 'no benchmark campaigns'):
            campaigns(self.build, [], 'run')
