import sys
from pathlib import Path
import unittest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'scripts'))
from profile_sync import summarize_sync


class SyncProfileTests(unittest.TestCase):
    def test_interleaved_calls_keep_their_files_and_elapsed_times(self):
        result = summarize_sync('''100 123.0 fdatasync(4</tmp/journal>) = 0 <0.000300>
101 123.1 fsync(7</tmp/db-wal> <unfinished ...>
102 123.2 fdatasync(8</tmp/db> <unfinished ...>
102 123.3 <... fdatasync resumed>) = 0 <0.004000>
101 123.4 <... fsync resumed>) = 0 <0.009000>
100 123.5 fdatasync(4</tmp/journal>) = 0 <0.000500>
''')
        self.assertEqual(result['/tmp/db-wal']['max_ms'], 9)
        self.assertEqual(result['/tmp/db']['max_ms'], 4)
        self.assertEqual(result['/tmp/journal']['calls'], 2)
        self.assertAlmostEqual(result['/tmp/journal']['total_ms'], .8)

    def test_non_sync_and_undecoded_calls_cannot_supply_file_evidence(self):
        self.assertEqual({}, summarize_sync('''100 123.0 fsync(4) = 0 <0.000300>
100 123.1 write(4</tmp/db>, "x", 1) = 1 <0.000300>
100 123.2 <... fsync resumed>) = 0 <0.000300>
100 123.3 fallocate(4</tmp/db>, 0, 0, 8388608) = 0 <0.100000>
100 123.4 ftruncate(4</tmp/db>, 8388608) = 0 <0.100000>
'''))
