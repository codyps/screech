from pathlib import Path
import sys
import tempfile
import unittest
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'scripts'))
from perf_control import recording


class PerfControl(unittest.TestCase):
    def command(self, directory, behavior):
        script = Path(directory)/'fake-perf.py'
        script.write_text('''import os,sys,time
control,ack=map(int,sys.argv[-1].removeprefix('fd:').split(','))
''' + behavior)
        return [sys.executable, str(script)]
    def test_waits_for_enable_ack_and_reaps_child(self):
        with tempfile.TemporaryDirectory() as directory, tempfile.TemporaryFile() as log:
            marker=Path(directory)/'ready'
            command=self.command(directory, f'''assert os.read(control,128)==b'enable\\n'
time.sleep(.25)
open({str(marker)!r},'w').write('ready')
os.write(ack,b'ack\\n')
time.sleep(30)
''')
            with recording(command,log) as process:
                self.assertEqual(marker.read_text(),'ready')
                self.assertIsNone(process.poll())
            self.assertIsNotNone(process.poll())
    def test_accepts_perf_nul_terminated_ack(self):
        with tempfile.TemporaryDirectory() as directory, tempfile.TemporaryFile() as log:
            command=self.command(directory, "os.read(control,128)\nos.write(ack,b'ack\\n\\0')\ntime.sleep(30)\n")
            with recording(command,log) as process:
                self.assertIsNone(process.poll())
    def test_exit_and_missing_ack_fail_instead_of_running_unprofiled(self):
        with tempfile.TemporaryDirectory() as directory, tempfile.TemporaryFile() as log:
            with self.assertRaisesRegex(RuntimeError,'exited|closed'):
                with recording(self.command(directory,'sys.exit(23)\n'),log):
                    self.fail('profiler was not ready')
            with self.assertRaises(TimeoutError):
                with recording(self.command(directory,'time.sleep(30)\n'),log,ready_timeout=.1):
                    self.fail('profiler was not ready')
    def test_startup_timeout_retries_before_workload_and_reaps_both_children(self):
        with tempfile.TemporaryDirectory() as directory, tempfile.TemporaryFile() as log:
            marker = Path(directory) / 'attempts'
            command = self.command(directory, f'''from pathlib import Path
marker=Path({str(marker)!r})
attempt=int(marker.read_text())+1 if marker.exists() else 1
marker.write_text(str(attempt))
assert os.read(control,128)==b'enable\\n'
if attempt==2: os.write(ack,b'ack\\n')
time.sleep(30)
''')
            with recording(command, log, ready_timeout=.5) as process:
                self.assertEqual(marker.read_text(), '2')
                self.assertIsNone(process.poll())
            self.assertIsNotNone(process.poll())
    def test_workload_timeout_is_never_retried(self):
        with tempfile.TemporaryDirectory() as directory, tempfile.TemporaryFile() as log:
            marker = Path(directory) / 'attempts'
            command = self.command(directory, f'''from pathlib import Path
marker=Path({str(marker)!r})
marker.write_text(marker.read_text()+'x' if marker.exists() else 'x')
assert os.read(control,128)==b'enable\\n'
os.write(ack,b'ack\\n')
time.sleep(30)
''')
            with self.assertRaisesRegex(TimeoutError, 'workload'):
                with recording(command, log):
                    raise TimeoutError('workload failed')
            self.assertEqual(marker.read_text(), 'x')
