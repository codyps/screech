#!/usr/bin/env python3
"""Profile separate Fetch(1)/ACK after prefill, before post-workload validation."""
import argparse
import json
import signal
from pathlib import Path
import subprocess
import tempfile
import time

from benchmark import start, usage
from perf_control import recording


def wait_for(path, driver, timeout=90):
    deadline = time.monotonic() + timeout
    while not path.exists():
        if driver.poll() is not None:
            stdout, stderr = driver.communicate()
            raise RuntimeError(f'driver exited before {path.name}: {stdout}\n{stderr}')
        if time.monotonic() >= deadline:
            raise TimeoutError(f'driver did not reach {path.name}')
        time.sleep(.005)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    for name in ('screech', 'nats', 'driver', 'perf'):
        parser.add_argument('--'+name, required=True, type=lambda s: str(Path(s).resolve()))
    parser.add_argument('--output', required=True, type=Path)
    parser.add_argument('--messages', type=int, default=30000)
    parser.add_argument('--include-kernel', action='store_true')
    args = parser.parse_args()
    if args.messages <= 0:
        parser.error('messages must be positive')
    args.output.mkdir(parents=True, exist_ok=True)
    for label, binary in [('screech-unsafe-ack', args.screech), ('nats-main', args.nats)]:
        prefix = args.output / label
        with tempfile.TemporaryDirectory(prefix='jetstream-profile-') as tmp:
            directory = Path(tmp)
            gate = directory / 'phase'
            process, port, log, command = start(binary, label, True, directory)
            driver = None
            try:
                driver = subprocess.Popen([args.driver, '-url', f'nats://127.0.0.1:{port}',
                    '-scenario', 'consume-limits', '-size', '128', '-subscribers', '1',
                    '-n', str(args.messages), '-profile-gate', str(gate)],
                    stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
                wait_for(Path(str(gate)+'.ready'), driver)
                event = 'cycles' if args.include_kernel else 'cycles:u'
                with prefix.with_suffix('.perf.log').open('w') as perf_log, recording(
                    [args.perf, 'record', '-e', event, '-F', '999', '--call-graph', 'dwarf,8192',
                     '-p', str(process.pid), '-o', str(prefix.with_suffix('.data'))], perf_log
                ) as profiler:
                    before = usage(process.pid)
                    Path(str(gate)+'.release').touch()
                    wait_for(Path(str(gate)+'.complete'), driver)
                    after = usage(process.pid)
                if profiler.returncode not in (0, 128 + signal.SIGINT, -signal.SIGINT):
                    raise RuntimeError(prefix.with_suffix('.perf.log').read_text())
                Path(str(gate)+'.finish').touch()
                stdout, stderr = driver.communicate(timeout=30)
                if driver.returncode:
                    raise RuntimeError(f'driver failed: {stdout}\n{stderr}')
                result = json.loads(stdout)
                if result.get('checked') is not True:
                    raise RuntimeError('unchecked profiled workload')
                result.update(server=label, command=command,
                    profile_scope='consumer phase only; prefill and validation excluded',
                    profile_event=event, profile_frequency_hz=999,
                    profiled_server_cpu_seconds=after['cpu_seconds']-before['cpu_seconds'])
                prefix.with_suffix('.json').write_text(json.dumps(result, indent=2)+'\n')
            finally:
                if driver is not None and driver.poll() is None:
                    driver.kill()
                    driver.wait()
                process.kill()
                process.wait()
                log.close()
                for artifact in args.output.glob(label + '.*'):
                    if artifact.is_file() and not artifact.is_symlink():
                        artifact.chmod(0o644)
        for children, suffix in [(False, '.txt'), (True, '-stacks.txt')]:
            with Path(str(prefix)+suffix).open('w') as report:
                subprocess.run([args.perf, 'report', '-i', str(prefix.with_suffix('.data')),
                    '--stdio', '--children' if children else '--no-children',
                    '--percent-limit', '1'], stdout=report, check=True)
            Path(str(prefix)+suffix).chmod(0o644)


if __name__ == '__main__':
    main()
