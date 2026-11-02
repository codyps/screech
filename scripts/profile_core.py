#!/usr/bin/env python3
"""Checked Core workloads with CPU profiles, separate from timed comparisons."""
import argparse
import json
import os
import platform
import signal
import subprocess
import tempfile
import time
from pathlib import Path

from benchmark import start, usage


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    for name in ('screech', 'nats', 'driver', 'perf'):
        parser.add_argument('--' + name, required=True, type=lambda s: str(Path(s).resolve()))
    parser.add_argument('--include-kernel', action='store_true', help='Include kernel CPU samples (CI runs this diagnostic as root)')
    parser.add_argument('--messages', type=int, default=5_000_000)
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    if args.messages <= 0:
        parser.error('--messages must be positive')
    args.output.mkdir(parents=True, exist_ok=True)
    rows = []
    event = 'cpu-clock' if args.include_kernel else 'cpu-clock:u'
    for label, binary in [('screech', args.screech), ('nats-main', args.nats)]:
        with tempfile.TemporaryDirectory(prefix='core-profile-') as tmp:
            process, port, log, command = start(binary, label, False, Path(tmp))
            profiler = None
            prefix = args.output / label
            driver = [args.driver, '-url', f'nats://127.0.0.1:{port}',
                      '-scenario', 'fanout', '-size', '1024', '-subscribers', '1']
            try:
                subprocess.run(driver + ['-n', '10000'], check=True, stdout=subprocess.DEVNULL, timeout=30)
                with prefix.with_suffix('.perf.log').open('w') as perf_log:
                    profiler = subprocess.Popen([args.perf, 'record', '-e', event, '-F', '199',
                        '--call-graph', 'dwarf,4096', '-p', str(process.pid), '-o', str(prefix.with_suffix('.data'))],
                        stdout=perf_log, stderr=subprocess.STDOUT)
                    time.sleep(.2)
                    if profiler.poll() is not None:
                        raise RuntimeError(prefix.with_suffix('.perf.log').read_text())
                    before = usage(process.pid)
                    result = subprocess.run(driver + ['-n', str(args.messages)], check=True,
                                            capture_output=True, text=True, timeout=90)
                    after = usage(process.pid)
                    row = json.loads(result.stdout)
                    if not row['checked']:
                        raise RuntimeError('unchecked profile workload')
                    row.update(server=label, command=command,
                        server_cpu_seconds=after['cpu_seconds'] - before['cpu_seconds'],
                        server_rss_kib=after['rss_kib'])
                    rows.append(row)
                    profiler.send_signal(signal.SIGINT)
                    if profiler.wait(timeout=15) not in (0, 128 + signal.SIGINT, -signal.SIGINT):
                        raise RuntimeError(prefix.with_suffix('.perf.log').read_text())
                    profiler = None
                with prefix.with_suffix('.txt').open('w') as report:
                    subprocess.run([args.perf, 'report', '-i', str(prefix.with_suffix('.data')), '--stdio',
                                    '--no-children', '--percent-limit', '1'], stdout=report, check=True)
            finally:
                if profiler is not None and profiler.poll() is None:
                    profiler.send_signal(signal.SIGINT)
                    try:
                        profiler.wait(timeout=15)
                    except subprocess.TimeoutExpired:
                        profiler.kill()
                        profiler.wait()
                process.kill()
                process.wait()
                log.close()
                for artifact in args.output.glob(label + '.*'):
                    if artifact.is_file() and not artifact.is_symlink():
                        artifact.chmod(0o644)
    cpu = next((line.split(':', 1)[1].strip() for line in Path('/proc/cpuinfo').read_text().splitlines()
                if line.startswith('model name')), 'unknown')
    (args.output / 'profile.json').write_text(json.dumps({
        'scope': 'profiled diagnostic workloads; use benchmark-results for throughput comparisons',
        'cpu': cpu, 'kernel': platform.release(), 'logical_cpus': os.cpu_count(),
        'server_workers': 2, 'event': event, 'frequency_hz': 199,
        'rows': rows}, indent=2) + '\n')
    (args.output / 'profile.json').chmod(0o644)


if __name__ == '__main__':
    main()
