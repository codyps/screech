#!/usr/bin/env python3
"""Profile all voters during publications only; diagnostic timings are not targets."""
import argparse
from contextlib import contextmanager, nullcontext
import json
from pathlib import Path
import signal
import subprocess
import tempfile

from benchmark import total_usage
from benchmark_cluster import BenchmarkCluster
from perf_control import recording
from profile_jetstream import wait_for
from profile_publication_io import wait_attached
from profile_sync import summarize_sync


@contextmanager
def tracing(binary, processes, prefix, sync_only=False):
    log_path = Path(str(prefix) + '.strace.log')
    with log_path.open('w') as log:
        options = ['-ttt', '-T', '-yy', '-e', 'trace=fdatasync,fsync'] if sync_only else [
                   '-c', '-w', '-e', 'trace=pwrite64,write,writev,fdatasync,fsync,futex,sendto,recvfrom,connect']
        command = [binary, '-f', '-qq', *options,
                   '-o', str(Path(str(prefix) + '.strace.txt'))]
        for process in processes:
            command += ['-p', str(process.pid)]
        tracer = subprocess.Popen(command, stdout=log, stderr=subprocess.STDOUT)
        try:
            for process in processes:
                wait_attached(process, tracer, log_path)
            yield tracer
        finally:
            if tracer.poll() is None:
                tracer.send_signal(signal.SIGINT)
                try:
                    tracer.wait(timeout=10)
                except subprocess.TimeoutExpired:
                    tracer.kill()
                    tracer.wait()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    for name in ('screech', 'nats', 'driver'):
        parser.add_argument('--' + name, required=True, type=lambda x: str(Path(x).resolve()))
    for name in ('perf', 'strace'):
        parser.add_argument('--' + name, type=lambda x: str(Path(x).resolve()))
    parser.add_argument('--check-only', action='store_true', help='Validate phase gates without attaching profilers')
    parser.add_argument('--voters', type=int, choices=(3, 5), default=3)
    parser.add_argument('--messages', type=int, default=4000)
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    if args.messages <= 0 or not (args.check_only or args.perf or args.strace):
        parser.error('positive message count and a profiler or --check-only are required')
    args.output = args.output.resolve()
    args.output.mkdir(parents=True, exist_ok=True)
    modes = ['check'] if args.check_only else [name for name in ('perf', 'strace') if getattr(args, name)]
    if args.strace and not args.check_only:
        modes.append('sync')
    for screech, binary in ((True, args.screech), (False, args.nats)):
        label = f'screech-cluster-{args.voters}' if screech else f'nats-cluster-{args.voters}-always'
        with tempfile.TemporaryDirectory(prefix='cluster-profile-') as tmp:
            cluster = BenchmarkCluster(binary, screech, args.voters, Path(tmp)).start()
            try:
                for scenario in ('durable', 'dedup'):
                    for mode in modes:
                        prefix = args.output / f'{label}-{scenario}-{mode}'
                        gate = Path(tmp) / f'{scenario}-{mode}'
                        driver = subprocess.Popen([args.driver, '-url', f'nats://127.0.0.1:{cluster.port}',
                            '-scenario', scenario, '-replicas', str(args.voters), '-size', '128',
                            '-n', str(args.messages), '-profile-gate', str(gate)],
                            stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
                        try:
                            wait_for(Path(str(gate) + '.ready'), driver)
                            with Path(str(prefix) + '.perf.log').open('w') as log:
                                command = [args.perf, 'record', '-e', 'cycles', '-F', '199', '-m', '256',
                                    '--call-graph', 'dwarf,8192', '-p', ','.join(str(p.pid) for p in cluster.processes),
                                    '-o', str(Path(str(prefix) + '.data'))] if mode == 'perf' else []
                                profile = (recording(command, log) if mode == 'perf' else
                                    tracing(args.strace, cluster.processes, prefix, sync_only=mode == 'sync')
                                    if mode in ('strace', 'sync') else nullcontext(None))
                                with profile as process:
                                    before = total_usage(cluster.processes)
                                    Path(str(gate) + '.release').touch()
                                    wait_for(Path(str(gate) + '.complete'), driver, timeout=120)
                                    after = total_usage(cluster.processes)
                            if process is not None and process.returncode not in (0, 128 + signal.SIGINT, -signal.SIGINT):
                                raise RuntimeError(f'{mode} failed for {label}/{scenario}: {process.returncode}')
                            Path(str(gate) + '.finish').touch()
                            stdout, stderr = driver.communicate(timeout=60)
                            if driver.returncode:
                                raise RuntimeError(f'driver failed: {stdout}\n{stderr}')
                            row = json.loads(stdout)
                            if row.get('checked') is not True or row.get('stream_replicas') != args.voters:
                                raise RuntimeError('unchecked or incorrectly replicated profile')
                            row.update(server=label, profiler=mode, profile_scope='publication phase only; setup and validation excluded; diagnostic timing',
                                commands=cluster.commands, profile_frequency_hz=199 if mode == 'perf' else None,
                                phase_server_cpu_seconds=after['cpu_seconds']-before['cpu_seconds']
                                if after['cpu_seconds'] is not None and before['cpu_seconds'] is not None else None)
                            Path(str(prefix) + '.json').write_text(json.dumps(row, indent=2) + '\n')
                            if mode == 'sync':
                                summary = summarize_sync(Path(str(prefix) + '.strace.txt').read_text())
                                if not summary:
                                    raise RuntimeError('sync profiler captured no decoded file syncs')
                                Path(str(prefix) + '-sync-summary.json').write_text(json.dumps(summary, indent=2) + '\n')
                            if mode == 'perf':
                                for children, suffix in ((False, '.txt'), (True, '-stacks.txt')):
                                    with Path(str(prefix) + suffix).open('w') as report:
                                        subprocess.run([args.perf, 'report', '-i', str(Path(str(prefix) + '.data')),
                                            '--stdio', '--children' if children else '--no-children', '--percent-limit', '1'],
                                            stdout=report, check=True)
                        finally:
                            if driver.poll() is None:
                                driver.kill()
                                driver.wait()
            finally:
                cluster.close()
                cluster.save_logs(args.output / 'profile.json', label)
                for artifact in args.output.glob(label + '*'):
                    if artifact.is_file() and not artifact.is_symlink():
                        artifact.chmod(0o644)


if __name__ == '__main__':
    main()
