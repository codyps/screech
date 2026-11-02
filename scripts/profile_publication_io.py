#!/usr/bin/env python3
"""Trace write and sync calls during checked durable publications, including setup."""
import argparse
import json
import os
from pathlib import Path
import shutil
import signal
import subprocess
import tempfile
import time

from benchmark import start, filesystem_type


def wait_attached(process, tracer, log, timeout=15):
    deadline = time.monotonic() + timeout
    while True:
        if tracer.poll() is not None:
            raise RuntimeError('strace exited before attachment: '+log.read_text())
        statuses = []
        for task in Path(f'/proc/{process.pid}/task').glob('*/status'):
            try:
                statuses.append(int(next(line.split()[1] for line in task.read_text().splitlines()
                    if line.startswith('TracerPid:'))))
            except FileNotFoundError:
                continue
        if statuses and all(pid == tracer.pid for pid in statuses):
            return
        if time.monotonic() >= deadline:
            raise TimeoutError('strace did not attach to all server threads')
        time.sleep(.005)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    for name in ('screech', 'nats', 'driver', 'strace'):
        parser.add_argument('--'+name, required=True, type=lambda s: str(Path(s).resolve()))
    parser.add_argument('--output', required=True, type=Path)
    parser.add_argument('--messages', type=int, default=9000)
    args = parser.parse_args()
    if args.messages <= 0:
        parser.error('messages must be positive')
    args.output.mkdir(parents=True, exist_ok=True)
    os.environ.setdefault('RUST_LOG', 'screech::storage=debug')
    for label, binary in [('screech-unsafe-ack', args.screech), ('nats-main', args.nats)]:
        prefix = args.output / label
        with tempfile.TemporaryDirectory(prefix='publication-io-') as tmp:
            directory = Path(tmp)
            process, port, server_log, command = start(binary, label, True, directory)
            tracer = None
            try:
                log_path = prefix.with_suffix('.strace.log')
                with log_path.open('w') as log:
                    tracer = subprocess.Popen([args.strace, '-f', '-qq', '-c', '-w',
                        '-e', 'trace=pwrite64,write,writev,fdatasync,fsync,futex,sendto,recvfrom',
                        '-p', str(process.pid), '-o', str(prefix.with_suffix('.txt'))],
                        stdout=log, stderr=subprocess.STDOUT)
                    wait_attached(process, tracer, log_path)
                    result = subprocess.run([args.driver, '-url', f'nats://127.0.0.1:{port}',
                        '-scenario', 'durable', '-n', str(args.messages), '-size', '128'],
                        check=True, capture_output=True, text=True, timeout=120)
                    row = json.loads(result.stdout)
                    if row.get('checked') is not True:
                        raise RuntimeError('unchecked publication workload')
                    tracer.send_signal(signal.SIGINT)
                    tracer.wait(timeout=10)
                    if tracer.returncode not in (0, 128+signal.SIGINT, -signal.SIGINT):
                        raise RuntimeError(log_path.read_text())
                    syncs = 0
                    for line in prefix.with_suffix('.txt').read_text().splitlines():
                        fields = line.split()
                        if fields and fields[-1] in ('fdatasync', 'fsync'):
                            syncs += int(fields[3])
                    if syncs < args.messages:
                        raise RuntimeError(f'missing publication syncs: {syncs} for {args.messages}')
                    row.update(server=label, command=command, sync_calls=syncs,
                        store_filesystem=filesystem_type(directory),
                        profile_scope='whole checked driver run, including setup, validation and cleanup; diagnostic timings under strace')
                    prefix.with_suffix('.json').write_text(json.dumps(row, indent=2)+'\n')
            finally:
                if tracer is not None and tracer.poll() is None:
                    tracer.send_signal(signal.SIGINT)
                    try: tracer.wait(timeout=10)
                    except subprocess.TimeoutExpired: tracer.kill(); tracer.wait()
                process.kill()
                process.wait()
                server_log.close()
                shutil.copy2(directory/'server.log', prefix.with_suffix('.server.log'))
                for artifact in args.output.glob(label+'.*'):
                    if artifact.is_file() and not artifact.is_symlink():
                        artifact.chmod(0o644)


if __name__ == '__main__':
    main()
