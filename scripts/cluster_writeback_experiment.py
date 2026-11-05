#!/usr/bin/env python3
"""Experimental WAL writeback wrapper. Never a durability or target certificate."""
import ctypes
import errno
import json
import os
from pathlib import Path
import sys
import time


def hint(parent, path):
    libc = ctypes.CDLL(None, use_errno=True)
    writeback = libc.sync_file_range
    writeback.argtypes = [ctypes.c_int, ctypes.c_longlong, ctypes.c_longlong, ctypes.c_uint]
    writeback.restype = ctypes.c_int
    file = None
    calls = 0
    failure = None
    try:
        while os.getppid() == parent:
            if file is None:
                try:
                    file = os.open(path, os.O_RDWR)
                except FileNotFoundError:
                    time.sleep(.01)
                    continue
            # WRITE only queues dirty data. All original journal/checkpoint
            # fsyncs remain in the unmodified server; this proves no durability.
            calls += 1
            if writeback(file, 0, 0, 2) != 0:
                code = ctypes.get_errno()
                if code in (errno.ENOSYS, errno.EOPNOTSUPP, errno.EINVAL):
                    failure = f'unsupported writeback: {code}'
                    return
                raise OSError(code, os.strerror(code))
            time.sleep(.01)
    except OSError as error:
        failure = str(error)
    finally:
        if file is not None:
            os.close(file)
        destination = Path.cwd()/'results'/f'preflush-{parent}.json'
        temporary = destination.with_suffix('.tmp')
        temporary.write_text(json.dumps(dict(parent=parent, path=path, calls=calls, error=failure)))
        os.replace(temporary, destination)


def report(directory):
    from check_jetstream_targets import compare
    deadline = time.monotonic()+2
    while len(list(directory.glob('preflush-*.json'))) < 8 and time.monotonic() < deadline:
        time.sleep(.01)
    helpers = [json.loads(path.read_text()) for path in directory.glob('preflush-*.json')]
    assert len(helpers) == 8 and all(row['calls'] > 0 and row['error'] is None for row in helpers), helpers
    cases = []
    for mode in ('native', 'hint'):
        for voters in (3, 5):
            path = directory / f'{mode}-{voters}.json'
            data = json.loads(path.read_text())
            expected = sorted({(r['scenario'], r['payload_bytes'], r['subscribers'], r['publishes']) for r in data['rows']})
            cases.extend(dict(mode=mode, voters=voters, **row) for row in compare(
                data, expected, f'screech-cluster-{voters}', 3,
                baseline=f'nats-cluster-{voters}-always', minimum_throughput_ratio=.9,
                maximum_p99_ratio=1.1))
    result = dict(diagnostic_only=True, full_campaign=False, goal_verified=False,
                  helper_resource_usage_not_counted=True, helpers=helpers, cases=cases)
    (directory/'writeback-diagnostic.json').write_text(json.dumps(result, indent=2)+'\n')
    for row in cases:
        print(row['mode'], row['voters'], row['scenario'], row['payload_bytes'],
              f"throughput={row['throughput_ratio']:.3f} p99={row['p99_ratio']:.3f}")


if __name__ == '__main__':
    if sys.argv[1:2] == ['--hint']:
        hint(int(sys.argv[2]), sys.argv[3])
    elif sys.argv[1:2] == ['--report']:
        report(Path(sys.argv[2]))
    else:
        import subprocess
        arguments = sys.argv[1:]
        store = arguments[arguments.index('--store')+1]
        subprocess.Popen([sys.executable, __file__, '--hint', str(os.getpid()), store+'-wal'],
                         stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, close_fds=True)
        server = str(Path(__file__).resolve().parents[1]/'target/release/screech-server')
        os.execv(server, [server, *arguments])
