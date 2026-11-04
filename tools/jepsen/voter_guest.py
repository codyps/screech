#!/usr/bin/env python3
"""One native-ext4 voter per disposable VM; no LazyFS and no host disk access."""
import argparse
import base64
from http.server import ThreadingHTTPServer
import hashlib
import json
import os
from pathlib import Path
import signal
import socketserver
import sqlite3
import subprocess
import threading
import time
from control import damage
from guest import Control, Gateway

ROOT = Path('/data/screech-jepsen')
NODE = None
SERVER = None
MAINTENANCE = False
DIRECTORY = None
BOOT_ID = None
FRESH_DISK = False
DURABLE = b'durable-canary-v1'
STABLE = b'stable-before-unsynced-overwrite'


def write_synced(path, data):
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open('wb') as file:
        file.write(data)
        file.flush()
        os.fsync(file.fileno())
    descriptor = os.open(path.parent, os.O_RDONLY | os.O_DIRECTORY)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def canary_values():
    return {'clean': (DIRECTORY / 'synced-canary').read_text(),
            'dirty': (DIRECTORY / 'dirty-canary').read_text(), 'boot_id': BOOT_ID}


def initialize():
    DIRECTORY.mkdir(parents=True, exist_ok=True)
    (DIRECTORY / 'store').mkdir(exist_ok=True)
    if FRESH_DISK:
        write_synced(DIRECTORY / 'synced-canary', DURABLE)
        write_synced(DIRECTORY / 'dirty-canary', STABLE)
        os.sync()  # Initial fixture only; never run this on a recovery boot.
    else:
        assert (DIRECTORY / 'synced-canary').exists() and (DIRECTORY / 'dirty-canary').exists(), 'power-loss control files disappeared'


def start():
    global SERVER
    assert not MAINTENANCE, 'the fault-injection boot must never start a voter'
    if SERVER is not None and SERVER.poll() is None:
        SERVER.send_signal(signal.SIGCONT)
    if SERVER is None or SERVER.poll() is not None:
        with (DIRECTORY / 'server.log').open('ab') as log:
            SERVER = subprocess.Popen(['/artifacts/screech-server', '--store', str(DIRECTORY / 'store/db'),
                                       '--cluster-config', '/bootshare/config.json'],
                                      stdin=subprocess.DEVNULL, stdout=log, stderr=subprocess.STDOUT)
    return {'started': True, 'boot_id': BOOT_ID}


def instance():
    manifest = DIRECTORY / 'store/db.node-state'
    record = json.loads(bytes(json.loads(manifest.read_text())['body']))
    value = record['instance']
    path = Path(value['path'])
    assert path.is_relative_to(DIRECTORY / 'store')
    return value, path


def inspect():
    assert SERVER is not None and SERVER.poll() is None, 'voter is not running'
    value, path = instance()
    with sqlite3.connect(f'file:{path}?mode=ro', uri=True, timeout=1) as db:
        end = db.execute('SELECT coalesce(max(seq),0) FROM event_index').fetchone()[0]
        floor = db.execute("SELECT value FROM cluster_meta WHERE key='app_sequence'").fetchone()
        view = db.execute("SELECT value FROM cluster_meta WHERE key='view'").fetchone()
        assert view is not None
        return {'member': value['member'], 'app_sequence': max(end, int.from_bytes(floor[0], 'little') if floor else 0),
                'view': int.from_bytes(view[0], 'little'), 'boot_id': BOOT_ID}


def act(request):
    assert Path('/run/screech-voter-vm').exists()
    op = request['op']
    if op == 'status':
        return {'isolated-vm': True, 'node': NODE, 'boot_id': BOOT_ID,
                'maintenance': MAINTENANCE, 'canary': canary_values() if DIRECTORY.exists() else None}
    if op == 'setup':
        initialize()
        start()
        return {'native_ext4': True, 'boot_id': BOOT_ID}
    if op == 'start':
        return start()
    if op == 'inspect':
        return inspect()
    if op == 'arm-power':
        try:
            member = instance()[0]['member']
        except (OSError, ValueError, KeyError, TypeError):
            member = None
        if SERVER is not None and SERVER.poll() is None:
            SERVER.send_signal(signal.SIGSTOP)
            until = time.monotonic() + 5
            while True:
                states = [p.read_text().split('State:', 1)[1].splitlines()[0].strip()[0]
                          for p in Path(f'/proc/{SERVER.pid}/task').glob('*/status')]
                if states and all(state in ('T', 't') for state in states):
                    break
                if time.monotonic() > until:
                    raise TimeoutError('voter threads did not stop before arming power loss')
                time.sleep(.001)
        # These blocks were allocated and fsynced at initialization. Only this
        # overwrite is unsynced; do no sync/shutdown operation before hypervisor kill.
        with (DIRECTORY / 'dirty-canary').open('r+b', buffering=0) as file:
            file.write(request['marker'].encode())
        return {'boot_id': BOOT_ID, 'member': member, 'armed': True}
    if op == 'damage':
        assert MAINTENANCE and SERVER is None
        kind = request['kind']
        mutations = []
        if kind == 'wipe':
            damage(DIRECTORY, kind)
        else:
            _, path = instance()
            # SQLite can reconstruct a damaged base from an intact WAL. Corrupt
            # both authoritative files to require quorum-based voter replacement.
            files = [path]
            wal = Path(str(path) + '-wal')
            if wal.exists() and wal.stat().st_size:
                files.append(wal)
            for target in files:
                before = {'size': target.stat().st_size, 'sha256': hashlib.sha256(target.read_bytes()).hexdigest()}
                with target.open('r+b', buffering=0) as file:
                    if kind == 'bitflip':
                        first = file.read(1)
                        assert first
                        file.seek(0)
                        file.write(bytes([first[0] ^ 1]))
                    elif kind == 'truncate':
                        file.truncate(before['size'] // 2)
                    else:
                        raise ValueError(kind)
                    os.fsync(file.fileno())
                after = {'size': target.stat().st_size, 'sha256': hashlib.sha256(target.read_bytes()).hexdigest()}
                assert before != after, 'disk mutation did not change bytes'
                mutations.append({'path': str(target), 'before': before, 'after': after})
        # This sync persists deliberately injected damage, after the original
        # power cut; the maintenance boot never starts a voter.
        os.sync()
        return {'injected': kind, 'files': mutations, 'voter_started': False}
    if op == 'exec':
        result = subprocess.run(['bash', '-c', request['cmd']], input=request.get('in', ''),
                                text=True, capture_output=True, timeout=90)
        return {'exit': result.returncode, 'out': result.stdout, 'err': result.stderr}
    if op in ('read', 'write'):
        path = Path(request['path'])
        assert path.is_relative_to(ROOT)
        if op == 'read':
            return {'data': base64.b64encode(path.read_bytes()).decode()}
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(base64.b64decode(request['data']))
        return {'written': True}
    raise ValueError(op)


class VoterControl(Control):
    def dispatch(self, request):
        return act(request)


if __name__ == '__main__':
    assert Path('/run/screech-voter-vm').exists()
    parser = argparse.ArgumentParser()
    parser.add_argument('--node', required=True, choices=[f'n{i}' for i in range(1, 6)])
    parser.add_argument('--maintenance', action='store_true')
    parser.add_argument('--fresh-disk', action='store_true')
    args = parser.parse_args()
    NODE, MAINTENANCE, FRESH_DISK = args.node, args.maintenance, args.fresh_disk
    DIRECTORY = ROOT / NODE
    BOOT_ID = Path('/proc/sys/kernel/random/boot_id').read_text().strip()
    if Path('/bootshare/config.json').exists():
        initialize()
        if not MAINTENANCE:
            start()
    gateway = socketserver.ThreadingTCPServer(('0.0.0.0', 5001), Gateway)
    gateway.daemon_threads = True
    gateway.target = f'10.0.1.{NODE[1:]}'
    threading.Thread(target=gateway.serve_forever, daemon=True).start()
    server = ThreadingHTTPServer(('0.0.0.0', 9000), VoterControl)
    server.token = Path('/artifacts/control-token').read_text().strip()
    print('SCREECH_VOTER_GUEST_READY', NODE, BOOT_ID, 'maintenance' if MAINTENANCE else 'voter', flush=True)
    server.serve_forever()
