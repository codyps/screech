#!/usr/bin/env python3
"""One native-ext4 voter per disposable VM; no LazyFS and no host disk access."""
import argparse
import base64
import fcntl
from http.server import ThreadingHTTPServer
import hashlib
import json
import os
from pathlib import Path
import signal
import socket
import socketserver
import sqlite3
import subprocess
import struct
import threading
import time
from control import damage
from guest import Control, Gateway
from wire import Client

ROOT = Path('/data/screech-jepsen')
NODE = None
SERVER = None
MAINTENANCE = False
DIRECTORY = None
BOOT_ID = None
FRESH_DISK = False
NET_RECEIVED = set()
NET_LOCK = threading.Lock()
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


def file_extents():
    """Observe allocated journal/DB/WAL sectors without changing sync state."""
    _, base = instance()
    result = []
    for path in [base.with_suffix('.cluster-watermark'), base, Path(str(base) + '-wal')]:
        if not path.exists():
            continue
        buffer = bytearray(struct.pack('=QQIIII', 0, 2**64-1, 0, 0, 256, 0) + bytes(56 * 256))
        with path.open('rb') as file:
            fcntl.ioctl(file.fileno(), 0xc020660b, buffer, True)
        mapped = struct.unpack_from('=I', buffer, 20)[0]
        for index in range(mapped):
            logical, physical, length, _, _, flags, _, _, _ = struct.unpack_from('=QQQQQIIII', buffer, 32 + index * 56)
            # Unknown/delayed allocation/unwritten extents cannot prove which
            # already allocated data sectors a live block request changes.
            if physical and not flags & (2 | 4 | 0x800):
                result.append({'path': str(path), 'logical': logical, 'physical': physical, 'length': length})
    assert result, 'no allocated authoritative file extents'
    return result


def network_receiver():
    connection = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    connection.bind(('0.0.0.0', 9900))
    while True:
        data, address = connection.recvfrom(2048)
        value = data.decode()
        with NET_LOCK:
            NET_RECEIVED.add((value, address[0]))


def cas_probe(request):
    """Bypass the public forwarding gateway: exercise this voter's own service."""
    client = None
    try:
        client = Client((f'10.0.1.{NODE[1:]}', 7500), timeout=3)
        stream = request['stream']
        if request['action'] == 'create':
            name = request['name']
            result = client.request('$JS.API.STREAM.CREATE.' + name,
                                    {'name': name, 'subjects': [name + '.data'], 'num_replicas': request['replicas'],
                                     'storage': 'file', 'retention': 'limits'})
            if result.get('error'):
                return {'outcome': 'rejected', 'response_error': result['error']}
            assert result['config']['name'] == name
            return {'outcome': 'ok', 'created': name}
        if request['action'] == 'write':
            payload = request['body'].encode()
            headers = (f'NATS/1.0\r\nNats-Msg-Id: {request["id"]}\r\n'
                       f'Nats-Expected-Last-Sequence: {request["expected"]}\r\n\r\n').encode()
            inbox = client.inbox + '.cas'
            client.send(f'SUB {inbox} 1\r\nUNSUB 1 1\r\nHPUB {stream}.data {inbox} {len(headers)} {len(headers)+len(payload)}\r\n'.encode()
                        + headers + payload + b'\r\n')
            message = client.read_message()
            if message['status'] is not None:
                return {'outcome': 'unavailable', 'status': message['status']}
            result = json.loads(message['payload'])
            if result.get('error'):
                return {'outcome': 'rejected', 'response_error': result['error']}
            assert isinstance(result.get('seq'), int) and not result.get('duplicate'), 'invalid conditional publication ACK'
            return {'outcome': 'ok', 'seq': result['seq']}
        result = client.request(f'$JS.API.STREAM.INFO.{stream}', {})
        if result.get('error'):
            return {'outcome': 'unavailable', 'response_error': result['error']}
        return {'outcome': 'ok', 'last_seq': result['state']['last_seq']}
    except (OSError, EOFError) as error:
        return {'outcome': 'unknown', 'reason': str(error)}
    finally:
        if client is not None:
            client.close()


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
    if op == 'cas':
        return dict(cas_probe(request), service_node=NODE)
    if op == 'net-send':
        with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as connection:
            for target in request['targets']:
                connection.sendto(f'{request["tag"]}:{NODE}:{target}'.encode(), (f'10.0.1.{target[1:]}', 9900))
        return {'sent': True}
    if op == 'net-received':
        with NET_LOCK:
            return {'received': [{'tag': value, 'source_ip': ip} for value, ip in sorted(NET_RECEIVED)
                                 if value.startswith(request['tag'] + ':')], 'boot_id': BOOT_ID}
    if op == 'extents':
        return {'extents': file_extents()}
    if op == 'io-errors':
        output = subprocess.check_output(['dmesg'], text=True)
        return {'errors': [line for line in output.splitlines()
                           if 'I/O error' in line or 'I/O Error' in line]}
    if op == 'leader':
        try:
            client = Client((f'10.0.1.{NODE[1:]}', 7500), timeout=2)
            try:
                # Only the local leader owns this listener. A linearizable API
                # read here would itself queue behind the workload's storage.
                client.flush()
                return {'leader': True}
            finally:
                client.close()
        except (OSError, EOFError, AssertionError):
            return {'leader': False}
    if op == 'arm-power':
        pause = request.get('pause', True)
        member = request.get('member')
        if pause:
            try:
                member = instance()[0]['member']
            except (OSError, ValueError, KeyError, TypeError):
                member = None
        if pause and SERVER is not None and SERVER.poll() is None:
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
        states = [p.read_text().split('State:', 1)[1].splitlines()[0].strip()[0]
                  for p in Path(f'/proc/{SERVER.pid}/task').glob('*/status')] if SERVER is not None and SERVER.poll() is None else []
        return {'boot_id': BOOT_ID, 'member': member, 'armed': True,
                'server_not_stopped': bool(states) and all(s not in ('T', 't') for s in states)}
    if op == 'damage':
        assert MAINTENANCE and SERVER is None
        kind = request['kind']
        mutations = []
        removed = []
        if kind == 'wipe':
            removed = [str(path) for path in (DIRECTORY / 'store').iterdir()]
            assert removed, 'wipe found no voter state'
            damage(DIRECTORY, kind)
            assert not list((DIRECTORY / 'store').iterdir())
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
        return {'injected': kind, 'files': mutations, 'voter_started': False,
                'removed_paths': removed, 'store_empty': kind == 'wipe'}
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
    threading.Thread(target=network_receiver, daemon=True).start()
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
