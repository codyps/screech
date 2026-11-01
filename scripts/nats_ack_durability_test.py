#!/usr/bin/env python3
"""Characterize confirmed-ACK recovery in an unmodified standalone NATS server.

Linux + strace required. A path-filtered rename stall holds ONLY the installation
of D's new consumer state. No server source or persisted state is rewritten.
Default exit status checks the reproduction; --require-durable fails on a gap.
"""
import argparse
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import select
import shutil
import signal
import socket
import subprocess
import tempfile
import time

from wire import Client

PINNED_COMMIT = 'd163ae747ad050ea89054f8fb03c43e64e737aea'


def wait_for(predicate, description, seconds=8):
    end = time.monotonic() + seconds
    while time.monotonic() < end:
        value = predicate()
        if value:
            return value
        time.sleep(.005)
    raise TimeoutError(description)


def state_prefix(path):
    """Read only the documented-in-source v2 header + first five uvarints.

    Used to observe the experiment, never to write or restore a state file.
    This deliberately refuses other versions/encryption instead of guessing.
    """
    try:
        data = path.read_bytes()
    except FileNotFoundError:
        return None
    if len(data) < 2:
        return None
    if data[:2] != bytes([22, 2]):
        raise AssertionError(f'Unsupported consumer state header: {data[:2].hex()}')
    values, pos = [], 2
    for _ in range(5):
        number, shift = 0, 0
        while pos < len(data) and shift < 70:
            byte = data[pos]; pos += 1
            number |= (byte & 127) << shift
            if byte < 128:
                values.append(number)
                break
            shift += 7
        else:
            return None
    return dict(zip(('ack_consumer', 'ack_stream', 'delivered_consumer', 'delivered_stream', 'pending'), values))


class Server:
    def __init__(self, binary, directory, mode, state=None, strace='strace'):
        self.binary = str(binary)
        self.directory = directory
        with socket.socket() as reservation:
            reservation.bind(('127.0.0.1', 0)); port = reservation.getsockname()[1]
        self.address = ('127.0.0.1', port)
        self.config = directory/'nats.conf'
        sync = '' if mode == 'default' else f' sync_interval: "{mode}"\n'
        self.config.write_text(f'host: 127.0.0.1\nport: {port}\njetstream {{\n store_dir: "{directory}/js"\n{sync}}}\n')
        command = [self.binary, '-c', str(self.config)]
        self.trace = directory/'ack-rename.trace'
        if state is not None:
            # Go uses direct syscalls: LD_PRELOAD would not cover this boundary.
            # Path filtering is essential: stream writes and other consumers are
            # not stalled, and no syscall is forced to report false success.
            command = [strace, '-f', '-qq', '-s', '4096', '-o', str(self.trace),
                       '-P', str(state), '-P', str(state)+'.tmp',
                       '-e', 'trace=rename,renameat,renameat2',
                       '-e', 'inject=rename,renameat,renameat2:delay_enter=10s', '--'] + command
        self.log = open(directory/'server.log', 'ab')
        self.process = subprocess.Popen(command, stdout=self.log, stderr=self.log, start_new_session=True)
        self.server_pid = None
        try:
            if state is not None:
                children = Path(f'/proc/{self.process.pid}/task/{self.process.pid}/children')
                self.server_pid = wait_for(lambda: children.read_text().split(), 'strace did not launch server')[0]
                self.server_pid = int(self.server_pid)
            else:
                self.server_pid = self.process.pid
            def ready():
                if self.process.poll() is not None:
                    raise RuntimeError((directory/'server.log').read_text())
                try:
                    c = Client(self.address); c.close(); return True
                except OSError:
                    return False
            wait_for(ready, 'NATS did not start')
        except BaseException:
            self.kill(); raise

    def kill(self):
        if self.server_pid is not None:
            try:
                os.kill(self.server_pid, signal.SIGKILL)
            except ProcessLookupError:
                pass
        try:
            self.process.wait(timeout=5)
        except subprocess.TimeoutExpired:
            os.killpg(self.process.pid, signal.SIGKILL); self.process.wait(timeout=5)
        self.log.close()


def prepare(binary, directory, mode):
    server = Server(binary, directory, mode)
    try:
        c = Client(server.address); c.create()
        c.create_consumer(ack_wait=200000000, max_pending=8)
        assert c.publish(b'unique-business-event') == 1
        message = c.pull()
        assert message['payload'] == b'unique-business-event'
        assert int(message['reply'].split('.')[-4]) == 1, message
        state = wait_for(lambda: next(iter((directory/'js').rglob('obs/D/o.dat')), None), 'consumer state file')
        wait_for(lambda: state_prefix(state) == {'ack_consumer': 0, 'ack_stream': 0, 'delivered_consumer': 1, 'delivered_stream': 1, 'pending': 1}, 'initial delivery not persisted')
        # Establish a known durable starting point, also in deferred-sync modes.
        # These are real flushes before the experiment, not replay/rollback writes.
        for path in (directory/'js').rglob('*'):
            if path.is_file():
                with path.open('rb') as f: os.fsync(f.fileno())
        for path in sorted([p for p in (directory/'js').rglob('*') if p.is_dir()], key=lambda p: len(p.parts), reverse=True):
            fd = os.open(path, os.O_RDONLY | os.O_DIRECTORY)
            try: os.fsync(fd)
            finally: os.close(fd)
        token = message['reply']; c.close()
    finally:
        server.kill()
    return state, token


def confirmed_ack(client, token):
    # Exactly the wire exchange performed by AckSync / DoubleAck. Never confuse
    # receipt of a PONG with confirmation of the consumer's ACK.
    client.counter += 1
    sid = str(client.counter); inbox = client.inbox+'.ack.'+sid
    client.send(f'SUB {inbox} {sid}\r\nUNSUB {sid} 1\r\nPUB {token} {inbox} 4\r\n+ACK\r\n'.encode())
    before = time.monotonic()
    if not select.select([client.socket], [], [], 1)[0]:
        return False, None
    response = client.read_message()
    assert response['subject'] == inbox and response['status'] is None and response['payload'] == b'', response
    return True, (time.monotonic()-before)*1000


def run_case(binary, parent, mode, stall, strace):
    directory = parent/(mode+'-'+('stalled' if stall else 'persisted-control')); directory.mkdir()
    state, token = prepare(binary, directory, mode)
    before = state.read_bytes()
    server = Server(binary, directory, mode, state if stall else None, strace)
    try:
        c = Client(server.address)
        confirmed, ack_ms = confirmed_ack(c, token)
        if not confirmed:
            raise AssertionError('No ACK confirmation within 1s; this build may have closed the gap. Inspect the trace rather than assuming a counterexample.')
        in_memory = c.request('$JS.API.CONSUMER.INFO.T.D', {})
        assert in_memory['num_ack_pending'] == 0 and in_memory['ack_floor']['stream_seq'] == 1, in_memory
        if stall:
            staged = Path(str(state)+'.tmp')
            # Require a complete ACKed candidate image, plus an unchanged installed
            # pending image. This proves the intended state installation is held.
            wait_for(lambda: state_prefix(staged) and state_prefix(staged)['ack_stream'] == 1, 'ACKed temporary state was not staged')
            assert state.read_bytes() == before
            assert state_prefix(state)['pending'] == 1
            time.sleep(.15) # Exceeds the flusher's 100ms cadence; no race-to-kill.
            assert state.read_bytes() == before
        else:
            wait_for(lambda: state_prefix(state)['ack_stream'] == 1 and state_prefix(state)['pending'] == 0, 'control ACK was not installed')
        disk_at_confirmation = state_prefix(state)
        c.close()
    finally:
        server.kill()
    # SIGKILL, not graceful shutdown: shutdown deliberately flushes dirty state.
    server = Server(binary, directory, mode)
    try:
        c = Client(server.address)
        recovered = c.request('$JS.API.CONSUMER.INFO.T.D', {})
        assert c.get(1) == b'unique-business-event' # No publication loss/deletion.
        message = c.pull()
        duplicate = message is not None
        attempt = None
        if duplicate:
            assert message['payload'] == b'unique-business-event'
            fields = message['reply'].split('.')
            assert int(fields[-4]) == 1, message
            attempt = int(fields[-5]); assert attempt >= 2, message
            c.ack(message)
        c.close()
    finally:
        server.kill()
    assert duplicate == stall, (mode, stall, recovered)
    result = {'sync_mode': mode, 'consumer': 'file / durable / explicit ACK / limits retention / replicas=1',
              'stall': 'consumer o.dat atomic rename only' if stall else None,
              'ack_confirmed': confirmed, 'ack_confirmation_ms': ack_ms,
              'memory_after_confirm': {'ack_floor': in_memory['ack_floor'], 'num_ack_pending': in_memory['num_ack_pending']},
              'installed_state_before_kill': disk_at_confirmation,
              'recovered_ack_floor': recovered['ack_floor'], 'recovered_num_ack_pending': recovered['num_ack_pending'],
              'redelivered_after_confirmed_ack': duplicate, 'redelivery_attempt': attempt,
              'publication_preserved': True, 'confirmed_ack_survived_crash': not duplicate,
              'characterization_passed': True}
    (directory/'result.json').write_text(json.dumps(result, indent=2)+'\n')
    return result


def main(args):
    binary = args.binary.resolve()
    if not shutil.which(args.strace): raise RuntimeError('strace is required on Linux')
    if args.work_dir:
        args.work_dir.mkdir(parents=True, exist_ok=False); work = args.work_dir.resolve(); cleanup = None
    else:
        cleanup = tempfile.TemporaryDirectory(prefix='nats-ack-durability-'); work = Path(cleanup.name)
    try:
        cases = [run_case(binary, work, mode, stall, args.strace) for mode in args.mode for stall in [False, True]]
        result = {'recorded_at': datetime.now(timezone.utc).isoformat(),
                  'nats_version': subprocess.check_output([str(binary), '--version'], text=True).strip(),
                  'nats_commit': args.nats_commit, 'binary_sha256': hashlib.sha256(binary.read_bytes()).hexdigest(),
                  'source_modified': False, 'strace': subprocess.check_output([args.strace, '--version'], text=True).splitlines()[0],
                  'scope': 'standalone process SIGKILL while one consumer-state rename is stalled; not a cluster or host power-loss test',
                  'counterexamples': sum(c['redelivered_after_confirmed_ack'] for c in cases), 'cases': cases}
        text = json.dumps(result, indent=2)+'\n'; print(text)
        if args.output: args.output.write_text(text)
        return 1 if args.require_durable and result['counterexamples'] else 0
    finally:
        if cleanup is not None: cleanup.cleanup()


if __name__ == '__main__':
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('binary', type=Path); p.add_argument('--strace', default='strace')
    p.add_argument('--mode', choices=['always', '10ms', 'default'], action='append')
    p.add_argument('--nats-commit', default=PINNED_COMMIT, help='Source revision used to build the supplied binary')
    p.add_argument('--work-dir', type=Path, help='Retain configs, logs, syscall traces and recovered databases')
    p.add_argument('--output', type=Path); p.add_argument('--require-durable', action='store_true')
    args = p.parse_args(); args.mode = args.mode or ['always', '10ms', 'default']
    raise SystemExit(main(args))
