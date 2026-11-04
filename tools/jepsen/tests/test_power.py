"""Fault-driver controls: real sockets and dispatch ordering, no simulated storage."""
import copy
import json
from pathlib import Path
import socket
import struct
import subprocess
import tempfile
import threading
import unittest
from unittest.mock import patch

from cluster_vm import Cluster, check_power_canary
from ethernet import Hub, receive_exact
import voter_guest


class Controls(unittest.TestCase):
    def test_reboot_control_rejects_reused_kernel_and_wrong_disk_evidence(self):
        arm = {'boot_id': 'old'}
        good = {'boot_id': 'new', 'canary': {'clean': 'durable-canary-v1', 'dirty': 'stable-before-unsynced-overwrite'}}
        self.assertTrue(check_power_canary(arm, good)['unsynced_lost'])
        for path, value in [(('boot_id',), 'old'), (('boot_id',), None),
                            (('canary', 'clean'), 'lost'), (('canary', 'dirty'), 'still-unsynced'), (('canary',), None)]:
            broken = copy.deepcopy(good)
            target = broken
            for key in path[:-1]:
                target = target[key]
            target[path[-1]] = value
            with self.assertRaises((AssertionError, TypeError)):
                check_power_canary(arm, broken)

    def test_recovery_never_recreates_missing_control_files(self):
        with tempfile.TemporaryDirectory() as temp:
            directory = Path(temp) / 'n1'
            with patch.multiple(voter_guest, DIRECTORY=directory, FRESH_DISK=False):
                with self.assertRaises(AssertionError):
                    voter_guest.initialize()
            self.assertFalse((directory / 'synced-canary').exists())
            self.assertFalse((directory / 'dirty-canary').exists())

    def test_group_sends_every_kill_before_waiting_for_any_exit(self):
        calls = []
        lock = threading.Lock()
        def record(kind, node):
            with lock:
                calls.append((kind, node))
        class Process:
            def __init__(self, node): self.node = node
            def poll(self): return None
            def kill(self): record('kill', self.node)
            def wait(self, timeout): record('wait', self.node)
        class Monitor:
            def __init__(self): self.paused = False
            def command(self, op):
                if op == 'stop': self.paused = True
                return {'status': 'paused' if self.paused else 'running'}
            def close(self): pass
        class Voter:
            def __init__(self, node):
                self.node, self.lock = node, threading.RLock()
                self.process, self.qmp = Process(node), Monitor()
            def call(self, op, **payload):
                record('arm', self.node)
                return {'armed': True, 'boot_id': self.node + '-old', 'member': int(self.node[1:])}
            def close_block(self): pass
        with tempfile.TemporaryDirectory() as temp:
            cluster = Cluster.__new__(Cluster)
            cluster.output, cluster.events, cluster.damages = Path(temp), [], []
            cluster.io_faults = []
            cluster.vms = {f'n{i}': Voter(f'n{i}') for i in range(1, 6)}
            result = cluster.power_cut(cluster.vms)
            self.assertTrue(result['guest_killed'])
            self.assertEqual(5, sum(kind == 'kill' for kind, _ in calls))
            first_wait = next(i for i, event in enumerate(calls) if event[0] == 'wait')
            self.assertTrue(all(kind != 'kill' for kind, _ in calls[first_wait:]))
            self.assertTrue(result['event']['all_paused_before_kill'])
            self.assertEqual(set(cluster.vms), set(result['event']['old_boot_ids']))
            saved = json.loads((Path(temp) / 'power-events.json').read_text())
            self.assertEqual('SIGKILL', saved['cuts'][0]['signal'])
            with self.assertRaises(AssertionError):
                cluster.power_cut(['n1', 'n1'])

    def test_ethernet_frames_survive_fragmented_tcp_and_disconnect(self):
        hub = Hub()
        peers = [socket.create_connection(('127.0.0.1', hub.port)) for _ in range(3)]
        for peer in peers: peer.settimeout(3)
        try:
            # Register every connection before transmitting; no guest traffic yet.
            import time
            deadline = time.monotonic() + 3
            while len(hub.peers) != 3:
                self.assertLess(time.monotonic(), deadline)
                time.sleep(.001)
            frame = b'ethernet-payload'
            packet = struct.pack('!I', len(frame)) + frame
            for part in [packet[:1], packet[1:3], packet[3:8], packet[8:]]:
                peers[0].sendall(part)
            for peer in peers[1:]:
                self.assertEqual(packet, receive_exact(peer, len(packet)))
            peers[2].close()
            peers[0].sendall(packet)
            self.assertEqual(packet, receive_exact(peers[1], len(packet)))
        finally:
            for peer in peers: peer.close()
            hub.close()

    def test_damage_includes_wal_and_never_starts_a_voter(self):
        with tempfile.TemporaryDirectory() as temp:
            directory = Path(temp) / 'n1'
            store = directory / 'store'
            store.mkdir(parents=True)
            base = store / 'db'
            wal = store / 'db-wal'
            record = {'instance': {'path': str(base), 'member': 1}}
            (store / 'db.node-state').write_text(json.dumps({'body': list(json.dumps(record).encode())}))
            for kind in ['bitflip', 'truncate']:
                base.write_bytes(b'SQLite format 3' + b'x' * 4096)
                wal.write_bytes(b'valid-wal-header' + b'y' * 8192)
                with patch.multiple(voter_guest, DIRECTORY=directory, MAINTENANCE=True, SERVER=None), \
                     patch('voter_guest.os.sync'), patch('voter_guest.Path.exists', return_value=True):
                    result = voter_guest.act({'op': 'damage', 'kind': kind})
                self.assertFalse(result['voter_started'])
                self.assertEqual({str(base), str(wal)}, {f['path'] for f in result['files']})
                self.assertTrue(all(f['before'] != f['after'] for f in result['files']))

    def test_active_arm_never_stops_threads_reads_the_database_or_syncs(self):
        class Process:
            pid = 'self'  # /proc may expose a parent PID namespace in the sandbox.
            def poll(self): return None
            def send_signal(self, _): raise AssertionError('active arm paused the voter')
        with tempfile.TemporaryDirectory() as temp:
            directory = Path(temp)
            (directory / 'dirty-canary').write_bytes(voter_guest.STABLE)
            with patch.multiple(voter_guest, DIRECTORY=directory, SERVER=Process(), BOOT_ID='kernel'), \
                 patch('voter_guest.Path.exists', return_value=True), \
                 patch('voter_guest.instance', side_effect=AssertionError('read DB on stalled disk')), \
                 patch('voter_guest.os.sync', side_effect=AssertionError('sync before power cut')), \
                 patch('voter_guest.os.fsync', side_effect=AssertionError('fsync before power cut')):
                proof = voter_guest.act({'op': 'arm-power', 'marker': 'x' * len(voter_guest.STABLE),
                                         'pause': False, 'member': 7})
            self.assertTrue(proof['server_not_stopped'])
            self.assertEqual(7, proof['member'])
            self.assertEqual(b'x' * len(voter_guest.STABLE), (directory / 'dirty-canary').read_bytes())

    def test_request_completion_cannot_be_reopened_by_a_late_begin_notification(self):
        cluster = Cluster.__new__(Cluster)
        cluster.requests, cluster.closed_requests = {}, set()
        cluster.request_lock = threading.Lock()
        cluster.dispatch({'op': 'request-window', 'phase': 'end', 'id': 'done'})
        cluster.dispatch({'op': 'request-window', 'phase': 'begin', 'id': 'done',
                          'kind': 'ack', 'subject': 'ack-token', 'node': 'n1'})
        self.assertEqual([], cluster.pending_requests('ack'))
        cluster.dispatch({'op': 'request-window', 'phase': 'begin', 'id': 'new',
                          'kind': 'ack', 'subject': 'ack-token', 'node': 'n1'})
        self.assertEqual(['new'], [r['id'] for r in cluster.pending_requests('ack')])
        cluster.dispatch({'op': 'request-window', 'phase': 'end', 'id': 'new'})
        self.assertEqual([], cluster.pending_requests('ack'))

    def test_failed_fault_disarms_every_device_recovers_hit_voters_and_remains_a_failure(self):
        calls = []
        class Device:
            def __init__(self, hit): self.hit = hit
            def disarm(self):
                calls.append(('disarm', self.hit))
                return self.hit
        class Voter:
            def __init__(self, hit): self.block = Device(hit)
            def stop(self): calls.append(('stop', self.block.hit))
        cluster = Cluster.__new__(Cluster)
        cluster.vms = {'n1': Voter(True), 'n2': Voter(False)}
        def fail(*_): raise TimeoutError('no match')
        cluster.active_fault = fail
        cluster.heal = lambda: calls.append(('heal', True))
        with self.assertRaisesRegex(TimeoutError, 'no match'):
            cluster.dispatch({'op': 'active-fault', 'mode': 'hold', 'operation': 'write', 'kind': 'ack'})
        self.assertEqual({('disarm', True), ('disarm', False), ('stop', True), ('heal', True)}, set(calls))

    def test_active_cut_stops_and_kills_real_qemu_host_process_without_qmp_io_drain(self):
        class QMP:
            def command(self, _): raise AssertionError('QMP must not drain active disk I/O')
            def close(self): pass
        class Voter:
            lock = threading.RLock()
            qmp = QMP()
            def call(self, op, **payload):
                self.assertion = payload
                return {'armed': True, 'boot_id': 'old', 'member': 1, 'server_not_stopped': True}
            def close_block(self): pass
        with tempfile.TemporaryDirectory() as temp:
            voter = Voter()
            voter.process = subprocess.Popen(['sleep', '30'])
            cluster = Cluster.__new__(Cluster)
            cluster.output, cluster.events, cluster.damages, cluster.io_faults = Path(temp), [], [], []
            cluster.vms = {'n1': voter}
            try:
                result = cluster.power_cut(['n1'], active=True, members={'n1': 1})
                self.assertEqual(-9, voter.process.returncode)
                self.assertFalse(voter.assertion['pause'])
                self.assertEqual('host-SIGSTOP-without-I/O-drain', result['event']['pause_mechanism'])
                self.assertFalse(result['event']['voter_threads_paused'])
            finally:
                if voter.process.poll() is None:
                    voter.process.kill()
                    voter.process.wait()


if __name__ == '__main__':
    unittest.main()
