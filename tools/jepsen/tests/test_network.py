"""Directed Ethernet controls, including reverse traffic and a rebooted NIC."""
import socket
import struct
import time
import unittest
from http.client import IncompleteRead
from types import SimpleNamespace
from unittest.mock import patch
import voter_guest

from ethernet import Hub, receive_exact
from mixed_faults import blocked_edges, Witness


class Network(unittest.TestCase):
    def test_truncated_guest_response_does_not_hide_conflicting_confirmations(self):
        def voter(node):
            def call(op, **payload):
                if payload['action'] == 'write' and node == 'n3':
                    raise IncompleteRead(b'', 88)
                return dict(service_node=node, outcome='ok', **(
                    {'seq': 2} if payload['action'] == 'write' else {'last_seq': 2}))
            return SimpleNamespace(process=SimpleNamespace(poll=lambda: None), call=call, client_port=1)
        cluster = SimpleNamespace(vms={n: voter(n) for n in ('n1', 'n2', 'n3')})
        case = dict(stream='T', seed='initial', survivors=['n1', 'n2'], targets=['n3'], rounds=[])
        witness = Witness(cluster, case)
        witness.phase = 'healed'
        with patch('mixed_faults.Client') as client, patch.object(witness, 'save'):
            client.return_value.request.return_value = {'state': {'last_seq': 1}}
            with self.assertRaisesRegex(AssertionError, 'split brain'):
                witness.round()
        self.assertEqual(2, sum(w['outcome'] == 'ok' for w in case['rounds'][0]['writes']))
        self.assertEqual('unknown', case['rounds'][0]['writes'][2]['outcome'])

    def test_expected_service_unavailability_does_not_masquerade_as_control_failure(self):
        with patch.multiple(voter_guest, NODE='n1'), patch('voter_guest.Path.exists', return_value=True), \
             patch('voter_guest.Client', side_effect=ConnectionRefusedError('no local leader')):
            result = voter_guest.act({'op': 'cas', 'stream': 'T', 'action': 'read'})
        self.assertEqual('unknown', result['outcome'])
        self.assertEqual('n1', result['service_node'])
        self.assertNotIn('error', result)

    def test_topologies_preserve_an_unfaulted_connected_majority(self):
        for nodes in [['n1', 'n2', 'n3'], ['n1', 'n2', 'n3', 'n4', 'n5']]:
            targets = nodes[:len(nodes)//2]
            for kind in ['split', 'send', 'receive']:
                edges = set(blocked_edges(nodes, targets, kind))
                self.assertTrue(edges)
                self.assertTrue(all(not (a not in targets and b not in targets) for a, b in edges))
                for a in targets:
                    for b in nodes[len(targets):]:
                        self.assertEqual(kind in ('split', 'send'), (a, b) in edges)
                        self.assertEqual(kind in ('split', 'receive'), (b, a) in edges)

    def test_one_way_drops_survive_reconnecting_the_same_voter_mac(self):
        hub = Hub()
        peers = []
        def connect():
            s = socket.create_connection(('127.0.0.1', hub.port));s.settimeout(2)
            peers.append(s)
            return s
        def frame(source):
            data = bytes.fromhex('0200000001020200000001') + bytes([source]) + b'\x08\x00payload'
            return struct.pack('!I', len(data)) + data
        def until(predicate):
            deadline = time.monotonic() + 3
            while not predicate():
                self.assertLess(time.monotonic(), deadline)
                time.sleep(.001)
        try:
            a, b, c = connect(), connect(), connect()
            until(lambda: len(hub.peers) == 3)
            for i, source in enumerate([a, b, c], 1):
                source.sendall(frame(i))
                for destination in [p for p in [a, b, c] if p is not source]:
                    self.assertEqual(frame(i), receive_exact(destination, len(frame(i))))
            epoch = hub.partition([('n1', 'n2')])
            a.sendall(frame(1))
            self.assertEqual(frame(1), receive_exact(c, len(frame(1))))
            b.settimeout(.05)
            with self.assertRaises(socket.timeout): b.recv(1)
            b.sendall(frame(2))
            self.assertEqual(frame(2), receive_exact(a, len(frame(2))))
            self.assertEqual(frame(2), receive_exact(c, len(frame(2))))
            b.close()
            until(lambda: len(hub.peers) == 2)
            b = connect()
            until(lambda: len(hub.peers) == 3)
            b.sendall(frame(2))
            for destination in [a, c]: receive_exact(destination, len(frame(2)))
            a.sendall(frame(1))
            receive_exact(c, len(frame(1)))
            b.settimeout(.05)
            with self.assertRaises(socket.timeout): b.recv(1)
            state = hub.snapshot()
            self.assertEqual(epoch, state['epoch'])
            counts = {(e['source'], e['destination']): e for e in state['edges']}
            self.assertEqual(2, counts[('n1', 'n2')]['dropped'])
            self.assertGreater(counts[('n2', 'n1')]['forwarded'], 0)
            hub.partition([])
            a.sendall(frame(1))
            self.assertEqual(frame(1), receive_exact(b, len(frame(1))))
            receive_exact(c, len(frame(1)))
        finally:
            for peer in peers: peer.close()
            hub.close()
