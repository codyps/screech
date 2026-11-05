#!/usr/bin/env python3
"""Kill a leader after spilling its accepted window, before publishing the seal."""
import argparse
import json
import tempfile
import time
from pathlib import Path

from cluster_tests import Cluster
from cluster_recovery_tests import verify, wait_healed, wait_marker
from wire import Client


def audit_voter(cluster, slot, expected, attempts=30):
    last = None
    for _ in range(attempts):
        try:
            client = cluster.client(slot)
            try:
                for sequence, body in expected.items():
                    assert client.get(sequence) == body, (slot, sequence)
                info = client.request('$JS.API.CONSUMER.INFO.T.D', {})
                assert info['ack_floor']['consumer_seq'] >= 1, info
                assert info['num_ack_pending'] == 0, info
                duplicate = client.publish_with_id(b'ignored', 'stable')
                assert duplicate['duplicate'] and duplicate['seq'] == 1, duplicate
            finally:
                client.close()
            return
        except (OSError, EOFError, TimeoutError) as error:
            # Admission can precede activation of the new leader's gateway.
            # Retry transport failures only; every data assertion stays binding.
            last = error
            time.sleep(.1)
    raise AssertionError(f'voter {slot} did not serve its recovered state') from last


def run_case(binary, count, damage):
    with tempfile.TemporaryDirectory(prefix='screech-journal-spill-') as temporary:
        cluster = Cluster(binary, Path(temporary), count=count)
        try:
            for slot in range(1, count + 1):
                node = cluster.root / str(slot)
                cluster.start(slot, extra_env={
                    'SCREECH_FAULT_POINT': 'after_cluster_journal_spill',
                    'SCREECH_FAULT_ACTION': 'hang',
                    'SCREECH_FAULT_ARM': str(node / 'arm'),
                    'SCREECH_FAULT_MARKER': str(node / 'spill'),
                })
            leader = cluster.wait_leader()
            client = cluster.client(leader)
            expected = {1: b'confirmed-before-spill'}
            try:
                result = client.request('$JS.API.STREAM.CREATE.T', dict(
                    name='T', subjects=['test.>'], storage='file',
                    retention='limits', num_replicas=count,
                    duplicate_window=120_000_000_000))
                assert 'error' not in result, result
                client.create_consumer(ack_wait=3_600_000_000_000)
                assert client.publish_with_id(expected[1], 'stable')['seq'] == 1
                message = client.pull()
                assert message['payload'] == expected[1]
                client.ack(message)
            finally:
                client.close()
            cluster.await_equal()
            node = cluster.root / str(leader)
            marker = node / 'spill'
            (node / 'arm').touch()
            client = Client(cluster.address(leader), timeout=2)
            try:
                for index in range(64):
                    body = f'publication-{index}:'.encode() + bytes([index]) * 1024
                    try:
                        sequence = client.publish(body)
                    except (OSError, EOFError, AssertionError):
                        break  # Uncertain outcomes are never added to confirmed history.
                    assert sequence not in expected, ('reused confirmed sequence', sequence)
                    expected[sequence] = body
                    if marker.exists():
                        break
            finally:
                client.close()
            wait_marker(cluster, leader, marker)
            previous = cluster.member(leader)
            cluster.kill(leader)
            cluster.wait_leader(exclude=(leader,))
            verify(cluster, expected, acked=False)
            if damage:
                seal = cluster.store_path(leader).with_suffix('.cluster-watermark')
                data = bytearray(seal.read_bytes())
                data[128] ^= 1
                seal.write_bytes(data)
            cluster.start(leader)
            if damage:
                wait_healed(cluster, leader, previous)
            else:
                cluster.await_equal()
            for slot in cluster.processes:
                audit_voter(cluster, slot, expected)
            # Exercise a complete cold restart after the spill and any replacement.
            for slot in list(cluster.processes):
                cluster.kill(slot)
            for slot in range(1, count + 1):
                cluster.start(slot)
            cluster.wait_leader()
            cluster.await_equal()
            verify(cluster, expected, acked=False)
            return dict(test='unpublished spill preserves confirmed publications, ACK and dedup',
                        voters=count, damaged_returning_seal=damage,
                        confirmed_publications=len(expected), passed=True)
        finally:
            cluster.close()


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('binary', type=lambda value: str(Path(value).resolve()))
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    results = [run_case(args.binary, count, damage)
               for count in (3, 5) for damage in (False, True)]
    text = json.dumps(results, indent=2) + '\n'
    args.output.write_text(text)
    print(text)
