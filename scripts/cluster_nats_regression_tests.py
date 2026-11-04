#!/usr/bin/env python3
"""Failure histories derived from upstream NATS replication/recovery regressions."""
import argparse
import hashlib
import socket
import select
import json
import zlib
import sqlite3
import tempfile
import time
from pathlib import Path
from cluster_tests import Cluster
from cluster_recovery_tests import compact, verify, wait_healed, damage_store


def wait_recovery(cluster, slot, previous):
    until = time.monotonic() + 15
    while time.monotonic() < until:
        if cluster.member(slot) != previous:
            return wait_healed(cluster, slot, previous)
        time.sleep(.05)
    raise AssertionError('corrupt application leader kept its voting identity instead of healing')


def missing_live_content(binary, kind):
    with tempfile.TemporaryDirectory(prefix='screech-nats-content-') as tmp:
        cluster = Cluster(binary, Path(tmp))
        try:
            for slot in range(1, 4):
                cluster.start(slot)
            leader = cluster.wait_leader()
            client = cluster.client(leader)
            client.create()
            expected = {client.publish(b'first'): b'first', client.publish(b'second'): b'second'}
            if kind == 'snapshot':
                compact(cluster)
            cluster.await_equal()
            previous = cluster.member(leader)
            # Lookup indexes are connection-local. Damage the accepted payload
            # they reference, or the FULL snapshot payload, through another connection.
            table = 'snapshot_bodies' if kind == 'snapshot' else 'entries'
            with sqlite3.connect(cluster.store_path(leader)) as db:
                assert db.execute(f'SELECT count(*) FROM {table}').fetchone()[0] > 0
                if kind == 'snapshot':
                    db.execute('DELETE FROM snapshot_bodies')
                else:
                    # Preserve the log frontier so this specifically exercises
                    # the application read of missing referenced content.
                    deleted = db.execute('DELETE FROM entries WHERE instr(data,?)>0', (b'first',))
                    assert deleted.rowcount == 1, deleted.rowcount
            # Missing referenced content is corruption, not a successful empty read.
            try:
                result = client.request('$JS.API.STREAM.MSG.GET.T', {'seq': 1})
                assert 'error' in result, result
            except (OSError, EOFError):
                pass
            finally:
                client.close()
            wait_recovery(cluster, leader, previous)
            verify(cluster, expected, acked=False)
            client = cluster.client(cluster.wait_leader())
            assert client.publish(b'after-repair') == 3
            client.close()
        finally:
            cluster.close()
    return {'test': f'missing live {kind} content fences the leader and heals automatically', 'passed': True}


def restart_all(cluster):
    slots = list(cluster.processes)
    for slot in slots:
        cluster.kill(slot)
    for slot in slots:
        cluster.start(slot)
    return cluster.wait_leader()


def create_after_empty_snapshot(binary):
    # NATS #5946: an older empty checkpoint must not erase later catalog entries.
    with tempfile.TemporaryDirectory(prefix='screech-nats-catalog-') as tmp:
        cluster = Cluster(binary, Path(tmp))
        try:
            for slot in range(1, 4):
                cluster.start(slot)
            compact(cluster)
            client = cluster.client(cluster.wait_leader())
            client.create()
            client.create_consumer(ack_wait=60_000_000_000)
            assert client.publish_with_id(b'committed-before-kill', 'catalog')['seq'] == 1
            message = client.pull()
            client.ack(message)
            client.close()
            restart_all(cluster)
            for slot in range(1, 4):
                client = cluster.client(slot)
                assert client.get(1) == b'committed-before-kill'
                info = client.request('$JS.API.CONSUMER.INFO.T.D', {})
                assert info['ack_floor']['consumer_seq'] == 1, info
                duplicate = client.publish_with_id(b'ignored', 'catalog')
                assert duplicate['duplicate'] and duplicate['seq'] == 1, duplicate
                client.close()
            cluster.await_equal()
        finally:
            cluster.close()
    return {'test': 'old empty snapshot plus committed stream/consumer tail survives immediate full SIGKILL', 'passed': True}


def stream_update_preserves_consumer(binary):
    # NATS #7939 and #8793: stream updates and consumer reincarnation share one log.
    with tempfile.TemporaryDirectory(prefix='screech-nats-consumer-') as tmp:
        cluster = Cluster(binary, Path(tmp))
        try:
            for slot in range(1, 4):
                cluster.start(slot)
            client = cluster.client(cluster.wait_leader())
            client.create()
            client.create_consumer(ack_wait=60_000_000_000)
            assert client.publish(b'first') == 1
            assert client.publish(b'second') == 2
            old = client.pull()
            client.ack(old)
            pending = client.pull()
            config = client.request('$JS.API.STREAM.INFO.T', {})['config']
            config['max_msgs'] = 100
            result = client.request('$JS.API.STREAM.UPDATE.T', config)
            assert 'error' not in result, result
            client.close()
            compact(cluster)
            restart_all(cluster)
            client = cluster.client(cluster.wait_leader())
            info = client.request('$JS.API.CONSUMER.INFO.T.D', {})
            assert info['ack_floor']['consumer_seq'] == 1 and info['num_ack_pending'] == 1, info
            client.ack(pending)
            assert client.request('$JS.API.CONSUMER.DELETE.T.D', {})['success']
            fresh = client.create_consumer(ack_wait=60_000_000_000)
            assert fresh['ack_floor']['consumer_seq'] == 0 and fresh['delivered']['consumer_seq'] == 0, fresh
            current = client.pull()
            assert current['payload'] == b'first'
            # A stale ACK for the deleted incarnation must not alter the new one.
            client.ack(old, confirm=False)
            client.flush()
            info = client.request('$JS.API.CONSUMER.INFO.T.D', {})
            assert info['ack_floor']['consumer_seq'] == 0 and info['num_ack_pending'] == 1, info
            client.close()
            compact(cluster)
            leader = cluster.wait_leader()
            cluster.kill(leader)
            client = cluster.client(cluster.wait_leader())
            info = client.request('$JS.API.CONSUMER.INFO.T.D', {})
            assert info['ack_floor']['consumer_seq'] == 0 and info['num_ack_pending'] == 1, info
            client.ack(current)
            assert client.request('$JS.API.CONSUMER.INFO.T.D', {})['ack_floor']['consumer_seq'] == 1
            client.close()
        finally:
            cluster.close()
    return {'test': 'stream update preserves consumers; recreated consumer fences stale ACKs across checkpoints/election', 'passed': True}


def minority_corruption(binary):
    # NATS #7549/#7556: two damaged voters must not erase a five-voter catalog.
    with tempfile.TemporaryDirectory(prefix='screech-nats-minority-') as tmp:
        cluster = Cluster(binary, Path(tmp), count=5)
        try:
            for slot in range(1, 6):
                cluster.start(slot)
            leader = cluster.wait_leader()
            client = cluster.client(leader)
            client.create()
            client.create_consumer(ack_wait=60_000_000_000)
            expected = {client.publish(b'first'): b'first', client.publish(b'second'): b'second'}
            first = client.pull()
            client.ack(first)
            client.close()
            compact(cluster)
            cluster.await_equal()
            victims = [leader, next(slot for slot in cluster.processes if slot != leader)]
            previous = {slot: cluster.member(slot) for slot in victims}
            paths = {slot: cluster.store_path(slot) for slot in victims}
            for slot in victims:
                cluster.kill(slot)
            for slot in victims:
                with sqlite3.connect(paths[slot]) as db:
                    row = db.execute("SELECT value FROM cluster_meta WHERE key='raft_snapshot'").fetchone()
                    if row:
                        damaged = bytearray(row[0])
                        damaged[len(damaged) // 2] ^= 1
                        db.execute("UPDATE cluster_meta SET value=? WHERE key='raft_snapshot'", [bytes(damaged)])
                    else:
                        index, data = db.execute('SELECT idx,data FROM entries WHERE length(data)>50 ORDER BY idx LIMIT 1').fetchone()
                        damaged = bytearray(data)
                        damaged[len(damaged) // 2] ^= 1
                        db.execute('UPDATE entries SET data=? WHERE idx=?', [bytes(damaged), index])
                        # The journal can repair a damaged query projection.
                        # Damage its published prefix too so this voter has
                        # actually lost authority and must use quorum repair.
                        journal = paths[slot].with_suffix('.cluster-watermark')
                        accepted = bytearray(journal.read_bytes())
                        assert len(accepted) > 8192 + 10
                        accepted[8192 + 10] ^= 1
                        journal.write_bytes(accepted)
            for slot in victims:
                cluster.start(slot)
            for slot in victims:
                wait_recovery(cluster, slot, previous[slot])
            for slot in victims:
                assert paths[slot].exists(), 'repair deleted evidence instead of fencing it'
            verify(cluster, expected, acked=False)
            client = cluster.client(cluster.wait_leader())
            info = client.request('$JS.API.CONSUMER.INFO.T.D', {})
            assert info['ack_floor']['consumer_seq'] == 1, info
            assert client.publish(b'after-two-corrupt-voters') == 3
            client.close()
        finally:
            cluster.close()
    return {'test': 'two corrupt voters including old leader heal without deleting catalog, payloads or ACK floors', 'passed': True}


def isolated_replayed_membership(binary):
    # NATS #7850: replaying membership may never turn a cluster into one voter.
    with tempfile.TemporaryDirectory(prefix='screech-nats-quorum-') as tmp:
        cluster = Cluster(binary, Path(tmp))
        try:
            for slot in range(1, 4):
                cluster.start(slot)
            leader = cluster.wait_leader()
            client = cluster.client(leader)
            client.create()
            assert client.publish(b'quorum-history') == 1
            client.close()
            victim = next(slot for slot in cluster.processes if slot != leader)
            previous = cluster.member(victim)
            cluster.kill(victim)
            damage_store(cluster, victim, 'missing')
            cluster.start(victim)
            wait_recovery(cluster, victim, previous)
            compact(cluster)
            for slot in list(cluster.processes):
                cluster.kill(slot)
            cluster.start(victim)
            time.sleep(2)
            # A replayed snapshot/configuration alone cannot authorize service.
            from wire import Client
            try:
                client = Client(cluster.address(victim, 'service'), timeout=.3)
                client.close()
                raise AssertionError('replayed membership elected an isolated single voter')
            except (OSError, EOFError):
                pass
            assert cluster.processes[victim].poll() is None
            for slot in range(1, 4):
                if slot != victim:
                    cluster.start(slot)
            verify(cluster, {1: b'quorum-history'}, acked=False)
            cluster.await_equal()
        finally:
            cluster.close()
    return {'test': 'replayed replacement membership cannot elect an isolated single voter', 'passed': True}


def malformed_application_recovery(binary):
    # NATS #7944: structural recovery failure must not leave a leader heartbeating.
    with tempfile.TemporaryDirectory(prefix='screech-nats-model-') as tmp:
        cluster = Cluster(binary, Path(tmp), spares=1)
        try:
            for slot in range(1, 4):
                cluster.start(slot)
            leader = cluster.wait_leader()
            client = cluster.client(leader)
            client.create()
            assert client.publish(b'canonical-history') == 1
            client.close()
            compact(cluster)
            previous = cluster.member(leader)
            with sqlite3.connect(cluster.store_path(leader)) as db:
                model = db.execute("SELECT value FROM cluster_meta WHERE key='app_snapshot'").fetchone()[0]
                position = db.execute("SELECT value FROM cluster_meta WHERE key='app_sequence'").fetchone()[0]
                bad = bytes([127]) + model[1:]
                # Keep the cache's byte checksum valid to exercise structural validation.
                crc = zlib.crc32(position + bad).to_bytes(4, 'little')
                db.execute("UPDATE cluster_meta SET value=? WHERE key='app_snapshot'", [bad])
                db.execute("UPDATE cluster_meta SET value=? WHERE key='app_crc'", [crc])
            # Standby enrollment changes the epoch, restarting the leader's model
            # without taking another voter offline. The canonical Raft image is intact.
            cluster.start(4)
            wait_recovery(cluster, leader, previous)
            verify(cluster, {1: b'canonical-history'}, acked=False)
            assert cluster.processes[leader].poll() is None
        finally:
            cluster.close()
    return {'test': 'malformed application recovery fences the leader and heals from canonical quorum state', 'passed': True}


def invalid_stored_model_waits_for_quorum(binary):
    with tempfile.TemporaryDirectory(prefix='screech-nats-invalid-root-') as tmp:
        cluster = Cluster(binary, Path(tmp))
        try:
            for slot in range(1, 4):
                cluster.start(slot)
            victim = cluster.wait_leader()
            client = cluster.client(victim)
            client.create()
            assert client.publish(b'valid-canonical-payload') == 1
            client.close()
            compact(cluster)
            cluster.await_equal()
            previous = cluster.member(victim)
            for slot in list(cluster.processes):
                cluster.kill(slot)
            with sqlite3.connect(cluster.store_path(victim)) as db:
                root = db.execute("SELECT value FROM cluster_meta WHERE key='raft_snapshot'").fetchone()[0]
                model = db.execute("SELECT value FROM cluster_meta WHERE key='app_snapshot'").fetchone()[0]
                assert root.count(model) == 1
                offset = root.index(model)
                damaged = root[:offset] + bytes([127]) + root[offset + 1:]
                db.execute("UPDATE cluster_meta SET value=? WHERE key='raft_snapshot'", [damaged])
                db.execute("UPDATE cluster_meta SET value=? WHERE key='raft_snapshot_hash'", [hashlib.sha256(damaged).digest()])
            cluster.start(victim)
            time.sleep(2)
            # Byte checksums alone are insufficient: no Raft runtime may start.
            try:
                connection = socket.create_connection(cluster.address(victim), timeout=.3)
                connection.close()
                raise AssertionError('invalid stored model started a voting runtime')
            except OSError:
                pass
            assert cluster.processes[victim].poll() is None
            for slot in range(1, 4):
                if slot != victim:
                    cluster.start(slot)
            wait_recovery(cluster, victim, previous)
            verify(cluster, {1: b'valid-canonical-payload'}, acked=False)
        finally:
            cluster.close()
    return {'test': 'checksum-valid malformed stored model waits without voting, then heals from the quorum', 'passed': True}


def failed_sync_never_confirms(binary, shim, mode):
    # NATS #7564/#7567: actual authoritative journal sync precedes replication success.
    with tempfile.TemporaryDirectory(prefix='screech-nats-sync-') as tmp:
        cluster = Cluster(binary, Path(tmp))
        try:
            for slot in range(1, 4):
                node = cluster.root / str(slot)
                cluster.start(slot, extra_env={
                    'LD_PRELOAD': str(shim), 'SCREECH_IO_MODE': mode,
                    'SCREECH_IO_ARM': str(node / 'arm'),
                    'SCREECH_IO_HIT': str(node / 'sync-fault'),
                    'SCREECH_IO_SUFFIX': '.cluster-watermark',
                })
            leader = cluster.wait_leader()
            client = cluster.client(leader)
            client.create()
            assert client.publish(b'fsynced-seed') == 1
            previous = cluster.member(leader)
            (cluster.root / str(leader) / 'arm').write_text('armed')
            try:
                result = client.request('test.x', b'unconfirmed-sync-failure')
                assert 'error' in result, ('failed fsync produced a durable publication ACK', result)
            except (OSError, EOFError):
                pass
            finally:
                client.close()
            marker = cluster.root / str(leader) / 'sync-fault'
            assert marker.read_text() in ('fsync', 'fdatasync')
            wait_recovery(cluster, leader, previous)
            verify(cluster, {1: b'fsynced-seed'}, acked=False)
            client = cluster.client(cluster.wait_leader())
            ack = client.publish_with_id(b'retry', 'sync-retry')
            duplicate = client.publish_with_id(b'retry', 'sync-retry')
            assert duplicate['duplicate'] and duplicate['seq'] == ack['seq']
            client.close()
            cluster.await_equal()
        finally:
            cluster.close()
    return {'test': f'{mode} on leader journal never releases a success ACK and heals without process restart', 'passed': True}


def stalled_ack_remains_recoverable(binary, shim):
    # NATS #8789: a disk stall must not leave durable consumer ACKs stuck forever.
    with tempfile.TemporaryDirectory(prefix='screech-nats-stalled-ack-') as tmp:
        cluster = Cluster(binary, Path(tmp))
        try:
            for slot in range(1, 4):
                node = cluster.root / str(slot)
                cluster.start(slot, extra_env={
                    'LD_PRELOAD': str(shim), 'SCREECH_IO_MODE': 'sync-hang-before',
                    'SCREECH_IO_ARM': str(node / 'arm'),
                    'SCREECH_IO_HIT': str(node / 'sync-fault'),
                    'SCREECH_IO_SUFFIX': '.cluster-watermark',
                })
            leader = cluster.wait_leader()
            client = cluster.client(leader)
            result = client.request('$JS.API.STREAM.CREATE.T', {
                'name': 'T', 'subjects': ['test.>'], 'storage': 'file', 'retention': 'workqueue',
            })
            assert 'error' not in result, result
            client.create_consumer(ack_wait=100_000_000, max_pending=1)
            assert client.publish(b'stalled-ack') == 1
            original = client.pull()
            previous = cluster.member(leader)
            (cluster.root / str(leader) / 'arm').write_text('armed')
            inbox = client.inbox + '.stalled'
            client.send(f'SUB {inbox} 9999\r\nUNSUB 9999 1\r\nPUB {original["reply"]} {inbox} 4\r\n+ACK\r\n'.encode())
            marker = cluster.root / str(leader) / 'sync-fault'
            until = time.monotonic() + 10
            while not marker.exists():
                assert time.monotonic() < until, 'ACK did not reach the real sync boundary'
                time.sleep(.01)
            replacement = cluster.wait_leader(exclude=(leader,))
            # A durable success cannot escape while the old leader's fsync hangs.
            if select.select([client.socket], [], [], .05)[0]:
                try:
                    response = client.read_message()
                    assert response['payload'] != b'' or response['status'] is not None, response
                except EOFError:
                    pass
            client.close()
            client = cluster.client(replacement)
            info = client.request('$JS.API.CONSUMER.INFO.T.D', {})
            uncertain_committed = info['ack_floor']['consumer_seq'] == 1
            if uncertain_committed:
                # Persistence-independent leader Append messages can reach a
                # durable follower quorum before its own fsync stalls. The next
                # leader may commit this unconfirmed operation; it must settle
                # as a complete ACK/retention transition, not a hybrid state.
                assert info['num_ack_pending'] == 0 and info['num_pending'] == 0, info
                assert client.pull() is None
            else:
                assert info['ack_floor']['consumer_seq'] == 0 and info['num_ack_pending'] == 1, info
                retried = client.pull()
                assert retried['payload'] == b'stalled-ack'
                assert int(retried['reply'].split('.')[6]) >= 2
                client.ack(original)
            assert client.get(1) is None
            info = client.request('$JS.API.CONSUMER.INFO.T.D', {})
            assert info['num_ack_pending'] == 0, info
            client.close()
            cluster.kill(leader)
            cluster.start(leader)
            wait_recovery(cluster, leader, previous)
            client = cluster.client(cluster.wait_leader())
            assert client.get(1) is None
            # Always exercise an actually unacknowledged reservation too,
            # regardless of how the earlier ambiguous ACK settled.
            assert client.publish(b'late-ack-after-stall') == 2
            pending = client.pull()
            victim = cluster.wait_leader()
            previous = cluster.member(victim)
            client.close()
            cluster.kill(victim)
            client = cluster.client(cluster.wait_leader())
            retried = client.pull()
            assert retried['payload'] == b'late-ack-after-stall'
            assert int(retried['reply'].split('.')[6]) >= 2
            client.ack(pending)
            assert client.get(2) is None
            client.close()
            cluster.start(victim)
            wait_recovery(cluster, victim, previous)
            client = cluster.client(cluster.wait_leader())
            assert client.publish(b'after-stall') == 3
            client.close()
        finally:
            cluster.close()
    return {'test': 'stalled ACK never confirms early; uncertain outcomes, redelivery and late ACK survive election and automatic repair', 'uncertain_ack_committed': uncertain_committed, 'passed': True}


def run(binary, shim=None):
    results = [missing_live_content(binary, kind) for kind in ('snapshot', 'log')]
    results.extend([create_after_empty_snapshot(binary), stream_update_preserves_consumer(binary), minority_corruption(binary), isolated_replayed_membership(binary), malformed_application_recovery(binary), invalid_stored_model_waits_for_quorum(binary)])
    if shim:
        results.extend(failed_sync_never_confirms(binary, shim, mode) for mode in ('sync-before-eio', 'sync-after-eio'))
        results.append(stalled_ack_remains_recoverable(binary, shim))
    return results


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('binary', type=lambda value: str(Path(value).resolve()))
    parser.add_argument('--output', type=Path)
    parser.add_argument('--shim', type=lambda value: Path(value).resolve())
    args = parser.parse_args()
    result = run(args.binary, args.shim)
    text = json.dumps(result, indent=2)
    print(text)
    if args.output:
        args.output.write_text(text + '\n')
