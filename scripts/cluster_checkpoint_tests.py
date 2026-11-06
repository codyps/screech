#!/usr/bin/env python3
"""Long retained history across automatic compaction, cold restart and repair."""
import argparse
import json
from pathlib import Path
import shutil
import signal
import sys
import tempfile
import time

from cluster_tests import Cluster
from cluster_recovery_tests import damage_store, status, wait_healed


def audit(cluster, last, retained):
    # Cold recovery can replace a fenced incarnation and retire the service
    # while a read-only audit is in flight. Reconnect to its current epoch;
    # never retry an invariant failure or accept an incomplete audit.
    deadline = time.monotonic() + 45
    while True:
        try:
            audit_once(cluster, last, retained)
            return
        except (OSError, EOFError):
            if time.monotonic() >= deadline:
                raise
            print('Checkpoint audit: reconnecting after service retirement', flush=True)
            time.sleep(.05)


def audit_once(cluster, last, retained):
    client = cluster.client(cluster.wait_leader())
    try:
        for seq in range(last - retained + 1, last + 1):
            assert client.get(seq) == f'body-{seq}'.encode(), seq
        assert client.get(last - retained) is None
        duplicate = client.publish_with_id(b'ignored', f'id-{last}')
        assert duplicate.get('duplicate') and duplicate['seq'] == last, duplicate
        consumer = client.request('$JS.API.CONSUMER.INFO.T.D', {})
        assert consumer['ack_floor']['consumer_seq'] == 1, consumer
        assert consumer['num_ack_pending'] == 1, consumer
    finally:
        client.close()


def run(binary, failure_dir=None):
    last, retained = 8200, 128
    began = time.monotonic()
    def progress(stage):
        print(f'Checkpoint history: {stage} ({time.monotonic()-began:.1f}s)', flush=True)
    with tempfile.TemporaryDirectory(prefix='screech-auto-checkpoint-') as tmp:
        cluster = Cluster(binary, Path(tmp))
        try:
            for node in range(1, 4):
                cluster.start(node)
            client = cluster.client(cluster.wait_leader())
            progress("cluster formed")
            try:
                created = client.request('$JS.API.STREAM.CREATE.T', dict(name='T', subjects=['test.>'],
                    storage='file', num_replicas=3, retention='limits', max_msgs=retained,
                    duplicate_window=3600_000_000_000))
                assert 'error' not in created, created
                for seq in range(1, last + 1):
                    result = client.publish_with_id(f'body-{seq}'.encode(), f'id-{seq}')
                    assert result['seq'] == seq and not result.get('duplicate'), result
                    if seq % 1024 == 0:
                        progress(f'{seq}/{last} publications confirmed')
                client.create_consumer(ack_wait=3600_000_000_000)
                first = client.pull()
                assert first['payload'] == f'body-{last-retained+1}'.encode(), first
                client.ack(first)
                second = client.pull()
                assert second['payload'] == f'body-{last-retained+2}'.encode(), second
            finally:
                client.close()
            deadline = time.monotonic() + 30
            while status(cluster)['snapshot_index'] == 0:
                assert time.monotonic() < deadline, 'automatic checkpoint did not run'
                time.sleep(.05)
            cluster.await_equal()
            progress("automatic compaction complete")
            audit(cluster, last, retained)
            cluster.close()
            progress("cold restart")
            for node in (1, 2):
                cluster.start(node)
            cluster.wait_leader()
            # A returning voter that discovers a live quorum must enroll a
            # fresh identity, even if its local store is intact. Deliberately
            # exercise that path and finish its promotion before losing another
            # voter. A serving leader alone does not establish that the next
            # fault will leave a durable quorum.
            cluster.start(3)
            wait_healed(cluster, 3)
            recovered = status(cluster)
            assert set(recovered['voters']) == {cluster.member(node) for node in (1, 2, 3)}, recovered
            assert not recovered['outgoing'] and recovered['replacement'] is None, recovered
            progress("cold restart voters fully recovered")
            audit(cluster, last, retained)
            progress("cold restart audited; replacing leader")
            leader = cluster.wait_leader()
            cluster.kill(leader)
            damage_store(cluster, leader, 'all-local-state-missing')
            cluster.start(leader)
            wait_healed(cluster, leader)
            progress("wiped leader repaired")
            audit(cluster, last, retained)
            return [dict(test='automatic compaction preserves retained payloads, dedup and confirmed ACKs across cold restart and voter repair',
                         publications=last, retained=retained, cold_returner_promoted_before_next_fault=True,
                         surviving_voters_during_leader_loss=2, passed=True)]
        except BaseException:
            cluster.close()
            if failure_dir is not None:
                saved = failure_dir / f'cluster-{time.time_ns()}'
                try:
                    shutil.copytree(cluster.root, saved)
                    print(f'Failed checkpoint history preserved in {saved}', file=sys.stderr)
                except OSError as error:
                    print(f'Could not preserve checkpoint history: {error}', file=sys.stderr)
            raise
        finally:
            cluster.close()


if __name__ == '__main__':
    # CI termination must preserve the stores just like an assertion failure.
    def terminated(number, _frame):
        raise SystemExit(128 + number)
    signal.signal(signal.SIGTERM, terminated)
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('binary', type=lambda s: str(Path(s).resolve()))
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    results = run(args.binary, args.output.with_suffix('.failure'))
    args.output.write_text(json.dumps(results, indent=2) + '\n')
    print(json.dumps(results, indent=2))
