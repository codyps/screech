#!/usr/bin/env python3
"""Cold restart after durable follower ACKs with uncommitted SQL projections."""
import argparse
from contextlib import closing
import json
from pathlib import Path
import shutil
import sqlite3
import tempfile

from cluster_tests import Cluster
from cluster_recovery_tests import wait_marker
from cluster_journal_tests import audit_voter


def run_case(binary, voters, lose_projections):
    with tempfile.TemporaryDirectory(prefix='screech-projection-window-') as tmp:
        cluster = Cluster(binary, Path(tmp), count=voters)
        try:
            for slot in range(1, voters + 1):
                node = cluster.root / str(slot)
                cluster.start(slot, extra_env={
                    'SCREECH_FAULT_POINT': 'after_cluster_accept_before_projection_commit',
                    'SCREECH_FAULT_ACTION': 'hang',
                    'SCREECH_FAULT_ARM': str(node / 'arm'),
                    'SCREECH_FAULT_MARKER': str(node / 'projection-open'),
                })
            leader = cluster.wait_leader()
            client = cluster.client(leader)
            try:
                info = client.request('$JS.API.STREAM.CREATE.T', dict(
                    name='T', subjects=['test.>'], storage='file', retention='limits',
                    num_replicas=voters, duplicate_window=3600_000_000_000))
                assert 'error' not in info, info
                client.create_consumer(ack_wait=3600_000_000_000)
                assert client.publish_with_id(b'confirmed-payload', 'stable')['seq'] == 1
                delivery = client.pull()
                assert delivery['payload'] == b'confirmed-payload', delivery
                cluster.await_equal()
                paths, backups = {}, {}
                for slot in cluster.processes:
                    paths[slot] = cluster.store_path(slot)
                    backups[slot] = cluster.root / f'projection-{slot}.sqlite'
                    with closing(sqlite3.connect(paths[slot])) as source:
                        with closing(sqlite3.connect(backups[slot])) as backup:
                            source.backup(backup)
                for slot in cluster.processes:
                    if slot != leader:
                        (cluster.root / str(slot) / 'arm').touch()
                # Each follower dispatches its durable acceptance response, then
                # hangs before COMMIT of the SQL transaction containing the ACK.
                client.ack(delivery)
                for slot in cluster.processes:
                    if slot != leader:
                        wait_marker(cluster, slot, cluster.root / str(slot) / 'projection-open')
            finally:
                client.close()
            for slot in list(cluster.processes):
                cluster.kill(slot)
            if lose_projections:
                # Restore only replayable SQL state. The canonical journals,
                # synced snapshot roots and voter identities remain untouched.
                for slot, path in paths.items():
                    for suffix in ['-wal', '-shm']:
                        Path(str(path) + suffix).unlink(missing_ok=True)
                    shutil.copy2(backups[slot], path)
            for slot in range(1, voters + 1):
                cluster.start(slot)
            cluster.wait_leader()
            cluster.await_equal()
            for slot in cluster.processes:
                audit_voter(cluster, slot, {1: b'confirmed-payload'})
            return dict(test='confirmed consumer ACK survives a full kill with follower SQL transactions open',
                        voters=voters, lost_sql_projections=lose_projections,
                        confirmed_consumer_acks=1, passed=True)
        finally:
            cluster.close()


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('binary', type=lambda value: str(Path(value).resolve()))
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    rows = [run_case(args.binary, voters, lose)
            for voters in [3, 5] for lose in [False, True]]
    text = json.dumps(rows, indent=2) + '\n'
    args.output.write_text(text)
    print(text)
