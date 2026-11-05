#!/usr/bin/env python3
"""Interrupt fresh duplicate-read replies across stream recreation and voter repair."""
import argparse
from concurrent.futures import ThreadPoolExecutor
import json
from pathlib import Path
import tempfile
import time

from cluster_tests import Cluster
from cluster_recovery_tests import damage_store, wait_healed, wait_marker


def run_case(binary, voters):
    with tempfile.TemporaryDirectory(prefix='screech-direct-read-') as tmp:
        root = Path(tmp)
        cluster = Cluster(binary, root, count=voters)
        def environment(slot):
            return dict(SCREECH_FAULT_POINT='before_async_duplicate_reply',
                        SCREECH_FAULT_ACTION='hang',
                        SCREECH_FAULT_ARM=str(root/str(slot)/'armed'),
                        SCREECH_FAULT_MARKER=str(root/str(slot)/'read-proof'))
        try:
            for slot in range(1, voters + 1):
                cluster.start(slot, extra_env=environment(slot))
            client = cluster.client(cluster.wait_leader())
            client.create()
            assert client.publish_with_id(b'original', 'stable')['seq'] == 1
            client.close()
            for generation, expected in [(1, b'original'), (2, b'recreated')]:
                leader = cluster.wait_leader()
                previous = cluster.member(leader)
                marker, arm = root/str(leader)/'read-proof', root/str(leader)/'armed'
                marker.unlink(missing_ok=True)
                client = cluster.client(leader)
                # Drain admission/completion leases before exercising the path.
                time.sleep(.05)
                arm.touch()
                with ThreadPoolExecutor(max_workers=1) as pool:
                    pending = pool.submit(client.publish_with_id, b'ignored', 'stable')
                    wait_marker(cluster, leader, marker)
                    assert not pending.done(), 'duplicate ACK escaped before the paused reply'
                    cluster.kill(leader)
                    try:
                        pending.result(timeout=10)
                    except (OSError, EOFError):
                        pass
                    else:
                        raise AssertionError('killed read returned a success ACK')
                client.close()
                arm.unlink()
                damage_store(cluster, leader, 'all-local-state-missing')
                cluster.start(leader, extra_env=environment(leader))
                wait_healed(cluster, leader, previous)
                client = cluster.client(cluster.wait_leader())
                result = client.publish_with_id(b'ignored', 'stable')
                assert result.get('duplicate') and result['seq'] == generation, result
                assert client.get(generation) == expected
                if generation == 1:
                    assert client.request('$JS.API.STREAM.DELETE.T', {})['success']
                    client.create()
                    assert client.publish(b'new generation filler') == 1
                    result = client.publish_with_id(b'recreated', 'stable')
                    assert result['seq'] == 2 and not result.get('duplicate'), result
                client.close()
            return dict(test='fresh asynchronous duplicate reads survive killed replies, recreated streams and wiped voters',
                        voters=voters, generations=2, passed=True)
        finally:
            cluster.close()


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('binary', type=lambda value: str(Path(value).resolve()))
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    result = [run_case(args.binary, voters) for voters in [3, 5]]
    text = json.dumps(result, indent=2) + '\n'
    args.output.write_text(text)
    print(text)
