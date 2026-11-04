#!/usr/bin/env python3
"""Pure replies, empty groups and ACKs must not bypass a lost quorum.

Requires --features fault-injection for the bounded ingress scheduling gate.
"""
import argparse
from concurrent.futures import ThreadPoolExecutor
import json
from pathlib import Path
import tempfile
import time

from cluster_tests import Cluster


def wait_for(predicate, description):
    deadline = time.monotonic() + 10
    while time.monotonic() < deadline:
        if predicate():
            return
        time.sleep(.005)
    raise TimeoutError(description)


def run_case(binary, count, kind, clients_count=2):
    with tempfile.TemporaryDirectory(prefix='screech-quorum-') as tmp:
        root = Path(tmp)
        gate = root/'gate'
        release, ready, ingress = (Path(str(gate)+suffix) for suffix in ('.release','.ready','.ingress'))
        release.touch()
        env = {'SCREECH_GROUP_GATE': str(gate)}
        cluster = Cluster(binary, root, count=count)
        try:
            for slot in range(1, count+1):
                cluster.start(slot, extra_env=env)
            leader = cluster.wait_leader()
            client = cluster.client(leader)
            try:
                client.create()
                client.create_consumer(ack_wait=60_000_000_000)
                assert client.publish_with_id(b'confirmed-payload', 'stable')['seq'] == 1
                message = client.pull()
                if kind != 'consumer-ack':
                    client.ack(message)
                if kind == 'pull-delivery':
                    assert client.publish(b'awaiting-delivery') == 2
            finally:
                client.close()

            clients = [cluster.client(leader) for _ in range(clients_count)]
            def attempt(client):
                try:
                    if kind == 'duplicate':
                        client.publish_with_id(b'ignored', 'stable')
                        return True
                    if kind == 'empty-pull':
                        reply = client.request_message('$JS.API.CONSUMER.MSG.NEXT.T.D', b'{"batch":1,"no_wait":true}')
                        return reply['status'] in (None, 404)
                    if kind == 'pull-delivery':
                        return client.pull()['payload'] == b'awaiting-delivery'
                    if kind == 'consumer-ack':
                        reply = client.request_message(message['reply'], b'+ACK')
                        return reply['status'] is None
                    reply = client.request('$JS.API.STREAM.INFO.T', {})
                    return 'error' not in reply
                except (OSError, EOFError, AssertionError):
                    return False
                finally:
                    client.close()

            def lose_quorum():
                victims = [slot for slot in cluster.processes if slot != leader][:count//2+1]
                for slot in victims:
                    cluster.kill(slot)
                return victims

            with ThreadPoolExecutor(max_workers=clients_count) as pool:
                if kind == 'api-read':
                    victims = lose_quorum()
                    futures = [pool.submit(attempt, client) for client in clients]
                else:
                    before = ingress.stat().st_size if ingress.exists() else 0
                    ready.unlink(missing_ok=True)
                    release.unlink()
                    futures = [pool.submit(attempt, client) for client in clients]
                    try:
                        wait_for(lambda: ready.exists() and ingress.exists() and ingress.stat().st_size >= before+clients_count,
                                 'requests were not admitted behind the scheduling gate')
                        victims = lose_quorum()
                    finally:
                        release.touch()
                assert not any(future.result(timeout=12) for future in futures), (count, kind, 'minority confirmed a reply')

            for slot in victims:
                cluster.start(slot, extra_env=env)
            cluster.wait_leader()
            # Rejoins may change the service epoch while an audit is in flight.
            deadline = time.monotonic()+30
            verified = False
            while time.monotonic() < deadline:
                client = None
                try:
                    client = cluster.client(cluster.wait_leader())
                    assert client.get(1) == b'confirmed-payload'
                    if kind == 'pull-delivery':
                        assert client.get(2) == b'awaiting-delivery'
                    duplicate = client.publish_with_id(b'ignored', 'stable')
                    assert duplicate['duplicate'] and duplicate['seq'] == 1
                    if kind != 'consumer-ack':
                        info = client.request('$JS.API.CONSUMER.INFO.T.D', {})
                        assert info['ack_floor']['consumer_seq'] == 1, info
                    verified = True
                    break
                except (OSError, EOFError):
                    time.sleep(.05)
                finally:
                    if client:
                        client.close()
            assert verified, 'automatic recovery did not preserve confirmed payloads/ACKs and dedup'
            cluster.await_equal()
        finally:
            release.touch()
            cluster.close()
    return dict(voters=count, test=kind, rejected=clients_count, recovered=True, passed=True)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('binary', type=lambda path: str(Path(path).resolve()))
    parser.add_argument('--output', type=Path)
    args = parser.parse_args()
    results = [run_case(args.binary, count, kind, clients_count) for count in (3, 5)
               for clients_count in (1, 2)
               for kind in ('duplicate', 'empty-pull', 'pull-delivery', 'consumer-ack', 'api-read')]
    text = json.dumps(results, indent=2)+'\n'
    print(text)
    if args.output:
        args.output.write_text(text)
