"""Disposable loopback clusters for performance campaigns (no fault hooks)."""
from contextlib import closing
import json
import os
import socket
import sqlite3
import subprocess
import time
import uuid

from wire import Client


class BenchmarkCluster:
    def __init__(self, binary, screech, count, directory):
        self.processes = []
        self.logs = []
        self.commands = []
        self.configs = []
        self.directory = directory
        self.screech = screech
        self.count = count
        self.readiness = {}
        held = []
        try:
            for _ in range(count * 3):
                sock = socket.socket()
                held.append(sock)
                sock.bind(('127.0.0.1', 0))
            ports = [sock.getsockname()[1] for sock in held]
        finally:
            for sock in held:
                sock.close()
        self.peers = {str(i + 1): dict(client=f'127.0.0.1:{ports[3*i]}',
                                     raft=f'127.0.0.1:{ports[3*i+1]}',
                                     service=f'127.0.0.1:{ports[3*i+2]}')
                      for i in range(count)}
        self.port = ports[0]
        cluster_id = 'bench-' + uuid.uuid4().hex
        for i in range(1, count + 1):
            node = directory / str(i)
            node.mkdir()
            if screech:
                config = node / 'cluster.json'
                config.write_text(json.dumps(dict(cluster_id=cluster_id, node_id=i,
                    secret=uuid.uuid5(uuid.NAMESPACE_OID, cluster_id).hex,
                    peers=self.peers, initial_voters=list(range(1, count + 1)))))
                command = [binary, '--store', str(node/'db'), '--cluster-config', str(config)]
            else:
                config = node / 'nats.conf'
                routes = [f'nats-route://{peer["raft"]}' for key, peer in self.peers.items() if key != str(i)]
                config.write_text(f'server_name: "n{i}"\nlisten: "{self.peers[str(i)]["client"]}"\n'
                    f'jetstream {{ store_dir: {json.dumps(str(node/"js"))}, sync_interval: always }}\n'
                    f'cluster {{ name: "{cluster_id}", listen: "{self.peers[str(i)]["raft"]}", '
                    f'routes: {json.dumps(routes)} }}\n')
                command = [binary, '-c', str(config)]
            self.commands.append(command)
            self.configs.append(config.read_text())

    def start(self):
        env = os.environ.copy()
        env.update(TOKIO_WORKER_THREADS='2', GOMAXPROCS='2')
        try:
            for i, command in enumerate(self.commands, 1):
                log = open(self.directory/str(i)/'server.log', 'wb')
                self.logs.append(log)
                self.processes.append(subprocess.Popen(command, stdout=log, stderr=subprocess.STDOUT, env=env))
            self.wait_ready()
            return self
        except BaseException:
            self.close()
            raise

    def assert_alive(self):
        for i, process in enumerate(self.processes, 1):
            if process.poll() is not None:
                raise RuntimeError(f'voter {i} exited: ' + (self.directory/str(i)/'server.log').read_text())

    def wait_ready(self):
        deadline = time.monotonic() + 45
        last_error = None
        while time.monotonic() < deadline:
            self.assert_alive()
            client = None
            try:
                client = Client(('127.0.0.1', self.port), timeout=2)
                if self.screech:
                    status = client.request('$JS.API.SCREECH.CLUSTER.STATUS', {})
                    if len(status.get('voters', [])) != self.count or status.get('replacement'):
                        raise ValueError(f'cluster not fully admitted: {status}')
                info = client.request('$JS.API.STREAM.CREATE.BENCH_READY', dict(
                    name='BENCH_READY', subjects=['bench.ready'], storage='file', num_replicas=self.count))
                if info.get('config', {}).get('num_replicas') != self.count:
                    raise ValueError(f'replica readiness: {info}')
                if self.screech:
                    status = client.request('$JS.API.SCREECH.CLUSTER.STATUS', {})
                    # Hold the target fixed while followers catch up. Issuing
                    # fresh API operations on every poll can keep moving it.
                    while not self.all_applied(status['commit_index']):
                        self.assert_alive()
                        if time.monotonic() >= deadline:
                            raise TimeoutError('Screech replicas have not all applied the readiness stream')
                        time.sleep(.05)
                else:
                    # Idempotent CREATE can be answered by the metadata leader
                    # with its local view. INFO is answered by the stream leader.
                    info = client.request('$JS.API.STREAM.INFO.BENCH_READY', {})
                    cluster = info.get('cluster', {})
                    peers = cluster.get('replicas', [])
                    if not cluster.get('leader') or len(peers) != self.count - 1 or not all(p.get('current') and not p.get('offline') for p in peers):
                        raise ValueError(f'NATS replicas not current: {info}')
                result = client.request('$JS.API.STREAM.DELETE.BENCH_READY', {})
                if not result.get('success'):
                    raise ValueError(f'readiness cleanup: {result}')
                greeting = getattr(client, 'info', {})
                greeting = greeting if isinstance(greeting, dict) else {}
                # A local handoff advertises the gateway port; a remote proxy
                # advertises the serving node's service port in the same INFO.
                # Observe placement without changing routing or issuing a probe.
                serving = next((int(slot) for slot, peer in self.peers.items()
                    if greeting.get('port') in (int(peer['client'].rsplit(':', 1)[1]),
                                               int(peer['service'].rsplit(':', 1)[1]))), None)
                self.readiness = dict(gateway_slot=1, greeting=greeting,
                    serving_node_slot_at_ready=serving if self.screech else None,
                    readiness_stream_leader=None if self.screech else info['cluster']['leader'])
                return
            except (OSError, EOFError, AssertionError, ValueError) as error:
                last_error = error
                time.sleep(.1)
            finally:
                if client:
                    client.close()
        raise TimeoutError(f'cluster readiness: {last_error}')

    def all_applied(self, commit):
        # Fresh benchmark stores use the initial path. This read-only check is
        # outside timing and waits for every voter, not only the serving quorum.
        for i in range(1, self.count + 1):
            try:
                with closing(sqlite3.connect(f'file:{self.directory/str(i)/"db"}?mode=ro', uri=True, timeout=.1)) as db:
                    row = db.execute("select value from cluster_meta where key='view'").fetchone()
                    if row is None or int.from_bytes(row[0], 'little') < commit:
                        return False
            except sqlite3.Error:
                return False
        return True

    def close(self):
        for process in self.processes:
            if process.poll() is None:
                process.kill()
        for process in self.processes:
            process.wait(timeout=10)
        for log in self.logs:
            log.close()

    def save_logs(self, output, label):
        import shutil
        for i in range(1, self.count + 1):
            log = self.directory/str(i)/'server.log'
            if log.exists():
                shutil.copy2(log, output.parent/f'{output.stem}-{label}-voter-{i}.log')
