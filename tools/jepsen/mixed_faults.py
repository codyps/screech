"""Network/power/disk faults with direct-service conditional-write witnesses.

Only ordinary client requests exercise consensus. UDP controls validate directed
links independently; no Raft messages or application responses are synthesized.
"""
from concurrent.futures import ThreadPoolExecutor
import copy
import json
from pathlib import Path
import sys
import threading
import time
import uuid

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / 'scripts'))
from wire import Client


def parallel(function, values):
    values = list(values)
    with ThreadPoolExecutor(max_workers=max(1, len(values))) as pool:
        return list(pool.map(function, values))


def blocked_edges(nodes, minority, topology):
    minority = set(minority)
    assert minority and len(minority) <= len(nodes)//2
    assert topology in ('split', 'send', 'receive')
    return sorted((a, b) for a in nodes for b in nodes if a != b and
                  ((topology in ('split', 'send') and a in minority and b not in minority) or
                   (topology in ('split', 'receive') and a not in minority and b in minority)))


def network_canary(cluster):
    tag = 'net-' + uuid.uuid4().hex
    nodes = list(cluster.vms)
    policy = cluster.hub.snapshot()
    pairs = {(a, b) for a in nodes for b in nodes if a != b}
    blocked = {tuple(p) for p in policy['blocked']}
    expected = pairs - blocked
    parallel(lambda node: cluster.vms[node].call('net-send', tag=tag, targets=[n for n in nodes if n != node]), nodes)
    deadline = time.monotonic() + 5
    while True:
        received = dict(zip(nodes, parallel(lambda node: cluster.vms[node].call('net-received', tag=tag), nodes)))
        observed = set()
        for destination, result in received.items():
            for message in result['received']:
                prefix, source, target = message['tag'].split(':')
                assert prefix == tag and target == destination
                assert message['source_ip'] == '10.0.1.' + source[1:]
                observed.add((source, destination))
        assert not observed & blocked, 'a blocked directed link delivered a new UDP control'
        if observed == expected:
            break
        assert time.monotonic() < deadline, f'network canary missed allowed links: {expected-observed}'
        time.sleep(.05)
        parallel(lambda node: cluster.vms[node].call('net-send', tag=tag, targets=[n for n in nodes if n != node]), nodes)
    after = cluster.hub.snapshot()
    assert after['epoch'] == policy['epoch'] and after['blocked'] == policy['blocked']
    counts = {(e['source'], e['destination']): e for e in after['edges']}
    assert all(counts[p]['dropped'] > 0 for p in blocked)
    assert all(counts[p]['forwarded'] > 0 for p in expected)
    return {'tag': tag, 'epoch': policy['epoch'], 'blocked': policy['blocked'],
            'attempted': [list(p) for p in sorted(pairs)], 'received': [list(p) for p in sorted(observed)],
            'boot_ids': {n: r['boot_id'] for n, r in received.items()}, 'edges': after['edges']}


class Witness:
    def __init__(self, cluster, case):
        self.cluster, self.case = cluster, case
        self.lock = threading.Lock()
        self.round_lock = threading.Lock()
        self.stop_event = threading.Event()
        self.phase = 'partitioned'
        self.error = None
        self.thread = threading.Thread(target=self.run, daemon=True)
        self.known = {1: case['seed']}
        self.observed = 1

    def save(self):
        with self.lock:
            data = json.dumps(self.case, indent=2)
        (self.cluster.proof_path.parent / 'mixed-progress.json').write_text(data)

    def switch(self, phase, action=lambda: None):
        # Drain old rounds before changing link policy: a response received after
        # healing must never be classified as a partition-side confirmation.
        with self.round_lock:
            action()
            self.phase = phase

    def probe(self, node, action, **payload):
        vm = self.cluster.vms[node]
        if vm.process is None or vm.process.poll() is not None:
            return dict(node=node, outcome='offline', **payload)
        try:
            result = vm.call('cas', rpc_timeout=8, stream=self.case['stream'], action=action, **payload)
            assert result['service_node'] == node, 'conditional probe reached the wrong guest'
            return dict(payload, node=node, **result)
        except OSError as error:
            return dict(payload, node=node, outcome='unknown', error=str(error))

    def round(self):
        node = self.case['survivors'][0]
        client = Client(('127.0.0.1', self.cluster.vms[node].client_port), timeout=3)
        try:
            info = client.request(f'$JS.API.STREAM.INFO.{self.case["stream"]}', {})
        finally:
            client.close()
        if info.get('error'):
            return
        expected = info['state']['last_seq']
        assert expected >= self.observed, 'quorum read rolled back an observed committed prefix'
        phase = self.phase
        def write(node):
            identity = uuid.uuid4().hex
            return self.probe(node, 'write', expected=expected, id=identity, body='cas-' + identity)
        writes = parallel(write, self.cluster.vms)
        successes = [w for w in writes if w['outcome'] == 'ok']
        creates = (parallel(lambda node: self.probe(node, 'create', name='MIN_' + uuid.uuid4().hex,
                                                    replicas=len(self.cluster.vms)), self.case['targets'])
                   if phase != 'healed' else [])
        floor = max([self.observed, expected] + [w['seq'] for w in successes])
        reads = parallel(lambda node: self.probe(node, 'read'), self.cluster.vms)
        row = {'phase': phase, 'expected': expected, 'writes': writes, 'reads': reads, 'read_floor': floor,
               'minority_creates': creates}
        with self.lock:
            self.case['rounds'].append(row)
        self.save()  # Preserve a violation too, before reporting it.
        assert len(successes) <= 1, 'split brain: conflicting conditional writes both confirmed'
        assert all(r['outcome'] != 'ok' for r in creates), 'isolated minority confirmed a new catalog mutation'
        for write in successes:
            assert write['seq'] == expected + 1
            assert phase == 'healed' or write['node'] in self.case['survivors'], 'isolated minority confirmed a fresh write'
            assert write['seq'] not in self.known or self.known[write['seq']] == write['body'], 'confirmed sequence forked'
            self.known[write['seq']] = write['body']
        for read in reads:
            assert read['outcome'] != 'ok' or read['last_seq'] >= floor, 'stale read after a completed conditional write'
        self.observed = max([floor] + [r['last_seq'] for r in reads if r['outcome'] == 'ok'])

    def run(self):
        try:
            while not self.stop_event.is_set():
                with self.round_lock:
                    try:
                        self.round()
                    except (OSError, EOFError):
                        pass  # No response establishes no success or read result.
                self.stop_event.wait(.5)
        except Exception as error:
            self.error = error
            self.stop_event.set()

    def progress(self, phase, timeout=45):
        deadline = time.monotonic() + timeout
        while True:
            if self.error:
                raise self.error
            eligible = set(self.cluster.vms) if phase == 'healed' else set(self.case['survivors'])
            with self.lock:
                count = sum(r['phase'] == phase and any(w['outcome'] == 'ok' and w['node'] in eligible
                                                       for w in r['writes']) for r in self.case['rounds'])
                reads = sum(r['phase'] == phase and any(v['outcome'] == 'ok' and v['node'] in eligible
                                                       for v in r['reads']) for r in self.case['rounds'])
            if count >= 2 and reads >= 2:
                return
            assert time.monotonic() < deadline, 'no surviving-quorum progress during ' + phase
            time.sleep(.05)

    def stop(self):
        self.stop_event.set()
        self.thread.join(timeout=20)
        assert not self.thread.is_alive(), 'conditional-write witness did not stop'
        if self.error:
            raise self.error


def mixed_fault(cluster, topology, damage):
    assert damage in ('bitflip', 'wipe') and cluster.fault_disks
    leader, replicas, membership = cluster.healthy()
    targets = [leader] + [n for n in cluster.vms if n != leader][:len(cluster.vms)//2-1]
    survivors = [n for n in cluster.vms if n not in targets]
    stream = 'MIXED_' + uuid.uuid4().hex
    client = Client(('127.0.0.1', cluster.vms[leader].client_port), timeout=5)
    try:
        response = client.request('$JS.API.STREAM.CREATE.' + stream,
                                  {'name': stream, 'subjects': [stream + '.data'], 'num_replicas': len(cluster.vms),
                                   'storage': 'file', 'retention': 'limits'})
        assert 'error' not in response
        assert client.publish(b'cas-seed', subject=stream + '.data') == 1
    finally:
        client.close()
    case = {'topology': topology, 'damage_kind': damage, 'leader': leader, 'targets': targets, 'survivors': survivors,
            'replicas_before': replicas, 'membership_before': membership, 'stream': stream, 'seed': 'cas-seed',
            'rounds': [], 'canaries': [], 'damage': [], 'complete': False}
    cluster.network_faults.append(case)
    case['epoch'] = cluster.hub.partition(blocked_edges(list(cluster.vms), targets, topology))
    case['canaries'].append(network_canary(cluster))
    witness = Witness(cluster, case)
    witness.thread.start()
    try:
        witness.progress('partitioned')
        cut = cluster.power_cut(targets)['event']
        case['cut_index'] = cluster.events.index(cut)
        witness.switch('powered-off')
        witness.progress('powered-off')
        witness.switch('repairing-partitioned')
        def corrupt(node):
            vm = cluster.vms[node]
            vm.start(maintenance=True)
            result = vm.call('damage', kind=damage)
            vm.stop()
            proof = dict(result, node=node, old_member=cut['old_members'][node])
            assert proof['old_member'] is not None
            return proof
        case['damage'] = parallel(corrupt, targets)
        cluster.damages.extend(case['damage'])
        parallel(lambda node: cluster.vms[node].start(), targets)
        case['canaries'].append(network_canary(cluster))
        witness.switch('restarted-partitioned')
        witness.progress('restarted-partitioned')
        witness.switch('healed', lambda: cluster.hub.partition([]))
        cluster.healthy()
        case['canaries'].append(network_canary(cluster))
        witness.progress('healed')
        witness.stop()
        case['confirmed'] = {str(seq): body for seq, body in witness.known.items()}
        def audit(node):
            client = Client(('127.0.0.1', cluster.vms[node].client_port), timeout=5)
            try:
                values = {str(seq): client.get(seq, name=stream).decode() for seq in sorted(witness.known)}
                assert values == case['confirmed'], 'confirmed conditional write was lost or changed'
                return node, values
            finally:
                client.close()
        case['audits'] = dict(parallel(audit, cluster.vms))
        case['network_after'] = cluster.hub.snapshot()
        case['complete'] = True
        witness.save()
        return copy.deepcopy(case)
    finally:
        witness.stop_event.set()
        witness.thread.join(timeout=20)
        cluster.heal()
