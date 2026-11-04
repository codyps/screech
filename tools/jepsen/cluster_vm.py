#!/usr/bin/env python3
"""Jepsen over independent QEMU kernels and native ext4 disks, without host root."""
import argparse
import base64
from concurrent.futures import ThreadPoolExecutor
import hashlib
from http.server import ThreadingHTTPServer
import json
import os
from pathlib import Path
import secrets
import signal
import shutil
import socket
import subprocess
import sys
import threading
import time
import urllib.request
import uuid

from ethernet import Hub, TCPProxy
from block_faults import Disk
from guest import Control
from vm import REPO, prepare, archive
sys.path.insert(0, str(REPO / 'scripts'))
from wire import Client
from mixed_faults import mixed_fault


def parallel(function, values):
    values = list(values)
    with ThreadPoolExecutor(max_workers=max(1, len(values))) as pool:
        return list(pool.map(function, values))


def rpc(port, token, payload, timeout=120):
    request = urllib.request.Request(f'http://127.0.0.1:{port}/', data=json.dumps(payload).encode(),
                                     headers={'Authorization': 'Bearer ' + token})
    with urllib.request.urlopen(request, timeout=timeout) as response:
        result = json.load(response)
    if 'error' in result:
        raise RuntimeError(result)
    return result


def check_power_canary(arm, status):
    canary = status['canary']
    old, new = arm['boot_id'], status['boot_id']
    assert isinstance(old, str) and old and isinstance(new, str) and new and new != old, 'reboot reused/missed kernel boot ID'
    assert canary and canary['clean'] == 'durable-canary-v1', 'fsynced control data was lost'
    assert canary['dirty'] == 'stable-before-unsynced-overwrite', 'unsynced overwrite survived: power-loss control not established'
    return {'old_boot_id': old, 'new_boot_id': new, 'fsynced_preserved': True, 'unsynced_lost': True}


class QMP:
    def __init__(self, path):
        self.socket = socket.socket(socket.AF_UNIX)
        self.socket.settimeout(10)
        self.socket.connect(str(path))
        self.file = self.socket.makefile('rwb', buffering=0)
        assert 'QMP' in json.loads(self.file.readline())
        self.command('qmp_capabilities')

    def command(self, name):
        self.file.write(json.dumps({'execute': name}).encode() + b'\n')
        while True:
            result = json.loads(self.file.readline())
            if 'error' in result:
                raise RuntimeError(result)
            if 'return' in result:
                return result['return']

    def close(self):
        self.file.close()
        self.socket.close()


class VM:
    def __init__(self, owner, node):
        self.owner, self.node = owner, node
        self.directory = owner.output / node
        self.directory.mkdir()
        (self.directory / 'bootshare').mkdir()
        self.disk = self.directory / 'disk.raw'
        self.process = None
        self.qmp = None
        self.block = None
        self.boots = []
        self.verified = []
        self.pending = None
        self.lock = threading.RLock()
        self.backend_client_port = None
        self.client_proxy = TCPProxy(lambda: ('127.0.0.1', self.backend_client_port)
                                     if self.process is not None and self.process.poll() is None else None)
        self.client_port = self.client_proxy.port
        self.control_port = None
        self.new_disk()

    def new_disk(self):
        self.fresh_disk = True
        with self.disk.open('wb') as file:
            file.truncate(2 * 1024 ** 3)
        subprocess.run([self.owner.runtime['mkfs'], '-q', '-t', 'ext4', '-F', str(self.disk)], check=True)

    def call(self, op, *, rpc_timeout=120, **payload):
        return rpc(self.control_port, self.owner.token, dict(payload, op=op, node=self.node), timeout=rpc_timeout)

    def start(self, maintenance=False):
        with self.lock:
            if self.process is not None and self.process.poll() is None and self.boots[-1]['maintenance'] != maintenance:
                self.stop()
            if self.process is not None and self.process.poll() is None:
                if self.qmp.command('query-status')['status'] == 'paused':
                    self.qmp.command('cont')
                if not maintenance:
                    self.call('start')
                return
            boot = len(self.boots)
            qmp_path = self.directory / 'qmp.sock'
            qmp_path.unlink(missing_ok=True)
            index = int(self.node[1:])
            reservations = [socket.socket() for _ in range(2)]
            for reservation in reservations:
                reservation.bind(('127.0.0.1', 0))
            self.control_port, self.backend_client_port = [r.getsockname()[1] for r in reservations]
            command = [self.owner.runtime['tools'] + '/bin/qemu-system-x86_64',
                       '-accel', 'tcg,thread=multi', '-m', str(self.owner.memory), '-smp', '2',
                       '-display', 'none', '-serial', 'stdio', '-monitor', 'none', '-no-reboot',
                       '-qmp', f'unix:{qmp_path},server=on,wait=off',
                       '-kernel', self.owner.runtime['kernel'], '-initrd', str(self.owner.output / 'common/initramfs.gz'),
                       '-append', f'console=ttyS0 rdinit=/init panic=-1 quiet screech.node={self.node} screech.maintenance={int(maintenance)} screech.fresh={int(self.fresh_disk)}',
                       '-drive', self.drive(),
                       '-netdev', f'user,id=management,hostfwd=tcp:127.0.0.1:{self.control_port}-:9000,hostfwd=tcp:127.0.0.1:{self.backend_client_port}-:5001',
                       '-device', f'e1000,netdev=management,mac=02:00:00:00:02:{index:02x}',
                       '-netdev', f'socket,id=cluster,connect=127.0.0.1:{self.owner.hub.port}',
                       '-device', f'e1000,netdev=cluster,mac=02:00:00:00:01:{index:02x}',
                       '-virtfs', 'local,path=/nix/store,mount_tag=nix-store,security_model=none,readonly=on',
                       '-virtfs', f'local,path={REPO},mount_tag=workspace,security_model=none,readonly=on',
                       '-virtfs', f'local,path={self.owner.output},mount_tag=artifacts,security_model=none,readonly=on',
                       '-virtfs', f'local,path={self.directory}/bootshare,mount_tag=bootshare,security_model=none,readonly=on']
            with (self.directory / f'boot-{boot:03}.log').open('wb') as log:
                for reservation in reservations:
                    reservation.close()
                self.process = subprocess.Popen(command, stdin=subprocess.DEVNULL, stdout=log, stderr=subprocess.STDOUT)
            self.boots.append({'maintenance': maintenance, 'pid': self.process.pid, 'command': command})
            deadline = time.monotonic() + 90
            while True:
                try:
                    status = self.call('status')
                    assert status['node'] == self.node and status['maintenance'] == maintenance
                    break
                except (OSError, RuntimeError, AssertionError):
                    if self.process.poll() is not None or time.monotonic() > deadline:
                        raise RuntimeError(f'{self.node} failed to boot; inspect {self.directory}/boot-{boot:03}.log')
                    time.sleep(.1)
            self.qmp = QMP(qmp_path)
            self.boots[-1]['boot_id'] = status['boot_id']
            self.fresh_disk = False
            if self.pending is not None:
                arm, event = self.pending
                proof = check_power_canary(arm, status)
                self.verified.append(proof)
                event['verified'][self.node] = proof
                self.pending = None

    def drive(self):
        if self.owner.fault_disks:
            self.block = Disk(self.disk)
            return f'file=nbd://127.0.0.1:{self.block.port},format=raw,if=virtio,cache=writeback,werror=report,rerror=report'
        return f'file={self.disk},format=raw,if=virtio,cache=none'

    def close_block(self):
        if self.block is not None:
            self.block.close()
            self.block = None

    def stop(self):
        with self.lock:
            if self.process is not None and self.process.poll() is None:
                self.process.kill()
                self.process.wait(timeout=10)
            if self.qmp is not None:
                self.qmp.close()
                self.qmp = None
            self.close_block()


class Cluster:
    def __init__(self, runtime, output, token, nodes, memory, fault_disks=False):
        self.runtime, self.output, self.token, self.memory = runtime, output, token, memory
        self.fault_disks = fault_disks
        self.hub = Hub()
        self.vms = {f'n{i}': VM(self, f'n{i}') for i in range(1, nodes + 1)}
        self.events = []
        self.damages = []
        self.case = None
        self.case_lock = threading.Lock()
        self.proof_path = output / 'convergence.json'
        self.requests = {}
        self.closed_requests = set()
        self.request_lock = threading.Lock()
        self.io_faults = []
        self.network_faults = []

    def power_cut(self, nodes, active=False, members=None, request_kind=None, prearmed=None):
        nodes = list(nodes)
        assert nodes and len(set(nodes)) == len(nodes) and all(n in self.vms for n in nodes)
        event = {'nodes': nodes, 'verified': {}, 'all_paused_before_kill': False, 'signal': 'SIGKILL'}
        self.events.append(event)
        def arm(node):
            vm = self.vms[node]
            with vm.lock:
                assert vm.process is not None and vm.process.poll() is None
                marker = uuid.uuid4().hex[:len('stable-before-unsynced-overwrite')]
                payload = {'pause': False, 'member': members[node]} if active else {}
                result = (prearmed or {}).get(node) or vm.call('arm-power', marker=marker, **payload)
                assert result['armed']
                if active:
                    assert result['server_not_stopped'], 'active cut stopped voter threads first'
                vm.pending = result, event
                if active:
                    # QMP stop drains block I/O and would wait forever for the
                    # deliberately held request. Stop QEMU's host threads
                    # instead, without draining or completing guest storage.
                    vm.process.send_signal(signal.SIGSTOP)
                    until = time.monotonic() + 5
                    while True:
                        pid, status = os.waitpid(vm.process.pid, os.WNOHANG | os.WUNTRACED)
                        if pid:
                            assert os.WIFSTOPPED(status)
                            break
                        assert time.monotonic() < until, 'QEMU did not stop at active I/O boundary'
                        time.sleep(.001)
                else:
                    vm.qmp.command('stop')
                    assert vm.qmp.command('query-status')['status'] == 'paused'
                return node, result
        arms = dict(parallel(arm, nodes))
        event['old_boot_ids'] = {n: a['boot_id'] for n, a in arms.items()}
        event['old_members'] = {n: a['member'] for n, a in arms.items()}
        event['all_paused_before_kill'] = True
        event['voter_threads_paused'] = not active
        event['pause_mechanism'] = 'host-SIGSTOP-without-I/O-drain' if active else 'QMP-stop'
        if getattr(self, 'hub', None) is not None:
            event['network'] = self.hub.snapshot()
        if request_kind is not None:
            event['requests_at_cut'] = self.pending_requests(request_kind)
        dispatched = []
        # Do not wait for any target's exit until every SIGKILL has been sent.
        for node in nodes:
            self.vms[node].process.kill()
            dispatched.append(time.monotonic_ns())
        event['dispatch_skew_ms'] = (max(dispatched) - min(dispatched)) / 1_000_000
        assert event['dispatch_skew_ms'] < 250, 'group cut was not simultaneous enough'
        for node in nodes:
            vm = self.vms[node]
            vm.process.wait(timeout=10)
            vm.qmp.close()
            vm.qmp = None
            vm.close_block()
        self.save_events()
        return {'guest_killed': True, 'event': event}

    def save_events(self):
        (self.output / 'power-events.json').write_text(json.dumps({'cuts': self.events, 'damage': self.damages,
                                                                'io_faults': self.io_faults,
                                                                'network_faults': getattr(self, 'network_faults', [])}, indent=2))

    def pending_requests(self, kind):
        with self.request_lock:
            return [dict(r) for r in self.requests.values() if r['kind'] == kind]

    def healthy(self):
        deadline = time.monotonic() + 45
        while True:
            try:
                leaders = parallel(lambda vm: (vm.node, vm.call('leader')['leader']), self.vms.values())
                leader = next(node for node, serving in leaders if serving)
                replicas = dict(zip(self.vms, parallel(lambda vm: vm.call('inspect'), self.vms.values())))
                client = Client(('127.0.0.1', self.vms[leader].client_port), timeout=3)
                try:
                    membership = client.request('$JS.API.SCREECH.CLUSTER.STATUS', {})
                finally:
                    client.close()
                assert {r['member'] for r in replicas.values()} == set(membership['voters'])
                assert len(membership['voters']) == len(self.vms)
                assert not membership['outgoing'] and not membership['learners'] and membership['replacement'] is None
                return leader, replicas, membership
            except (OSError, EOFError, AssertionError, RuntimeError, KeyError, StopIteration):
                assert time.monotonic() < deadline, 'no fully admitted durable quorum before active fault'
                time.sleep(.2)

    def active_fault(self, mode, operation, kind):
        assert self.fault_disks
        leader, replicas, membership = self.healthy()
        # Target only a minority, including the actual service leader. The other
        # kernels and durable disks stay running throughout the fault and cut.
        targets = [leader] + [n for n in self.vms if n != leader][:len(self.vms)//2-1]
        members = {n: replicas[n]['member'] for n in targets}
        extents = self.vms[leader].call('extents')['extents']
        trace = self.vms[leader].call('trace-sync') if mode == 'eio' else None
        block = self.vms[leader].block
        block.arm(mode, operation, lambda: self.pending_requests(kind), extents)
        assert block.hit.wait(45), f'no real {operation} while {kind} was in flight'
        proof = dict(block.proof, leader=leader, targets=targets, request_kind=kind,
                     surviving_nodes=[n for n in self.vms if n not in targets],
                     membership_before=membership, replicas_before=replicas)
        prearmed = {}
        if mode == 'eio':
            prearmed[leader] = self.vms[leader].call('arm-power', pause=False, member=members[leader],
                                                   marker=uuid.uuid4().hex[:len('stable-before-unsynced-overwrite')])
            assert prearmed[leader]['armed'] and prearmed[leader]['server_not_stopped']
            proof['error_after_canary'] = True
            block.error_release.set()
            assert block.replied.wait(3), 'EIO reply was not delivered to QEMU'
            proof['reply_sent'] = block.proof['reply_sent']
            proof['subsequent_io_held'] = block.proof['subsequent_io_held']
            deadline = time.monotonic() + 5
            while True:
                observed = self.vms[leader].call('io-errors')
                assert observed['boot_id'] == trace['boot_id'] and observed['pid'] == trace['pid']
                proof['guest_errors'] = observed['errors']
                proof['guest_sync_errors'] = observed['sync_errors']
                proof['sync_trace'] = trace
                if proof['guest_errors'] or proof['guest_sync_errors']:
                    break
                assert time.monotonic() < deadline, 'guest never reported the injected disk I/O error'
                time.sleep(.02)
        cut = self.power_cut(targets, active=True, members=members, request_kind=kind, prearmed=prearmed)['event']
        proof['requests_at_cut'] = cut['requests_at_cut']
        # Check after stopping QEMU, not before the guest canary RPC. An EIO
        # may instead complete the API with an error before the subsequent cut.
        if mode != 'eio':
            assert {r['id'] for r in proof['requests']} & {r['id'] for r in proof['requests_at_cut']}, 'request completed before cut'
        proof['cut_index'] = self.events.index(cut)
        if mode == 'tear':
            with self.vms[leader].disk.open('rb') as file:
                file.seek(proof['offset'])
                proof['cold_sha256'] = hashlib.sha256(file.read(proof['length'])).hexdigest()
            assert proof['cold_sha256'] == proof['expected_sha256'], 'torn sectors changed before recovery'
        self.io_faults.append(proof)
        self.save_events()
        return proof

    def heal(self):
        if self.hub.blocked:
            self.hub.partition([])
        parallel(lambda vm: vm.start(), self.vms.values())
        self.save_events()
        return {'restarted': list(self.vms)}

    def damage(self, kind):
        targets = list(self.vms)[:len(self.vms) // 2]
        cut = self.power_cut(self.vms)['event']
        def inject(node):
            vm = self.vms[node]
            vm.start(maintenance=True)  # Mount/replay the crashed filesystem, never start Screech.
            if kind == 'blank':
                vm.stop()
                archive_path = vm.directory / f'replaced-{len(vm.boots):03}-{len(self.damages):03}.raw'
                assert not archive_path.exists()
                vm.disk.rename(archive_path)
                vm.new_disk()
                result = {'injected': 'blank', 'archived_disk': str(archive_path), 'voter_started': False}
            else:
                result = vm.call('damage', kind=kind)
                vm.stop()
            proof = dict(result, node=node, old_member=cut['old_members'][node])
            assert proof['old_member'] is not None
            self.damages.append(proof)
            return proof
        result = parallel(inject, targets)
        self.save_events()
        return {'power_cut': cut, 'damaged': result}

    def verify(self):
        deadline = time.monotonic() + 90
        last = None
        while time.monotonic() < deadline:
            try:
                replicas = dict(zip(self.vms, parallel(lambda vm: vm.call('inspect'), self.vms.values())))
                client = Client(('127.0.0.1', self.vms['n1'].client_port), timeout=3)
                try:
                    status = client.request('$JS.API.SCREECH.CLUSTER.STATUS', {})
                finally:
                    client.close()
                members = {row['member'] for row in replicas.values()}
                ids = {node: row['boot_id'] for node, row in replicas.items()}
                assert len(members) == len(self.vms) and members == set(status['voters'])
                assert len(set(ids.values())) == len(self.vms), 'voters share a kernel'
                assert not status['outgoing'] and not status['learners'] and status['replacement'] is None
                positions = {(r['app_sequence'], r['view']) for r in replicas.values()}
                assert len(positions) == 1 and next(iter(positions))[0] == status['event_sequence'] > 0
                assert all(set(e['verified']) == set(e['nodes']) for e in self.events)
                for proof in self.damages:
                    assert replicas[proof['node']]['member'] != proof['old_member'], 'damaged voter identity was reused'
                canaries = {node: {'verified': bool(vm.verified), 'boots': len(vm.boots),
                                   'checks': vm.verified} for node, vm in self.vms.items()}
                assert all(c['verified'] for c in canaries.values()), 'no real power-loss control for a voter'
                result = {'converged': True, 'replicas': replicas, 'cluster': status,
                          'independent_vms': True, 'boot_ids': ids, 'power_canaries': canaries,
                          'power_events': self.events, 'disk_damage': self.damages}
                result['io_faults'] = self.io_faults
                result['network_faults'] = self.network_faults
                assert not self.hub.blocked
                # Jepsen kills guests before snarfing logs. Preserve the proof on
                # the controller too: those final guest writes may be unsynced.
                self.proof_path.write_text(json.dumps(result, indent=2))
                for node, vm in self.vms.items():
                    vm.call('write', path=f'/data/screech-jepsen/{node}/convergence.json',
                            data=base64.b64encode(json.dumps(result, indent=2).encode()).decode())
                    vm.call('write', path=f'/data/screech-jepsen/{node}/power-canary.json',
                            data=base64.b64encode(json.dumps(canaries[node], indent=2).encode()).decode())
                self.save_events()
                return result
            except (OSError, EOFError, AssertionError, RuntimeError, KeyError, TypeError) as error:
                last = repr(error)
                time.sleep(.2)
        raise TimeoutError('Independent voters did not converge: ' + str(last))

    def dispatch(self, request):
        op, node = request['op'], request.get('node')
        if op == 'status':
            return {'isolated-vm': True, 'independent_vms': True, 'nodes': list(self.vms)}
        if op == 'power-cut':
            return self.power_cut(request['nodes'])
        if op == 'power-damage':
            return self.damage(request['kind'])
        if op == 'mixed-fault':
            try:
                return mixed_fault(self, request['topology'], request['damage'])
            finally:
                self.heal()
        if op == 'active-fault':
            try:
                return self.active_fault(request['mode'], request['operation'], request['kind'])
            except Exception:
                # A failed match must not leave an armed or held device behind
                # to stall an unrelated future request after the RPC has failed.
                def cancel(vm):
                    if vm.block is not None and vm.block.disarm():
                        vm.stop()
                parallel(cancel, self.vms.values())
                self.heal()
                raise
        if op == 'request-window':
            with self.request_lock:
                if request['phase'] == 'begin':
                    # Completion can race the begin RPC on another HTTP thread.
                    if request['id'] not in self.closed_requests:
                        self.requests[request['id']] = {k: request[k] for k in ('id', 'kind', 'subject', 'node')}
                else:
                    self.requests.pop(request['id'], None)
                    self.closed_requests.add(request['id'])
            return {'recorded': True}
        if op == 'heal':
            return self.heal()
        if op == 'verify':
            return self.verify()
        if op == 'preflight':
            self.power_cut(self.vms)
            self.heal()
            return self.dispatch({'op': 'ready'})
        if op == 'ready':
            deadline = time.monotonic() + 90
            for vm in self.vms.values():
                while True:
                    try:
                        client = Client(('127.0.0.1', vm.client_port), timeout=2)
                        client.close()
                        break
                    except (OSError, EOFError, AssertionError):
                        if time.monotonic() > deadline:
                            raise TimeoutError('No serving gateway on ' + vm.node)
                        time.sleep(.1)
            return {'ready': True}
        assert node in self.vms
        vm = self.vms[node]
        if op == 'setup':
            with self.case_lock:
                cluster_id = request['config']['cluster_id']
                if cluster_id != self.case:
                    self.case = cluster_id
                    self.events, self.damages, self.io_faults = [], [], []
                    self.network_faults = []
                    self.requests = {}
                    self.closed_requests = set()
            vm.stop()
            vm.verified, vm.pending = [], None
            vm.new_disk()
            (vm.directory / 'bootshare/config.json').write_text(json.dumps(request['config']))
            vm.start()
            return vm.call('setup')
        if op == 'exec' and (vm.process is None or vm.process.poll() is not None):
            vm.start(maintenance=True)
        if op == 'teardown':
            vm.stop()
            return {'stopped': True}
        if op == 'kill':
            return self.power_cut([node])
        if op == 'start':
            vm.start()
            self.save_events()
            return {'started': True}
        if op in ('pause', 'resume'):
            if op == 'pause' or vm.qmp.command('query-status')['status'] == 'paused':
                vm.qmp.command('stop' if op == 'pause' else 'cont')
            return {op: True}
        return vm.call(op, **{k: v for k, v in request.items() if k not in ('op', 'node')})

    def close(self):
        parallel(lambda vm: vm.stop(), self.vms.values())
        parallel(lambda vm: vm.client_proxy.close(), self.vms.values())
        self.hub.close()
        self.save_events()
        (self.output / 'boots.json').write_text(json.dumps({n: vm.boots for n, vm in self.vms.items()}, indent=2))


def prepare_cluster(args, runtime, output):
    common = output / 'common'
    common.mkdir()
    prepare(args, runtime, common)
    root = common / 'initramfs'
    for name in ['artifacts', 'bootshare']:
        (root / name).mkdir()
    original = (root / 'init').read_text()
    prefix = original[:original.index('mount -t 9p')]
    init = prefix + f'''mount -t 9p -o trans=virtio,version=9p2000.L,ro nix-store /nix/store || exec sh
mount -t 9p -o trans=virtio,version=9p2000.L,ro workspace {REPO} || exec sh
mount -t 9p -o trans=virtio,version=9p2000.L,ro artifacts /artifacts || exec sh
mount -t 9p -o trans=virtio,version=9p2000.L,ro bootshare /bootshare || exec sh
mount -t ext4 -o commit=600 /dev/vda /data || exec sh
ln -sf {runtime['bash']} /bin/bash
ln -sf {runtime['python']} /bin/python3
export PATH={runtime['tools']}/bin:/bin:/sbin
export TOKIO_WORKER_THREADS=2
# Delay background writeback; explicit fsync/flush behavior remains required.
echo 0 >/proc/sys/vm/dirty_writeback_centisecs
echo 360000 >/proc/sys/vm/dirty_expire_centisecs
echo 70 >/proc/sys/vm/dirty_background_ratio
echo 80 >/proc/sys/vm/dirty_ratio
node=n1
maintenance=0
fresh=0
for word in $(cat /proc/cmdline); do
  case "$word" in
    screech.node=*) node=${{word#*=}};;
    screech.maintenance=*) maintenance=${{word#*=}};;
    screech.fresh=*) fresh=${{word#*=}};;
  esac
done
ip link set lo up
ip address add 10.0.2.15/24 dev eth0
ip link set eth0 up
ip route add default via 10.0.2.2
ip address add 10.0.1.${{node#n}}/24 dev eth1
ip link set eth1 up
for i in 1 2 3 4 5; do
  ip neigh replace 10.0.1.$i lladdr 02:00:00:00:01:0$i nud permanent dev eth1
done
touch /run/screech-voter-vm
if [ "$fresh" = 1 ]; then fresh_arg=--fresh-disk; else fresh_arg=; fi
if [ "$maintenance" = 1 ]; then
  python3 {REPO}/tools/jepsen/voter_guest.py --node "$node" --maintenance $fresh_arg
else
  python3 {REPO}/tools/jepsen/voter_guest.py --node "$node" $fresh_arg
fi
poweroff -f
'''
    (root / 'init').write_text(init)
    archive(root, common / 'initramfs.gz')
    (common / 'disk.raw').unlink()


def run(args):
    runtime = json.loads(args.runtime.read_text())
    output = args.output.resolve()
    output.mkdir(parents=True, exist_ok=False)
    (output / 'specs').mkdir()
    shutil.copy2(args.binary, output / 'screech-server')
    token = secrets.token_hex(32)
    token_path = output / 'control-token'
    token_path.write_text(token)
    token_path.chmod(0o600)
    prepare_cluster(args, runtime, output)
    active_scenarios = {'vm-active-write', 'vm-active-flush', 'vm-torn', 'vm-eio-write', 'vm-eio-flush'}
    mixed_scenarios = {f'vm-{network}-{damage}' for network in ('split', 'send', 'receive') for damage in ('bitflip', 'wipe')}
    fault_disks = args.fault_disks or bool((active_scenarios | mixed_scenarios) & set(args.scenarios.split(',')))
    cluster = Cluster(runtime, output, token, args.nodes, args.memory, fault_disks)
    class Coordinator(Control):
        def dispatch(self, request):
            return cluster.dispatch(request)
    server = ThreadingHTTPServer(('127.0.0.1', 0), Coordinator)
    server.token = token
    threading.Thread(target=server.serve_forever, daemon=True).start()
    endpoint = f'http://127.0.0.1:{server.server_address[1]}/'
    workloads, scenarios = args.workloads.split(','), args.scenarios.split(',')
    for index, (workload, scenario) in enumerate((w, s) for w in workloads for s in scenarios):
        spec = {'workload': workload, 'scenario': scenario, 'topology': 'independent-vms',
                'node-count': args.nodes, 'concurrency': args.nodes * (2 if workload == 'queue' else 1),
                'rate': args.rate, 'time-limit': args.time_limit, 'final-time-limit': args.final_time_limit,
                'active-io': scenario in active_scenarios,
                'mixed-faults': scenario in mixed_scenarios,
                'binary': '/artifacts/screech-server', 'control-endpoint': endpoint,
                'client-addresses': {n: f'nats://127.0.0.1:{vm.client_port}' for n, vm in cluster.vms.items()}}
        (output / 'specs' / f'{index:02}-{workload}-{scenario}.json').write_text(json.dumps(spec))
    metadata = dict(runtime, screech_commit=subprocess.check_output(['git', 'rev-parse', 'HEAD'], text=True).strip(),
                    binary_sha256=hashlib.sha256((output / 'screech-server').read_bytes()).hexdigest(),
                    topology='one QEMU process/kernel/native ext4 disk per voter', memory_mib_per_voter=args.memory,
                    cpus_per_voter=2, disk_cache='none', acceleration='tcg', upstream=json.loads((REPO / 'tools/jepsen/upstream.json').read_text()))
    if fault_disks:
        metadata['disk_cache'] = 'NBD volatile cache; FLUSH/FUA fsync before reply; power loss discards pending writes'
    (output / 'runtime.json').write_text(json.dumps(metadata, indent=2))
    environment = dict(os.environ, SCREECH_JEPSEN_CONTROL_TOKEN=token, PATH=runtime['tools'] + '/bin:' + os.environ['PATH'])
    classpath = (REPO / 'results/jepsen-runtime/classpath.txt').read_text().strip()
    exits, error = [], None
    deadline = time.monotonic() + args.timeout
    try:
        for spec in sorted((output / 'specs').glob('*.json')):
            directory = output / 'runs' / spec.stem
            directory.mkdir(parents=True)
            cluster.proof_path = directory / 'convergence.json'
            print('Jepsen independent VMs:', spec.stem, flush=True)
            with (directory / 'controller.log').open('wb') as log:
                result = subprocess.run([runtime['java'], '-Xmx2g', '-Djava.awt.headless=true', '-cp', classpath,
                                         'clojure.main', '-m', 'jepsen.screech.cli', str(spec)], cwd=directory,
                                        env=environment, stdout=log, stderr=subprocess.STDOUT,
                                        timeout=max(1, deadline - time.monotonic()))
            exits.append(result.returncode)
            if result.returncode:
                print((directory / 'controller.log').read_text(errors='replace')[-12000:])
                break
    except (Exception, KeyboardInterrupt) as failure:
        error = str(failure)
    finally:
        cluster.close()
        server.shutdown()
        server.server_close()
        token_path.unlink(missing_ok=True)
    summaries = [json.loads(path.read_text()) for path in sorted((output / 'runs').glob('*/summary.json'))]
    valid = error is None and len(summaries) == len(workloads) * len(scenarios) and all(s['valid?'] is True for s in summaries) and all(x == 0 for x in exits)
    result = {'passed': valid, 'scenarios': summaries, 'runtime': metadata, 'controller_exits': exits, 'error': error,
              'scope': 'Independent guest OS power loss on one physical host; host/controller power loss is not simulated'}
    (output / 'results.json').write_text(json.dumps(result, indent=2))
    print(json.dumps({'passed': valid, 'completed': len(summaries), 'error': error, 'results': str(output / 'results.json')}, indent=2))
    return 0 if valid else 1


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--runtime', type=Path, required=True)
    parser.add_argument('--output', type=Path, required=True)
    parser.add_argument('--binary', type=Path, default=REPO / 'target/debug/screech-server')
    parser.add_argument('--nodes', type=int, choices=[3, 5], default=3)
    parser.add_argument('--workloads', default='consumer-lifecycle')
    parser.add_argument('--scenarios', default='baseline,vm-power,vm-damage,vm-wipe,vm-blank')
    parser.add_argument('--rate', type=int, default=1)
    parser.add_argument('--time-limit', type=int, default=60)
    parser.add_argument('--final-time-limit', type=int, default=180)
    parser.add_argument('--memory', type=int, default=1024)
    parser.add_argument('--fault-disks', action='store_true', help='Use volatile NBD devices even without an active-I/O scenario')
    parser.add_argument('--timeout', type=int, default=3600)
    raise SystemExit(run(parser.parse_args()))
