#!/usr/bin/env python3
"""Run real Jepsen/LazyFS in a disposable root-capable VM; no host FUSE/root needed."""
import argparse
import hashlib
import json
import lzma
import platform
import os
from pathlib import Path
import shlex
import secrets
import shutil
import socket
import urllib.request
import subprocess
import sys
import time

REPO = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO / 'scripts'))
from vm_crash_test import archive, copy_file


def prepare(args, runtime, output):
    root = output / 'initramfs'
    root.mkdir()
    for name in ['dev', 'proc', 'sys', 'run', 'tmp', 'bin', 'sbin', 'etc', 'var', 'data', 'results', 'modules', 'nix/store']:
        (root / name).mkdir(parents=True, exist_ok=True)
    workspace = root / str(REPO).lstrip('/')
    workspace.mkdir(parents=True)
    copy_file(root, runtime['busybox'], 'bin/busybox')
    modules = []
    mandatory = ['virtio_pci', 'virtio_blk', 'ext4', '9pnet_virtio', '9p', 'fuse', 'veth', 'bridge', 'e1000']
    optional = ['ip_tables', 'iptable_filter', 'iptable_raw', 'nf_tables', 'nft_compat', 'nft_chain_nat', 'nft_counter', 'nft_reject', 'nft_reject_ipv4', 'sch_netem', 'xt_tcpudp', 'xt_mark', 'nfnetlink', 'ip6_tables']
    release = next((Path(runtime['modules']) / 'lib/modules').iterdir()).name
    for name in mandatory + optional:
        deps = subprocess.run([runtime['modprobe'], '-d', runtime['modules'], '-S', release, '--show-depends', name], text=True, capture_output=True)
        if deps.returncode and name in mandatory:
            raise RuntimeError(deps.stderr)
        for line in deps.stdout.splitlines():
            if line.startswith('insmod '):
                path = Path(line.split()[1])
                if path not in modules:
                    modules.append(path)
    for i, path in enumerate(modules):
        content = path.read_bytes()
        if path.suffix == '.xz':
            content = lzma.decompress(content)
        elif path.suffix == '.zst':
            content = subprocess.check_output(['zstd', '-dc', str(path)])
        (root / f'modules/{i:03}.ko').write_bytes(content)
    (root / 'etc/passwd').write_text('root:x:0:0:root:/root:/bin/bash\n')
    (root / 'etc/group').write_text('root:x:0:\n')
    (root / 'etc/nsswitch.conf').write_text('hosts: files dns\npasswd: files\ngroup: files\n')
    (root / 'etc/hosts').write_text('127.0.0.1 localhost\n' + ''.join(f'10.0.1.{i} n{i}\n' for i in range(1, 6)))
    (root / 'etc/fuse.conf').write_text('user_allow_other\n')
    # Only inside this root-owned disposable guest; supports Jepsen's c/su wrapper.
    (root / 'bin/sudo').write_text('''#!/bin/busybox sh
while [ "$#" -gt 0 ]; do
  case "$1" in
    -k|-S) shift;;
    -u) [ "$2" = root ] || exit 90; shift 2;;
    --) shift; break;;
    *) break;;
  esac
done
exec "$@"
''')
    (root / 'bin/sudo').chmod(0o755)
    load = '\n'.join(f'/bin/busybox insmod /modules/{i:03}.ko' for i in range(len(modules)))
    init = f'''#!/bin/busybox sh
/bin/busybox --install -s /bin
mount -t proc proc /proc
mount -t sysfs sysfs /sys
mount -t devtmpfs devtmpfs /dev
mount -t tmpfs tmpfs /run
mount -t tmpfs tmpfs /tmp
mkdir -p /root
ln -s /run /var/run
{load}
mount -t 9p -o trans=virtio,version=9p2000.L,ro nix-store /nix/store || exec sh
mount -t 9p -o trans=virtio,version=9p2000.L,ro workspace {shlex.quote(str(REPO))} || exec sh
mount -t 9p -o trans=virtio,version=9p2000.L results /results || exec sh
mount -t ext4 /dev/vda /data || exec sh
ln -sf {shlex.quote(runtime['bash'])} /bin/bash
ln -sf {shlex.quote(runtime['python'])} /bin/python3
ln -sf {shlex.quote(runtime['tools'] + '/bin/tc')} /sbin/tc
export PATH={shlex.quote(runtime['tools'] + '/bin')}:/bin:/sbin
export HOME=/root
export TMPDIR=/tmp
export LANG=C
export TOKIO_WORKER_THREADS=2
touch /run/screech-jepsen-vm
ip link set lo up
ip address add 10.0.2.15/24 dev eth0
ip link set eth0 up
ip route add default via 10.0.2.2
ip link add br-jepsen type bridge
ip address add 10.0.1.254/24 dev br-jepsen
ip link set br-jepsen up
for i in 1 2 3 4 5; do
  ip netns add n$i
  ip link add host$i type veth peer name eth0 netns n$i
  ip link set host$i master br-jepsen
  ip link set host$i up
  ip netns exec n$i ip link set lo up
  ip netns exec n$i ip address add 10.0.1.$i/24 dev eth0
  ip netns exec n$i ip link set eth0 up
done
echo SCREECH_JEPSEN_NETWORK_READY
python3 {shlex.quote(str(REPO / 'tools/jepsen/guest.py'))} --token-file /results/control-token
poweroff -f
'''
    (root / 'init').write_text(init)
    (root / 'init').chmod(0o755)
    archive(root, output / 'initramfs.gz')
    with (output / 'disk.raw').open('wb') as disk:
        disk.truncate(2 * 1024 ** 3)
    subprocess.run([runtime['mkfs'], '-q', '-t', 'ext4', '-F', str(output / 'disk.raw')], check=True)


def run(args):
    runtime = json.loads(args.runtime.read_text())
    output = args.output.resolve()
    output.mkdir(parents=True, exist_ok=False)
    (output / 'specs').mkdir()
    shutil.copy2(args.binary, output / 'screech-server')
    scenarios = args.scenarios.split(',')
    workloads = args.workloads.split(',')
    cases = [(workload, scenario) for workload in workloads for scenario in scenarios]
    # Keep ports reserved until QEMU is ready to bind them.
    reservations = [socket.socket() for _ in range(6)]
    for reservation in reservations:
        reservation.bind(('127.0.0.1', 0))
    ports = [reservation.getsockname()[1] for reservation in reservations]
    endpoint = f'http://127.0.0.1:{ports[0]}/'
    token = secrets.token_hex(32)
    token_file = output / 'control-token'
    token_file.write_text(token)
    token_file.chmod(0o600)
    for index, (workload, scenario) in enumerate(cases):
        spec = {'scenario': scenario, 'workload': workload, 'node-count': args.nodes,
                'concurrency': args.nodes * (2 if workload == 'queue' else 1),
                'rate': args.rate, 'time-limit': args.time_limit, 'final-time-limit': args.final_time_limit,
                'project': str(REPO / 'tools/jepsen'), 'binary': '/results/screech-server',
                'lazyfs-binary': str((REPO / 'upstream/lazyfs/lazyfs/build/lazyfs').resolve()),
                'control-endpoint': endpoint,
                'client-addresses': {f'n{i}': f'nats://127.0.0.1:{ports[i]}' for i in range(1, args.nodes + 1)}}
        (output / 'specs' / f'{index:02}-{workload}-{scenario}.json').write_text(json.dumps(spec))
    runtime['upstream'] = json.loads((REPO / 'tools/jepsen/upstream.json').read_text())
    with (output / 'screech-server').open('rb') as binary:
        runtime['binary_sha256'] = hashlib.file_digest(binary, 'sha256').hexdigest()
    runtime['host'] = platform.platform()
    runtime['guest'] = {'acceleration': 'tcg', 'cpus': 4, 'memory_mib': 4096, 'disk_cache': 'none'}
    runtime['screech_commit'] = subprocess.check_output(['git', 'rev-parse', 'HEAD'], cwd=REPO, text=True).strip()
    (output / 'runtime.json').write_text(json.dumps(runtime, indent=2))
    prepare(args, runtime, output)
    forwards = ','.join(f'hostfwd=tcp:127.0.0.1:{port}-:{guest}' for port, guest in zip(ports, [9000, 5001, 5002, 5003, 5004, 5005]))
    command = [runtime['tools'] + '/bin/qemu-system-x86_64', '-accel', 'tcg,thread=multi', '-m', '4096', '-smp', '4',
               '-display', 'none', '-serial', 'stdio', '-monitor', 'none', '-no-reboot',
               '-netdev', f'user,id=net0,{forwards}', '-device', 'e1000,netdev=net0',
               '-kernel', runtime['kernel'], '-initrd', str(output / 'initramfs.gz'),
               '-append', 'console=ttyS0 rdinit=/init panic=-1 quiet',
               '-drive', f'file={output}/disk.raw,format=raw,if=virtio,cache=none',
               '-virtfs', 'local,path=/nix/store,mount_tag=nix-store,security_model=none,readonly=on',
               '-virtfs', f'local,path={REPO},mount_tag=workspace,security_model=none,readonly=on',
               '-virtfs', f'local,path={output},mount_tag=results,security_model=none']
    print('Starting isolated Jepsen VM:', output, flush=True)
    completed = []
    error = None
    environment = dict(os.environ, SCREECH_JEPSEN_CONTROL_TOKEN=token,
                       PATH=runtime['tools'] + '/bin:' + os.environ['PATH'])
    classpath = (REPO / 'results/jepsen-runtime/classpath.txt').read_text().strip()
    with (output / 'guest.log').open('wb') as log:
        for reservation in reservations:
            reservation.close()
        process = subprocess.Popen(command, stdin=subprocess.DEVNULL, stdout=log, stderr=subprocess.STDOUT)
        deadline = time.monotonic() + args.timeout
        try:
            boot_deadline = time.monotonic() + 90
            while True:
                try:
                    request = urllib.request.Request(endpoint, data=b'{"op":"status"}', headers={'Authorization': 'Bearer ' + token})
                    with urllib.request.urlopen(request, timeout=3) as response:
                        assert json.load(response)['isolated-vm']
                    break
                except (OSError, AssertionError):
                    if time.monotonic() > boot_deadline or process.poll() is not None:
                        raise RuntimeError('Guest control did not become ready')
                    time.sleep(.5)
            for spec in sorted((output / 'specs').glob('*.json')):
                directory = output / 'runs' / spec.stem
                directory.mkdir(parents=True)
                print('Jepsen:', spec.stem, flush=True)
                with (directory / 'controller.log').open('wb') as controller:
                    result = subprocess.run([runtime['java'], '-Xmx2g', '-Djava.awt.headless=true', '-cp', classpath,
                                             'clojure.main', '-m', 'jepsen.screech.cli', str(spec)],
                                            cwd=directory, env=environment, stdout=controller, stderr=subprocess.STDOUT,
                                            timeout=max(1, deadline - time.monotonic()))
                completed.append(result.returncode)
                if result.returncode:
                    print((directory / 'controller.log').read_text(errors='replace')[-14000:])
                    break
        except (Exception, KeyboardInterrupt) as failure:
            error = str(failure)
        finally:
            process.kill()
            process.wait(timeout=10)
            token_file.unlink(missing_ok=True)
    summaries = [json.loads(path.read_text()) for path in sorted((output / 'runs').glob('*/summary.json'))]
    valid = error is None and len(summaries) == len(cases) and all(row['valid?'] is True for row in summaries) and all(code == 0 for code in completed)
    result = {'passed': valid, 'scenarios': summaries, 'runtime': runtime, 'controller_exits': completed, 'error': error,
              'scope': 'Real Jepsen host controller, real LazyFS and voters in separate network namespaces on one isolated Linux kernel; TCG; no host/controller failure simulation'}
    (output / 'results.json').write_text(json.dumps(result, indent=2))
    print(json.dumps({'passed': valid, 'completed': len(summaries), 'error': error, 'results': str(output / 'results.json')}, indent=2))
    if not valid:
        print((output / 'guest.log').read_text(errors='replace')[-14000:])
    return 0 if valid else 1


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--runtime', type=Path, required=True)
    parser.add_argument('--output', type=Path, required=True)
    parser.add_argument('--binary', type=Path, default=REPO / 'target/debug/screech-server')
    parser.add_argument('--workloads', default='queue', help='Comma-separated queue,consumer-lifecycle workloads')
    parser.add_argument('--scenarios', default='baseline,power,pause-kill,partition,corrupt,wipe')
    parser.add_argument('--nodes', type=int, choices=[3, 5], default=5)
    parser.add_argument('--rate', type=int, default=30)
    parser.add_argument('--time-limit', type=int, default=30)
    parser.add_argument('--final-time-limit', type=int, default=60)
    parser.add_argument('--timeout', type=int, default=1800)
    sys.exit(run(parser.parse_args()))
