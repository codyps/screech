#!/usr/bin/env python3
"""Boot a disposable Linux guest, SIGKILL QEMU, boot the same disk, verify ACKs.
No root/KVM required: uses TCG. Requires Linux kernel + matching modules, static
BusyBox, QEMU with user networking, modprobe and mkfs.ext4 on the host.
The guest's RAM/page cache is discarded; host storage must honor flushes.
"""
import concurrent.futures
import argparse
import gzip
import json
import lzma
import os
from pathlib import Path
import re
import shutil
import socket
import stat
import subprocess
import time
from wire import Client

def copy_file(root, source, destination=None):
    source=Path(source)
    target=root/(destination or str(source).lstrip('/'))
    target.parent.mkdir(parents=True,exist_ok=True)
    shutil.copy2(source,target,follow_symlinks=True)

def archive(root, destination):
    """newc archive including device nodes without requiring host mknod."""
    data=bytearray();ino=0
    def entry(name, mode, content=b'', rmajor=0, rminor=0):
        nonlocal ino
        ino+=1;n=name.encode()+b'\0'
        fields=[ino,mode,0,0,1,0,len(content),0,0,rmajor,rminor,len(n),0]
        data.extend(b'070701'+''.join(f'{x:08x}' for x in fields).encode());data.extend(n)
        data.extend(b'\0'*(-len(data)%4));data.extend(content);data.extend(b'\0'*(-len(data)%4))
    for path in sorted(root.rglob('*')):
        name=str(path.relative_to(root));mode=path.stat().st_mode
        if path.is_dir():entry(name,stat.S_IFDIR|0o755)
        else:entry(name,stat.S_IFREG|(mode&0o777),path.read_bytes())
    entry('dev/console',stat.S_IFCHR|0o600,rmajor=5,rminor=1)
    entry('dev/null',stat.S_IFCHR|0o666,rmajor=1,rminor=3)
    entry('TRAILER!!!',0)
    with gzip.open(destination,'wb') as f:f.write(data)

def prepare(args, work):
    root=work/'root';root.mkdir()
    for name in ('dev','proc','sys','data','tmp','bin','modules','etc'): (root/name).mkdir()
    (root/'tmp').chmod(0o1777)
    copy_file(root,args.busybox,'bin/busybox');copy_file(root,args.binary,'bin/screech-server')
    dependencies=subprocess.check_output(['ldd',args.binary],text=True)
    for path in set(re.findall(r'/[^\s()]+',dependencies)):copy_file(root,path)
    release_dirs=list((Path(args.modules)/'lib/modules').iterdir())
    if args.kernel_release:
        release=args.kernel_release
    else:
        assert len(release_dirs)==1, "Use --kernel-release when modules contains multiple releases"
        release=release_dirs[0].name
    ordered=[]
    for name in ('virtio_pci','virtio_blk','ext4','e1000'):
        deps=subprocess.check_output([args.modprobe,'-d',args.modules,'-S',release,'--show-depends',name],text=True)
        for line in deps.splitlines():
            if not line.startswith('insmod '):continue
            path=Path(line.split()[1])
            if path not in ordered:ordered.append(path)
    load=[]
    for i,path in enumerate(ordered):
        name=f'{i:02d}.ko'
        content=(lzma.decompress(path.read_bytes()) if path.suffix=='.xz' else subprocess.check_output(['zstd','-dc',str(path)]) if path.suffix=='.zst' else path.read_bytes())
        (root/'modules'/name).write_bytes(content);load.append('/bin/busybox insmod /modules/'+name)
    init='''#!/bin/busybox sh
/bin/busybox --install -s /bin
mount -t proc proc /proc
mount -t sysfs sysfs /sys
mount -t devtmpfs devtmpfs /dev
mount -t tmpfs tmpfs /tmp
'''+ '\n'.join(load)+'''
ip link set lo up
ip link set eth0 up
ip addr add 10.0.2.15/24 dev eth0
ip route add default via 10.0.2.2
for i in 1 2 3 4 5; do [ -b /dev/vda ] && break; sleep 1; done
mount -t ext4 /dev/vda /data || exec sh
export TOKIO_WORKER_THREADS=2
exec /bin/screech-server --addr 0.0.0.0 --port 4222 --store /data/screech.db
'''
    (root/'init').write_text(init);(root/'init').chmod(0o755)
    archive(root,work/'initramfs.gz')
    with open(work/'disk.raw','wb') as f:f.truncate(128*1024*1024)
    subprocess.run([args.mkfs,'-q','-t','ext4','-F',str(work/'disk.raw')],check=True)

class VM:
    def __init__(self,args,work,cycle):
        sock=socket.socket();sock.bind(('127.0.0.1',0));port=sock.getsockname()[1];sock.close()
        self.address=('127.0.0.1',port)
        self.log=open(work/f'guest-{cycle}.log','wb')
        cmd=[args.qemu,'-accel','tcg','-m','512','-smp','2','-display','none','-serial','stdio','-monitor','none','-no-reboot',
             '-kernel',args.kernel,'-initrd',str(work/'initramfs.gz'),'-append','console=ttyS0 rdinit=/init panic=-1 quiet',
             '-drive',f'file={work}/disk.raw,format=raw,if=virtio,cache=none',
             '-netdev',f'user,id=net0,hostfwd=tcp:127.0.0.1:{port}-:4222','-device','e1000,netdev=net0']
        self.process=subprocess.Popen(cmd,stdin=subprocess.DEVNULL,stdout=self.log,stderr=subprocess.STDOUT)
        deadline=time.monotonic()+60
        while True:
            if self.process.poll() is not None:
                self.kill();raise RuntimeError('QEMU exited; inspect '+str(work/f'guest-{cycle}.log'))
            try:
                c=Client(self.address);c.close();break
            except (OSError,AssertionError,EOFError):
                if time.monotonic()>deadline:self.kill();raise TimeoutError('guest did not start; inspect '+str(work/f'guest-{cycle}.log'))
                time.sleep(.1)
    def kill(self):
        if self.process.poll() is None:self.process.kill()
        self.process.wait(timeout=5);self.log.close()

def run(args):
    work=args.work_dir.resolve();work.mkdir(parents=True,exist_ok=False)
    prepare(args,work)
    expected={};cycles=[];prior_pending=None;prior_acked=None;prior_id=None;prior_next_acked=None;prior_next_pending=None
    for cycle in range(args.cycles):
        vm=VM(args,work,cycle)
        try:
            c=Client(vm.address)
            if cycle==0:
                c.create()
                result=c.request('$JS.API.STREAM.CREATE.W',{'name':'W','subjects':['work.>'],'storage':'file','retention':'workqueue'})
                assert 'error' not in result,result
                c.create_consumer(stream='W')
                result=c.request('$JS.API.STREAM.CREATE.A',{'name':'A','subjects':['next.>'],'storage':'file','retention':'workqueue'})
                assert 'error' not in result,result
                c.create_consumer(stream='A')
            if prior_id:
                duplicate=c.publish_with_id(b'ignored',prior_id)
                assert duplicate['duplicate'] and duplicate['seq'] in expected
            if prior_acked:
                assert c.get(prior_acked,name='W') is None
                for seq in prior_pending:
                    assert c.get(seq,name='W') is not None
                    pending=c.pull(stream='W',no_wait=False);assert pending and pending['payload']==b'pending'
                    c.ack(pending);assert c.get(seq,name='W') is None
            if prior_next_acked:
                assert c.get(prior_next_acked,name='A') is None
                message=c.pull(stream='A',no_wait=False);assert message and message['payload']==b'next'
                c.ack(message);assert c.get(prior_next_pending,name='A') is None
            for seq,value in expected.items():assert c.get(seq)==value,(cycle,seq)
            if args.concurrent:
                def publish_worker(worker):
                    client=Client(vm.address);pairs=[]
                    try:
                        for i in range(worker,args.messages,16):
                            value=f'{cycle}-{i}'.encode();pairs.append((client.publish(value),value))
                    finally:client.close()
                    return pairs
                with concurrent.futures.ThreadPoolExecutor(max_workers=16) as pool:
                    batches=list(pool.map(publish_worker,range(16)))
                for pairs in batches:
                    for seq,value in pairs:assert seq not in expected;expected[seq]=value
                assert c.request('$JS.API.SCREECH.GROUPS',{})['committed_groups']>0
            else:
                for i in range(args.messages):
                    value=f'{cycle}-{i}'.encode();seq=c.publish(value);expected[seq]=value
            prior_id=f'vm-id-{cycle}'
            a=c.publish_with_id(f'dedup-{cycle}'.encode(),prior_id);expected[a['seq']]=f'dedup-{cycle}'.encode()
            prior_acked=c.publish(b'acked',subject='work.x');message=c.pull(stream='W');c.ack(message)
            prior_pending=[c.publish(b'pending',subject='work.x') for _ in range(8)]
            c.counter+=1;inbox=f'_INBOX.vm.{c.counter}'
            payload=b'{"batch":8,"expires":2000000000}'
            c.send(f'SUB {inbox} {c.counter}\r\nUNSUB {c.counter} 8\r\nPUB $JS.API.CONSUMER.MSG.NEXT.W.D {inbox} {len(payload)}\r\n'.encode()+payload+b'\r\n')
            for _ in range(8):assert c.read_message()['payload']==b'pending'
            prior_next_acked=c.publish(b'acked',subject='next.x')
            prior_next_pending=c.publish(b'next',subject='next.x')
            old=c.pull(stream='A');following=c.request_message(old['reply'],b'+NXT')
            assert following['payload']==b'next'
            if args.force_checkpoint:
                checkpoint=c.request('$JS.API.SCREECH.CHECKPOINT',{});assert checkpoint.get('success'),checkpoint
            c.close()
        finally:vm.kill() # SIGKILL the entire guest, with no guest shutdown or sync.
        cycles.append({'cycle':cycle,'acknowledged':len(expected),'guest_sigkill':True})
    vm=VM(args,work,'verify')
    try:
        c=Client(vm.address)
        for seq,value in expected.items():assert c.get(seq)==value,seq
        duplicate=c.publish_with_id(b'ignored',prior_id);assert duplicate['duplicate'] and duplicate['seq'] in expected
        assert c.get(prior_acked,name='W') is None
        for seq in prior_pending:
            message=c.pull(stream='W',no_wait=False);assert message and message['payload']==b'pending'
            c.ack(message);assert c.get(seq,name='W') is None
        assert c.get(prior_next_acked,name='A') is None
        message=c.pull(stream='A',no_wait=False);assert message and message['payload']==b'next'
        c.ack(message);assert c.get(prior_next_pending,name='A') is None
        c.close()
    finally:vm.kill()
    result={'passed':True,'cycles':cycles,'acknowledged_verified':len(expected),'acceleration':'tcg','drive_cache':'none',
            'scope':'guest kernel crash; host disk/controller failure not simulated','consumer_ack_and_redelivery_verified':True,'dedup_verified':True,'workqueue_retention_verified':True,'checkpoint_recovery_verified':args.force_checkpoint,'reservation_batch_size':8,'ack_next_recovery_verified':True,'concurrent_group_commits_verified':args.concurrent}
    text=json.dumps(result,indent=2);print(text)
    if args.output:args.output.write_text(text+'\n')
if __name__=='__main__':
    p=argparse.ArgumentParser()
    for key in ('binary','kernel','modules','busybox','qemu','modprobe','mkfs'):p.add_argument('--'+key,required=True)
    p.add_argument('--work-dir',type=Path,required=True);p.add_argument('--output',type=Path)
    p.add_argument('--concurrent',action='store_true');p.add_argument('--force-checkpoint',action='store_true');p.add_argument('--kernel-release');p.add_argument('--cycles',type=int,default=3);p.add_argument('--messages',type=int,default=50)
    run(p.parse_args())
