#!/usr/bin/env python3
"""Compaction, snapshot transfer and replacement histories over real processes."""
import argparse,json,sqlite3,tempfile,time,os,shutil
from pathlib import Path
from cluster_tests import Cluster
from wire import Client

def request(cluster,subject,body=None,attempts=30):
    last=None
    for _ in range(attempts):
        try:
            leader=cluster.wait_leader();c=cluster.client(leader)
            try:return c.request(subject,{} if body is None else body)
            finally:c.close()
        except (OSError,EOFError,AssertionError,TimeoutError) as e:last=e;time.sleep(.1)
    raise AssertionError(last)
def status(cluster):return request(cluster,'$JS.API.SCREECH.CLUSTER.STATUS')
def compact(cluster):
    r=request(cluster,'$JS.API.SCREECH.CLUSTER.COMPACT');assert r.get('success'),r
    assert status(cluster)['snapshot_index']>0

def wait_healed(cluster,slot,previous=None):
    if previous is None:previous=cluster.started_from.get(slot)
    until=time.monotonic()+45
    while time.monotonic()<until:
        info=status(cluster)
        try:member=cluster.member(slot)
        except (FileNotFoundError,ValueError,KeyError,TypeError):time.sleep(.05);continue
        if member in info['voters'] and not info['outgoing'] and info['replacement'] is None:
            if previous is not None and member==previous:time.sleep(.05);continue
            cluster.await_equal()
            return info
        time.sleep(.1)
    raise AssertionError(('automatic healing failed',slot,status(cluster)))

def wait_marker(cluster,slot,marker):
    until=time.monotonic()+30
    while not marker.exists():
        process=cluster.processes[slot]
        if process.poll() is not None or time.monotonic()>until:
            raise AssertionError(('fault not reached',slot,marker,(cluster.root/str(slot)/'server.log').read_text()))
        time.sleep(.02)

def damage_store(cluster,slot,damage):
    path=cluster.store_path(slot)
    if damage=='snapshot-corrupt':
        with sqlite3.connect(path) as db:db.execute("UPDATE snapshot_bodies SET data=x'000000'")
    elif damage=='snapshot-body-missing':
        with sqlite3.connect(path) as db:
            assert db.execute('SELECT count(*) FROM snapshot_bodies').fetchone()[0]>0
            db.execute('DELETE FROM snapshot_bodies')
    elif damage=='log-tail-missing':
        with sqlite3.connect(path) as db:
            assert db.execute('SELECT count(*) FROM entries').fetchone()[0]>0
            db.execute('DELETE FROM entries WHERE idx=(SELECT max(idx) FROM entries)')
    elif damage=='incarnation-corrupt':
        with sqlite3.connect(path) as db:db.execute("UPDATE cluster_meta SET value=?1 WHERE key='incarnation'",[bytes([123])*16])
    elif damage=='watermark-missing':path.with_suffix('.cluster-watermark').unlink()
    elif damage=='watermark-corrupt':path.with_suffix('.cluster-watermark').write_bytes(b'broken recovery seal')
    elif damage in ('watermark-slot-0','watermark-slot-1'):
        seal=path.with_suffix('.cluster-watermark');data=bytearray(seal.read_bytes())
        assert len(data)>=8192,'expected authenticated recovery-fence records and journal'
        data[int(damage[-1])*4096+128]^=0x80;seal.write_bytes(data)
    elif damage=='manifest-corrupt':
        (cluster.root/str(slot)/'db.node-state').write_bytes(b'broken startup identity')
    elif damage=='all-local-state-missing':
        node=cluster.root/str(slot)
        for entry in node.iterdir():
            if entry.name in ['config.json','server.log']:continue
            if entry.is_dir():shutil.rmtree(entry)
            else:entry.unlink()
    elif damage=='wal-only':path.unlink()
    else:
        for suffix in ['-wal','-shm']:Path(str(path)+suffix).unlink(missing_ok=True)
        if damage=='missing':path.unlink()
        elif damage=='blank':path.write_bytes(b'')
        elif damage=='garbage':path.write_bytes(b'not a sqlite database')
        elif damage=='stale':shutil.copy2(cluster.root/str(slot)/'stale',path)
        else:raise AssertionError(damage)

def verify(cluster,expected,acked=True):
    c=cluster.client(cluster.wait_leader())
    try:
        for seq,body in expected.items():assert c.get(seq)==body,(seq,len(body))
        if acked:
            assert c.get(1) is None
            info=c.request('$JS.API.CONSUMER.INFO.T.D',{});assert info['ack_floor']['consumer_seq']>=1,info
    finally:c.close()

def replay_projection_without_quorum(binary):
    with tempfile.TemporaryDirectory(prefix='screech-projection-replay-') as tmp:
        cluster=Cluster(binary,Path(tmp))
        try:
            for slot in range(1,4):cluster.start(slot)
            leader=cluster.wait_leader();c=cluster.client(leader);c.create()
            assert c.publish(b'old-projection')==1;c.close();cluster.await_equal()
            victim=next(slot for slot in cluster.processes if slot!=leader)
            previous=cluster.member(victim);path=cluster.store_path(victim)
            with sqlite3.connect(path) as source,sqlite3.connect(cluster.root/str(victim)/'stale') as dest:source.backup(dest)
            source.close();dest.close()
            c=cluster.client(leader);assert c.publish(b'journaled-payload')==2;c.close();cluster.await_equal()
            with sqlite3.connect(path) as db:last=db.execute('SELECT max(idx) FROM entries').fetchone()[0]
            db.close()
            for slot in list(cluster.processes):cluster.kill(slot)
            damage_store(cluster,victim,'stale');cluster.start(victim);time.sleep(2)
            assert cluster.member(victim)==previous
            deadline=time.monotonic()+10
            while True:
                db=None
                try:
                    db=sqlite3.connect(f'file:{path}?mode=ro',uri=True,timeout=.1)
                    restored=db.execute('SELECT max(idx) FROM entries').fetchone()[0]>=last
                    restored &= any(b'journaled-payload' in row[0] for row in db.execute('SELECT data FROM entries'))
                    if restored:break
                except sqlite3.Error:pass
                finally:
                    if db is not None:db.close()
                assert time.monotonic()<deadline,'journal did not restore the lost payload projection'
                time.sleep(.05)
            try:
                c=Client(cluster.address(victim,'service'),timeout=.3);c.close()
                raise AssertionError('replayed minority served without quorum')
            except (OSError,EOFError):pass
            for slot in range(1,4):
                if slot!=victim:cluster.start(slot)
            verify(cluster,{1:b'old-projection',2:b'journaled-payload'},acked=False)
            cluster.await_equal()
            return dict(test='journal restores lost SQL payloads before quorum returns without allowing minority service',passed=True)
        finally:cluster.close()

def run(binary):
    results=[replay_projection_without_quorum(binary)]
    with tempfile.TemporaryDirectory(prefix='screech-recovery-') as tmp:
        cluster=Cluster(binary,Path(tmp),spares=0)
        expected={}
        try:
            for n in range(1,4):cluster.start(n)
            leader=cluster.wait_leader();c=cluster.client(leader)
            r=c.request('$JS.API.STREAM.CREATE.T',dict(name='T',subjects=['test.>'],storage='file',retention='workqueue'));assert 'error' not in r,r
            c.create_consumer()
            for n in range(1,7):
                body=bytes([n])*900000;seq=c.publish(body);assert seq==n;expected[seq]=body
            first=c.pull();c.ack(first);del expected[1];c.close()
            # Take one follower offline before compaction. Its old log cannot
            # catch up without the complete multi-chunk snapshot and payloads.
            victim=next(n for n in cluster.processes if n!=leader)
            cluster.kill(victim);compact(cluster)
            c=cluster.client(cluster.wait_leader());seq=c.publish(b'tail');expected[seq]=b'tail';c.close()
            cluster.start(victim);wait_healed(cluster,victim);verify(cluster,expected)
            # Force the caught-up follower to become leader, proving its local
            # installed image (rather than a gateway read) recovers the bodies.
            current=cluster.wait_leader();cluster.kill(current)
            cluster.wait_leader();verify(cluster,expected);cluster.start(current);wait_healed(cluster,current)
            results.append(dict(test='multi-chunk snapshot catches up a killed follower and survives leader loss',passed=True))
            for point in ['after_cluster_snapshot_chunk','before_cluster_snapshot_install','after_cluster_snapshot_install']:
                victim=next(n for n in cluster.processes if n!=cluster.wait_leader())
                cluster.kill(victim)
                c=cluster.client(cluster.wait_leader());seq=c.publish(point.encode());expected[seq]=point.encode();c.close();compact(cluster)
                marker=cluster.root/str(victim)/'snapshot-fault';marker.unlink(missing_ok=True)
                cluster.start(victim,extra_env={'SCREECH_FAULT_POINT':point,'SCREECH_FAULT_ACTION':'hang','SCREECH_FAULT_MARKER':str(marker)})
                wait_marker(cluster,victim,marker)
                # Replace the donor image while the receiver is paused. Old transfer
                # bytes remain a complete image; subsequent catch-up observes purge.
                if point=='after_cluster_snapshot_chunk':
                    c=cluster.client(cluster.wait_leader());assert c.request('$JS.API.STREAM.PURGE.T',{})['success'];expected.clear()
                    seq=c.publish(b'after-purge');expected[seq]=b'after-purge';c.close();compact(cluster)
                cluster.kill(victim);cluster.start(victim);wait_healed(cluster,victim);verify(cluster,expected)
                results.append(dict(test='SIGKILL at '+point+' preserves snapshot recovery',passed=True))

            for damage in ['missing','blank','garbage','snapshot-corrupt','snapshot-body-missing','log-tail-missing','wal-only','incarnation-corrupt','stale','watermark-missing','watermark-corrupt','watermark-slot-0','watermark-slot-1','manifest-corrupt','all-local-state-missing']:
                compact(cluster);old=cluster.wait_leader();cluster.await_equal()
                previous=cluster.member(old);node=cluster.root/str(old);dbfile=cluster.store_path(old)
                if damage=='log-tail-missing':
                    # ReadIndex does not manufacture a log tail after compaction.
                    # Remove an actual confirmed application transition instead.
                    c=cluster.client(old);seq=c.publish(b'confirmed-tail-before-damage');expected[seq]=b'confirmed-tail-before-damage';c.close();cluster.await_equal()
                if damage=='stale':
                    with sqlite3.connect(dbfile) as source,sqlite3.connect(node/'stale') as dest:source.backup(dest)
                    c=cluster.client(old);seq=c.publish(b'newer-than-backup');expected[seq]=b'newer-than-backup';c.close()
                cluster.kill(old);damage_store(cluster,old,damage)
                cluster.wait_leader();verify(cluster,expected)
                cluster.start(old);info=wait_healed(cluster,old,previous)
                assert previous not in info['voters'],info
                compact(cluster);verify(cluster,expected)
                c=cluster.client(cluster.wait_leader());seq=c.publish(damage.encode());expected[seq]=damage.encode();c.close()
                cluster.await_equal()
                # Exercise the repaired store as a future leader too.
                leader=cluster.wait_leader();cluster.kill(leader)
                cluster.wait_leader();verify(cluster,expected)
                cluster.start(leader);wait_healed(cluster,leader)
                results.append(dict(test='same-command recovery after killed voter storage '+damage,passed=True))
            for n in list(cluster.processes):cluster.kill(n)
            for n in range(1,4):cluster.start(n)
            cluster.wait_leader();verify(cluster,expected);cluster.await_equal()
            results.append(dict(test='automatic replacement membership survives complete process restart',passed=True))
        finally:cluster.close()
    with tempfile.TemporaryDirectory(prefix='screech-joint-recovery-') as tmp:
        cluster=Cluster(binary,Path(tmp),count=5)
        try:
            for n in range(1,6):
                node=cluster.root/str(n)
                cluster.start(n,extra_env={'SCREECH_FAULT_POINT':'after_cluster_membership_apply','SCREECH_FAULT_AFTER':'2','SCREECH_FAULT_ACTION':'hang','SCREECH_FAULT_MARKER':str(node/'joint-fault'),'SCREECH_FAULT_ARM':str(node/'arm')})
            leader=cluster.wait_leader();c=cluster.client(leader);c.create();assert c.publish(b'joint-state')==1;c.close()
            old=next(n for n in cluster.processes if n!=leader);previous=cluster.member(old)
            cluster.kill(old);damage_store(cluster,old,'missing')
            (cluster.root/str(leader)/'arm').write_text('armed');cluster.start(old)
            wait_marker(cluster,leader,cluster.root/str(leader)/'joint-fault')
            cluster.kill(leader);damage_store(cluster,leader,'all-local-state-missing')
            wait_healed(cluster,old,previous);verify(cluster,{1:b'joint-state'},acked=False)
            cluster.start(leader);wait_healed(cluster,leader);verify(cluster,{1:b'joint-state'},acked=False)
            results.append(dict(test='automatic healing survives leader SIGKILL during joint apply and lost leader disk',passed=True))
        finally:cluster.close()
    with tempfile.TemporaryDirectory(prefix='screech-lost-learner-') as tmp:
        cluster=Cluster(binary,Path(tmp))
        try:
            for n in range(1,4):cluster.start(n)
            leader=cluster.wait_leader();c=cluster.client(leader);c.create();assert c.publish(b'learner-state')==1;c.close();compact(cluster)
            old=next(n for n in cluster.processes if n!=leader);cluster.kill(old)
            marker=cluster.root/str(old)/'learner-fault'
            cluster.start(old,extra_env={'SCREECH_FAULT_POINT':'after_cluster_snapshot_chunk','SCREECH_FAULT_ACTION':'hang','SCREECH_FAULT_MARKER':str(marker)})
            wait_marker(cluster,old,marker);failed=cluster.member(old)
            info=status(cluster);assert failed in info['learners'] and failed not in info['voters'],info
            cluster.kill(old);damage_store(cluster,old,'all-local-state-missing');cluster.start(old)
            info=wait_healed(cluster,old,failed)
            assert info['retired'].get(str(failed))==0 and failed not in info['learners'],info
            verify(cluster,{1:b'learner-state'},acked=False)
            results.append(dict(test='destroyed learner is automatically retired and replaced during recovery',passed=True))
        finally:cluster.close()
    for point in ['before_cluster_startup_manifest','after_cluster_startup_manifest','before_cluster_watermark','after_cluster_watermark_write','after_cluster_watermark']:
        with tempfile.TemporaryDirectory(prefix='screech-startup-fault-') as tmp:
            cluster=Cluster(binary,Path(tmp))
            try:
                for n in range(1,4):cluster.start(n)
                leader=cluster.wait_leader();c=cluster.client(leader);c.create();assert c.publish(b'manifest-state')==1;c.close()
                old=next(n for n in cluster.processes if n!=leader);cluster.kill(old);damage_store(cluster,old,'all-local-state-missing')
                marker=cluster.root/str(old)/'startup-fault'
                # First save records discovery identity; second publishes recovery generation.
                cluster.start(old,extra_env={'SCREECH_FAULT_POINT':point,'SCREECH_FAULT_AFTER':'2','SCREECH_FAULT_ACTION':'hang','SCREECH_FAULT_MARKER':str(marker)})
                wait_marker(cluster,old,marker);cluster.kill(old);cluster.start(old)
                wait_healed(cluster,old);verify(cluster,{1:b'manifest-state'},acked=False)
                results.append(dict(test='same-command restart after SIGKILL at '+point,passed=True))
            finally:cluster.close()
    with tempfile.TemporaryDirectory(prefix='screech-recovery-minority-') as tmp:
        cluster=Cluster(binary,Path(tmp))
        try:
            for n in range(1,4):cluster.start(n)
            leader=cluster.wait_leader();c=cluster.client(leader);c.create();assert c.publish(b'quorum-state')==1;c.close();cluster.await_equal()
            victim=next(n for n in cluster.processes if n!=leader)
            for n in list(cluster.processes):cluster.kill(n)
            damage_store(cluster,victim,'missing');cluster.start(victim);time.sleep(2)
            assert cluster.processes[victim].poll() is None
            assert not cluster.store_path(victim).exists(),'damaged minority silently recreated genesis'
            for n in range(1,4):
                if n!=victim:cluster.start(n)
            wait_healed(cluster,victim);verify(cluster,{1:b'quorum-state'},acked=False)
            results.append(dict(test='damaged minority waits safely and heals when surviving quorum returns',passed=True))
        finally:cluster.close()
    with tempfile.TemporaryDirectory(prefix='screech-stale-minority-') as tmp:
        cluster=Cluster(binary,Path(tmp))
        try:
            for n in range(1,4):cluster.start(n)
            leader=cluster.wait_leader();c=cluster.client(leader);c.create();assert c.publish(b'old-state')==1;c.close();cluster.await_equal()
            victim=leader;previous=cluster.member(victim)
            with sqlite3.connect(cluster.store_path(victim)) as source,sqlite3.connect(cluster.root/str(victim)/'stale') as dest:source.backup(dest)
            # A matching journal can replay a lost SQL projection. Cross a FULL
            # checkpoint root so this backup actually lacks required recovery
            # authority and must wait for the surviving quorum instead.
            compact(cluster)
            c=cluster.client(leader);assert c.publish(b'new-state')==2;c.close();cluster.await_equal()
            for n in list(cluster.processes):cluster.kill(n)
            damage_store(cluster,victim,'stale');cluster.start(victim);time.sleep(2)
            assert cluster.processes[victim].poll() is None
            try:
                c=Client(cluster.address(victim,'service'),timeout=.3);c.close();raise AssertionError('stale minority served')
            except (OSError,EOFError):pass
            assert cluster.member(victim)==previous
            for n in range(1,4):
                if n!=victim:cluster.start(n)
            wait_healed(cluster,victim,previous);verify(cluster,{1:b'old-state',2:b'new-state'},acked=False)
            results.append(dict(test='stale checkpoint root cannot resume voting without its promised history; quorum heals it',passed=True))
        finally:cluster.close()
    with tempfile.TemporaryDirectory(prefix='screech-automatic-standby-') as tmp:
        cluster=Cluster(binary,Path(tmp),spares=1)
        try:
            for n in range(1,4):cluster.start(n)
            leader=cluster.wait_leader();c=cluster.client(leader);c.create();assert c.publish(b'standby-state')==1;c.close();compact(cluster)
            cluster.start(4);until=time.monotonic()+30
            while True:
                info=status(cluster)
                try:member=cluster.member(4)
                except (FileNotFoundError,ValueError,KeyError,TypeError):member=0
                if member in info['learners']:break
                assert time.monotonic()<until,info;time.sleep(.05)
            cluster.await_equal();assert len(info['voters'])==3,info
            results.append(dict(test='ordinary spare startup enrolls a nonvoting standby without increasing voter count',passed=True))
        finally:cluster.close()
    with tempfile.TemporaryDirectory(prefix='screech-startup-formation-') as tmp:
        cluster=Cluster(binary,Path(tmp))
        try:
            cluster.start(1);cluster.start(2);time.sleep(2)
            assert not cluster.store_path(1).exists() and not cluster.store_path(2).exists()
            cluster.start(3);leader=cluster.wait_leader()
            c=cluster.client(leader);c.create();assert c.publish(b'formation')==1;c.close()
            results.append(dict(test='fresh genesis waits for complete formation agreement without a flag',passed=True))
        finally:cluster.close()
    for point in ['before_cluster_startup_manifest','after_cluster_startup_manifest']:
        with tempfile.TemporaryDirectory(prefix='screech-formation-crash-') as tmp:
            cluster=Cluster(binary,Path(tmp))
            try:
                marker=cluster.root/'1'/'formation-fault'
                cluster.start(1,extra_env={'SCREECH_FAULT_POINT':point,'SCREECH_FAULT_AFTER':'2','SCREECH_FAULT_ACTION':'hang','SCREECH_FAULT_MARKER':str(marker)})
                cluster.start(2);cluster.start(3);wait_marker(cluster,1,marker);cluster.kill(1)
                # A crash before certificate publication may leave neither other
                # node with the complete proof. Resume ordinary startup before
                # requiring first formation; a serving cluster may not exist yet.
                cluster.start(1)
                leader=cluster.wait_leader();c=cluster.client(leader);c.create();assert c.publish(b'formation-crash')==1;c.close()
                wait_healed(cluster,1);verify(cluster,{1:b'formation-crash'},acked=False)
                results.append(dict(test='automatic startup survives initial formation SIGKILL at '+point,passed=True))
            finally:cluster.close()
    with tempfile.TemporaryDirectory(prefix='screech-interrupted-genesis-') as tmp:
        cluster=Cluster(binary,Path(tmp))
        try:
            for n in range(1,4):
                marker=cluster.root/str(n)/'initial-database-fault'
                cluster.start(n,extra_env={'SCREECH_FAULT_POINT':'before_cluster_watermark','SCREECH_FAULT_ACTION':'hang','SCREECH_FAULT_MARKER':str(marker)})
            for n in range(1,4):wait_marker(cluster,n,cluster.root/str(n)/'initial-database-fault')
            for n in range(1,4):cluster.kill(n)
            for n in range(1,4):cluster.start(n)
            leader=cluster.wait_leader();c=cluster.client(leader);c.create();assert c.publish(b'completed-genesis')==1;c.close()
            results.append(dict(test='all nodes killed during initial database creation resume agreed genesis automatically',passed=True))
        finally:cluster.close()
    with tempfile.TemporaryDirectory(prefix='screech-runtime-recovery-') as tmp:
        cluster=Cluster(binary,Path(tmp))
        try:
            for n in range(1,4):cluster.start(n)
            leader=cluster.wait_leader();c=cluster.client(leader);c.create();assert c.publish(b'runtime-state')==1;c.close();cluster.await_equal()
            previous=cluster.member(leader)
            with sqlite3.connect(cluster.store_path(leader)) as db:db.execute("UPDATE cluster_meta SET value=x'ff' WHERE key='hard'")
            wait_healed(cluster,leader,previous);verify(cluster,{1:b'runtime-state'},acked=False)
            assert cluster.processes[leader].poll() is None
            results.append(dict(test='runtime storage failure automatically fences and heals without process restart',passed=True))
        finally:cluster.close()
    with tempfile.TemporaryDirectory(prefix='screech-live-recovery-') as tmp:
        import threading
        cluster=Cluster(binary,Path(tmp));acknowledged={};errors=[];started=threading.Event();stop=threading.Event()
        try:
            for n in range(1,4):cluster.start(n)
            leader=cluster.wait_leader();c=cluster.client(leader);c.create();c.close()
            gateway=next(i for i in cluster.processes if i!=leader)
            def publish_during_recovery():
                c=None
                try:
                    for n in range(40):
                        if stop.is_set():break
                        body=('during-recovery-'+str(n)).encode();until=time.monotonic()+30
                        while not stop.is_set():
                            try:
                                if c is None:c=cluster.client(gateway)
                                ack=c.publish_with_id(body,'recovery-'+str(n));acknowledged[ack['seq']]=body;break
                            except (OSError,EOFError,AssertionError):
                                if c:c.close()
                                c=None
                                if time.monotonic()>until:raise
                        if n>=5:started.set()
                        time.sleep(.05)
                except Exception as e:errors.append(e)
                finally:
                    if c:c.close()
            thread=threading.Thread(target=publish_during_recovery);thread.start()
            try:
                assert started.wait(15),errors
                previous=cluster.member(leader);cluster.kill(leader);damage_store(cluster,leader,'garbage')
                cluster.wait_leader();cluster.start(leader);wait_healed(cluster,leader,previous)
                thread.join(40);assert not thread.is_alive() and not errors,errors
                assert len(acknowledged)==40,acknowledged.keys()
                verify(cluster,acknowledged,acked=False)
            finally:stop.set();thread.join(10)
            results.append(dict(test='publications retain every ACK through leader kill, damaged disk and automatic recovery',passed=True))
        finally:cluster.close()
    return results
if __name__=='__main__':
    p=argparse.ArgumentParser();p.add_argument('binary',type=lambda x:str(Path(x).resolve()));p.add_argument('--output',type=Path);a=p.parse_args()
    result=run(a.binary);text=json.dumps(result,indent=2);print(text)
    if a.output:a.output.write_text(text+'\n')
