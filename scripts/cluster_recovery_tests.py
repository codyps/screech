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

def nonce(cluster,node):
    until=time.monotonic()+10
    while time.monotonic()<until:
        process=cluster.processes[node]
        if process.poll() is not None:raise AssertionError((node,process.returncode,(cluster.root/str(node)/'server.log').read_text()))
        if not (cluster.root/str(node)/'db').exists():time.sleep(.05);continue
        try:
            with sqlite3.connect(cluster.root/str(node)/'db') as db:
                row=db.execute("SELECT value FROM cluster_meta WHERE key='incarnation'").fetchone()
                if row:return row[0].hex()
        except sqlite3.Error:pass
        time.sleep(.05)
    raise AssertionError('join incarnation missing')
def replace(cluster,old,new,start=True):
    if start:cluster.start(new,join=True)
    fresh=nonce(cluster,new)
    until=time.monotonic()+30
    while time.monotonic()<until:
        info=status(cluster)
        if info['retired'].get(str(old))==new and new in info['voters'] and not info['outgoing']:
            return info
        response=request(cluster,'$JS.API.SCREECH.CLUSTER.REPLACE',dict(old=old,new=new,nonce=fresh))
        assert response.get('accepted') or 'error' in response,response
        time.sleep(.1)
    raise AssertionError(('replacement failed',old,new,status(cluster)))
def verify(cluster,expected,acked=True):
    c=cluster.client(cluster.wait_leader())
    try:
        for seq,body in expected.items():assert c.get(seq)==body,(seq,len(body))
        if acked:
            assert c.get(1) is None
            info=c.request('$JS.API.CONSUMER.INFO.T.D',{});assert info['ack_floor']['consumer_seq']>=1,info
    finally:c.close()
def run(binary):
    results=[]
    with tempfile.TemporaryDirectory(prefix='screech-recovery-') as tmp:
        cluster=Cluster(binary,Path(tmp),spares=11)
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
            cluster.start(victim);cluster.await_equal();verify(cluster,expected)
            # Force the caught-up follower to become leader, proving its local
            # installed image (rather than a gateway read) recovers the bodies.
            current=cluster.wait_leader();cluster.kill(current)
            cluster.wait_leader();verify(cluster,expected);cluster.start(current);cluster.await_equal()
            results.append(dict(test='multi-chunk snapshot catches up a killed follower and survives leader loss',passed=True))
            for point in ['after_cluster_snapshot_chunk','before_cluster_snapshot_install','after_cluster_snapshot_install']:
                victim=next(n for n in cluster.processes if n!=cluster.wait_leader())
                cluster.kill(victim)
                c=cluster.client(cluster.wait_leader());seq=c.publish(point.encode());expected[seq]=point.encode();c.close();compact(cluster)
                marker=cluster.root/str(victim)/'snapshot-fault';marker.unlink(missing_ok=True)
                cluster.start(victim,extra_env={'SCREECH_FAULT_POINT':point,'SCREECH_FAULT_ACTION':'hang','SCREECH_FAULT_MARKER':str(marker)})
                until=time.monotonic()+15
                while not marker.exists():
                    if time.monotonic()>until:raise AssertionError(('snapshot fault not reached',point,(cluster.root/str(victim)/'server.log').read_text()))
                    time.sleep(.02)
                # Replace the donor image while the receiver is paused. Old transfer
                # bytes remain a complete image; subsequent catch-up observes purge.
                if point=='after_cluster_snapshot_chunk':
                    c=cluster.client(cluster.wait_leader());assert c.request('$JS.API.STREAM.PURGE.T',{})['success'];expected.clear()
                    seq=c.publish(b'after-purge');expected[seq]=b'after-purge';c.close();compact(cluster)
                cluster.kill(victim);cluster.start(victim);cluster.await_equal();verify(cluster,expected)
                results.append(dict(test='SIGKILL at '+point+' preserves snapshot recovery',passed=True))

            for new,damage in enumerate(['missing','blank','garbage','snapshot-corrupt','snapshot-body-missing','log-tail-missing','wal-only','incarnation-corrupt','stale'],start=4):
                compact(cluster);old=cluster.wait_leader();cluster.await_equal()
                node=cluster.root/str(old);dbfile=node/'db'
                if damage=='stale':
                    with sqlite3.connect(dbfile) as source,sqlite3.connect(node/'stale') as dest:source.backup(dest)
                    c=cluster.client(old);seq=c.publish(b'newer-than-backup');expected[seq]=b'newer-than-backup';c.close()
                cluster.kill(old)
                if damage=='snapshot-corrupt':
                    with sqlite3.connect(dbfile) as db:db.execute("UPDATE snapshot_bodies SET data=x'000000'")
                elif damage=='snapshot-body-missing':
                    with sqlite3.connect(dbfile) as db:
                        assert db.execute('SELECT count(*) FROM snapshot_bodies').fetchone()[0]>0
                        db.execute('DELETE FROM snapshot_bodies')
                elif damage=='log-tail-missing':
                    with sqlite3.connect(dbfile) as db:
                        assert db.execute('SELECT count(*) FROM entries').fetchone()[0]>0
                        db.execute('DELETE FROM entries WHERE idx=(SELECT max(idx) FROM entries)')
                elif damage=='incarnation-corrupt':
                    with sqlite3.connect(dbfile) as db:db.execute("UPDATE cluster_meta SET value=?1 WHERE key='incarnation'",[bytes([123])*16])
                elif damage=='wal-only':dbfile.unlink()
                elif damage=='stale':
                    # A consistent stale copy is not a safe voter incarnation.
                    # Replacement still uses a fresh ID and the healthy quorum.
                    for suffix in ['-wal','-shm']:(node/('db'+suffix)).unlink(missing_ok=True)
                    shutil.copy2(node/'stale',dbfile)
                else:
                    for suffix in ['-wal','-shm']:(node/('db'+suffix)).unlink(missing_ok=True)
                    if damage=='missing':dbfile.unlink()
                    elif damage=='blank':dbfile.write_bytes(b'')
                    else:dbfile.write_bytes(b'not a sqlite database')
                cluster.wait_leader();verify(cluster,expected)
                info=replace(cluster,old,new);assert old not in info['voters']
                compact(cluster);verify(cluster,expected)
                c=cluster.client(cluster.wait_leader());seq=c.publish(damage.encode());expected[seq]=damage.encode();c.close()
                cluster.await_equal()
                if damage=='stale':
                    # A retired process with a valid old copy must not regain a
                    # vote or disturb the healthy configuration's term progression.
                    cluster.start(old);time.sleep(.5)
                    c=cluster.client(cluster.wait_leader());seq=c.publish(b'retired-id-fenced');expected[seq]=b'retired-id-fenced';c.close();cluster.kill(old)
                else:
                    cluster.start(old);p=cluster.processes[old]
                    assert p.wait(timeout=8)!=0,('damaged old voter started',damage)
                    cluster.kill(old)
                results.append(dict(test='safe replacement after killed voter storage '+damage,old=old,new=new,passed=True))
            # All original voters have now been replaced. Restart the survivors
            # using their own durable identities, without genesis/bootstrap flags.
            members=status(cluster)['voters']
            for n in list(cluster.processes):cluster.kill(n)
            for n in members:cluster.start(n)
            cluster.wait_leader();verify(cluster,expected)
            results.append(dict(test='replacement membership and snapshot survive complete process restart',passed=True))
        finally:cluster.close()
    with tempfile.TemporaryDirectory(prefix='screech-joint-recovery-') as tmp:
        cluster=Cluster(binary,Path(tmp),count=5,spares=2)
        try:
            for n in range(1,6):
                node=cluster.root/str(n)
                cluster.start(n,extra_env={'SCREECH_FAULT_POINT':'after_cluster_membership_apply','SCREECH_FAULT_AFTER':'2','SCREECH_FAULT_ACTION':'hang','SCREECH_FAULT_MARKER':str(node/'joint-fault'),'SCREECH_FAULT_ARM':str(node/'arm')})
            leader=cluster.wait_leader();c=cluster.client(leader);c.create();assert c.publish(b'joint-state')==1;c.close()
            old=next(n for n in cluster.processes if n!=leader);cluster.kill(old)
            cluster.start(6,join=True);fresh=nonce(cluster,6);(cluster.root/str(leader)/'arm').write_text('armed')
            request(cluster,'$JS.API.SCREECH.CLUSTER.REPLACE',dict(old=old,new=6,nonce=fresh))
            marker=cluster.root/str(leader)/'joint-fault';until=time.monotonic()+15
            while not marker.exists():
                if time.monotonic()>until:raise AssertionError('joint configuration fault not reached')
                time.sleep(.02)
            cluster.kill(leader)
            # The surviving old/new majorities finish the committed replacement
            # without consulting any bytes from the killed leader.
            replace(cluster,old,6,start=False);verify(cluster,{1:b'joint-state'},acked=False)
            victim_dir=cluster.root/str(leader)
            for suffix in ['', '-wal','-shm']:(victim_dir/('db'+suffix)).unlink(missing_ok=True)
            replace(cluster,leader,7);verify(cluster,{1:b'joint-state'},acked=False)
            results.append(dict(test='leader killed during joint apply; surviving quorum completes replacement despite lost leader disk',passed=True))
        finally:cluster.close()
    with tempfile.TemporaryDirectory(prefix='screech-lost-learner-') as tmp:
        cluster=Cluster(binary,Path(tmp),spares=3)
        try:
            for n in range(1,4):cluster.start(n)
            leader=cluster.wait_leader();c=cluster.client(leader);c.create();assert c.publish(b'learner-state')==1;c.close();compact(cluster)
            old=next(n for n in cluster.processes if n!=leader);cluster.kill(old)
            marker=cluster.root/'4'/'learner-fault'
            cluster.start(4,join=True,extra_env={'SCREECH_FAULT_POINT':'after_cluster_snapshot_chunk','SCREECH_FAULT_ACTION':'hang','SCREECH_FAULT_MARKER':str(marker)})
            first_nonce=nonce(cluster,4)
            request(cluster,'$JS.API.SCREECH.CLUSTER.REPLACE',dict(old=old,new=4,nonce=first_nonce))
            until=time.monotonic()+15
            while not marker.exists():
                if time.monotonic()>until:raise AssertionError('learner transfer did not pause')
                time.sleep(.02)
            info=status(cluster);assert 4 in info['learners'] and 4 not in info['voters'],info
            cluster.kill(4)
            for p in (cluster.root/'4').glob('db*'):p.unlink()
            # Retargeting retires the failed learner before enrolling a fresh ID.
            replace(cluster,old,5);info=status(cluster)
            assert info['retired'].get('4')==0 and 4 not in info['learners'],info
            verify(cluster,{1:b'learner-state'},acked=False)
            # Wipe an already-promoted voter and try the old ID as a fresh join.
            # Its new incarnation must be unable to resume voting or replication.
            old_nonce=nonce(cluster,5);cluster.kill(5)
            for p in (cluster.root/'5').glob('db*'):p.unlink()
            cluster.start(5,join=True);assert nonce(cluster,5)!=old_nonce;time.sleep(.5)
            c=cluster.client(cluster.wait_leader());assert c.publish(b'identity-fence')==2;c.close()
            with sqlite3.connect(cluster.root/'5'/'db') as db:
                assert db.execute('SELECT count(*) FROM event_index').fetchone()[0]==0
            replace(cluster,5,6);cluster.kill(5)
            verify(cluster,{1:b'learner-state',2:b'identity-fence'},acked=False)
            results.append(dict(test='lost learner retargeted; wiped promoted voter cannot reuse its incarnation',passed=True))
        finally:cluster.close()
    with tempfile.TemporaryDirectory(prefix='screech-live-leader-replace-') as tmp:
        cluster=Cluster(binary,Path(tmp),spares=1)
        try:
            for n in range(1,4):cluster.start(n)
            leader=cluster.wait_leader();c=cluster.client(leader);c.create();assert c.publish(b'live-leader')==1;c.close()
            import threading
            acknowledged={1:b'live-leader'};errors=[];started=threading.Event();stop=threading.Event()
            gateway=next(i for i in cluster.processes if i!=leader)
            def publish_during_replacement():
                c=None
                try:
                    for n in range(40):
                        if stop.is_set():break
                        body=('during-replacement-'+str(n)).encode();until=time.monotonic()+25
                        while not stop.is_set():
                            try:
                                if c is None:c=cluster.client(gateway)
                                ack=c.publish_with_id(body,'replacement-'+str(n));acknowledged[ack['seq']]=body;break
                            except (OSError,EOFError,AssertionError):
                                if c:c.close()
                                c=None
                                if time.monotonic()>until:raise
                        if n>=5:started.set()
                        time.sleep(.01)
                except Exception as e:errors.append(e)
                finally:
                    if c:c.close()
            thread=threading.Thread(target=publish_during_replacement);thread.start()
            try:
                assert started.wait(15),errors
                replace(cluster,leader,4)
                thread.join(35);assert not thread.is_alive() and not errors,errors
                assert len(acknowledged)==41,acknowledged.keys()
                verify(cluster,acknowledged,acked=False)
                assert cluster.wait_leader()!=leader
            finally:
                stop.set();thread.join(10)
            results.append(dict(test='live leader replacement fences old writers while publications continue',passed=True))
        finally:cluster.close()
    return results
if __name__=='__main__':
    p=argparse.ArgumentParser();p.add_argument('binary',type=lambda x:str(Path(x).resolve()));p.add_argument('--output',type=Path);a=p.parse_args()
    result=run(a.binary);text=json.dumps(result,indent=2);print(text)
    if a.output:a.output.write_text(text+'\n')
