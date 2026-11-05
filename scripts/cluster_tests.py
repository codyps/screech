#!/usr/bin/env python3
"""Real three-process Raft histories. Every child is killed/reaped on failure."""
import argparse,errno,json,socket,sqlite3,subprocess,tempfile,time
from contextlib import closing
from pathlib import Path
from wire import Client

class Cluster:
    def __init__(self,binary,root,count=3,spares=0):
        self.binary=binary;self.root=root;self.processes={};self.logs={};self.started_from={}
        held=[]
        for _ in range(3*(count+spares)):
            s=socket.socket();s.bind(('127.0.0.1',0));held.append(s)
        ports=[s.getsockname()[1] for s in held]
        self.peers={str(i+1):dict(raft=f'127.0.0.1:{ports[3*i]}',client=f'127.0.0.1:{ports[3*i+1]}',service=f'127.0.0.1:{ports[3*i+2]}') for i in range(count+spares)}
        for i in range(1,count+spares+1):
            node=root/str(i);node.mkdir()
            (node/'config.json').write_text(json.dumps(dict(cluster_id='process-test',node_id=i,secret='test-secret-'+'x'*32,peers=self.peers,initial_voters=list(range(1,count+1)))))
        for s in held:s.close()
    def start(self,i,extra_env=None):
        try:self.started_from[i]=self.member(i)
        except (FileNotFoundError,ValueError,KeyError,TypeError):self.started_from[i]=None
        node=self.root/str(i);log=open(node/'server.log','ab');self.logs[i]=log
        args=[self.binary,'--store',str(node/'db'),'--cluster-config',str(node/'config.json')]
        import os
        env=os.environ.copy()
        if extra_env:env.update(extra_env)
        self.processes[i]=subprocess.Popen(args,stdout=log,stderr=log,env=env)
    def kill(self,i):
        p=self.processes.pop(i,None)
        if p is not None:
            p.kill();p.wait(timeout=5);self.logs.pop(i).close()
    def close(self):
        for i in list(self.processes):self.kill(i)
    def address(self,i,kind='client'):
        host,port=self.peers[str(i)][kind].rsplit(':',1);return host,int(port)
    def failure_details(self):
        details={}
        for i,p in list(self.processes.items()):
            node=dict(exit_code=p.poll())
            try:node['log_tail']=(self.root/str(i)/'server.log').read_text()[-8192:]
            except OSError as error:node['log_error']=str(error)
            try:
                path=self.store_path(i)
                with closing(sqlite3.connect(f'file:{path}?mode=ro',uri=True,timeout=.1)) as db:
                    node['metadata']={key:value.hex() for key,value in db.execute(
                        "SELECT key,value FROM cluster_meta WHERE key IN ('hard','view','conf','conf_index','event_sequence','app_sequence')")}
                    node['entries']=db.execute('SELECT count(*) FROM entries').fetchone()[0]
            except (OSError,sqlite3.Error) as error:node['store_error']=str(error)
            details[i]=node
        return details
    def wait_leader(self,exclude=()):
        until=time.monotonic()+20
        while time.monotonic()<until:
            for i,p in list(self.processes.items()):
                if p.poll() is not None:raise AssertionError((i,p.returncode,(self.root/str(i)/'server.log').read_text()))
                if i in exclude:continue
                try:
                    c=Client(self.address(i,'service'),timeout=.3)
                    c.request('$JS.API.INFO',{});c.close();return i
                except (OSError,EOFError,AssertionError):pass
            time.sleep(.05)
        raise TimeoutError(('no serving leader',self.failure_details()))
    def client(self,i):return Client(self.address(i),timeout=8)
    def store_path(self,i):
        node=self.root/str(i);manifest=node/'db.node-state'
        try:
            record=json.loads(bytes(json.loads(manifest.read_text())['body']))
            if record.get('instance'):return Path(record['instance']['path'])
        except (FileNotFoundError,ValueError,KeyError):pass
        return node/'db'
    def member(self,i):
        record=json.loads(bytes(json.loads((self.root/str(i)/'db.node-state').read_text())['body']))
        return record['instance']['member']
    def await_equal(self):
        until=time.monotonic()+30
        while time.monotonic()<until:
            values=[]
            try:
                for i in self.processes:
                    path=self.store_path(i)
                    if not path.exists():break
                    with closing(sqlite3.connect(f'file:{path}?mode=ro',uri=True)) as db:
                        progress=db.execute("select value from cluster_meta where key='event_sequence'").fetchone()
                        end=int.from_bytes(progress[0],'little') if progress else db.execute('select coalesce(max(seq),0) from event_index').fetchone()[0]
                        floor=db.execute("select value from cluster_meta where key='app_sequence'").fetchone()
                        view=db.execute("select value from cluster_meta where key='view'").fetchone()
                        values.append((max(end,int.from_bytes(floor[0],'little') if floor else 0),int.from_bytes(view[0],'little') if view else 0))
                if len(values)==len(self.processes) and values and all(v==values[0] for v in values):return
            except sqlite3.Error:pass
            time.sleep(.05)
        raise AssertionError(('committed replicas did not converge',values))

def run(binary):
    results=[]
    with tempfile.TemporaryDirectory(prefix='screech-cluster-') as tmp:
        cluster=Cluster(binary,Path(tmp))
        try:
            cluster.start(1)
            until=time.monotonic()+10
            while True:
                try:
                    with socket.create_connection(cluster.address(1,'raft'),timeout=.1):pass
                    break
                except OSError:
                    assert cluster.processes[1].poll() is None,cluster.failure_details()
                    assert time.monotonic()<until,cluster.failure_details()
                    time.sleep(.02)
            # No quorum exists yet. Both application endpoints must already be
            # reserved, so an outgoing ephemeral socket cannot claim them.
            for kind in ['client','service']:
                with socket.socket() as outgoing:
                    try:outgoing.bind(cluster.address(1,kind))
                    except OSError as error:assert error.errno==errno.EADDRINUSE,error
                    else:raise AssertionError(('unreserved startup endpoint',kind))
            results.append(dict(test='client and service ports stay reserved before formation',passed=True))
            for i in range(2,4):cluster.start(i)
            leader=cluster.wait_leader()
            c=cluster.client(1)
            result=c.request('$JS.API.STREAM.CREATE.T',dict(name='T',subjects=['test.>'],storage='file',num_replicas=3,retention='workqueue',duplicate_window=60000000000));assert 'error' not in result,result
            assert result['config']['num_replicas']==3,result
            c.create_consumer(ack_wait=100000000)
            assert c.publish_with_id(b'one','one')['seq']==1
            assert c.publish_with_id(b'two','two')['seq']==2
            c.close()
            for i in range(1,4):
                c=cluster.client(i);assert c.get(1)==b'one';assert c.get(2)==b'two';c.close()
            results.append(dict(test='publication and reads through every node',passed=True))
            sub=cluster.client(3);pub=cluster.client(2)
            sub.send(b'SUB live 7\r\n');sub.flush();pub.send(b'PUB live 3\r\na\x00b\r\n');pub.flush()
            assert sub.file.readline()==b'MSG live 7 3\r\n';assert sub.file.read(5)==b'a\x00b\r\n'
            sub.close();pub.close()
            results.append(dict(test='Core binary routing across different public nodes',passed=True))

            c=cluster.client(2);first=c.pull();assert first['payload']==b'one';c.ack(first);assert c.get(1) is None
            second=c.pull();assert second['payload']==b'two'
            checkpoint=c.request('$JS.API.SCREECH.CHECKPOINT',{});assert 'error' not in checkpoint,checkpoint
            c.close()
            cluster.kill(leader)
            replacement=cluster.wait_leader()
            c=cluster.client(replacement)
            assert c.get(1) is None
            info=c.request('$JS.API.CONSUMER.INFO.T.D',{});assert info['ack_floor']['consumer_seq']==1,info
            retry=c.publish_with_id(b'ignored','two');assert retry['duplicate'] and retry['seq']==2,retry
            again=c.pull();assert again['payload']==b'two';assert int(again['reply'].split('.')[6])>=2;c.ack(again)
            assert c.publish(b'three')==3;c.close()
            results.append(dict(test='leader loss preserves payloads, confirmed ACK, reservations and dedup',passed=True))
            cluster.start(leader);cluster.wait_leader();cluster.await_equal()
            results.append(dict(test='old leader rejoins and catches up',passed=True))
            # Kill a second node after rejoining has caught up. A minority must
            # neither confirm a write nor return a current model read.
            active_leader=cluster.wait_leader()
            others=[i for i in cluster.processes if i!=active_leader]
            for i in others:cluster.kill(i)
            for subject,payload in [('test.x',b'unknown'),('$JS.API.STREAM.INFO.T',b'{}')]:
                c=None
                try:
                    c=Client(cluster.address(active_leader),timeout=5)
                    result=c.request(subject,payload)
                    assert 'error' in result,('minority returned success',result)
                except (OSError,EOFError):pass
                finally:
                    if c:c.close()
            results.append(dict(test='minority refuses durable success and current reads',passed=True))
            for i in others:cluster.start(i)
            recovered=cluster.wait_leader()
            c=cluster.client(recovered);assert c.get(1) is None;assert c.get(2) is None;assert c.get(3)==b'three';c.close()
            cluster.await_equal()
            for i in list(cluster.processes):cluster.kill(i)
            for i in range(1,4):cluster.start(i)
            recovered=cluster.wait_leader();c=cluster.client(recovered)
            assert c.get(1) is None;assert c.get(2) is None;assert c.get(3)==b'three';assert c.publish(b'after-restart')>=4;c.close()
            results.append(dict(test='whole cluster process restart preserves acknowledged state',passed=True))
        finally:cluster.close()
    with tempfile.TemporaryDirectory(prefix='screech-five-voters-') as tmp:
        cluster=Cluster(binary,Path(tmp),count=5)
        try:
            for i in range(1,6):cluster.start(i)
            leader=cluster.wait_leader();c=cluster.client(1);c.create();assert c.publish(b'five-voter')==1;c.close()
            cluster.await_equal()
            victim=next(i for i in cluster.processes if i!=leader)
            cluster.kill(leader);cluster.kill(victim)
            survivor=cluster.wait_leader();c=cluster.client(survivor)
            assert c.get(1)==b'five-voter';assert c.publish(b'majority')==2;c.close()
            results.append(dict(test='five voters tolerate two process failures',passed=True))
        finally:cluster.close()
    return results
if __name__=='__main__':
    p=argparse.ArgumentParser();p.add_argument('binary',type=lambda x:str(Path(x).resolve()));p.add_argument('--output',type=Path);args=p.parse_args()
    result=run(args.binary);text=json.dumps(result,indent=2);print(text)
    if args.output:args.output.write_text(text+'\n')
