#!/usr/bin/env python3
"""Deterministically enqueue concurrent requests, interrupt an entire atomic group."""
import argparse,json,select,tempfile,time
from pathlib import Path
from crash_tests import Server
from wire import Client
from batch_crash_tests import no_response

def wait_until(predicate):
    deadline=time.monotonic()+10
    while not predicate():
        assert time.monotonic()<deadline,'group schedule timed out';time.sleep(.002)
def create(c,name,subject,retention='workqueue'):
    r=c.request(f'$JS.API.STREAM.CREATE.{name}',{'name':name,'subjects':[subject],'storage':'file','retention':retention,'duplicate_window':60000000000})
    assert 'error' not in r,r
    c.create_consumer(stream=name)
def subscribe(c,inbox):c.send(f'SUB {inbox} 100\r\n'.encode());c.flush()
def publish(c,index,headers=None):
    payload=f'p{index}'.encode();reply=f'_INBOX.pub.{index}'
    if headers is None:c.send(f'PUB test.x {reply} {len(payload)}\r\n'.encode()+payload+b'\r\n')
    else:c.send(f'HPUB test.x {reply} {len(headers)} {len(headers)+len(payload)}\r\n'.encode()+headers+payload+b'\r\n')
def pull(c,index):
    payload=b'{"batch":1,"expires":5000000000}'
    c.send(f'PUB $JS.API.CONSUMER.MSG.NEXT.T.D _INBOX.pull.{index} {len(payload)}\r\n'.encode()+payload+b'\r\n')
def ack(c,index,token):c.send(f'PUB {token} _INBOX.ack.{index} 4\r\n+ACK\r\n'.encode())
def ingress(gate,count):wait_until(lambda:Path(str(gate)+'.ingress').exists() and Path(str(gate)+'.ingress').stat().st_size>=count)
def bootstrap(binary,directory,phase):
    server=Server(binary,directory);tokens=[]
    try:
        c=Client(server.address);create(c,'T','test.>')
        if phase=='consumer':
            for i in range(64):c.publish(f'p{i}'.encode())
        if phase=='mixed':
            create(c,'W','work.>')
            for i in range(32):c.publish(f'w{i}'.encode(),subject='work.x')
            for i in range(32):tokens.append(c.pull(stream='W')['reply'])
        c.close()
    finally:server.kill()
    return tokens

def run_case(binary,phase,point,action,io=None):
    print('RUN',phase,point,action,flush=True)
    with tempfile.TemporaryDirectory(prefix='screech-group-') as tmp:
        directory=Path(tmp);tokens=bootstrap(binary,directory,phase);gate=directory/'gate'
        env={'SCREECH_GROUP_GATE':str(gate)}
        if io:
            shim,mode=io;env.update(LD_PRELOAD=str(shim),SCREECH_IO_MODE=mode,SCREECH_IO_ARM=str(directory/'armed'),SCREECH_IO_HIT=str(directory/'hit'),SCREECH_IO_TARGET='wal')
        server=Server(binary,directory,point,action,extra_env=env);clients=[];monitor=None;reader=None
        try:
            monitor=Client(server.address);monitor.send(b'SUB test.> 444\r\n');monitor.flush()
            if phase=='mixed':
                fetch=[Client(server.address) for _ in range(32)];pub=[Client(server.address) for _ in range(32)];acks=[Client(server.address) for _ in range(32)];clients=fetch+pub+acks
                for i,c in enumerate(fetch):subscribe(c,f'_INBOX.pull.{i}')
                for i,c in enumerate(pub):subscribe(c,f'_INBOX.pub.{i}')
                for i,c in enumerate(acks):subscribe(c,f'_INBOX.ack.{i}')
                for i,c in enumerate(fetch):pull(c,i)
                ingress(gate,32)
                for i in range(32):
                    publish(pub[i],i);ingress(gate,32+2*i+1)
                    ack(acks[i],i,tokens[i]);ingress(gate,32+2*i+2)
            else:
                clients=[Client(server.address) for _ in range(64)]
                for i,c in enumerate(clients):subscribe(c,f'_INBOX.{"pub" if phase=="publisher" else "pull"}.{i}')
                for i,c in enumerate(clients):
                    if phase=='publisher':publish(c,i)
                    else:pull(c,i)
                ingress(gate,64)
            reader=Client(server.address);subscribe(reader,'_INBOX.fence')
            reader.send(b'PUB $JS.API.STREAM.INFO.T _INBOX.fence 2\r\n{}\r\n')
            ingress(gate,97 if phase=='mixed' else 65)
            wait_until(lambda:Path(str(gate)+'.ready').exists())
            if io:(directory/'armed').touch()
            Path(str(gate)+'.release').touch()
            if io:wait_until(lambda:(directory/'hit').exists())
            else:server.wait_fault()
            live_count=64 if phase=='publisher' else 32 if phase=='mixed' else 0
            live=[monitor.read_message() for _ in range(live_count)]
            assert {m['payload'] for m in live}=={f'p{i}'.encode() for i in range(live_count)}
            no_response(monitor) # Each Core publication is forwarded exactly once.
            context=json.loads(Path(str(server.marker)+'.group').read_text());rows=context['records'];assert len(rows)>=2,context
            kinds={next(iter(row['event'])) for row in rows}
            if phase=='mixed':assert {'Publish','Ack','Deliver'}<=kinds,kinds
            if not io:
                # Durable replies/deliveries and fenced reads wait; Core is already live.
                for c in clients:no_response(c)
                no_response(reader)
            elif io[1]=='short-write':
                for c in clients:assert json.loads(c.read_message()['payload'])['seq']>0
                assert json.loads(reader.read_message()['payload'])['state']['messages']==64
            else:
                for c in clients:
                    if select.select([c.socket],[],[],.01)[0]:
                        try:c.read_message()
                        except (OSError,EOFError,RuntimeError):pass
                        else:raise AssertionError('I/O failure exposed a group reply')
        finally:
            for c in clients:c.close()
            if monitor:monitor.close()
            if reader:reader.close()
            server.kill()
        expected=point.startswith('after_') or point=='before_group_replies'
        server=Server(binary,directory)
        try:
            c=Client(server.address)
            publications=[];reservations=[];acknowledgements=[]
            for row in rows:
                event=row['event']
                if 'Publish' in event:publications.append((event['Publish']['message']['sequence'],bytes(row['payload'])))
                if 'Deliver' in event:reservations.append(event['Deliver']['delivery']['sequence'])
                if 'Ack' in event:acknowledgements.append(event['Ack']['sequence'])
            if io:
                observed=[c.get(seq)==payload for seq,payload in publications];assert len(set(observed))==1,observed
                expected=observed[0]
                if io[1]=='short-write':assert expected
            for seq,payload in publications:assert c.get(seq)==(payload if expected else None),(phase,point,seq)
            for seq in acknowledgements:assert (c.get(seq,name='W') is None)==expected,(point,seq)
            if phase=='consumer':
                time.sleep(.12)
                payload=b'{"batch":64,"expires":5000000000}'
                c.send(f'SUB _INBOX.recover 99999\r\nUNSUB 99999 64\r\nPUB $JS.API.CONSUMER.MSG.NEXT.T.D _INBOX.recover {len(payload)}\r\n'.encode()+payload+b'\r\n')
                messages=[c.read_message() for _ in range(64)];assert len({m['payload'] for m in messages})==64
                for message in messages:
                    seq=int(message['reply'].split('.')[7]);attempt=int(message['reply'].split('.')[6])
                    if seq in reservations:assert attempt==(2 if expected else 1),(seq,attempt,expected)
                    c.ack(message)
                assert c.request('$JS.API.STREAM.INFO.T',{})['state']['messages']==0
            if phase=='mixed':
                info=c.request('$JS.API.CONSUMER.INFO.T.D',{})
                assert info['num_ack_pending']==(len(reservations) if expected else 0),info
                assert info['delivered']['consumer_seq']==(len(reservations) if expected else 0),info
            c.close()
        finally:server.kill()
        return {'phase':phase,'point':point,'action':action,'records':len(rows),'kinds':sorted(kinds),'group_recovered':expected,'passed':True}

def functional_case(binary,conditional):
    with tempfile.TemporaryDirectory(prefix='screech-group-conditions-') as tmp:
        directory=Path(tmp);bootstrap(binary,directory,'publisher');gate=directory/'gate'
        server=Server(binary,directory,extra_env={'SCREECH_GROUP_GATE':str(gate)});clients=[]
        try:
            clients=[Client(server.address) for _ in range(32)]
            for i,c in enumerate(clients):subscribe(c,f'_INBOX.pub.{i}')
            for i,c in enumerate(clients):
                header=(b'NATS/1.0\r\nNats-Expected-Last-Sequence: 0\r\n\r\n' if conditional else f'NATS/1.0\r\nNats-Msg-Id: id{i%8}\r\n\r\n'.encode())
                publish(c,i,header)
            ingress(gate,32);Path(str(gate)+'.release').touch()
            replies=[json.loads(c.read_message()['payload']) for c in clients]
            if conditional:
                assert sum('error' not in r for r in replies)==1,replies
                expected=1
            else:
                assert sum(not r.get('duplicate',False) for r in replies)==8,replies
                assert len({r['seq'] for r in replies})==8
                for i,r in enumerate(replies):assert r['seq']==replies[i%8]['seq']
                expected=8
            c=Client(server.address)
            info=c.request('$JS.API.STREAM.INFO.T',{});assert info['state']['messages']==expected and info['state']['last_seq']==expected,info
            c.close()
        finally:
            for c in clients:c.close()
            server.kill()
        server=Server(binary,directory)
        try:
            c=Client(server.address);info=c.request('$JS.API.STREAM.INFO.T',{})
            assert info['state']['messages']==expected and info['state']['last_seq']==expected
            if not conditional:
                for i in range(8):
                    reply=c.publish_with_id(b'ignored',f'id{i}');assert reply['duplicate'] and reply['seq']==replies[i]['seq']
            c.close()
        finally:server.kill()
        return {'phase':'conditional' if conditional else 'dedup','requests':32,'stored':expected,'passed':True}

def run(binary,shim):
    results=[]
    for phase in ['publisher','consumer','mixed']:
        for action in ['hang','exit']:
            for point in ['before_group_commit','before_group_store_commit','after_group_store_commit','after_group_commit','before_group_replies']:
                results.append(run_case(binary,phase,point,action))
    if shim:
        for mode in ['sync-before-eio','sync-after-eio','write-enospc','short-write']:
            results.append(run_case(binary,'publisher','capture_group',mode,(shim,mode)))
    results.append(functional_case(binary,False))
    results.append(functional_case(binary,True))
    return results
if __name__=='__main__':
    p=argparse.ArgumentParser();p.add_argument('binary');p.add_argument('--shim',type=lambda x:Path(x).resolve());p.add_argument('--output',type=Path);args=p.parse_args()
    text=json.dumps(run(args.binary,args.shim),indent=2);print(text)
    if args.output:args.output.write_text(text+'\n')
