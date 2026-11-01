#!/usr/bin/env python3
"""Crash shared-tree projections with existing messages, dedup and pending ACKs."""
import argparse,json,tempfile
from pathlib import Path
from crash_tests import Server
from wire import Client
from group_crash_tests import ingress,subscribe,wait_until
from batch_crash_tests import no_response

def seed(binary,directory,count=160):
    server=Server(binary,directory)
    try:
        c=Client(server.address)
        for name,subject,retention in [('T','test.>','limits'),('W','work.>','workqueue'),('U','untouched.>','limits')]:
            r=c.request('$JS.API.STREAM.CREATE.'+name,{'name':name,'subjects':[subject],'storage':'file','retention':retention,'duplicate_window':3600000000000})
            assert 'error' not in r,r
            c.create_consumer(stream=name,ack_wait=3600000000000)
            for i in range(count):
                r=c.publish_with_id(f'{name}{i}'.encode(),f'{name}-id{i}',subject=subject[:-1]+str(i%7));assert r['seq']==i+1
        tokens=[c.pull(stream='W')['reply'] for _ in range(64)]
        assert c.request('$JS.API.SCREECH.CHECKPOINT',{}).get('success')
        c.close()
    finally:server.kill()
    return tokens

def run_case(binary,point,action):
    with tempfile.TemporaryDirectory(prefix='screech-tree-crash-') as tmp:
        directory=Path(tmp);tokens=seed(binary,directory);gate=directory/'gate'
        server=Server(binary,directory,point,action,extra_env={'SCREECH_GROUP_GATE':str(gate)});clients=[]
        try:
            for i in range(16):
                for kind in ['ack','pub']:
                    c=Client(server.address);clients.append(c);subscribe(c,f'_INBOX.{kind}.{i}')
                    if kind=='ack':c.send(f'PUB {tokens[i]} _INBOX.ack.{i} 4\r\n+ACK\r\n'.encode())
                    else:
                        payload=f'new{i}'.encode();c.send(f'PUB test.new _INBOX.pub.{i} {len(payload)}\r\n'.encode()+payload+b'\r\n')
                    ingress(gate,len(clients))
            wait_until(lambda:Path(str(gate)+'.ready').exists());Path(str(gate)+'.release').touch();server.wait_fault()
            rows=json.loads(Path(str(server.marker)+'.group').read_text())['records']
            assert len(rows)==32 and sum('Ack' in r['event'] for r in rows)==16,rows
            if action=='hang':
                for c in clients:no_response(c)
        finally:
            for c in clients:c.close()
            server.kill()
        committed=point.startswith('after_') or point=='before_group_replies'
        server=Server(binary,directory)
        try:
            c=Client(server.address)
            for name in ['T','W','U']:
                for i in range(160):
                    expected=None if name=='W' and i<16 and committed else f'{name}{i}'.encode()
                    assert c.get(i+1,name)==expected,(point,name,i)
                info=c.request('$JS.API.STREAM.INFO.'+name,{})
                assert info['state']['messages']==160+(16 if name=='T' and committed else -16 if name=='W' and committed else 0),info
            for i in range(16):assert c.get(161+i)==(f'new{i}'.encode() if committed else None)
            info=c.request('$JS.API.CONSUMER.INFO.W.D',{})
            assert info['num_ack_pending']==64-(16 if committed else 0),info
            for name,subject in [('T','test.x'),('W','work.x'),('U','untouched.x')]:
                for i in [0,31,64,159]:
                    r=c.publish_with_id(b'ignored',f'{name}-id{i}',subject);assert r['duplicate'] and r['seq']==i+1,r
            c.close()
        finally:server.kill()
    return {'point':point,'action':action,'seeded_messages':480,'seeded_pending':64,'records':32,'group_recovered':committed,'untouched_stream_verified':True,'passed':True}

def cross_version(writer,reader):
    with tempfile.TemporaryDirectory(prefix='screech-tree-codec-') as tmp:
        directory=Path(tmp);seed(writer,directory,count=710) # Force a checkpoint including nested consumer indexes.
        server=Server(reader,directory)
        try:
            c=Client(server.address)
            for name,subject in [('T','test.x'),('W','work.x'),('U','untouched.x')]:
                for i in [0,31,64,709]:
                    assert c.get(i+1,name)==f'{name}{i}'.encode()
                    r=c.publish_with_id(b'ignored',f'{name}-id{i}',subject);assert r['duplicate'] and r['seq']==i+1
            assert c.request('$JS.API.CONSUMER.INFO.W.D',{})['num_ack_pending']==64
            c.ack(c.pull(stream='W'))
            c.close()
        finally:server.kill()
    return {'writer':str(writer),'reader':str(reader),'checkpoint_and_log_compatible':True,'passed':True}

if __name__=='__main__':
    p=argparse.ArgumentParser();p.add_argument('binary');p.add_argument('--previous');p.add_argument('--output',type=Path);args=p.parse_args()
    results=[run_case(args.binary,point,action) for point in ['before_group_commit','before_group_store_commit','after_group_store_commit','after_group_commit','before_group_replies'] for action in ['hang','exit']]
    if args.previous:results.extend([cross_version(args.previous,args.binary),cross_version(args.binary,args.previous)])
    text=json.dumps(results,indent=2);print(text)
    if args.output:args.output.write_text(text+'\n')
