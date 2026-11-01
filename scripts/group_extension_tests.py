#!/usr/bin/env python3
"""Force arrivals during projection; verify bounded FIFO groups and durable replies."""
import argparse,json,tempfile
from pathlib import Path
from crash_tests import Server
from wire import Client
from group_crash_tests import bootstrap,ingress,subscribe,wait_until
from batch_crash_tests import no_response

def run_case(binary,schedule,point=None,action='hang',shim=None,mode=None):
    with tempfile.TemporaryDirectory(prefix='screech-group-extension-') as tmp:
        directory=Path(tmp);bootstrap(binary,directory,'publisher')
        gate=directory/'initial';extend=directory/'extend';hit=directory/'hit'
        env={'SCREECH_GROUP_GATE':str(gate),'SCREECH_GROUP_EXTEND_GATE':str(extend)}
        if mode:env.update(LD_PRELOAD=str(shim),SCREECH_IO_MODE=mode,SCREECH_IO_ARM=str(directory/'armed'),SCREECH_IO_HIT=str(hit),SCREECH_IO_TARGET='wal')
        server=Server(binary,directory,point,action,extra_env=env);clients=[];reader=None;monitor=None
        expected_count={'late':8,'fence':4,'count':32,'bytes':2}[schedule]
        payloads=[]
        def enqueue(payload):
            index=len(clients);c=Client(server.address);subscribe(c,f'_INBOX.pub.{index}');clients.append(c);payloads.append(payload)
            c.send(f'PUB test.x _INBOX.pub.{index} {len(payload)}\r\n'.encode()+payload+b'\r\n')
            ingress(gate,len(clients)+(1 if reader else 0)+1)
        def fence():
            nonlocal reader
            reader=Client(server.address);subscribe(reader,'_INBOX.fence')
            reader.send(b'PUB $JS.API.STREAM.INFO.T _INBOX.fence 2\r\n{}\r\n')
            ingress(gate,len(clients)+2)
        try:
            # Prime the WAL after startup; syscall faults below target a commit,
            # rather than the extra header sync of the first write after reuse.
            control=Client(server.address)
            result=control.request('$JS.API.STREAM.CREATE.U',{'name':'U','subjects':['unused.>'],'storage':'file'})
            control.close();assert 'error' not in result,result
            monitor=Client(server.address);monitor.send(b'SUB test.> 444\r\n');monitor.flush()
            enqueue(b'p0');enqueue(b'p1')
            wait_until(lambda:Path(str(gate)+'.ready').exists());Path(str(gate)+'.release').touch()
            # The initial two requests are now projected, with replies still private.
            wait_until(lambda:Path(str(extend)+'.ready').exists())
            for c in clients:no_response(c)
            if schedule=='late':
                for i in range(2,8):enqueue(f'p{i}'.encode())
            elif schedule=='fence':
                enqueue(b'p2');enqueue(b'p3');fence();enqueue(b'p4')
            elif schedule=='count':
                for i in range(2,34):enqueue(f'p{i}'.encode())
            else:
                enqueue(b'x'*(1024*1024));enqueue(b'p3')
            if reader is None:fence()
            # Core forwarding and the read loop remain live during assembly.
            for payload in payloads:assert monitor.read_message()['payload']==payload
            no_response(monitor)
            if mode:(directory/'armed').touch()
            Path(str(extend)+'.release').touch()
            if point:
                if mode:wait_until(hit.exists)
                else:server.wait_fault()
                rows=json.loads(Path(str(server.marker)+'.group').read_text())['records']
                assert len(rows)==expected_count,(schedule,len(rows),expected_count)
                assert all('Publish' in row['event'] for row in rows),rows
                for c in clients:no_response(c)
                no_response(reader)
            else:
                replies=[json.loads(c.read_message()['payload']) for c in clients]
                assert [r['seq'] for r in replies]==list(range(1,len(clients)+1)),replies
                fenced=json.loads(reader.read_message()['payload'])['state']['messages']
                # Only the explicit fence interrupts the publication sequence;
                # count/byte limits end a group but the later read sees all groups.
                assert fenced==(4 if schedule=='fence' else len(clients)),fenced
        finally:
            for c in clients:c.close()
            if reader:reader.close()
            if monitor:monitor.close()
            server.kill()
        server=Server(binary,directory)
        try:
            c=Client(server.address)
            committed=c.get(1) is not None
            if not mode:assert committed==(point is None or point.startswith('after_') or point=='before_group_replies')
            if mode=='sync-hang-after':assert committed
            persisted=expected_count if point and committed else 0 if point else len(payloads)
            for i,payload in enumerate(payloads):assert c.get(i+1)==(payload if i<persisted else None),(schedule,i,persisted)
            info=c.request('$JS.API.STREAM.INFO.T',{})['state'];assert info['messages']==persisted and info['last_seq']==persisted,info
            assert c.publish(b'after')==persisted+1
            c.close()
        finally:server.kill()
        return {'schedule':schedule,'point':point,'action':mode or (action if point else 'none'),'group_members':expected_count,'persisted':persisted,'passed':True}

def run(binary,shim):
    rows=[]
    for schedule in ['late','fence','count','bytes']:
        for action in ['hang','exit']:
            for point in ['before_group_commit','after_group_store_commit','before_group_replies']:
                rows.append(run_case(binary,schedule,point,action))
        rows.append(run_case(binary,schedule))
    for mode in ['sync-hang-before','sync-hang-after']:
        rows.append(run_case(binary,'late','capture_group',shim=shim,mode=mode))
    return rows
if __name__=='__main__':
    p=argparse.ArgumentParser();p.add_argument('binary');p.add_argument('--shim',type=lambda x:Path(x).resolve(),required=True);p.add_argument('--output',type=Path);args=p.parse_args()
    rows=run(args.binary,args.shim);text=json.dumps(rows,indent=2);print(text)
    if args.output:args.output.write_text(text+'\n')
