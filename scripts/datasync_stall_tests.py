#!/usr/bin/env python3
"""Kill actual fdatasync waits without exposing reservations or ACK success early."""
import argparse,json,tempfile
from pathlib import Path
from crash_tests import Server
from wire import Client
from group_crash_tests import wait_until
from batch_crash_tests import no_response

def run(binary,shim):
    results=[]
    for phase in ['reservation','ack']:
        # WAL reuse synchronizes its header before synchronizing the commit.
        for boundary,skip in [('commit',0),('reused-wal-header',0),('reused-wal-commit',1)]:
            for mode in ['sync-hang-before','sync-hang-after']:
                with tempfile.TemporaryDirectory(prefix='screech-datasync-') as tmp:
                    directory=Path(tmp);arm=directory/'armed';hit=directory/'hit'
                    server=Server(binary,directory,extra_env={'LD_PRELOAD':str(shim),'SCREECH_IO_ARM':str(arm),'SCREECH_IO_HIT':str(hit),'SCREECH_IO_MODE':mode,'SCREECH_IO_TARGET':'wal','SCREECH_IO_AFTER':str(skip)})
                    try:
                        c=Client(server.address)
                        r=c.request('$JS.API.STREAM.CREATE.T',{'name':'T','subjects':['test.>'],'storage':'file','retention':'workqueue'});assert 'error' not in r
                        c.create_consumer(ack_wait=60000000000)
                        for i in range(64):c.publish(f'v{i}'.encode())
                        # Preserve an earlier confirmed ACK through every later failure.
                        prior=c.pull();assert prior['payload']==b'v0';c.ack(prior)
                        pending=c.pull() if phase=='ack' else None
                        if boundary.startswith('reused-'):
                            assert c.request('$JS.API.SCREECH.CHECKPOINT',{})['success']
                        c.send(b'SUB _RESULT 909\r\n');c.flush()
                        observer=Client(server.address);observer.send(b'SUB core.probe 1\r\n');observer.flush()
                        arm.touch()
                        if phase=='ack':subject=pending['reply'];body=b'+ACK'
                        else:subject='$JS.API.CONSUMER.MSG.NEXT.T.D';body=b'{"batch":1,"expires":60000000000}'
                        c.send(f'PUB {subject} _RESULT {len(body)}\r\n'.encode()+body+b'\r\n')
                        wait_until(hit.exists);assert hit.read_text()=='fdatasync'
                        # The same read loop continues while its durable response waits.
                        c.send(b'PUB core.probe 4\r\nlive\r\nPING\r\n')
                        assert c.file.readline()==b'PONG\r\n'
                        assert observer.read_message()['payload']==b'live'
                        no_response(c);c.close();observer.close()
                    finally:server.kill()
                    server=Server(binary,directory)
                    try:
                        c=Client(server.address);info=c.request('$JS.API.CONSUMER.INFO.T.D',{})
                        if phase=='ack':
                            committed=c.get(2) is None
                            assert info['ack_floor']['consumer_seq']==1+int(committed),info
                            assert info['num_ack_pending']==int(not committed),info
                        else:
                            committed=info['delivered']['consumer_seq']==2
                            assert info['delivered']['consumer_seq'] in [1,2],info
                            assert info['num_ack_pending']==int(committed),info
                            assert c.get(2)==b'v1'
                        assert c.get(1) is None,'earlier confirmed ACK was undone'
                        if mode=='sync-hang-after' and boundary!='reused-wal-header':assert committed,'real successful sync was not recovered'
                        for i in range(2,64):assert c.get(i+1)==f'v{i}'.encode()
                        # Continue with independent fetch and ACK commits after recovery.
                        message=c.pull();assert message['payload'] in [b'v1',b'v2'];c.ack(message)
                        c.close()
                    finally:server.kill()
                    results.append({'phase':phase,'boundary':boundary,'mode':mode,'syscall':'fdatasync','same_connection_core_progress':True,'no_early_durable_reply':True,'committed':committed,'passed':True})
    return results
if __name__=='__main__':
    p=argparse.ArgumentParser();p.add_argument('binary');p.add_argument('--shim',type=lambda x:Path(x).resolve(),required=True);p.add_argument('--output',type=Path);args=p.parse_args()
    text=json.dumps(run(args.binary,args.shim),indent=2);print(text)
    if args.output:args.output.write_text(text+'\n')
