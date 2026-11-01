#!/usr/bin/env python3
"""Inject real WAL/database write and sync failures, then kill/recover the server."""
import argparse,json,tempfile,time
from pathlib import Path
from crash_tests import Server
from wire import Client

def attempt(call):
    try:return call()
    except (OSError,AssertionError,EOFError,RuntimeError):return None

def run(binary,shim):
    results=[]
    for phase in ['reservation','ack','checkpoint','reservation-reuse']:
        for mode in ['sync-before-eio','sync-after-eio','write-enospc','short-write']:
            with tempfile.TemporaryDirectory(prefix='screech-io-') as tmp:
                directory=Path(tmp);arm=directory/'armed';hit=directory/'hit'
                server=Server(binary,directory,extra_env={'LD_PRELOAD':str(shim),'SCREECH_IO_ARM':str(arm),'SCREECH_IO_HIT':str(hit),'SCREECH_IO_MODE':mode,'SCREECH_IO_TARGET':'db' if phase=='checkpoint' else 'wal'})
                try:
                    c=Client(server.address)
                    r=c.request('$JS.API.STREAM.CREATE.T',{'name':'T','subjects':['test.>'],'storage':'file','retention':'workqueue'})
                    assert 'error' not in r,r
                    c.create_consumer();c.publish(b'old');old=c.pull();c.ack(old);c.publish(b'value')
                    pending=c.pull() if phase in ['ack','checkpoint'] else None
                    if phase=='reservation-reuse':assert c.request('$JS.API.SCREECH.CHECKPOINT',{})['success']
                    arm.touch()
                    if phase=='ack':result=attempt(lambda:(c.ack(pending),True)[1]);success=result is not None
                    elif phase=='checkpoint':result=attempt(lambda:c.request('$JS.API.SCREECH.CHECKPOINT',{}));success=bool(result and result.get('success'))
                    else:result=attempt(c.pull);success=bool(result and result.get('payload')==b'value')
                    assert hit.exists(),(phase,mode,'interposer never hit',result)
                    assert success==(mode=='short-write'),(phase,mode,result,success)
                    c.close()
                    if mode!='short-write':
                        # The engine must not continue acknowledging writes after uncertain I/O.
                        rejected=Client(server.address)
                        try:assert attempt(lambda:rejected.publish(b'forbidden')) is None
                        finally:rejected.close()
                finally:server.kill()
                server=Server(binary,directory)
                try:
                    c=Client(server.address)
                    assert c.get(1) is None,'earlier confirmed ACK was undone'
                    info=c.request('$JS.API.CONSUMER.INFO.T.D',{})
                    retained=c.get(2)
                    if phase=='ack':
                        assert retained in [None,b'value']
                        committed=retained is None
                        assert info['num_ack_pending']==int(not committed),info
                        assert info['ack_floor']['consumer_seq']==(2 if committed else 1),info
                        if mode=='short-write':assert committed
                    else:
                        assert retained==b'value','earlier acknowledged publication was lost'
                        committed=info['delivered']['consumer_seq']==2
                        assert info['delivered']['consumer_seq'] in [1,2],info
                        assert info['num_ack_pending']==int(committed),info
                        assert info['ack_floor']['consumer_seq']==1,info
                        if phase=='checkpoint' or mode=='short-write':assert committed
                    if retained is not None:
                        time.sleep(.12);message=c.pull();assert message and message['payload']==b'value';c.ack(message)
                    assert c.get(2) is None
                    assert c.publish(b'after')==3,'stream sequence regressed'
                    c.close()
                finally:server.kill()
                results.append({'phase':phase,'mode':mode,'syscall':hit.read_text(),'operation_recovered':committed,'earlier_acknowledgements_preserved':True,'passed':True})
    return results
if __name__=='__main__':
    p=argparse.ArgumentParser();p.add_argument('binary');p.add_argument('--shim',type=lambda x:Path(x).resolve(),required=True);p.add_argument('--output',type=Path);args=p.parse_args()
    text=json.dumps(run(args.binary,args.shim),indent=2);print(text)
    if args.output:args.output.write_text(text+'\n')
