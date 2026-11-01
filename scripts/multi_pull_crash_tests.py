#!/usr/bin/env python3
"""Crash the third durable chunk while a larger pull is draining ready data."""
import argparse,json,tempfile
from pathlib import Path
from crash_tests import Server
from wire import Client
from batch_crash_tests import no_response

def run(binary):
    results=[]
    for point,committed in [('before_delivery_batch_commit',128),('after_delivery_batch_commit',192)]:
        for action in ['hang','exit']:
            with tempfile.TemporaryDirectory(prefix='screech-multi-pull-') as tmp:
                directory=Path(tmp);server=Server(binary,directory)
                try:
                    c=Client(server.address)
                    r=c.request('$JS.API.STREAM.CREATE.T',{'name':'T','subjects':['test.>'],'storage':'file','retention':'workqueue'});assert 'error' not in r
                    c.create_consumer(ack_wait=60000000000)
                    for i in range(256):c.publish(str(i).encode())
                    c.close()
                finally:server.kill()
                server=Server(binary,directory,point,action,extra_env={'SCREECH_FAULT_AFTER':'3'})
                try:
                    c=Client(server.address)
                    payload=b'{"batch":256,"expires":60000000000}'
                    c.send(f'SUB _PULL 1\r\nPUB $JS.API.CONSUMER.MSG.NEXT.T.D _PULL {len(payload)}\r\n'.encode()+payload+b'\r\n')
                    server.wait_fault()
                    context=json.loads(Path(str(server.marker)+'.context').read_text());assert context==list(range(129,193)),context
                    # Abrupt exit may lose frames queued after the first commits.
                    observed=[]
                    if action=='hang':
                        observed=[c.read_message()['payload'] for _ in range(128)]
                    else:
                        try:
                            while True:observed.append(c.read_message()['payload'])
                        except (EOFError,OSError,AssertionError):pass
                    assert len(observed)<=128 and observed==[str(i).encode() for i in range(len(observed))],observed
                    if action=='hang':
                        c.send(b'PING\r\n');assert c.file.readline()==b'PONG\r\n'
                        no_response(c)
                    c.close()
                finally:server.kill()
                server=Server(binary,directory)
                try:
                    c=Client(server.address);info=c.request('$JS.API.CONSUMER.INFO.T.D',{})
                    assert info['delivered']['consumer_seq']==committed and info['num_ack_pending']==committed,info
                    r=c.request('$JS.API.CONSUMER.CREATE.T.D',{'action':'update','stream_name':'T','config':{'durable_name':'D','ack_policy':'explicit','ack_wait':1,'max_ack_pending':1000,'deliver_policy':'all','replay_policy':'instant'}})
                    assert 'error' not in r,r
                    for i in range(256):
                        message=c.pull();assert message['payload']==str(i).encode()
                        assert int(message['reply'].split('.')[6])==(2 if i<committed else 1)
                        c.ack(message)
                    assert c.request('$JS.API.STREAM.INFO.T',{})['state']['messages']==0
                    assert c.request('$JS.API.CONSUMER.INFO.T.D',{})['num_ack_pending']==0
                    c.close()
                finally:server.kill()
                results.append({'point':point,'occurrence':3,'action':action,'received_before_fault':len(observed),'reservations_recovered':committed,'backlog_checked':256,'passed':True})
    return results
if __name__=='__main__':
    p=argparse.ArgumentParser();p.add_argument('binary');p.add_argument('--output',type=Path);args=p.parse_args()
    text=json.dumps(run(args.binary),indent=2);print(text)
    if args.output:args.output.write_text(text+'\n')
