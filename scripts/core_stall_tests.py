#!/usr/bin/env python3
"""Captured Core PUB/HPUB and same-connection control frames bypass stalled storage."""
import argparse,json,select,tempfile,time
from pathlib import Path
from crash_tests import Server
from wire import Client
from batch_crash_tests import no_response
from group_crash_tests import ingress,wait_until

def packet(subject,reply,payload,headers=b''):
    if headers:return f'HPUB {subject} {reply} {len(headers)} {len(headers)+len(payload)}\r\n'.encode()+headers+payload+b'\r\n'
    return f'PUB {subject} {reply} {len(payload)}\r\n'.encode()+payload+b'\r\n'

def run(binary):
    results=[]
    for point in ['before_commit','after_commit','before_publish_ack','before_group_store_commit','after_group_store_commit']:
        grouped='group' in point
        with tempfile.TemporaryDirectory(prefix='screech-core-stall-') as tmp:
            directory=Path(tmp);server=Server(binary,directory)
            try:c=Client(server.address);c.create();c.close()
            finally:server.kill()
            gate=directory/'gate';server=Server(binary,directory,point,'hang',extra_env={'SCREECH_GROUP_GATE':str(gate)} if grouped else None)
            try:
                observer=Client(server.address);observer.send(b'SUB test.> 1\r\nSUB plain 2\r\n');observer.flush()
                publisher=Client(server.address);publisher.send(b'SUB _PUBACK 9\r\n');publisher.flush()
                initial=[f'initial-{i}'.encode() for i in range(8 if grouped else 1)]
                publisher.send(b''.join(packet('test.a','_PUBACK',value) for value in initial)+b'PING\r\n')
                assert publisher.file.readline()==b'PONG\r\n','same-connection PING waited for storage'
                if grouped:
                    ingress(gate,len(initial));wait_until(lambda:Path(str(gate)+'.ready').exists());Path(str(gate)+'.release').touch()
                server.wait_fault()
                for value in initial:assert observer.read_message()['payload']==value
                headers=b'NATS/1.0\r\nX-Stall: yes\r\n\r\n'
                publisher.send(packet('test.b','_PUBACK',b'later')+packet('test.c','_PUBACK',b'bin\0ary',headers)+b'PUB plain 4\r\nlive\r\nPING\r\n')
                assert publisher.file.readline()==b'PONG\r\n','read loop stalled behind its first publication'
                messages=[observer.read_message() for _ in range(3)]
                assert [m['payload'] for m in messages]==[b'later',b'bin\0ary',b'live'],messages
                assert messages[1]['headers']==headers
                other=Client(server.address);other.send(b'SUB _OTHERACK 3\r\n');other.flush()
                other.send(packet('test.d','_OTHERACK',b'other')+b'PING\r\n')
                assert other.file.readline()==b'PONG\r\n'
                assert observer.read_message()['payload']==b'other'
                no_response(publisher);no_response(other);no_response(observer)
                publisher.close();other.close();observer.close()
            finally:server.kill()
            persisted=point in ['after_commit','before_publish_ack','after_group_store_commit']
            server=Server(binary,directory)
            try:
                c=Client(server.address)
                for seq,value in enumerate(initial,1):assert c.get(seq)==(value if persisted else None)
                assert c.get(len(initial)+1) is None,'queued tail committed past the stalled boundary'
                c.close()
            finally:server.kill()
            results.append({'point':point,'core_deliveries':len(initial)+4,'same_publisher_progress':True,'other_publisher_progress':True,'no_durable_ack_before_release':True,'committed_prefix_recovered':persisted,'passed':True})
    return results
if __name__=='__main__':
    p=argparse.ArgumentParser();p.add_argument('binary');p.add_argument('--output',type=Path);args=p.parse_args()
    text=json.dumps(run(args.binary),indent=2);print(text)
    if args.output:args.output.write_text(text+'\n')
