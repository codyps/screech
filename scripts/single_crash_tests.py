#!/usr/bin/env python3
"""ACK-and-next commits its ACK, retention deletion and next reservation atomically."""
import argparse,json,tempfile,time
from pathlib import Path
from crash_tests import Server
from wire import Client
from consumer_crash_tests import trigger
from batch_crash_tests import no_response

def run(binary):
    results=[]
    for action in ['hang','exit']:
        for point,persisted in [('before_ack_next_commit',False),('after_ack_next_commit',True),('before_commit',False),('after_commit',True),('before_ack_reply',True),('before_delivery_send',True)]:
            with tempfile.TemporaryDirectory(prefix='screech-ack-next-') as tmp:
                directory=Path(tmp);server=Server(binary,directory)
                try:
                    c=Client(server.address)
                    r=c.request('$JS.API.STREAM.CREATE.T',{'name':'T','subjects':['test.>'],'storage':'file','retention':'workqueue'})
                    assert 'error' not in r,r
                    c.create_consumer(max_pending=1);c.publish(b'first');c.publish(b'second');old=c.pull();assert old['payload']==b'first';c.close()
                finally:server.kill()
                server=Server(binary,directory,point,action)
                try:
                    c=Client(server.address);trigger(c,old['reply'],b'+NXT');server.wait_fault();no_response(c);c.close()
                finally:server.kill()
                server=Server(binary,directory)
                try:
                    c=Client(server.address)
                    info=c.request('$JS.API.CONSUMER.INFO.T.D',{})
                    assert info['delivered']['consumer_seq']==(2 if persisted else 1),info
                    assert info['ack_floor']['consumer_seq']==int(persisted),info
                    assert info['num_ack_pending']==1,info
                    assert (c.get(1) is None)==persisted
                    assert c.get(2)==b'second'
                    time.sleep(.12)
                    following=c.request_message(old['reply'],b'+NXT')
                    assert following['payload']==b'second',following
                    assert int(following['reply'].split('.')[6])==(2 if persisted else 1),following
                    assert c.get(1) is None
                    c.ack(following);assert c.get(2) is None
                    assert c.request('$JS.API.CONSUMER.INFO.T.D',{})['num_ack_pending']==0;c.close()
                finally:server.kill()
                results.append({'test':point,'action':action,'ack_and_reservation_survived':persisted,'retry_redelivery_verified':True,'passed':True})
    for body in [b'+NXT {"batch":0}', b'+NXT {"batch":1,"max_bytes":1}']:
        with tempfile.TemporaryDirectory(prefix='screech-ack-next-limits-') as tmp:
            directory=Path(tmp);server=Server(binary,directory)
            try:
                c=Client(server.address);c.request('$JS.API.STREAM.CREATE.T',{'name':'T','subjects':['test.>'],'storage':'file','retention':'workqueue'})
                c.create_consumer(max_pending=1);c.publish(b'first');c.publish(b'second');old=c.pull()
                status=c.request_message(old['reply'],body);assert status['status']==409,status
                assert c.get(1) is None and c.get(2)==b'second'
                info=c.request('$JS.API.CONSUMER.INFO.T.D',{});assert info['delivered']['consumer_seq']==1,info
                following=c.pull();assert following['payload']==b'second';c.ack(following);c.close()
            finally:server.kill()
            results.append({'test':'ACK next preserves ACK with rejected pull','body':body.decode(),'passed':True})
    with tempfile.TemporaryDirectory(prefix='screech-ack-next-wait-') as tmp:
        directory=Path(tmp);server=Server(binary,directory)
        try:
            c=Client(server.address);c.request('$JS.API.STREAM.CREATE.T',{'name':'T','subjects':['test.>'],'storage':'file','retention':'workqueue'})
            c.create_consumer(max_pending=1);c.publish(b'first');old=c.pull();trigger(c,old['reply'],b'+NXT')
            other=Client(server.address)
            try:
                deadline=time.monotonic()+5
                while other.get(1) is not None:
                    assert time.monotonic()<deadline;time.sleep(.005)
                other.publish(b'future')
            finally:other.close()
            following=c.read_message();assert following['payload']==b'future',following
            c.ack(following);assert c.get(2) is None;c.close()
        finally:server.kill()
        results.append({'test':'ACK next commits ACK then waits for future publication','passed':True})
    return results
if __name__=='__main__':
    p=argparse.ArgumentParser();p.add_argument('binary');p.add_argument('--output',type=Path);args=p.parse_args()
    text=json.dumps(run(args.binary),indent=2);print(text)
    if args.output:args.output.write_text(text+'\n')
