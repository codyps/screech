#!/usr/bin/env python3
"""Real process crashes: reservation/ACK batches are atomic and no response is early."""
import argparse,json,select,tempfile,time
from pathlib import Path
from crash_tests import Server
from wire import Client
from consumer_crash_tests import trigger

def bootstrap(binary,directory,count):
    server=Server(binary,directory)
    try:
        c=Client(server.address)
        result=c.request('$JS.API.STREAM.CREATE.T',{'name':'T','subjects':['test.>'],'storage':'file','retention':'workqueue'})
        assert 'error' not in result,result
        c.create_consumer(ack_wait=100000000)
        for i in range(count):c.publish(str(i).encode())
        c.close()
    finally:server.kill()

def no_response(c):
    # readline/read_message can read ahead past a PONG or the last expected MSG.
    # Inspect that buffer as well as the socket, without consuming or timing out
    # the BufferedReader (a timed-out reader cannot be safely reused).
    timeout=c.socket.gettimeout();c.socket.setblocking(False)
    try:
        data=c.file.peek(1)
        if not data and select.select([c.socket],[],[],.02)[0]:data=c.file.peek(1)
        assert not data, f'response escaped before the durability boundary: {data!r}'
    finally:c.socket.settimeout(timeout)

def run(binary):
    results=[]
    for action in ('hang','exit'):
        for point,persisted in [('before_delivery_batch_commit',False),('after_delivery_batch_commit',True),('before_delivery_send',True),('before_commit',False),('after_commit',True)]:
            with tempfile.TemporaryDirectory(prefix='screech-batch-reserve-') as tmp:
                directory=Path(tmp);bootstrap(binary,directory,16)
                server=Server(binary,directory,point,action)
                try:
                    c=Client(server.address);trigger(c,'$JS.API.CONSUMER.MSG.NEXT.T.D',b'{"batch":16,"expires":5000000000}')
                    server.wait_fault();no_response(c);c.close()
                finally:server.kill()
                server=Server(binary,directory)
                try:
                    c=Client(server.address);info=c.request('$JS.API.CONSUMER.INFO.T.D',{})
                    assert info['delivered']['consumer_seq']==(16 if persisted else 0),info
                    assert info['num_ack_pending']==(16 if persisted else 0),info
                    time.sleep(.12)
                    for i in range(16):
                        m=c.pull();assert m and m['payload']==str(i).encode(),m
                        assert int(m['reply'].split('.')[6])==(2 if persisted else 1),m
                        c.ack(m)
                    assert c.request('$JS.API.STREAM.INFO.T',{})['state']['messages']==0;c.close()
                finally:server.kill()
                results.append({'test':point,'action':action,'batch_size':16,'reservation_survived':persisted,'passed':True})
        for point,persisted in [('before_ack_batch_commit',False),('after_ack_batch_commit',True)]:
            with tempfile.TemporaryDirectory(prefix='screech-batch-ack-') as tmp:
                directory=Path(tmp);bootstrap(binary,directory,64)
                server=Server(binary,directory,point,action);clients=[]
                try:
                    c=Client(server.address)
                    # All reservations are durably established before concurrent ACK ingress.
                    trigger(c,'$JS.API.CONSUMER.MSG.NEXT.T.D',b'{"batch":64,"expires":5000000000}')
                    messages=[c.read_message() for _ in range(64)]
                    clients=[Client(server.address) for _ in messages]
                    for seq,client in enumerate(clients,1):client.send(f'SUB _INBOX.batch.{seq} 999\r\n'.encode());client.flush()
                    for seq,(client,m) in enumerate(zip(clients,messages),1):
                        payload=b'+ACK';client.send(f'PUB {m["reply"]} _INBOX.batch.{seq} 4\r\n'.encode()+payload+b'\r\n')
                    server.wait_fault()
                    sequences=json.loads(Path(str(server.marker)+'.context').read_text());assert len(sequences)>=2,sequences
                    for seq in sequences:
                        try:no_response(clients[seq-1])
                        except AssertionError as error:raise AssertionError((point,action,sequences,seq,str(error))) from error
                    c.close()
                finally:
                    for client in clients:client.close()
                    server.kill()
                server=Server(binary,directory)
                try:
                    c=Client(server.address)
                    for seq in sequences:assert (c.get(seq) is None)==persisted,(point,seq)
                    info=c.request('$JS.API.CONSUMER.INFO.T.D',{})
                    # All remaining data has a reservation; no partial ACK/retention effect.
                    stored=c.request('$JS.API.STREAM.INFO.T',{})['state']['messages']
                    assert stored==info['num_ack_pending'],(stored,info)
                    c.close()
                finally:server.kill()
                results.append({'test':point,'action':action,'batch_size':len(sequences),'ack_and_retention_survived':persisted,'passed':True})
    return results
if __name__=='__main__':
    p=argparse.ArgumentParser();p.add_argument('binary');p.add_argument('--output',type=Path);args=p.parse_args()
    text=json.dumps(run(args.binary),indent=2);print(text)
    if args.output:args.output.write_text(text+'\n')
