#!/usr/bin/env python3
import argparse,json,select,tempfile,time
from pathlib import Path
from crash_tests import Server
from wire import Client

def bootstrap(binary,directory,retention='workqueue',dedup=False):
    server=Server(binary,directory)
    try:
        c=Client(server.address)
        response=c.request('$JS.API.STREAM.CREATE.T',{'name':'T','subjects':['test.>'],'storage':'file','retention':retention,'duplicate_window':60000000000 if dedup else 0})
        assert 'error' not in response,response
        c.create_consumer();c.publish(b'value');c.close()
    finally:server.kill()
def trigger(c,subject,payload=b'{}'):
    c.send(f'SUB _INBOX.fault 999\r\nPUB {subject} _INBOX.fault {len(payload)}\r\n'.encode()+payload+b'\r\n')
def fault_server(binary,directory,point,action='hang'):return Server(binary,directory,point,action)
def run(binary):
    results=[]
    for action in ('hang','exit'):
        for point,persisted in [('before_delivery_commit',False),('after_delivery_commit',True),('before_delivery_send',True)]:
            with tempfile.TemporaryDirectory(prefix='screech-delivery-') as tmp:
                directory=Path(tmp);bootstrap(binary,directory);server=fault_server(binary,directory,point,action)
                try:
                    c=Client(server.address);trigger(c,'$JS.API.CONSUMER.MSG.NEXT.T.D',b'{"batch":1,"no_wait":true}')
                    server.wait_fault();c.close()
                finally:server.kill()
                server=Server(binary,directory)
                try:
                    c=Client(server.address);info=c.request('$JS.API.CONSUMER.INFO.T.D',{});assert info['delivered']['consumer_seq']==int(persisted),info
                    time.sleep(.12);m=c.pull();assert m and m['payload']==b'value';assert int(m['reply'].split('.')[6])==(2 if persisted else 1);c.ack(m);assert c.get(1) is None;c.close()
                finally:server.kill()
                results.append({'test':point,'action':action,'reservation_survived':persisted,'passed':True})
        for point,persisted in [('before_ack_commit',False),('after_ack_commit',True),('before_ack_reply',True)]:
            with tempfile.TemporaryDirectory(prefix='screech-ack-') as tmp:
                directory=Path(tmp);bootstrap(binary,directory);server=fault_server(binary,directory,point,action)
                try:
                    c=Client(server.address);m=c.pull();assert m
                    trigger(c,m['reply'],b'+ACK');server.wait_fault()
                    ready=select.select([c.socket],[],[],.02)[0]
                    if ready:assert c.socket.recv(1)==b'','ACK confirmation escaped early'
                    c.close()
                finally:server.kill()
                server=Server(binary,directory)
                try:
                    c=Client(server.address);info=c.request('$JS.API.CONSUMER.INFO.T.D',{});assert info['num_ack_pending']==int(not persisted),info
                    assert (c.get(1) is None)==persisted
                    if not persisted:time.sleep(.12);m=c.pull();assert m;c.ack(m)
                    assert c.get(1) is None;c.close()
                finally:server.kill()
                results.append({'test':point,'action':action,'ack_and_retention_survived':persisted,'passed':True})
        for point in ('before_checkpoint_commit','after_checkpoint_commit','before_wal_restart','after_wal_restart'):
            with tempfile.TemporaryDirectory(prefix='screech-checkpoint-') as tmp:
                directory=Path(tmp);bootstrap(binary,directory,retention='limits',dedup=True)
                server=Server(binary,directory)
                try:
                    c=Client(server.address);a=c.publish_with_id(b'dedup','id');m=c.pull();c.ack(m);pending=c.pull();assert pending;c.close()
                finally:server.kill()
                server=fault_server(binary,directory,point,action)
                try:
                    c=Client(server.address);trigger(c,'$JS.API.SCREECH.CHECKPOINT');server.wait_fault();c.close()
                finally:server.kill()
                server=Server(binary,directory)
                try:
                    c=Client(server.address);b=c.publish_with_id(b'ignored','id');assert b['duplicate'] and b['seq']==a['seq'];info=c.request('$JS.API.CONSUMER.INFO.T.D',{});assert info['num_ack_pending']==1;assert info['ack_floor']['consumer_seq']==1;time.sleep(.12);m=c.pull();assert m['payload']==b'dedup';c.ack(m);c.close()
                finally:server.kill()
                results.append({'test':point,'action':action,'dedup_and_consumer_state_survived':True,'passed':True})
    for point,persisted in [('before_consumer_commit',False),('after_consumer_commit',True)]:
        for action in ('hang','exit'):
            with tempfile.TemporaryDirectory(prefix='screech-consumer-create-') as tmp:
                directory=Path(tmp);bootstrap(binary,directory,retention='limits');server=fault_server(binary,directory,point,action)
                try:
                    c=Client(server.address);trigger(c,'$JS.API.CONSUMER.DURABLE.CREATE.T.E',json.dumps({'stream_name':'T','config':{'durable_name':'E','ack_policy':'explicit'}}).encode());server.wait_fault();c.close()
                finally:server.kill()
                server=Server(binary,directory)
                try:
                    c=Client(server.address);info=c.request('$JS.API.CONSUMER.INFO.T.E',{});assert ('error' not in info)==persisted,info
                    assert c.request('$JS.API.CONSUMER.INFO.T.D',{})['delivered']['consumer_seq']==0;c.close()
                finally:server.kill()
                results.append({'test':point,'action':action,'consumer_creation_survived':persisted,'passed':True})
    for point in ('before_expiration_commit','after_expiration_commit'):
        with tempfile.TemporaryDirectory(prefix='screech-expiration-') as tmp:
            directory=Path(tmp);server=Server(binary,directory)
            try:
                c=Client(server.address);r=c.request('$JS.API.STREAM.CREATE.T',{'name':'T','subjects':['test.>'],'storage':'file','retention':'workqueue','max_age':200000000});assert 'error' not in r,r
                c.create_consumer();c.publish(b'expiring');assert c.pull();c.close()
            finally:server.kill()
            server=fault_server(binary,directory,point)
            try:server.wait_fault()
            finally:server.kill()
            server=Server(binary,directory)
            try:
                c=Client(server.address);assert c.get(1) is None;info=c.request('$JS.API.CONSUMER.INFO.T.D',{});assert info['num_ack_pending']==0;assert c.publish(b'next')==2;c.close()
            finally:server.kill()
            results.append({'test':point,'expired_message_stays_absent':True,'passed':True})
    with tempfile.TemporaryDirectory(prefix='screech-ack-next-') as tmp:
        directory=Path(tmp);bootstrap(binary,directory);server=Server(binary,directory)
        try:
            c=Client(server.address);c.publish(b'next');old=c.pull();following=c.request_message(old['reply'],b'+NXT');assert following['payload']==b'next';assert c.get(1) is None;c.ack(following);assert c.get(2) is None;c.close()
        finally:server.kill()
        results.append({'test':'ACK next commits current ACK and fetches next','passed':True})
    # Delete/recreate must fence the old ACK even if stream/consumer names match.
    with tempfile.TemporaryDirectory(prefix='screech-generation-') as tmp:
        directory=Path(tmp);bootstrap(binary,directory);server=Server(binary,directory)
        try:
            c=Client(server.address);old=c.pull();assert c.request('$JS.API.CONSUMER.DELETE.T.D',{})['success'];c.create_consumer();new=c.pull();c.ack(old,confirm=False);c.flush();assert c.get(1)==b'value';c.ack(new);assert c.get(1) is None;c.close()
        finally:server.kill()
        results.append({'test':'delete/recreate fences old ACK','passed':True})
    # Unknown publish outcome retries deduplicate after process death.
    with tempfile.TemporaryDirectory(prefix='screech-dedup-') as tmp:
        directory=Path(tmp);bootstrap(binary,directory,retention='limits',dedup=True);server=fault_server(binary,directory,'before_publish_ack')
        try:
            c=Client(server.address);headers=b'NATS/1.0\r\nNats-Msg-Id: ambiguous\r\n\r\n';c.send(f'HPUB test.x _INBOX.unknown {len(headers)} {len(headers)+5}\r\n'.encode()+headers+b'value\r\n');server.wait_fault();c.close()
        finally:server.kill()
        server=Server(binary,directory)
        try:
            c=Client(server.address);ack=c.publish_with_id(b'retry','ambiguous');assert ack['seq']==2 and ack['duplicate'];assert c.get(2)==b'value';c.close()
        finally:server.kill()
        results.append({'test':'ambiguous publish retry after crash deduplicates','passed':True})
    return results
if __name__=='__main__':
    p=argparse.ArgumentParser();p.add_argument('binary');p.add_argument('--output',type=Path);a=p.parse_args();result=run(str(Path(a.binary).resolve()));text=json.dumps(result,indent=2);print(text)
    if a.output:a.output.write_text(text+'\n')
