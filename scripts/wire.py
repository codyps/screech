"""Tiny synchronous protocol driver shared by crash and VM tests, independent of clients."""
import base64
import json
import socket
import uuid

class Client:
    def __init__(self, address, timeout=5):
        self.socket = socket.create_connection(address, timeout=timeout)
        self.socket.settimeout(timeout)
        self.file = self.socket.makefile('rb')
        assert self.file.readline().startswith(b'INFO ')
        self.send(b'CONNECT {"verbose":false,"headers":true,"no_responders":true}\r\n')
        self.counter = 0
        self.inbox = "_INBOX.test." + uuid.uuid4().hex
    def close(self):
        self.file.close()
        self.socket.close()
    def send(self, data):
        self.socket.sendall(data)
    def flush(self):
        self.send(b'PING\r\n')
        while True:
            line = self.file.readline()
            if line == b'PING\r\n': self.send(b'PONG\r\n')
            elif line == b'PONG\r\n': return
            else: raise AssertionError(repr(line))
    def request(self, subject, value):
        payload = value if isinstance(value, bytes) else json.dumps(value).encode()
        self.counter += 1
        sid = str(self.counter)
        inbox = self.inbox + '.' + sid
        self.send(f'SUB {inbox} {sid}\r\nUNSUB {sid} 1\r\nPUB {subject} {inbox} {len(payload)}\r\n'.encode() + payload + b'\r\n')
        while True:
            line = self.file.readline()
            if line == b'PING\r\n': self.send(b'PONG\r\n'); continue
            if not line: raise EOFError('server disconnected')
            assert line.startswith(b'MSG '), line
            length = int(line.split()[-1])
            data = self.file.read(length)
            assert self.file.read(2) == b'\r\n'
            return json.loads(data)
    def create(self, name='T', subject='test.>'):
        result = self.request('$JS.API.STREAM.CREATE.' + name,
                              {'name': name, 'subjects': [subject], 'storage': 'file',
                               'retention': 'limits', 'num_replicas': 1, 'duplicate_window': 120000000000})
        assert 'error' not in result, result
        return result
    def publish(self, payload, subject='test.x'):
        result = self.request(subject, payload)
        assert 'error' not in result, result
        return result['seq']
    def get(self, seq, name='T'):
        result = self.request('$JS.API.STREAM.MSG.GET.' + name, {'seq': seq})
        if 'error' in result: return None
        return base64.b64decode(result['message']['data'])

    def read_message(self):
        while True:
            line=self.file.readline()
            if line==b'PING\r\n':self.send(b'PONG\r\n');continue
            if not line:raise EOFError('server disconnected')
            if line.startswith(b'-ERR'):raise RuntimeError(line)
            fields=line.split()
            assert fields[0] in (b'MSG',b'HMSG'),line
            has_headers=fields[0]==b'HMSG'
            total=int(fields[-1]);header_len=int(fields[-2]) if has_headers else 0
            reply=fields[3].decode() if len(fields)==(6 if has_headers else 5) else None
            content=self.file.read(total);assert len(content)==total and self.file.read(2)==b'\r\n'
            headers=content[:header_len];payload=content[header_len:]
            status=int(headers.split(b'\r\n',1)[0].split()[1]) if headers.startswith(b'NATS/1.0 ') else None
            return {'subject':fields[1].decode(),'reply':reply,'headers':headers,'payload':payload,'status':status}
    def request_message(self,subject,payload):
        self.counter+=1;sid=str(self.counter);inbox=self.inbox+'.raw.'+sid
        self.send(f'SUB {inbox} {sid}\r\nUNSUB {sid} 1\r\nPUB {subject} {inbox} {len(payload)}\r\n'.encode()+payload+b'\r\n')
        return self.read_message()
    def create_consumer(self,name='D',stream='T',ack_wait=100000000,max_pending=1000):
        result=self.request(f'$JS.API.CONSUMER.DURABLE.CREATE.{stream}.{name}',{'stream_name':stream,'config':{'durable_name':name,'ack_policy':'explicit','ack_wait':ack_wait,'max_ack_pending':max_pending,'deliver_policy':'all','replay_policy':'instant'}})
        assert 'error' not in result,result
        return result
    def pull(self,name='D',stream='T',no_wait=True):
        msg=self.request_message(f'$JS.API.CONSUMER.MSG.NEXT.{stream}.{name}',json.dumps({'batch':1,'no_wait':no_wait,'expires':2000000000}).encode())
        return None if msg['status'] in (404,408) else msg
    def ack(self,message,action=b'+ACK',confirm=True):
        assert message['reply']
        if confirm:
            response=self.request_message(message['reply'],action)
            assert response['payload']==b'' and response['status'] is None,response
        else:self.send(f"PUB {message['reply']} {len(action)}\r\n".encode()+action+b'\r\n')
    def publish_with_id(self,payload,msg_id,subject='test.x'):
        headers=f'NATS/1.0\r\nNats-Msg-Id: {msg_id}\r\n\r\n'.encode()
        self.counter+=1;sid=str(self.counter);inbox=self.inbox+'.id.'+sid
        self.send(f'SUB {inbox} {sid}\r\nUNSUB {sid} 1\r\nHPUB {subject} {inbox} {len(headers)} {len(headers)+len(payload)}\r\n'.encode()+headers+payload+b'\r\n')
        response=json.loads(self.read_message()['payload']);assert 'error' not in response,response
        return response
