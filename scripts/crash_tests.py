#!/usr/bin/env python3
"""Kill actual server processes at deterministic commit/ack boundaries.
Run after cargo build --features fault-injection. Every child is reaped.
"""
import argparse
import json
import os
from pathlib import Path
import select
import subprocess
import tempfile
import time
from wire import Client

class Server:
    def __init__(self, binary, directory, point=None, action='hang', extra_env=None, extra_args=()):
        self.marker = directory / 'fault.marker'
        self.marker.unlink(missing_ok=True)
        env = os.environ.copy()
        for key in ('SCREECH_FAULT_POINT','SCREECH_FAULT_ACTION','SCREECH_FAULT_MARKER'): env.pop(key, None)
        if point:
            env.update(SCREECH_FAULT_POINT=point, SCREECH_FAULT_ACTION=action,
                       SCREECH_FAULT_MARKER=str(self.marker))
        if extra_env:env.update(extra_env)
        self.log = open(directory/'stderr.log', 'ab')
        self.process = subprocess.Popen([binary,'--port','0','--store',str(directory/'data.db'), *extra_args],
                                         stdout=subprocess.PIPE, stderr=self.log, env=env)
        if not select.select([self.process.stdout], [], [], 15)[0]:
            self.kill(); raise TimeoutError('server startup timeout')
        line = self.process.stdout.readline().decode().strip()
        if not line.startswith('LISTEN '):
            self.kill(); raise RuntimeError('server startup failed: ' + line)
        host, port = line.split()[1].rsplit(':', 1)
        self.address = (host, int(port))
    def wait_fault(self):
        deadline=time.monotonic()+10
        while not self.marker.exists():
            if time.monotonic()>deadline: raise TimeoutError('fault point not reached')
            time.sleep(.005)
    def kill(self):
        if self.process.poll() is None: self.process.kill()
        self.process.wait(timeout=5)
        self.process.stdout.close()
        self.log.close()

def run(binary):
    results=[]
    with tempfile.TemporaryDirectory(prefix='screech-crash-') as tmp:
        directory=Path(tmp)
        server=Server(binary,directory)
        try:
            c=Client(server.address);c.create()
            acknowledged={c.publish(str(i).encode()):str(i).encode() for i in range(50)}
            c.close()
        finally: server.kill()
        server=Server(binary,directory)
        try:
            c=Client(server.address)
            assert all(c.get(seq)==value for seq,value in acknowledged.items())
            c.close()
        finally: server.kill()
        results.append({'test':'SIGKILL preserves acknowledged messages','acknowledged':len(acknowledged),'passed':True})
    for point, present in [('before_commit',False),('after_commit',True),('before_publish_ack',True)]:
        for action in ('hang','exit'):
            with tempfile.TemporaryDirectory(prefix='screech-fault-') as tmp:
                directory=Path(tmp)
                server=Server(binary,directory)
                try: c=Client(server.address);c.create();c.close()
                finally: server.kill()
                server=Server(binary,directory,point,action)
                try:
                    c=Client(server.address)
                    # Send without waiting: the child hangs/exits before responding.
                    c.send(b'SUB _INBOX.fault 99\r\nPUB test.x _INBOX.fault 5\r\nvalue\r\n')
                    server.wait_fault()
                    if action=='hang':
                        # Storage is wedged, but an unrelated Core publication must progress.
                        healthy=Client(server.address)
                        healthy.send(b'SUB live 1\r\n');healthy.flush()
                        healthy.send(b'PUB live 2\r\nok\r\n')
                        assert healthy.file.readline()==b'MSG live 1 2\r\n'
                        assert healthy.file.read(4)==b'ok\r\n'
                        healthy.close()
                    # No response may escape from a hook before publish acknowledgement.
                    ready=select.select([c.socket],[],[],.05)[0]
                    if ready: assert c.socket.recv(1)==b'', 'premature acknowledgement'
                    if action=='exit': assert server.process.wait(timeout=5)==86
                    c.close()
                finally: server.kill()
                server=Server(binary,directory)
                try:
                    c=Client(server.address);assert c.get(1)==(b'value' if present else None)
                    assert c.publish(b'next')==(2 if present else 1);c.close()
                finally: server.kill()
                results.append({'test':point,'action':action,'record_recovered':present,'passed':True})
    # A committed purge must never resurrect messages on SIGKILL/replay.
    with tempfile.TemporaryDirectory(prefix='screech-purge-') as tmp:
        directory=Path(tmp);server=Server(binary,directory)
        try:
            c=Client(server.address);c.create();c.publish(b'old')
            result=c.request('$JS.API.STREAM.PURGE.T',{});assert result['success'];c.close()
        finally: server.kill()
        server=Server(binary,directory)
        try:
            c=Client(server.address);assert c.get(1) is None;assert c.publish(b'new')==2;c.close()
        finally: server.kill()
        results.append({'test':'purge survives SIGKILL without sequence reuse','passed':True})
    # A blocked disk worker must not keep SIGTERM shutdown alive forever.
    with tempfile.TemporaryDirectory(prefix='screech-shutdown-') as tmp:
        directory=Path(tmp);server=Server(binary,directory)
        try:
            c=Client(server.address);c.create();c.publish(b'seed');c.close()
        finally:server.kill()
        server=Server(binary,directory,'before_commit','hang')
        try:
            c=Client(server.address)
            c.send(b'PUB test.x 3\r\nnew\r\n');server.wait_fault()
            server.process.terminate()
            assert server.process.wait(timeout=8)==0
            c.close()
        finally:server.kill()
        server=Server(binary,directory)
        try:
            c=Client(server.address);assert c.get(1)==b'seed';assert c.get(2) is None;c.close()
        finally:server.kill()
        results.append({'test':'SIGTERM is bounded during hung commit; prior ACK survives','passed':True})
    return results

if __name__=='__main__':
    p=argparse.ArgumentParser();p.add_argument('binary');p.add_argument('--output',type=Path);args=p.parse_args()
    result=run(str(Path(args.binary).resolve()));text=json.dumps(result,indent=2);print(text)
    if args.output: args.output.write_text(text+'\n')
