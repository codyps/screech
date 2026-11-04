#!/usr/bin/env python3
"""Authenticated guest-only controls and opaque TCP gateways for host Jepsen clients."""
import argparse
import base64
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import hmac
import json
from pathlib import Path
import selectors
import shutil
import signal
import socket
import socketserver
import subprocess
import threading
import time
import traceback
from control import configure, canary, clear, damage
from convergence import verify

ROOT = Path('/data/screech-jepsen')
NODES = {}
LOCKS = {f'n{i}': threading.Lock() for i in range(1, 6)}


def launch(node, argv, logfile):
    with logfile.open('ab') as log:
        return subprocess.Popen((['ip', 'netns', 'exec', node] if node else []) + argv, stdin=subprocess.DEVNULL, stdout=log, stderr=subprocess.STDOUT)


def stop(process):
    if process is not None:
        if process.poll() is None:
            process.kill()
        process.wait(timeout=10)


def act(request):
    op, node = request['op'], request.get('node')
    assert Path('/run/screech-jepsen-vm').exists()
    if op == 'status':
        return {'isolated-vm': True}
    if op == 'verify':
        return verify(ROOT, NODES, request['nodes'])
    if op == 'ready':
        deadline = time.monotonic() + 90
        for target in request['nodes']:
            while True:
                state = NODES[target]
                if state['server'].poll() is not None:
                    raise RuntimeError(f'{target} exited: ' + (ROOT / target / 'server.log').read_text()[-4000:])
                try:
                    with socket.create_connection((f'10.0.1.{target[1:]}', 4222), timeout=2) as client:
                        client.settimeout(2)
                        if client.recv(4096).startswith(b'INFO '):
                            break
                except OSError:
                    pass
                if time.monotonic() > deadline:
                    raise TimeoutError(f'{target} did not become ready: ' + (ROOT / target / 'server.log').read_text()[-4000:])
                time.sleep(.2)
        return {'ready': True}
    assert node in LOCKS
    if op == 'exec':
        result = subprocess.run(['ip', 'netns', 'exec', node, 'bash', '-c', request['cmd']], input=request.get('in', ''), text=True, capture_output=True, timeout=90)
        return {'exit': result.returncode, 'out': result.stdout, 'err': result.stderr}
    if op in ('read', 'write'):
        path = Path(request['path'])
        assert path.is_relative_to(ROOT)
        if op == 'read':
            return {'data': base64.b64encode(path.read_bytes()).decode()}
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(base64.b64decode(request['data']))
        return {'written': True}
    with LOCKS[node]:
        directory = ROOT / node
        if op == 'setup':
            assert node not in NODES
            shutil.rmtree(directory, ignore_errors=True)
            (directory / 'store').mkdir(parents=True)
            (directory / 'real').mkdir()
            (directory / 'config.json').write_text(json.dumps(request['config']))
            configure(directory)
            filesystem = launch(None, [request['lazyfs'], str(directory / 'store'), '--config-path', str(directory / 'lazyfs.toml'), '-o', 'allow_other', '-o', 'modules=subdir', '-o', f'subdir={directory}/real', '-f', '-s'], directory / 'lazyfs-stdout.log')
            NODES[node] = {'filesystem': filesystem, 'server': None, 'binary': request['binary']}
            canary(directory)
            op = 'start'
        if op == 'teardown' and node not in NODES:
            return {'stopped': True}
        state = NODES[node]
        if op == 'start':
            if state['server'] is None or state['server'].poll() is not None:
                state['server'] = launch(node, [state['binary'], '--store', str(directory / 'store/db'), '--cluster-config', str(directory / 'config.json')], directory / 'server.log')
            return {'started': True}
        if op == 'kill':
            stop(state['server'])
            state['server'] = None
            clear(directory)
            return {'cache_clear_acknowledged': True}
        if op in ('pause', 'resume'):
            if state['server'] is not None and state['server'].poll() is None:
                state['server'].send_signal(signal.SIGSTOP if op == 'pause' else signal.SIGCONT)
            return {op: True}
        if op in ('wipe', 'bitflip', 'truncate'):
            assert state['server'] is None or state['server'].poll() is not None
            damage(directory, op)
            return {'injected': op}
        if op == 'teardown':
            stop(state['server'])
            subprocess.run(['fusermount3', '-uz', str(directory / 'store')], check=True)
            stop(state['filesystem'])
            del NODES[node]
            return {'stopped': True}
        raise ValueError(op)


class Control(BaseHTTPRequestHandler):
    def log_message(self, *_):
        pass

    def dispatch(self, request):
        return act(request)

    def do_POST(self):
        if not hmac.compare_digest(self.headers.get('Authorization', ''), 'Bearer ' + self.server.token):
            self.send_error(403)
            return
        try:
            length = int(self.headers['Content-Length'])
            assert 0 < length < 16 * 1024 * 1024
            response = self.dispatch(json.loads(self.rfile.read(length)))
        except Exception as error:
            traceback.print_exc()
            response = {'error': str(error), 'type': type(error).__name__}
        data = json.dumps(response).encode()
        self.send_response(200)
        self.send_header('Content-Length', str(len(data)))
        self.end_headers()
        self.wfile.write(data)


class Gateway(socketserver.BaseRequestHandler):
    def handle(self):
        try:
            with socket.create_connection((self.server.target, 4222), timeout=5) as peer:
                peer.settimeout(None)
                with selectors.DefaultSelector() as poll:
                    poll.register(self.request, selectors.EVENT_READ, peer)
                    poll.register(peer, selectors.EVENT_READ, self.request)
                    while True:
                        for key, _ in poll.select(timeout=30):
                            data = key.fileobj.recv(65536)
                            if not data:
                                return
                            key.data.sendall(data)
        except (OSError, ConnectionError):
            pass


if __name__ == '__main__':
    assert Path('/run/screech-jepsen-vm').exists(), 'control server requires the isolated guest'
    parser = argparse.ArgumentParser()
    parser.add_argument('--token-file', type=Path, required=True)
    args = parser.parse_args()
    for i in range(1, 6):
        server = socketserver.ThreadingTCPServer(('0.0.0.0', 5000 + i), Gateway)
        server.daemon_threads = True
        server.target = f'10.0.1.{i}'
        threading.Thread(target=server.serve_forever, daemon=True).start()
    server = ThreadingHTTPServer(('0.0.0.0', 9000), Control)
    server.token = args.token_file.read_text().strip()
    print('SCREECH_JEPSEN_GUEST_READY', flush=True)
    server.serve_forever()
