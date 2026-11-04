"""Opaque Ethernet hub for QEMU's length-prefixed TCP socket network backend.

Wire format: https://github.com/qemu/qemu/blob/master/net/socket.c
No Raft, NATS or storage messages are interpreted or fabricated here.
"""
import socket
import socketserver
import struct
import threading
import copy


def receive_exact(connection, length):
    result = bytearray()
    while len(result) < length:
        chunk = connection.recv(length - len(result))
        if not chunk:
            raise EOFError()
        result.extend(chunk)
    return bytes(result)


class Peer:
    def __init__(self, connection):
        self.connection = connection
        self.lock = threading.Lock()
        self.node = None

    def send(self, frame):
        with self.lock:
            self.connection.sendall(frame)


class Hub:
    def __init__(self):
        self.peers = {}
        self.lock = threading.Lock()
        self.blocked = set()
        self.epoch = 0
        self.edges = {}
        hub = self

        class Handler(socketserver.BaseRequestHandler):
            def handle(self):
                peer = Peer(self.request)
                self.request.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
                with hub.lock:
                    hub.peers[id(peer)] = peer
                try:
                    while True:
                        header = receive_exact(self.request, 4)
                        size = struct.unpack('!I', header)[0]
                        if not 0 < size <= 65536:
                            raise ValueError('invalid Ethernet frame size')
                        frame = header + receive_exact(self.request, size)
                        with hub.lock:
                            mac = frame[10:16]  # Four-byte length, then destination/source MACs.
                            if len(mac) == 6 and mac[:5] == bytes.fromhex('0200000001') and 1 <= mac[5] <= 5:
                                peer.node = f'n{mac[5]}'
                            targets = [p for p in hub.peers.values() if p is not peer]
                        for target in targets:
                            with hub.lock:
                                pair = (peer.node, target.node)
                                denied = pair in hub.blocked or (bool(hub.blocked) and None in pair)
                                if None not in pair:
                                    counts = hub.edges.setdefault(pair, {'forwarded': 0, 'dropped': 0})
                                    counts['dropped' if denied else 'forwarded'] += 1
                            if denied:
                                continue
                            try:
                                target.send(frame)
                            except OSError:
                                pass  # A powered-off NIC has no receiver.
                except (OSError, EOFError, ValueError):
                    pass
                finally:
                    with hub.lock:
                        hub.peers.pop(id(peer), None)

        self.server = socketserver.ThreadingTCPServer(('127.0.0.1', 0), Handler)
        self.server.daemon_threads = True
        self.port = self.server.server_address[1]
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()

    def partition(self, blocked):
        """Drop directed source-voter -> destination-voter Ethernet edges.

        Policy is outside the guest and keyed by its fixed MAC, so VM reboots
        cannot heal it. Frames already in flight remain ordinary network traffic.
        """
        blocked = {tuple(pair) for pair in blocked}
        assert all(len(p) == 2 and p[0] != p[1] and all(n in {f'n{i}' for i in range(1, 6)} for n in p) for p in blocked)
        with self.lock:
            self.blocked = blocked
            self.epoch += 1
            self.edges = {}
            return self.epoch

    def snapshot(self):
        with self.lock:
            return {'epoch': self.epoch, 'blocked': [list(p) for p in sorted(self.blocked)],
                    'edges': [dict(source=a, destination=b, **copy.copy(c))
                              for (a, b), c in sorted(self.edges.items())]}

    def close(self):
        self.server.shutdown()
        self.server.server_close()

class TCPProxy:
    """Keep a stable client address while each reboot uses new management ports."""
    def __init__(self, target):
        import selectors
        class Handler(socketserver.BaseRequestHandler):
            def handle(self):
                address = target()
                if address is None:
                    return
                try:
                    with socket.create_connection(address, timeout=3) as peer:
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
                except OSError:
                    pass
        self.server = socketserver.ThreadingTCPServer(('127.0.0.1', 0), Handler)
        self.server.daemon_threads = True
        self.port = self.server.server_address[1]
        threading.Thread(target=self.server.serve_forever, daemon=True).start()

    def close(self):
        self.server.shutdown()
        self.server.server_close()
