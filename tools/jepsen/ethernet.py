"""Opaque Ethernet hub for QEMU's length-prefixed TCP socket network backend.

Wire format: https://github.com/qemu/qemu/blob/master/net/socket.c
No Raft, NATS or storage messages are interpreted or fabricated here.
"""
import socket
import socketserver
import struct
import threading


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

    def send(self, frame):
        with self.lock:
            self.connection.sendall(frame)


class Hub:
    def __init__(self):
        self.peers = {}
        self.lock = threading.Lock()
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
                            targets = [p for p in hub.peers.values() if p is not peer]
                        for target in targets:
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
