"""Disposable, single-connection NBD disk with an explicit volatile write cache.

Protocol: https://github.com/NetworkBlockDevice/nbd/blob/master/doc/proto.md
Only READ, WRITE, DISC and FLUSH are advertised. Successful FLUSH/FUA replies
follow host fsync; power loss discards the cache and cancels held requests.
This is a test device, never a production NBD service.
"""
import errno
import hashlib
import os
import socket
import socketserver
import struct
import threading
import time

from ethernet import receive_exact

READ, WRITE, DISC, FLUSH = range(4)
NAMES = {READ: 'read', WRITE: 'write', FLUSH: 'flush'}


def digest(data):
    return hashlib.sha256(data).hexdigest()


class Disk:
    def __init__(self, path):
        self.path = path
        self.fd = os.open(path, os.O_RDWR)
        self.size = os.fstat(self.fd).st_size
        self.cache = []
        self.lock = threading.RLock()
        self.dead = threading.Event()
        self.hit = threading.Event()
        self.replied = threading.Event()
        self.rule = None
        self.proof = None
        self.connections = set()
        disk = self

        class Handler(socketserver.BaseRequestHandler):
            def handle(self):
                self.request.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
                with disk.lock:
                    disk.connections.add(self.request)
                try:
                    disk.serve(self.request)
                except (EOFError, OSError):
                    pass  # A VM power cut closes the connection without DISC.
                finally:
                    with disk.lock:
                        disk.connections.discard(self.request)

        self.server = socketserver.ThreadingTCPServer(('127.0.0.1', 0), Handler)
        self.server.daemon_threads = True
        self.port = self.server.server_address[1]
        threading.Thread(target=self.server.serve_forever, daemon=True).start()

    def arm(self, mode, operation, pending, extents=()):
        assert mode in ('hold', 'tear', 'eio') and operation in ('read', 'write', 'flush')
        assert mode != 'tear' or operation == 'write'
        with self.lock:
            assert self.rule is None and not self.dead.is_set()
            self.rule = mode, operation, pending, extents
            self.hit.clear()
            self.replied.clear()

    def persist(self):
        for offset, data in self.cache:
            self.write_all(offset, data)
        os.fsync(self.fd)
        self.cache.clear()

    def write_all(self, offset, data):
        while data:
            written = os.pwrite(self.fd, data, offset)
            assert written > 0
            offset, data = offset + written, data[written:]

    def read(self, offset, length):
        result = bytearray(os.pread(self.fd, length, offset))
        assert len(result) == length
        for start, data in self.cache:
            lo, hi = max(offset, start), min(offset + length, start + len(data))
            if lo < hi:
                result[lo-offset:hi-offset] = data[lo-start:hi-start]
        return bytes(result)

    def execute(self, operation, flags, offset, data, length):
        """Return (errno, reply data, hold). The protocol thread alone does I/O."""
        with self.lock:
            if self.dead.is_set():
                raise EOFError()
            if offset + length > self.size or length > 32 * 1024**2:
                return errno.EINVAL, b'', False
            if self.rule and NAMES.get(operation) == self.rule[1]:
                mode, _, pending, extents = self.rule
                requests = pending()
                overlaps = operation != WRITE or any(e['physical'] <= offset and
                                                       offset + length <= e['physical'] + e['length'] for e in extents)
                old = self.read(offset, length) if mode == 'tear' else b''
                tearable = mode != 'tear' or (length >= 1024 and old[:512] != data[:512] and old[512:] != data[512:])
                if requests and overlaps and tearable:
                    self.rule = None
                    self.proof = {'mode': mode, 'operation': NAMES[operation], 'offset': offset,
                                  'length': length, 'flags': flags, 'requests': requests,
                                  'boundary_ns': time.monotonic_ns(), 'reply_sent': False}
                    if mode == 'tear':
                        # Persist older writes, then only the first sector of this
                        # multi-sector request. Never acknowledge the torn write.
                        self.persist()
                        old = os.pread(self.fd, length, offset)
                        self.write_all(offset, data[:512])
                        os.fsync(self.fd)
                        actual = os.pread(self.fd, length, offset)
                        assert actual == data[:512] + old[512:] and actual != old and actual != data
                        self.proof.update(persisted_bytes=512, before_sha256=digest(old),
                                          requested_sha256=digest(data), after_sha256=digest(actual),
                                          expected_sha256=digest(data[:512] + old[512:]),
                                          extents=extents)
                    self.hit.set()
                    if mode == 'eio':
                        self.proof['errno'] = errno.EIO
                        return errno.EIO, b'', False
                    return 0, b'', True
            if operation == READ:
                return 0, self.read(offset, length), False
            if operation == WRITE:
                self.cache.append((offset, data))
                if flags & 1:  # NBD_CMD_FLAG_FUA
                    self.persist()
                return 0, b'', False
            if operation == FLUSH:
                self.persist()
                return 0, b'', False
            return errno.EINVAL, b'', False

    def serve(self, connection):
        connection.sendall(struct.pack('!QQH', 0x4e42444d41474943, 0x49484156454f5054, 3))
        flags, = struct.unpack('!I', receive_exact(connection, 4))
        if flags & ~3:
            return
        while True:
            magic, option, length = struct.unpack('!QII', receive_exact(connection, 16))
            if magic != 0x49484156454f5054 or length > 65536:
                return
            name = receive_exact(connection, length)
            if option == 1:  # EXPORT_NAME, fallback after unsupported GO/structured replies
                if name:
                    return
                connection.sendall(struct.pack('!QH', self.size, 1 | 4 | 8) + (b'' if flags & 2 else bytes(124)))
                break
            connection.sendall(struct.pack('!QIII', 0x3e889045565a9, option,
                                           1 if option == 2 else 0x80000001, 0))
            if option == 2:
                return
        while not self.dead.is_set():
            magic, flags, operation, cookie, offset, length = struct.unpack('!IHHQQI', receive_exact(connection, 28))
            if magic != 0x25609513 or length > 32 * 1024**2:
                return
            if operation == DISC:
                return
            data = receive_exact(connection, length) if operation == WRITE else b''
            error, result, hold = self.execute(operation, flags, offset, data, length)
            if hold:
                self.dead.wait()  # Only a real VM cut cancels the in-flight request.
                return
            with self.lock:
                if self.dead.is_set():
                    return
                connection.sendall(struct.pack('!IIQ', 0x67446698, error, cookie) + result)
                if error == errno.EIO and self.proof and self.proof.get('errno') == errno.EIO:
                    self.proof['reply_sent'] = True
                    self.replied.set()

    def close(self):
        with self.lock:
            self.dead.set()
            self.cache.clear()  # No flush during power loss or teardown.
            for connection in list(self.connections):
                try:
                    connection.shutdown(socket.SHUT_RDWR)
                except OSError:
                    pass
            os.close(self.fd)
        self.server.shutdown()
        self.server.server_close()
