"""Exercise the real NBD wire, durable bytes, error replies and cancelled I/O."""
import errno
from pathlib import Path
import socket
import struct
import tempfile
import unittest

from block_faults import Disk, READ, WRITE, FLUSH


def exact(connection, length):
    result = bytearray()
    while len(result) < length:
        chunk = connection.recv(length - len(result))
        if not chunk:
            raise EOFError()
        result.extend(chunk)
    return bytes(result)


class NBD:
    def __init__(self, disk):
        self.socket = socket.create_connection(('127.0.0.1', disk.port), timeout=2)
        self.cookie = 0
        self.magic = 0x49484156454f5054
        assert struct.unpack('!QQH', exact(self.socket, 18)) == (0x4e42444d41474943, self.magic, 3)
        self.socket.sendall(struct.pack('!I', 3))
        # QEMU probes structured replies/GO before falling back to EXPORT_NAME.
        self.socket.sendall(struct.pack('!QII', self.magic, 8, 0))
        assert struct.unpack('!QIII', exact(self.socket, 20)) == (0x3e889045565a9, 8, 0x80000001, 0)
        self.socket.sendall(struct.pack('!QII', self.magic, 1, 0))
        assert struct.unpack('!QH', exact(self.socket, 10)) == (disk.size, 13)

    def send(self, operation, offset=0, data=b'', length=0, flags=0):
        self.cookie += 1
        self.socket.sendall(struct.pack('!IHHQQI', 0x25609513, flags, operation, self.cookie, offset,
                                       len(data) if operation == WRITE else length) + data)

    def reply(self, length=0):
        magic, error, cookie = struct.unpack('!IIQ', exact(self.socket, 16))
        assert magic == 0x67446698 and cookie == self.cookie
        return error, exact(self.socket, length) if not error else b''


class BlockFaults(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.path = Path(self.temp.name) / 'disk.raw'
        self.path.write_bytes(b'o' * 65536)
        self.disk = Disk(self.path)
        self.client = NBD(self.disk)

    def tearDown(self):
        self.client.socket.close()
        self.disk.close()
        self.temp.cleanup()

    def test_read_your_volatile_writes_and_flush_durability(self):
        self.client.send(WRITE, data=b'a' * 4096)
        self.assertEqual((0, b''), self.client.reply())
        self.assertEqual(b'o' * 4096, self.path.read_bytes()[:4096])
        self.client.send(READ, length=4096)
        self.assertEqual((0, b'a' * 4096), self.client.reply(4096))
        self.client.send(FLUSH)
        self.assertEqual((0, b''), self.client.reply())
        self.assertEqual(b'a' * 4096, self.path.read_bytes()[:4096])
        self.client.send(WRITE, offset=4096, data=b'b' * 4096, flags=1)
        self.assertEqual((0, b''), self.client.reply())
        self.assertEqual(b'b' * 4096, self.path.read_bytes()[4096:8192])

    def test_power_loss_discards_the_volatile_cache_without_acknowledging_held_io(self):
        self.client.send(WRITE, data=b'a' * 4096)
        self.client.reply()
        self.disk.arm('hold', 'flush', lambda: [{'id': 'ack-1'}])
        self.client.send(FLUSH)
        self.assertTrue(self.disk.hit.wait(2))
        self.assertFalse(self.disk.proof['reply_sent'])
        self.disk.close()
        self.assertEqual(b'o' * 65536, self.path.read_bytes())
        with self.assertRaises(EOFError):
            self.client.reply()
        # TearDown closes its own device; supply a fresh instance after the cut.
        self.disk = Disk(self.path)

    def test_torn_fua_write_persists_exactly_one_sector_without_a_success_reply(self):
        self.disk.arm('tear', 'write', lambda: [{'id': 'publish-1'}],
                      [{'physical': 4096, 'length': 4096, 'path': 'db-wal'}])
        self.client.send(WRITE, offset=4096, data=b'n' * 4096, flags=1)
        self.assertTrue(self.disk.hit.wait(2))
        self.assertEqual(b'n' * 512 + b'o' * 3584, self.path.read_bytes()[4096:8192])
        self.assertNotEqual(self.disk.proof['after_sha256'], self.disk.proof['requested_sha256'])
        self.assertEqual(512, self.disk.proof['persisted_bytes'])
        self.client.socket.settimeout(.05)
        with self.assertRaises(socket.timeout):
            self.client.reply()

    def test_read_write_and_flush_eio_are_real_error_replies_and_one_shot(self):
        for operation, name in [(READ, 'read'), (WRITE, 'write'), (FLUSH, 'flush')]:
            self.disk.arm('eio', name, lambda: [{'id': 'request-1'}], [{'physical': 0, 'length': 65536}])
            fields = {'data': b'n' * 4096} if operation == WRITE else {'length': 4096} if operation == READ else {}
            self.client.send(operation, **fields)
            self.assertEqual((errno.EIO, b''), self.client.reply(4096 if operation == READ else 0))
            self.assertTrue(self.disk.replied.wait(2))
            self.assertTrue(self.disk.proof['reply_sent'])
            self.client.send(operation, **fields)
            self.assertEqual(0, self.client.reply(4096 if operation == READ else 0)[0])

    def test_no_fault_without_pending_client_or_authoritative_file_overlap(self):
        pending = []
        self.disk.arm('tear', 'write', lambda: pending, [{'physical': 4096, 'length': 4096}])
        for offset in [4096, 8192]:
            self.client.send(WRITE, offset=offset, data=b'n' * 4096)
            self.client.reply()
            self.assertFalse(self.disk.hit.is_set())
            pending.append({'id': 'ack-1'})
        self.client.send(WRITE, offset=4096, data=b'x' * 4096)
        self.assertTrue(self.disk.hit.wait(2))


if __name__ == '__main__':
    unittest.main()
