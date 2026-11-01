"""An early reply must fail even when a preceding read buffered it already."""
import socket
import sys
from pathlib import Path
from types import SimpleNamespace
import unittest
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'scripts'))
from batch_crash_tests import no_response

class NoResponseTests(unittest.TestCase):
    def setUp(self):
        self.reader,self.writer=socket.socketpair()
        self.reader.settimeout(1)
        self.file=self.reader.makefile('rb')
        self.client=SimpleNamespace(socket=self.reader,file=self.file)
    def tearDown(self):
        self.file.close();self.reader.close();self.writer.close()
    def test_buffered_reply_after_pong_is_rejected(self):
        self.writer.sendall(b'PONG\r\nMSG early\r\n')
        self.assertEqual(self.file.readline(),b'PONG\r\n')
        # Force read-ahead before testing the oracle; socket readiness is insufficient.
        self.assertTrue(self.file.peek(1))
        with self.assertRaisesRegex(AssertionError,'response escaped'):no_response(self.client)
        self.assertEqual(self.reader.gettimeout(),1)
    def test_socket_reply_is_rejected(self):
        self.writer.sendall(b'MSG early\r\n')
        with self.assertRaisesRegex(AssertionError,'response escaped'):no_response(self.client)
    def test_silent_connection_remains_readable_and_eof_is_allowed(self):
        no_response(self.client)
        self.assertEqual(self.reader.gettimeout(),1)
        self.writer.sendall(b'PONG\r\n')
        self.assertEqual(self.file.readline(),b'PONG\r\n')
        self.writer.close()
        no_response(self.client)
