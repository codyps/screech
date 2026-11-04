"""A gateway changing leaders may close before its first protocol greeting."""
from pathlib import Path
import socket
import sys
import unittest
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[3] / 'scripts'))
from wire import Client


class Greeting(unittest.TestCase):
    def test_eof_is_retryable_bad_protocol_is_not_and_both_close_the_socket(self):
        for greeting, exception in [(b'', EOFError), (b'BAD\r\n', AssertionError)]:
            with self.subTest(greeting=greeting):
                client, server = socket.socketpair()
                server.sendall(greeting)
                server.close()
                try:
                    with patch('wire.socket.create_connection', return_value=client):
                        with self.assertRaises(exception):
                            Client(('unused', 0))
                    self.assertEqual(-1, client.fileno())
                finally:
                    client.close()
