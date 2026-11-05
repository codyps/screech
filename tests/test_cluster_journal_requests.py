from pathlib import Path
import sys
import unittest
from unittest.mock import Mock, patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'scripts'))
from cluster_journal_tests import audit_voter


class JournalAudits(unittest.TestCase):
    def client(self):
        client = Mock()
        client.get.return_value = b'confirmed'
        client.request.return_value = {'ack_floor': {'consumer_seq': 1}, 'num_ack_pending': 0}
        client.publish_with_id.return_value = {'duplicate': True, 'seq': 1}
        return client

    def test_connection_change_retries_the_complete_audit_and_closes_clients(self):
        cluster = Mock()
        first, second = self.client(), self.client()
        first.request.side_effect = EOFError('leader changed after payload read')
        cluster.client.side_effect = [first, second]
        with patch('cluster_journal_tests.time.sleep'):
            audit_voter(cluster, 2, {1: b'confirmed'}, attempts=2)
        for client in [first, second]:
            client.get.assert_called_once_with(1)
            client.close.assert_called_once()
        second.publish_with_id.assert_called_once_with(b'ignored', 'stable')

    def test_wrong_recovered_data_fails_without_retrying(self):
        cluster = Mock()
        client = self.client()
        client.get.return_value = b'wrong'
        cluster.client.return_value = client
        with patch('cluster_journal_tests.time.sleep') as sleep:
            with self.assertRaises(AssertionError):
                audit_voter(cluster, 2, {1: b'confirmed'}, attempts=2)
        cluster.client.assert_called_once_with(2)
        client.close.assert_called_once()
        sleep.assert_not_called()

    def test_persistent_connection_failure_is_bounded(self):
        cluster = Mock()
        cluster.client.side_effect = EOFError('no active gateway')
        with patch('cluster_journal_tests.time.sleep'):
            with self.assertRaises(AssertionError) as failure:
                audit_voter(cluster, 2, {1: b'confirmed'}, attempts=2)
        self.assertIsInstance(failure.exception.__cause__, EOFError)
        self.assertEqual(cluster.client.call_count, 2)


if __name__ == '__main__':
    unittest.main()
