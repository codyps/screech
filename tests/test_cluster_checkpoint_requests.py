from pathlib import Path
import sys
import unittest
from unittest.mock import Mock, patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'scripts'))
from cluster_checkpoint_tests import audit


class CheckpointRequests(unittest.TestCase):
    def client(self):
        client = Mock()
        client.get.side_effect = lambda sequence: {2: b'body-2', 3: b'body-3'}.get(sequence)
        client.publish_with_id.return_value = {'duplicate': True, 'seq': 3}
        client.request.return_value = {'ack_floor': {'consumer_seq': 1}, 'num_ack_pending': 1}
        return client

    def test_retired_connection_restarts_the_complete_audit(self):
        cluster = Mock()
        retired, current = self.client(), self.client()
        retired.get.side_effect = [b'body-2', EOFError('configuration epoch changed')]
        cluster.client.side_effect = [retired, current]
        with patch('cluster_checkpoint_tests.time.sleep'):
            audit(cluster, 3, 2)
        self.assertEqual([call.args[0] for call in current.get.call_args_list], [2, 3, 1])
        current.publish_with_id.assert_called_once_with(b'ignored', 'id-3')
        current.request.assert_called_once_with('$JS.API.CONSUMER.INFO.T.D', {})
        retired.close.assert_called_once()
        current.close.assert_called_once()

    def test_persistent_disconnect_exhausts_the_recovery_budget(self):
        cluster = Mock()
        client = cluster.client.return_value
        client.get.side_effect = EOFError('still unavailable')
        with patch('cluster_checkpoint_tests.time.monotonic', side_effect=[0, 1, 46]), \
                patch('cluster_checkpoint_tests.time.sleep') as sleep:
            with self.assertRaises(EOFError):
                audit(cluster, 3, 2)
        self.assertEqual(client.close.call_count, 2)
        sleep.assert_called_once()

    def test_payload_dedup_retention_and_ack_failures_are_never_retried(self):
        for failure in ['payload', 'retention', 'dedup', 'ack-floor', 'pending']:
            with self.subTest(failure=failure):
                cluster = Mock()
                client = self.client()
                cluster.client.return_value = client
                if failure == 'payload':
                    client.get.side_effect = lambda _: b'wrong confirmed payload'
                elif failure == 'retention':
                    client.get.side_effect = lambda sequence: f'body-{sequence}'.encode()
                elif failure == 'dedup':
                    client.publish_with_id.return_value = {'duplicate': False, 'seq': 4}
                elif failure == 'ack-floor':
                    client.request.return_value['ack_floor']['consumer_seq'] = 0
                else:
                    client.request.return_value['num_ack_pending'] = 2
                with patch('cluster_checkpoint_tests.time.sleep') as sleep:
                    with self.assertRaises(AssertionError):
                        audit(cluster, 3, 2)
                self.assertEqual(cluster.client.call_count, 1)
                client.close.assert_called_once()
                sleep.assert_not_called()


if __name__ == '__main__':
    unittest.main()
