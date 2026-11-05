from pathlib import Path
import sys
import unittest
from unittest.mock import Mock, patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'scripts'))
from cluster_recovery_tests import request


class RecoveryRequests(unittest.TestCase):
    def test_service_error_is_retried_instead_of_returned_as_status(self):
        cluster = Mock()
        clients = [Mock(), Mock()]
        error = {'error': {'code': 400, 'description': 'application epoch changed'}}
        expected = {'voters': [1, 2, 3], 'replacement': None}
        clients[0].request.return_value = error
        clients[1].request.return_value = expected
        cluster.client.side_effect = clients
        with patch('cluster_recovery_tests.time.sleep') as sleep:
            actual = request(cluster, '$JS.API.SCREECH.CLUSTER.STATUS', attempts=2)
        self.assertEqual(actual, expected)
        sleep.assert_called_once()
        for client in clients:
            client.close.assert_called_once()

    def test_persistent_service_errors_still_fail_with_the_last_response(self):
        cluster = Mock()
        client = cluster.client.return_value
        response = {'error': {'code': 400, 'description': 'replica quarantined'}}
        client.request.return_value = response
        with patch('cluster_recovery_tests.time.sleep'):
            with self.assertRaises(AssertionError) as failure:
                request(cluster, '$JS.API.SCREECH.CLUSTER.STATUS', attempts=2)
        self.assertIn('replica quarantined', str(failure.exception))
        self.assertEqual(client.request.call_count, 2)
        self.assertEqual(client.close.call_count, 2)


if __name__ == '__main__':
    unittest.main()
