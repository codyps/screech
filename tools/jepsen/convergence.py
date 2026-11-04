"""Read-only final membership and durable-position checks inside the guest."""
import json
from pathlib import Path
import sqlite3
import sys
import time

# Reuse the repository's small independent protocol driver for inspection only.
sys.path.insert(0, str(Path(__file__).resolve().parents[2] / 'scripts'))
from wire import Client


def verify(root, processes, nodes, timeout=60):
    deadline = time.monotonic() + timeout
    last = None
    while time.monotonic() < deadline:
        try:
            replicas = {}
            for node in nodes:
                process = processes[node]['server']
                assert process is not None and process.poll() is None, f'{node} is not running'
                manifest = json.loads((root / node / 'store/db.node-state').read_text())
                record = json.loads(bytes(manifest['body']))
                instance = record['instance']
                path = Path(instance['path'])
                assert path.is_relative_to(root / node / 'store')
                with sqlite3.connect(f'file:{path}?mode=ro', uri=True, timeout=1) as db:
                    progress = db.execute("SELECT value FROM cluster_meta WHERE key='event_sequence'").fetchone()
                    end = int.from_bytes(progress[0], 'little') if progress else db.execute('SELECT coalesce(max(seq),0) FROM event_index').fetchone()[0]
                    floor = db.execute("SELECT value FROM cluster_meta WHERE key='app_sequence'").fetchone()
                    view = db.execute("SELECT value FROM cluster_meta WHERE key='view'").fetchone()
                    sequence = max(end, int.from_bytes(floor[0], 'little') if floor else 0)
                    assert view is not None
                    replicas[node] = {'member': instance['member'], 'app_sequence': sequence,
                                      'view': int.from_bytes(view[0], 'little')}
            client = Client(('10.0.1.1', 4222), timeout=2)
            try:
                status = client.request('$JS.API.SCREECH.CLUSTER.STATUS', {})
            finally:
                client.close()
            members = {replica['member'] for replica in replicas.values()}
            assert len(members) == len(nodes), replicas
            assert members == set(status['voters']), (replicas, status)
            assert not status['outgoing'] and not status['learners'] and status['replacement'] is None, status
            positions = {(r['app_sequence'], r['view']) for r in replicas.values()}
            assert len(positions) == 1 and next(iter(positions))[0] > 0, replicas
            assert next(iter(positions))[0] == status['event_sequence'], (replicas, status)
            result = {'converged': True, 'replicas': replicas, 'cluster': status}
            for node in nodes:
                (root / node / 'convergence.json').write_text(json.dumps(result, indent=2))
            return result
        except (OSError, EOFError, AssertionError, ValueError, KeyError, TypeError, sqlite3.Error) as error:
            last = repr(error)
            time.sleep(.2)
    raise TimeoutError(f'Voters did not converge after healing: {last}')
