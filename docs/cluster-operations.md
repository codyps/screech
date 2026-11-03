# Running and repairing a cluster

Screech supports experimental Raft clusters with three or five initial voters. Three voters
can continue with one failed process; five can continue with two, provided a healthy
majority can communicate and persist data. Every stream and consumer belongs to
one replicated group. The default standalone and `--single-voter` modes keep their
existing formats and behavior.

## Start three local nodes

Build the binary and generate configurations sharing a cluster identity, peer list
and a randomly generated authentication secret. This address book includes three
spare identities/endpoints for later replacement:

```sh
cargo build --release --locked
python3 - <<'PY'
from pathlib import Path
import json, secrets
root = Path('cluster-data')
if any((root / str(n) / 'cluster.json').exists() for n in range(1, 7)):
    raise SystemExit('Configuration already exists; reuse it for restart')
secret = secrets.token_hex(32)
peers = {str(n): {
    'raft': f'127.0.0.1:{7400+n}',
    'client': f'127.0.0.1:{4200+n}',
    'service': f'127.0.0.1:{7500+n}',
} for n in range(1, 7)}
for n in range(1, 7):
    node = root / str(n)
    node.mkdir(parents=True, exist_ok=True)
    config = node / 'cluster.json'
    config.write_text(json.dumps({
        'cluster_id': 'example-cluster', 'node_id': n,
        'secret': secret, 'peers': peers, 'initial_voters': [1, 2, 3],
        'max_log_bytes': 536870912,
    }, indent=2))
    config.chmod(0o600)
PY
```

In three terminals, initialize each voter once:

```sh
./target/release/screech-server --store cluster-data/1/cluster.db --cluster-config cluster-data/1/cluster.json --cluster-bootstrap
./target/release/screech-server --store cluster-data/2/cluster.db --cluster-config cluster-data/2/cluster.json --cluster-bootstrap
./target/release/screech-server --store cluster-data/3/cluster.db --cluster-config cluster-data/3/cluster.json --cluster-bootstrap
```

Connect NATS clients to ports 4201–4203. Each public endpoint routes connections
to the elected leader's service. INFO advertises all public endpoints so compatible
clients discover reconnect targets. Connections close on a leadership change;
clients reconnect and retry outstanding operations under their normal retry rules.
A timed-out publication has an unknown outcome; use `Nats-Msg-Id` for deduplication
within the configured window. Outstanding pulls must be retried. Committed delivery
reservations, redelivery attempts and confirmed ACKs survive leader changes.

The three addresses per node must be distinct and reachable at their declared
addresses. The cluster configuration supplies listen addresses; `--addr` and
`--port` apply to standalone mode. Other server limits and `--token` still apply.
On separate hosts, replace the addresses consistently in every configuration.
Peer frames authenticate the cluster ID, sender and complete payload using
HMAC-SHA256. Traffic is not encrypted; use a trusted private network or encrypted
transport outside Screech. Client/service endpoints retain the existing NATS token
authentication behavior. Protect both the peer and service endpoints accordingly.

## Restart and storage identity

Restart a stopped process with the same configuration and store, omitting
`--cluster-bootstrap`. Keep the database, WAL/SHM files and `.cluster-lock` marker
together. Bootstrap initializes fresh storage for the original voters only.
Normal startup refuses missing or blank databases. Do not recreate a voter with
lost vote history using bootstrap. Use the learner replacement procedure below.
A store with untrustworthy bytes is never used to reconstruct replacement state;
the surviving healthy quorum supplies that state.

Node identity, cluster identity, secret and initial voter IDs are persisted and
must match at restart. Active membership is recovered from committed Raft changes.
The address book can add spare endpoints; update it consistently on every node and
restart to load changed routes before enrolling those identities. Keep initial voter
IDs unchanged. `max_log_bytes` is an operational budget and can be raised on restart
without changing identity. Standalone databases cannot be opened as
cluster databases; offline import is future work. Graceful shutdown and SIGKILL
restarts preserve acknowledged state under the configured local storage contract.

All streams use the full voter group. API responses report three or five replicas.
Client default replica requests of zero/one use the full group; explicitly requesting
the full group is supported. Other replica counts are rejected. There is no
per-stream replica placement or sharding.

## Persistence and current limits

SQLite WAL FULL atomically persists complete Raft entries and required term/vote
state before persistence-dependent replication messages. Entries contain application
transitions and their payloads. Record/content indexes refer into those entries and
expose only the durably committed prefix. Recovery rebuilds indexes from that prefix,
then reconstructs the model from a validated checkpoint and its committed tail.
Uncommitted accepted entries remain available to Raft for conflict replacement.

Checkpoints atomically publish the complete snapshot and reclaim its covered log
prefix. Snapshot metadata binds the model, event position, term/index, configuration,
member incarnations and retired identities. SHA-256 manifests bind every required
body. Local payload rows stay separate from the model and are verified at startup.
Complete accepted entries after the snapshot remain available to Raft until they
are committed or replaced.

A returning member whose log falls behind the retained prefix receives a complete
snapshot, then its tail. Transfer uses authenticated chunks, an overall digest,
explicit byte budgets and a 30-second transfer deadline. No incomplete transfer
changes the recovery root. Installation, hard state and covered-prefix deletion
share a FULL transaction. A donor may purge data or publish a newer image while an
older transfer is in flight; the already-constructed transfer retains its bytes.
Snapshot retries restart the transfer rather than trusting an incomplete image.

The leader checkpoints automatically after 256 Raft positions, when the log uses
more than half its byte budget, or after a configuration change. An operator can
request `$JS.API.SCREECH.CLUSTER.COMPACT` to checkpoint immediately. Free SQLite
pages are reused; the database file can retain its physical high-water size.
`max_log_bytes` must be at least 80 MiB to reserve batch/checkpoint headroom.
Transfers currently materialize complete images and are capped at 512 MiB, including
metadata and live bodies. Keep total retained state below that capacity; larger
images and incremental file-based transfer remain future work. Log admission still
bounds the uncompacted suffix when reclamation cannot make progress.

The consensus event pump is separate from the application worker. Content reads
and checkpoints use the storage executor, but SQLite access is serialized and can
still delay that node's Raft I/O. Surviving nodes can elect another leader. Throughput,
tail latency, host power-loss behavior and the stronger corruption-repair fault
model have not been certified. This remains experimental software.

## Replace a damaged voter

This requires a healthy majority of the current voters. Three-voter clusters can
repair one unavailable member; five-voter clusters can repair two unavailable
members by replacing them one at a time. Replication cannot recover acknowledged
bytes after every authoritative copy has been destroyed.

1. Stop the damaged process. Preserve its files for diagnosis. If the store is
   missing, corrupt or rolled back, leave that identity offline during replacement.
   A consistent old copy can pass checksums; do not trust it as current vote history.
2. Choose an unused node ID with distinct endpoints already in every node's address
   book. Never recycle a retired ID. Start it with an empty store using join mode:

   ```sh
   ./target/release/screech-server --store cluster-data/4/cluster.db --cluster-config cluster-data/4/cluster.json --cluster-join
   ```

   Startup prints `JOIN 4 <32-hex-digit-nonce>`. The fresh incarnation is durable.
   The process starts without voting authority.
3. Submit a replacement through any healthy public endpoint. For example, replacing
   voter 2 with learner 4, copy the actual JOIN nonce into this request:

   ```sh
   PYTHONPATH=scripts python3 - <<'PY'
   from wire import Client
   c = Client(('127.0.0.1', 4201))
   print(c.request('$JS.API.SCREECH.CLUSTER.REPLACE', {
       'old': 2, 'new': 4, 'nonce': '<actual JOIN nonce>',
   }))
   c.close()
   PY
   ```

   `accepted` means the operation was queued, not that replacement is complete.
   Configuration changes close client connections; an interrupted reply has an
   unknown outcome. Repeating the same request is safe.
4. Poll `$JS.API.SCREECH.CLUSTER.STATUS` using an empty JSON object. Completion means
   `voters` contains 4, `outgoing` is empty and `retired["2"]` is 4. The leader first
   commits learner enrollment, waits for verified durable catch-up, then commits
   joint promotion/removal and leaves the joint configuration. New leaders resume
   an operation from committed metadata after a failure.

If a candidate learner loses its disk before promotion, choose another unused ID
and submit the same `old` voter with the new target/nonce. The leader commits removal
of the failed learner before enrolling the new target; abandoned learner identities
are tombstoned with a retired value of zero. If joint promotion has already begun,
allow it to finish, then replace the damaged promoted member under its now-current ID.

An active voter whose store is wiped cannot rejoin by generating a new incarnation
under the old ID. Its messages do not match the committed enrollment, and a snapshot
for that old incarnation cannot activate the new one. Use another unused ID and the
healthy quorum. Retired senders are fenced before Raft processes their votes, even
if they restart with a valid stale store. Keep retired processes stopped.

These admin subjects use the existing NATS authentication/authorization boundary.
Peer authentication binds sender incarnation as well as cluster ID and message bytes.
There is no automatic failure-detector removal or forced quorum reduction.
The removed leader steps down after committed removal, and a new application
writer session fences any work from a retiring service before recovery begins.

## Upgrade from the earlier retained-log cluster build

Stop the cluster, preserve its database/WAL/lock files, and update all binaries
before restarting against those stores. The previous persisted batch format is
readable, and configuration is reconstructed before serving. This change extends
the peer wire format with incarnation and chunk framing; mixed-version operation
is not supported. Existing standalone and strict single-voter stores keep their
original behavior and formats.

## Reproduce correctness checks

```sh
cargo test --locked --all-features
cargo build --locked --features fault-injection
python3 scripts/cluster_tests.py target/debug/screech-server
python3 scripts/cluster_recovery_tests.py target/debug/screech-server
```

The process suite checks public-node routing, Core binary traffic, confirmed ACKs,
retention, deduplication, forced checkpoints, leader loss, minority refusal,
rejoining replicas, full process restart and five voters with two failures. The
Rust suite also exercises the real Raft/storage integration with partitions,
reversed/duplicated messages, conflicting payloads and restart. CI runs both.

The recovery suite additionally kills receivers during transfer and on both sides
of snapshot activation, changes the donor image during a paused transfer, and
repairs killed members with absent, blank, unreadable, truncated and stale stores.
It checks missing/corrupt snapshot bodies, lost incarnation metadata, a leader killed
during joint apply, and a learner whose store disappears during enrollment. Live leader replacement
also runs while publications continue; every acknowledged sequence is checked. A full
restart after replacing the original voters verifies configuration recovery from
snapshots. The ordinary standalone crash suites remain separate regression gates.
