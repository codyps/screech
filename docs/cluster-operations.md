# Running a fixed-membership cluster

Screech supports experimental Raft clusters of three or five voters. Three voters
can continue with one failed process; five can continue with two, provided a healthy
majority can communicate and persist data. Every stream and consumer belongs to
one replicated group. The default standalone and `--single-voter` modes keep their
existing formats and behavior.

## Start three local nodes

Build the binary and generate configurations sharing a cluster identity, peer list
and a randomly generated authentication secret:

```sh
cargo build --release --locked
python3 - <<'PY'
from pathlib import Path
import json, secrets
root = Path('cluster-data')
if any((root / str(n) / 'cluster.json').exists() for n in range(1, 4)):
    raise SystemExit('Configuration already exists; reuse it for restart')
secret = secrets.token_hex(32)
peers = {str(n): {
    'raft': f'127.0.0.1:{7400+n}',
    'client': f'127.0.0.1:{4200+n}',
    'service': f'127.0.0.1:{7500+n}',
} for n in range(1, 4)}
for n in range(1, 4):
    node = root / str(n)
    node.mkdir(parents=True, exist_ok=True)
    config = node / 'cluster.json'
    config.write_text(json.dumps({
        'cluster_id': 'example-cluster', 'node_id': n,
        'secret': secret, 'peers': peers,
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
lost vote history using bootstrap: safe replacement requires a membership-change
protocol, which this fixed-membership release does not implement.

Node identity, cluster identity, secret and membership/endpoints are persisted and
must match at restart. `max_log_bytes` is an operational budget and can be raised
on restart without changing identity. Standalone databases cannot be opened as
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

Application checkpoints are checksummed caches tied to their event position. They
do not delete Raft entries or payloads. A returning voter catches up from retained
history, including payloads. Raft log compaction, snapshot transport, dynamic
membership and automatic replacement of damaged voters remain unimplemented.
To bound retained history, new proposals and logged read barriers stop at the
configured log-byte budget. Raise that budget on restart before it fills; this
version does not reclaim the authoritative log. A detected storage error quarantines
that replica and requires investigation rather than silently truncating history.

The consensus event pump is separate from the application worker. Content reads
and checkpoints use the storage executor, but SQLite access is serialized and can
still delay that node's Raft I/O. Surviving nodes can elect another leader. Throughput,
tail latency, host power-loss behavior and the stronger corruption-repair fault
model have not been certified. This remains experimental software.

## Reproduce correctness checks

```sh
cargo test --locked --all-features
cargo build --locked --features fault-injection
python3 scripts/cluster_tests.py target/debug/screech-server
```

The process suite checks public-node routing, Core binary traffic, confirmed ACKs,
retention, deduplication, forced checkpoints, leader loss, minority refusal,
rejoining replicas, full process restart and five voters with two failures. The
Rust suite also exercises the real Raft/storage integration with partitions,
reversed/duplicated messages, conflicting payloads and restart. CI runs both.
