# Running and repairing a cluster

Screech supports experimental Raft clusters with three or five initial voters. Three voters
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
        'secret': secret, 'peers': peers, 'initial_voters': [1, 2, 3],
        'max_log_bytes': 536870912,
    }, indent=2))
    config.chmod(0o600)
PY
```

In three terminals, start the servers:

```sh
./target/release/screech-server --store cluster-data/1/cluster.db --cluster-config cluster-data/1/cluster.json
./target/release/screech-server --store cluster-data/2/cluster.db --cluster-config cluster-data/2/cluster.json
./target/release/screech-server --store cluster-data/3/cluster.db --cluster-config cluster-data/3/cluster.json
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

Use exactly the same command for first startup, restart and repair:

```sh
./target/release/screech-server --store cluster-data/1/cluster.db --cluster-config cluster-data/1/cluster.json
```

There are no bootstrap or join flags and no separate repair operation. On first
formation, all configured initial voters must be reachable once. Each supplies an
authenticated claim tied to its fresh local startup identity. The complete formation
certificate is persisted before voting. A partially completed formation resumes
from that certificate; missing historical stores cannot issue fresh genesis votes.
Later intact cluster restarts need a current voting majority.

The configured `node_id` identifies a stable server slot and its three addresses.
Raft member IDs are internal and can change during recovery. When a live quorum is
reachable, a returning server starts a fresh nonvoting incarnation, including when
its local database appears intact. This avoids trusting a consistent stale database
as current vote history. The quorum enrolls it, transfers verified state, then
promotes it through joint consensus while retiring the previous voter in that slot.
A damaged store waits without voting if no healthy quorum is available. The same
supervisor retries after runtime storage failures and interrupted recovery.

The `.node-state` file durably selects the current store generation. Replacement
stores live beneath `<store stem>.recovery/<member>/store.db`. Original and failed
generations are preserved for diagnosis and consume disk until archived or removed
while the server is stopped. Keep the active generation, WAL/SHM files, startup
manifest, recovery watermarks and lock markers together. Startup verifies persisted identities, log
continuity, snapshots and required bodies before resuming a stored voter. A complete
restart relies on the surviving stores retaining their durable voting history;
restoring an entire voting quorum to older, internally consistent backups is not a
supported recovery procedure. Rolling back a database together with its watermark
requires a reachable healthy quorum: a consistent rollback of every local file
cannot be recognized from local checks alone.

Keep the cluster identity, secret and initial voter list unchanged. Protect the
configuration as well as data files. `max_log_bytes` can be raised on restart.
Standalone stores are not imported into the cluster; recovery obtains authoritative
state from peers. Their standalone behavior and formats remain unchanged.

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

Before releasing persistence-dependent messages, an authenticated local watermark
seals the durable term, vote, commit and accepted log position. Startup compares the
database against this watermark, rejecting rolled-back history even without a
reachable leader. Missing or corrupt watermarks require quorum repair. A database
ahead of the watermark is safe to resume after interrupted publication. SQLite is
the only source of replicated application state; the watermark can only reject a
store. Atomic watermark publication adds file and directory syncs when the durable
position changes. Its throughput/latency cost still needs measurement and optimization.

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

## Automatic repair and observation

Restart the failed server with its original configuration and command. Missing,
blank, malformed, truncated and corrupt stores recover under fresh internal member
IDs at the same addresses. No spare endpoints, edited configuration, nonce copying
or admin request is required. A failed learner is retired before the next candidate
is enrolled; a new leader resumes interrupted committed membership changes.

Repair needs a healthy majority of the current voters. Three voters can repair one
unavailable member; five can repair two, serializing membership transitions. No
failure detector removes voters or reduces quorum requirements. Replication cannot
recover acknowledged bytes after all authoritative copies have been destroyed.

For observation, request `$JS.API.SCREECH.CLUSTER.STATUS` with an empty JSON object.
`voters`, `learners`, `outgoing`, `replacement` and `retired` expose the committed
internal membership and recovery progress. The selected member ID and store path
are logged at startup. Completion means the selected member is in `voters`,
`outgoing` is empty and `replacement` is null. Nonvoter slots listed only in the
address book automatically enroll as standbys and do not increase the voter count.
The earlier `REPLACE` admin API remains available for explicit membership requests;
automatic startup does not require it.

Retired or wrong-incarnation messages are fenced before Raft processes votes.
The removed leader steps down after committed removal. Application writer sessions
fence retiring services across configuration changes before another model recovers.
Client connections can close during this transition; use ordinary reconnect and
publication deduplication behavior.

## Upgrade from the earlier retained-log cluster build

Stop the cluster, preserve its databases, WAL files and startup metadata, and update all binaries
before restarting against those stores. The previous persisted batch format is
readable, and configuration is reconstructed before serving. Startup now multiplexes authenticated formation/recovery requests on the Raft
port alongside replication; mixed-version operation
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
It checks missing/corrupt snapshot bodies, lost incarnation/startup metadata, crashes
during formation, a leader killed during joint apply, and a learner whose store
disappears during enrollment. Publications also continue through leader loss and
automatic repair; every acknowledged sequence is checked. A full
restart after replacing the original voters verifies configuration recovery from
snapshots. The ordinary standalone crash suites remain separate regression gates.
