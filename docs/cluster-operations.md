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
formation, all configured initial voters must be reachable until the formation
certificate is established. Each supplies an authenticated claim tied to its fresh local startup identity. The complete formation
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
manifest, accepted journals (`.cluster-watermark`) and lock markers together. The
journal contains payloads and must be included in backups. Startup verifies identities, log
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

An authenticated accepted journal atomically seals complete Raft entries, their
payloads, and the resulting term/vote promise with one file sync. That sync precedes
SQLite projection updates and persistence-dependent replication messages. A client
success also requires quorum commitment and application. SQLite uses NORMAL for
accepted-entry and query projections between FULL snapshot roots; losing those
updates cannot lose accepted bytes because startup replays the journal first.
Commit-index notifications may persist asynchronously, as permitted by Raft. A new
leader commits a current-term observation before exposing the recovered model.
Indexes expose only the committed prefix; the accepted suffix remains available
for conflict replacement. Standalone storage keeps its existing FULL behavior.
Event and content lookup indexes use connection-local SQLite temporary tables,
with a 4 MiB page cache and spill to disposable files. They are rebuilt from the
committed prefix and FULL snapshot before voting or serving. Complete accepted
entries remain in both the journal and SQLite; snapshot payloads remain in the
FULL root. The persisted `event_sequence` metadata reports committed projection
progress for diagnostics and does not authorize reads or voting.
Input validation, Raft storage reads and projection updates share one transaction
for already queued Raft inputs (at most eight, with a 100 µs admission budget and
no wait for more input). Each input applies committed configuration changes before
the next is authenticated. Client replies wait for successful transaction completion
and publication of the committed service epoch. A committed membership position is
sealed before applying the new voter configuration, so lost SQL projection writes
cannot resurrect a removed voter.
New cluster projections use 1 KiB SQLite pages to reduce WAL write amplification;
existing databases keep their page size. Passive SQLite page checkpoints run on a
separate connection after 1,024 committed projection row changes and at least 2,048
pages of WAL capacity (2 MiB for new databases, 8 MiB for legacy 4 KiB pages). Retained capacity alone
does not trigger another checkpoint. The primary connection retains an automatic
checkpoint fallback at 32,768 frames, with a retained WAL size limit of 32,768
database pages (about 32 MiB for new databases, 128 MiB for legacy 4 KiB pages).
Checkpoint I/O errors fence the voter. These physical page checkpoints
preserve FULL snapshot roots and do not change journal syncing.

The journal retains the `.cluster-watermark` filename. Two fixed-size authenticated
control records bind its root, published length, hash chain and voting/log position.
Both must validate with consecutive generations. A torn control record or damaged
published payload requires quorum repair, even if an older record is readable.
Bytes beyond the authenticated published length are unconfirmed and can be ignored.
Journal space is reserved in 1 MiB chunks; physical file size can exceed that
authenticated length. Unsupported reservation falls back to ordinary file growth.
This distinguishes an interrupted append from damage to acknowledged history.
SQLite restored within the same checkpoint root can replay its missing projection;
an unrelated or older root requires quorum repair. A newer SQLite term or accepted
suffix outside the journal also requires repair.

Snapshots atomically install a FULL SQLite root and its signed journal predecessor.
Only after that commit does an atomic file replacement reclaim the old journal.
Startup completes an interrupted replacement only when the signed root names the
exact old journal tip. Old JSON and metadata-only fences upgrade during ordinary
startup after validating their FULL SQLite history. A synced format marker prevents
reverting an upgraded store to an old fence. Older binaries cannot open the new
local format; there is no separate migration command.

Read-only responses use Raft ReadIndex quorum checks, without appending empty log
entries. A committed mutation supplies the quorum observation for its own reply;
duplicate replies and empty batches still require ReadIndex. These observations
are not timed leases and are not reused for newly arriving client requests.
Raft's persistence-independent leader messages may start follower writes while
the leader flushes locally. Term/vote changes, follower ACKs and client success
remain behind their required persistence boundaries.

Checkpoints atomically publish the complete snapshot and reclaim its covered log
prefix. Snapshot metadata binds the model, event position, term/index, configuration,
member incarnations and retired identities. SHA-256 manifests bind every required
body. Local payload rows stay separate from the model and are verified at startup.
Complete accepted entries after the snapshot remain available to Raft until they
are committed or replaced.

A returning member whose log falls behind the retained prefix receives a complete
snapshot, then its tail. Transfer uses authenticated chunks, an overall digest,
explicit byte budgets and a 30-second transfer deadline. Peer connections persist
across messages; framing, authentication and incarnation checks apply to every
message. Failed connections are discarded and Raft retries through a new connection.
No incomplete transfer
changes the recovery root. Installation, hard state and covered-prefix deletion
share a FULL transaction. A donor may purge data or publish a newer image while an
older transfer is in flight; the already-constructed transfer retains its bytes.
Snapshot retries restart the transfer rather than trusting an incomplete image.

The leader checkpoints automatically after 8,192 Raft positions, when the log uses
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
