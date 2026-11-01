# Clustering plan

Proposal only: no routes, replication, membership changes or distributed durability
are implemented. Long-running standalone fault and upgrade gates are prerequisites. NATS-compatible clients and interoperating with a Go NATS cluster are
separate goals; the latter also needs its route/account/JetStream internal protocols.

## Proposed contract

Use the same synchronous application state machine and logical commands on each
node. Transport, timers, randomness, disk completions and consensus are adapters.
Do not replicate SQLite files or WAL byte offsets: replicate versioned commands and
stable object IDs. SQLite remains each node's local durable substrate.

Core routing retains at-most-once semantics and availability on connected nodes.
Durable stream/consumer operations require a majority. A partitioned minority must
reject or time out durable requests; it must not acknowledge independently. A successful
publication must be recoverable after any permitted sequence of node crashes.
A timeout is an unknown outcome, not an aborted write. Reads of mutable durable state
use a current leader's quorum/read-index barrier; stale follower reads require an
explicitly weaker interface.

Start with three voters and one Raft group for the entire catalog, streams and
consumers. This sacrifices parallelism but avoids cross-group create/delete/consumer
transactions in the first implementation. Per-stream groups come only after measured
need and a design for cross-group lifecycle fencing. Five voters and topology-aware
placement are a later option, not an automatic availability improvement.

## Implementation sequence and gates

1. **Harden standalone semantics.** Consumers, delivery reservations,
   explicit ACK/NAK/progress/TERM, deduplication and conditional publication, injected
   time in preparation and immutable stream/consumer generation IDs so delete and
   recreate cannot inherit pending state. Gate: official clients, independently checked
   operation histories, process/VM recovery and bounded storage tests all pass.
2. **Define persistence and snapshots.** Add hard state `(term, voted_for)`, log
   `(index, term, command, checksum)`, committed/applied indices, object generations
   and a snapshot manifest. Atomically persist hard-state/log updates before voting
   or acknowledging append entries. Application state and its applied index must be
   one transaction. Snapshot records its exact covered index/term/configuration/hash.
   Sync snapshot contents and manifest, publish with atomic rename and directory sync,
   persist the covered index, and only then truncate covered log entries. Gate: crash
   or I/O failure at every step never loses acknowledged effects; corruption blocks
   serving and never authorizes deleting local objects.
3. **Select a consensus library with a spike.** Compare [OpenRaft](https://github.com/databendlabs/openraft)
   and [raft-rs](https://github.com/tikv/raft-rs) against required persistence ordering,
   snapshots, membership changes, backpressure, reproducible scheduling and maintenance
   burden. Prefer a mature library to a new Raft implementation. OpenRaft provides
   higher-level integration; raft-rs exposes a lower-level state machine suited to
   explicit event pumping. These are candidates, not a completed choice. Gate: a
   simulated three-node append/read/change-membership prototype passes fault tests
   and its durable write ordering is documented against the [Raft paper](https://raft.github.io/raft.pdf).
4. **Build deterministic simulation first.** Represent network send/deliver/drop,
   timeout, disk-write/sync completion and restart as explicit events. A simulated disk
   separates volatile writes from stable writes: process crash and power loss have
   different effects. Generate partitions, reordered/duplicate messages, asymmetric
   links, leader isolation, pauses, disk-full/EIO and bounded corruption with proptest;
   preserve and shrink schedules/seeds. Check election safety, log matching, committed
   prefix agreement, monotonic application and acknowledged-set survival. Add an
   independent linearizability checker for append/CAS/metadata/ACK histories, preserving
   unknown outcomes. Liveness checks begin only after a stable majority and bounded
   scheduling/disk assumptions return.
5. **Connect real replication and clients.** Authenticate node identities with mTLS;
   persist node IDs and cluster IDs to prevent accidental cross-cluster joins. Add
   bounded peer queues, chunked snapshots with hashes and resumable transfer, current
   term/generation fencing, request forwarding, leader discovery and reconnect URLs.
   Persist a majority before success; group commit may amortize fsync but never weaken
   that rule. Test joint-consensus membership changes and learner catch-up before
   promotion. Explicit removal must be committed; absence is not permission for orphan
   cleanup. Gate: three/five-node process and independent-VM campaigns pass with all
   acknowledged writes and consumer state recoverable after fault healing.
6. **Add Core routes and distribution.** Model local versus remote interest, ownership
   and subscription generations. Ensure one routing path per logical peer, deduplicate
   simultaneous route establishment and avoid forwarding loops. Queue selection must
   count eligible local/remote members once, preserve already-enqueued deliveries across
   UNSUB and avoid unbounded remote caches. If Go NATS route interoperability is
   required, implement and test the [route protocol](https://docs.nats.io/reference/protocols/route)
   (`INFO/CONNECT`, `RS+`, `RS-`, `RMSG`, heartbeat, accounts/queue weights) against a
   pinned upstream server. This alone does not provide JetStream-cluster interoperability.
7. **Release campaign and optimization.** Exercise mixed-version rolling upgrades,
   snapshot install interrupted at each boundary, simultaneous restart, minority disk
   corruption, disk replacement, clock changes, node-ID reuse and repeated topology
   changes. Run an adapted [Jepsen NATS harness](https://github.com/jepsen-io/nats),
   add a linearizability checker, retain raw histories and check per-node divergence.
   Benchmark healthy, degraded and recovering clusters, including p99/p999 latency,
   fsync latency, recovery duration, resource use and bounded queues. Publish failing
   histories as release blockers before optimizing batching/sharding.

## Storage design choice

Keep SQLite for the first replicated implementation: hard state, commands, catalog,
applied indices and a materialized state can share transaction boundaries. Implement
batched append/sync APIs rather than mapping each Raft entry to an independent
transaction. The current `Store::append` / `append_batch` log is a useful single-node seam but is not
sufficient as a Raft storage API; term/index, atomic batches, conflict truncation and
snapshot-install contracts must be added explicitly.

Before splitting into per-stream groups, decide how committed catalog deletion fences
an independently recovering stream leader, how a consumer's generation is bound to
its stream generation, and how metadata restores avoid reviving deleted objects.
Do not solve this with best-effort cleanup, directory names or wall-clock timestamps.
