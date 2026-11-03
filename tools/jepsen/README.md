# Screech / Jepsen / LazyFS

This runs the queue workload and loss/divergence checker from
[jepsen-io/nats](https://github.com/jepsen-io/nats), using the real Jepsen library,
Java NATS client, and [LazyFS](https://github.com/dsrhaslab/lazyfs). Exact upstream
revisions and dependency versions are in [upstream.json](upstream.json).
The adapted upstream files retain their EPL-2.0 attribution and license.

## Run

Requirements: x86-64 Linux, Nix with flakes enabled, a C++ compiler, Cargo, and
network access for dependencies. The runner uses QEMU TCG; host root, Docker,
KVM and `/dev/fuse` are unnecessary. Allow about 4 GiB for the guest, 2 GiB for
Jepsen and space for the dependency cache and sparse 2 GiB guest disk.

From the repository root:

```sh
cargo build --locked
python3 tools/jepsen/bootstrap.py
python3 tools/jepsen/check.py
python3 tools/jepsen/vm.py \
  --runtime results/jepsen-runtime/runtime.json \
  --binary target/debug/screech-server \
  --output results/jepsen-five-voters
```

`bootstrap.py` builds the pinned LazyFS source and resolves the Jepsen dependencies;
it records the resolved Nixpkgs source and runtime store paths. Use
`bootstrap.py --nixpkgs <locked-flake-URI>` to select a specific package set. It refuses to
replace an edited LazyFS checkout. The source checkout and dependency cache live
under the ignored `upstream/` and `results/` directories. It does not install
services or change host network/filesystem settings.

The default campaign uses five voters, ten client threads, 30 publications/s,
30 seconds of workload per scenario and up to 60 seconds for the final drain.
Use a new output directory for each run. For example:

```sh
python3 tools/jepsen/vm.py \
  --runtime results/jepsen-runtime/runtime.json \
  --output results/jepsen-three-voters --nodes 3 \
  --scenarios power,pause-kill,partition,corrupt,wipe \
  --time-limit 60 --rate 20 --final-time-limit 120
```

A short duration that never reaches all the required faults fails the assurance
checker. Increase the duration when testing a slow machine or sustained outages.
The runner returns nonzero on an invalid history, incomplete campaign, checker
exception or timeout. `results.json` contains each checker result and runtime
provenance. Each `runs/<workload-scenario>/` contains the controller log and normal Jepsen
store: histories, per-node logs, canary evidence, loss plots and checker output.
The guest serial log and disk image are retained as well.

## Faults and controls

Each voter runs as a separate real process in its own guest network namespace,
with its own LazyFS mount and backing directory. The mount covers the database,
WAL, recovery generations, watermark and node manifest. All starts use the same
ordinary `--store ... --cluster-config ...` command. No bootstrap, join, recovery
or replacement command is used.

Before a voter starts, a real FUSE mount is required. A canary writes both dirty
and fsynced data, clears the filesystem cache and verifies that only the fsynced
bytes survive. Cache loss always waits for LazyFS's `finished::clear-cache`
completion notification. Sending a FIFO command alone does not count as success.

| Scenario | Repeated schedule |
| --- | --- |
| `baseline` | Concurrent durable publications and final drain, without workload faults. |
| `power` | Kill a majority, discard their unsynced filesystem data, restart; then do the same to all voters. |
| `pause-kill` | Pause one minority, kill and clear the pivot voter's cache, resume the first minority, pause the other minority, restart the pivot, then heal. This follows the overlapping-quorum crash/pauses pattern in the NATS investigation. |
| `partition` | Split the group, kill and clear a voter, then restore links and processes. |
| `corrupt` | Kill and clear `n1`, flip a byte in its active database header, restart; later kill, clear and truncate that database, then restart. |
| `wipe` | Kill and clear `n1`, delete its entire mounted voter state, and restart at the same configured address and slot. |

Disk damage targets one fixed voter so successive faults cannot deliberately
destroy every copy of committed history. Faults run during publication, followed
by link healing, process resumption, normal starts and the final read phase.
Required fault counts, per-mount canaries and nonzero acknowledged writes are
checked independently of the workload checker. After the final drain, an additional
read-only inspection requires every local manifest identity to be a current voter,
no unfinished replacement or joint configuration, and equal persisted application
sequence/view positions on all voters, matching the leader. Per-node
`convergence.json` files record that evidence.

The controller JVM runs on the host to avoid JIT faults under CPU emulation.
Jepsen's `Remote` implementation executes controls inside the guest through an
authenticated HTTP endpoint forwarded only on host loopback. Clients use opaque
TCP forwarding to the chosen voter's public gateway; server discovery is disabled.
Partitions apply inside the voter namespaces to the real Raft, service and client
traffic. No fault executes host `iptables`, host `kill` or host disk corruption.
The guest control service requires an isolated-VM marker and a per-run random
token; the runner removes the token when the guest stops.

## Queue workload

The NATS queue workload publishes unique process/counter values. Once faults heal,
each client creates an independent ephemeral consumer and drains the stream.
The upstream checker requires every acknowledged publication to be read globally
and through every configured public node address, and rejects unexpected values.
It reports prefix, middle and suffix loss separately. Client timeouts are uncertain
operations, never successful publications. The added checker tests deliberately
remove data globally and on just one node, introduce an unattempted value, and
reject empty or incompletely faulted histories.

Public gateways forward to the current leader, so the upstream per-address check
proves client-visible agreement after recovery. The additional final inspection
proves that local identities were admitted as voters and durable application
positions converged. It does not compare every follower's payload bytes directly;
record validation and deeper recovery-image checks remain in the Rust/process
recovery suites.

## Consumer ACK and lifecycle workload

Use `--workloads consumer-lifecycle` for the consumer histories, or
`--workloads queue,consumer-lifecycle` to run both suites against every selected
fault schedule. The queue workload remains the default. Consumer histories use one
client per node, seed fixtures before faults, then attempt up to two more cycles
per client during faults. All confirmed results in an interrupted cycle remain
binding; ambiguous requests may have committed and are never labelled confirmed.

```sh
python3 tools/jepsen/vm.py \
  --runtime results/jepsen-runtime/runtime.json \
  --output results/jepsen-consumer-five --nodes 5 \
  --workloads consumer-lifecycle --rate 1 \
  --time-limit 30 --final-time-limit 180
```

Each cycle owns three streams and records individual successful API responses in
its Jepsen completion, including completions of type `:info`. An ACK counts as
confirmed only after receiving its empty, non-status response to a request on the
actual delivery ACK subject. A Core `PONG`, successful socket write, or ACK timeout
does not count as confirmation. The audit also journals every successful read and
deduplication/stale-ACK probe, so a later request timeout cannot erase evidence of
a violation.

| Fixture | Required behavior through faults and final recovery |
| --- | --- |
| Limits retention | A confirmed ACK advances the durable consumer floor and leaves no pending ACK. The payload stays present. A separately deleted consumer stays absent. Retrying the original publication ID with different bytes returns `duplicate=true` and the original sequence, with one unchanged stored record. |
| Interest retention | A message ACKed by both consumers stays deleted. A second message ACKed by just one consumer stays present with the other consumer's reservation pending. Both consumers retain their confirmed ACK floors. |
| Workqueue retention and recreation | A confirmed ACK's message stays deleted. A second, unacknowledged message is reserved, its consumer is deleted and recreated under the same name, and the new consumer receives that message with a new generation. The old ACK is replayed before and after faults; it must neither confirm nor remove the new reservation or advance its ACK floor. Retrying the deleted message's publication ID must still deduplicate without resurrecting it. |

Every public node address audits every attempted cycle after healing. The checker
reconstructs required stream/consumer existence, ACK floors, delivery reservations,
retention and deduplication results from recorded operations, rather than trusting
an expected-state object returned by the server. It also checks ACK policy and
ACK-wait configuration, pending counts, original payload bytes and recreated
consumer generations. Shared client state is an address catalog and probe-target
list; it does not decide which recorded obligations the checker enforces. Failed
requests leave weaker obligations only for their own unknown outcomes, preserving
earlier confirmed results. Missing audits, missing seeded coverage, skipped faults,
or missing final voter convergence fail the run.

To replay the consumer checker against saved histories without restarting any
servers, run:

```sh
python3 tools/jepsen/check.py --recheck results/jepsen-consumer-five
```

This writes `rechecked-consumers.json`. A successful report requires both the
original complete campaign and all replayed consumer checks to pass.

The deduplication window is 24 hours and ACK wait is one hour, keeping expiry and
scheduled redelivery outside these bounded histories. Tests deliberately leave
some messages pending and inspect their persisted reservations. They exercise
consumer creation, deletion and recreation, three retention policies and retries
of publication IDs. They do not cover every consumer option, retention count/age
limit, stream deletion/recreation or full cross-stream linearizability.

## Scope

The queue workload derives from the
[NATS Jepsen investigation](https://jepsen.io/analyses/nats-2.12.1); the consumer
workload adds Screech protocol histories. Neither suite is a full linearizability
or general exactly-once test. All voters share one guest kernel and backing virtual
disk. LazyFS models loss of unsynced file data, rather than arbitrary controller
corruption, coherent rollback of an entire quorum, independent-machine power loss,
or every filesystem metadata/reordering behavior. These histories are correctness
checks and are not throughput/latency benchmarks or production certification.

## Checked queue runs

The completed three- and five-voter campaigns each passed all six scenarios, using
20 publications/s, 30 seconds of faulted workload per scenario and up to 90 seconds
for the final drain. Both used LazyFS 0.3.0, Jepsen 0.3.10 and jnats 2.24.1, with
Linux 6.18.59 under QEMU TCG.

| Scenario | Three-voter acknowledged publications | Five-voter acknowledged publications |
| --- | ---: | ---: |
| `baseline` | 545 | 578 |
| `power` | 69 | 110 |
| `pause-kill` | 211 | 241 |
| `partition` | 409 | 438 |
| `corrupt` | 280 | 377 |
| `wipe` | 340 | 418 |

All 4,016 acknowledged publications were retained globally and through every
public node address, with zero unexpected values. All twelve final membership and
durable-position inspections passed. The 48 mount canaries demonstrated both
unsynced-byte loss and fsynced-byte preservation; there were 93 additional
acknowledged process-kill/cache-clear operations. The checker negative controls
passed 13 assertions across two tests. These histories required no server changes.

The tested server binary SHA-256 was
`95a41ee9b1ebda6d8394a1bca04ce766192f6d1b564fb1143dd91b2eff18cc84`.
The harness implementation is commit `81eb0f7`; server source was unchanged by the
harness commit. Full results, histories, plots, canaries and convergence evidence
are retained locally in `results/jepsen-three-voters-retry/` and
`results/jepsen-five-voters-retry/`. They remain ignored artifacts.

During bring-up, failed attempts exposed a missing guest `getent` dependency and
an inspection request that failed immediately on a transient gateway disconnect.
The assurance checker rejected those attempts. The guest now includes the resolver
tool, and final inspection retries disconnects within its bounded recovery window;
both complete campaigns then passed. The scope limits above still apply.
