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
provenance. Each `runs/<scenario>/` contains the controller log and normal Jepsen
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

## What is checked

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

This is the publication/queue portion of the
[NATS Jepsen investigation](https://jepsen.io/analyses/nats-2.12.1), not a full
linearizability or exactly-once test. It does not yet adapt confirmed consumer ACK,
retention, deduplication or catalog lifecycle workloads. Those contracts have
separate model/process regressions. All voters share one guest kernel and backing
virtual disk; LazyFS models loss of unsynced file data, not arbitrary controller
corruption, coherent rollback of an entire quorum, independent-machine power loss,
or every filesystem metadata/reordering behavior. These histories are correctness
checks and are not throughput/latency benchmarks or production certification.

## Checked runs

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
