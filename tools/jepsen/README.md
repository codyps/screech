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

## Independent voter VMs and native power loss

`cluster_vm.py` places each voter in its own QEMU process, kernel, RAM and native
ext4 disk image. It uses the same Jepsen workloads and consumer checker. It needs
the same bootstrap/runtime setup as the LazyFS runner, with about 1 GiB of guest
RAM and two virtual CPUs per voter by default. Host root, Docker, KVM, TAP devices
and host network changes are unnecessary.

```sh
python3 tools/jepsen/cluster_vm.py \
  --runtime results/jepsen-runtime/runtime.json \
  --output results/jepsen-native-five --nodes 5 \
  --time-limit 45 --final-time-limit 180
```

The default workload is `consumer-lifecycle`. Use `--workloads queue` or
`--workloads queue,consumer-lifecycle` to select queue histories too. The following
native scenarios are the default matrix:

| Scenario | Fault |
| --- | --- |
| `baseline` | Seed and audit consumer fixtures after the power-loss control. |
| `vm-power` | Simultaneously cut power to a majority, recover, then cut power to all voters and recover. |
| `vm-damage` | Cut all VMs; flip the active database and WAL headers on a minority, restart all; later repeat with truncated active database/WAL files. |
| `vm-wipe` | Cut all VMs, erase a minority's entire voter stores including manifests/watermarks, then restart all. |
| `vm-blank` | Cut all VMs, archive a minority's old disk images and attach newly formatted empty ext4 disks, then restart all. |

A loopback Ethernet hub forwards opaque frames between each VM's private NICs,
using QEMU's [TCP socket network framing](https://github.com/qemu/qemu/blob/master/net/socket.c).
Each guest runs the actual TCP/Raft stack. A separate management NIC and stable
TCP forwarding keep Jepsen controls and public client addresses reachable across
reboots; no replication or application messages are synthesized. Every voter has
a distinct kernel boot ID, checked again after recovery.

A group power cut pauses the voter threads without syncing, overwrites a previously
allocated control file without fsync, then stops all target virtual CPUs through
QMP. After every target reports paused, the driver sends SIGKILL to every QEMU
process before waiting for any of them to exit. No guest shutdown, filesystem sync
or QMP quit occurs before the cut. The dispatch interval must be below 250 ms and
is recorded in `power-events.json`. All VM disks use `cache=none`.

Before workloads start, a real all-voter cut establishes the fault controls.
Every reboot must produce a new kernel boot ID, preserve the fsynced canary and
lose its unsynced overwrite. Recovery must never recreate missing control files;
only an explicitly fresh, formatted test disk may initialize them. The guests
suppress periodic writeback and use an ext4 journal commit interval of 600 seconds
to expose reliance on unsynced data; explicit fsync and disk flush semantics remain
required. The settings are local to the disposable guest kernels.

Disk mutations happen after the original power loss in a maintenance boot of the
affected disk. This boot checks the cold-reboot controls and never starts Screech.
The driver persists the injected file damage before the ordinary voter boot.
Corruption and truncation affect both the active database and its WAL when present.
Changing only the base file can leave the complete durable history recoverable
from the WAL and does not necessarily require replacing a voter. Mutation proofs
record each changed path, size and digest. For
`vm-blank`, it preserves the original image and provisions an empty replacement.
At most one of three or two of five stores are damaged, retaining a healthy quorum's
history. The test records the old identities and requires every damaged slot to
return as a different admitted voter. All normal starts use the same ordinary
`--store ... --cluster-config ...` command and the same static slot/address
configuration. No bootstrap, join or repair command is involved in product recovery.

Final audits verify the confirmed ACK, retention, deduplication and recreation
obligations through every public node. The independent-node inspection additionally
requires full membership, no outstanding joint replacement, equal durable positions,
separate kernels and verified reboot controls for every power cut. A reused kernel,
missing canary, unverified cut or unfinished voter replacement fails the run.

Artifacts include each VM's serial logs, its final disk, archived replacement disks,
`boots.json` with QEMU commands/boot IDs, `power-events.json`, normal Jepsen histories,
per-node logs and `results.json`. Each `runs/<case>/convergence.json` is a host-side
copy of the full proof, including all power canaries and disk mutations. The
successful final `:verify` operation also preserves that proof in `history.edn`.
Guest-side proof files are best effort: Jepsen cuts power before downloading logs,
so recent unsynced proof writes can disappear. Consumer histories can be
replayed with `check.py --recheck` just like LazyFS histories. Use a fresh output
directory for every campaign. The driver defaults to a one-hour overall deadline;
`--memory` sets guest MiB per voter and `--timeout` changes that deadline.

### Active I/O, torn writes and disk errors

Select the additional active-I/O schedules explicitly:

```sh
python3 tools/jepsen/cluster_vm.py \
  --runtime results/jepsen-runtime/runtime.json \
  --output results/jepsen-inflight-five --nodes 5 \
  --scenarios vm-active-write,vm-active-flush,vm-torn,vm-eio-write,vm-eio-flush \
  --time-limit 90 --final-time-limit 600 --timeout 5400
```

These schedules automatically attach a disposable controller-side
[NBD](https://github.com/NetworkBlockDevice/nbd/blob/master/doc/proto.md) device
to every VM. The real guest ext4/virtio stack issues its normal block requests.
Normal writes enter an explicit volatile cache; reads see that cache, and successful
FLUSH/FUA replies follow host `fsync`. A power cut discards the cache. The backend
advertises only READ, WRITE, DISC and FLUSH, supports FUA, and does not synthesize
filesystem or Screech responses. Other native schedules keep their direct raw disks
unless selected in the same campaign as an active-I/O schedule.
Use `--fault-disks` to select these devices explicitly for other native schedules.
For example, `--fault-disks --scenarios vm-power` checks majority/all-voter cold
recovery after confirmed writes, discarding every target's volatile device cache.

Each fault chooses the serving leader and at most a minority of voters. The
remaining quorum's kernels/disks keep running. Both publication and consumer ACK
requests must be interrupted in every scenario. Jepsen submits an asynchronous
NATS request before reporting its open window; completion callbacks close it,
including completion that races the opening notification. The proof IDs must
match attempts in the saved consumer history. Observer errors invalidate a history.
Before each fault, live replica identities must match the complete voter set,
without learners, outgoing voters or an unfinished replacement. Those identities
and membership are included in the proof; configured slots alone do not establish
a surviving quorum. A failed matching attempt disarms its device and cold-restarts
any device already holding a request, then reports failure rather than leaving a
latent stall behind.
Each active schedule injects one fault during publication and one during an ACK.
The main generator allows up to twelve additional cycles per client, followed by a
required successful recovery lifecycle through every public address and final
audits of both seeded and interrupted cycles.

| Scenario | Boundary and fault |
| --- | --- |
| `vm-active-write` | Hold a real database/WAL block write without replying, then cut power. |
| `vm-active-flush` | Hold a real block FLUSH without replying, then cut power. |
| `vm-torn` | Persist exactly the first 512 bytes of a changing multi-sector database/WAL request, withhold the reply, then cut power. |
| `vm-eio-write` | Return `EIO` for a real database/WAL block write, observe a guest kernel I/O error, then cut power. |
| `vm-eio-flush` | Return `EIO` for a real block FLUSH, observe a guest kernel I/O error, then cut power. |

FIEMAP without its sync flag identifies allocated authoritative file extents.
Selected writes must fit entirely inside one extent. Torn proofs include the
old, requested, mixed and cold disk hashes; the persisted prefix and lost suffix
must both change bytes, and the cold disk must match the expected mixed image
before any recovery boot. This models a partial uncompleted write, not loss of
a correctly completed flush on the surviving quorum.

Active cuts never SIGSTOP the voter threads inside the guest. After arming the
unsynced canary, they SIGSTOP the QEMU host process and require its stopped status,
then dispatch every SIGKILL before awaiting any exit. This avoids QMP stop's I/O
drain, which cannot complete a deliberately held request. A held/torn request must
still overlap an open client request at the cut. EIO must have a real error reply
and guest kernel evidence; it may already have failed the client request when
power is cut. The matched EIO request is initially held while the controller arms
the canary, then its real error reply is delivered. Later disk requests remain
held until the cut. This permits Linux to remount the filesystem read-only without
requiring a canary write to that read-only filesystem or allowing a later flush to
persist the dirty canary. All cuts still require new kernel IDs and the cold-boot canaries.
Recovery uses ordinary voter starts, without removing or editing application files.
The final `io_faults` proof is retained beside the other convergence evidence.

## Mixed network, power and disk faults

Run the mixed matrix with either three or five independent voters:

```sh
python3 tools/jepsen/cluster_vm.py \
  --runtime results/jepsen-runtime/runtime.json \
  --output results/jepsen-mixed-five --nodes 5 \
  --scenarios vm-split-bitflip,vm-split-wipe,vm-send-bitflip,vm-send-wipe,vm-receive-bitflip,vm-receive-wipe \
  --time-limit 30 --final-time-limit 300 --timeout 5400
```

These scenarios select volatile NBD disks automatically. The Ethernet hub drops
directed voter-to-voter frames, using the private NIC's fixed MAC to retain policy
across reboots. `split` drops both directions across a majority/minority boundary;
`send` drops minority-to-majority traffic, and `receive` drops majority-to-minority
traffic. The minority includes the original serving leader. Management connections
stay reachable, and public NATS connections still use the actual cluster gateway.
Static private-neighbor entries let UDP controls verify the allowed direction of
one-way links without depending on reverse ARP traffic.

Every case requires a complete admitted voter set before installing its partition.
Real guest UDP controls must reach every allowed edge and no blocked edge, with
positive hub forwarding/drop counters. The control repeats after the minority
reboots, requiring new kernel IDs only on those targets and the same link policy.
A final control requires every directed edge to work after healing.

The quorum must confirm conditional writes before the cut, while the minority is
off, after its damaged disks restart behind the partition, and after healing.
Each phase requires two successful conditional-write rounds and two successful
local-service read rounds. During separation these successes must come from the
untouched majority; after healing any admitted voter may lead. The power cut uses the existing
thread-pause/cold-boot controls; the other voters keep running. Maintenance boots
then either corrupt both the active database and WAL when present (`bitflip`) or
erase the entire store, including identity/watermark files (`wipe`). No voter runs
while its disk is being damaged. Normal starts and restored links must complete
automatic membership replacement; recovery uses the retained disk images.

A shared stream exercises `Nats-Expected-Last-Sequence` concurrently through every
voter's own leader-local service. The witness requires at most one confirmation per
expected position and consistent confirmed payloads across rounds. Fresh catalog
creation probes must never succeed in the isolated minority, including on empty
replacement stores. Successful local and quorum reads must preserve every earlier
observed committed prefix. This tests the old leader directly during separation.
After healing, every public address must return every confirmed witness payload.
The consumer checker separately verifies confirmed ACK floors, retention,
deduplication, recreation and stale ACK isolation, with a fresh successful lifecycle
through each public address.

The 30-second main-client scheduling window bounds extra consumer cycles; the
in-flight composite nemesis may continue beyond it. It has a five-minute control
deadline and retains conditional-write traffic during the fault and repair phases.
Recovery cycles and audits start only after it finishes. Full link controls,
conditional operations, disk mutations and audits are included in the final
`network_faults` proof and saved Jepsen history. `mixed-progress.json` also retains
partial evidence when a fault or safety check fails. Replay validates these proofs
independently. Neither successful role claims nor missing responses establish a
successful conditional write.

## Scope

The queue workload derives from the
[NATS Jepsen investigation](https://jepsen.io/analyses/nats-2.12.1); the consumer
workload adds Screech protocol histories. Neither suite is a full linearizability
or general exactly-once test. The LazyFS runner shares one guest kernel and backing
virtual disk; the independent-VM runner uses separate guest kernels and block
images. Both run on one physical host. LazyFS models loss of unsynced file data,
rather than arbitrary controller
corruption, coherent rollback of an entire quorum or every filesystem
metadata/reordering behavior. Native VM power cuts discard each target kernel and
its page cache; physical-host power loss and storage-controller failures are not
simulated, and host storage must honor flushes. Disk damage tests require a
mountable filesystem or a provisioned replacement block volume. These histories are correctness
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

## Checked consumer runs

Three- and five-voter consumer campaigns each passed all six LazyFS schedules.
Each used one client per node, a one-cycle/s main generator, a 30-second fault
phase and a 180-second final audit scheduling window, preceded by seeded fixtures.
The main generator attempts at most two additional cycles per client.

| Scenario | Three-voter confirmed ACKs | Five-voter confirmed ACKs |
| --- | ---: | ---: |
| `baseline` | 45 | 75 |
| `power` | 17 | 27 |
| `pause-kill` | 22 | 27 |
| `partition` | 33 | 53 |
| `corrupt` | 26 | 32 |
| `wipe` | 32 | 46 |

The 144 attempted cycles recorded 435 confirmed ACK responses and
435 acknowledged publications. All required consumer/stream ACK floors,
reservations, configuration, retention, deduplication and recreation obligations
passed, including obligations from interrupted cycles. All 48 successful node
audits and all twelve final voter-membership/durable-position inspections passed.
The 48 real-mount canaries passed, alongside 95 acknowledged process-kill/cache-clear
operations. This exercises ordinary automatic voter replacement without a separate
recovery command.

All saved histories were replayed against the final checker (`46d057f`), including
its checks that both ACK floors preserve confirmed positions and stay behind pending
reservations. Both replay reports passed. The checker suite passed 62 assertions
across eight tests. A separate three-voter queue baseline passed to check the
existing default workload after adding workload selection. No server changes were
required; the binary hash matches the earlier queue campaigns above.

The implementation is commit `9e03543`, followed by the ACK-floor oracle refinement
in `46d057f`. Full histories, original results, `rechecked-consumers.json`, node logs,
canaries and convergence records are retained under the ignored
`results/jepsen-consumer-three/` and `results/jepsen-consumer-five/` directories.
The compatibility history is in `results/jepsen-queue-compatibility/`. The runtime
versions and single-kernel scope are the same as the queue runs.

## Checked independent-VM runs

Three- and five-voter native-ext4 campaigns each passed all five scenarios. Every
voter had its own QEMU process, Linux kernel, 1 GiB RAM and sparse 2 GiB block
image, using two TCG vCPUs and `cache=none`. Each campaign used one client per
voter, a one-cycle/s main generator, a 45-second fault phase and a 180-second final
audit window. Clients seed one fixture cycle each and attempt two more during
faults.

| Scenario | Three-voter confirmed ACKs | Five-voter confirmed ACKs |
| --- | ---: | ---: |
| `baseline` | 45 | 75 |
| `vm-power` | 15 | 25 |
| `vm-damage` | 16 | 25 |
| `vm-wipe` | 15 | 26 |
| `vm-blank` | 15 | 25 |

All 120 attempted cycles passed their obligations: 282 confirmed ACK responses,
288 acknowledged publications, retention, deduplication, consumer recreation and
stale ACK isolation. All 40 public-address audits and ten final membership and
durable-position checks passed. Replay of both campaigns also passed the consumer
checker and revalidated the physical fault controls from the saved histories.

The campaigns recorded 28 grouped power cuts, including the all-voter preflight
in every scenario, and 106 successful cold-boot checks. Every check preserved the
fsynced canary, lost the unsynced overwrite and observed a different kernel boot
ID. All targets were paused before killing; no exit was awaited until all kill
signals were sent. The largest measured signal dispatch skew was 0.932 ms. These
controls bound the model described above; they do not establish arbitrary
mid-fsync or torn-controller-write coverage.

All 18 injected damaged-voter instances recovered as different, fully admitted
voter identities through ordinary server startup. The damage schedule modifies
both the active database and its WAL; whole-store erasure and new blank volumes
also passed. A bring-up attempt that damaged only the main database correctly
recovered from its intact WAL and retained its identity, so the fault injector was
corrected to damage both files before these complete runs. Damage is confined to
a minority while every VM loses power, preserving enough durable copies to repair.

The fault-driver tests passed five Python tests and the checker suite passed nine
Clojure tests with 76 assertions, including rejection of ineffective/reused boot
controls. A three-voter LazyFS queue baseline also passed after the new topology
was added. No server changes were required; the server binary SHA-256 matches the
earlier campaigns above.

The implementation is `8b53257`, with database/WAL damage coverage in `25c8c7a`
and host-side proof retention/replay in `de5b0de`.
Complete results, histories, runtime metadata, block images, `boots.json`,
`power-events.json`, per-case `convergence.json` and replay reports are retained
under the ignored `results/jepsen-native-three-complete/` and
`results/jepsen-native-five-complete/` directories. The scope remains independent
guest power loss on one physical host, with mountable filesystems or provisioned
replacement volumes and a surviving durable quorum.

## Checked active-I/O runs

The final three- and five-voter campaigns each passed all five active-I/O
schedules, with one independent VM and volatile NBD device per voter. They used
one client per voter, a one-cycle/s generator, a 90-second fault phase and up to
600 seconds for fresh recovery lifecycles and audits. Each client seeds a fixture
and attempts at most twelve more cycles during faults. Both publication and ACK
requests were faulted in every scenario.

| Scenario | Three-voter confirmed ACKs | Five-voter confirmed ACKs |
| --- | ---: | ---: |
| `vm-active-write` | 105 | 132 |
| `vm-active-flush` | 100 | 133 |
| `vm-torn` | 103 | 108 |
| `vm-eio-write` | 99 | 103 |
| `vm-eio-flush` | 98 | 106 |

These ten histories preserved 1,087 confirmed ACK responses and 1,112 acknowledged
publications. All 40 required recovery lifecycles and public-address audits passed,
including ACK floors, retention, deduplication, consumer recreation and stale ACK
isolation. Each fault's actual client request ID matched a saved attempt. Before
each cut, live identities matched the full admitted voter set with no unfinished
replacement, and the untouched voters formed a majority.

The 20 active faults comprised eight held writes/flushes, four torn writes and
eight EIO replies. Torn writes persisted exactly 512 bytes; the cold-image hashes
matched the expected mixture and differed from both the old and full requested
images. Every EIO had a real NBD error reply and a guest kernel I/O-error record,
with canaries armed before delivery and later I/O held until power loss. All
targeted voters recovered through ordinary startup and completed full membership.

Two additional `--fault-disks --scenarios vm-power` campaigns passed majority and
all-voter cold cuts after confirmed consumer state, preserving 15 confirmed ACKs
with three voters and 25 with five. These used a 45-second fault phase and a
240-second final audit window. Every target's volatile device cache was discarded,
including all devices during each all-voter cut.

Across all twelve histories, 452 attempted cycles recorded 1,127 confirmed ACK
responses and 1,154 acknowledged publications. All 48 public-address audits and
twelve final membership/durable-position inspections passed. Each final cluster
also had a nonzero compacted snapshot index. The 37 grouped cuts, including twelve
preflights, produced 93 successful cold-boot canary checks. All preserved fsynced
bytes, lost unsynced overwrites and observed new kernel IDs. The largest measured
SIGKILL dispatch skew was 0.070 ms. Replay of all twelve histories passed the
consumer checker and fault/recovery proof checks.

The control suite passed fifteen Python tests and twelve Clojure tests with 108
assertions. Three-voter LazyFS and direct-ext4 queue baselines also passed, and both
earlier LazyFS consumer campaigns passed replay. No server changes were required;
the tested binary SHA-256 matches the earlier campaigns above. The VM/runtime
versions and physical-host scope are unchanged.

The implementation is `b799e99`, with admitted-quorum/failed-match controls in
`be7d145`, read-only-remount-safe EIO controls in `96cdddd`, and explicit volatile
device selection in `c856f27`. Bring-up demonstrated that Linux can remount an
EIO-affected filesystem read-only before a later canary write. Those attempts were
rejected; the checked runs use the canary-first delivery gate described above.

Full active histories, proofs, replay reports, images and runtime metadata are in
the ignored `results/jepsen-inflight-three-final/` and
`results/jepsen-inflight-five-final/` directories. The simultaneous volatile-device
cross-checks are in `results/jepsen-inflight-three-device-power/` and
`results/jepsen-inflight-five-device-power/`. The aggregate proof audit is
`results/jepsen-inflight-verification.json`. Compatibility artifacts are in
`results/jepsen-inflight-lazyfs-compatibility/` and
`results/jepsen-inflight-native-compatibility/`.

## Checked mixed-network runs

The three- and five-voter matrices each passed all six combinations of directed
network loss and disk damage. Each used one client per voter, seeded consumer
fixtures, at most two additional main cycles per client, a 30-second main-client
scheduling window and a 300-second final recovery/audit window. The composite
fault and its conditional-write witness ran to completion before final recovery
cycles began. Runtime versions and independent-VM hardware match the native runs
above; every voter used a volatile NBD device.

| Scenario | Three-voter confirmed consumer ACKs | Five-voter confirmed consumer ACKs |
| --- | ---: | ---: |
| `vm-split-bitflip` | 41 | 65 |
| `vm-split-wipe` | 41 | 71 |
| `vm-send-bitflip` | 40 | 66 |
| `vm-send-wipe` | 36 | 65 |
| `vm-receive-bitflip` | 41 | 71 |
| `vm-receive-wipe` | 41 | 66 |

Across 192 attempted consumer cycles, all 644 confirmed consumer ACKs and 648
acknowledged workload publications passed their durability and lifecycle checks.
Every public address completed a fresh recovery lifecycle and its consumer audit:
48 successful recovery cycles and 48 audits, with no ACK-floor rollback, retention,
deduplication, recreation or stale-ACK violations.

The independent witness recorded 194 concurrent conditional-write rounds and 173
confirmed conditional publications, with no conflicting confirmations or confirmed
sequence forks. Its 169 successful local-service reads preserved the required
committed prefix. All 208 minority catalog probes remained unconfirmed, including
probes against erased and restarted stores. Every fault phase had at least two
successful quorum write rounds and two successful local-service read rounds.
All 48 post-heal witness audits returned every confirmed payload through each
public address.

The 36 real UDP controls checked 468 directed source/destination pairs, proving
both blocked and allowed links before the cut, after partitioned reboots and after
healing. The 24 grouped cuts, including twelve preflights, produced 66 successful
cold-boot canary checks. All preserved fsynced bytes, lost unsynced overwrites and
observed new kernel IDs; surviving-quorum kernel IDs stayed unchanged. The maximum
SIGKILL dispatch skew was below 0.06 ms. All eighteen damaged voter instances returned
as different fully admitted members through ordinary startup, with matching final
durable positions and no outstanding joint replacement.

All twelve histories passed replay against the final consumer, network-control,
conditional-write/read and recovery oracles. The control suite passed twenty
Python tests and sixteen Clojure tests with 144 assertions. Both three-voter queue
baselines (LazyFS and direct native disks) passed, and the earlier three/five-voter
active-I/O histories also passed replay. No production server changes were needed;
the binary SHA-256 matches the earlier campaigns.

The mixed implementation is `800565d`. The shared wire-client EOF/resource fix is
`738918a`; malformed greetings remain protocol failures. Final per-phase read
coverage and recovered-leader checks are in `efe6e77`. Full histories, proofs,
replay reports, images and runtime metadata are retained in the ignored
`results/jepsen-mixed-three-final/` and `results/jepsen-mixed-five-final/` directories.
The aggregate proof audit is `results/jepsen-mixed-verification.json`; compatibility
artifacts are in `results/jepsen-mixed-native-compatibility/` and
`results/jepsen-mixed-lazyfs-compatibility/`. Physical-host/controller failures and
arbitrary network schedules remain outside these bounded histories.
