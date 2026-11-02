# Testing

The deterministic Core and durable models run without sockets or SQLite. Storage
is a separate Store interface; MemoryStore supports fast transition histories,
while SqliteStore tests exercise actual transactions and recovery. Fault hooks exist
only in builds with the fault-injection feature. Production builds ignore fault
control environment variables.

## Local checks

```sh
cargo fmt --all -- --check
cargo clippy --locked --all-targets --all-features -- -D warnings
cargo clippy --locked --all-targets -- -D warnings
cargo test --locked --all-features
python3 -m unittest discover -s tests -p 'test_*.py' -v
node --test tests/benchmark_dashboard.test.cjs
```

Property tests cover routing against a reference model, fragmented/binary framing,
stream/consumer transitions, retention, deduplication, ACK-and-next, atomic storage
and mixed group refinement. Branching projection histories mutate shared trees,
drop one projection and commit another, then checkpoint/recover. Committed state
and sibling projections must remain independent. Ambiguous Store errors exercise
both no-write and persisted-write outcomes; the engine must poison itself without
exposing speculative state.

CI uses 256 cases per property family and extends two targeted histories:

```sh
PROPTEST_CASES=1024 cargo test --locked --all-features --test durable_model tree_projections
PROPTEST_CASES=2048 cargo test --locked --all-features --test durable_model mixed_groups
```

## Process and I/O failures

```sh
cargo build --locked --features fault-injection
mkdir -p results
cc -shared -fPIC -O2 -Wall -Wextra -Werror \
  -o sqlite-io-fault.so scripts/sqlite_io_fault.c -ldl
python3 scripts/crash_tests.py target/debug/screech-server --output results/process-tests.json
python3 scripts/consumer_crash_tests.py target/debug/screech-server --output results/consumer-process-tests.json
python3 scripts/batch_crash_tests.py target/debug/screech-server --output results/batch-process-tests.json
python3 scripts/single_crash_tests.py target/debug/screech-server --output results/single-process-tests.json
python3 scripts/io_failure_tests.py target/debug/screech-server --shim ./sqlite-io-fault.so --output results/io-process-tests.json
python3 scripts/datasync_stall_tests.py target/debug/screech-server --shim ./sqlite-io-fault.so --output results/datasync-stall-tests.json
python3 scripts/multi_pull_crash_tests.py target/debug/screech-server --output results/multi-pull-tests.json
python3 scripts/group_crash_tests.py target/debug/screech-server --shim ./sqlite-io-fault.so --output results/group-process-tests.json
python3 scripts/core_stall_tests.py target/debug/screech-server --output results/core-stall-tests.json
python3 scripts/persistent_crash_tests.py target/debug/screech-server --output results/persistent-process-tests.json
```

These scripts start disposable servers/databases and reap their children. Marker
files establish deterministic schedules. Faults hang or exit at engine/SQLite commit,
reservation, confirmation, checkpoint and output boundaries; hung children are killed
with SIGKILL. Shutdown tests exercise SIGTERM while storage is wedged. A syscall shim
injects EIO before/after real sync, ENOSPC and short writes, including WAL reuse.
The shim is restricted to disposable test children and is not a production component.

Group gates enqueue known publication, Fetch and ACK requests before releasing the
worker. Tests check ordered whole-group recovery, read fences, pending state, retention,
deduplication and payloads. Populated-tree cases include 480 stored messages, 64 pending
ACKs and an untouched stream. Core stall cases require live PUB/HPUB and same/other-
publisher PING traffic to continue while durable replies remain blocked. Core-visible
uncommitted tails may disappear on restart; durable success must never be emitted early.

Assertions distinguish an unknown commit outcome from a rejected write. If storage
reports an ambiguous failure, restart may recover the whole committed transaction
or none of it; the running poisoned engine cannot acknowledge further mutation.
Optional --previous on persistent_crash_tests.py checks checkpoint compatibility with
another fault-injection binary.

## Guest VM recovery

The reusable vm.yml workflow installs a Linux kernel/modules, static BusyBox, QEMU,
mkfs.ext4 and modprobe, then runs three VM kills. It can also be dispatched alone.
To run locally, provide matching kernel/modules and tools:

```sh
python3 scripts/vm_crash_test.py \
  --binary target/debug/screech-server \
  --kernel /path/to/bzImage --modules /path/to/module-root \
  --busybox /path/to/static-busybox --qemu /path/to/qemu-system-x86_64 \
  --modprobe /path/to/modprobe --mkfs /path/to/mkfs.ext4 \
  --work-dir vm-work --concurrent --force-checkpoint --output results/vm-tests.json
```

Use --kernel-release when the module root contains multiple versions. The harness
boots an ext4 data disk under QEMU TCG with cache=none, publishes acknowledged data,
changes consumer/dedup state, forces checkpoints and verifies actual multi-record
commits. It SIGKILLs QEMU without guest shutdown/sync, boots the same disk and checks
payloads, ACK deletion, pending redelivery, ACK-and-next and deduplication. This loses
the guest kernel/page cache; host/controller power failure is not simulated. Host
storage must honor flushes. Logs and structured results are CI artifacts, not source
files committed to main.

## Benchmarks

See [CI benchmarks](ci-benchmarks.md) for scenarios, sync modes, timing boundaries,
reproduction and the generated [dashboard](https://codyps.github.io/screech/). Keep
benchmark runs separate from tests, profiles and VMs on the same host. Generated
results and profiles belong in artifacts or local ignored directories.

## Linux data-sync and ACK floors

Cargo enables SQLite's supported fdatasync path on Linux x86-64/AArch64 GNU and
musl targets. SQLite remains in WAL/FULL mode; reservations and confirmed ACKs
still require separate completed durable commits. Other targets retain their sync
defaults. The selected Linux syscall persists data and metadata required to retrieve
it, including file size; unrelated inode timestamps are not part of the contract.

The syscall interposer verifies fdatasync is actually used, injects errors before
and after real sync, and hangs it on both sides of reservation and ACK commits.
Tests require no early durable response, same-connection Core progress, committed
state recovery after successful commit sync and preservation of the remaining backlog.
After WAL restart, tests separately interrupt the header sync and commit sync. A
completed header sync does not imply the new transaction committed. An earlier
confirmed ACK must survive each interruption. WAL reuse is also tested with ACK
sync errors, ENOSPC and short writes; uncertain failures poison further mutation.
Process kills before sync may still recover OS-cached writes; they do not model
page-cache loss. Guest VM kills test the separate page-cache-loss boundary.

ACK-floor histories include first deliveries, redeliveries, NAK/progress/TERM,
exhaustion, checkpoint/reopen and varied ACK order. Floors are checked against
independent minima of the reservation history. Recovery rejects missing, duplicate
or inconsistent history entries before optimized index lookups can be used.

```sh
PROPTEST_CASES=1024 cargo test --locked --all-features --test durable_model ack_floors_refine
```

Larger pulls continue across bounded 64-message chunks without an idle sleep after
delivery progress. Between chunks the worker checks ingress and shutdown; no-progress
polls retain a bounded wait. The multi-pull crash schedule interrupts the third
reservation chunk, verifies its committed prefix and Core control traffic, then
checks all redelivery attempts and ACK/retention effects after restart. The fault-only
SCREECH_FAULT_AFTER setting chooses a specific occurrence of a selected boundary.

Group-extension schedules pause after the initial requests have been projected, then
queue later requests. Kills before/after commit check the exact durable prefix, with
separate cases for read fences, request-count bounds and input-byte bounds. Functional
cases verify the fenced read's state and later replies. Real sync stalls verify that
Core forwarding continues and that JetStream replies remain private until commit.

Checkpoint-budget tests cross the record threshold in individual and grouped
appends, restart just below it, and verify group overshoot and checkpoint replay.
Byte-budget tests restart with an uncheckpointed prefix and require its encoded
bytes to count toward the next automatic checkpoint.

Core wildcard routes and interest checks use an iterative subject-token index. The
scan-based model independently checks matching, exact-first direct delivery order,
queue cursors, queue-restricted routing, echo changes, duplicate SIDs, disconnects
and automatic unsubscribe through cached routes. Additional cases cross the route
cache limit, exercise Unicode and deep filters, and check index pruning/slot reuse.

```sh
PROPTEST_CASES=1024 cargo test --locked --all-features --test core_model token_index_refines
```

Core transport retains validated immutable PUB/HPUB frames through forwarding.
Framing tests compare owned and wire-backed messages against independent MSG/HMSG
encoding, including arbitrary binary bodies, Unicode fields, header stripping,
partial writes before/within/across vectors, interrupted/zero writes and backends
without vectored writes. Mixed control frames must remain ordered. Large-header,
empty-body and multi-vector batches check framing and lifetime boundaries.

A bounded duplex transport stalls after a partial write to verify that in-flight
bytes/counts remain charged until completion and that the existing write deadline
closes a stalled writer. Network tests fragment mixed PUB/HPUB inputs across read
boundaries and check fanout, queue delivery, replies and legacy subscribers.

```sh
PROPTEST_CASES=1024 cargo test --locked --all-features --lib partial_output_refines
```

CI publishes `core-profiles` separately from benchmark results. It runs checked
1 KiB Core forwarding workloads against both servers with a user-space CPU-clock
sample profile, keeping profiling overhead outside the benchmark measurements.
The artifact includes raw perf samples, symbol reports, CPU time, RSS and runner
CPU/kernel metadata. Download it from the CI run; it is not part of the website.

```sh
python3 scripts/profile_core.py --screech target/release/screech-server \
  --nats /path/to/nats-server --driver /path/to/bench-driver \
  --perf /path/to/perf --output /tmp/core-profiles
```

The parser boxes INFO/CONNECT data so hot publication results stay compact.
Handshake tests retain UTF-8 fields and check pipelined binary publications.
Routing tests warm exact/queue caches before changing echo, delivery limits,
subscriptions and disconnects, and compare every delivery with the scan model.

Core receiver notifications are deferred until the available input burst has
been routed (at most 256 commands), before any wait/yield. A drop guard wakes
accepted deliveries on early errors and EOF as well. Tests verify pending
byte/count limits during deferred notification and ordered 1 KiB delivery across
the command budget followed by publisher EOF or a malformed command.
