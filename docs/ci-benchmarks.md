# CI and the benchmark dashboard

[Live dashboard](https://codyps.github.io/screech/) · [CI runs](https://github.com/codyps/screech/actions/workflows/ci.yml)

CI runs on pushes to main, pull requests, manual dispatch and Mondays at 04:17 UTC.
The correctness job runs formatting, default/all-feature Clippy, Rust tests, extended
projection/group/ACK-floor properties, dashboard reducer/generator tests and process faults, including waits inside actual data-sync syscalls.
The reusable guest workflow kills and recovers a disposable VM three times, including
forced checkpoints and concurrent commits. Each test artifact is downloadable from
its workflow run. A manual guest-only run remains available in vm.yml.

After correctness passes, separate runners execute guest recovery and benchmarks.
Benchmark jobs build release Screech without fault hooks, the official nats.go driver,
and NATS from main (or a manually supplied ref). The resolved NATS SHA, binary hashes,
versions, CPU/kernel/filesystem, trial counts and commands are preserved with raw JSON.
Tests and VMs run separately; profiling starts after all benchmark timing finishes.
Hosted machines are shared and
unpinned; cross-run differences can reflect hardware and filesystem variation.

The default campaign runs three checked trials per case, plus warmups. It covers
Core fanout/wildcards/queues/request-reply, live Core on a stream, sequential and
concurrent durable publishers, pull consumers, ACK-and-next, dedup, retention and
mixed workloads, including a 1000-message outstanding ACK window. A second campaign runs four concurrent cases at 20,000 messages and a 10,000-message limits-retention consumer case.
Separate three- and five-voter campaigns run all 14 JetStream cases at 3,000
publications per trial against both servers, covering repeated checkpoint and WAL
reuse cycles. There are 513 measured trials at the
default settings: 270 standard, 75 larger and 168 clustered.
A manual CI run can set trials (1–10) and nats_ref (branch, tag or SHA).
The manual `Cluster benchmark targets` workflow runs just the two clustered
campaigns on a fresh runner and uploads their raw trials and target report; its
`messages` input controls publications per trial. It
requires at least 90% of NATS always throughput and at most 110% of its p99 for
every case at both voter counts, using medians of at least three matched checked
trials. Core-on-stream uses durable ACK p99; Fetch(1) uses the full Fetch/ACK cycle
p99. One-trial runs cannot certify the target. Main CI also preserves this report
with the published benchmark artifacts.
The focused workflow also captures CPU stacks and disk/syscall summaries across
all five voters, including per-file sync latency distributions. Separate publication-phase gates exclude setup and payload
validation; these diagnostic timings never count toward the performance target.

## Storage modes

| Series | Configuration | Interpretation |
| --- | --- | --- |
| Screech · FULL | SQLite WAL, synchronous FULL | Durable publish ACKs, reservations and confirmed consumer ACKs follow commit |
| Screech · UNSAFE ACK durability | --unsafe-ack-durability | Consumer state may roll back after failure; publication ACKs remain FULL |
| NATS · always | sync_interval: always | Stream writes are synced; standalone consumer state can still flush asynchronously |
| NATS · 10 ms | sync_interval: 10ms | Stream syncing is deferred between intervals |
| NATS · default 2m | sync_interval omitted | NATS' documented two-minute default; this is not an equal power-loss contract |
| Screech · 3/5 voters · FULL | Ordinary startup, synced accepted journals and FULL checkpoint roots | Committed publication, reservation and consumer ACK state follows a durable quorum |
| NATS · 3/5 replicas · always | JetStream cluster, sync_interval: always on every server | File streams and durable consumers explicitly request matching R3/R5 replication; persistence contracts differ from Screech |

Core cases use no stream storage; their labels say Core. Additional intervals can
be added to the runner with repeatable --nats-sync-interval values. These are NATS
settings. Unsafe standalone Screech is the performance target; strict-default and
clustered results are reported separately. The standalone target requires every
JetStream case to reach at least 1.10x throughput and at most 0.90x p99 versus NATS always, across three or more checked trials. Upstream configuration
and behavior can evolve, so each campaign records the exact NATS commit.

## Publishing and reading results

A successful main run publishes the static dashboard to GitHub Pages after both
recovery and benchmark jobs pass. PRs upload the same generated site as a downloadable
benchmark-site artifact without deploying it. Failed campaigns do not replace the
last successful page. Benchmark-results retains raw JSON/JSONL and launch metadata;
benchmark-site contains a self-contained page and raw downloads for that run.

Use Campaign to select standard, larger, three-voter or five-voter workloads.
Use Metric for throughput, latency percentiles, live-Core durable ACK latency, CPU
or final RSS. Comparison badges compare Screech with the selected server within the
same case. Checkboxes show/hide sync modes; Table exposes medians, ranges and counts.
Search matches case names, payload size and publication count. Campaign, metric,
baseline, search and view are encoded in the URL for sharing. Series visibility is
local to the current view. Bar scales are per case; raw numbers remain visible.

Sequential consumer latency samples time ACK confirmation only; Fetch is included
in overall throughput but excluded from those latency samples. Separate Fetch(1)
also reports fetch_p50/p95/p99_us (request to delivery) and cycle_p50/p95/p99_us
(request through confirmed ACK, including client validation). The dashboard exposes
Fetch and cycle p50 separately; cycle percentiles are measured directly rather than
computed by adding Fetch and ACK percentiles. Concurrent consumer
samples cover Fetch(1) through confirmed ACK. Core-on-stream callback and durable
ACK latency are separate metrics. Missing samples remain absent. Reported percentile
values are medians of per-trial percentiles, not pooled sample percentiles. CPU deltas
include setup, prefill, validation and teardown; throughput excludes those phases.
RSS is final, not peak. Cluster CPU and RSS sum all voter processes; unavailable
process counters remain missing. NATS modes differ in durability and are not interchangeable.

Cluster voters are separate processes on the same runner, with separate store
directories on the same filesystem and loopback TCP between voters. These campaigns
measure replication overhead on one host, not cross-host latency or independent
storage devices. Each voter gets two Tokio workers / GOMAXPROCS=2, so total available
server concurrency grows with voter count. The driver uses the first voter's public
client endpoint, disables discovery/reconnect, and preserves Screech's ordinary
gateway routing and NATS' ordinary stream/consumer leader routing. No fault hooks
or failures run during timing. These performance trials do not establish crash safety;
the separate fault campaigns cover durability and recovery.

Each campaign checks cluster readiness before warmups. Every workload verifies the
returned stream and consumer replica counts, in addition to its existing payload,
sequence, retention and ACK checks. All voter commands/configurations and logs are
retained. Cluster mode rejects unsafe ACK and deferred-sync options. Use
`--cluster-messages` to change the publication count or repeat `--scenario` for a
focused JetStream campaign; the actual counts remain part of each case's identity.
NATS commits consumer ACKs and stream retention through separate Raft groups. Final
clustered retention/ACK-floor checks allow up to five seconds for convergence,
outside throughput and ACK latency timing; every final invariant must still hold.
Standalone validation remains immediate. An ACK latency measurement does not imply
that a subsequent stream read will already reflect the retention deletion.

## Reproduce locally

```sh
cargo build --release --locked
mkdir -p bin results
# Build an explicitly selected upstream checkout and record its revision.
git clone https://github.com/nats-io/nats-server.git upstream
git -C upstream rev-parse HEAD > results/nats-commit.txt
go -C upstream build -o "$PWD/bin/nats-server" .
go -C tools/bench build -o "$PWD/bin/bench-driver" .
python3 scripts/benchmark.py --screech target/release/screech-server \
  --nats bin/nats-server --driver bin/bench-driver \
  --nats-commit "$(cat results/nats-commit.txt)" \
  --nats-default --nats-sync-interval 10ms --screech-unsafe-ack --trials 3 --output results/standard.json
# Repeat with --scale-only --output results/concurrent.json for larger cases.
for voters in 3 5; do
  python3 scripts/benchmark.py --screech target/release/screech-server \
    --nats bin/nats-server --driver bin/bench-driver \
    --nats-commit "$(cat results/nats-commit.txt)" \
    --cluster-voters "$voters" --cluster-messages 3000 --trials 3 --output "results/cluster-$voters.json"
done
python3 scripts/render_benchmarks.py \
  --input 'Local standard' results/standard.json \
  --input 'Local cluster · 3 voters' results/cluster-3.json \
  --input 'Local cluster · 5 voters' results/cluster-5.json --output _site
python3 -m http.server --directory _site 8000
```

The renderer uses Python's standard library and bundles vanilla HTML/CSS/JavaScript;
no third-party CDN or browser build dependencies are needed. Its reducer is tested
with node --test tests/benchmark_dashboard.test.cjs. Run Python tests with
python3 -m unittest discover -s tests -p 'test_*.py'.
Run driver contract checks with `go -C tools/bench test .`.

Core bulk sends use 1024-publication windows. The publisher flush follows routing;
subscriber flushes follow their queued deliveries before the next window starts.
This keeps the workload within bounded pending queues, applies identical pacing to
both servers and is included in throughput timing. Raw rows record core_window.
Driver errors and per-server logs are preserved in the benchmark-results artifact.

## Core subject and subscription scaling

Core cases run with JetStream disabled. In addition to the single-subject fanout,
wildcard and queue cases, CI includes these 128-byte, one-subscriber cases with
1,000,000 publications per trial:

| Case | Rotating subjects | Nonmatching wildcard subscriptions |
| --- | ---: | ---: |
| Subject churn | 8,192 | 0 |
| Many subscriptions | 1 | 512 |
| Subject churn and many subscriptions | 8,192 | 512 |

Each subscriber also has one active wildcard subscription. Subjects rotate in
publication order; the unused filters have a separate literal prefix. This separates
cache-hit behavior from lookup work when the subject set exceeds route-cache capacity.
Untimed control publications first verify every unused subscription receives its own
probe exactly once. Raw rows record core_subjects, unused_wildcards,
verified_unused_wildcards and core_window. The dashboard shows
these counts and supports searching for them. Core modes carry no storage-sync label.

Every delivery is checked for subject, sequence, length and ordering. Queue cases
also check exactly-once selection across subscribers. Subscriber connections drain
after timing ends so late callbacks and unexpected deliveries to unused subscriptions
cannot escape validation. Subscription setup and drain are outside throughput timing;
CPU deltas include them, and RSS is final rather than peak.
