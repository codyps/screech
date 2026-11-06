# Cluster benchmark target qualification

Qualification requires one complete R3/R5 campaign to pass all 28 cases: at least 1.200× NATS throughput and at most 0.800× NATS end-to-end p99, using the existing workload boundaries. The campaigns recorded here use five checked trials per case. Passing cases from separate campaigns are never combined.

The [benchmark dashboard](https://codyps.github.io/screech/) identifies its measured revision and CI run. Check that run's `cluster-targets.json` and retained raw trials for its qualification result. A green main CI run alone does not certify clustered performance because its clustered target report is informational. The historical reports below retain both successful and unsuccessful campaigns; they do not stand in for a new revision's measurement.

## Historical qualifying campaign

[Campaign 42139057786](https://github.com/codyps/screech/actions/runs/42139057786) qualifies all 28 three- and five-voter cases against the 20% target. Each case uses five checked trials of 3,000 publications, plus warmups, with the existing throughput and end-to-end latency boundaries. The checker reports `goal_verified: true`; recomputing the report from the retained raw trials gives the same result.

The lowest throughput ratio is 1.241× NATS (24.1% higher); the highest p99 ratio is 0.746× NATS (25.4% lower). Ratios below compare medians of each server’s checked trials. Higher throughput and lower p99 are better.

| Workload | Bytes | R3 throughput | R3 p99 | R5 throughput | R5 p99 |
| --- | ---: | ---: | ---: | ---: | ---: |
| core-stream | 128 | 1.721× | 0.492× | 1.478× | 0.658× |
| durable | 128 | 1.739× | 0.487× | 1.514× | 0.665× |
| consume | 128 | 1.867× | 0.455× | 1.795× | 0.510× |
| consume-limits | 128 | 1.481× | 0.670× | 1.440× | 0.656× |
| consume-one | 128 | 1.974× | 0.561× | 1.809× | 0.561× |
| consume-one | 1024 | 2.002× | 0.566× | 1.896× | 0.572× |
| consume-next | 128 | 1.704× | 0.487× | 1.639× | 0.668× |
| consume-parallel | 128 | 1.772× | 0.507× | 1.833× | 0.439× |
| consume-ack-window | 128 | 1.678× | 0.602× | 1.753× | 0.452× |
| dedup | 128 | 1.481× | 0.542× | 1.241× | 0.724× |
| durable-parallel | 128 | 1.718× | 0.665× | 1.270× | 0.746× |
| consume-one-parallel | 128 | 1.848× | 0.560× | 1.944× | 0.547× |
| mixed-parallel | 128 | 1.936× | 0.537× | 2.002× | 0.496× |
| mixed-streams | 128 | 2.999× | 0.239× | 3.373× | 0.223× |

Core-on-stream uses durable publication ACK p99. Fetch(1), listed as `consume-one`, uses its complete Fetch/confirmed-ACK cycle p99. All other boundaries match the existing target checker; live Core callback timing and resource usage do not substitute for these metrics.

The measured Screech source is `d40c8cf` (`codex/cluster-coalesced-completion-bench`). NATS is the unmodified upstream commit `8397827bfdcfa742057590f154c27b97363539d0`, with file streams and consumers requesting matching R3/R5 replication and `sync_interval: always` on every voter. The official benchmark driver is unchanged.

The runner has four logical CPUs on an AMD EPYC 7763, Linux `7.0.0-1017-azure`, and a shared filesystem. Each voter runs in its own process with two server workers; voters communicate over loopback TCP. These results measure replication overhead on one host. They do not measure cross-machine latency or independent storage devices. Shared-runner hardware and scheduling affect results; this qualification describes this complete campaign.

The changes send grouped appends promptly to every voter so the first durable majority can complete, drive eligible duplicate-read proofs in the existing request completion task, and coalesce background voter commit notifications with their accepted suffix for up to two milliseconds. Read contacts and other urgent traffic release that bounded wait without reordering frames. Cancelling a duplicate-read caller retains its admission lease until its proof finishes. Every new duplicate request still requires its own fresh Raft ReadIndex observation; synced accepted bytes, voting promises, checkpoint roots, storage failure checks and service epoch fencing retain their existing boundaries. Standalone and strict single-voter stores retain their existing worker paths.

[The grouped-fanout campaign](https://github.com/codyps/screech/actions/runs/42134441962) passed 27 of 28 comparisons, missing R5 duplicate throughput. [The scheduling-only campaign](https://github.com/codyps/screech/actions/runs/42138333410) passed 16 of 28 on an AMD EPYC 9V74 runner. Their raw results are retained. Different hosts and runs prevent attributing the entire difference to one change. Passing cases from these campaigns are not combined with this qualification.

The workflow retains raw trials, environment metadata, binary hashes, launch configurations and the complete `cluster-targets.json` report in `cluster-benchmark-timings`. CPU/syscall profiles are separate diagnostics and do not count toward qualification. Crash safety is checked separately by the Rust, process, independent-VM and LazyFS suites described in [testing](testing.md).

## Subsequent measurements and checkpoint correction

[Main campaign 42151468327](https://github.com/codyps/screech/actions/runs/42151468327), source `07893ea`, passed throughput in all 28 cases but passed both throughput and p99 in only 13. It used five checked trials of 3,000 publications per case and the same upstream NATS commit. Its correctness, process-failure, guest power-loss, benchmark and dashboard deployment jobs all passed. These CI results do not turn its missed latency targets into a qualification. The published dashboard retains that complete campaign.

A reproduced checkpoint bookkeeping defect was then corrected in `ec678e8`: SQLite PASSIVE checkpoints can return success while an existing reader prevents complete backfill. The background worker previously counted this partial pass as completed work, so it could abandon the remaining frames after the reader released if no further writes arrived. The worker now keeps that work pending until the reported frame and backfill counts match. The regression test fails against the old implementation and passes after the correction, without issuing any additional writes after reader release. Checkpoint polling, row and WAL budgets, synchronous journal acceptance, FULL roots and quorum rules are unchanged.

[Checkpoint correction campaign 42153796720](https://github.com/codyps/screech/actions/runs/42153796720) passed throughput in all 28 cases and passed both targets in 27. R5 concurrent publishing missed p99: 0.817× NATS against the required 0.800×. It used five trials of 3,000 publications and the same NATS revision. Both campaigns reported an AMD EPYC 7763 with four logical CPUs, but ran on separate shared runners; the difference is not a controlled measurement of the checkpoint fix's effect. These incomplete campaigns are retained separately and their passing cases are not pooled.

The checkpoint correction also passed 257 Rust tests, Clippy with default and all features, 86 process failure cases, the 8,200-publication automatic-checkpoint/restart/repair case, seven independent-VM scenarios at each voter count, and three LazyFS scenarios. The fault campaigns checked confirmed ACKs, retention, deduplication and consumer recreation through power loss, torn writes, write/flush errors, asymmetric partitions and damaged-voter replacement. These safety checks are independent of performance qualification.

## Complete five-trial campaign comparison

Each row below is a whole R3/R5 campaign using the same 3,000-publication workloads and pinned NATS revision. The throughput column is the lowest ratio among its 28 cases; p99 is the highest ratio. A campaign qualifies only if all 28 cases pass both targets. Different shared runners prevent treating these rows as a controlled estimate of any individual change.

| Campaign | R5 runner CPU | Cases passing both | Lowest throughput | Highest p99 | Change measured |
| --- | --- | ---: | ---: | ---: | --- |
| [42139057786](https://github.com/codyps/screech/actions/runs/42139057786) | EPYC 7763 | 28/28 | 1.241× | 0.746× | Grouped fanout and completion scheduling |
| [42141817751](https://github.com/codyps/screech/actions/runs/42141817751) | EPYC 9V45 | 25/28 | 1.255× | 2.577× | Same runtime, main repeat |
| [42146585037](https://github.com/codyps/screech/actions/runs/42146585037) | EPYC 7763 | 26/28 | 1.237× | 1.028× | 10 ms checkpoint polling experiment |
| [42147606381](https://github.com/codyps/screech/actions/runs/42147606381) | EPYC 9V74 | 12/28 | 0.944× | 4.220× | full admission-window experiment |
| [42150826564](https://github.com/codyps/screech/actions/runs/42150826564) | EPYC 9V74 | 20/28 | 1.264× | 1.398× | admitted-count batching experiment |
| [42151468327](https://github.com/codyps/screech/actions/runs/42151468327) | EPYC 7763 | 13/28 | 1.291× | 1.850× | Same runtime, main repeat |
| [42153796720](https://github.com/codyps/screech/actions/runs/42153796720) | EPYC 7763 | 27/28 | 1.232× | 0.817× | Complete partial checkpoints after reader release |
| [42156853421](https://github.com/codyps/screech/actions/runs/42156853421) | EPYC 9V45 | 23/28 | 1.245× | 2.258× | projection statement-reuse experiment |
| [42157079837](https://github.com/codyps/screech/actions/runs/42157079837) | EPYC 9V45 | 25/28 | 1.312× | 0.973× | Checkpoint completion on main |
| [42158575551](https://github.com/codyps/screech/actions/runs/42158575551) | EPYC 7763 | 26/28 | 1.244× | 0.830× | learned burst-admission experiment |
| [42159305374](https://github.com/codyps/screech/actions/runs/42159305374) | EPYC 9V74 | 18/28 | 1.242× | 1.881× | dirty-frame checkpoint-trigger experiment |
| [42161467251](https://github.com/codyps/screech/actions/runs/42161467251) | EPYC 7763 | 27/28 | 1.131× | 0.730× | learned admission with 10 ms checkpoint polling |
| [42159736231](https://github.com/codyps/screech/actions/runs/42159736231) | EPYC 9V74 | 16/28 | 0.784× | 5.412× | Unsafe polling; clustered path unchanged |
| [42164213943](https://github.com/codyps/screech/actions/runs/42164213943) | Xeon Platinum 8573C | 25/28 | 1.273× | 1.431× | Commit-aware read probes with learned admission and checkpoint pacing |
| [42166305893](https://github.com/codyps/screech/actions/runs/42166305893) | EPYC 9V74 | 11/28 | 1.162× | 2.805× | Integrated main; all CI jobs and standalone targets passed |
| [42172626951](https://github.com/codyps/screech/actions/runs/42172626951) | R3: EPYC 7763; R5: EPYC 9V45 | 25/28 | 1.193× | 2.254× | Isolated timing jobs; raw provenance independently verified; CI postprocessing failed |
| [42176137594](https://github.com/codyps/screech/actions/runs/42176137594) | EPYC 9V74 | 11/28 | 0.381× | 71.084× | Publication collection candidate; all CI jobs, standalone targets and local fault histories passed |
| [42179667716](https://github.com/codyps/screech/actions/runs/42179667716) | R3: EPYC 7763; R5: EPYC 9V45 | 27/28 | 1.363× | 1.662× | Transport queue/failure feedback; all CI, standalone targets and 20 local fault histories passed |
| [42190078293](https://github.com/codyps/screech/actions/runs/42190078293) | R3: EPYC 7763; R5: EPYC 9V74 | 22/28 | 1.088× | 3.518× | Commit-progress correction; all CI, standalone targets and 20 local fault histories passed |

The individual experiments retain their branches and raw campaign artifacts. Shorter or diagnostic runs are not combined with these five-trial results to fill missing targets.

Main campaign 42157079837 missed clustered p99 for R3 Fetch(1) with 1 KiB messages (0.824×), R3 concurrent publishing (0.917×), and R5 sequential publishing (0.973×). All clustered throughput targets passed. Its separate existing standalone unsafe-ACK gate passed 18 of 19 cases, missing only the 10,000-message limits-retention throughput target (1.078× against 1.100×); correctness and guest power-loss checks passed. The benchmark job failed on that gate and dashboard deployment was skipped. That standalone requirement is distinct from the clustered 20% target.

The combined learned-admission/checkpoint-pacing campaign 42161467251 passed every p99 target and 27 of 28 throughput targets. R3 deduplication throughput was 1.131× NATS against the required 1.200×. This is a complete campaign result, rather than a combination of passing cases from the two individual experiments.

The separate unsafe-polling candidate `3268c64`, measured in [campaign 42159736231](https://github.com/codyps/screech/actions/runs/42159736231), passed all 19 standalone unsafe-ACK targets using five checked trials, including the 10,000-message limits-retention case at 1.108× throughput and 0.835× p99. Its correctness and guest power-loss jobs passed. That runner reported an EPYC 9V74; the failed prior main repeat used an EPYC 9V45, so these results alone do not isolate the polling change's effect. Strict single-voter and clustered stores do not enter the changed unsafe polling path.

The integrated candidate combines learned admission, 10 ms checkpoint polling, commit-aware initial read probes, and the separately tested unsafe polling change. Read-probe selection prefers voters whose Raft progress already reports the leader's commit only when enough such voters exist. Each read still obtains fresh context-bound replies from a distinct current quorum; unknown progress uses the prior selection and the existing delayed probes. New tests reproduce the former choice of a commit-lagging voter and preserve fallback, rotation among equally caught-up voters, and rejection of old read contexts. The combined source must be measured as a whole before claiming qualification.

The commit-aware read-probe campaign 42164213943 passed all throughput targets, including R3/R5 deduplication at 1.487× and 1.421×. It passed both targets in 25 of 28 cases. Concurrent-publish p99 missed at R3 (0.874×) and R5 (1.431×), as did R3 concurrent Fetch(1) p99 (0.837×). Its Xeon Platinum 8573C runner differs from the prior EPYC runners; these separate campaigns do not establish a controlled effect size for read selection.


Integrated main `faed73b` in campaign 42166305893 passed the existing 19 standalone
unsafe-ACK targets and all correctness, guest recovery and publication jobs. Its
complete five-trial clustered result passed both targets in only 11 of 28 cases;
R5 deduplication throughput was 1.162× and R3 concurrent-publish p99 was 2.805×.
A green CI result does not certify the informational clustered target report.

Subsequent CI campaigns isolate standalone, R3 and R5 timing onto separate fresh
runners, using the identical three binaries from one manifest-verified build. Each
Screech/NATS pair still shares one runner. This changes measurement conditions,
not the server implementation or target. Prior suite activity and storage history
are possible contributors to variability; the previous runs do not establish a
causal effect. Hardware remains unpinned and is reported for each campaign. Trial
counts, publication counts, workload checks, durability settings, latency boundaries
and the all-28 qualification requirement are unchanged. Dashboard assembly rejects
mixed builds, workflow IDs or measurement attempts; no passing cases are pooled
across campaigns to claim qualification.


The first complete isolated timing set, campaign 42172626951 at `8432fb7`, passed
both targets in 25 of 28 clustered cases using five checked trials. R3 concurrent
publishing missed p99 at 0.809×; R5 concurrent publishing missed throughput at
1.193× and p99 at 2.254×; R5 concurrent Fetch/ACK missed p99 at 0.911×. The other
25 cases passed both targets. R3 ran on an EPYC 7763 and R5 on an EPYC 9V45, with
Screech/NATS pairs sharing their respective hosts. These separate machines and
changed measurement conditions prevent attributing the difference from earlier
campaigns solely to isolation.

Both raw clustered files independently match the shared build manifest, source,
NATS pin, workflow ID and measurement attempt. Their CI jobs failed after timing
because the provenance CLI's positional arguments following options were rejected
by the runner's Python version. Explicit intermixed argument parsing fixes that
failure, with a CLI regression checked against Python 3.12.3. The timing results
remain retained and are not a successful CI or performance qualification.


The isolated campaign's standalone unsafe-ACK comparison independently passed 18
of 19 targets. Sequential publication throughput was 1.094× against the existing
1.100× requirement; its p99 was 0.809×. This gate is still required for publication.

Focused diagnostic run [42173951057](https://github.com/codyps/screech/actions/runs/42173951057)
profiled both concurrent workloads at R5 with 9,000 checked publications. The
publisher profile recorded about 1,117 accepted-journal syncs per voter, or eight
publications per sync despite 16 clients. A 256 microsecond collection experiment
improved publisher throughput in both orders of a local five-trial comparison,
but extending consumer collection also regressed Fetch/ACK p99 in the reversed
comparison. That broader policy was rejected. The candidate extends only active
clustered publication collection from 64 to 256 microseconds. Consumer collection
keeps 64 microseconds; sequential traffic never waits for a second request, and
standalone collection keeps its zero wait. Count/byte bounds and all durability
fences are unchanged.

The local comparisons used five checked trials of 3,000 publications for each of
the two R5 workloads, on one Ryzen 9955HX host. Publisher throughput improved by
13% in the first order and 5% in the reverse order. NATS results and natural leader
placement also varied, so these diagnostics do not establish a controlled effect
size or qualify the complete clustered target. Fresh complete CI measurements are
required for the final publication-only policy.


Complete five-trial campaign 42176137594 at `20c9023` passed all 19 standalone
targets and all CI jobs, and published its results. Its clustered comparison
passed only 11 of 28 cases. R3 concurrent publishing had throughput 0.381× and p99
71.084× NATS; three of five trials had p99 near 180–188 ms, while their medians
remained near 0.82 ms. R5 also missed several targets. The previous local publisher
improvements did not establish qualification or predictable hosted performance.
All 20 local independent-VM/LazyFS histories passed at this source.

Investigating the stalls exposed two reproducible transport defects: each peer's
output queue held only four messages although Raft permits eight appends in flight,
and dispatch ignored rejected enqueues. A controlled real-socket test reproduces
dropping a permitted replication burst before the writer runs. Two actual Raft
state tests reproduce append progress remaining in replication and snapshot
progress remaining paused after their sends were rejected. All three fail against
the old implementation and pass after the correction.

The corrected bounded queue derives its capacity from the replication window plus
four control slots. Rejected sends report unreachability to Raft; rejected snapshots
also report transfer failure. Partial-frame ordering, bounded admission, synced
acceptance, distinct-voter quorum requirements, application projection fences and
standalone behavior retain their existing contracts. Fresh complete measurements
must determine the effect on the observed stalls and the overall target.


Complete five-trial campaign 42179667716 at `d2b4be2` passed every clustered
throughput target and both targets in 27 of 28 cases. R3 passed all 14 cases. Only
R5 concurrent-publish p99 missed, at 1.662× NATS; its throughput was 1.376×. All 19
standalone targets, all CI jobs and all 20 local VM/LazyFS histories passed, and
the page was published. The runners differed from the prior campaign, so the
complete results do not isolate a causal effect size for the transport fix.

The allocation trace in diagnostic run [42182193134](https://github.com/codyps/screech/actions/runs/42182193134)
records explicit journal growth alongside the ordinary sync calls. Several first
syncs after growth took 6–16 ms. Many other long syncs did not coincide with growth;
allocation is a contribution to investigate, rather than an explanation of every
stall. Diagnostic phases share a cluster and retain earlier phase history; they
are not performance qualification data.

The next candidate reserves new journal space in 8 MiB chunks instead of 1 MiB,
reducing repeated extent allocation and materialization during publication bursts.
This adds physical disk headroom at journal creation and rollover; it changes
neither logical log limits nor the on-disk format. Existing smaller journals remain
valid. Every acceptance still performs the same mandatory sync, and standalone
storage retains its existing path. The new regression forces the old reservation
to grow during a 3 MiB burst, then requires the candidate to replay every byte after
losing SQL projection state. Existing cursor/prefix-preservation tests still force
real growth beyond the configured reservation. Fresh complete timing and fault
campaigns must validate the tradeoff before claiming the target.


The journal-reservation campaign [42184075460](https://github.com/codyps/screech/actions/runs/42184075460),
source `3c03a72`, passed both targets in 27 of 28 clustered cases using five
checked trials. Every p99 target passed. Only R5 deduplication throughput missed,
at 1.176× NATS against 1.200×; its p99 was 0.722×. R3 passed all 14 cases, and
R5 concurrent publication passed at 1.210× throughput and 0.740× p99. R5 ran on
a Xeon Platinum 8370C, a different host from the preceding campaign, so these
results do not isolate the reservation change's effect. This incomplete result
is retained independently and does not qualify the target.

A subsequent read-contact regression test found that the socket fast path
returned commit index zero even though its existing validation required the
request's commit to be recorded locally. Ordinary Raft heartbeat replies include
the follower commit frontier; omitting that hint prevents the leader from
learning the fast responder's commit progress. The corrected fast response
advertises the validated request commit as a conservative lower bound. The
actual TCP test fails against the previous reply and passes for both R3 and R5
after the correction, including updating Raft's leader progress. Newer commits
still require the actor, and every read still needs distinct, fresh, context-bound
quorum responses. The change adds no storage operation or cached quorum proof.
Fresh complete timings are required to establish its performance effect.


Complete five-trial campaign [42190078293](https://github.com/codyps/screech/actions/runs/42190078293)
at `057af09` passed 22 of 28 clustered comparisons. R3 passed every p99 target
and missed only deduplication throughput (1.088× NATS). R5 missed sequential
publication p99, limits-retention throughput/p99, 128-byte Fetch(1) p99, concurrent
Fetch(1) p99 and multi-stream p99. All 19 existing standalone targets, all CI jobs,
and all 20 local VM/LazyFS histories passed, and the page was published. The
complete raw timing set independently matches one build, revision, pinned NATS
commit, workflow and measurement attempt. This is not a clustered qualification.

The preceding attempt [42188617441](https://github.com/codyps/screech/actions/runs/42188617441)
was incomplete: pinned NATS returned `no responders available for request` during
R3 limits retention. Its R5 data passed all 14 comparisons but is not combined
with the replacement campaign. Twenty separate local repetitions using that
exact pinned NATS executable and checked driver did not reproduce the failure.
No retry was added inside a checked workload.

A counterbalanced local experiment briefly polled for consensus replies after
new work instead of immediately parking the owner thread. Across both orders and
both voter counts, the changes were about one percent or less in throughput,
without a consistent latency improvement. That polling experiment was rejected.

The next read-contact candidate distinguishes durable acceptance from a follower's
commit projection. Raft's [linearizable-read rules](https://raft.github.io/raft.pdf)
require the leader to know its committed prefix and obtain fresh majority
contacts. The candidate lets a stable, authenticated voter confirm its already
recorded leader term when the requested prefix is durably accepted, even while
a commit notification awaits the actor. Its response reports only the lower of
the requested and locally recorded commit indices. The actor still processes
the notification through the ordinary commit pipeline. The leader's own durable
applied prefix and each read's distinct, context-bound quorum proof remain
required. Membership transitions, unaccepted prefixes, changed terms and failed
storage retain their existing fallback or fencing behavior.

The new regression fails against the old commit guard. With the change, actual
R3/R5 Raft leaders complete fresh reads while followers retain older commit
cursors; after discarding volatile state and isolating the former leader, the
survivors elect a leader and recover the exact publication. Real TCP tests hold
the projection gate and verify that a lagging commit never appears in the reply
as already recorded. Fresh complete timings and failure histories are required
for this candidate before claiming a performance result.
