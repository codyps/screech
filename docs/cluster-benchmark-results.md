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
