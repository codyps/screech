# Cluster benchmark target qualification

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
