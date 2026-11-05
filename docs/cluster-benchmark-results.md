# Cluster benchmark target qualification

[Campaign 42115366217](https://github.com/codyps/screech/actions/runs/42115366217) qualifies all 28 three- and five-voter cases against the 20% target. Each case uses five checked trials of 3,000 publications, plus warmups, with the existing throughput and end-to-end latency boundaries. The checker reports `goal_verified: true`.

The lowest throughput ratio is 1.247× NATS (24.7% higher); the highest p99 ratio is 0.781× NATS (21.9% lower). Ratios below are medians of each server’s checked trials. Higher throughput and lower p99 are better.

| Workload | Bytes | R3 throughput | R3 p99 | R5 throughput | R5 p99 |
| --- | ---: | ---: | ---: | ---: | ---: |
| core-stream | 128 | 1.711× | 0.505× | 1.475× | 0.556× |
| durable | 128 | 1.737× | 0.480× | 1.487× | 0.520× |
| consume | 128 | 1.903× | 0.388× | 1.750× | 0.488× |
| consume-limits | 128 | 1.721× | 0.488× | 1.438× | 0.556× |
| consume-one | 128 | 2.010× | 0.568× | 1.740× | 0.609× |
| consume-one | 1024 | 1.869× | 0.571× | 1.740× | 0.614× |
| consume-next | 128 | 1.501× | 0.436× | 1.490× | 0.552× |
| consume-parallel | 128 | 1.800× | 0.478× | 1.859× | 0.425× |
| consume-ack-window | 128 | 1.755× | 0.483× | 1.707× | 0.546× |
| dedup | 128 | 1.460× | 0.550× | 1.247× | 0.612× |
| durable-parallel | 128 | 1.804× | 0.777× | 1.427× | 0.745× |
| consume-one-parallel | 128 | 1.826× | 0.781× | 1.980× | 0.523× |
| mixed-parallel | 128 | 1.954× | 0.642× | 2.120× | 0.486× |
| mixed-streams | 128 | 3.157× | 0.265× | 3.678× | 0.222× |

Core-on-stream uses durable publication ACK p99. Fetch(1), listed as `consume-one`, uses its complete Fetch/confirmed-ACK cycle p99. All other boundaries match the existing target checker; live Core callback timing and resource usage do not substitute for these metrics.

The measured Screech source is `b97f474` (`codex/cluster-async-duplicate-bench`). NATS is the unmodified upstream commit `8397827bfdcfa742057590f154c27b97363539d0`, with file streams and consumers requesting matching R3/R5 replication and `sync_interval: always` on every voter. The official benchmark driver is unchanged.

The runner has four logical CPUs on an AMD EPYC 7763, Linux `7.0.0-1017-azure`, and a shared filesystem. Each voter runs in its own process with two server workers; voters communicate over loopback TCP. These results measure replication overhead on one host. They do not measure cross-machine latency or independent storage devices. Shared-runner hardware and scheduling affect results; this qualification describes this complete campaign.

The change routes eligible duplicate publications from a committed immutable model to a fresh asynchronous Raft ReadIndex observation. It removes application-worker handoffs while retaining storage-owner failure checks, service epoch checks and a separate quorum observation for every newly arriving request. Mutations, expired metadata, queued commands and catalog changes retain the ordered worker path. Standalone and strict single-voter stores retain their existing worker path.

The workflow retains raw trials, environment metadata, binary hashes, launch configurations and the complete `cluster-targets.json` report in `cluster-benchmark-timings`. Its CPU/syscall profiles are separate diagnostics and do not count toward target qualification. Crash safety is checked separately by the Rust, process, independent-VM and LazyFS suites described in [testing](testing.md).
