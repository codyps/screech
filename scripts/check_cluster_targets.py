#!/usr/bin/env python3
"""Require matched R3/R5 trials within 10% of NATS always throughput and p99."""
import argparse
import json
from pathlib import Path

from benchmark import JETSTREAM_CASES
from check_jetstream_targets import compare
from render_benchmarks import read_campaign


def compare_cluster(data, voters, minimum_trials=3):
    if data['environment'].get('voters') != voters:
        raise ValueError('wrong cluster topology')
    counts = {r['publishes'] for r in data['rows']}
    if len(counts) != 1:
        raise ValueError('cluster campaign must use one matched publication count')
    count = counts.pop()
    server, baseline = f'screech-cluster-{voters}', f'nats-cluster-{voters}-always'
    for row in data['rows']:
        if row.get('voters') != voters or row.get('stream_replicas') != voters or row['server'] not in (server, baseline):
            raise ValueError('wrong cluster replication or durability series')
    expected = [(scenario, size, subs, count) for scenario, size, subs, _ in JETSTREAM_CASES]
    return [dict(voters=voters, **row) for row in compare(data, expected, server, minimum_trials,
        baseline=baseline, minimum_throughput_ratio=.9, maximum_p99_ratio=1.1)]


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--three', type=Path, required=True)
    parser.add_argument('--five', type=Path, required=True)
    parser.add_argument('--minimum-trials', type=int, choices=range(1, 11), default=3)
    parser.add_argument('--report-only', action='store_true')
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    results = []
    for voters, path in ((3, args.three), (5, args.five)):
        results.extend(compare_cluster(read_campaign(path, f'R{voters}'), voters, args.minimum_trials))
    report = dict(minimum_throughput_ratio=.9, maximum_p99_ratio=1.1,
        minimum_trials=args.minimum_trials, all_passed=all(row['passed'] for row in results),
        goal_verified=all(row['passed'] and row['trials'] >= 3 for row in results), cases=results)
    args.output.write_text(json.dumps(report, indent=2)+'\n')
    for row in results:
        print(f"R{row['voters']} {row['scenario']} {row['payload_bytes']} B: throughput {row['throughput_ratio']:.3f}x, {row['latency_metric']} {row['p99_ratio']:.3f}x {'PASS' if row['passed'] else 'MISS'}")
    return 0 if report['goal_verified'] or args.report_only else 1


if __name__ == '__main__':
    raise SystemExit(main())
