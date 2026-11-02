#!/usr/bin/env python3
"""Verify every checked JetStream case against NATS sync_interval=always."""
import argparse
import json
import math
import statistics
from pathlib import Path
from benchmark import JETSTREAM_CASES, CONCURRENT_CASES


def compare(data, expected, server, minimum_trials=3):
    rows = data['rows']
    results = []
    for scenario, size, subscribers, count in expected:
        key = (scenario, size, subscribers, count)
        metric = {'core-stream':'durable_p99_us', 'consume-one':'cycle_p99_us'}.get(scenario, 'p99_us')
        series = {}
        trials = None
        for label in [server, 'nats-main']:
            matched = [r for r in rows if (r['scenario'],r['payload_bytes'],r['subscribers'],r['publishes'],r['server']) == (*key,label)]
            if len(matched) < minimum_trials:
                raise ValueError(f'{key} {label}: missing checked trials')
            ids = [r['trial'] for r in matched]
            if len(set(ids)) != len(ids) or set(ids) != set(range(len(ids))):
                raise ValueError(f'{key} {label}: duplicate or non-contiguous trial IDs')
            if trials is not None and set(ids) != trials:
                raise ValueError(f'{key}: mismatched trial IDs')
            trials = set(ids)
            for row in matched:
                if row.get('checked') is not True:
                    raise ValueError(f'{key} {label}: unchecked trial')
                for field in ['publishes_per_second', metric]:
                    value = row.get(field)
                    if isinstance(value, bool) or not isinstance(value, (int,float)) or not math.isfinite(value) or value <= 0:
                        raise ValueError(f'{key} {label}: invalid {field}')
            series[label] = {
                'throughput': statistics.median(r['publishes_per_second'] for r in matched),
                'p99_us': statistics.median(r[metric] for r in matched),
            }
        own, base = series[server], series['nats-main']
        throughput_ratio = own['throughput'] / base['throughput']
        p99_ratio = own['p99_us'] / base['p99_us']
        results.append({'scenario':scenario,'payload_bytes':size,'subscribers':subscribers,'messages':count,'trials':len(trials),
            'latency_metric':metric,'series':series,'throughput_ratio':throughput_ratio,'p99_ratio':p99_ratio,
            'passed':throughput_ratio >= 1.1 and p99_ratio <= 0.9})
    return results


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--standard',type=Path,required=True)
    p.add_argument('--concurrent',type=Path,required=True)
    p.add_argument('--server',choices=['screech','screech-unsafe-ack'],required=True)
    p.add_argument('--output',type=Path,required=True)
    p.add_argument('--minimum-trials',type=int,choices=range(1,11),default=3)
    p.add_argument('--report-only',action='store_true',help='Report target misses without failing; invalid or incomplete data still fails')
    args = p.parse_args()
    results = []
    for name,path,expected in [('standard',args.standard,JETSTREAM_CASES),('concurrent',args.concurrent,CONCURRENT_CASES)]:
        results.extend(dict(campaign=name,**r) for r in compare(json.loads(path.read_text()),expected,args.server,args.minimum_trials))
    report = {'server':args.server,'baseline':'nats-main (sync_interval: always)',
        'minimum_throughput_ratio':1.1,'maximum_p99_ratio':0.9,'minimum_trials':args.minimum_trials,'all_passed':all(r['passed'] for r in results),'goal_verified':all(r['passed'] and r['trials']>=3 for r in results),'cases':results}
    args.output.write_text(json.dumps(report,indent=2)+'\n')
    for r in results:
        print(f"{r['campaign']} {r['scenario']} {r['payload_bytes']} B: throughput {r['throughput_ratio']:.3f}x, p99 {r['p99_ratio']:.3f}x {'PASS' if r['passed'] else 'MISS'}")
    return 0 if report['goal_verified'] or args.report_only else 1


if __name__ == '__main__':
    raise SystemExit(main())
