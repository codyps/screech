#!/usr/bin/env python3
"""Retain content larger than the server RSS budget, then SIGKILL and read it back.
Linux /proc measures process RSS, excluding the kernel's reclaimable file cache.
"""
import argparse
import json
from pathlib import Path
import sqlite3
import tempfile
import time
from crash_tests import Server
from wire import Client


def rss_bytes(server):
    status = Path(f'/proc/{server.process.pid}/status').read_text()
    return int(next(line.split()[1] for line in status.splitlines() if line.startswith('VmRSS:'))) * 1024


def run(binary, content_mib=256, budget_mib=96):
    size = 256 * 1024
    count = content_mib * 1024 * 1024 // size
    assert count >= 16 and content_mib > budget_mib
    budget = budget_mib * 1024 * 1024
    peak = 0
    def sample(server):
        nonlocal peak
        peak = max(peak, rss_bytes(server))
        assert peak < budget, f'server RSS {peak} exceeds {budget}; retained bytes {count * size}'
    def body(i):
        # A distinct body for every message; no shared allocation can hide retention.
        return i.to_bytes(8, 'little') + bytes([i % 251]) * (size - 8)
    with tempfile.TemporaryDirectory(prefix='screech-content-memory-') as tmp:
        directory = Path(tmp)
        server = Server(binary, directory)
        started = time.monotonic()
        try:
            c = Client(server.address, timeout=30)
            c.create()
            c.create_consumer(ack_wait=1)
            for i in range(count):
                assert c.publish(body(i)) == i + 1
                sample(server)
            assert c.request('$JS.API.SCREECH.CHECKPOINT', {}).get('success')
            sample(server)
            # Leave one reservation unacknowledged across SIGKILL.
            message = c.pull()
            assert message['payload'] == body(0)
            c.close()
        finally:
            server.kill()
        publish_seconds = time.monotonic() - started
        server = Server(binary, directory)
        try:
            c = Client(server.address, timeout=30)
            sample(server)
            for i in [0, count // 4, count // 2, count - 1]:
                assert c.get(i + 1) == body(i)
                sample(server)
            message = c.pull()
            assert message['payload'] == body(0)
            c.ack(message)
            # Checkpoint after a durable ACK, then ensure its floor survives another kill.
            assert c.request('$JS.API.SCREECH.CHECKPOINT', {}).get('success')
            c.close()
        finally:
            server.kill()
        server = Server(binary, directory)
        try:
            c = Client(server.address, timeout=30)
            assert c.request('$JS.API.CONSUMER.INFO.T.D', {})['ack_floor']['consumer_seq'] >= 1
            assert c.request('$JS.API.STREAM.PURGE.T', {}).get('success')
            assert c.request('$JS.API.SCREECH.CHECKPOINT', {}).get('success')
            sample(server)
            c.close()
        finally:
            server.kill()
        with sqlite3.connect(directory / 'data.db') as conn:
            assert conn.execute('SELECT count(*) FROM retained_content').fetchone()[0] == 0
    return {'passed': True, 'retained_bytes': count * size, 'messages': count,
            'rss_budget_bytes': budget, 'peak_sampled_rss_bytes': peak,
            'publish_seconds': publish_seconds, 'sigkill_restarts': 2,
            'content_reads_redelivery_ack_and_gc_verified': True}


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('binary')
    parser.add_argument('--content-mib', type=int, default=256)
    parser.add_argument('--budget-mib', type=int, default=96)
    parser.add_argument('--output', type=Path)
    args = parser.parse_args()
    result = run(args.binary, args.content_mib, args.budget_mib)
    text = json.dumps(result, indent=2)
    print(text)
    if args.output:
        args.output.write_text(text + '\n')
