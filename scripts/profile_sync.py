"""Summarize timestamped strace sync calls, including interleaved completions."""
import re


def summarize_sync(text):
    pending = {}
    durations = {}
    for line in text.splitlines():
        fields = line.split(None, 2)
        if len(fields) < 3 or not fields[0].isdigit():
            continue
        thread, _, call = fields
        if '<unfinished ...>' in call:
            pending[thread] = call
            continue
        if '<... ' in call and ' resumed>' in call:
            call = pending.pop(thread, '') + call
        elapsed = re.search(r'<([0-9.]+)>$', call)
        path = re.search(r'\(\d+<([^>]+)>', call)
        if not elapsed or not path or not re.search(r'\b(?:fsync|fdatasync)\(', call):
            continue
        durations.setdefault(path[1], []).append(float(elapsed[1]) * 1000)
    result = {}
    for path, values in sorted(durations.items()):
        values.sort()
        percentile = lambda p: values[min(len(values)-1, int((len(values)-1) * p))]
        result[path] = dict(calls=len(values), total_ms=sum(values), p50_ms=percentile(.5),
                            p95_ms=percentile(.95), p99_ms=percentile(.99), max_ms=values[-1])
    return result
