#!/usr/bin/env python3
"""Build a self-contained, dependency-free Pages dashboard from checked trials."""
import argparse
from datetime import datetime, timezone
import json
import math
from pathlib import Path
import shutil

REQUIRED = ('server', 'scenario', 'payload_bytes', 'subscribers', 'publishes', 'deliveries', 'seconds', 'deliveries_per_second', 'trial')

CORE_TOPOLOGIES = {
    'core-subject-churn': (8192, 0),
    'core-many-subscriptions': (1, 512),
    'core-churn-many-subscriptions': (8192, 512),
}

def read_campaign(path, label):
    value = json.loads(path.read_text())
    if not isinstance(value.get('environment'), dict) or not value.get('rows'):
        raise ValueError(f'{path}: missing environment or trials')
    seen = set()
    for row in value['rows']:
        if row.get('checked') is not True or any(key not in row for key in REQUIRED):
            raise ValueError(f'{path}: incomplete or unchecked trial')
        if row.get('scenario') in CORE_TOPOLOGIES:
            actual = (row.get('core_subjects'), row.get('unused_wildcards'))
            if actual != CORE_TOPOLOGIES[row['scenario']] or row.get('core_window') != 1024:
                raise ValueError(f'{path}: invalid Core scaling topology')
        for key, number in row.items():
            if isinstance(number, (float, int)) and (not math.isfinite(number) or number < 0):
                raise ValueError(f'{path}: invalid {key}')
        if row['seconds'] <= 0 or row['publishes'] <= 0:
            raise ValueError(f'{path}: empty timed workload')
        key = tuple(row[k] for k in ('server', 'scenario', 'payload_bytes', 'subscribers', 'publishes', 'trial'))
        if key in seen:
            raise ValueError(f'{path}: duplicate trial')
        seen.add(key)
    return {'label': label, 'environment': value['environment'], 'rows': value['rows']}

def build(inputs, output, revision='', run_url='', repository='https://github.com/codyps/screech'):
    # Validate everything before touching an existing site.
    campaigns = [read_campaign(Path(path), label) for label, path in inputs]
    template = Path(__file__).resolve().parents[1] / 'tools/dashboard'
    output.mkdir(parents=True, exist_ok=True)
    for name in ('index.html', 'dashboard.css', 'dashboard.js'):
        shutil.copy2(template/name, output/name)
    raw = output/'data'; raw.mkdir(exist_ok=True)
    for i, campaign in enumerate(campaigns):
        campaign['id'] = f'campaign-{i}'
        campaign['download'] = f'data/campaign-{i}.json'
        shutil.copy2(inputs[i][1], output/campaign['download'])
    payload = {'schema': 1, 'generated_at': datetime.now(timezone.utc).isoformat(),
               'revision': revision, 'run_url': run_url, 'repository': repository, 'campaigns': campaigns}
    serialized = json.dumps(payload, allow_nan=False, separators=(',', ':')).replace('<', '\\u003c').replace('\u2028', '\\u2028').replace('\u2029', '\\u2029')
    (output/'data.js').write_text('window.BENCHMARK_DATA = '+serialized+';\n')
    (output/'.nojekyll').touch()
    return payload

if __name__ == '__main__':
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--input', nargs=2, metavar=('LABEL', 'JSON'), action='append', required=True)
    p.add_argument('--output', type=Path, required=True)
    p.add_argument('--revision', default=''); p.add_argument('--run-url', default='')
    p.add_argument('--repository', default='https://github.com/codyps/screech')
    args = p.parse_args()
    payload = build(args.input, args.output, args.revision, args.run_url, args.repository)
    print(f"Built {len(payload['campaigns'])} campaigns in {args.output}")
