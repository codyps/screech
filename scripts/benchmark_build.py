#!/usr/bin/env python3
"""Keep independently hosted benchmark campaigns tied to one immutable build."""
import argparse
import hashlib
import json
from pathlib import Path

BINARIES = {
    'screech_binary_sha256': 'screech-server',
    'nats_binary_sha256': 'nats-server',
    'driver_binary_sha256': 'bench-driver',
}


def digest(path):
    with path.open('rb') as file:
        return hashlib.file_digest(file, 'sha256').hexdigest()


def create(directory, revision, nats_commit, rustc):
    value = dict(schema=1, revision=revision, nats_commit=nats_commit, rustc=rustc)
    value.update((key, digest(directory / name)) for key, name in BINARIES.items())
    (directory / 'benchmark-build.json').write_text(json.dumps(value, indent=2) + '\n')
    return value


def verify(directory, revision):
    value = json.loads((directory / 'benchmark-build.json').read_text())
    if value.get('schema') != 1 or value.get('revision') != revision:
        raise ValueError('benchmark build has the wrong schema or source revision')
    if not value.get('nats_commit') or not value.get('rustc'):
        raise ValueError('benchmark build is missing toolchain or NATS provenance')
    for key, name in BINARIES.items():
        if digest(directory / name) != value.get(key):
            raise ValueError(f'benchmark binary changed: {name}')
    return value


def campaigns(build, paths, run_id):
    if not paths:
        raise ValueError('no benchmark campaigns supplied')
    identities = set()
    for path in paths:
        environment = json.loads(path.read_text())['environment']
        for field in (*BINARIES, 'revision', 'nats_commit', 'rustc'):
            if environment.get(field) != build[field]:
                raise ValueError(f'{path}: benchmark build differs at {field}')
        attempt = environment.get('ci_run_attempt')
        if environment.get('ci_run_id') != run_id or not attempt or not str(attempt).isdigit() or int(attempt) < 1:
            raise ValueError(f'{path}: missing or incorrect workflow provenance')
        identities.add((run_id, str(attempt)))
    if len(identities) != 1:
        raise ValueError('cannot combine benchmark measurements from different attempts')
    return identities.pop()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('operation', choices=('create', 'verify', 'campaigns'))
    parser.add_argument('--directory', type=Path, required=True)
    parser.add_argument('--revision', required=True)
    parser.add_argument('--nats-commit')
    parser.add_argument('--rustc')
    parser.add_argument('--run-id')
    parser.add_argument('paths', type=Path, nargs='*')
    args = parser.parse_intermixed_args()
    if args.operation == 'create':
        if not args.nats_commit or not args.rustc:
            parser.error('create requires --nats-commit and --rustc')
        create(args.directory, args.revision, args.nats_commit, args.rustc)
    else:
        build = verify(args.directory, args.revision)
        if args.operation == 'campaigns':
            if not args.run_id:
                parser.error('campaigns requires --run-id')
            run_id, attempt = campaigns(build, args.paths, args.run_id)
            print(f'Verified {len(args.paths)} campaigns from workflow {run_id}, attempt {attempt}')


if __name__ == '__main__':
    main()
