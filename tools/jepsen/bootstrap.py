#!/usr/bin/env python3
"""Install the real upstream tools; no root or host FUSE device is required."""
import argparse
import json
import os
from pathlib import Path
import subprocess

REPO = Path(__file__).resolve().parents[2]
PROJECT = REPO / 'tools/jepsen'
RUNTIME = REPO / 'results/jepsen-runtime'


def run(*command, **kwargs):
    return subprocess.run(command, check=True, text=True, **kwargs)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--nixpkgs", default="nixpkgs", help="Nixpkgs flake URI; resolved source is recorded")
    args = parser.parse_args()
    RUNTIME.mkdir(parents=True, exist_ok=True)
    metadata = json.loads(run('nix', 'flake', 'metadata', args.nixpkgs, '--json', capture_output=True).stdout)
    result = run('nix', 'build', '--impure', '--file', str(PROJECT / 'runtime.nix'),
                 '--argstr', 'nixpkgs', metadata['url'], '--no-link', '--print-out-paths', capture_output=True)
    runtime = json.loads(Path(result.stdout.strip()).read_text())
    runtime['nixpkgs'] = metadata['locked']
    (RUNTIME / 'runtime.json').write_text(json.dumps(runtime, indent=2))
    source = REPO / 'upstream/lazyfs'
    upstream = json.loads((PROJECT / 'upstream.json').read_text())
    source.parent.mkdir(parents=True, exist_ok=True)
    if not source.exists():
        run('git', 'clone', upstream['lazyfs']['url'], str(source))
    head = run('git', 'rev-parse', 'HEAD', cwd=source, capture_output=True).stdout.strip()
    expected = upstream['lazyfs']['commit']
    if head != expected:
        # Never silently overwrite edits or another checkout in a developer's tree.
        if run('git', 'status', '--porcelain', cwd=source, capture_output=True).stdout:
            raise RuntimeError(f'{source} has local changes; expected LazyFS {expected}')
        run('git', 'checkout', '--detach', expected, cwd=source)
    environment = dict(os.environ, PATH=runtime['tools'] + '/bin:' + os.environ['PATH'])
    for component in ['libs/libpcache', 'lazyfs']:
        directory = source / component
        flags = ['-DCMAKE_BUILD_TYPE=Release', '-DCMAKE_POLICY_VERSION_MINIMUM=3.5']
        if component == 'lazyfs':
            flags += ['-DLAZYFS_BUILD_TESTS=OFF',
                      '-DCMAKE_CXX_FLAGS=-I' + runtime['fuse_dev'] + '/include',
                      '-DCMAKE_EXE_LINKER_FLAGS=-L' + runtime['fuse_lib'] + '/lib -Wl,-rpath,' + runtime['fuse_lib'] + '/lib']
        run('cmake', '-S', str(directory), '-B', str(directory / 'build'), *flags, env=environment)
        run('cmake', '--build', str(directory / 'build'), '-j', '4', env=environment)
    environment['LEIN_HOME'] = str(RUNTIME / 'lein-home')
    result = run(runtime['lein'], 'classpath', cwd=PROJECT, env=environment, capture_output=True)
    (RUNTIME / 'classpath.txt').write_text(result.stdout.strip() + '\n')
    print('Ready:', RUNTIME / 'runtime.json')


if __name__ == '__main__':
    main()
