#!/usr/bin/env python3
"""Control real LazyFS faults; no success without the completion acknowledgement."""
import argparse
import json
import os
from pathlib import Path
import select
import time


def clear(directory):
    completion = os.open(directory / 'completed.fifo', os.O_RDONLY | os.O_NONBLOCK)
    try:
        with (directory / 'faults.fifo').open('w') as fifo:
            fifo.write('lazyfs::clear-cache\n')
        until = time.monotonic() + 15
        reply = b''
        while b'finished::clear-cache' not in reply:
            if time.monotonic() > until:
                raise TimeoutError(f'LazyFS did not acknowledge cache loss: {reply!r}')
            if select.select([completion], [], [], .2)[0]:
                reply += os.read(completion, 4096)
        print(json.dumps({'cache_clear_acknowledged': True}))
    finally:
        os.close(completion)


def canary(directory):
    until = time.monotonic() + 30
    while not os.path.ismount(directory / 'store'):
        if time.monotonic() > until:
            raise TimeoutError('LazyFS mount never became ready')
        time.sleep(.05)
    dirty = directory / 'store' / 'unsynced-canary'
    clean = directory / 'store' / 'synced-canary'
    dirty.write_bytes(b'must-be-lost-without-fsync')
    with clean.open('wb') as file:
        file.write(b'must-survive-fsync')
        file.flush()
        os.fsync(file.fileno())
    clear(directory)
    observed_dirty = dirty.read_bytes() if dirty.exists() else b''
    observed_clean = clean.read_bytes()
    assert observed_dirty != b'must-be-lost-without-fsync', 'LazyFS did not discard unsynced bytes'
    assert observed_clean == b'must-survive-fsync', 'LazyFS discarded fsynced bytes'
    result = {'unsynced_bytes_lost': True, 'fsynced_bytes_preserved': True, 'real_fuse_mount': True}
    (directory / 'canary.json').write_text(json.dumps(result))
    dirty.unlink(missing_ok=True)
    clean.unlink()
    print(json.dumps(result))


def configure(directory):
    (directory / 'lazyfs.toml').write_text(f'''[faults]
fifo_path="{directory}/faults.fifo"
fifo_path_completed="{directory}/completed.fifo"
[cache]
apply_eviction=false
[cache.simple]
custom_size="64MB"
blocks_per_page=1
[filesystem]
logfile="{directory}/lazyfs.log"
log_all_operations=false
''')


def damage(directory, kind):
    mount = directory / 'store'
    if kind == 'wipe':
        import shutil
        for path in mount.iterdir():
            if path.is_dir():
                shutil.rmtree(path)
            else:
                path.unlink()
        return
    record = json.loads(bytes(json.loads((mount / 'db.node-state').read_text())['body']))
    path = Path(record['instance']['path'])
    assert path.is_relative_to(mount), 'refusing to damage files outside the voter mount'
    with path.open('r+b', buffering=0) as file:
        length = path.stat().st_size
        assert length > 0
        if kind == 'truncate':
            file.truncate(length // 2)
        elif kind == 'bitflip':
            # Flip the SQLite magic, rather than a possibly unused page byte.
            offset = 0
            file.seek(offset)
            byte = file.read(1)
            file.seek(offset)
            file.write(bytes([byte[0] ^ 1]))
        else:
            raise ValueError(kind)
        os.fsync(file.fileno())
    print(json.dumps({'fault': kind, 'path': str(path)}))


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('action', choices=['configure', 'clear', 'canary', 'wipe', 'bitflip', 'truncate'])
    parser.add_argument('directory', type=Path)
    args = parser.parse_args()
    assert Path('/run/screech-jepsen-vm').exists(), 'fault controls may only run inside the disposable VM'
    if args.action in ['configure', 'clear', 'canary']:
        globals()[args.action](args.directory)
    else:
        damage(args.directory, args.action)
