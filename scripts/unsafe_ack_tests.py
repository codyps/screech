#!/usr/bin/env python3
"""Opt-in ACK buffering, lost-buffered-state recovery and durable fences."""
import argparse
import json
from pathlib import Path
import subprocess
import tempfile
from crash_tests import Server
from wire import Client
from batch_crash_tests import no_response
from group_crash_tests import wait_until
from io_failure_tests import attempt


def seed(binary, directory):
    server = Server(binary, directory)
    try:
        c = Client(server.address)
        assert 'error' not in c.request('$JS.API.STREAM.CREATE.T', {'name':'T','subjects':['test.>'],'storage':'file','retention':'workqueue'})
        c.create_consumer(ack_wait=3600000000000)
        c.publish(b'value')
        message = c.pull()
        assert c.request('$JS.API.SCREECH.CHECKPOINT', {})['success']
        c.close()
        return message
    finally:
        server.kill()


def run(binary, shim):
    results = []
    rejected = subprocess.run([binary, '--unsafe-ack-durability'], capture_output=True, text=True, timeout=5)
    assert rejected.returncode == 2 and '--store' in rejected.stderr
    results.append({'flag_requires_store':True,'passed':True})
    for point in ['before_unsafe_ack_flush','after_unsafe_ack_flush']:
        with tempfile.TemporaryDirectory(prefix='screech-unsafe-ack-') as tmp:
            directory = Path(tmp)
            message = seed(binary, directory)
            server = Server(binary, directory, point, extra_args=['--unsafe-ack-durability'], extra_env={'SCREECH_UNSAFE_ACK_FLUSH_GATE':str(directory/'flush-release')})
            try:
                c = Client(server.address)
                c.ack(message)
                (directory/'flush-release').touch()
                server.wait_fault()
                core = Client(server.address)
                core.send(b'SUB alive 99\r\nPUB alive 2\r\nok\r\n')
                assert core.read_message()['payload'] == b'ok'
                core.close()
                c.close()
            finally:
                server.kill()
            server = Server(binary, directory)
            try:
                c = Client(server.address)
                committed = point.startswith('after_')
                assert (c.get(1) is None) == committed
                info = c.request('$JS.API.CONSUMER.INFO.T.D', {})
                assert info['num_ack_pending'] == int(not committed), info
                assert info['ack_floor']['consumer_seq'] == int(committed), info
                c.close()
            finally:
                server.kill()
            results.append({'point':point,'confirmed_ack_recovered':committed,'confirmed_ack_lost_on_sigkill':not committed,'passed':True})
    for mode in ['sync-before-eio','sync-after-eio']:
        with tempfile.TemporaryDirectory(prefix='screech-unsafe-flush-') as tmp:
            directory = Path(tmp)
            message = seed(binary, directory)
            arm, hit = directory/'armed', directory/'hit'
            server = Server(binary, directory, extra_args=['--unsafe-ack-durability'], extra_env={
                'SCREECH_UNSAFE_ACK_FLUSH_GATE':str(directory/'flush-release'),
                'LD_PRELOAD':str(shim),'SCREECH_IO_ARM':str(arm),'SCREECH_IO_HIT':str(hit),'SCREECH_IO_MODE':mode})
            try:
                c = Client(server.address)
                c.ack(message)
                arm.touch()
                (directory/'flush-release').touch()
                wait_until(hit.exists)
                assert hit.read_text() == 'fdatasync'
                # A background flush failure must stop subsequent durable success.
                result = attempt(lambda: c.request('test.x', b'forbidden'))
                assert result is None or 'error' in result, result
                c.close()
            finally:
                server.kill()
            results.append({'mode':mode,'flush_failure_stops_writes':True,'passed':True})
    for point in ['before_commit','after_commit']:
        with tempfile.TemporaryDirectory(prefix='screech-unsafe-publish-fence-') as tmp:
            directory = Path(tmp)
            message = seed(binary, directory)
            server = Server(binary, directory, point, extra_args=['--unsafe-ack-durability'], extra_env={'SCREECH_UNSAFE_ACK_FLUSH_GATE':str(directory/'flush-release')})
            try:
                c = Client(server.address)
                c.ack(message) # This consumer transition is still buffered.
                c.send(b'SUB _INBOX.pub 99\r\nPUB test.x _INBOX.pub 3\r\nnew\r\n')
                server.wait_fault()
                no_response(c)
                c.close()
            finally:
                server.kill()
            server = Server(binary, directory)
            try:
                c = Client(server.address)
                committed = point == 'after_commit'
                assert c.get(2) == (b'new' if committed else None)
                assert c.get(1) == (None if committed else b'value')
                c.close()
            finally:
                server.kill()
            results.append({'publication_point':point,'buffered_ack_and_publication_atomic':True,'passed':True})
    with tempfile.TemporaryDirectory(prefix='screech-unsafe-shutdown-') as tmp:
        directory = Path(tmp)
        message = seed(binary, directory)
        server = Server(binary, directory, extra_args=['--unsafe-ack-durability'], extra_env={'SCREECH_UNSAFE_ACK_FLUSH_GATE':str(directory/'flush-release')})
        try:
            c = Client(server.address)
            c.ack(message)
            c.close()
            server.process.terminate()
            assert server.process.wait(timeout=5) == 0
            assert 'UNSAFE ACK DURABILITY ENABLED' in (directory/'stderr.log').read_text()
        finally:
            server.kill()
        server = Server(binary, directory)
        try:
            c = Client(server.address)
            assert c.get(1) is None
            c.close()
        finally:
            server.kill()
        results.append({'graceful_shutdown_flushes_buffered_ack':True,'passed':True})
    return results


if __name__ == '__main__':
    p = argparse.ArgumentParser()
    p.add_argument('binary')
    p.add_argument('--shim', type=lambda s: Path(s).resolve(), required=True)
    p.add_argument('--output', type=Path)
    args = p.parse_args()
    text = json.dumps(run(args.binary, args.shim), indent=2)
    print(text)
    if args.output: args.output.write_text(text+'\n')
