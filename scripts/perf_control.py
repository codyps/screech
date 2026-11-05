"""Wait for perf to acknowledge event enablement before starting a workload."""
from contextlib import contextmanager
import os
import select
import signal
import subprocess
import time


@contextmanager
def recording(command, log, ready_timeout=15):
    # Startup can stall on hosted perf before it consumes the enable command.
    # Retry only that pre-workload timeout, after closing/reaping the first
    # process. Never retry the profiled body or accept an unacknowledged run.
    for attempt in range(2):
        enabled = False
        try:
            with _recording_once(command, log, ready_timeout) as process:
                enabled = True
                yield process
            return
        except TimeoutError:
            if enabled or attempt == 1:
                raise
            log.flush()
            os.write(log.fileno(), b'perf enable timed out; retrying startup before workload release\n')


@contextmanager
def _recording_once(command, log, ready_timeout):
    control_read, control_write = os.pipe()
    ack_read, ack_write = os.pipe()
    child_fds = [control_read, ack_write]
    parent_fds = [control_write, ack_read]
    process = None
    try:
        process = subprocess.Popen(command + ['--delay', '-1', '--control', f'fd:{control_read},{ack_write}'],
            pass_fds=tuple(child_fds), stdout=log, stderr=subprocess.STDOUT)
        for fd in child_fds: os.close(fd)
        child_fds.clear()
        try: os.write(control_write, b'enable\n')
        except BrokenPipeError as error:
            raise RuntimeError('perf closed the control pipe before enabling events') from error
        deadline = time.monotonic() + ready_timeout
        response = b''
        while b'\n' not in response:
            if process.poll() is not None:
                raise RuntimeError(f'perf exited before enabling events: {process.returncode}')
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise TimeoutError('perf did not acknowledge event enablement')
            if select.select([ack_read], [], [], min(remaining, .1))[0]:
                part = os.read(ack_read, 128)
                if not part:
                    raise RuntimeError('perf closed the control pipe before acknowledging enablement')
                response += part
        if response not in (b'ack\n', b'ack\n\0'):
            raise RuntimeError(f'unexpected perf enable response: {response!r}')
        yield process
    finally:
        if process is not None and process.poll() is None:
            process.send_signal(signal.SIGINT)
            try: process.wait(timeout=10)
            except subprocess.TimeoutExpired: process.kill(); process.wait()
        for fd in child_fds + parent_fds: os.close(fd)
