"""Bounded shutdown of a process group explicitly created by this runner."""
import json
import os
import signal
import socket
import time


def port_closed(port):
    with socket.socket() as probe:
        return probe.connect_ex(('127.0.0.1', port)) != 0


def stop_owned_group(proc, timeout=5):
    # proc was created with start_new_session=True; never derive identity from a port.
    def alive():
        try:
            os.killpg(proc.pid, 0)
            return True
        except ProcessLookupError:
            return False
    if alive():
        os.killpg(proc.pid, signal.SIGTERM)
    end = time.monotonic() + timeout
    while alive() and time.monotonic() < end:
        proc.poll()
        time.sleep(.05)
    if alive():
        os.killpg(proc.pid, signal.SIGKILL)
    proc.wait(timeout=timeout)
    end = time.monotonic() + timeout
    while alive() and time.monotonic() < end:
        time.sleep(.05)
    if alive():
        raise RuntimeError('Owned process group did not settle after SIGKILL')


def settle_port(port, timeout=5):
    end = time.monotonic() + timeout
    while not port_closed(port) and time.monotonic() < end:
        time.sleep(.05)
    return port_closed(port)


def persist_cleanup(receipt, target, actions):
    errors = receipt.setdefault('cleanupErrors', [])
    try:
        for name, action in actions:
            try:
                receipt[name] = action()
            except Exception as error:
                errors.append(f'{name}: {error}')
    finally:
        receipt['finishedAt'] = time.time()
        target.write_text(json.dumps(receipt, indent=2) + '\n')
