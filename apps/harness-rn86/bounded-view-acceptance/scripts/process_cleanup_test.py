import json
import pathlib
import signal
import tempfile
import unittest
from unittest.mock import Mock, patch
from process_cleanup import persist_cleanup, settle_port, stop_owned_group

class CleanupTest(unittest.TestCase):
    def test_receipt_persists_and_later_actions_run_when_cleanup_readback_fails(self):
        with tempfile.TemporaryDirectory() as directory:
            target = pathlib.Path(directory) / 'receipt.json'
            def fail(): raise OSError('readback failed')
            persist_cleanup({}, target, [('reverse', fail), ('settingRestored', lambda: True)])
            result = json.loads(target.read_text())
            self.assertTrue(result['settingRestored'])
            self.assertIn('readback failed', result['cleanupErrors'][0])
    @patch('process_cleanup.os.killpg')
    @patch('process_cleanup.time.monotonic', side_effect=[0, 1, 2, 3, 4])
    def test_owned_group_escalates_and_waits(self, _clock, kill):
        proc = Mock(pid=123)
        # live before TERM, live when checking deadline, live for escalation;
        # after KILL/wait it is gone.
        checks = iter([True, True, True, False, False])
        def send(pid, sig):
            self.assertEqual(pid, 123)
            if sig == 0 and not next(checks): raise ProcessLookupError()
        kill.side_effect = send
        stop_owned_group(proc, timeout=.1)
        self.assertIn(((123, signal.SIGTERM),), kill.call_args_list)
        self.assertIn(((123, signal.SIGKILL),), kill.call_args_list)
        proc.wait.assert_called_once_with(timeout=.1)
    @patch('process_cleanup.port_closed', return_value=False)
    @patch('process_cleanup.time.monotonic', side_effect=[0, 10])
    def test_occupied_port_reports_failure_without_killing_any_process(self, _clock, _port):
        self.assertFalse(settle_port(19061, timeout=1))

if __name__ == '__main__': unittest.main()
