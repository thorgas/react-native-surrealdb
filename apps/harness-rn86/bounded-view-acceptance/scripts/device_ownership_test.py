import subprocess
import unittest
from device_ownership import snapshot, create_mappings, restore

class Device:
    def __init__(self, mappings='', prior='null', fail_port=None):
        self.mappings = mappings
        self.prior = prior
        self.fail_port = fail_port
        self.calls = []
    def __call__(self, *args):
        self.calls.append(args)
        output = ''
        code = 0
        if args == ('reverse', '--list'): output = self.mappings
        elif args[:3] == ('shell', 'settings', 'get'): output = self.prior
        elif args[:2] == ('reverse', '--remove'):
            self.mappings = '\n'.join(line for line in self.mappings.splitlines() if args[2] != line.split()[-2])
        elif args[0] == 'reverse':
            if args[1] == self.fail_port: code = 1
            else: self.mappings += f'UsbFfs {args[1]} {args[2]}\n'
        return subprocess.CompletedProcess(args, code, output, '')

class OwnershipTest(unittest.TestCase):
    def test_occupied_planned_port_refused_without_mutation(self):
        device = Device('UsbFfs tcp:8080 tcp:9999\n')
        with self.assertRaises(RuntimeError): snapshot(device)
        self.assertEqual(device.calls, [('reverse', '--list')])
    def test_partial_creation_failure_cleans_only_created_and_restores_null(self):
        device = Device('UsbFfs tcp:5555 tcp:5555\n', fail_port='tcp:19061')
        baseline = snapshot(device)
        created = []
        with self.assertRaises(subprocess.CalledProcessError): create_mappings(device, created)
        self.assertEqual(created, [8080])
        self.assertEqual(restore(device, baseline, created), [])
        self.assertIn('tcp:5555', device.mappings)
        self.assertNotIn('tcp:8080', device.mappings)
        self.assertIn(('shell', 'settings', 'delete', 'global', 'hide_error_dialogs'), device.calls)
    def test_setting_exact_and_already_removed_mapping_tolerated(self):
        device = Device(prior='1')
        baseline = snapshot(device)
        self.assertEqual(restore(device, baseline, [19061]), [])
        self.assertIn(('shell', 'settings', 'put', 'global', 'hide_error_dialogs', '1'), device.calls)
        self.assertFalse(any(call[:2] == ('reverse', '--remove') for call in device.calls))
    def test_reverse_readback_failure_still_restores_setting(self):
        calls = []
        def broken(*args):
            calls.append(args)
            if args == ('reverse', '--list'): raise OSError('ADB disconnected')
            return subprocess.CompletedProcess(args, 0, '', '')
        errors = restore(broken, {'hideErrorDialogs': '1'}, [8080])
        self.assertTrue(errors)
        self.assertIn(('shell', 'settings', 'put', 'global', 'hide_error_dialogs', '1'), calls)
    def test_changed_mapping_preserved(self):
        device = Device()
        baseline = snapshot(device)
        device.mappings = 'UsbFfs tcp:19061 tcp:9999\n'
        self.assertTrue(restore(device, baseline, [19061]))
        self.assertIn('tcp:9999', device.mappings)

if __name__ == '__main__': unittest.main()
