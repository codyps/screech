import os
from pathlib import Path
import subprocess
import tempfile
import unittest


class GuestKernelSelection(unittest.TestCase):
    def install(self, metadata):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / 'metadata').write_text(metadata)
            for name, body in {
                'apt-cache': 'test "$*" = "depends linux-image-generic" || exit 9\ncat "$VM_TOOLS_TEST_DIR/metadata"\n',
                'apt-get': 'printf "%s\\n" "$*" >> "$VM_TOOLS_TEST_DIR/calls"\n',
            }.items():
                path = root / name
                path.write_text('#!/usr/bin/env bash\nset -eu\n' + body)
                path.chmod(0o755)
            script = Path(__file__).resolve().parents[1] / 'scripts/install_vm_tools.sh'
            result = subprocess.run(['bash', str(script)], capture_output=True, text=True,
                                    env=dict(os.environ, PATH=str(root) + os.pathsep + os.environ['PATH'],
                                             VM_TOOLS_TEST_DIR=str(root)))
            return result, (root / 'calls').read_text().splitlines()

    def test_installs_only_the_generic_tracks_versioned_image(self):
        result, calls = self.install('''linux-image-generic
  Depends: linux-image-6.8.0-149-generic
  Depends: linux-modules-extra-6.8.0-149-generic
  Depends: linux-firmware
  Recommends: linux-image-7.0.0-42-generic
''')
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(len(calls), 2)
        self.assertIn(' update', calls[0])
        self.assertIn('linux-image-6.8.0-149-generic busybox-static', calls[1])
        for unwanted in ('7.0.0', 'linux-image-generic', 'linux-firmware', 'modules-extra'):
            self.assertNotIn(unwanted, calls[1])

    def test_missing_or_ambiguous_image_fails_before_install(self):
        for metadata in ('linux-image-generic\n  Depends: linux-firmware\n',
                         '  Depends: linux-image-6.8.0-148-generic\n'
                         '  Depends: linux-image-6.8.0-149-generic\n'):
            with self.subTest(metadata=metadata):
                result, calls = self.install(metadata)
                self.assertNotEqual(result.returncode, 0)
                self.assertIn('Expected one versioned image', result.stderr)
                self.assertEqual(len(calls), 1)
