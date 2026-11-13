import json
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest


class CommandTests(unittest.TestCase):
    def test_preview_and_raw_capture(self):
        with tempfile.TemporaryDirectory() as directory:
            process = subprocess.run([sys.executable,'-m','robot_recordings','demo'],capture_output=True,text=True,timeout=15)
            self.assertEqual(process.returncode,0,process.stderr)
            result = json.loads(process.stdout)
            self.assertIn('资料预览',result)
            self.assertGreater(result['原始资料']['bytes'],0)
            self.assertEqual(len(result['原始资料']['sha256']),64)
