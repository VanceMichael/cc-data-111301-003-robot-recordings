import json
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path


class CommandTests(unittest.TestCase):
    def run_cli(self, *arguments):
        return subprocess.run(
            [sys.executable, '-m', 'robot_recordings', *arguments],
            capture_output=True, text=True, timeout=30,
        )

    def test_demo_end_to_end(self):
        process = self.run_cli('demo')
        self.assertEqual(process.returncode, 0, process.stderr)
        result = json.loads(process.stdout)
        self.assertIn('资料预览', result)
        self.assertGreater(result['原始资料']['bytes'], 0)
        self.assertEqual(len(result['原始资料']['sha256']), 64)
        states = {entry['state'] for entry in result['关联快照']}
        # part-0 检测在补交载荷后关联，part-1 无引用
        self.assertEqual(states, {'linked', 'no_reference'})

    def test_legacy_inspect_and_capture(self):
        inspect = self.run_cli('inspect', 'fixtures/part-0.mcap')
        self.assertEqual(inspect.returncode, 0, inspect.stderr)
        self.assertIn('/camera/objects', json.loads(inspect.stdout)['topics'])
        with tempfile.TemporaryDirectory() as directory:
            capture = self.run_cli('capture', 'fixtures/part-0.mcap',
                                   '--archive', str(Path(directory) / 'raw'))
            self.assertEqual(capture.returncode, 0, capture.stderr)
            self.assertEqual(len(json.loads(capture.stdout)['sha256']), 64)

    def test_collect_supplement_workflow_and_exit_codes(self):
        with tempfile.TemporaryDirectory() as directory:
            store = str(Path(directory) / 'store')
            before = str(Path(directory) / 'before.json')
            first = self.run_cli('collect', 'fixtures/part-0.mcap', 'fixtures/part-1.mcap',
                                 '--store', store)
            self.assertEqual(first.returncode, 0, first.stderr)
            snap = self.run_cli('snapshot', '--store', store, '--output', before)
            self.assertEqual(snap.returncode, 0, snap.stderr)
            second = self.run_cli('collect', 'fixtures/part-2.mcap',
                                  '--store', store, '--snapshot-before', before)
            self.assertEqual(second.returncode, 0, second.stderr)
            payload = json.loads(second.stdout)
            change = payload['snapshot_diff']['state_changes'][0]
            self.assertEqual(change['from_state'], 'unlinked')
            self.assertEqual(change['to_state'], 'linked')
            # 重复采集是幂等的
            again = self.run_cli('collect', 'fixtures/part-0.mcap', '--store', store)
            self.assertTrue(json.loads(again.stdout)['reports'][0]['duplicate'])
            # 清单保留
            manifest = self.run_cli('manifest', '--store', store)
            self.assertEqual(manifest.returncode, 0, manifest.stderr)
            self.assertEqual(len(json.loads(manifest.stdout)['sources']), 3)

    def test_invalid_input_fails_command(self):
        with tempfile.TemporaryDirectory() as directory:
            bad = Path(directory) / 'bad.mcap'
            bad.write_bytes(b'not mcap')
            process = self.run_cli('collect', str(bad),
                                   '--store', str(Path(directory) / 'store'))
            self.assertNotEqual(process.returncode, 0)
            payload = json.loads(process.stdout)
            self.assertFalse(payload['reports'][0]['accepted'])

    def test_bad_chunk_returns_partial_exit_code_without_full_failure(self):
        from tests.fixture_builder import build_corrupt_chunk

        with tempfile.TemporaryDirectory() as directory:
            bad = Path(directory) / 'bad.mcap'
            build_corrupt_chunk(bad)
            store = str(Path(directory) / 'store')
            process = self.run_cli('collect', str(bad), '--store', store)
            self.assertEqual(process.returncode, 3, process.stderr)
            report = json.loads(process.stdout)['reports'][0]
            self.assertEqual(report['status'], 'partial')
            self.assertEqual(report['failures'][0]['scope'], 'chunk')
            # 好 chunk 的消息仍可查询，且结果标注局部失败来源
            read = self.run_cli('read', '--store', store)
            self.assertEqual(read.returncode, 0, read.stderr)
            result = json.loads(read.stdout)
            self.assertGreater(len(result['messages']), 0)
            self.assertEqual(len(result['partial_sources']), 1)
            # 清单不声称完整
            manifest = json.loads(self.run_cli('manifest', '--store', store).stdout)
            self.assertEqual(manifest['sources'][0]['status'], 'partial')


if __name__ == '__main__':
    unittest.main()
