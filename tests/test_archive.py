import hashlib
from pathlib import Path
import tempfile
import unittest

from robot_recordings.archive import archive_file, read_object


class ArchiveTests(unittest.TestCase):
    def test_roundtrip_binary(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            source = root / '原始资料.bin'
            source.write_bytes(bytes(range(256)) * 513)
            result = archive_file(source, root / 'archive')
            self.assertEqual(result['bytes'], 256 * 513)
            self.assertEqual(read_object(root / 'archive', result['sha256']), source.read_bytes())

    def test_duplicate_has_same_identity(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            source = root / 'a'
            source.write_bytes(b'abc')
            first = archive_file(source, root / 'archive')
            second = archive_file(source, root / 'archive')
            self.assertEqual(first, second)
            self.assertEqual(len(list((root / 'archive').iterdir())), 1)

    def test_empty_object_is_not_missing(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            source = root / 'empty'
            source.touch()
            result = archive_file(source, root / 'archive')
            self.assertEqual(result['sha256'], hashlib.sha256(b'').hexdigest())
            self.assertEqual(read_object(root / 'archive', result['sha256']), b'')

    def test_identifier_rejects_path(self):
        with self.assertRaises(ValueError):
            read_object('.', '../escape')

    def test_corrupt_object_is_detected(self):
        with tempfile.TemporaryDirectory() as directory:
            identifier = hashlib.sha256(b'original').hexdigest()
            (Path(directory) / identifier).write_bytes(b'changed')
            with self.assertRaises(ValueError):
                read_object(directory, identifier)

    def test_missing_source_leaves_no_object(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            with self.assertRaises(FileNotFoundError):
                archive_file(root / 'missing', root / 'archive')
            self.assertEqual(list((root / 'archive').iterdir()), [])
