import hashlib
import json
import os
from pathlib import Path
import tempfile


def archive_file(source, destination):
    source = Path(source)
    destination = Path(destination)
    destination.mkdir(parents=True, exist_ok=True)
    digest = hashlib.sha256()
    size = 0
    temporary = None
    try:
        with source.open('rb') as reader, tempfile.NamedTemporaryFile(dir=destination, delete=False) as writer:
            temporary = Path(writer.name)
            while block := reader.read(65536):
                writer.write(block)
                digest.update(block)
                size += len(block)
            writer.flush()
            os.fsync(writer.fileno())
        identifier = digest.hexdigest()
        target = destination / identifier
        os.replace(temporary, target)
        temporary = None
        return {'sha256': identifier, 'bytes': size, 'source': source.name, 'object': str(target)}
    finally:
        if temporary is not None:
            temporary.unlink(missing_ok=True)


def read_object(destination, identifier):
    if len(identifier) != 64 or any(character not in '0123456789abcdef' for character in identifier):
        raise ValueError('对象摘要格式不正确')
    content = (Path(destination) / identifier).read_bytes()
    if hashlib.sha256(content).hexdigest() != identifier:
        raise ValueError('归档对象与摘要不一致')
    return content
