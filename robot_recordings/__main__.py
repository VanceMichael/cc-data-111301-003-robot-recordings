DESCRIPTION = '机器人 MCAP 记录的通道世代与观测关联采集'
SAMPLE = 'fixtures/part-0.mcap'

import argparse
import json
from pathlib import Path
import tempfile

from .archive import archive_file
from .core import inspect_file


def main():
    parser = argparse.ArgumentParser(description=DESCRIPTION)
    parser.add_argument('command', choices=['inspect', 'capture', 'demo'])
    parser.add_argument('path', nargs='?', default=SAMPLE)
    parser.add_argument('--archive', default='.work/raw')
    args = parser.parse_args()
    if args.command == 'inspect':
        result = inspect_file(Path(args.path))
    elif args.command == 'capture':
        result = archive_file(Path(args.path), Path(args.archive))
    else:
        with tempfile.TemporaryDirectory() as directory:
            result = {'原始资料': archive_file(Path(args.path), Path(directory)),
                      '资料预览': inspect_file(Path(args.path))}
    print(json.dumps(result, ensure_ascii=False, indent=2))


if __name__ == '__main__':
    main()
