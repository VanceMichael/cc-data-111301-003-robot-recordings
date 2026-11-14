"""命令行入口：保留单份资料命令，新增跨记录采集与查询。"""

DESCRIPTION = '机器人 MCAP 记录的通道世代与观测关联采集'
SAMPLE = 'fixtures/part-0.mcap'

import argparse
import json
from pathlib import Path
import sys
import tempfile

from .archive import archive_file
from .core import inspect_file
from .store import EXIT_INVALID, EXIT_PARTIAL, InvalidSource, Store

DEFAULT_STORE = '.work/store'


def _print(result):
    print(json.dumps(result, ensure_ascii=False, indent=2))


def _load_snapshot(path):
    with Path(path).open('r', encoding='utf-8') as stream:
        return json.load(stream)


def _save_snapshot(path, snapshot):
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    with target.open('w', encoding='utf-8') as stream:
        json.dump(snapshot, stream, ensure_ascii=False, indent=2)


def build_parser():
    parser = argparse.ArgumentParser(description=DESCRIPTION)
    subparsers = parser.add_subparsers(dest='command', required=True)

    inspect_parser = subparsers.add_parser('inspect', help='单份 MCAP 资料预览（保留原命令）')
    inspect_parser.add_argument('path', nargs='?', default=SAMPLE)

    capture_parser = subparsers.add_parser('capture', help='原始字节内容寻址归档（保留原命令）')
    capture_parser.add_argument('path', nargs='?', default=SAMPLE)
    capture_parser.add_argument('--archive', default='.work/raw')

    subparsers.add_parser('demo', help='在临时目录跑通完整采集流程')

    collect_parser = subparsers.add_parser('collect', help='采集一份或多份 MCAP 到资料库')
    collect_parser.add_argument('paths', nargs='+')
    collect_parser.add_argument('--store', default=DEFAULT_STORE)
    collect_parser.add_argument('--snapshot-before',
                                help='补交前的观测快照文件；存在时报告附带状态迁移')

    manifest_parser = subparsers.add_parser('manifest', help='资料库清单：来源、失败范围、channel 世代')
    manifest_parser.add_argument('--store', default=DEFAULT_STORE)

    obs_parser = subparsers.add_parser('observations', help='查询观测与载荷的关联状态')
    obs_parser.add_argument('--store', default=DEFAULT_STORE)
    obs_parser.add_argument('--boot')
    obs_parser.add_argument('--state', choices=['linked', 'unlinked', 'no_reference', 'boot_mismatch'])
    obs_parser.add_argument('--snapshot', help='把当前快照写入该文件，供补交前后对比')

    read_parser = subparsers.add_parser('read', help='按启动周期与时间段索引化读取消息')
    read_parser.add_argument('--store', default=DEFAULT_STORE)
    read_parser.add_argument('--boot')
    read_parser.add_argument('--start', type=int)
    read_parser.add_argument('--end', type=int)
    read_parser.add_argument('--time-basis', default='sample_time',
                            choices=['sample_time', 'log_time', 'publish_time'])
    read_parser.add_argument('--topic', action='append', dest='topics')

    diff_parser = subparsers.add_parser('diff', help='对比两次观测快照的状态迁移')
    diff_parser.add_argument('--before', required=True)
    diff_parser.add_argument('--after', required=True)

    snapshot_parser = subparsers.add_parser('snapshot', help='导出观测关联状态快照')
    snapshot_parser.add_argument('--store', default=DEFAULT_STORE)
    snapshot_parser.add_argument('--output', required=True)

    status_parser = subparsers.add_parser('status', help='资料库汇总状态')
    status_parser.add_argument('--store', default=DEFAULT_STORE)
    return parser


def _run_collect(args):
    reports = []
    before = None
    before_path = Path(args.snapshot_before) if args.snapshot_before else None
    if before_path is not None and before_path.exists():
        before = _load_snapshot(before_path)
    partial = False
    invalid = False
    with Store(args.store) as store:
        for raw_path in args.paths:
            try:
                report = store.collect_file(Path(raw_path))
            except (InvalidSource, FileNotFoundError) as exc:
                reports.append({'source': str(raw_path), 'error': str(exc), 'accepted': False})
                invalid = True
                continue
            report['accepted'] = True
            if report['status'] == 'partial':
                partial = True
            reports.append(report)
        after = store.observation_snapshot()
        result = {
            'reports': reports,
            'summary': store.status_summary(),
            'snapshot_after': after,
        }
        if before is not None:
            result['snapshot_diff'] = store.diff_snapshots(before, after)
    _print(result)
    if invalid:
        return EXIT_INVALID
    if partial:
        return EXIT_PARTIAL
    return 0


def main(argv=None):
    parser = build_parser()
    args = parser.parse_args(argv)
    if args.command == 'inspect':
        _print(inspect_file(Path(args.path)))
        return 0
    if args.command == 'capture':
        _print(archive_file(Path(args.path), Path(args.archive)))
        return 0
    if args.command == 'collect':
        return _run_collect(args)
    if args.command == 'manifest':
        with Store(args.store) as store:
            _print(store.manifest())
        return 0
    if args.command == 'observations':
        with Store(args.store) as store:
            entries = store.query_observations(boot=args.boot, state=args.state)
            if args.snapshot:
                _save_snapshot(args.snapshot, entries)
            _print(entries)
        return 0
    if args.command == 'read':
        with Store(args.store) as store:
            _print(store.read_messages(
                boot=args.boot, start=args.start, end=args.end,
                time_basis=args.time_basis, topics=args.topics))
        return 0
    if args.command == 'diff':
        before = _load_snapshot(args.before)
        after = _load_snapshot(args.after)
        _print(Store.diff_snapshots(before, after))
        return 0
    if args.command == 'snapshot':
        with Store(args.store) as store:
            _save_snapshot(args.output, store.observation_snapshot())
        _print({'output': args.output})
        return 0
    if args.command == 'status':
        with Store(args.store) as store:
            _print(store.status_summary())
        return 0
    if args.command == 'demo':
        with tempfile.TemporaryDirectory() as directory:
            result = {'原始资料': archive_file(Path(SAMPLE), Path(directory) / 'raw'),
                      '资料预览': inspect_file(Path(SAMPLE))}
            with Store(Path(directory) / 'store') as store:
                for part in sorted(Path('fixtures').glob('part-*.mcap')):
                    result.setdefault('采集', []).append(store.collect_file(part))
                result['关联快照'] = store.observation_snapshot()
                result['清单'] = store.manifest()
        _print(result)
        return 0
    parser.error(f'未知命令 {args.command}')
    return 2


if __name__ == '__main__':
    sys.exit(main())
