"""跨 MCAP 记录的资料采集与查询索引。

本模块在 :mod:`robot_recordings.archive` 的内容寻址归档之上增加一层 SQLite 索引，
解决单份资料预览无法回答的跨记录问题：

* 容器局部的 channel id / schema id 只在解析当下用于映射，索引里保留每次出现对应的
  channel 世代（topic、消息编码、channel metadata、schema 名称/编码/原文），同名 topic
  在不同文件换 schema 时不会互相覆盖。
* 消息内的 ``boot``（设备启动身份）、传感器身份、``sample_time``（设备时钟）与 MCAP 的
  ``log_time``、``publish_time`` 分别建列，不混用。
* 观测消息通过设备给出的 ``payload_ref`` 关联载荷的 ``observation_id``，关联范围限定在
  同一 boot；不依赖到达顺序，载荷先到、后到、永久缺失对应不同的可查询状态。
* 采集按消息原始字节做内容去重，重复片段可重复采集而不重复计数，每次交付的出处仍保留。
* 解析以 chunk 为隔离单位：坏 chunk 只记录局部失败的字节范围，其余消息正常入索引，
  不会把整趟试验标记为完整。
"""

from __future__ import annotations

import hashlib
import io
import json
import sqlite3
import struct
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path

from mcap.data_stream import ReadDataStream
from mcap.exceptions import McapError
from mcap.opcode import Opcode
from mcap.records import Channel, Chunk, Message, Schema
from mcap.stream_reader import breakup_chunk, read_magic

from .archive import archive_file

SCHEMA_VERSION = 1

# 采集结果退出码：完整成功 / 存在局部失败 / 输入无法识别
EXIT_OK = 0
EXIT_PARTIAL = 3
EXIT_INVALID = 1

_OBSERVATION_KIND = "observation"
_PAYLOAD_KIND = "payload"
_UNKNOWN_KIND = "unknown"


class InvalidSource(McapError):
    """输入不是可识别的 MCAP 资料。"""


@dataclass
class Occurrence:
    """一条消息在某次交付中的出现记录。"""

    data: bytes
    channel_id_local: int
    sequence: int
    log_time: int
    publish_time: int
    record_offset: int


@dataclass
class _LightweightMessage:
    """韧性解析路径下模拟 MCAP Message 记录的最小结构。"""

    data: bytes
    log_time: int
    publish_time: int
    sequence: int


@dataclass
class ParseOutcome:
    """单份资料的韧性解析结果。"""

    occurrences: list = field(default_factory=list)
    # 每项：(channel_id_local, topic, message_encoding, metadata, schema_id_local, schema)
    channels: list = field(default_factory=list)
    # (scope, byte_start, byte_end, log_start, log_end, error_type, text)
    failures: list = field(default_factory=list)
    has_chunk_index: bool = False
    valid_magic: bool = True


def _extract_json(data: bytes):
    """解析 JSON 消息体；失败时返回 (None, 错误说明)，不吞掉原始字节。"""
    try:
        return json.loads(data.decode("utf-8")), None
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        return None, f"{type(exc).__name__}: {exc}"


def parse_mcap(blob: bytes) -> ParseOutcome:
    """顺序解析 MCAP 字节，按 chunk 隔离解析失败。

    顶层（chunk 外）与每个 chunk 内部各自维护局部 schema/channel 映射，模拟容器局部
    编号的作用域；坏 chunk 只登记失败范围并跳过，不影响其它 chunk。
    """
    outcome = ParseOutcome()
    stream = ReadDataStream(io.BytesIO(blob))
    try:
        read_magic(stream)
    except (McapError, struct.error, EOFError) as exc:
        outcome.valid_magic = False
        text = str(exc) or "文件过短或缺少 MCAP 魔数"
        outcome.failures.append(("file", None, None, None, None, type(exc).__name__, text))
        return outcome

    top_schemas: dict[int, Schema] = {}
    top_channels: dict[int, Channel] = {}
    footer_seen = False
    while not footer_seen:
        record_start = stream.count
        try:
            opcode = stream.read1()
            length = stream.read8()
            record_end = stream.count + length
            if opcode == Opcode.SCHEMA:
                record = Schema.read(stream)
                top_schemas[record.id] = record
            elif opcode == Opcode.CHANNEL:
                record = Channel.read(stream)
                top_channels[record.id] = record
            elif opcode == Opcode.MESSAGE:
                record = Message.read(stream, length)
                outcome.occurrences.append(
                    Occurrence(
                        data=record.data,
                        channel_id_local=record.channel_id,
                        sequence=record.sequence,
                        log_time=record.log_time,
                        publish_time=record.publish_time,
                        record_offset=record_start,
                    )
                )
            elif opcode == Opcode.CHUNK:
                record = Chunk.read(stream)
                # record_end 只覆盖 chunk 载荷字段，记录前缀（opcode+8 字节长度）另计
                chunk_end = record_end + 9
                _consume_chunk(record, record_start, chunk_end, top_schemas, top_channels, outcome)
            elif opcode == Opcode.CHUNK_INDEX:
                outcome.has_chunk_index = True
            elif opcode == Opcode.FOOTER:
                footer_seen = True
            # 其它记录类型（Header/DataEnd/Statistics/各类索引等）直接跳过
            remaining = record_end - stream.count
            if remaining > 0:
                stream.read(remaining)
        except Exception as exc:
            # 流被截断或记录无法解析（含底层解码库错误）：登记文件级局部失败后停止扫描
            outcome.failures.append(
                ("file", record_start, stream.count, None, None,
                 type(exc).__name__, str(exc))
            )
            break
    outcome.channels = [
        (
            channel.id,
            channel.topic,
            channel.message_encoding,
            dict(channel.metadata),
            channel.schema_id,
            top_schemas.get(channel.schema_id),
        )
        for channel in top_channels.values()
    ]
    return outcome


def _consume_chunk(chunk, record_start, record_end, top_schemas, top_channels, outcome):
    try:
        inner_records = breakup_chunk(chunk)
    except Exception as exc:
        # 坏 chunk（压缩损坏、CRC 不符、解压库错误等）：保留局部失败范围，跳过整块。
        # chunk 记录自带消息 log_time 边界，查询时可据此排除失败区间。
        outcome.failures.append(
            ("chunk", record_start, record_end,
             chunk.message_start_time, chunk.message_end_time,
             type(exc).__name__, str(exc))
        )
        return

    chunk_schemas: dict[int, Schema] = {}
    chunk_channels: dict[int, Channel] = {}
    chunk_messages: list[Message] = []
    for record in inner_records:
        if isinstance(record, Schema):
            chunk_schemas[record.id] = record
        elif isinstance(record, Channel):
            chunk_channels[record.id] = record
        elif isinstance(record, Message):
            chunk_messages.append(record)

    # 容器允许在每个 chunk 内重复局部 schema/channel 定义：它们补充顶层映射，
    # 同号冲突时以最近一次 chunk 内定义为准，并各自保留为独立 channel 世代
    for local_id, schema in chunk_schemas.items():
        top_schemas[local_id] = schema
    for local_id, channel in chunk_channels.items():
        top_channels[local_id] = channel

    for channel in chunk_channels.values():
        schema = chunk_schemas.get(channel.schema_id) or top_schemas.get(channel.schema_id)
        outcome.channels.append(
            (
                channel.id,
                channel.topic,
                channel.message_encoding,
                dict(channel.metadata),
                channel.schema_id,
                schema,
            )
        )

    for record in chunk_messages:
        channel = chunk_channels.get(record.channel_id) or top_channels.get(record.channel_id)
        if channel is None:
            outcome.failures.append(
                (
                    "message",
                    record_start,
                    record_end,
                    None,
                    None,
                    "UnknownChannel",
                    f"chunk 内消息引用了未知的局部 channel id {record.channel_id}",
                )
            )
            continue
        outcome.occurrences.append(
            Occurrence(
                data=record.data,
                channel_id_local=channel.id,
                sequence=record.sequence,
                log_time=record.log_time,
                publish_time=record.publish_time,
                record_offset=record_start,
            )
        )


def _classify(schema: Schema | None) -> str:
    """按 schema 名称区分观测与载荷；无 schema 或名称不可识别时归为 unknown。"""
    if schema is None:
        return _UNKNOWN_KIND
    name = schema.name.lower()
    if name == "observation":
        return _OBSERVATION_KIND
    if name == "payload":
        return _PAYLOAD_KIND
    return _UNKNOWN_KIND


class Store:
    """采集资料库：内容寻址原始归档 + SQLite 关系索引。"""

    def __init__(self, root):
        self.root = Path(root)
        self.raw_dir = self.root / "raw"
        self.db_path = self.root / "index.sqlite3"
        self.root.mkdir(parents=True, exist_ok=True)
        self.raw_dir.mkdir(parents=True, exist_ok=True)
        self.connection = sqlite3.connect(self.db_path)
        self.connection.row_factory = sqlite3.Row
        self.connection.execute("PRAGMA foreign_keys = ON")
        self._create_schema()

    def close(self):
        self.connection.close()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        self.close()

    def _create_schema(self):
        self.connection.executescript(
            """
            CREATE TABLE IF NOT EXISTS meta (
                key TEXT PRIMARY KEY,
                value TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS sources (
                sha256 TEXT PRIMARY KEY,
                filename TEXT NOT NULL,
                byte_size INTEGER NOT NULL,
                collected_at TEXT NOT NULL,
                has_chunk_index INTEGER NOT NULL,
                status TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS channel_generations (
                generation_id INTEGER PRIMARY KEY AUTOINCREMENT,
                source_sha256 TEXT NOT NULL REFERENCES sources(sha256),
                channel_id_local INTEGER NOT NULL,
                topic TEXT NOT NULL,
                message_encoding TEXT NOT NULL,
                channel_metadata_json TEXT NOT NULL,
                schema_id_local INTEGER,
                schema_name TEXT,
                schema_encoding TEXT,
                schema_data TEXT,
                UNIQUE(source_sha256, channel_id_local)
            );
            CREATE TABLE IF NOT EXISTS messages (
                body_sha256 TEXT PRIMARY KEY,
                kind TEXT NOT NULL,
                topic TEXT NOT NULL,
                boot TEXT,
                sensor TEXT,
                sample_time INTEGER,
                payload_ref TEXT,
                payload_id TEXT,
                json_valid INTEGER NOT NULL,
                body_bytes INTEGER NOT NULL,
                first_seen_sha256 TEXT NOT NULL REFERENCES sources(sha256)
            );
            CREATE TABLE IF NOT EXISTS message_sources (
                body_sha256 TEXT NOT NULL REFERENCES messages(body_sha256),
                source_sha256 TEXT NOT NULL REFERENCES sources(sha256),
                generation_id INTEGER NOT NULL REFERENCES channel_generations(generation_id),
                sequence INTEGER NOT NULL,
                log_time INTEGER NOT NULL,
                publish_time INTEGER NOT NULL,
                PRIMARY KEY (body_sha256, source_sha256, sequence, log_time, publish_time)
            );
            CREATE TABLE IF NOT EXISTS failures (
                failure_id INTEGER PRIMARY KEY AUTOINCREMENT,
                source_sha256 TEXT NOT NULL REFERENCES sources(sha256),
                scope TEXT NOT NULL,
                byte_start INTEGER,
                byte_end INTEGER,
                message_start_time INTEGER,
                message_end_time INTEGER,
                error_type TEXT NOT NULL,
                message TEXT NOT NULL
            );
            CREATE INDEX IF NOT EXISTS idx_messages_boot_sample ON messages(boot, sample_time);
            CREATE INDEX IF NOT EXISTS idx_messages_topic ON messages(topic);
            CREATE INDEX IF NOT EXISTS idx_messages_ref ON messages(boot, payload_ref);
            CREATE INDEX IF NOT EXISTS idx_messages_payload_id ON messages(boot, payload_id);
            CREATE INDEX IF NOT EXISTS idx_sources_log_time ON message_sources(log_time);
            """
        )
        self.connection.execute(
            "INSERT OR IGNORE INTO meta(key, value) VALUES('schema_version', ?)",
            (str(SCHEMA_VERSION),),
        )
        self.connection.commit()

    # ------------------------------------------------------------------ 采集

    def collect_file(self, path) -> dict:
        """采集一份 MCAP：归档原始字节并把可解析部分写入索引。

        返回采集报告。内容完全相同的重复文件只登记一次；坏 chunk 记为局部失败，
        输入无法识别时抛出 :class:`InvalidSource` 且不写归档、不写索引。
        """
        path = Path(path)
        blob = path.read_bytes()
        digest = hashlib.sha256(blob).hexdigest()

        existing = self.connection.execute(
            "SELECT status FROM sources WHERE sha256 = ?", (digest,)
        ).fetchone()
        if existing is not None:
            return {
                "source": path.name,
                "sha256": digest,
                "duplicate": True,
                "status": existing["status"],
                "new_messages": 0,
                "new_occurrences": 0,
                "failures": self._failures_for(digest),
            }

        outcome = parse_mcap(blob)
        if not outcome.valid_magic:
            failure = outcome.failures[0]
            # 元组：(scope, byte_start, byte_end, log_start, log_end, error_type, text)
            raise InvalidSource(f"{path.name} 不是可识别的 MCAP 资料：{failure[6]}")

        # 解析通过（可能带局部失败）后才落原始归档，文件名不参与内容身份
        archived = archive_file(path, self.raw_dir)
        status = "partial" if outcome.failures else "ok"
        report = self._index_outcome(digest, path.name, len(blob), outcome, status)
        report["source"] = path.name
        report["sha256"] = digest
        report["archive"] = archived["object"]
        return report

    def _index_outcome(self, digest, filename, byte_size, outcome: ParseOutcome, status: str) -> dict:
        now = datetime.now(timezone.utc).isoformat()
        new_messages = 0
        new_occurrences = 0
        json_failures = 0
        with self.connection:
            self.connection.execute(
                "INSERT INTO sources(sha256, filename, byte_size, collected_at, has_chunk_index, status)"
                " VALUES(?,?,?,?,?,?)",
                (digest, filename, byte_size, now, int(outcome.has_chunk_index), status),
            )
            generation_ids = self._insert_generations(digest, outcome.channels)
            channel_lookup = {channel[0]: channel for channel in outcome.channels}
            for occurrence in outcome.occurrences:
                channel_info = channel_lookup.get(occurrence.channel_id_local)
                if channel_info is None:
                    continue
                body_digest = hashlib.sha256(occurrence.data).hexdigest()
                parsed, parse_error = _extract_json(occurrence.data)
                if parse_error is not None:
                    json_failures += 1
                    self._insert_failure(
                        digest,
                        "message",
                        occurrence.record_offset,
                        None,
                        None,
                        None,
                        "InvalidJson",
                        parse_error,
                    )
                inserted = self._insert_message(
                    body_digest, digest, occurrence, channel_info, parsed, parse_error
                )
                if inserted:
                    new_messages += 1
                generation_id = generation_ids[occurrence.channel_id_local]
                cursor = self.connection.execute(
                    "INSERT OR IGNORE INTO message_sources"
                    "(body_sha256, source_sha256, generation_id, sequence, log_time, publish_time)"
                    " VALUES(?,?,?,?,?,?)",
                    (
                        body_digest,
                        digest,
                        generation_id,
                        occurrence.sequence,
                        occurrence.log_time,
                        occurrence.publish_time,
                    ),
                )
                new_occurrences += cursor.rowcount
            for scope, start, end, log_start, log_end, error_type, text in outcome.failures:
                self._insert_failure(digest, scope, start, end, log_start, log_end,
                                     error_type, text)
        return {
            "duplicate": False,
            "status": status,
            "new_messages": new_messages,
            "new_occurrences": new_occurrences,
            "json_failures": json_failures,
            "failures": self._failures_for(digest),
        }

    def _insert_generations(self, digest, channels):
        generation_ids = {}
        for local_id, topic, message_encoding, metadata, schema_id_local, schema in channels:
            self.connection.execute(
                "INSERT INTO channel_generations"
                "(source_sha256, channel_id_local, topic, message_encoding, channel_metadata_json,"
                " schema_id_local, schema_name, schema_encoding, schema_data)"
                " VALUES(?,?,?,?,?,?,?,?,?)",
                (
                    digest,
                    local_id,
                    topic,
                    message_encoding,
                    json.dumps(metadata, ensure_ascii=False, sort_keys=True),
                    schema_id_local,
                    schema.name if schema else None,
                    schema.encoding if schema else None,
                    schema.data.decode("utf-8") if schema and schema.data else None,
                ),
            )
            row = self.connection.execute(
                "SELECT generation_id FROM channel_generations"
                " WHERE source_sha256 = ? AND channel_id_local = ?",
                (digest, local_id),
            ).fetchone()
            generation_ids[local_id] = row["generation_id"]
        return generation_ids

    def _insert_message(self, body_digest, source_digest, occurrence, channel_info, parsed, parse_error):
        topic = channel_info[1]
        schema = channel_info[5]
        kind = _classify(schema)
        if parse_error is not None or not isinstance(parsed, dict):
            boot = sensor = sample_time = payload_ref = payload_id = None
            json_valid = 0
        else:
            json_valid = 1
            boot = parsed.get("boot")
            sample_time = parsed.get("sample_time")
            payload_ref = parsed.get("payload_ref")
            payload_id = parsed.get("observation_id")
            sensor = parsed.get("sensor") or channel_info[3].get("sensor")
        cursor = self.connection.execute(
            "INSERT OR IGNORE INTO messages"
            "(body_sha256, kind, topic, boot, sensor, sample_time, payload_ref, payload_id,"
            " json_valid, body_bytes, first_seen_sha256) VALUES(?,?,?,?,?,?,?,?,?,?,?)",
            (
                body_digest,
                kind,
                topic,
                boot,
                sensor,
                sample_time,
                payload_ref,
                payload_id,
                json_valid,
                len(occurrence.data),
                source_digest,
            ),
        )
        return cursor.rowcount == 1

    def _insert_failure(self, digest, scope, start, end, log_start, log_end, error_type, text):
        self.connection.execute(
            "INSERT INTO failures(source_sha256, scope, byte_start, byte_end,"
            " message_start_time, message_end_time, error_type, message)"
            " VALUES(?,?,?,?,?,?,?,?)",
            (digest, scope, start, end, log_start, log_end, error_type, text),
        )

    def _failures_for(self, digest):
        rows = self.connection.execute(
            "SELECT scope, byte_start, byte_end, message_start_time, message_end_time,"
            " error_type, message FROM failures"
            " WHERE source_sha256 = ? ORDER BY failure_id",
            (digest,),
        ).fetchall()
        return [dict(row) for row in rows]

    # ------------------------------------------------------------------ 清单

    def manifest(self) -> dict:
        sources = [
            dict(row)
            for row in self.connection.execute(
                "SELECT sha256, filename, byte_size, collected_at, has_chunk_index, status"
                " FROM sources ORDER BY collected_at"
            ).fetchall()
        ]
        for source in sources:
            digest = source["sha256"]
            source["message_occurrences"] = self.connection.execute(
                "SELECT COUNT(*) FROM message_sources WHERE source_sha256 = ?", (digest,)
            ).fetchone()[0]
            source["failures"] = self._failures_for(digest)
        generations = [
            dict(row)
            for row in self.connection.execute(
                "SELECT g.source_sha256, s.filename, g.channel_id_local, g.topic, g.message_encoding,"
                " g.channel_metadata_json, g.schema_id_local, g.schema_name, g.schema_encoding, g.schema_data"
                " FROM channel_generations g JOIN sources s ON s.sha256 = g.source_sha256"
                " ORDER BY g.generation_id"
            ).fetchall()
        ]
        totals = self.connection.execute(
            "SELECT COUNT(*) AS messages,"
            " SUM(CASE WHEN kind='observation' THEN 1 ELSE 0 END) AS observations,"
            " SUM(CASE WHEN kind='payload' THEN 1 ELSE 0 END) AS payloads"
            " FROM messages"
        ).fetchone()
        return {
            "schema_version": SCHEMA_VERSION,
            "totals": dict(totals),
            "sources": sources,
            "channel_generations": generations,
        }

    # ------------------------------------------------------ 观测/载荷关联查询

    def observation_snapshot(self) -> list[dict]:
        """返回每条观测引用的关联状态快照，用于补交前后对比。"""
        observations = self.connection.execute(
            "SELECT body_sha256, boot, payload_ref, topic, sensor, sample_time"
            " FROM messages WHERE kind = ? ORDER BY boot, sample_time, body_sha256",
            (_OBSERVATION_KIND,),
        ).fetchall()
        payload_index = {
            (row["boot"], row["payload_id"]): row
            for row in self.connection.execute(
                "SELECT body_sha256, boot, payload_id, topic FROM messages"
                " WHERE kind = ? AND payload_id IS NOT NULL"
            , (_PAYLOAD_KIND,)).fetchall()
        }
        entries = []
        for row in observations:
            ref = row["payload_ref"]
            if ref is None:
                state = "no_reference"
                payload = None
            else:
                payload = payload_index.get((row["boot"], ref))
                if payload is not None:
                    state = "linked"
                else:
                    state = "unlinked"
                    # 引用存在但 boot 对不上：登记为数据质量问题，不强行配对
                    foreign = next(
                        (p for (pboot, pid), p in payload_index.items() if pid == ref and pboot != row["boot"]),
                        None,
                    )
                    if foreign is not None:
                        state = "boot_mismatch"
            entries.append(
                {
                    "observation": row["body_sha256"],
                    "boot": row["boot"],
                    "payload_ref": ref,
                    "state": state,
                    "topic": row["topic"],
                    "sensor": row["sensor"],
                    "sample_time": row["sample_time"],
                    "payload_message": payload["body_sha256"] if payload else None,
                }
            )
        return entries

    @staticmethod
    def diff_snapshots(before: list[dict], after: list[dict]) -> dict:
        """对比两次快照，列出从缺资料变为可用等状态迁移。"""
        def key(entry):
            return entry["observation"]

        before_map = {key(entry): entry for entry in before}
        after_map = {key(entry): entry for entry in after}
        added = [entry for k, entry in after_map.items() if k not in before_map]
        removed = [entry for k, entry in before_map.items() if k not in after_map]
        changed = []
        for k, current in after_map.items():
            previous = before_map.get(k)
            if previous is not None and previous["state"] != current["state"]:
                changed.append(
                    {
                        "observation": k,
                        "boot": current["boot"],
                        "payload_ref": current["payload_ref"],
                        "from_state": previous["state"],
                        "to_state": current["state"],
                    }
                )
        linked_now = [entry for entry in added if entry["state"] == "linked"]
        return {
            "added": added,
            "removed": removed,
            "state_changes": changed,
            "newly_available": linked_now + [c for c in changed if c["to_state"] == "linked"],
        }

    def orphan_payloads(self) -> list[dict]:
        """已到达但没有任何同 boot 观测引用的载荷（载荷先到/无主载荷）。"""
        rows = self.connection.execute(
            "SELECT p.body_sha256, p.boot, p.payload_id, p.topic, p.sample_time"
            " FROM messages p WHERE p.kind = ? AND p.payload_id IS NOT NULL"
            " AND NOT EXISTS (SELECT 1 FROM messages o WHERE o.kind = ?"
            " AND o.boot = p.boot AND o.payload_ref = p.payload_id)"
            " ORDER BY p.boot, p.sample_time",
            (_PAYLOAD_KIND, _OBSERVATION_KIND),
        ).fetchall()
        return [dict(row) for row in rows]

    def query_observations(self, boot=None, state=None) -> list[dict]:
        entries = self.observation_snapshot()
        if boot is not None:
            entries = [entry for entry in entries if entry["boot"] == boot]
        if state is not None:
            entries = [entry for entry in entries if entry["state"] == state]
        return entries

    # ------------------------------------------------------------ 索引化读取

    def read_messages(self, boot=None, start=None, end=None, time_basis="sample_time", topics=None):
        """按启动周期与时间段读取消息，优先走索引而不是展开所有载荷。

        先用 SQLite 索引定位候选消息所在的来源文件与 log_time 边界，再对有 chunk 索引
        的 MCAP 用 SeekingReader 只解压相关 chunk；sample_time 过滤在 SQLite 侧完成。
        带失败记录的来源会在结果里标注局部失败范围。
        """
        if time_basis not in {"sample_time", "log_time", "publish_time"}:
            raise ValueError("time_basis 必须是 sample_time、log_time 或 publish_time")

        where, params = [], []
        if boot is not None:
            where.append("m.boot = ?")
            params.append(boot)
        if topics is not None:
            where.append(f"m.topic IN ({','.join('?' * len(topics))})")
            params.extend(topics)
        time_column = {"sample_time": "m.sample_time", "log_time": "ms.log_time",
                       "publish_time": "ms.publish_time"}[time_basis]
        if start is not None:
            where.append(f"{time_column} >= ?")
            params.append(start)
        if end is not None:
            where.append(f"{time_column} < ?")
            params.append(end)
        where_sql = (" WHERE " + " AND ".join(where)) if where else ""

        candidates = self.connection.execute(
            "SELECT m.body_sha256, m.boot, m.sensor, m.sample_time, m.payload_ref, m.payload_id,"
            " m.kind, m.topic, m.json_valid, ms.source_sha256, ms.log_time, ms.publish_time,"
            " ms.sequence, g.channel_metadata_json, g.schema_name, g.message_encoding"
            " FROM messages m JOIN message_sources ms ON ms.body_sha256 = m.body_sha256"
            " JOIN channel_generations g ON g.generation_id = ms.generation_id"
            + where_sql
            + " ORDER BY ms.log_time, m.body_sha256",
            params,
        ).fetchall()

        # 按来源分组，推导每个来源需要读取的 log_time 窗口
        per_source: dict[str, dict] = {}
        for row in candidates:
            bucket = per_source.setdefault(
                row["source_sha256"],
                {"rows": [], "log_min": row["log_time"], "log_max": row["log_time"], "topics": set()},
            )
            bucket["rows"].append(row)
            bucket["log_min"] = min(bucket["log_min"], row["log_time"])
            bucket["log_max"] = max(bucket["log_max"], row["log_time"])
            bucket["topics"].add(row["topic"])

        messages, partial_sources, emitted = [], [], set()
        for source_digest, bucket in per_source.items():
            failures = self._failures_for(source_digest)
            if failures:
                partial_sources.append({"sha256": source_digest, "failures": failures})
            wanted = {row["body_sha256"]: row for row in bucket["rows"]}
            excluded_windows = [
                (failure["message_start_time"], failure["message_end_time"])
                for failure in failures
                if failure["scope"] == "chunk"
                and failure["message_start_time"] is not None
            ]
            for occurrence in self._read_source_window(
                source_digest,
                topics=sorted(bucket["topics"]),
                start_time=bucket["log_min"] if time_basis != "log_time" or start is None else start,
                end_time=(bucket["log_max"] + 1) if time_basis != "log_time" or end is None else end,
                resilient=bool(failures),
                excluded_windows=excluded_windows,
            ):
                schema, channel, message = occurrence
                body_digest = hashlib.sha256(message.data).hexdigest()
                row = wanted.get(body_digest)
                if row is None or body_digest in emitted:
                    # chunk 级裁剪可能带进窗口内的其它消息；同一消息在多份资料出现只输出一次
                    continue
                emitted.add(body_digest)
                parsed, _ = _extract_json(message.data)
                messages.append(
                    {
                        "body_sha256": body_digest,
                        "source_sha256": source_digest,
                        "topic": row["topic"],
                        "schema_name": row["schema_name"],
                        "message_encoding": row["message_encoding"],
                        "boot": row["boot"],
                        "sensor": row["sensor"],
                        "sample_time": row["sample_time"],
                        "log_time": message.log_time,
                        "publish_time": message.publish_time,
                        "sequence": message.sequence,
                        "payload_ref": row["payload_ref"],
                        "payload_id": row["payload_id"],
                        "json_valid": bool(row["json_valid"]),
                        "data": parsed,
                    }
                )
        return {"messages": messages, "partial_sources": partial_sources}

    def _read_source_window(self, source_digest, topics, start_time, end_time,
                            resilient=False, excluded_windows=None):
        from mcap.reader import make_reader

        object_path = self.raw_dir / source_digest
        if not object_path.exists():
            raise FileNotFoundError(f"归档对象缺失：{source_digest}")
        blob = object_path.read_bytes()
        if resilient:
            # 已知带坏 chunk 的来源：走韧性解析，坏 chunk 已在采集阶段登记为局部失败，
            # 这里跳过它们并按 log_time 窗口、topic、失败时间区间过滤，
            # 避免单个坏 chunk 让整个查询失败，也不会把坏区间内的消息当作有效结果
            excluded_windows = excluded_windows or []
            outcome = parse_mcap(blob)
            channel_topics = {channel[0]: channel[1] for channel in outcome.channels}
            for occurrence in outcome.occurrences:
                if occurrence.log_time < start_time or occurrence.log_time >= end_time:
                    continue
                if any(
                    window_start <= occurrence.log_time <= window_end
                    for window_start, window_end in excluded_windows
                ):
                    continue
                topic = channel_topics.get(occurrence.channel_id_local)
                if topics is not None and topic not in topics:
                    continue
                yield None, None, _LightweightMessage(
                    data=occurrence.data,
                    log_time=occurrence.log_time,
                    publish_time=occurrence.publish_time,
                    sequence=occurrence.sequence,
                )
            return
        with object_path.open("rb") as stream:
            reader = make_reader(stream)
            yield from reader.iter_messages(
                topics=topics, start_time=start_time, end_time=end_time
            )

    # ------------------------------------------------------------- 汇总状态

    def status_summary(self) -> dict:
        snapshot = self.observation_snapshot()
        counts: dict[str, int] = {}
        for entry in snapshot:
            counts[entry["state"]] = counts.get(entry["state"], 0) + 1
        source_rows = self.connection.execute(
            "SELECT status, COUNT(*) AS n FROM sources GROUP BY status"
        ).fetchall()
        return {
            "sources": {row["status"]: row["n"] for row in source_rows},
            "observation_states": counts,
            "orphan_payloads": len(self.orphan_payloads()),
        }
