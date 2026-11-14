"""测试用 MCAP 资料构造器。

与业务 fixtures 隔离，只在临时目录生成；覆盖多 chunk、带索引、同名 topic 换 schema、
局部坏 chunk 等场景。
"""

from __future__ import annotations

import io
import json

from mcap.writer import CompressionType, IndexType, Writer

OBSERVATION_SCHEMA_V1 = json.dumps(
    {"type": "object", "properties": {"boot": {"type": "string"}, "sample": {"type": "integer"}}},
    ensure_ascii=False,
).encode("utf-8")
OBSERVATION_SCHEMA_V2 = json.dumps(
    {"type": "object", "properties": {"boot": {"type": "string"}, "sample": {"type": "number"},
                                      "sample_time": {"type": "integer"}},
     "required": ["boot", "sample_time"]},
    ensure_ascii=False,
).encode("utf-8")
PAYLOAD_SCHEMA = json.dumps({"type": "object"}, ensure_ascii=False).encode("utf-8")


def write_mcap(path, groups, chunk_size=1024 * 1024, compression=CompressionType.ZSTD,
               indexed=True):
    """按 (schema, channel, messages) 分组写出一个 MCAP。

    groups: [(schema_name, schema_data, topic, channel_metadata, [message_kwargs, ...]), ...]
    message_kwargs: dict(log_time, publish_time, data)
    """
    with open(path, "wb") as stream:
        writer = Writer(
            stream,
            chunk_size=chunk_size,
            compression=compression,
            use_chunking=True,
            use_summary_offsets=indexed,
            use_statistics=indexed,
            index_types=IndexType.ALL if indexed else IndexType(0),
        )
        writer.start()
        channel_ids = []
        for schema_name, schema_data, topic, metadata, _messages in groups:
            schema_id = writer.register_schema(schema_name, "jsonschema", schema_data)
            channel_ids.append(writer.register_channel(topic, "json", schema_id, metadata))
        for channel_id, (_schema_name, _schema_data, _topic, _metadata, messages) in zip(
            channel_ids, groups
        ):
            for index, message in enumerate(messages):
                writer.add_message(
                    channel_id,
                    log_time=message["log_time"],
                    publish_time=message["publish_time"],
                    data=message["data"],
                    sequence=message.get("sequence", index),
                )
        writer.finish()


def observation(boot, sample, sample_time, payload_ref, x=12, label="周转箱"):
    return json.dumps(
        {"boot": boot, "sample": sample, "sample_time": sample_time,
         "payload_ref": payload_ref, "objects": [{"label": label, "x": x}]},
        ensure_ascii=False,
    ).encode("utf-8")


def payload(boot, observation_id, sample_time, width=42):
    return json.dumps(
        {"boot": boot, "observation_id": observation_id, "sensor": "camera-a",
         "sample_time": sample_time, "payload": {"label": "周转箱", "width": width}},
        ensure_ascii=False,
    ).encode("utf-8")


def build_corrupt_chunk(path, chunk_size=150):
    """构造一个含多个 chunk 的文件，再破坏中间 chunk 的压缩载荷。

    返回 (好 chunk 时间区间, 坏 chunk 字节范围, 坏 chunk 时间区间)。
    """
    groups = [(
        "Observation", b"{}", "/t", {"sensor": "s"},
        [
            {"log_time": 1000 + i * 10, "publish_time": 900 + i * 10,
             "data": json.dumps({"boot": "b", "i": i, "pad": "x" * 80}).encode()}
            for i in range(4)
        ],
    )]
    write_mcap(path, groups, chunk_size=chunk_size)
    with open(path, "rb") as stream:
        blob = bytearray(stream.read())
    from mcap.reader import make_reader

    indexes = make_reader(io.BytesIO(bytes(blob))).get_summary().chunk_indexes
    bad = indexes[1]
    # chunk 记录头后才是 zstd 压缩数据（opcode/长度/时间戳/CRC/压缩名/解压长度共 53 字节）
    start = bad.chunk_start_offset + 53
    for position in range(start, min(start + 30, bad.chunk_start_offset + bad.chunk_length)):
        blob[position] ^= 0xA5
    with open(path, "wb") as stream:
        stream.write(bytes(blob))
    good = indexes[0]
    # 解析器记录的失败范围从 chunk 记录首字节（opcode）起算
    return (
        (good.message_start_time, good.message_end_time),
        (bad.chunk_start_offset, bad.chunk_start_offset + 9 + bad.chunk_length),
        (bad.message_start_time, bad.message_end_time),
    )
