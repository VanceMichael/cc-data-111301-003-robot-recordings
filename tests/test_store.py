"""跨记录采集、关联、去重、局部失败与索引化读取的测试。"""

from __future__ import annotations

import hashlib
import json
import tempfile
import unittest
from pathlib import Path

from tests.fixture_builder import (
    OBSERVATION_SCHEMA_V1,
    OBSERVATION_SCHEMA_V2,
    PAYLOAD_SCHEMA,
    build_corrupt_chunk,
    observation,
    payload,
    write_mcap,
)
from robot_recordings.store import (
    EXIT_PARTIAL,
    InvalidSource,
    Store,
)


class StoreTestBase(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)
        self.store_dir = self.root / "store"
        self.store = Store(self.store_dir)

    def tearDown(self):
        self.store.close()
        self.temp.cleanup()

    def collect_text(self, name, groups, **kwargs):
        path = self.root / name
        write_mcap(path, groups, **kwargs)
        return self.store.collect_file(path)


def camera_channel(schema_name, schema_data, topic="/camera/objects", metadata=None):
    return (
        schema_name,
        schema_data,
        topic,
        metadata or {"sensor": "camera-a", "unit": "pixel"},
    )


class AssociationTests(StoreTestBase):
    def test_payload_arrives_after_observation(self):
        ref = "boot-a:payload:10"
        self.collect_text(
            "obs.mcap",
            [(*camera_channel("Observation", OBSERVATION_SCHEMA_V1),
              [{"log_time": 1000, "publish_time": 900,
                "data": observation("boot-a", 10, 100, ref)}])],
        )
        pending = self.store.query_observations(boot="boot-a")
        self.assertEqual(pending[0]["state"], "unlinked")
        self.assertEqual(pending[0]["payload_ref"], ref)
        self.assertEqual(self.store.status_summary()["orphan_payloads"], 0)

        self.collect_text(
            "payload.mcap",
            [("Payload", PAYLOAD_SCHEMA, "/camera/payload", {"sensor": "camera-a"},
              [{"log_time": 2000, "publish_time": 1900,
                "data": payload("boot-a", ref, 100)}])],
        )
        linked = self.store.query_observations(state="linked")
        self.assertEqual(len(linked), 1)
        self.assertEqual(linked[0]["boot"], "boot-a")
        self.assertIsNotNone(linked[0]["payload_message"])
        self.assertEqual(self.store.orphan_payloads(), [])

    def test_payload_arrives_before_observation_is_orphan_then_linked(self):
        ref = "boot-a:payload:20"
        report = self.collect_text(
            "payload-first.mcap",
            [("Payload", PAYLOAD_SCHEMA, "/camera/payload", {"sensor": "camera-a"},
              [{"log_time": 800, "publish_time": 700,
                "data": payload("boot-a", ref, 200)}])],
        )
        self.assertEqual(report["status"], "ok")
        orphans = self.store.orphan_payloads()
        self.assertEqual(len(orphans), 1)
        self.assertEqual(orphans[0]["payload_id"], ref)

        self.collect_text(
            "obs-later.mcap",
            [(*camera_channel("Observation", OBSERVATION_SCHEMA_V1),
              [{"log_time": 3000, "publish_time": 2900,
                "data": observation("boot-a", 20, 200, ref)}])],
        )
        self.assertEqual(self.store.query_observations(state="linked")[0]["payload_ref"], ref)
        self.assertEqual(self.store.orphan_payloads(), [])

    def test_missing_payload_stays_unlinked_and_no_reference_distinct(self):
        self.collect_text(
            "missing.mcap",
            [(*camera_channel("Observation", OBSERVATION_SCHEMA_V1), [
                {"log_time": 1000, "publish_time": 900,
                 "data": observation("boot-a", 1, 10, "boot-a:payload:missing")},
                {"log_time": 1001, "publish_time": 901,
                 "data": observation("boot-b", 1, 10, None)},
            ])],
        )
        states = {entry["state"] for entry in self.store.observation_snapshot()}
        self.assertEqual(states, {"unlinked", "no_reference"})

    def test_reference_scoped_by_boot_never_paired_across_boots(self):
        ref = "shared-ref"
        self.collect_text(
            "a.mcap",
            [(*camera_channel("Observation", OBSERVATION_SCHEMA_V1),
              [{"log_time": 1000, "publish_time": 900,
                "data": observation("boot-a", 1, 10, ref)}]),
             ("Payload", PAYLOAD_SCHEMA, "/camera/payload", {"sensor": "camera-a"},
              [{"log_time": 1100, "publish_time": 1000,
                "data": payload("boot-b", ref, 10)}])],
        )
        entries = self.store.query_observations(boot="boot-a")
        self.assertEqual(entries[0]["state"], "boot_mismatch")
        self.assertIsNone(entries[0]["payload_message"])
        self.assertEqual(len(self.store.orphan_payloads()), 1)


class DuplicateAndBackfillTests(StoreTestBase):
    def test_recollect_same_file_is_duplicate_no_double_count(self):
        ref = "boot-a:payload:10"
        self.collect_text(
            "part.mcap",
            [(*camera_channel("Observation", OBSERVATION_SCHEMA_V1),
              [{"log_time": 1000, "publish_time": 900,
                "data": observation("boot-a", 10, 100, ref)}])],
        )
        again = self.collect_text(
            "part-copy.mcap",
            [(*camera_channel("Observation", OBSERVATION_SCHEMA_V1),
              [{"log_time": 1000, "publish_time": 900,
                "data": observation("boot-a", 10, 100, ref)}])],
        )
        self.assertTrue(again["duplicate"])
        self.assertEqual(again["new_messages"], 0)
        totals = self.store.manifest()["totals"]
        self.assertEqual(totals["messages"], 1)
        self.assertEqual(len(list(self.store.raw_dir.iterdir())), 1)

    def test_overlapping_supplement_dedups_and_backfills_link(self):
        ref = "boot-a:payload:30"
        shared = observation("boot-a", 30, 300, ref)
        # 原始片段：一条观测
        self.collect_text(
            "orig.mcap",
            [(*camera_channel("Observation", OBSERVATION_SCHEMA_V1),
              [{"log_time": 1000, "publish_time": 900, "data": shared}])],
        )
        before = self.store.observation_snapshot()
        self.assertEqual(before[0]["state"], "unlinked")
        # 补交片段包含同一条观测（重复字节）加缺失载荷
        report = self.collect_text(
            "supplement.mcap",
            [(*camera_channel("Observation", OBSERVATION_SCHEMA_V1),
              [{"log_time": 1000, "publish_time": 900, "data": shared}]),
             ("Payload", PAYLOAD_SCHEMA, "/camera/payload", {"sensor": "camera-a"},
              [{"log_time": 2000, "publish_time": 1900,
                "data": payload("boot-a", ref, 300)}])],
        )
        self.assertFalse(report["duplicate"])
        # 消息身份只有载荷是新的；重复观测不产生新消息
        self.assertEqual(report["new_messages"], 1)
        # 出处行：重复观测在新来源里的交付出处 + 载荷出处，共两条
        self.assertEqual(report["new_occurrences"], 2)
        # 同一观测仍只保留一条消息身份
        self.assertEqual(
            self.store.connection.execute(
                "SELECT COUNT(*) FROM messages WHERE kind = 'observation'"
            ).fetchone()[0],
            1,
        )
        # 观测的两次交付出处、载荷的一次出处都保留
        self.assertEqual(
            self.store.connection.execute(
                "SELECT COUNT(*) FROM message_sources"
            ).fetchone()[0],
            3,
        )
        # 同一观测在两个来源里各有一条出处，载荷只有一处
        self.assertEqual(
            self.store.connection.execute(
                "SELECT COUNT(*) FROM message_sources"
                " WHERE body_sha256 = ?",
                (hashlib.sha256(shared).hexdigest(),),
            ).fetchone()[0],
            2,
        )
        after = self.store.observation_snapshot()
        diff = self.store.diff_snapshots(before, after)
        self.assertEqual(diff["state_changes"][0]["from_state"], "unlinked")
        self.assertEqual(diff["state_changes"][0]["to_state"], "linked")
        self.assertEqual(len(diff["newly_available"]), 1)
        self.assertEqual(len(self.store.query_observations(state="linked")), 1)


class ChannelGenerationTests(StoreTestBase):
    def test_same_topic_different_schema_kept_as_generations(self):
        topic = "/camera/objects"
        self.collect_text(
            "gen1.mcap",
            [(*camera_channel("Observation", OBSERVATION_SCHEMA_V1, topic,
                              {"sensor": "camera-a", "unit": "pixel"}),
              [{"log_time": 1000, "publish_time": 900,
                "data": observation("boot-a", 1, 10, None)}])],
        )
        # 下一个启动周期：同名 topic 换了 schema（sample 由 integer 变 number），单位也变了
        self.collect_text(
            "gen2.mcap",
            [("Observation", OBSERVATION_SCHEMA_V2, topic,
              {"sensor": "camera-a", "unit": "mm"},
              [{"log_time": 5000, "publish_time": 4900,
                "data": observation("boot-c", 2, 20, None)}])],
        )
        manifest = self.store.manifest()
        generations = manifest["channel_generations"]
        self.assertEqual(len(generations), 2)
        self.assertEqual({g["schema_data"] for g in generations},
                         {OBSERVATION_SCHEMA_V1.decode(), OBSERVATION_SCHEMA_V2.decode()})
        units = {json.loads(g["channel_metadata_json"])["unit"] for g in generations}
        self.assertEqual(units, {"pixel", "mm"})
        # 容器局部 id 在两个文件里都叫 1，但世代记录各自独立
        self.assertEqual({g["channel_id_local"] for g in generations}, {1})
        self.assertEqual({g["source_sha256"] for g in generations}.__len__(), 2)

    def test_local_channel_id_reused_for_different_topics(self):
        self.collect_text(
            "multi.mcap",
            [("Observation", OBSERVATION_SCHEMA_V1, "/camera/a", {"sensor": "a"},
              [{"log_time": 1000, "publish_time": 900,
                "data": observation("boot-a", 1, 10, None)}]),
             ("Payload", PAYLOAD_SCHEMA, "/camera/p", {"sensor": "b"},
              [{"log_time": 1001, "publish_time": 901,
                "data": payload("boot-a", "boot-a:payload:1", 10)}])],
        )
        manifest = self.store.manifest()
        topics = {g["topic"] for g in manifest["channel_generations"]}
        self.assertEqual(topics, {"/camera/a", "/camera/p"})


class PartialFailureTests(StoreTestBase):
    def test_bad_chunk_keeps_local_failure_scope_and_partial_status(self):
        path = self.root / "corrupt.mcap"
        good_range, bad_byte_range, bad_time_range = build_corrupt_chunk(path)
        report = self.store.collect_file(path)
        self.assertEqual(report["status"], "partial")
        self.assertEqual(len(report["failures"]), 1)
        failure = report["failures"][0]
        self.assertEqual(failure["scope"], "chunk")
        self.assertEqual(failure["byte_start"], bad_byte_range[0])
        self.assertEqual(failure["byte_end"], bad_byte_range[1])
        self.assertEqual(failure["message_start_time"], bad_time_range[0])
        self.assertEqual(failure["message_end_time"], bad_time_range[1])
        # 好 chunk 的消息仍然入索引；坏 chunk 时间区间内的消息不返回
        result = self.store.read_messages()
        returned_times = {m["log_time"] for m in result["messages"]}
        self.assertIn(good_range[0], returned_times)
        self.assertFalse(
            any(bad_time_range[0] <= t <= bad_time_range[1] for t in returned_times)
        )
        self.assertGreaterEqual(len(returned_times), 2)  # 坏 chunk 前后各有好 chunk
        # 清单不能声称整趟完整
        source = self.store.manifest()["sources"][0]
        self.assertEqual(source["status"], "partial")
        self.assertEqual(len(source["failures"]), 1)

    def test_invalid_input_rejected_without_archive_or_index(self):
        bad = self.root / "invalid.mcap"
        bad.write_bytes(b"not a valid source at all")
        with self.assertRaises(InvalidSource):
            self.store.collect_file(bad)
        self.assertEqual(list(self.store.raw_dir.iterdir()), [])
        self.assertEqual(self.store.manifest()["sources"], [])

    def test_partial_source_flagged_on_indexed_read(self):
        path = self.root / "corrupt.mcap"
        build_corrupt_chunk(path)
        self.store.collect_file(path)
        result = self.store.read_messages()
        self.assertEqual(len(result["partial_sources"]), 1)
        self.assertEqual(result["partial_sources"][0]["failures"][0]["scope"], "chunk")


class IndexedReadTests(StoreTestBase):
    def _populate_two_boots(self):
        groups = [
            (*camera_channel("Observation", OBSERVATION_SCHEMA_V1), [
                {"log_time": 1000 + i * 100, "publish_time": 900 + i * 100,
                 "data": observation("boot-a", i, i * 10, None)}
                for i in range(4)
            ]),
            ("Payload", PAYLOAD_SCHEMA, "/camera/payload", {"sensor": "camera-a"}, [
                {"log_time": 5000, "publish_time": 4900,
                 "data": payload("boot-b", "boot-b:payload:1", 70)}
            ]),
        ]
        self.collect_text("indexed.mcap", groups, chunk_size=120)

    def test_filter_by_boot_and_sample_time_window(self):
        self._populate_two_boots()
        result = self.store.read_messages(boot="boot-a", start=10, end=30)
        self.assertEqual([m["sample_time"] for m in result["messages"]], [10, 20])
        self.assertTrue(all(m["boot"] == "boot-a" for m in result["messages"]))

    def test_time_bases_are_distinct_columns(self):
        self._populate_two_boots()
        by_sample = self.store.read_messages(boot="boot-a", start=20, end=40,
                                             time_basis="sample_time")
        self.assertEqual([m["sample_time"] for m in by_sample["messages"]], [20, 30])
        by_log = self.store.read_messages(boot="boot-a", start=1100, end=1300,
                                          time_basis="log_time")
        self.assertEqual([m["log_time"] for m in by_log["messages"]], [1100, 1200])
        by_publish = self.store.read_messages(boot="boot-a", start=1000, end=1200,
                                              time_basis="publish_time")
        self.assertEqual([m["publish_time"] for m in by_publish["messages"]], [1000, 1100])

    def test_three_clock_identities_preserved_on_read(self):
        self._populate_two_boots()
        message = self.store.read_messages(boot="boot-a")["messages"][0]
        self.assertEqual(message["sample_time"], 0)
        self.assertEqual(message["log_time"], 1000)
        self.assertEqual(message["publish_time"], 900)
        self.assertNotIn(message["sample_time"], (message["log_time"], message["publish_time"]))

    def test_topic_filter_and_indexed_file_prunes_chunks(self):
        self._populate_two_boots()
        result = self.store.read_messages(topics=["/camera/payload"])
        self.assertEqual({m["topic"] for m in result["messages"]}, {"/camera/payload"})
        manifest = self.store.manifest()
        self.assertTrue(manifest["sources"][0]["has_chunk_index"])

    def test_unindexed_file_still_readable_sequentially(self):
        self.collect_text(
            "noindex.mcap",
            [(*camera_channel("Observation", OBSERVATION_SCHEMA_V1),
              [{"log_time": 1000, "publish_time": 900,
                "data": observation("boot-a", 1, 10, None)}])],
            indexed=False,
        )
        manifest = self.store.manifest()
        self.assertFalse(manifest["sources"][0]["has_chunk_index"])
        result = self.store.read_messages(boot="boot-a")
        self.assertEqual(len(result["messages"]), 1)

    def test_message_repeated_in_two_sources_emitted_once(self):
        shared = observation("boot-a", 1, 10, None)
        self.collect_text(
            "a.mcap",
            [(*camera_channel("Observation", OBSERVATION_SCHEMA_V1),
              [{"log_time": 1000, "publish_time": 900, "data": shared}])],
        )
        self.collect_text(
            "b.mcap",
            [(*camera_channel("Observation", OBSERVATION_SCHEMA_V1),
              [{"log_time": 1000, "publish_time": 900, "data": shared}]),
             (*camera_channel("Observation", OBSERVATION_SCHEMA_V1),
              [{"log_time": 2000, "publish_time": 1900,
                "data": observation("boot-a", 2, 20, None)}])],
        )
        result = self.store.read_messages(boot="boot-a")
        self.assertEqual(len(result["messages"]), 2)
        # 但两次出处都保留
        rows = self.store.connection.execute(
            "SELECT COUNT(*) FROM message_sources"
        ).fetchone()[0]
        self.assertEqual(rows, 3)


class EncodingEdgeCaseTests(StoreTestBase):
    def test_lz4_compression_and_schemaless_invalid_json(self):
        from mcap.writer import CompressionType, IndexType, Writer

        path = self.root / "mixed.mcap"
        with open(path, "wb") as stream:
            writer = Writer(stream, compression=CompressionType.LZ4,
                            index_types=IndexType.ALL)
            writer.start()
            # schema_id=0 表示无 schema；消息体也不是合法 JSON
            channel_id = writer.register_channel("/raw/bin", "binary", 0,
                                                 {"sensor": "raw"})
            writer.add_message(channel_id, 1000, b"\xff\xfe not json", 900)
            writer.finish()
        report = self.store.collect_file(path)
        self.assertEqual(report["status"], "ok")
        self.assertEqual(report["json_failures"], 1)
        self.assertEqual(report["failures"][0]["scope"], "message")
        self.assertEqual(report["failures"][0]["error_type"], "InvalidJson")

        result = self.store.read_messages()
        message = result["messages"][0]
        self.assertFalse(message["json_valid"])
        self.assertIsNone(message["data"])  # 不吞原始字节，但也不伪造解析结果
        self.assertIsNone(message["schema_name"])
        self.assertEqual(message["message_encoding"], "binary")
        generation = self.store.manifest()["channel_generations"][0]
        self.assertIsNone(generation["schema_data"])
        self.assertEqual(json.loads(generation["channel_metadata_json"])["sensor"], "raw")

    def test_uncompressed_file_is_supported(self):
        from mcap.writer import CompressionType

        self.collect_text(
            "plain.mcap",
            [(*camera_channel("Observation", OBSERVATION_SCHEMA_V1),
              [{"log_time": 1000, "publish_time": 900,
                "data": observation("boot-a", 1, 10, None)}])],
            compression=CompressionType.NONE,
        )
        self.assertEqual(len(self.store.read_messages(boot="boot-a")["messages"]), 1)


class SnapshotDiffTests(StoreTestBase):
    def test_diff_detects_new_observation_and_removal_semantics(self):
        ref = "boot-a:payload:9"
        self.collect_text(
            "obs.mcap",
            [(*camera_channel("Observation", OBSERVATION_SCHEMA_V1),
              [{"log_time": 1000, "publish_time": 900,
                "data": observation("boot-a", 9, 90, ref)}])],
        )
        before = self.store.observation_snapshot()
        self.collect_text(
            "payload.mcap",
            [("Payload", PAYLOAD_SCHEMA, "/camera/payload", {"sensor": "camera-a"},
              [{"log_time": 2000, "publish_time": 1900,
                "data": payload("boot-a", ref, 90)}])],
        )
        after = self.store.observation_snapshot()
        diff = self.store.diff_snapshots(before, after)
        self.assertEqual(diff["added"], [])
        self.assertEqual(diff["removed"], [])
        change = diff["state_changes"][0]
        self.assertEqual(change["from_state"], "unlinked")
        self.assertEqual(change["to_state"], "linked")


if __name__ == "__main__":
    unittest.main()
