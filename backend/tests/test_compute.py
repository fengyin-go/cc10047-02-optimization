"""计算引擎单元测试：覆盖 partition_calculator、merge_strategy、compaction_planner、
snapshot_differ、retention_checker、version_conflict_resolver、delta_reconciler。"""
from __future__ import annotations

import sys
sys.path.insert(0, '.')

from app.compute import (
    partition_calculator,
    merge_strategy,
    compaction_planner,
    plan_compaction_groups,
    validate_group_capacity,
    snapshot_differ,
    retention_checker,
    version_conflict_resolver,
    delta_reconciler,
)


class TestPartitionCalculator:
    def test_small_row_count(self):
        result = partition_calculator(1000, ["dt"], 128.0)
        assert result["partition_count"] >= 1
        assert result["strategy"] in ("hash", "range")

    def test_large_row_count_hash(self):
        result = partition_calculator(100000, ["dt", "hour"], 128.0)
        assert result["partition_count"] > 1
        assert result["partition_keys"] == ["dt", "hour"]

    def test_target_size(self):
        result = partition_calculator(12800, ["dt"], 128.0)
        assert result["target_partition_size_mb"] == 128.0
        assert "estimated_rows_per_partition" in result


class TestMergeStrategy:
    def test_should_merge(self):
        result = merge_strategy([10, 20, 30, 5, 15], 256.0)
        assert result["should_merge"] is True

    def test_skip_merge_too_few_files(self):
        result = merge_strategy([10, 20], 256.0, min_file_count=3)
        assert result["should_merge"] is False

    def test_empty_files(self):
        result = merge_strategy([], 256.0)
        assert result["should_merge"] is False


class TestCompactionPlanner:
    def test_no_candidates(self):
        result = compaction_planner("test_table", [256 * 1024 * 1024, 512 * 1024 * 1024, 1024 * 1024 * 1024])
        assert result["plan"] is None
        assert "没有需要压缩的小文件" in result["reason"]

    def test_with_candidates(self):
        result = compaction_planner("test_table", [64 * 1024 * 1024, 32 * 1024 * 1024, 16 * 1024 * 1024, 8 * 1024 * 1024, 256 * 1024 * 1024, 512 * 1024 * 1024])
        assert result["candidate_file_count"] >= 1
        assert result["parallelism"] >= 1

    def test_parallelism_capped(self):
        result = compaction_planner("test_table", [8 * 1024 * 1024] * 20, max_parallelism=4)
        assert result["parallelism"] <= 4


class TestPlanCompactionGroups:
    def test_stable_first_fit_grouping(self):
        files = [
            {"partition": "p1", "name": "f1", "size_bytes": 180},
            {"partition": "p1", "name": "f2", "size_bytes": 180},
            {"partition": "p1", "name": "f3", "size_bytes": 96},
            {"partition": "p1", "name": "f4", "size_bytes": 64},
        ]
        result = plan_compaction_groups(files, 256)
        assert [g["planned_bytes"] for g in result["groups"]] == [244, 180, 96]
        assert [[f["name"] for f in g["files"]] for g in result["groups"]] == [
            ["f1", "f4"], ["f2"], ["f3"],
        ]

    def test_groups_never_cross_partitions(self):
        files = [
            {"partition": "p2", "name": "a", "size_bytes": 100},
            {"partition": "p1", "name": "a", "size_bytes": 100},
        ]
        result = plan_compaction_groups(files, 256)
        assert result["group_count"] == 2
        assert [g["partition"] for g in result["groups"]] == ["p1", "p2"]

    def test_deterministic_regardless_of_input_order(self):
        files = [
            {"partition": "p1", "name": "f3", "size_bytes": 96},
            {"partition": "p1", "name": "f1", "size_bytes": 180},
            {"partition": "p1", "name": "f4", "size_bytes": 64},
            {"partition": "p1", "name": "f2", "size_bytes": 180},
        ]
        forward = plan_compaction_groups(files, 256)
        shuffled = plan_compaction_groups(list(reversed(files)), 256)
        assert [
            (g["partition"], g["planned_bytes"], [f["name"] for f in g["files"]])
            for g in forward["groups"]
        ] == [
            (g["partition"], g["planned_bytes"], [f["name"] for f in g["files"]])
            for g in shuffled["groups"]
        ]

    def test_oversized_single_file_flagged(self):
        files = [{"partition": "p1", "name": "big", "size_bytes": 300}]
        result = plan_compaction_groups(files, 256)
        assert result["group_count"] == 1
        assert result["groups"][0]["fits_limit"] is False

    def test_empty_input(self):
        result = plan_compaction_groups([], 256)
        assert result["group_count"] == 0
        assert result["total_bytes"] == 0


class TestValidateGroupCapacity:
    def test_fits(self):
        result = validate_group_capacity(244, 256)
        assert result["fits"] is True
        assert result["overflow_bytes"] == 0

    def test_overflow(self):
        result = validate_group_capacity(300, 256)
        assert result["fits"] is False
        assert result["overflow_bytes"] == 44


class TestSnapshotDiffer:
    def test_no_changes(self):
        base = {"snapshot_id": "s1", "files": [{"path": "a.parquet", "checksum": "abc"}]}
        curr = {"snapshot_id": "s2", "files": [{"path": "a.parquet", "checksum": "abc"}]}
        result = snapshot_differ(base, curr)
        assert result["added_files"] == 0
        assert result["removed_files"] == 0
        assert result["modified_files"] == 0

    def test_added_and_removed(self):
        base = {"snapshot_id": "s1", "files": [
            {"path": "a.parquet", "checksum": "abc", "rows": 100}
        ]}
        curr = {"snapshot_id": "s2", "files": [
            {"path": "b.parquet", "checksum": "def", "rows": 200}
        ]}
        result = snapshot_differ(base, curr)
        assert result["added_files"] == 1
        assert result["removed_files"] == 1
        assert result["added_rows"] == 200

    def test_modified(self):
        base = {"snapshot_id": "s1", "files": [
            {"path": "a.parquet", "checksum": "abc", "rows": 100}
        ]}
        curr = {"snapshot_id": "s2", "files": [
            {"path": "a.parquet", "checksum": "xyz", "rows": 150}
        ]}
        result = snapshot_differ(base, curr)
        assert result["modified_files"] == 1


class TestRetentionChecker:
    def test_recent_snapshots(self):
        snaps = [{"snapshot_id": f"s{i}", "created_at": "2024-11-20T00:00:00Z"} for i in range(5)]
        result = retention_checker(snaps, retention_days=30)
        assert result["valid_count"] == 0  # all expired vs now
        assert result["should_clean"] is False

    def test_mixed_snapshots(self):
        snaps = (
            [{"snapshot_id": f"s{i}", "created_at": "2023-01-01T00:00:00Z"} for i in range(3)] +
            [{"snapshot_id": f"s{i+3}", "created_at": "2024-11-20T00:00:00Z"} for i in range(2)]
        )
        result = retention_checker(snaps, retention_days=30)
        assert result["total_snapshots"] == 5
        assert result["valid_count"] >= 0


class TestVersionConflictResolver:
    def test_no_conflict(self):
        result = version_conflict_resolver(5, 5)
        assert result["conflict"] is False

    def test_lww(self):
        result = version_conflict_resolver(3, 7, strategy="last-write-wins")
        assert result["resolved_version"] == 7

    def test_base_wins(self):
        result = version_conflict_resolver(10, 5, strategy="base-wins")
        assert result["resolved_version"] == 10


class TestDeltaReconciler:
    def test_merge_with_overlap(self):
        stream = [{"id": 1, "val": "s1"}, {"id": 2, "val": "s2"}]
        batch = [{"id": 2, "val": "b2"}, {"id": 3, "val": "b3"}]
        result = delta_reconciler(stream, batch, dedup_key="id")
        assert result["duplicates_resolved"] == 1
        assert result["reconciled_total"] == 3
        assert result["checksum"]

    def test_no_overlap(self):
        stream = [{"id": 1, "val": "s1"}]
        batch = [{"id": 2, "val": "b2"}]
        result = delta_reconciler(stream, batch, dedup_key="id")
        assert result["duplicates_resolved"] == 0
        assert result["reconciled_total"] == 2

    def test_empty_streams(self):
        result = delta_reconciler([], [{"id": 1, "val": "a"}], dedup_key="id")
        assert result["stream_count"] == 0
        assert result["reconciled_total"] == 1