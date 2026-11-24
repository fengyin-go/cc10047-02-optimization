"""压缩执行清单链路测试：稳定分组、实际产出、容量阈值校验、版本冲突改排、
并发幂等、同名文件归属、历史计划兼容和中断恢复重试。"""
from __future__ import annotations

import sys
import threading

sys.path.insert(0, '.')

import pytest

from app.lake_storage import lake_storage
from app.services import service_for

MiB = 1024 * 1024
PART = "dt=2026-11-01"


@pytest.fixture()
def lake(tmp_path):
    lake_storage.configure(tmp_path / "lake")
    return lake_storage


def _make_compaction(table: str, target_size: str = "256MiB") -> dict:
    svc = service_for("compaction_task")
    entry, missing = svc.create_entry({
        "任务标识": f"CMP-{table}",
        "目标湖表": table,
        "压缩算法": "zstd",
        "目标大小": target_size,
        "并行度": "4",
    })
    assert not missing
    return entry


def _make_merge(table: str) -> dict:
    svc = service_for("partition_merge")
    entry, missing = svc.create_entry({
        "任务标识": f"PM-{table}",
        "目标湖表": table,
        "源分区": PART,
        "合并策略": "bin-packing",
    })
    assert not missing
    return entry


def _seed_four_files(lake, table: str, partition: str = PART) -> None:
    """任务验收场景：同分区 180、180、96、64 MiB 四件文件。"""
    lake.put_file(table, partition, "f1.parquet", 180 * MiB)
    lake.put_file(table, partition, "f2.parquet", 180 * MiB)
    lake.put_file(table, partition, "f3.parquet", 96 * MiB)
    lake.put_file(table, partition, "f4.parquet", 64 * MiB)


class TestStableGrouping:
    def test_manifest_groups_by_partition_limit(self, lake):
        table = "t_stable"
        _seed_four_files(lake, table)
        svc = service_for("compaction_task")
        task = _make_compaction(table)

        result = svc.domain_plan_compaction(task["id"])
        assert result["ok"] is True
        groups = result["groups"]
        # 256 MiB 上限下的稳定装箱：[180+64] [180] [96]
        assert [g["计划字节"] for g in groups] == [244 * MiB, 180 * MiB, 96 * MiB]
        assert groups[0]["文件"] == ["f1.parquet", "f4.parquet"]
        assert groups[1]["文件"] == ["f2.parquet"]
        assert groups[2]["文件"] == ["f3.parquet"]
        # 清单挂在任务详情上，不再只是估算组数
        detail = svc.get_entry(task["id"])
        assert detail["执行清单"]["计划状态"] == "草稿"
        assert detail["执行清单"]["容量上限字节"] == 256 * MiB

    def test_replan_is_deterministic(self, lake):
        table = "t_deterministic"
        _seed_four_files(lake, table)
        svc = service_for("compaction_task")
        task = _make_compaction(table)

        first = svc.domain_plan_compaction(task["id"])
        second = svc.domain_plan_compaction(task["id"])
        assert [
            (g["分区"], g["计划字节"], g["文件"]) for g in first["groups"]
        ] == [
            (g["分区"], g["计划字节"], g["文件"]) for g in second["groups"]
        ]

    def test_replan_keeps_written_groups(self, lake):
        table = "t_keep"
        _seed_four_files(lake, table)
        comp_svc = service_for("compaction_task")
        merge_svc = service_for("partition_merge")
        task = _make_compaction(table)
        merge = _make_merge(table)

        comp_svc.domain_plan_compaction(task["id"])
        comp_svc.run_action(task["id"], "启动压缩")
        merge_svc.domain_merge_partitions(merge["id"], compaction_id=task["id"], max_groups=1)

        replanned = comp_svc.domain_plan_compaction(task["id"])
        groups = replanned["groups"]
        # 已写出的第一组原样保留，不为它重新分组
        assert groups[0]["状态"] == "已写出"
        assert groups[0]["文件"] == ["f1.parquet", "f4.parquet"]
        assert groups[0]["组号"] == 1
        # 剩余文件重新装箱，不覆盖已确认的输入
        remaining = [name for g in groups[1:] for name in g["文件"]]
        assert sorted(remaining) == ["f2.parquet", "f3.parquet"]


class TestBackfillConflictRecovery:
    def test_replace_before_merge_then_retry_from_detail(self, lake):
        """压缩计划批准后回填替换源文件：中断恢复后重试，核对实际字节、
        源文件版本和下一轮待办。"""
        table = "t_backfill"
        _seed_four_files(lake, table)
        comp_svc = service_for("compaction_task")
        merge_svc = service_for("partition_merge")
        task = _make_compaction(table)
        merge = _make_merge(table)

        # 生成执行清单并批准（启动压缩）
        plan = comp_svc.domain_plan_compaction(task["id"])
        assert [g["计划字节"] for g in plan["groups"]] == [244 * MiB, 180 * MiB, 96 * MiB]
        comp_svc.run_action(task["id"], "启动压缩")
        assert comp_svc.get_entry(task["id"])["执行清单"]["计划状态"] == "已批准"

        # 回填在分区合并入口执行前，把同一路径的 96 MiB 文件换成 200 MiB
        replaced = lake.replace_file(table, PART, "f3.parquet", 200 * MiB)
        assert replaced["version"] == 2

        # 分区合并入口执行：第一组写出后中断（部分执行）
        first = merge_svc.domain_merge_partitions(
            merge["id"], compaction_id=task["id"], max_groups=1,
        )
        assert first["ok"] is True
        manifest = comp_svc.get_entry(task["id"])["执行清单"]
        assert [g["状态"] for g in manifest["组"]] == ["已写出", "待执行", "待执行"]
        assert manifest["组"][0]["实际字节"] == 244 * MiB

        # 值班员从中断恢复：任务详情能看出已写出的组
        detail = comp_svc.get_entry(task["id"])
        assert detail["执行清单"]["计划状态"] == "部分完成"

        # 从压缩任务详情重试
        retry = comp_svc.domain_retry_compaction(task["id"])
        assert retry["ok"] is True
        groups = retry["组"]
        # 各组实际字节：按实际读取的文件计算
        assert groups[0]["状态"] == "已写出" and groups[0]["实际字节"] == 244 * MiB
        assert groups[1]["状态"] == "已写出" and groups[1]["实际字节"] == 180 * MiB
        # 96→200 的组：源文件版本冲突，需改排
        assert groups[2]["状态"] == "需改排"
        assert "版本冲突" in groups[2]["改排原因"]
        f3 = groups[2]["文件"][0]
        assert f3["计划版本"] == 1
        assert f3["当前版本"] == 2
        assert f3["实际字节"] == 200 * MiB

        # 下一轮待办：保留源文件版本和改排原因
        todo = retry["下轮待办"]
        assert len(todo) == 1
        assert todo[0]["文件名"] == "f3.parquet"
        assert todo[0]["当前字节"] == 200 * MiB
        assert todo[0]["计划版本"] == 1
        assert todo[0]["当前版本"] == 2
        assert "版本冲突" in todo[0]["改排原因"]

        # 产物真实落盘且字节正确，冲突组没有产出
        outputs = lake.list_files(table, include_outputs=True)
        output_names = [f["name"] for f in outputs if f["kind"] == "output"]
        assert output_names == [groups[0]["产物"], groups[1]["产物"]]
        assert lake.read_file_info(table, PART, groups[0]["产物"])["size_bytes"] == 244 * MiB
        assert lake.read_file_info(table, PART, groups[1]["产物"])["size_bytes"] == 180 * MiB

    def test_replan_after_conflict_covers_todo(self, lake):
        """冲突改排的文件在重新规划时被下一轮计划接住。"""
        table = "t_replan_todo"
        _seed_four_files(lake, table)
        comp_svc = service_for("compaction_task")
        merge_svc = service_for("partition_merge")
        task = _make_compaction(table)
        merge = _make_merge(table)

        comp_svc.domain_plan_compaction(task["id"])
        comp_svc.run_action(task["id"], "启动压缩")
        lake.replace_file(table, PART, "f3.parquet", 200 * MiB)
        merge_svc.domain_merge_partitions(merge["id"], compaction_id=task["id"])
        assert comp_svc.get_entry(task["id"])["执行清单"]["下轮待办"]

        replanned = comp_svc.domain_plan_compaction(task["id"])
        # 已写出的组保留；f3 按当前 200 MiB / v2 重新入组
        new_files = {
            name for g in replanned["groups"] if g["状态"] == "待执行"
            for name in g["文件"]
        }
        assert "f3.parquet" in new_files
        manifest = comp_svc.get_entry(task["id"])["执行清单"]
        assert manifest["下轮待办"] == []


class TestConcurrentExecution:
    def test_same_file_written_once(self, lake):
        """原计划收到并发执行请求：同一个文件只写一次。"""
        table = "t_concurrent"
        _seed_four_files(lake, table)
        comp_svc = service_for("compaction_task")
        merge_svc = service_for("partition_merge")
        task = _make_compaction(table)
        merge = _make_merge(table)
        comp_svc.domain_plan_compaction(task["id"])
        comp_svc.run_action(task["id"], "启动压缩")

        results = []

        def worker():
            results.append(merge_svc.domain_merge_partitions(
                merge["id"], compaction_id=task["id"],
            ))

        threads = [threading.Thread(target=worker) for _ in range(4)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()

        assert all(r["ok"] for r in results)
        # 产物写盘次数 == 组数，没有重复写
        assert lake.output_write_count == 3
        assert sum(r["执行"]["written_outputs"] for r in results) == 3
        manifest = comp_svc.get_entry(task["id"])["执行清单"]
        assert [g["状态"] for g in manifest["组"]] == ["已写出"] * 3
        outputs = [
            f["name"] for f in lake.list_files(table, include_outputs=True)
            if f["kind"] == "output"
        ]
        assert len(outputs) == 3


class TestSameNameAcrossPartitions:
    def test_ownership_by_partition(self, lake):
        """两个目标分区里的同名文件各有归属。"""
        table = "t_same_name"
        lake.put_file(table, "dt=2026-11-01", "data.parquet", 100 * MiB)
        lake.put_file(table, "dt=2026-11-02", "data.parquet", 120 * MiB)
        comp_svc = service_for("compaction_task")
        merge_svc = service_for("partition_merge")
        task = _make_compaction(table)
        merge = _make_merge(table)

        plan = comp_svc.domain_plan_compaction(task["id"])
        # 组不跨分区：两个同名文件各自成组
        assert len(plan["groups"]) == 2
        assert {g["分区"] for g in plan["groups"]} == {"dt=2026-11-01", "dt=2026-11-02"}

        comp_svc.run_action(task["id"], "启动压缩")
        result = merge_svc.domain_merge_partitions(merge["id"], compaction_id=task["id"])
        assert result["ok"] is True

        files_p1 = {f["name"]: f for f in lake.list_files(table, "dt=2026-11-01", include_outputs=True)}
        files_p2 = {f["name"]: f for f in lake.list_files(table, "dt=2026-11-02", include_outputs=True)}
        # 每个分区各有一份产物，字节数对应各自分区的源文件
        out_p1 = [f for f in files_p1.values() if f["kind"] == "output"]
        out_p2 = [f for f in files_p2.values() if f["kind"] == "output"]
        assert len(out_p1) == 1 and out_p1[0]["size_bytes"] == 100 * MiB
        assert len(out_p2) == 1 and out_p2[0]["size_bytes"] == 120 * MiB
        # 源文件保持各自归属与版本
        assert files_p1["data.parquet"]["version"] == 1
        assert files_p2["data.parquet"]["version"] == 1


class TestLegacyManifest:
    def _legacy_manifest(self) -> dict:
        return {
            "计划版本": 1,
            "计划状态": "已批准",
            "容量上限字节": 256 * MiB,
            "生成时间": "2026-11-01T00:00:00+00:00",
            "组": [
                {
                    "组号": 1,
                    "分区": PART,
                    "计划字节": 50 * MiB,
                    "实际字节": None,
                    "状态": "待执行",
                    "产物": "legacy-g1.parquet",
                    "改排原因": None,
                    # 历史计划已确认的输入：没有版本字段
                    "文件": [{"分区": PART, "文件名": "old1.parquet", "计划字节": 50 * MiB}],
                },
            ],
            "下轮待办": [],
        }

    def test_legacy_manifest_without_versions_executes(self, lake):
        """历史计划没有版本信息：跳过版本校验，照常执行。"""
        table = "t_legacy"
        lake.put_file(table, PART, "old1.parquet", 50 * MiB)
        comp_svc = service_for("compaction_task")
        merge_svc = service_for("partition_merge")
        task = _make_compaction(table)
        merge = _make_merge(table)
        comp_svc.get_entry(task["id"])["执行清单"] = self._legacy_manifest()

        result = merge_svc.domain_merge_partitions(merge["id"], compaction_id=task["id"])
        assert result["ok"] is True
        assert result["执行"]["written_outputs"] == 1
        group = comp_svc.get_entry(task["id"])["执行清单"]["组"][0]
        assert group["状态"] == "已写出"
        assert group["实际字节"] == 50 * MiB

    def test_confirmed_output_is_adopted_not_rewritten(self, lake):
        """历史计划已确认的产物：直接采纳，不重复写。"""
        table = "t_adopt"
        lake.put_file(table, PART, "old1.parquet", 50 * MiB)
        lake.write_output(table, PART, "legacy-g1.parquet", 50 * MiB)  # 历史产物
        writes_before = lake.output_write_count
        comp_svc = service_for("compaction_task")
        merge_svc = service_for("partition_merge")
        task = _make_compaction(table)
        merge = _make_merge(table)
        comp_svc.get_entry(task["id"])["执行清单"] = self._legacy_manifest()

        result = merge_svc.domain_merge_partitions(merge["id"], compaction_id=task["id"])
        assert result["ok"] is True
        assert result["执行"]["written_outputs"] == 0
        assert len(result["执行"]["adopted_groups"]) == 1
        assert lake.output_write_count == writes_before
        group = comp_svc.get_entry(task["id"])["执行清单"]["组"][0]
        assert group["状态"] == "已写出"


class TestCapacityValidation:
    def test_oversized_group_rescheduled_to_todo(self, lake):
        """单文件超上限：容量阈值校验不通过，进入下次压缩待办。"""
        table = "t_overflow"
        lake.put_file(table, PART, "big.parquet", 300 * MiB)
        comp_svc = service_for("compaction_task")
        merge_svc = service_for("partition_merge")
        task = _make_compaction(table)
        merge = _make_merge(table)

        comp_svc.domain_plan_compaction(task["id"])
        comp_svc.run_action(task["id"], "启动压缩")
        result = merge_svc.domain_merge_partitions(merge["id"], compaction_id=task["id"])
        assert result["ok"] is True

        manifest = comp_svc.get_entry(task["id"])["执行清单"]
        group = manifest["组"][0]
        assert group["状态"] == "需改排"
        assert "超上限" in group["改排原因"]
        assert group["实际字节"] == 300 * MiB
        todo = manifest["下轮待办"]
        assert len(todo) == 1
        assert todo[0]["文件名"] == "big.parquet"
        assert todo[0]["当前字节"] == 300 * MiB
        assert "超上限" in todo[0]["改排原因"]
        # 没有产出任何文件
        assert lake.output_write_count == 0


class TestStorageOptimizeChain:
    def test_chain_flows_manifest_into_merge(self, lake):
        """存储优化链路：规划在前、合并消费同一份执行清单。"""
        from fastapi.testclient import TestClient

        from app.main import app

        table = "t_chain"
        _seed_four_files(lake, table)
        task = _make_compaction(table)
        merge = _make_merge(table)

        client = TestClient(app)
        resp = client.post(
            "/api/chains/storage-optimize",
            params={"table_id": 1, "merge_id": merge["id"], "compaction_id": task["id"]},
        )
        assert resp.status_code == 200
        data = resp.json()
        assert data["ok"] is True
        assert [step["step"] for step in data["steps"]] == [
            "define_table", "plan_compaction", "merge_partitions", "compact_table",
        ]
        assert data["summary"]["merged_files"] == 3
        manifest = service_for("compaction_task").get_entry(task["id"])["执行清单"]
        assert manifest["计划状态"] == "已完成"
        assert lake.output_write_count == 3
