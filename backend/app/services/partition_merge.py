"""分区合并领域服务：读取压缩任务的分区压缩执行清单并实际产出文件。

执行语义：
- 字节上限按实际读取的文件计算（os.stat），不轻信计划值；
- 同一压缩计划的并发执行请求按任务串行，同一个文件只写一次；
- 同名文件按 (分区, 文件名) 确定归属，跨分区互不干扰；
- 兼容历史计划：无版本信息的输入跳过版本校验，已确认的产物直接
  采纳不重复写。
"""
from __future__ import annotations

import threading
from typing import Any

from app.compaction_manifest import refresh_plan_status, sync_todo
from app.compute import validate_group_capacity
from app.lake_storage import lake_storage
from app.services.base import BaseModuleService
from app.store import store

# 每个压缩计划一把执行锁：并发执行请求串行化，保证同一文件只写一次。
_EXEC_LOCKS: dict[int, threading.Lock] = {}
_EXEC_LOCKS_GUARD = threading.Lock()


def _lock_for(task_id: int) -> threading.Lock:
    with _EXEC_LOCKS_GUARD:
        return _EXEC_LOCKS.setdefault(int(task_id), threading.Lock())


class PartitionMergeService(BaseModuleService):
    MODULE = "partition_merge"
    ENTITY = "合并任务"
    OPERATION_TERM = "存储"
    LIST_FIELDS = ["任务标识", "目标湖表", "源分区", "目标分区", "合并策略", "文件大小", "执行状态", "耗时秒数"]
    REQUIRED_FIELDS = ["任务标识", "目标湖表", "源分区"]
    STATUS_ORDER = ["排队中", "合并中", "已完成", "合并失败"]
    ACTION_RULES = {"启动合并": "排队中", "取消合并": "合并中"}
    NEGATIVE_ACTIONS = []

    def _safe_int(self, val: Any, default: int = 0) -> int:
        try: return int(val)
        except (ValueError, TypeError): return default

    # ------------------------------------------------------------------
    # 合并入口：读取分区压缩执行清单并实际产出文件
    # ------------------------------------------------------------------

    def domain_merge_partitions(
        self,
        entry_id: int,
        compaction_id: int | None = None,
        max_groups: int | None = None,
    ) -> dict[str, Any]:
        """执行分区合并：优先消费压缩任务已批准的执行清单；找不到清单的
        历史合并任务保持旧的估算行为。"""
        entry = store.find(self.MODULE, entry_id)
        if not entry:
            return {"ok": False, "message": f"合并任务 {entry_id} 不存在"}
        source = str(entry.get("源分区", "0")).split(",")

        compaction_entry = self._find_manifest_entry(entry, compaction_id)
        if compaction_entry is None:
            # 历史兼容：没有执行清单的合并任务维持原估算输出。
            return {
                "ok": True,
                "task_id": entry.get("任务标识"),
                "table": entry.get("目标湖表"),
                "source_partitions": source,
                "target_partition": entry.get("目标分区", source[0]),
                "merged_files": len(source) * 3,
                "output_size_mb": self._safe_int(entry.get("文件大小"), 128),
                "strategy": entry.get("合并策略", "bin-packing"),
            }

        execution = self.execute_manifest(
            compaction_entry, merge_entry=entry, max_groups=max_groups,
        )
        manifest = compaction_entry["执行清单"]
        return {
            "ok": True,
            "task_id": entry.get("任务标识"),
            "table": entry.get("目标湖表"),
            "source_partitions": source,
            "target_partition": entry.get("目标分区", source[0]),
            "strategy": entry.get("合并策略", "bin-packing"),
            "compaction_task_id": compaction_entry.get("id"),
            "计划状态": manifest.get("计划状态"),
            "merged_files": execution["written_outputs"],
            "output_size_mb": round(execution["written_bytes"] / 1024 / 1024, 2),
            "执行": execution,
        }

    def _find_manifest_entry(
        self, merge_entry: dict[str, Any], compaction_id: int | None,
    ) -> dict[str, Any] | None:
        """定位要消费的执行清单：显式指定压缩任务优先，否则取同湖表最新一份。"""
        if compaction_id is not None:
            candidate = store.find("compaction_task", compaction_id)
            return candidate if candidate and candidate.get("执行清单") else None
        table = merge_entry.get("目标湖表")
        candidates = [
            row for row in store.rows("compaction_task")
            if row.get("目标湖表") == table and row.get("执行清单")
        ]
        return candidates[-1] if candidates else None

    # ------------------------------------------------------------------
    # 清单执行器
    # ------------------------------------------------------------------

    def execute_manifest(
        self,
        compaction_entry: dict[str, Any],
        merge_entry: dict[str, Any] | None = None,
        max_groups: int | None = None,
    ) -> dict[str, Any]:
        """按执行清单逐组校验并产出文件。

        max_groups 限制本次处理的待执行组数（用于部分执行/中断恢复）；
        已写出组与产物已存在的组不占用该额度。
        """
        task_id = int(compaction_entry.get("id", 0))
        with _lock_for(task_id):
            return self._execute_manifest_locked(
                compaction_entry, merge_entry, max_groups,
            )

    def _execute_manifest_locked(
        self,
        compaction_entry: dict[str, Any],
        merge_entry: dict[str, Any] | None,
        max_groups: int | None,
    ) -> dict[str, Any]:
        manifest = compaction_entry["执行清单"]
        table = compaction_entry.get("目标湖表", "")
        limit = self._safe_int(manifest.get("容量上限字节"), 0)

        executed: list[dict[str, Any]] = []
        adopted: list[dict[str, Any]] = []
        rescheduled: list[dict[str, Any]] = []
        budget = max_groups

        for group in manifest.get("组", []):
            if group.get("状态") == "已写出":
                continue
            partition = group.get("分区", "")
            output = group.get("产物") or f"compact-t{task_id}-g{group.get('组号')}.parquet"
            group["产物"] = output

            # 历史计划已确认的产物：直接采纳，不重复写。
            if lake_storage.output_exists(table, partition, output):
                info = lake_storage.read_file_info(table, partition, output)
                group["状态"] = "已写出"
                group["实际字节"] = info["size_bytes"] if info else group.get("计划字节")
                adopted.append({"组号": group.get("组号"), "分区": partition, "产物": output})
                continue

            if budget is not None and budget <= 0:
                continue
            if budget is not None:
                budget -= 1

            # 按实际读取的文件计算字节数，并核对源文件版本。
            actual_bytes = 0
            problems: list[str] = []
            for item in group.get("文件", []):
                info = lake_storage.read_file_info(table, item.get("分区", ""), item.get("文件名", ""))
                if info is None:
                    item["校验"] = "缺失"
                    problems.append(f"源文件缺失：{item.get('文件名')}")
                    continue
                item["实际字节"] = info["size_bytes"]
                item["当前版本"] = info["version"]
                actual_bytes += info["size_bytes"]
                planned_version = item.get("计划版本")
                if planned_version is not None and info["version"] != planned_version:
                    item["校验"] = "版本冲突"
                    problems.append(
                        f"源文件版本冲突：{item.get('文件名')} "
                        f"计划v{planned_version}→当前v{info['version']}"
                    )
                else:
                    item["校验"] = "通过"

            if problems:
                group["状态"] = "需改排"
                group["改排原因"] = "；".join(problems)
                group["实际字节"] = actual_bytes
                rescheduled.append({
                    "组号": group.get("组号"),
                    "分区": partition,
                    "改排原因": group["改排原因"],
                })
                continue

            # 每组容量阈值校验：按实际读取的字节数核对上限。
            capacity = validate_group_capacity(actual_bytes, limit)
            if not capacity["fits"]:
                group["状态"] = "需改排"
                group["改排原因"] = (
                    f"组实际字节 {capacity['actual_bytes']} 超上限 "
                    f"{capacity['limit_bytes']}（溢出 {capacity['overflow_bytes']}）"
                )
                group["实际字节"] = actual_bytes
                rescheduled.append({
                    "组号": group.get("组号"),
                    "分区": partition,
                    "改排原因": group["改排原因"],
                })
                continue

            lake_storage.write_output(table, partition, output, actual_bytes)
            group["状态"] = "已写出"
            group["实际字节"] = actual_bytes
            group["改排原因"] = None
            executed.append({
                "组号": group.get("组号"),
                "分区": partition,
                "产物": output,
                "实际字节": actual_bytes,
            })

        # 每组校验结果同步到下次压缩待办（保留源文件版本与改排原因）。
        sync_todo(manifest)
        refresh_plan_status(manifest)

        return {
            "executed_groups": executed,
            "adopted_groups": adopted,
            "rescheduled_groups": rescheduled,
            "written_outputs": len(executed),
            "written_bytes": sum(item["实际字节"] for item in executed),
            "todo_count": len(manifest.get("下轮待办", [])),
        }
