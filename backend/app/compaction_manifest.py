"""分区压缩执行清单的共享结构约定与状态维护。

清单挂在压缩任务记录的「执行清单」字段上，压缩任务服务负责生成，
分区合并服务负责执行，两边通过这里的函数维护同一份状态：

- 组状态：待执行 → 已写出 / 需改排
- 计划状态：草稿 → 已批准 → 部分完成 → 已完成
- 下轮待办：每组容量阈值校验与版本校验的改排结果，保留源文件版本
  （计划版本 + 当前版本）和改排原因。
"""
from __future__ import annotations

from typing import Any


def output_name(task_id: Any, group_no: int) -> str:
    """产物文件名：计划时确认，执行时幂等复用。"""
    return f"compact-t{task_id}-g{group_no}.parquet"


def sync_todo(
    manifest: dict[str, Any],
    previous_todo: list[dict[str, Any]] | None = None,
) -> None:
    """把改排结果同步到下次压缩待办。

    已被有效组（待执行/已写出）覆盖的文件不再占用待办；历史待办里
    仍未被覆盖的条目保留，避免中断恢复后丢失改排上下文。
    """
    todo: list[dict[str, Any]] = []
    covered: set[tuple[str, str]] = set()
    for group in manifest.get("组", []):
        if group.get("状态") in ("待执行", "已写出"):
            for item in group.get("文件", []):
                covered.add((item.get("分区"), item.get("文件名")))
    for group in manifest.get("组", []):
        if group.get("状态") != "需改排":
            continue
        for item in group.get("文件", []):
            todo.append({
                "分区": item.get("分区"),
                "文件名": item.get("文件名"),
                "计划版本": item.get("计划版本"),
                "当前版本": item.get("当前版本"),
                "当前字节": item.get("实际字节"),
                "改排原因": group.get("改排原因"),
            })
    seen = {(item.get("分区"), item.get("文件名")) for item in todo}
    for old in previous_todo or []:
        key = (old.get("分区"), old.get("文件名"))
        if key not in covered and key not in seen:
            todo.append(old)
            seen.add(key)
    manifest["下轮待办"] = todo


def refresh_plan_status(manifest: dict[str, Any]) -> None:
    """根据组状态回写计划状态，值班员恢复后可直接看出进度。"""
    states = {group.get("状态") for group in manifest.get("组", [])}
    if states and states <= {"已写出"}:
        manifest["计划状态"] = "已完成"
    elif "已写出" in states:
        manifest["计划状态"] = "部分完成"
