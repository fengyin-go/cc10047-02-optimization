"""模拟湖表文件存储：真实落盘的稀疏文件 + 每表一份版本元数据。

压缩/合并链路里"实际读取的文件"都走这里：
- 字节数来自 ``os.stat`` 的 ``st_size``（稀疏文件，不占真实磁盘）；
- 版本来自 ``<表目录>/_meta.json``，每次写入/替换（回填覆盖）递增。

目录布局：``<root>/<表名>/<分区名>/<文件名>``。
"""
from __future__ import annotations

import json
import os
import threading
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

_DEFAULT_ROOT = Path(__file__).resolve().parent.parent / "var" / "lake"
_META_FILE = "_meta.json"


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


class LakeStorage:
    """按 表/分区/文件 三级组织的模拟湖存储，线程安全。"""

    def __init__(self, root: Path | str | None = None) -> None:
        self._lock = threading.RLock()
        self.root = Path(
            root if root else os.environ.get("LAKE_STORAGE_ROOT", str(_DEFAULT_ROOT))
        )
        # 产物实际写盘次数，供并发幂等性断言使用。
        self.output_write_count = 0

    def configure(self, root: Path | str) -> None:
        """就地切换存储根目录（测试隔离用），实例身份保持不变。"""
        with self._lock:
            self.root = Path(root)
            self.output_write_count = 0

    # ------------------------------------------------------------------
    # 路径与元数据
    # ------------------------------------------------------------------

    def _table_dir(self, table: str) -> Path:
        return self.root / str(table)

    def _file_path(self, table: str, partition: str, name: str) -> Path:
        return self._table_dir(table) / str(partition) / str(name)

    def _meta_path(self, table: str) -> Path:
        return self._table_dir(table) / _META_FILE

    @staticmethod
    def _key(partition: str, name: str) -> str:
        return f"{partition}/{name}"

    def _load_meta(self, table: str) -> dict[str, Any]:
        path = self._meta_path(table)
        if not path.exists():
            return {"files": {}}
        try:
            return json.loads(path.read_text(encoding="utf-8"))
        except (ValueError, OSError):
            return {"files": {}}

    def _save_meta(self, table: str, meta: dict[str, Any]) -> None:
        path = self._meta_path(table)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(meta, ensure_ascii=False, indent=2), encoding="utf-8")

    # ------------------------------------------------------------------
    # 写入
    # ------------------------------------------------------------------

    def put_file(
        self, table: str, partition: str, name: str, size_bytes: int,
        kind: str = "source",
    ) -> dict[str, Any]:
        """新建或覆盖写入文件（覆盖即回填替换），版本递增，返回文件信息。"""
        size = max(0, int(size_bytes))
        with self._lock:
            path = self._file_path(table, partition, name)
            path.parent.mkdir(parents=True, exist_ok=True)
            meta = self._load_meta(table)
            key = self._key(partition, name)
            recorded = int(meta["files"].get(key, {}).get("version", 0))
            if recorded:
                version = recorded + 1
            else:
                # 元数据缺失但文件物理存在（历史遗留），视为一次替换。
                version = 2 if path.exists() else 1
            with open(path, "wb") as fh:
                fh.truncate(size)
            meta["files"][key] = {
                "version": version,
                "kind": kind,
                "updated_at": _utc_now(),
            }
            self._save_meta(table, meta)
            return {
                "table": table,
                "partition": partition,
                "name": name,
                "size_bytes": size,
                "version": version,
                "kind": kind,
            }

    def replace_file(
        self, table: str, partition: str, name: str, size_bytes: int,
    ) -> dict[str, Any]:
        """回填覆盖同一路径的源文件：内容替换、版本递增。"""
        return self.put_file(table, partition, name, size_bytes)

    def write_output(
        self, table: str, partition: str, name: str, size_bytes: int,
    ) -> dict[str, Any]:
        """写出合并/压缩产物文件（登记为 output，不再参与后续规划）。"""
        info = self.put_file(table, partition, name, size_bytes, kind="output")
        with self._lock:
            self.output_write_count += 1
        return info

    # ------------------------------------------------------------------
    # 读取（字节数按实际读取的文件计算）
    # ------------------------------------------------------------------

    def read_file_info(
        self, table: str, partition: str, name: str,
    ) -> dict[str, Any] | None:
        """实际 stat 文件得到字节数，并读出当前版本；文件不存在返回 None。"""
        with self._lock:
            path = self._file_path(table, partition, name)
            if not path.exists():
                return None
            size = path.stat().st_size
            record = self._load_meta(table)["files"].get(
                self._key(partition, name), {},
            )
            return {
                "table": table,
                "partition": partition,
                "name": name,
                "size_bytes": size,
                "version": int(record.get("version", 1)),
                "kind": record.get("kind", "source"),
            }

    def output_exists(self, table: str, partition: str, name: str) -> bool:
        with self._lock:
            return self._file_path(table, partition, name).exists()

    def list_partitions(self, table: str) -> list[str]:
        with self._lock:
            table_dir = self._table_dir(table)
            if not table_dir.is_dir():
                return []
            return sorted(
                child.name for child in table_dir.iterdir() if child.is_dir()
            )

    def list_files(
        self,
        table: str,
        partition: str | None = None,
        include_outputs: bool = False,
    ) -> list[dict[str, Any]]:
        """列出表（或指定分区）下的文件，按 (分区, 文件名) 稳定排序。

        默认只列源文件；压缩/合并产物（kind=output）不参与后续规划，
        避免已确认的产出被重新当作输入。
        """
        with self._lock:
            partitions = [partition] if partition else self.list_partitions(table)
            files: list[dict[str, Any]] = []
            for part in partitions:
                part_dir = self._table_dir(table) / part
                if not part_dir.is_dir():
                    continue
                for child in sorted(part_dir.iterdir()):
                    if not child.is_file():
                        continue
                    info = self.read_file_info(table, part, child.name)
                    if info is None:
                        continue
                    if not include_outputs and info["kind"] == "output":
                        continue
                    files.append(info)
            files.sort(key=lambda f: (f["partition"], f["name"]))
            return files


lake_storage = LakeStorage()
