"""版本履历领域服务：数据资产的多版本Schema快照、差异对比和回查能力。

维护跨时间点的数据资产版本快照，支持回溯和差异分析。
版本边界与字段差异取数统一委托给 :mod:`app.services.field_changes`，
与变更捕获共用同一份取数记录。
"""
from __future__ import annotations

import time as _time
from typing import Any

from app.services import field_changes
from app.services.base import BaseModuleService
from app.store import store


class VersionHistoryService(BaseModuleService):
    MODULE = "version_history"
    ENTITY = "版本快照"
    OPERATION_TERM = "校验"
    LIST_FIELDS = ["快照编号", "目标资产", "版本标签", "Schema摘要", "行数记录", "创建原因", "差异字段", "快照时间"]
    REQUIRED_FIELDS = ["快照编号", "目标资产", "版本标签"]
    STATUS_ORDER = ["当前版本", "历史版本", "已标注"]
    ACTION_RULES = {"创建快照": "历史版本", "标注版本": "已标注"}
    NEGATIVE_ACTIONS = []

    def create_version(self, asset_code: str, version_tag: str, reason: str, schema_summary: str = "",
                       row_count: str = "0", diff_fields: str = "") -> dict[str, Any]:
        """创建资产版本快照。返回值保持原有结构不变。"""
        snap_id = store.next_id(self.MODULE)
        snapshot = {
            "id": snap_id,
            "快照编号": f"VERS-{snap_id:04d}",
            "目标资产": asset_code,
            "版本标签": version_tag,
            "Schema摘要": schema_summary or f"资产{asset_code}快照",
            "行数记录": row_count,
            "创建原因": reason,
            "差异字段": diff_fields,
            "快照时间": _time.strftime("%Y-%m-%d %H:%M:%S"),
            "status": "历史版本",
            "pending": False,
            "abnormal": False,
            "created_at": _time.strftime("%Y-%m-%d %H:%M:%S"),
        }
        store.rows(self.MODULE).append(snapshot)

        # 版本写入登记一条不可变边界：之后的结构变更不会改写这次比较的取数。
        field_changes.register_version(
            asset_code,
            version_tag,
            schema_summary,
            snapshot_code=snapshot["快照编号"],
            row_id=snap_id,
            created_at=snapshot["快照时间"],
            reason=reason,
        )

        # 同时使用 store 级的快照功能保存完整数据副本
        store_snapshot = store.create_snapshot(f"version-{version_tag}-{asset_code}")

        return {
            "version": snapshot,
            "store_snapshot": store_snapshot,
        }

    def create_entry(self, values: dict[str, Any]) -> tuple[dict[str, Any] | None, list[str]]:
        """通用登记入口：除写入版本快照外，立即登记不可变版本边界。"""
        entry, missing = super().create_entry(values)
        if entry is None or missing:
            return entry, missing
        field_changes.register_version(
            str(values.get("目标资产") or ""),
            str(values.get("版本标签") or ""),
            values.get("Schema摘要", ""),
            snapshot_code=str(values.get("快照编号") or ""),
            row_id=int(entry["id"]),
            created_at=str(entry.get("created_at", "")),
            reason=str(values.get("创建原因") or ""),
        )
        return entry, missing

    def annotate_version(self, snapshot_id: int, annotation: str) -> dict[str, Any] | None:
        """标注版本快照。"""
        entry, msg = self.run_action(snapshot_id, "标注版本")
        if entry:
            entry["标注"] = annotation
            entry["标注时间"] = _time.strftime("%Y-%m-%d %H:%M:%S")
        return entry

    def get_versions_by_asset(self, asset_code: str) -> list[dict[str, Any]]:
        """获取资产的所有版本快照（按创建先后倒序）。"""
        versions = store.find_by_field(self.MODULE, "目标资产", asset_code)
        # 同一秒内创建的版本也要有确定次序，用 id 作为时间戳的次级排序键。
        versions.sort(
            key=lambda v: (str(v.get("快照时间", "")), int(v.get("id", 0))),
            reverse=True,
        )
        return versions

    def get_current_version(self, asset_code: str) -> dict[str, Any] | None:
        """获取资产的当前版本快照。"""
        versions = self.get_versions_by_asset(asset_code)
        return versions[0] if versions else None

    # ── 版本结构读取与比较（与变更捕获共用取数记录） ──────────────

    def get_version_schema(self, asset_code: str, version_ref: str) -> list[dict[str, str]] | None:
        """读取指定版本边界上的字段结构；``current`` 表示当前有效结构。"""
        return field_changes.get_version_schema(asset_code, version_ref)

    def get_current_schema(self, asset_code: str) -> list[dict[str, str]]:
        """读取当前有效的字段结构，包含最近一次版本边界之后的修改。"""
        return field_changes.current_schema(asset_code)

    def compare_versions(self, asset_code: str, version_a: str, version_b: str) -> list[dict[str, Any]]:
        """比较资产两个版本的字段结构差异，返回与原单次查询相同的形式。"""
        diffs = field_changes.query_field_changes(asset_code, version_a, version_b)
        field_changes.persist_comparison(asset_code, version_a, version_b, diffs)
        return diffs

    # 常用别名，避免调用方各自再写一套取数。
    def diff_versions(self, asset_code: str, version_a: str, version_b: str) -> list[dict[str, Any]]:
        return self.compare_versions(asset_code, version_a, version_b)

    def get_field_changes(self, asset_code: str, version_a: str, version_b: str) -> list[dict[str, Any]]:
        return self.compare_versions(asset_code, version_a, version_b)
