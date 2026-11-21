"""变更捕获领域服务：Schema和元数据的变更捕获、版本对比和差异通知。

Pipeline 6: Change capture → diff reporting → version history

字段结构变化的取数统一委托给 :mod:`app.services.field_changes`，
与版本履历共用同一份记录，避免两个入口各写一套结构对比。
"""
from __future__ import annotations

import time as _time
from typing import Any

from app.services import field_changes
from app.services.base import BaseModuleService
from app.store import store


def _should_record(change_type: str, before: Any, after: Any) -> bool:
    """占位/空值的手工登记不产生可取数的字段变化。"""
    if before in (None, "", "-") and after in (None, "", "-"):
        return False
    return bool(change_type) or before not in (None, "") or after not in (None, "")


class ChangeCaptureService(BaseModuleService):
    MODULE = "change_capture"
    ENTITY = "变更记录"
    OPERATION_TERM = "校验"
    LIST_FIELDS = ["记录编号", "目标资产", "变更类型", "变更前值", "变更后值", "变更来源", "变更时间", "影响范围"]
    REQUIRED_FIELDS = ["记录编号", "目标资产", "变更类型"]
    STATUS_ORDER = ["已捕获", "已对比", "已通知", "已确认"]
    ACTION_RULES = {"启动对比": "已对比", "发送通知": "已通知", "确认变更": "已确认"}
    NEGATIVE_ACTIONS = []

    def create_entry(self, values: dict[str, Any]) -> tuple[dict[str, Any] | None, list[str]]:
        """通用登记入口：除写入变更记录外，同步登记字段级取数。"""
        entry, missing = super().create_entry(values)
        if entry is None or missing:
            return entry, missing
        change_type = str(values.get("变更类型") or "")
        before = values.get("变更前值")
        after = values.get("变更后值")
        field = str(values.get("变更字段") or "").strip() or None
        if _should_record(change_type, before, after):
            field_changes.record_capture(
                str(values.get("目标资产") or ""),
                change_type,
                before,
                after,
                source=str(values.get("变更来源") or "manual"),
                change_id=int(entry["id"]),
                field=field,
            )
        return entry, missing

    def capture_change(self, asset_code: str, change_type: str, before: Any, after: Any,
                       source: str = "schema_evolution", field: str | None = None) -> dict[str, Any]:
        """捕获一次变更并自动触发差异对比和版本快照（Pipeline 6 入口）。

        ``field`` 可选，用于在只给出类型值时指明改的是哪一列；
        不传时由共享取数层按字段结构推断，原有调用方式不受影响。
        """
        change_id = store.next_id(self.MODULE)
        change = {
            "id": change_id,
            "记录编号": f"CHAN-{change_id:04d}",
            "目标资产": asset_code,
            "变更类型": change_type,
            "变更前值": str(before),
            "变更后值": str(after),
            "变更来源": source,
            "变更时间": _time.strftime("%Y-%m-%d %H:%M:%S"),
            "影响范围": self._compute_impact_scope(asset_code, change_type),
            "status": "已捕获",
            "pending": True,
            "abnormal": False,
            "created_at": _time.strftime("%Y-%m-%d %H:%M:%S"),
        }
        if field:
            change["变更字段"] = field
        store.rows(self.MODULE).append(change)

        # 先登记字段级事件，后续版本比较从这里取数。
        field_changes.record_capture(
            asset_code, change_type, before, after,
            source=source, change_id=change_id, field=field,
        )

        # Pipeline 6: 自动触发差异报告和版本快照
        diff_result = self._generate_diff_report(change)
        version_snap = self._create_version_snapshot(change)

        # 记录血缘中受影响的下游资产
        affected_downstream = self._find_downstream_assets(asset_code)

        store.log_change(
            self.MODULE, change_id, "变更捕获完成",
            before=change_type, after=f"差异{diff_result.get('diff_count',0)}条，影响{len(affected_downstream)}个下游",
            source="pipeline_6", scope="change_capture->diff_report->version_history"
        )

        self.emit_event("change_captured", {
            "change_id": change_id,
            "asset_code": asset_code,
            "change_type": change_type,
            "downstream_count": len(affected_downstream),
        })

        return {
            "change": change,
            "diff_report": diff_result,
            "version_snapshot": version_snap,
            "affected_downstream": affected_downstream,
            "pipeline": "change_capture->diff_report->version_history",
        }

    def _compute_impact_scope(self, asset_code: str, change_type: str) -> str:
        """计算影响范围。"""
        downstream = self._find_downstream_assets(asset_code)
        if downstream:
            return f"影响{len(downstream)}个下游资产: {', '.join(downstream)}"
        return f"无下游依赖"

    def _generate_diff_report(self, change: dict[str, Any]) -> dict[str, Any]:
        """生成差异报告：按字段级取数写入前后值，返回既有形状。"""
        events = field_changes.collect_change_events(
            change.get("目标资产", ""), change["id"],
        )
        if not events:
            events = [{
                "field_name": change.get("变更类型", ""),
                "before": change.get("变更前值", ""),
                "after": change.get("变更后值", ""),
                "kind": "modify",
            }]

        first_id: int | None = None
        for event in events:
            diff_id = store.next_id("diff_report")
            if first_id is None:
                first_id = diff_id
            kind = event.get("kind", "modify")
            diff = {
                "id": diff_id,
                "记录编号": f"DIFF-CHG-{diff_id:04d}",
                "比较对象": change.get("目标资产", ""),
                "比较维度": change.get("变更类型", ""),
                "差异字段": event.get("field_name", ""),
                "源值": event.get("before", ""),
                "目标值": event.get("after", ""),
                "差异等级": "新增" if kind == "add" else ("删除" if kind == "remove" else "修改"),
                "报告时间": _time.strftime("%Y-%m-%d %H:%M:%S"),
                "status": "已生成",
                "pending": False,
                "abnormal": False,
                "change_id": change["id"],
                "created_at": _time.strftime("%Y-%m-%d %H:%M:%S"),
            }
            store.rows("diff_report").append(diff)
        return {"diff_id": first_id, "diff_count": len(events)}

    def _create_version_snapshot(self, change: dict[str, Any]) -> dict[str, Any]:
        """创建版本快照记录变更历史。"""
        store.create_snapshot(f"change-{change['id']}", [change.get("目标资产", "unknown")])
        return {"snapshot_id": f"change-{change['id']}", "created": True}

    def _find_downstream_assets(self, asset_code: str) -> list[str]:
        """查找下游资产（通过血缘关系）。"""
        downstream = []
        for row in store.rows("lineage_track"):
            if row.get("上游资产") == asset_code:
                ds = row.get("下游资产", "")
                if ds and ds not in downstream:
                    downstream.append(ds)
        return downstream

    def compare_versions(self, asset_code: str, version_a: str, version_b: str) -> list[dict[str, Any]]:
        """比较资产的两个版本差异（与版本履历共用取数记录）。

        两个引用都不在共享取数记录里时，回退到存储层的快照差异，
        保持旧调用（按存储快照名比较模块数据）的可用性。
        """
        if not (
            field_changes.has_version_boundary(asset_code, version_a)
            or field_changes.has_version_boundary(asset_code, version_b)
        ) and store.get_snapshot(version_a) is not None \
                and store.get_snapshot(version_b) is not None:
            return store.diff_snapshots(version_a, version_b, asset_code)
        diffs = field_changes.query_field_changes(asset_code, version_a, version_b)
        field_changes.persist_comparison(asset_code, version_a, version_b, diffs)
        return diffs

    def capture_field_change(self, asset_code: str, field: str, change_type: str,
                             before: Any, after: Any,
                             source: str = "field_structure") -> dict[str, Any]:
        """显式登记一笔字段结构变化（含精确字段名）。"""
        return field_changes.capture_field_change(
            asset_code, field, change_type, before, after, source=source,
        )

    def get_version_schema(self, asset_code: str, version_ref: str) -> list[dict[str, str]] | None:
        """读取版本边界上的字段结构，供变更捕获生成前后值。"""
        return field_changes.get_version_schema(asset_code, version_ref)

    def get_changes_by_asset(self, asset_code: str) -> list[dict[str, Any]]:
        """获取资产的所有变更记录。"""
        return store.find_by_field(self.MODULE, "目标资产", asset_code)

    def get_pending_changes(self) -> list[dict[str, Any]]:
        """获取未确认的变更。"""
        return [
            r for r in store.rows(self.MODULE)
            if r.get("status") in ("已捕获", "已对比")
        ]