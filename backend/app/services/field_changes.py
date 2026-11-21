"""版本结构变更取数：版本履历与变更捕获共用的字段结构变化查询。

历史上“版本比对”和“版本履历回读”各自维护了一套“从版本边界取字段结构、
再算字段差异”的逻辑，导致同一笔变更在两个入口里可能得到不同的粒度、
次序甚至相反的来源/目标值。本模块把这段取数收敛到一处：

* 版本写入登记一条不可变的边界（boundary），记录当时的字段结构；
* 变更捕获登记按时间递增的字段级事件（event），记录原值/新值；
* 两个入口都通过 :func:`collect_version_changes` 按“指定两版”的边界取数；
* 差异的源值/目标值、明细次序由 :func:`render_field_changes` 按比较方向生成。

取数记录保存在 ``store`` 的管道状态里，``store.reset()`` 会一并清空。
"""
from __future__ import annotations

import json
import re
import time as _time
from typing import Any, Iterable

from app.store import store

STATE_KEY = "field_change_ledger"
COMPARISON_KEY = "field_change_comparisons"

# 字段结构变化的归类词。中英文都接受，避免上游模块各自约定。
_RENAME_WORDS = ("rename", "改名", "重命名", "更名", "改名为")
_PRECISION_WORDS = (
    "precision", "scale", "decimal", "numeric", "精度", "小数",
    "长度", "字段类型", "类型", "type",
)
_ADD_WORDS = ("add", "新增", "添加", "增加")
_REMOVE_WORDS = ("remove", "drop", "delete", "删除", "移除", "下线")

# 结构里的主键字段，推断“是哪一列”时优先排除。
_KEY_FIELD_NAMES = {"id", "ID", "Id", "主键"}

# 读取“当前状态”时使用的版本引用，表示边界之后仍有效的修改。
_CURRENT_REFS = {"current", "head", "latest", "now", "当前", "当前版本", "最新"}

# 由存储层自动生成的快照行不是资产版本边界，同步时跳过。
_SKIP_TAGS = ("change-", "archive-", "pre-cleanup-")
_SKIP_REASONS = {"自动快照"}


# ── 基础解析 ──────────────────────────────────────────────────


def _split_top_level(text: str, separator: str = ",") -> list[str]:
    """按顶层分隔符切分，括号/引号内的分隔符不切（如 decimal(10,2)）。"""
    parts: list[str] = []
    buffer: list[str] = []
    depth = 0
    quote: str | None = None
    for char in text:
        if quote:
            buffer.append(char)
            if char == quote:
                quote = None
            continue
        if char in "\"'":
            quote = char
            buffer.append(char)
        elif char in "([{":
            depth += 1
            buffer.append(char)
        elif char in ")]}":
            depth = max(0, depth - 1)
            buffer.append(char)
        elif char == separator and depth == 0:
            parts.append("".join(buffer).strip())
            buffer = []
        else:
            buffer.append(char)
    tail = "".join(buffer).strip()
    if tail or not parts:
        parts.append(tail)
    return [part for part in parts if part != ""]


def _split_name_type(token: str) -> tuple[str, str]:
    """从 ``name:type``、``name(type)`` 里拆出字段名和类型。"""
    token = str(token).strip()
    if not token:
        return "", ""
    if ":" in token:
        name, type_value = token.split(":", 1)
        return name.strip(), type_value.strip()
    matched = re.match(r"^([^()]+?)\((.*)\)$", token)
    if matched and not matched.group(1).strip().casefold().startswith(("decimal", "numeric", "varchar", "char")):
        return matched.group(1).strip(), matched.group(2).strip()
    return token, ""


def _normalise_column(item: Any) -> dict[str, str]:
    """把各种字段结构描述统一成 ``{"name": ..., "type": ...}``。"""
    if isinstance(item, str):
        name, type_value = _split_name_type(item)
        return {"name": name, "type": type_value}
    if isinstance(item, dict):
        name = str(
            item.get("name")
            or item.get("字段名称")
            or item.get("字段")
            or item.get("column")
            or item.get("field")
            or ""
        ).strip()
        type_value = (
            item.get("type")
            or item.get("字段类型")
            or item.get("data_type")
            or item.get("dataType")
            or item.get("类型")
            or ""
        )
        if not type_value:
            precision = item.get("precision") or item.get("精度")
            scale = item.get("scale") or item.get("小数位")
            if precision not in (None, ""):
                type_value = f"decimal({precision},{scale if scale not in (None, '') else 0})"
        return {"name": name, "type": str(type_value).strip()}
    return {"name": str(item).strip(), "type": ""}


def parse_schema(value: Any) -> list[dict[str, str]]:
    """解析版本摘要/变更值里的字段结构，保持原始列次序。

    支持 ``id,gross:decimal(10,2),amount``、JSON 数组、字段字典、
    ``{"gross": "decimal(10,2)"}`` 等常见写法。
    """
    if value is None:
        return []
    if isinstance(value, dict):
        if isinstance(value.get("columns"), list):
            return [col for col in (_normalise_column(item) for item in value["columns"]) if col["name"]]
        if isinstance(value.get("fields"), list):
            return [col for col in (_normalise_column(item) for item in value["fields"]) if col["name"]]
        if all(not isinstance(item, (dict, list)) for item in value.values()):
            # 形如 {"gross": "decimal(10,2)"} 的映射
            return [{"name": str(name).strip(), "type": str(type_value).strip()}
                    for name, type_value in value.items() if str(name).strip()]
        return [col for col in (_normalise_column(item) for item in value.values()) if col["name"]]
    if isinstance(value, (list, tuple)):
        return [col for col in (_normalise_column(item) for item in value) if col["name"]]
    text = str(value).strip()
    if not text or text in ("-", "—", "无"):
        return []
    if text.startswith(("{", "[")):
        try:
            return parse_schema(json.loads(text))
        except (ValueError, TypeError):
            pass
    return [col for col in (_normalise_column(part) for part in _split_top_level(text)) if col["name"]]


def _is_placeholder(summary: Any, columns: list[dict[str, str]]) -> bool:
    """没有真实结构时的占位描述（如“资产X快照”）不应覆盖已知结构。"""
    text = str(summary).strip()
    if not text or text in ("-", "—", "无"):
        return True
    return (
        len(columns) == 1
        and not columns[0].get("type")
        and (columns[0].get("name") == text and text.endswith("快照"))
    )


def _looks_like_schema(value: Any) -> bool:
    """判断一个变更值是不是“整段字段结构”而不是单个值。"""
    if isinstance(value, (dict, list, tuple)):
        return True
    text = str(value).strip()
    if text.startswith(("{", "[")):
        return True
    return len(_split_top_level(text)) > 1


def _column_map(columns: Iterable[dict[str, str]]) -> dict[str, dict[str, str]]:
    return {col["name"]: col for col in columns if col.get("name")}


def _kind_matches(text: str, words: tuple[str, ...]) -> bool:
    lowered = text.casefold()
    return any(word.casefold() in lowered for word in words)


def _same_type(left: Any, right: Any) -> bool:
    return str(left or "").strip().casefold() == str(right or "").strip().casefold()


# ── 取数状态 ──────────────────────────────────────────────────


def _state() -> dict[str, Any]:
    state = store.get_pipeline_state(STATE_KEY)
    if state is None:
        state = {"next_seq": 1, "assets": {}}
        store.set_pipeline_state(STATE_KEY, state)
    state.setdefault("next_seq", 1)
    state.setdefault("assets", {})
    return state


def _next_seq(state: dict[str, Any]) -> int:
    seq = int(state.get("next_seq", 1))
    state["next_seq"] = seq + 1
    return seq


def _asset_state(asset_code: str) -> dict[str, Any]:
    entry = _state()["assets"].get(asset_code)
    if entry is None:
        entry = {
            "asset": asset_code,
            "boundaries": [],
            "events": [],
            "aliases": {},
            "current_schema": [],
            "last_field": "",
            "known_row_ids": set(),
        }
        _state()["assets"][asset_code] = entry
    return entry


def _stable_alias(entry: dict[str, Any], name: str) -> str:
    """改名后仍把后续变化归到同一列的稳定标识。"""
    return entry["aliases"].get(name, name)


def _remember_alias(entry: dict[str, Any], name: str, stable: str) -> None:
    if name:
        entry["aliases"][name] = stable


def _find_column(entry: dict[str, Any], name: str) -> dict[str, str] | None:
    if not name:
        return None
    for column in entry["current_schema"]:
        if column["name"] == name:
            return column
    stable = _stable_alias(entry, name)
    for column in entry["current_schema"]:
        if _stable_alias(entry, column["name"]) == stable:
            return column
    return None


# ── 版本边界 ──────────────────────────────────────────────────


def register_version(
    asset_code: str,
    version_tag: str,
    schema_summary: Any = "",
    *,
    snapshot_code: str = "",
    row_id: int | None = None,
    created_at: str = "",
    reason: str = "",
    lazy: bool = False,
) -> dict[str, Any]:
    """登记一条不可变的版本边界，供后续按“指定两版”复现比较结果。

    ``lazy=True`` 表示这条边界是从已有表数据补登记的，没有同步登记的事件，
    比较时只按两版结构本身求差，避免把无关事件算进来。
    """
    entry = _asset_state(asset_code)
    columns = parse_schema(schema_summary)
    # 摘要为空或只是占位说明时，沿用当前已知结构，避免边界把结构清空。
    if _is_placeholder(schema_summary, columns):
        columns = [dict(col) for col in entry.get("current_schema", [])]
    else:
        # 新边界的结构是权威快照；已登记的名称保留其稳定标识，
        # 这样跨边界改名的列在后续事件里仍能被认成同一列。
        for col in columns:
            _remember_alias(entry, col["name"], entry["aliases"].get(col["name"], col["name"]))

    boundary = {
        "asset": asset_code,
        "tag": version_tag,
        "snapshot_code": snapshot_code,
        "row_id": row_id,
        "reason": reason,
        "seq": _next_seq(_state()),
        "created_at": created_at,
        "schema": [dict(col) for col in columns],
        "lazy": lazy,
        # 兼容存储层快照 id 作为比较端点。
        "extra_refs": [
            ref for ref in (
                f"version-{version_tag}-{asset_code}",
                f"version-{version_tag}",
            ) if ref
        ],
    }
    _insert_boundary(entry, boundary)
    if row_id is not None:
        entry["known_row_ids"].add(row_id)
    # 只有位于时间线末尾的边界才代表“当前结构”；补登记的历史边界
    # 不能覆盖边界之后已经发生的修改。
    if entry["boundaries"][-1] is boundary:
        entry["current_schema"] = [dict(col) for col in columns]
        entry["last_field"] = ""
    return boundary


def _insert_boundary(entry: dict[str, Any], boundary: dict[str, Any]) -> None:
    """把边界按版本表行 id 插到时间线上的正确位置。

    补登记的边界可能晚于已有事件才被发现，插入中间时用相邻边界序号的
    中点作为位置，保证“哪一版更早”仍由版本表次序决定。
    """
    boundaries = entry["boundaries"]
    row_id = boundary.get("row_id")
    if row_id is not None:
        for index, existing in enumerate(boundaries):
            existing_row = existing.get("row_id")
            if existing_row is not None and int(existing_row) > int(row_id):
                prev_seq = float(boundaries[index - 1]["seq"]) if index > 0 else 0.0
                next_seq = float(existing["seq"])
                boundary["seq"] = (prev_seq + next_seq) / 2
                boundaries.insert(index, boundary)
                return
    boundaries.append(boundary)


def _seq_of(boundary: dict[str, Any] | None) -> float:
    return float(boundary.get("seq", 0)) if boundary is not None else 0.0


def _find_boundary(entry: dict[str, Any], version_ref: Any) -> dict[str, Any] | None:
    ref = str(version_ref).strip()
    if not ref:
        return None
    boundaries = entry.get("boundaries") or []
    # 快照编号/行 id 比版本标签更精确，版本标签重复时优先使用它们。
    for boundary in boundaries:
        if str(boundary.get("snapshot_code") or "") == ref:
            return boundary
        if ref in {str(item) for item in boundary.get("extra_refs", ())}:
            return boundary
    for boundary in boundaries:
        if str(boundary.get("row_id") or "") == ref:
            return boundary
    for boundary in boundaries:
        if str(boundary.get("tag") or "") == ref:
            return boundary
    return None


def _current_boundary(entry: dict[str, Any]) -> dict[str, Any]:
    """把“当前状态”投影成一个位于所有事件之后的虚拟边界。"""
    position = float(_state().get("next_seq", 1))
    for event in entry.get("events", []):
        position = max(position, float(event.get("seq", 0)) + 1)
    for boundary in entry.get("boundaries", []):
        position = max(position, _seq_of(boundary) + 1)
    return {
        "asset": entry["asset"],
        "tag": "current",
        "snapshot_code": "",
        "row_id": None,
        "reason": "当前状态",
        "seq": position,
        "created_at": "",
        "schema": [dict(col) for col in entry["current_schema"]],
        "current": True,
    }


def _resolve_boundary(entry: dict[str, Any], version_ref: Any) -> dict[str, Any] | None:
    if str(version_ref).strip().casefold() in _CURRENT_REFS:
        return _current_boundary(entry)
    boundary = _find_boundary(entry, version_ref)
    if boundary is not None:
        return boundary
    return _boundary_from_change(entry, version_ref)


def _schema_at(entry: dict[str, Any], position: float) -> list[dict[str, str]]:
    """重建某一位置上的字段结构：从最近的边界开始重放事件。"""
    boundaries = entry.get("boundaries") or []
    anchor = None
    for boundary in boundaries:
        if _seq_of(boundary) < position:
            anchor = boundary
        else:
            break
    start = anchor["schema"] if anchor is not None else (boundaries[0]["schema"] if boundaries else [])
    start_seq = _seq_of(anchor) if anchor is not None else 0.0
    columns = [{"name": col["name"], "type": col.get("type", "")} for col in start]
    replay = {
        "current_schema": columns,
        "aliases": {},
        "boundaries": [],
        "events": [],
        "asset": entry["asset"],
    }
    for event in sorted(entry.get("events", []), key=lambda item: float(item.get("seq", 0))):
        if start_seq < float(event.get("seq", 0)) < position:
            _apply_event(replay, event)
    return [dict(col) for col in columns]


def _boundary_from_change(entry: dict[str, Any], version_ref: Any) -> dict[str, Any] | None:
    """允许用变更记录编号/存储快照 id 作为比较端点：位置取在该笔变更之后。"""
    ref = str(version_ref).strip()
    if not ref:
        return None
    for row in store.rows("change_capture"):
        if row.get("目标资产") != entry["asset"]:
            continue
        change_id = int(row.get("id", 0))
        accepted = {
            str(row.get("记录编号", "")),
            str(change_id),
            f"change-{change_id}",
        }
        if ref not in accepted:
            continue
        matching = [
            event for event in entry.get("events", [])
            if event.get("change_id") == change_id
        ]
        if not matching:
            continue
        position = max(int(event.get("seq", 0)) for event in matching) + 1
        return {
            "asset": entry["asset"],
            "tag": ref,
            "snapshot_code": "",
            "row_id": None,
            "reason": "变更记录",
            "seq": position,
            "created_at": str(row.get("变更时间", "")),
            "schema": _schema_at(entry, position),
        }
    return None


def _sync_versions_from_store(asset_code: str) -> None:
    """兼容直接写 ``version_history`` 表的调用方，惰性补登记边界。"""
    entry = _asset_state(asset_code)
    rows = [
        row for row in store.rows("version_history")
        if row.get("目标资产") == asset_code
    ]
    for row in sorted(rows, key=lambda item: int(item.get("id", 0))):
        row_id = int(row.get("id", 0))
        if row_id and row_id in entry["known_row_ids"]:
            continue
        tag = str(row.get("版本标签", ""))
        reason = str(row.get("创建原因", ""))
        summary = str(row.get("Schema摘要", "")).strip()
        # 自动快照/空摘要是存储层的附属产物，不构成资产版本边界。
        if reason in _SKIP_REASONS or tag.startswith(_SKIP_TAGS):
            continue
        if summary in ("", "[]", "-"):
            continue
        register_version(
            asset_code,
            tag,
            summary,
            snapshot_code=str(row.get("快照编号", "")),
            row_id=row_id,
            created_at=str(row.get("快照时间", "")),
            reason=reason,
            lazy=True,
        )


def current_schema(asset_code: str) -> list[dict[str, str]]:
    """读取资产当前有效的字段结构（包含最近一次边界之后的修改）。"""
    _sync_versions_from_store(asset_code)
    entry = _asset_state(asset_code)
    return [dict(col) for col in entry["current_schema"]]


def get_version_schema(asset_code: str, version_ref: str) -> list[dict[str, str]] | None:
    """按版本标签、快照编号或变更记录读取边界字段结构。"""
    _sync_versions_from_store(asset_code)
    boundary = _resolve_boundary(_asset_state(asset_code), version_ref)
    if boundary is None:
        return None
    return [dict(col) for col in boundary["schema"]]


# ── 结构对比（纯函数） ────────────────────────────────────────


def _diff_schemas(
    before_schema: list[dict[str, str]],
    after_schema: list[dict[str, str]],
) -> list[dict[str, Any]]:
    """整段结构对比：同名看类型，剩余删除/新增按次序配对成改名。

    返回不带 seq 的规格列表，调用方决定是登记事件还是直接渲染。
    """
    before_map = _column_map(before_schema)
    after_map = _column_map(after_schema)
    removed = [col for col in before_schema if col["name"] not in after_map]
    added = [col for col in after_schema if col["name"] not in before_map]
    rename_pairs = list(zip(removed, added))
    paired_old = {old["name"] for old, _ in rename_pairs}
    paired_new = {new["name"] for _, new in rename_pairs}

    specs: list[tuple[int, dict[str, Any]]] = []
    for index, column in enumerate(before_schema):
        name = column["name"]
        if name in after_map:
            if not _same_type(column.get("type"), after_map[name].get("type")):
                specs.append((index, {
                    "kind": "precision", "field_name": name,
                    "before": column.get("type", ""),
                    "after": after_map[name].get("type", ""),
                }))
            continue
        pair = next(((old, new) for old, new in rename_pairs if old["name"] == name), None)
        if pair is not None:
            old, new = pair
            specs.append((index, {
                "kind": "rename", "field_name": old["name"],
                "before": old["name"], "after": new["name"],
            }))
            if not _same_type(old.get("type"), new.get("type")):
                specs.append((index, {
                    "kind": "precision", "field_name": new["name"],
                    "before": old.get("type", ""),
                    "after": new.get("type", ""),
                }))
        elif name not in paired_old:
            specs.append((index, {
                "kind": "remove", "field_name": name,
                "before": name, "after": "",
            }))
    for offset, column in enumerate(after_schema):
        name = column["name"]
        if name in before_map or name in paired_new:
            continue
        specs.append((len(before_schema) + offset, {
            "kind": "add", "field_name": name,
            "before": "", "after": column.get("type", "") or name,
        }))

    return [
        {"order": index, **spec}
        for index, spec in sorted(specs, key=lambda item: item[0])
    ]


# ── 字段级变更事件 ────────────────────────────────────────────


def _add_type(event: dict[str, Any]) -> str:
    """新增事件的 after 可能是字段名（未给类型），此时列类型记为未知。"""
    after = str(event.get("after", ""))
    if not after or after == str(event.get("field_name", "")):
        return ""
    return after


def _apply_event(entry: dict[str, Any], event: dict[str, Any]) -> None:
    columns = entry["current_schema"]
    kind = event["kind"]
    field_name = event.get("field_name") or ""
    if kind == "rename":
        target = event.get("after", "")
        for column in columns:
            if column["name"] == field_name:
                column["name"] = target
                break
    elif kind == "add":
        if field_name and not any(column["name"] == field_name for column in columns):
            columns.append({"name": field_name, "type": _add_type(event)})
    elif kind == "remove":
        entry["current_schema"] = [
            column for column in columns if column["name"] != field_name
        ]
    elif kind == "precision":
        # 只有类型/精度变化才改写结构；普通字段值修改不影响字段结构。
        column = _find_column(entry, field_name)
        if column is not None:
            column["type"] = str(event.get("after", ""))
    if field_name:
        entry["last_field"] = event.get("after") if kind == "rename" else field_name


def _make_event(
    entry: dict[str, Any],
    *,
    kind: str,
    field_name: str,
    before: Any,
    after: Any,
    change_type: str,
    change_id: int | None = None,
    source: str = "",
    order: int | None = None,
) -> dict[str, Any]:
    stable = _stable_alias(entry, field_name)
    if kind == "rename" and after:
        _remember_alias(entry, str(after), stable)
    _remember_alias(entry, field_name, stable)
    event = {
        "seq": _next_seq(_state()),
        "order": order if order is not None else 0,
        "asset": entry["asset"],
        "field": stable,
        "field_name": field_name,
        "kind": kind,
        "before": "" if before is None else str(before),
        "after": "" if after is None else str(after),
        "change_type": change_type,
        "change_id": change_id,
        "source": source,
    }
    entry["events"].append(event)
    _apply_event(entry, event)
    return event


def _spec_to_event(
    entry: dict[str, Any],
    spec: dict[str, Any],
    *,
    change_type: str,
    change_id: int | None,
    source: str,
    seq: int | None = None,
    register: bool = False,
) -> dict[str, Any]:
    """把结构对比规格转成事件；register=True 时消费全局序号并落库。"""
    stable = _stable_alias(entry, spec["field_name"])
    if register:
        seq_value = _next_seq(_state())
    else:
        seq_value = 0 if seq is None else seq
    return {
        "seq": seq_value,
        "order": int(spec.get("order", 0)),
        "asset": entry["asset"],
        "field": stable,
        "field_name": spec["field_name"],
        "kind": spec["kind"],
        "before": str(spec.get("before", "")),
        "after": str(spec.get("after", "")),
        "change_type": change_type,
        "change_id": change_id,
        "source": source,
        "synthetic": not register,
    }


def _events_from_schema_diff(
    entry: dict[str, Any],
    before_schema: list[dict[str, str]],
    after_schema: list[dict[str, str]],
    *,
    change_type: str,
    change_id: int | None,
    source: str,
) -> list[dict[str, Any]]:
    produced = []
    for spec in _diff_schemas(before_schema, after_schema):
        event = _spec_to_event(
            entry, spec, change_type=change_type,
            change_id=change_id, source=source, register=True,
        )
        event.pop("synthetic", None)
        produced.append(event)
        entry["events"].append(event)
        _apply_event(entry, event)
    return produced


def _resolve_field(
    entry: dict[str, Any],
    explicit: str | None,
    before: Any,
    after: Any,
) -> str:
    if explicit:
        return str(explicit).strip()
    # 形如 gross:decimal(10,2)，字段名直接写在值里。
    for raw in (before, after):
        name, _ = _split_name_type(str(raw))
        if name and _find_column(entry, name) is not None:
            return name
    # 用“变更前/后的类型”反查当前结构，只有唯一命中时才认定。
    before_text = str(before) if before is not None else ""
    after_text = str(after) if after is not None else ""
    candidates = [
        column for column in entry["current_schema"]
        if (before_text and _same_type(before_text, column.get("type")))
        or (after_text and _same_type(after_text, column.get("type")))
    ]
    if len(candidates) == 1:
        return candidates[0]["name"]
    # 再尝试从值里出现的列名定位。
    for column in entry["current_schema"]:
        if column["name"] and column["name"] in before_text + after_text:
            return column["name"]
    if entry.get("last_field"):
        return str(entry["last_field"])
    # 只有一列带类型声明时（如 id,name 无类型），精度变化必然落在它上面。
    typed = [col for col in entry["current_schema"] if col.get("type")]
    if len(typed) == 1:
        return typed[0]["name"]
    non_key = [col for col in entry["current_schema"] if col["name"] not in _KEY_FIELD_NAMES]
    if len(non_key) == 1:
        return non_key[0]["name"]
    return ""


def _rename_parts(before_text: str, after_text: str) -> tuple[str, str] | None:
    """识别 ``旧名 -> 新名`` / ``旧名→新名`` 这类写法。"""
    arrow = re.split(r"\s*(?:->|→|=>)\s*", f"{before_text} {after_text}")
    if len(arrow) != 2:
        return None
    old, new = (part.strip() for part in arrow)
    if old and new and old != new:
        return old, new
    return None


def record_capture(
    asset_code: str,
    change_type: str,
    before: Any,
    after: Any,
    *,
    source: str = "schema_evolution",
    change_id: int | None = None,
    field: str | None = None,
) -> list[dict[str, Any]]:
    """把一次变更捕获拆成按次序排列的字段级事件并登记取数。

    支持整段结构（``id,gross:decimal(10,2)``）、单字段描述（``gross`` ->
    ``gross_amount``）、``{"gross": "decimal(10,2)"}`` 以及
    ``{"字段": "gross", "变更前值": ..., "变更后值": ...}``。
    既有 ``capture_change`` 的入参形式不需要改变。
    """
    entry = _asset_state(asset_code)

    # 结构描述对象：{"字段": ..., "变更前值": ..., "变更后值": ...}
    if isinstance(before, dict) and any(
        key in before for key in ("字段", "field", "字段名称", "name")
    ):
        field = field or str(
            before.get("字段") or before.get("field")
            or before.get("字段名称") or before.get("name") or ""
        )
        raw_before = before.get("变更前值", before.get("before", before.get("原值", before.get("old"))))
        if after is None:
            after = before.get("变更后值", before.get("after", before.get("新值", before.get("new"))))
        before = raw_before
    if isinstance(after, dict) and any(
        key in after for key in ("字段", "field", "字段名称", "name")
    ):
        field = field or str(
            after.get("字段") or after.get("field")
            or after.get("字段名称") or after.get("name") or ""
        )
        after = after.get("变更后值", after.get("after", after.get("新值", after.get("new"))))

    # 整段字段结构的变化，直接按列对比。
    if _looks_like_schema(before) and _looks_like_schema(after):
        before_schema = parse_schema(before)
        after_schema = parse_schema(after)
        if before_schema or after_schema:
            return _events_from_schema_diff(
                entry, before_schema, after_schema,
                change_type=change_type, change_id=change_id, source=source,
            )

    before_text = "" if before is None else str(before)
    after_text = "" if after is None else str(after)
    before_name, before_type = _split_name_type(before_text)
    after_name, after_type = _split_name_type(after_text)
    kind_text = f"{change_type} {field or ''}"

    # “gross -> gross_amount”写法直接按改名处理。
    arrow = _rename_parts(before_text, after_text)
    if arrow is not None and not _kind_matches(kind_text, _PRECISION_WORDS):
        return [_make_event(
            entry, kind="rename", field_name=arrow[0],
            before=arrow[0], after=arrow[1],
            change_type=change_type, change_id=change_id, source=source,
        )]

    # name:type 形式：同名即精度/类型变化，异名同时包含改名。
    if ":" in before_text and ":" in after_text and before_name and after_name:
        events: list[dict[str, Any]] = []
        renamed = before_name != after_name
        if renamed:
            events.append(_make_event(
                entry, kind="rename", field_name=before_name,
                before=before_name, after=after_name,
                change_type=change_type, change_id=change_id, source=source,
            ))
        if not _same_type(before_type, after_type):
            if not renamed:
                # 同一列的精度调整只记新旧类型，不重复记字段名。
                events.append(_make_event(
                    entry, kind="precision", field_name=before_name,
                    before=before_type, after=after_type,
                    change_type=change_type, change_id=change_id, source=source,
                ))
            else:
                events.append(_make_event(
                    entry, kind="precision", field_name=after_name,
                    before=before_type, after=after_type,
                    change_type=change_type, change_id=change_id, source=source,
                ))
        if events:
            return events

    if _kind_matches(kind_text, _RENAME_WORDS):
        old_name = str(field or "").strip() or before_name or entry.get("last_field", "")
        new_name = after_name if ":" in after_text or after_name else after_text
        if old_name and new_name and old_name != new_name:
            return [_make_event(
                entry, kind="rename", field_name=old_name,
                before=old_name, after=new_name,
                change_type=change_type, change_id=change_id, source=source,
            )]

    # 没写“改名”但给了两个同类型的字段名，按改名处理（结构变化里最常见）。
    if (
        not _kind_matches(kind_text, _PRECISION_WORDS + _ADD_WORDS + _REMOVE_WORDS)
        and before_type and after_type
        and _same_type(before_type, after_type)
        and before_name != after_name
        and _find_column(entry, before_name) is not None
        and _find_column(entry, after_name) is None
    ):
        return [_make_event(
            entry, kind="rename", field_name=before_name,
            before=before_name, after=after_name,
            change_type=change_type, change_id=change_id, source=source,
        )]

    resolved = _resolve_field(entry, field or None, before, after)

    if _kind_matches(kind_text, _ADD_WORDS):
        name = str(field or "").strip() or after_name or resolved or after_text
        return [_make_event(
            entry, kind="add", field_name=name,
            before="", after=after if after is not None else name,
            change_type=change_type, change_id=change_id, source=source,
        )]

    if _kind_matches(kind_text, _REMOVE_WORDS):
        name = str(field or "").strip() or before_name or resolved or before_text
        return [_make_event(
            entry, kind="remove", field_name=name,
            before=before if before is not None else name, after="",
            change_type=change_type, change_id=change_id, source=source,
        )]

    # 精度/类型变化：调用方可能只给一个类型值，另一侧从当前结构/边界补全。
    if _kind_matches(kind_text, _PRECISION_WORDS):
        column = _find_column(entry, resolved)
        current_type = column.get("type", "") if column is not None else ""
        before_value, after_value = before, after
        if before in (None, "") and after not in (None, ""):
            before_value = current_type
        if after in (None, "") and before not in (None, ""):
            after_value = current_type
        return [_make_event(
            entry, kind="precision", field_name=resolved,
            before=before_value, after=after_value,
            change_type=change_type, change_id=change_id, source=source,
        )]

    # 兜底：按普通字段修改登记，仍保留原值/新值；缺的一侧从当前结构补。
    column = _find_column(entry, resolved)
    if before in (None, "") and after not in (None, "") and column is not None:
        before = column.get("type", "") or before
    elif after in (None, "") and before not in (None, "") and column is not None:
        after = column.get("type", "") or after
    return [_make_event(
        entry, kind="modify", field_name=resolved or change_type,
        before=before, after=after,
        change_type=change_type, change_id=change_id, source=source,
    )]


def capture_field_change(
    asset_code: str,
    field: str,
    change_type: str,
    before: Any,
    after: Any,
    *,
    source: str = "field_structure",
    change_id: int | None = None,
) -> dict[str, Any]:
    """显式登记一笔字段结构变化，供需要精确字段名的调用方使用。"""
    events = record_capture(
        asset_code, change_type, before, after,
        source=source, change_id=change_id, field=field,
    )
    return events[0] if events else {}


def collect_change_events(
    asset_code: str,
    change_id: int,
) -> list[dict[str, Any]]:
    """读取某笔变更捕获登记下来的字段级事件（按原次序）。"""
    _sync_versions_from_store(asset_code)
    entry = _asset_state(asset_code)
    events = [
        event for event in entry.get("events", [])
        if event.get("change_id") == change_id
    ]
    events.sort(key=lambda event: (int(event.get("seq", 0)), int(event.get("order", 0))))
    return events


# ── 取数与渲染 ────────────────────────────────────────────────


def _replay_schema(
    entry: dict[str, Any],
    base_schema: list[dict[str, str]],
    events: list[dict[str, Any]],
) -> list[dict[str, str]]:
    """在起始结构上按次序重放事件，得到事件实际造成的结构。"""
    columns = [{"name": col["name"], "type": col.get("type", "")} for col in base_schema]
    # 起始结构里的列名可能是改名后的名字，用取数记录里的稳定标识对齐事件。
    stable_of = {
        col["name"]: entry["aliases"].get(col["name"], col["name"])
        for col in columns
    }

    def find(stable: str) -> dict[str, str] | None:
        for column in columns:
            if stable_of.get(column["name"]) == stable:
                return column
        # 事件登记的稳定标识可能早于当前别名表，退化为按当前列名匹配。
        for column in columns:
            if column["name"] == stable:
                return column
        return None

    for event in sorted(events, key=lambda item: (int(item.get("seq", 0)), int(item.get("order", 0)))):
        stable = event.get("field") or event.get("field_name", "")
        kind = event.get("kind", "modify")
        if kind == "rename":
            column = find(stable)
            if column is not None:
                stable_of.pop(column["name"], None)
                column["name"] = event.get("after", column["name"])
                stable_of[column["name"]] = stable
        elif kind == "add":
            if not any(column["name"] == event.get("field_name") for column in columns):
                name = event.get("field_name", "")
                columns.append({"name": name, "type": _add_type(event)})
                stable_of[name] = stable or name
        elif kind == "remove":
            column = find(stable)
            if column is not None:
                stable_of.pop(column["name"], None)
                columns.remove(column)
        elif kind == "precision":
            column = find(stable)
            if column is not None:
                column["type"] = str(event.get("after", ""))
    return columns


def _merge_uncovered_net(
    entry: dict[str, Any],
    events: list[dict[str, Any]],
    before_schema: list[dict[str, str]],
    after_schema: list[dict[str, str]],
) -> list[dict[str, Any]]:
    """事件重放后仍与目标边界不符的净变化补进来，避免真实变化被漏掉。

    以“事件实际造成的结构”对比目标边界，而不是按字段名去猜覆盖关系，
    这样改名后同一列的精度变化不会被重复展示。
    """
    replayed = _replay_schema(entry, before_schema, events)
    specs = _diff_schemas(replayed, after_schema)
    if not specs:
        return events
    merged = list(events)
    base = max((int(event.get("seq", 0)) for event in events), default=0)
    for index, spec in enumerate(specs):
        merged.append(_spec_to_event(
            entry, spec, change_type="字段结构比较",
            change_id=None, source="version_boundary", seq=base + index + 1,
        ))
    return merged


def collect_version_changes(
    asset_code: str,
    version_a: str,
    version_b: str,
) -> dict[str, Any]:
    """读取“指定两版”之间的字段结构变化取数记录。

    返回 ``{"events": [...], "reverse": bool, "boundary_a": ..., "boundary_b": ...}``。
    事件只在两版边界之间取，因此即便之后又写入新版本/新变更，本次比较仍可复现。
    """
    _sync_versions_from_store(asset_code)
    entry = _asset_state(asset_code)
    boundary_a = _resolve_boundary(entry, version_a)
    boundary_b = _resolve_boundary(entry, version_b)
    reverse = bool(
        boundary_a is not None and boundary_b is not None
        and _seq_of(boundary_a) > _seq_of(boundary_b)
    )
    events = list(entry.get("events", []))

    if boundary_a is not None and boundary_b is not None:
        # 边界和事件共用同一条序号时间线，按事件位置截取即可；
        # 早于 A、晚于 B 的修改都不进入本次比较，重复提交或后续写入不影响结果。
        low, high = sorted((_seq_of(boundary_a), _seq_of(boundary_b)))
        events = [
            event for event in events
            if low < float(event.get("seq", 0)) < high
        ]
    elif boundary_a is not None or boundary_b is not None:
        anchor = boundary_a if boundary_a is not None else boundary_b
        events = [
            event for event in events
            if float(event.get("seq", 0)) > _seq_of(anchor)
        ]
    else:
        events = []

    events.sort(key=lambda event: (float(event.get("seq", 0)), int(event.get("order", 0))))

    if boundary_a is not None and boundary_b is not None:
        low_boundary, high_boundary = sorted(
            (boundary_a, boundary_b), key=_seq_of,
        )
        # 补登记的边界没有配套事件，只按两版结构本身求差。
        if low_boundary.get("lazy") or high_boundary.get("lazy"):
            events = []
        if events:
            events = _merge_uncovered_net(
                entry, events, low_boundary["schema"], high_boundary["schema"],
            )
        else:
            events = [
                _spec_to_event(
                    entry, spec, change_type="字段结构比较",
                    change_id=None, source="version_boundary", seq=index + 1,
                )
                for index, spec in enumerate(
                    _diff_schemas(low_boundary["schema"], high_boundary["schema"])
                )
            ]
    else:
        # 任一侧版本无法解析时不做臆断，与原单次查询一致返回空明细。
        events = []

    return {
        "events": events,
        "reverse": reverse,
        "boundary_a": boundary_a,
        "boundary_b": boundary_b,
    }


def render_field_changes(
    asset_code: str,
    events: list[dict[str, Any]],
    *,
    reverse: bool = False,
) -> list[dict[str, Any]]:
    """按比较方向渲染字段差异，保持原有单次查询的六字段返回形式。

    明细次序始终按事件发生的先后，同一列的多笔修改也保持原次序；
    倒序比较只交换源值与目标值、对调新增/删除等级，差异字段仍取该笔
    修改原来的列名，因此同一笔字段修改在正反两个方向都能对上。
    """
    diffs: list[dict[str, Any]] = []
    for event in events:
        before = event.get("before", "")
        after = event.get("after", "")
        if reverse:
            before, after = after, before
        kind = event.get("kind", "modify")
        if kind == "add":
            level = "删除" if reverse else "新增"
        elif kind == "remove":
            level = "新增" if reverse else "删除"
        else:
            level = "修改"
        diffs.append({
            "比较对象": asset_code,
            "比较维度": "字段结构",
            "差异字段": event.get("field_name", ""),
            "源值": before,
            "目标值": after,
            "差异等级": level,
        })
    return diffs


def query_field_changes(
    asset_code: str,
    version_a: str,
    version_b: str,
) -> list[dict[str, Any]]:
    """共用入口：按两版取数并渲染为字段差异明细。"""
    collected = collect_version_changes(asset_code, version_a, version_b)
    return render_field_changes(
        asset_code, collected["events"], reverse=collected["reverse"],
    )


def has_version_boundary(asset_code: str, version_ref: str) -> bool:
    """判断一个引用在共享取数记录里是否有对应的版本边界。"""
    _sync_versions_from_store(asset_code)
    return _resolve_boundary(_asset_state(asset_code), version_ref) is not None


# ── 比较结果落盘（重复提交只留一组） ──────────────────────────


def _comparison_state() -> dict[str, Any]:
    state = store.get_pipeline_state(COMPARISON_KEY)
    if state is None:
        state = {"reports": {}}
        store.set_pipeline_state(COMPARISON_KEY, state)
    state.setdefault("reports", {})
    return state


def _comparison_key(asset_code: str, version_a: str, version_b: str) -> str:
    # 同一对版本只保留一组字段差异：重复提交（含反向重提）都原位更新，
    # 差异汇总的内容跟随最近一次比较方向。
    first, second = sorted((str(version_a), str(version_b)))
    return f"{asset_code}|{first}|{second}"


def persist_comparison(
    asset_code: str,
    version_a: str,
    version_b: str,
    diffs: list[dict[str, Any]],
) -> list[int]:
    """把一次比较的差异写入差异报告表；重复提交时原位更新而非追加。"""
    state = _comparison_state()
    key = _comparison_key(asset_code, version_a, version_b)
    previous_ids = [int(item) for item in state["reports"].get(key, [])]
    rows = store.rows("diff_report")
    by_id = {int(row.get("id", 0)): row for row in rows}
    previous_rows = [by_id[item] for item in previous_ids if item in by_id]

    report_ids: list[int] = []
    for index, diff in enumerate(diffs):
        values = {
            "比较对象": diff.get("比较对象", asset_code),
            "比较维度": diff.get("比较维度", ""),
            "差异字段": diff.get("差异字段", ""),
            "源值": diff.get("源值", ""),
            "目标值": diff.get("目标值", ""),
            "差异等级": diff.get("差异等级", "修改"),
            "报告时间": _now(),
            "status": "已生成",
            "pending": False,
            "abnormal": False,
        }
        if index < len(previous_rows):
            report = previous_rows[index]
            report.update(values)
        else:
            report_id = store.next_id("diff_report")
            report = {
                "id": report_id,
                "记录编号": f"DIFF-{report_id:04d}",
                **values,
            }
            rows.append(report)
        report_ids.append(int(report["id"]))

    # 上次的差异更多时，删掉多出来的那部分，确保只留一组。
    keep_ids = set(report_ids)
    stale_ids = {int(row.get("id", 0)) for row in previous_rows} - keep_ids
    if stale_ids:
        store.rows("diff_report")[:] = [
            row for row in rows if int(row.get("id", 0)) not in stale_ids
        ]

    state["reports"][key] = report_ids
    return report_ids


def _now() -> str:
    return _time.strftime("%Y-%m-%d %H:%M:%S")
