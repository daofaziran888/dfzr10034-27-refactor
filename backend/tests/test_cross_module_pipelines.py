"""跨模块数据流测试：验证9条数据管道的端到端行为。

关键原则：
- 每个测试使用唯一的资产编号避免相互干扰
- 使用 store.reset() 确保每个测试干净启动
- 通过业务字段查找新建的记录，不硬编码 entry_id
"""
import app.store as store_mod
from app.services.asset_registry import AssetRegistryService
from app.services.metadata_index import MetadataIndexService
from app.services.quality_rule import QualityRuleService
from app.services.profile_stats import ProfileStatsService
from app.services.classification import ClassificationService
from app.services.lifecycle_policy import LifecyclePolicyService
from app.services.cold_archive import ColdArchiveService
from app.services.retention_job import RetentionJobService
from app.services.data_anonym import DataAnonymService
from app.services.operation_log import OperationLogService
from app.services.change_capture import ChangeCaptureService
from app.services.lineage_track import LineageTrackService
from app.services.impact_analysis import ImpactAnalysisService
from app.services.tag_registry import TagRegistryService
from app.services.govern_score import GovernScoreService
from app.services.data_contract import DataContractService
from app.services.compliance_scan import ComplianceScanService
from app.services.issue_track import IssueTrackService
from app.services.version_history import VersionHistoryService
from app.services.schedule_job import ScheduleJobService
from app.services.diff_report import DiffReportService
from app.services.owner_mgr import OwnerMgrService


def _fresh():
    """重置存储并返回服务实例字典。"""
    store_mod.store.reset()
    return {
        "asset_registry": AssetRegistryService(),
        "metadata_index": MetadataIndexService(),
        "quality_rule": QualityRuleService(),
        "profile_stats": ProfileStatsService(),
        "classification": ClassificationService(),
        "lifecycle_policy": LifecyclePolicyService(),
        "cold_archive": ColdArchiveService(),
        "retention_job": RetentionJobService(),
        "data_anonym": DataAnonymService(),
        "operation_log": OperationLogService(),
        "change_capture": ChangeCaptureService(),
        "lineage_track": LineageTrackService(),
        "impact_analysis": ImpactAnalysisService(),
        "tag_registry": TagRegistryService(),
        "govern_score": GovernScoreService(),
        "data_contract": DataContractService(),
        "compliance_scan": ComplianceScanService(),
        "issue_track": IssueTrackService(),
        "version_history": VersionHistoryService(),
        "schedule_job": ScheduleJobService(),
        "diff_report": DiffReportService(),
        "owner_mgr": OwnerMgrService(),
    }


def _find_by_code(module: str, field: str, value: str) -> list:
    """按业务字段查找记录。"""
    return [r for r in store_mod.store.rows(module) if r.get(field) == value]


def _first_by_code(module: str, field: str, value: str):
    """按业务字段查找第一条记录。"""
    items = _find_by_code(module, field, value)
    return items[0] if items else None


# ══════════════════════════════════════════════════════════════════
# Pipeline 1: 资产注册 → 元数据发现 → 索引
# ══════════════════════════════════════════════════════════════════

class TestPipeline1AssetToMetadata:

    def test_register_asset_auto_discovers_metadata(self):
        s = _fresh()
        result = s["asset_registry"].register_asset({
            "资产编号": "T1-0001", "资产名称": "交易订单表",
            "资产类型": "结构化数据表", "所属系统": "交易系统",
            "存储位置": "hdfs://datalake/orders", "负责人": "张三",
            "保密等级": "内部", "注册时间": "2026-11-17",
        })
        assert result["ok"] is True
        assert result["asset"]["资产编号"] == "T1-0001"
        assert result["asset"]["status"] == "已注册"
        assert len(result["metadata_entries"]) >= 1

    def test_register_asset_stores_metadata_in_metadata_index(self):
        s = _fresh()
        s["asset_registry"].register_asset({
            "资产编号": "T1-0002", "资产名称": "用户画像表", "资产类型": "宽表",
        })
        entries = _find_by_code("metadata_index", "所属资产", "T1-0002")
        assert len(entries) >= 1
        assert entries[0]["status"] == "已索引"

    def test_get_asset_metadata_returns_correct_fields(self):
        s = _fresh()
        s["asset_registry"].register_asset({
            "资产编号": "T1-0003", "资产名称": "流量日志表", "资产类型": "日志数据",
        })
        metadata = _find_by_code("metadata_index", "所属资产", "T1-0003")
        assert len(metadata) >= 1
        for m in metadata:
            assert m["所属资产"] == "T1-0003"


# ══════════════════════════════════════════════════════════════════
# Pipeline 2: 数据画像 → 分类 → 敏感标签
# ══════════════════════════════════════════════════════════════════

class TestPipeline2ProfileToClassification:

    def test_profile_triggers_sensitive_field_detection(self):
        s = _fresh()
        sample_data = [
            {"id": "1", "姓名": "张三", "手机号": "13800138000",
             "身份证号": "110101199001011234", "地址": "北京市", "金额": "100.00"},
            {"id": "2", "姓名": "李四", "手机号": "13900139000",
             "身份证号": "110102199102021234", "地址": "上海市", "金额": "200.00"},
        ]
        result = s["profile_stats"].profile_dataset("T2_ASSET", sample_data)
        assert result["profile"]["status"] == "可用"
        assert result["profile"]["目标资产"] == "T2_ASSET"
        labels = result["classification_labels"]
        assert len(labels) >= 1
        field_targets = [str(l["目标字段"]) for l in labels]
        assert any("手机号" in t for t in field_targets)
        assert any("身份证" in t for t in field_targets)

    def test_sensitive_fields_stored_in_classification(self):
        s = _fresh()
        sample_data = [
            {"姓名": "赵六", "邮箱": "zhao@example.com", "手机号": "13600136000"},
        ]
        s["profile_stats"].profile_dataset("T2_ASSET2", sample_data)
        # 查找对应的分级标签
        labels = [c for c in store_mod.store.rows("classification")
                   if "T2_ASSET2" in str(c.get("目标字段", ""))]
        assert len(labels) >= 1
        phone = [c for c in labels if "手机号" in str(c.get("目标字段", ""))]
        assert len(phone) >= 1
        assert phone[0]["敏感等级"] == "高"


# ══════════════════════════════════════════════════════════════════
# Pipeline 3: 质量规则 → 监控 → 问题追踪
# ══════════════════════════════════════════════════════════════════

class TestPipeline3QualityToIssue:

    def test_quality_breach_creates_issue(self):
        s = _fresh()
        s["quality_rule"].define_rule("T3_ASSET", "完整性", "90", "100", "阻断")
        rules = _find_by_code("quality_rule", "目标资产", "T3_ASSET")
        assert len(rules) >= 1
        result = s["quality_rule"].execute_check(rules[0]["id"], 70.0)
        assert result["breached"] is True
        assert result.get("issue_created") is True
        issues = _find_by_code("issue_track", "关联资产", "T3_ASSET")
        assert len(issues) >= 1
        assert "完整性" in issues[0]["问题类型"]

    def test_quality_not_breached_no_issue(self):
        s = _fresh()
        s["quality_rule"].define_rule("T3_ASSET2", "准确性", "80", "100", "阻断")
        rules = _find_by_code("quality_rule", "目标资产", "T3_ASSET2")
        result = s["quality_rule"].execute_check(rules[0]["id"], 95.0)
        assert result["breached"] is False


# ══════════════════════════════════════════════════════════════════
# Pipeline 4: 生命周期 → 冷归档 → 保留清理
# ══════════════════════════════════════════════════════════════════

class TestPipeline4LifecycleToArchive:

    def _create_policy(self, s, code, asset):
        s["lifecycle_policy"].create_entry({
            "策略编号": code, "目标资产": asset, "创建阶段": "数据准备阶段",
        })
        return _first_by_code("lifecycle_policy", "策略编号", code)

    def test_lifecycle_archive_stage_triggers_cold_archive(self):
        s = _fresh()
        policy = self._create_policy(s, "LIFE-TA-001", "T4_ASSET")
        result = s["lifecycle_policy"].transition_stage(policy["id"], "归档阶段")
        assert result["ok"] is True
        assert "cold_archive" in result["actions"]
        assert result["cold_archive"]["源资产"] == "T4_ASSET"
        assert result["cold_archive"]["status"] == "待执行"

    def test_lifecycle_destroy_stage_triggers_retention(self):
        s = _fresh()
        policy = self._create_policy(s, "LIFE-TD-001", "T4_OLD")
        result = s["lifecycle_policy"].transition_stage(policy["id"], "销毁阶段")
        assert result["ok"] is True
        assert "retention_cleanup" in result["actions"]
        assert result["retention_cleanup"]["目标资产"] == "T4_OLD"

    def test_full_lifecycle_flow(self):
        s = _fresh()
        policy = self._create_policy(s, "LIFE-TF-001", "T4_FULL")
        stages = ["活跃阶段", "保留阶段", "归档阶段", "销毁阶段"]
        for stage in stages:
            result = s["lifecycle_policy"].transition_stage(policy["id"], stage)
            assert result["ok"] is True, f"Failed at stage {stage}"
        assert len(_find_by_code("cold_archive", "源资产", "T4_FULL")) >= 1
        assert len(_find_by_code("retention_job", "目标资产", "T4_FULL")) >= 1


# ══════════════════════════════════════════════════════════════════
# Pipeline 5: 数据契约 → Schema 验证 → 合规扫描
# ══════════════════════════════════════════════════════════════════

class TestPipeline5ContractToCompliance:

    def test_contract_sign_triggers_compliance_scan(self):
        s = _fresh()
        result = s["data_contract"].define_contract(
            "T5_ASSET", {"summary": "id,name,amount"}, "99.9%可用性",
            "每日更新", "张三", "下游分析系统"
        )
        assert result["contract"]["status"] == "已签署"
        assert result["compliance_scan"]["passed"] is True

    def test_schema_validation_detects_violation(self):
        s = _fresh()
        s["data_contract"].define_contract(
            "T5_ASSET2", {"summary": "id,name,amount"}, "99.9%可用性",
            "每日更新", "张三", "下游系统"
        )
        contracts = _find_by_code("data_contract", "数据资产", "T5_ASSET2")
        assert len(contracts) >= 1
        result = s["data_contract"].validate_schema(
            contracts[0]["id"], {"summary": "id,name,amount,new_field"}
        )
        assert result["valid"] is False
        assert result.get("issue_created") is True

    def test_schema_violation_creates_issue(self):
        s = _fresh()
        s["data_contract"].define_contract(
            "T5_ASSET3", {"summary": "id,email,phone"}, "99.9%可用性",
            "实时", "李四", "消费者"
        )
        contracts = _find_by_code("data_contract", "数据资产", "T5_ASSET3")
        s["data_contract"].validate_schema(
            contracts[0]["id"], {"summary": "id,email"}
        )
        issues = _find_by_code("issue_track", "关联资产", "T5_ASSET3")
        assert len(issues) >= 1
        assert issues[0]["问题类型"] == "契约违约"


# ══════════════════════════════════════════════════════════════════
# Pipeline 6: 变更捕获 → 差异报告 → 版本历史
# ══════════════════════════════════════════════════════════════════

class TestPipeline6ChangeToVersion:

    def test_capture_change_creates_diff_and_version(self):
        s = _fresh()
        result = s["change_capture"].capture_change(
            "T6_ASSET", "schema_change",
            before="id,name,age", after="id,name,age,phone",
        )
        assert result["change"]["status"] == "已捕获"
        assert result["diff_report"]["diff_count"] == 1
        assert result["version_snapshot"]["created"] is True

    def test_change_logged_and_diff_stored(self):
        s = _fresh()
        s["change_capture"].capture_change(
            "T6_ASSET2", "field_removal",
            before="id,name,email,phone", after="id,name",
        )
        changes = _find_by_code("change_capture", "目标资产", "T6_ASSET2")
        diffs = _find_by_code("diff_report", "比较对象", "T6_ASSET2")
        assert len(changes) >= 1
        assert len(diffs) >= 1

    def test_change_identifies_downstream_impact(self):
        s = _fresh()
        s["lineage_track"].derive_lineage("T6_SRC", "T6_DST", "asset_id", "直接引用", True)
        result = s["change_capture"].capture_change("T6_SRC", "schema_change",
                                                      before="old", after="new")
        assert len(result["affected_downstream"]) >= 1
        assert "T6_DST" in result["affected_downstream"]


# ══════════════════════════════════════════════════════════════════
# Pipeline 7: 血缘追踪 → 影响分析 → 治理评分
# ══════════════════════════════════════════════════════════════════

class TestPipeline7LineageToGovern:

    def test_lineage_triggers_impact_and_score(self):
        s = _fresh()
        result = s["lineage_track"].derive_lineage(
            "T7_UP", "T7_DOWN", "order_id",
            "SELECT ... FROM T7_UP WHERE status='active'", auto_derived=True
        )
        assert result["lineage"]["status"] == "自动生成"
        assert result["impact_analysis"] is not None
        assert result["governance_scoring"] is not None

    def test_lineage_stored_correctly(self):
        s = _fresh()
        s["lineage_track"].derive_lineage("T7_SRC", "T7_DST", "key_id", "ETL转换", True)
        assert len(store_mod.store.rows("lineage_track")) >= 1
        assert len(store_mod.store.rows("impact_analysis")) >= 1
        assert len(store_mod.store.rows("govern_score")) >= 1

    def test_downstream_chain_returns_correct_depth(self):
        s = _fresh()
        s["lineage_track"].derive_lineage("A7", "B7", "id", "t1", True)
        s["lineage_track"].derive_lineage("B7", "C7", "id", "t2", True)
        chain = s["lineage_track"].get_downstream_chain("A7")
        assert "B7" in chain
        assert "C7" in chain

    def test_governance_scoring_evaluates_all_dimensions(self):
        s = _fresh()
        s["lineage_track"].derive_lineage("T7_GOV", "T7_DST", "id", "t", True)
        result = s["govern_score"].evaluate_asset("T7_GOV")
        assert result["overall"] > 0
        assert "元数据完备性" in result["dimensions"]
        assert "血缘完备性" in result["dimensions"]


# ══════════════════════════════════════════════════════════════════
# Pipeline 8: 操作日志 → 异常检测 → 合规联动
# ══════════════════════════════════════════════════════════════════

class TestPipeline8LogToCompliance:

    def test_normal_action_recorded(self):
        s = _fresh()
        record = s["operation_log"].record_action(
            "T8_ASSET", "张三", "查询", "workstation-01", "成功"
        )
        assert record["status"] == "已记录"

    def test_delete_operation_detected_as_anomaly(self):
        s = _fresh()
        record = s["operation_log"].record_action(
            "T8_ASSET2", "李四", "删除数据", "workstation-02", "成功"
        )
        assert record["status"] == "已异常"
        assert record.get("anomaly") is not None

    def test_sensitive_data_operation_by_non_admin_detected(self):
        s = _fresh()
        store_mod.store.rows("classification").clear()
        store_mod.store.rows("classification").append({
            "id": 99, "标签编号": "CLAS-HIGH8",
            "目标字段": "T8_SENSITIVE.field1",
            "敏感等级": "高", "识别规则": "test", "识别置信度": "1.0",
            "人工复核": "是", "复核人": "admin", "生效日期": "2026-11-01",
            "status": "已确认", "pending": False, "abnormal": False,
        })
        record = s["operation_log"].record_action(
            "T8_SENSITIVE", "普通用户", "查询", "node-01", "成功"
        )
        assert record["status"] == "已异常"

    def test_anomaly_triggers_compliance_alert(self):
        s = _fresh()
        store_mod.store.rows("compliance_scan").clear()
        s["operation_log"].record_action(
            "T8_ASSET3", "王五", "清空数据", "node-02", "成功"
        )
        alerts = [sc for sc in store_mod.store.rows("compliance_scan")
                   if sc.get("triggered_by") == "operation_log"]
        assert len(alerts) >= 1, f"Expected at least one alert, found {len(alerts)}: {alerts}"
        assert alerts[0]["status"] == "存在违规"

    def test_generate_compliance_report_returns_summary(self):
        s = _fresh()
        s["operation_log"].record_action("T8_A1", "u1", "查询", "n1", "成功")
        s["operation_log"].record_action("T8_A1", "u1", "写入", "n1", "成功")
        report = s["operation_log"].generate_compliance_report()
        assert report["total_operations"] >= 2


# ══════════════════════════════════════════════════════════════════
# Pipeline 9: 标签注册 → 资产关联 → 标签传播
# ══════════════════════════════════════════════════════════════════

class TestPipeline9TagPropagation:

    def test_register_tag_and_associate_with_asset(self):
        s = _fresh()
        s["asset_registry"].register_asset({
            "资产编号": "T9_A1", "资产名称": "标签测试资产", "资产类型": "数据表",
        })
        tag = s["tag_registry"].register_tag(
            "PII数据9", "一级", "根标签", "包含个人身份信息的数据", "admin"
        )
        assert tag["标签名称"] == "PII数据9"
        assert tag["status"] == "已发布"
        result = s["tag_registry"].associate_with_asset(tag["id"], "T9_A1")
        assert result["ok"] is True

    def test_tag_propagation_through_lineage(self):
        s = _fresh()
        s["asset_registry"].register_asset({"资产编号": "T9_SRC", "资产名称": "源表", "资产类型": "数据表"})
        s["asset_registry"].register_asset({"资产编号": "T9_DST", "资产名称": "目标表", "资产类型": "数据表"})
        s["lineage_track"].derive_lineage("T9_SRC", "T9_DST", "id", "ETL", True)
        tag = s["tag_registry"].register_tag("金融数据9", "一级", "根", "金融相关", "admin")
        result = s["tag_registry"].associate_with_asset(tag["id"], "T9_SRC")
        assert len(result["propagated_to"]) >= 1
        assert "T9_DST" in result["propagated_to"]
        propagated = [sc for sc in store_mod.store.rows("govern_score")
                       if sc.get("propagated_tag_id") == tag["id"]]
        assert len(propagated) >= 1

    def test_merge_tags_updates_counts(self):
        s = _fresh()
        tag1 = s["tag_registry"].register_tag("标签A9", "一级", "根", "descA", "admin")
        tag2 = s["tag_registry"].register_tag("标签B9", "一级", "根", "descB", "admin")
        tag1["关联资产数"] = "5"
        tag2["关联资产数"] = "3"
        result = s["tag_registry"].merge_tags(tag1["id"], tag2["id"])
        assert result["ok"] is True
        assert s["tag_registry"].get_entry(tag1["id"])["status"] == "已废弃"
        assert int(s["tag_registry"].get_entry(tag2["id"])["关联资产数"]) == 8


# ══════════════════════════════════════════════════════════════════
# 跨模块集成测试
# ══════════════════════════════════════════════════════════════════

class TestCrossModuleIntegration:

    def test_full_governance_workflow(self):
        """完整数据治理工作流：注册 -> 画像 -> 分级 -> 质量 -> 契约 -> 合规。"""
        s = _fresh()

        # 1. 注册资产
        reg = s["asset_registry"].register_asset({
            "资产编号": "GOV-001", "资产名称": "核心交易表",
            "资产类型": "结构化数据表", "所属系统": "交易核心",
            "存储位置": "datalake/transactions", "负责人": "数据团队",
            "保密等级": "高", "注册时间": "2026-11-17",
        })
        assert reg["ok"]

        # 2. 数据画像
        sample = [
            {"交易ID": "T001", "客户姓名": "张三", "手机号": "13800138000", "交易金额": "1000"},
            {"交易ID": "T002", "客户姓名": "李四", "手机号": "13900139000", "交易金额": "2000"},
        ]
        profile = s["profile_stats"].profile_dataset("GOV-001", sample)
        assert profile["profile"]["status"] == "可用"
        assert len(profile["classification_labels"]) >= 1

        # 3. 质量规则
        s["quality_rule"].define_rule("GOV-001", "完整性", "95", "100", "阻断")
        rules = _find_by_code("quality_rule", "目标资产", "GOV-001")
        check_result = s["quality_rule"].execute_check(rules[0]["id"], 70.0)
        assert check_result["breached"]

        # 4. 数据契约
        contract = s["data_contract"].define_contract(
            "GOV-001", {"summary": "交易ID,客户姓名,手机号,交易金额"},
            "99.9%可用性", "实时", "数据团队", "风控系统"
        )
        assert contract["contract"]["status"] == "已签署"

        # 5. 合规扫描
        s["compliance_scan"].scan_asset(
            "GOV-001", "数据安全合规",
            ["分类标签复核", "脱敏规则完备性", "契约合规", "审计异常"]
        )

        # 6. 操作日志
        s["operation_log"].record_action("GOV-001", "分析员", "查询", "node-01", "成功")

        # 验证关键数据产物
        assert len(store_mod.store.rows("asset_registry")) >= 1
        assert len(store_mod.store.rows("metadata_index")) >= 1
        assert len(store_mod.store.rows("classification")) >= 1
        issues = _find_by_code("issue_track", "关联资产", "GOV-001")
        assert len(issues) >= 1, f"Expected issues for GOV-001, got {len(issues)}"

    def test_governance_score_after_full_setup(self):
        """设置完整治理环境后计算综合评分。"""
        s = _fresh()
        s["asset_registry"].register_asset({
            "资产编号": "SCORE-001", "资产名称": "评分测试资产", "资产类型": "数据表",
        })
        s["quality_rule"].define_rule("SCORE-001", "完整性", "90", "100", "阻断")
        s["lineage_track"].derive_lineage("SCORE-001", "SCORE-DST", "id", "ETL", True)
        score_result = s["govern_score"].evaluate_asset("SCORE-001")
        assert score_result["overall"] > 0
        assert len(store_mod.store.rows("change_capture")) >= 1
        assert len(store_mod.store.rows("govern_score")) >= 1

# ══════════════════════════════════════════════════════════════════
# 版本结构变更取数：版本履历与变更捕获共用同一份记录
# ══════════════════════════════════════════════════════════════════

def _structure_reports(asset: str) -> list:
    return [
        row for row in store_mod.store.rows("diff_report")
        if row.get("比较对象") == asset and row.get("比较维度") == "字段结构"
    ]


class TestVersionFieldChangeLedger:

    def _seed_v1(self, s, asset="T10_ASSET", schema="id,gross:decimal(10,2),name"):
        s["version_history"].create_version(asset, "v1", "初始导入", schema)
        return asset

    def test_capture_change_keeps_existing_return_shape(self):
        s = _fresh()
        result = s["change_capture"].capture_change(
            "T10_SHAPE", "add", before="", after="phone",
        )
        assert set(result) >= {"change", "diff_report", "version_snapshot", "affected_downstream", "pipeline"}
        assert result["diff_report"]["diff_count"] == 1
        assert result["version_snapshot"]["created"] is True
        assert result["change"]["status"] == "已捕获"

    def test_two_entrypoints_share_one_ledger(self):
        s = _fresh()
        asset = self._seed_v1(s, "T10_SHARE")
        s["change_capture"].capture_change(asset, "precision", "decimal(10,2)", "decimal(16,4)")
        s["version_history"].create_version(asset, "v2", "精度调整", "id,gross:decimal(16,4),name")
        from_history = s["version_history"].compare_versions(asset, "v1", "v2")
        from_capture = s["change_capture"].compare_versions(asset, "v1", "v2")
        assert from_history == from_capture
        assert from_history[0]["差异字段"] == "gross"
        assert (from_history[0]["源值"], from_history[0]["目标值"]) == ("decimal(10,2)", "decimal(16,4)")

    def test_comparison_reproducible_after_later_writes(self):
        s = _fresh()
        asset = self._seed_v1(s, "T10_REPLAY")
        s["change_capture"].capture_change(asset, "rename", "gross", "gross_amount")
        s["version_history"].create_version(asset, "v2", "改名", "id,gross_amount:decimal(10,2),name")
        first = s["version_history"].compare_versions(asset, "v1", "v2")

        # 冲突的后续写入：再改精度、再建版本
        s["change_capture"].capture_change(
            asset, "precision", "decimal(10,2)", "decimal(20,6)", field="gross_amount")
        s["version_history"].create_version(asset, "v3", "再次调整", "id,gross_amount:decimal(20,6),name")

        assert s["version_history"].compare_versions(asset, "v1", "v2") == first
        assert s["change_capture"].compare_versions(asset, "v1", "v2") == first

    def test_current_reader_sees_later_effective_changes(self):
        s = _fresh()
        asset = self._seed_v1(s, "T10_CURRENT")
        s["version_history"].create_version(asset, "v2", "改名", "id,gross_amount:decimal(10,2),name")
        s["change_capture"].capture_change(
            asset, "precision", "decimal(10,2)", "decimal(18,4)", field="gross_amount")

        current = s["version_history"].get_current_schema(asset)
        assert {"name": "gross_amount", "type": "decimal(18,4)"} in current
        latest = s["version_history"].compare_versions(asset, "v2", "current")
        assert latest[-1]["差异字段"] == "gross_amount"
        assert latest[-1]["目标值"] == "decimal(18,4)"

    def test_repeat_submission_keeps_single_diff_group(self):
        s = _fresh()
        asset = self._seed_v1(s, "T10_REPEAT")
        s["change_capture"].capture_change(asset, "precision", "decimal(10,2)", "decimal(16,4)")
        s["version_history"].create_version(asset, "v2", "精度调整", "id,gross:decimal(16,4),name")
        for _ in range(3):
            s["version_history"].compare_versions(asset, "v1", "v2")
        reports = _structure_reports(asset)
        assert len(reports) == 1
        assert reports[0]["差异字段"] == "gross"

        # 反向重提仍是同一组，内容跟随比较方向，记录数不增长
        reversed_reports = s["version_history"].compare_versions(asset, "v2", "v1")
        assert len(_structure_reports(asset)) == 1
        assert reversed_reports[0]["源值"] == "decimal(16,4)"
        assert reversed_reports[0]["目标值"] == "decimal(10,2)"

    def test_same_column_multiple_changes_keep_original_order(self):
        s = _fresh()
        asset = self._seed_v1(s, "T10_ORDER")
        s["change_capture"].capture_change(asset, "precision", "decimal(10,2)", "decimal(12,3)")
        s["change_capture"].capture_change(asset, "precision", "decimal(12,3)", "decimal(16,4)")
        s["version_history"].create_version(asset, "v2", "精度连续调整", "id,gross:decimal(16,4),name")
        diffs = s["version_history"].compare_versions(asset, "v1", "v2")
        assert [(d["源值"], d["目标值"]) for d in diffs] == [
            ("decimal(10,2)", "decimal(12,3)"),
            ("decimal(12,3)", "decimal(16,4)"),
        ]

    def test_reverse_comparison_flips_values_for_same_change(self):
        s = _fresh()
        asset = self._seed_v1(s, "T10_REVERSE")
        s["change_capture"].capture_change(asset, "rename", "gross", "gross_amount")
        s["version_history"].create_version(asset, "v2", "改名", "id,gross_amount:decimal(10,2),name")
        forward = s["version_history"].compare_versions(asset, "v1", "v2")
        backward = s["version_history"].compare_versions(asset, "v2", "v1")
        assert len(forward) == len(backward) == 1
        assert forward[0]["差异字段"] == "gross"
        assert forward[0]["源值"] == "gross" and forward[0]["目标值"] == "gross_amount"
        assert backward[0]["源值"] == "gross_amount" and backward[0]["目标值"] == "gross"
        assert backward[0]["比较维度"] == forward[0]["比较维度"]

    def test_middle_rename_not_hidden_by_same_head_and_tail(self):
        s = _fresh()
        asset = self._seed_v1(s, "T10_ROUNDTRIP")
        s["change_capture"].capture_change(asset, "rename", "gross", "gross_amount")
        s["change_capture"].capture_change(asset, "precision", "decimal(10,2)", "decimal(16,4)")
        s["change_capture"].capture_change(asset, "rename", "gross_amount", "gross")
        s["version_history"].create_version(asset, "v2", "收尾", "id,gross:decimal(16,4),name")

        diffs = s["version_history"].compare_versions(asset, "v1", "v2")
        pairs = [(d["源值"], d["目标值"]) for d in diffs]
        assert ("gross", "gross_amount") in pairs
        assert ("decimal(10,2)", "decimal(16,4)") in pairs
        assert ("gross_amount", "gross") in pairs

    def test_reverse_after_three_changes_keeps_detail_order(self):
        s = _fresh()
        asset = self._seed_v1(s, "T10_TRIPLE")
        s["change_capture"].capture_change(asset, "precision", "decimal(10,2)", "decimal(12,3)")
        s["change_capture"].capture_change(asset, "rename", "gross", "gross_amount")
        s["change_capture"].capture_change(asset, "precision", "decimal(12,3)", "decimal(16,4)")
        s["version_history"].create_version(asset, "v2", "结构调整", "id,gross_amount:decimal(16,4),name")

        forward = s["version_history"].compare_versions(asset, "v1", "v2")
        assert [(d["差异字段"], d["源值"], d["目标值"]) for d in forward] == [
            ("gross", "decimal(10,2)", "decimal(12,3)"),
            ("gross", "gross", "gross_amount"),
            ("gross_amount", "decimal(12,3)", "decimal(16,4)"),
        ]

        backward = s["version_history"].compare_versions(asset, "v2", "v1")
        assert [(d["差异字段"], d["源值"], d["目标值"]) for d in backward] == [
            ("gross", "decimal(12,3)", "decimal(10,2)"),
            ("gross", "gross_amount", "gross"),
            ("gross_amount", "decimal(16,4)", "decimal(12,3)"),
        ]

    def test_version_schema_readable_from_both_entrypoints(self):
        s = _fresh()
        asset = self._seed_v1(s, "T10_SCHEMA")
        s["version_history"].create_version(asset, "v2", "改名", "id,gross_amount:decimal(10,2),name")
        expected = [
            {"name": "id", "type": ""},
            {"name": "gross_amount", "type": "decimal(10,2)"},
            {"name": "name", "type": ""},
        ]
        assert s["version_history"].get_version_schema(asset, "v2") == expected
        assert s["change_capture"].get_version_schema(asset, "v2") == expected

    def test_unknown_version_yields_no_diff(self):
        s = _fresh()
        asset = self._seed_v1(s, "T10_MISSING")
        # 未知版本不臆断为“整段结构被删除”，与原单次查询一致返回空明细。
        assert s["version_history"].compare_versions(asset, "v1", "不存在") == []

    def test_generic_entry_registration_feeds_ledger(self):
        s = _fresh()
        s["version_history"].create_entry({
            "快照编号": "VERS-E1", "目标资产": "T10_ENTRY", "版本标签": "v1",
            "Schema摘要": "id,gross:decimal(10,2)",
        })
        s["change_capture"].create_entry({
            "记录编号": "CHAN-E1", "目标资产": "T10_ENTRY", "变更类型": "rename",
            "变更前值": "gross", "变更后值": "gross_amount",
        })
        s["version_history"].create_entry({
            "快照编号": "VERS-E2", "目标资产": "T10_ENTRY", "版本标签": "v2",
            "Schema摘要": "id,gross_amount:decimal(10,2)",
        })
        diffs = s["version_history"].compare_versions("T10_ENTRY", "v1", "v2")
        assert [(d["源值"], d["目标值"]) for d in diffs] == [("gross", "gross_amount")]

    def test_capture_derives_missing_before_from_boundary(self):
        s = _fresh()
        asset = self._seed_v1(s, "T10_DERIVE")
        result = s["change_capture"].capture_change(asset, "precision", None, "decimal(16,4)")
        assert result["diff_report"]["diff_count"] == 1
        stored = [d for d in store_mod.store.rows("diff_report")
                  if d.get("change_id") == result["change"]["id"]]
        assert (stored[0]["差异字段"], stored[0]["源值"], stored[0]["目标值"]) == (
            "gross", "decimal(10,2)", "decimal(16,4)",
        )

    def test_store_snapshot_comparison_kept_working(self):
        s = _fresh()
        store_mod.store.create_snapshot("legacy-a", ["asset_registry"])
        store_mod.store.rows("asset_registry")[0]["资产名称"] = "改过的名字"
        store_mod.store.create_snapshot("legacy-b", ["asset_registry"])
        diffs = s["change_capture"].compare_versions("asset_registry", "legacy-a", "legacy-b")
        assert any(d["差异字段"] == "资产名称" for d in diffs)

    def test_change_captured_before_version_stays_in_earlier_interval(self):
        s = _fresh()
        asset = self._seed_v1(s, "T10_BOUNDARY")
        # 变更写在 v2 之前：归 v1->v2，不应出现在 v2->v3
        s["change_capture"].capture_change(asset, "rename", "gross", "gross_amount")
        s["version_history"].create_version(asset, "v2", "改名", "id,gross_amount:decimal(10,2),name")
        s["change_capture"].capture_change(
            asset, "precision", "decimal(10,2)", "decimal(18,4)", field="gross_amount")
        s["version_history"].create_version(asset, "v3", "调精度", "id,gross_amount:decimal(18,4),name")

        early = s["version_history"].compare_versions(asset, "v1", "v2")
        late = s["version_history"].compare_versions(asset, "v2", "v3")
        assert len(early) == 1 and early[0]["差异字段"] == "gross"
        assert len(late) == 1 and late[0]["目标值"] == "decimal(18,4)"
