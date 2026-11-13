"""Delta Lake 销售订单表金额非负约束启用工具的核心逻辑。

本模块直接读写一张**本地 Delta Lake 表目录**（不依赖 Spark / Delta 运行时），
帮助湖仓值班员在启用 ``delta.constraints.nonnegative_amount``（语义为
``amount_cents >= 0``）前完成数据体检，并在通过后以一次标准 Delta 提交原子
发布协议升级与元数据变更。

支持范围（超出范围直接拒绝并指出原因）：

- 仅本地可写目录中的单张表；
- 版本 0 起 JSON 提交连续无断档（检查点只作加速基线，不替代连续性检查）；
- 初始读写协议为 minReaderVersion=1 / minWriterVersion=2；
- 无分区表（metadata.partitionColumns 必须为空，add 动作 partitionValues 为空）；
- 不支持删除向量（protocol 特性或任意 add/remove 带 deletionVector 即拒绝）。

并发模型：其他写入方只允许“新增 / 删除数据文件”式提交。扫描期间不持有任何
阻止并发提交的锁；每次发布前重新以最新快照复核，撞到新版本则整体重验，绝不
覆盖已有提交（发布用 O_CREAT|O_EXCL 原子创建，撞名即重试）。
"""

from __future__ import annotations

import json
import os
import re
import time
import urllib.parse
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq

# Delta 日志文件名均为 20 位补零版本号
_COMMIT_RE = re.compile(r"^(\d{20})\.json$")
# 经典单文件检查点：00000000000000000010.checkpoint.parquet
_CHECKPOINT_RE = re.compile(r"^(\d{20})\.checkpoint\.parquet$")
# 分片检查点：N.checkpoint.K.M.parquet（本工具只读经典单文件检查点）
_MULTI_CHECKPOINT_RE = re.compile(r"^\d{20}\.checkpoint\.\d+\.\d+\.parquet$")

DV_FEATURE_NAME = "deletionVectors"
CONSTRAINT_NAME = "nonnegative_amount"
CONSTRAINT_KEY = f"delta.constraints.{CONSTRAINT_NAME}"
CONSTRAINT_EXPR = "amount_cents >= 0"
ORDER_ID_COLUMN = "order_id"
AMOUNT_COLUMN = "amount_cents"
# 约束需要 minWriterVersion >= 3（Delta 规范：约束随 writer v3 引入）
TARGET_WRITER_VERSION = 3
# 发布撞版本时最多重验次数，避免与持续写入方死循环
MAX_CONFLICT_RETRIES = 5


class DeltaError(Exception):
    """面向用户的表格式 / 前置条件 / IO 错误。"""


# --------------------------------------------------------------------------- #
# 数据结构
# --------------------------------------------------------------------------- #

@dataclass
class Commit:
    version: int
    source: str
    protocol: dict | None = None
    metadata: dict | None = None
    adds: list[dict] = field(default_factory=list)        # 完整 add 动作 dict
    removes: list[dict] = field(default_factory=list)     # 完整 remove 动作 dict


@dataclass
class Protocol:
    min_reader_version: int
    min_writer_version: int
    reader_features: list[str] = field(default_factory=list)
    writer_features: list[str] = field(default_factory=list)
    source: str = ""


@dataclass
class Snapshot:
    """某一版本上的表状态（重放 / 检查点重建结果）。"""

    version: int
    protocol: Protocol
    metadata: dict
    # 表内相对路径 -> add 动作（路径不复用：同一路径后 add 覆盖先 add）
    active_files: dict[str, dict]
    constrained: bool = False
    constraint_version: int | None = None


@dataclass
class NegativeRow:
    order_id: int | None
    amount_cents: int | None
    path: str
    amount_null: bool = False


@dataclass
class ScanReport:
    """一次完整体检的结论。任何阻塞项非空时都不得发布约束。"""

    snapshot_version: int
    protocol: Protocol
    schema_fields: list[dict]
    active_files: list[str]
    negatives: list[NegativeRow] = field(default_factory=list)
    missing_files: list[str] = field(default_factory=list)
    unreadable_files: list[str] = field(default_factory=list)
    constrained: bool = False
    constraint_version: int | None = None
    extra_config: dict[str, str] = field(default_factory=dict)

    @property
    def blocked(self) -> bool:
        return bool(self.negatives or self.missing_files or self.unreadable_files)


# --------------------------------------------------------------------------- #
# 路径与 JSON 工具
# --------------------------------------------------------------------------- #

def normalize_data_path(raw: Any) -> str:
    """把日志中的路径归一化为表内相对路径（去 ``./``、``/`` 前缀并 percent-decode）。"""
    if not isinstance(raw, str):
        raise DeltaError(f"数据文件路径不是字符串：{raw!r}")
    path = raw.strip()
    while path.startswith("./"):
        path = path[2:]
    while path.startswith("/"):
        path = path[1:]
    return urllib.parse.unquote(path)


def _iter_json_objects(text: str, source: str):
    """Delta 提交是 ndjson（每行一个动作对象），同时兼容整体 JSON 数组。"""
    stripped = text.strip()
    if not stripped:
        return
    line_objs: list[Any] = []
    ndjson_ok = True
    for line in stripped.splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            obj = json.loads(line)
        except json.JSONDecodeError:
            ndjson_ok = False
            break
        if not isinstance(obj, dict):
            ndjson_ok = False
            break
        line_objs.append(obj)
    if ndjson_ok and line_objs:
        yield from line_objs
        return
    try:
        whole = json.loads(stripped)
    except json.JSONDecodeError as exc:
        raise DeltaError(f"{source} 既不是 ndjson 也不是合法 JSON：{exc}") from exc
    if isinstance(whole, list):
        yield from whole
    else:
        yield whole


def _features_of(protocol: dict) -> tuple[list[str], list[str], list[str]]:
    reader = [str(v) for v in protocol.get("readerFeatures", [])
              if isinstance(protocol.get("readerFeatures"), list)]
    writer = [str(v) for v in protocol.get("writerFeatures", [])
              if isinstance(protocol.get("writerFeatures"), list)]
    shared = [str(v) for v in protocol.get("features", [])
              if isinstance(protocol.get("features"), list)]
    return reader, writer, shared


# --------------------------------------------------------------------------- #
# 日志读取与解析
# --------------------------------------------------------------------------- #

def _read_commit(path: Path, version: int) -> Commit:
    commit = Commit(version=version, source=path.name)
    try:
        text = path.read_text(encoding="utf-8")
    except (OSError, UnicodeDecodeError) as exc:
        raise DeltaError(f"{path.name} 无法读取为 UTF-8 提交：{exc}") from exc
    for obj in _iter_json_objects(text, path.name):
        if not isinstance(obj, dict):
            continue
        if isinstance(obj.get("protocol"), dict):
            commit.protocol = obj["protocol"]
        if isinstance(obj.get("metaData"), dict):
            commit.metadata = obj["metaData"]
        if isinstance(obj.get("add"), dict):
            commit.adds.append(obj["add"])
        if isinstance(obj.get("remove"), dict):
            commit.removes.append(obj["remove"])
    return commit


def _read_checkpoint(path: Path, version: int) -> dict[str, Any]:
    """读经典单文件 Parquet 检查点，返回重建快照所需的全部要素。"""
    try:
        table = pq.read_table(path)
    except Exception as exc:  # parquet 损坏 / 非 parquet
        raise DeltaError(f"{path.name} 无法作为 Parquet 检查点读取：{exc}") from exc

    out: dict[str, Any] = {
        "adds": [], "removes": [], "protocol": None, "metadata": None,
        "dv_detected": False, "protocol_features": [],
    }
    columns = set(table.column_names)

    if "add" in columns:
        add_col = table.column("add").combine_chunks()
        if not pa.types.is_struct(add_col.type):
            raise DeltaError(f"{path.name} 的 add 列不是 struct，不是合法检查点")
        n = table.num_rows
        if add_col.type.get_field_index("deletionVector") != -1:
            dv_values = add_col.field("deletionVector").to_pylist()
            if any(v is not None for v in dv_values):
                out["dv_detected"] = True
        else:
            dv_values = [None] * n
        path_idx = add_col.type.get_field_index("path")
        if path_idx == -1:
            raise DeltaError(f"{path.name} 的 add struct 缺少 path 字段")
        paths = add_col.field("path").to_pylist()
        for row, path_value, dv in zip(add_col.to_pylist(), paths, dv_values):
            if path_value is None:
                continue
            if isinstance(row, dict):
                row = dict(row)
                row["path"] = normalize_data_path(path_value)
                row["deletionVector"] = dv
            else:
                row = {"path": normalize_data_path(path_value),
                       "deletionVector": dv}
            out["adds"].append(row)

    if "remove" in columns:
        rm_col = table.column("remove").combine_chunks()
        if pa.types.is_struct(rm_col.type):
            if rm_col.type.get_field_index("deletionVector") != -1:
                if any(v is not None for v in
                       rm_col.field("deletionVector").to_pylist()):
                    out["dv_detected"] = True
            if rm_col.type.get_field_index("path") != -1:
                for p in rm_col.field("path").to_pylist():
                    if p is not None:
                        out["removes"].append({"path": normalize_data_path(p)})

    if "protocol" in columns:
        proto_col = table.column("protocol").combine_chunks()
        if pa.types.is_struct(proto_col.type):
            rows = proto_col.to_pylist()
            for row in rows:
                if row:
                    out["protocol"] = row
                    for key in ("readerFeatures", "writerFeatures", "features"):
                        val = row.get(key)
                        if isinstance(val, list):
                            out["protocol_features"].extend(str(v) for v in val)
                    break

    if "metaData" in columns:
        meta_col = table.column("metaData").combine_chunks()
        for row in meta_col.to_pylist():
            if row:
                out["metadata"] = row
                break

    return out


def list_log_files(log_dir: Path) -> tuple[dict[int, Path], dict[int, Path], list[str]]:
    """盘点 _delta_log 目录：提交、经典检查点、被忽略文件名。"""
    commits: dict[int, Path] = {}
    checkpoints: dict[int, Path] = {}
    ignored: list[str] = []
    if not log_dir.is_dir():
        raise DeltaError(f"_delta_log 目录不存在：{log_dir}")
    for child in sorted(log_dir.iterdir()):
        if not child.is_file():
            continue
        m_commit = _COMMIT_RE.match(child.name)
        m_ckpt = _CHECKPOINT_RE.match(child.name)
        if m_commit:
            commits[int(m_commit.group(1))] = child
        elif m_ckpt:
            checkpoints[int(m_ckpt.group(1))] = child
        elif _MULTI_CHECKPOINT_RE.match(child.name) or child.name == "_last_checkpoint":
            ignored.append(child.name)
        # 其它杂项文件（如 crc）不参与，也不打扰用户
    return commits, checkpoints, ignored


# --------------------------------------------------------------------------- #
# 快照构建（检查点基线 + 连续 JSON 重放）
# --------------------------------------------------------------------------- #

def _apply_protocol(target: Protocol, raw: dict, source: str) -> None:
    try:
        target.min_reader_version = int(raw["minReaderVersion"])
        target.min_writer_version = int(raw["minWriterVersion"])
    except (KeyError, TypeError, ValueError) as exc:
        raise DeltaError(f"{source} 的 protocol 缺少合法的 minReaderVersion/"
                         f"minWriterVersion：{raw!r}") from exc
    reader, writer, shared = _features_of(raw)
    target.reader_features = reader + shared
    target.writer_features = writer + shared
    target.source = source


def _guard_no_dv_in_commit(commit: Commit) -> None:
    features = set()
    if commit.protocol is not None:
        r, w, s = _features_of(commit.protocol)
        features.update(r + w + s)
    if DV_FEATURE_NAME in features:
        raise DeltaError(
            f"表启用了删除向量：提交 {commit.source} 的 protocol 特性包含 "
            f"{DV_FEATURE_NAME}。本工具只处理无删除向量的表。")
    for action in (*commit.adds, *commit.removes):
        if action.get("deletionVector") is not None:
            kind = "add" if action in commit.adds else "remove"
            raise DeltaError(
                f"表包含删除向量：提交 {commit.source} 的 {kind} 动作 "
                f"{action.get('path')!r} 带有 deletionVector 字段。")


def load_snapshot(table_dir: Path) -> Snapshot:
    """读取表目录并重建最新版本的完整快照，同时完成全部前置结构校验。"""
    table_dir = Path(table_dir)
    log_dir = table_dir / "_delta_log"
    commit_paths, checkpoint_paths, _ignored = list_log_files(log_dir)

    if not commit_paths:
        raise DeltaError("表目录中没有任何 JSON 提交，无法识别为 Delta Lake 表")
    if 0 not in commit_paths:
        raise DeltaError("缺少版本 0 的提交，无法确定表的初始状态")

    # 期望最高版本 = 最新 JSON 提交与最新检查点中的较大者；
    # 不能只看 max(commit_paths)，否则尾巴上的提交丢失会被悄悄掩盖
    latest_commit = max(commit_paths)
    latest_checkpoint = max(checkpoint_paths, default=-1)
    latest_version = max(latest_commit, latest_checkpoint)

    # 1) 版本 0 起 JSON 提交必须连续（检查点不能掩盖断档，含尾部断档）
    missing = [v for v in range(0, latest_version + 1) if v not in commit_paths]
    if missing:
        shown = ", ".join(f"v{v}" for v in missing[:10])
        more = " …" if len(missing) > 10 else ""
        raise DeltaError(f"JSON 提交不连续，缺失提交：{shown}{more}。"
                         "检查点不能替代缺失的 JSON 提交。")

    # 2) 选择不晚于最新版本的最新经典检查点作为加速基线
    base_version: int | None = None
    base: dict[str, Any] | None = None
    eligible = sorted((v for v in checkpoint_paths if v <= latest_version),
                      reverse=True)
    if eligible:
        base_version = eligible[0]
        base = _read_checkpoint(checkpoint_paths[base_version], base_version)
        if base["dv_detected"] or DV_FEATURE_NAME in base["protocol_features"]:
            raise DeltaError(
                f"检查点 {checkpoint_paths[base_version].name} 含删除向量，"
                "本工具只处理无删除向量的表。")

    # 3) 从基线（或 v0）出发顺序重放
    protocol: Protocol
    metadata: dict | None = None
    active: dict[str, dict] = {}

    if base is not None:
        if base["protocol"] is not None:
            protocol = Protocol(min_reader_version=0, min_writer_version=0)
            _apply_protocol(protocol, base["protocol"],
                            checkpoint_paths[base_version].name)
        else:
            # 检查点没带 protocol 行时，回退到 v0 提交里的协议
            protocol = Protocol(min_reader_version=0, min_writer_version=0)
        if base["metadata"] is not None:
            metadata = base["metadata"]
        for add in base["adds"]:
            active[add["path"]] = add
        replay_from = base_version + 1
        # 检查点之前若缺 protocol/metadata，重放时会从 JSON 补齐
    else:
        protocol = Protocol(min_reader_version=0, min_writer_version=0)
        replay_from = 0

    constrained = False
    constraint_version: int | None = None

    for version in range(replay_from, latest_version + 1):
        commit = _read_commit(commit_paths[version], version)
        _guard_no_dv_in_commit(commit)
        if commit.protocol is not None:
            _apply_protocol(protocol, commit.protocol, commit.source)
        if commit.metadata is not None:
            metadata = commit.metadata
            config = metadata.get("configuration") or {}
            if CONSTRAINT_KEY in config:
                constrained = True
                constraint_version = version
        for remove in commit.removes:
            active.pop(normalize_data_path(remove.get("path")), None)
        for add in commit.adds:
            path = normalize_data_path(add.get("path"))
            active[path] = add  # 路径不复用：同路径新 add 替换旧 add

    # 基线检查点已含约束配置的情形
    if not constrained and metadata is not None:
        config = metadata.get("configuration") or {}
        if CONSTRAINT_KEY in config:
            constrained = True
            constraint_version = base_version

    if metadata is None:
        raise DeltaError("重放到最新版本仍未取得 metaData 动作，表元数据不完整")
    if protocol.min_reader_version == 0 or protocol.min_writer_version == 0:
        raise DeltaError("重放后仍未取得 protocol 动作，表协议不完整")

    snapshot = Snapshot(version=latest_version, protocol=protocol,
                        metadata=metadata, active_files=active,
                        constrained=constrained,
                        constraint_version=constraint_version)
    _validate_table_shape(snapshot)
    return snapshot


def _validate_table_shape(snapshot: Snapshot) -> None:
    """任务前置条件：初始协议 1/2、无分区、无删除向量、两列均为非空 int64。"""
    # 协议：重放后允许 writer 已升到 3（幂等场景），但 reader 仍应是 1
    proto = snapshot.protocol
    if proto.min_reader_version != 1:
        raise DeltaError(
            f"仅支持初始读协议 minReaderVersion=1 的表，当前为 "
            f"minReaderVersion={proto.min_reader_version}（来源 {proto.source}）")
    if proto.min_writer_version not in (2, TARGET_WRITER_VERSION):
        raise DeltaError(
            f"仅支持初始写协议 minWriterVersion=2（或已升级到 3）的表，当前为 "
            f"minWriterVersion={proto.min_writer_version}（来源 {proto.source}）")
    if proto.reader_features or proto.writer_features:
        feats = sorted(set(proto.reader_features + proto.writer_features))
        raise DeltaError(f"表启用了列映射 / 表特性等高级协议特性：{feats}，"
                         "本工具只处理协议 1/2 起步的基础表")

    # 无分区
    partition_cols = snapshot.metadata.get("partitionColumns")
    if partition_cols:
        raise DeltaError(f"仅支持无分区表，当前分区列为 {partition_cols}")
    for path, add in snapshot.active_files.items():
        pv = add.get("partitionValues")
        # 检查点里零字段 struct 经 Parquet 落盘后可能只含占位子字段，视同空映射
        if isinstance(pv, dict):
            pv = {k: v for k, v in pv.items() if not k.startswith("__empty")}
        if pv:
            raise DeltaError(f"数据文件 {path} 带非空 partitionValues={pv}，"
                             "本工具只处理无分区表")

    # 模式：order_id、amount_cents 均为非空 int64（long）
    schema = snapshot.metadata.get("schemaString")
    fields = _schema_fields(snapshot.metadata)
    by_name = {f.get("name"): f for f in fields}
    for column in (ORDER_ID_COLUMN, AMOUNT_COLUMN):
        field_meta = by_name.get(column)
        if field_meta is None:
            raise DeltaError(f"表模式缺少必需列 {column}，当前列为 "
                             f"{sorted(by_name)}")
        if field_meta.get("type") != "long":
            raise DeltaError(f"列 {column} 必须是 long（int64），实际类型为 "
                             f"{field_meta.get('type')!r}")
        if field_meta.get("nullable") is not False:
            raise DeltaError(f"列 {column} 必须声明为非空（nullable=false）")
    # schemaString 与 fields 一致性的轻量校验
    if isinstance(schema, str):
        try:
            parsed = json.loads(schema)
            parsed_names = {f.get("name"): f for f in parsed.get("fields", [])}
            for column in (ORDER_ID_COLUMN, AMOUNT_COLUMN):
                fmeta = parsed_names.get(column)
                if not fmeta or fmeta.get("type") != "long" or \
                        fmeta.get("nullable") is not False:
                    raise DeltaError(
                        f"schemaString 中的列 {column} 与 fields 模式不一致或"
                        "不满足非空 int64 要求")
        except json.JSONDecodeError as exc:
            raise DeltaError(f"metadata.schemaString 不是合法 JSON：{exc}") from exc


def _schema_fields(metadata: dict) -> list[dict]:
    """从 metadata 取模式字段（兼容 schemaString 与 fields 两种存放方式）。"""
    schema_string = metadata.get("schemaString")
    if isinstance(schema_string, str):
        try:
            parsed = json.loads(schema_string)
        except json.JSONDecodeError as exc:
            raise DeltaError(f"metadata.schemaString 不是合法 JSON：{exc}") from exc
        fields = parsed.get("fields")
        if isinstance(fields, list):
            return fields
    fields = metadata.get("fields")
    if isinstance(fields, list):
        return fields
    raise DeltaError("metadata 中找不到可用的表模式（schemaString/fields）")


# --------------------------------------------------------------------------- #
# 数据文件扫描
# --------------------------------------------------------------------------- #

def _read_order_batch(path: Path) -> tuple[list[NegativeRow], bool]:
    """读一个 Parquet 数据文件，返回负金额行；第二返回值表示文件不可读。"""
    try:
        table = pq.read_table(path, columns=[ORDER_ID_COLUMN, AMOUNT_COLUMN])
    except Exception:
        return [], True

    schema = table.schema
    for name in (ORDER_ID_COLUMN, AMOUNT_COLUMN):
        idx = schema.get_field_index(name)
        if idx == -1:
            # 列缺失等价于读不出所需数据
            return [], True
        if not pa.types.is_int64(schema.field(idx).type):
            return [], True
        if schema.field(idx).nullable is not False:
            # 物理文件允许 nullable 不阻断，但 Delta 元数据已声明非空；
            # null 值在下面与负值一起被视为违约数据
            pass

    order_ids = table.column(ORDER_ID_COLUMN).to_pylist()
    amounts = table.column(AMOUNT_COLUMN).to_pylist()
    rows: list[NegativeRow] = []
    for oid, amount in zip(order_ids, amounts):
        # 非空列里出现 null、或金额为负，都无法满足 amount_cents >= 0
        if amount is None:
            rows.append(NegativeRow(
                order_id=oid if isinstance(oid, int) else None,
                amount_cents=None, path="", amount_null=True))
        elif amount < 0:
            rows.append(NegativeRow(
                order_id=oid if isinstance(oid, int) else None,
                amount_cents=amount, path=""))
    return rows, False


def scan_table(table_dir: Path) -> ScanReport:
    """以最新快照为准做一次完整体检（不阻塞并发提交）。"""
    table_dir = Path(table_dir)
    snapshot = load_snapshot(table_dir)
    metadata = snapshot.metadata
    config = dict(metadata.get("configuration") or {})
    extra_config = {k: v for k, v in config.items() if k != CONSTRAINT_KEY}

    report = ScanReport(
        snapshot_version=snapshot.version,
        protocol=snapshot.protocol,
        schema_fields=_schema_fields(metadata),
        active_files=sorted(snapshot.active_files),
        constrained=snapshot.constrained,
        constraint_version=snapshot.constraint_version,
        extra_config=extra_config,
    )

    for rel_path in sorted(snapshot.active_files):
        abs_path = table_dir / rel_path
        if not abs_path.is_file():
            report.missing_files.append(rel_path)
            continue
        rows, unreadable = _read_order_batch(abs_path)
        if unreadable:
            report.unreadable_files.append(rel_path)
            continue
        for row in rows:
            row.path = rel_path
            report.negatives.append(row)

    # 稳定排序：先文件后订单号，方便页面与测试断言
    report.negatives.sort(key=lambda r: (r.path, r.order_id))
    return report


# --------------------------------------------------------------------------- #
# 约束提交（协议升级 + 元数据变更，单文件原子发布）
# --------------------------------------------------------------------------- #

def _normalize_metadata_jsonable(metadata: dict) -> dict:
    """把检查点回放产生的 datetime / tuple 等非 JSON 原生值转回 Delta JSON 形态。"""
    import datetime

    def convert(value: Any) -> Any:
        if isinstance(value, datetime.datetime):
            # 检查点列是 timestamp[us]，Delta JSON 时间戳与该列同为微秒整数
            return int(value.timestamp() * 1_000_000)
        if isinstance(value, dict):
            return {k: convert(v) for k, v in value.items()}
        if isinstance(value, (list, tuple)):
            return [convert(v) for v in value]
        return value

    return convert(metadata)


def _build_constraint_actions(snapshot: Snapshot, now_millis: int) -> list[dict]:
    """基于当前快照构造一次标准 Delta 提交的动作序列。

    - protocol：minWriterVersion 升到 3，reader 保持 1，其余字段原样保留；
    - metaData：完整保留原 metadata（id/name/description/format/schema/
      partitionColumns/createdTime 等），仅在 configuration 中加入约束键，
      并刷新 lastUpdate。
    """
    old_proto = snapshot.protocol
    protocol_action = {
        "protocol": {
            "minReaderVersion": old_proto.min_reader_version,
            "minWriterVersion": TARGET_WRITER_VERSION,
        }
    }

    metadata = _normalize_metadata_jsonable(snapshot.metadata)
    new_metadata = json.loads(json.dumps(metadata))  # 深拷贝，不污染调用方
    configuration = dict(new_metadata.get("configuration") or {})
    configuration[CONSTRAINT_KEY] = CONSTRAINT_EXPR
    new_metadata["configuration"] = configuration
    new_metadata["lastUpdate"] = now_millis
    # 保留 createdTime 等其余全部元数据键，不做删改
    return [protocol_action, {"metaData": new_metadata}]


def _publish_commit_atomic(log_dir: Path, version: int,
                           actions: list[dict]) -> Path:
    """以 O_CREAT|O_EXCL 原子创建提交文件；撞名抛 CommitConflict，绝不覆盖。"""
    target = log_dir / f"{version:020d}.json"
    payload = ("\n".join(json.dumps(a, ensure_ascii=False) for a in actions)
               + "\n").encode("utf-8")
    try:
        # 排他创建：文件已存在（并发写入方抢先提交）直接失败，不做任何覆盖
        fd = os.open(target, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o644)
    except FileExistsError as exc:
        raise CommitConflict(version) from exc
    try:
        with os.fdopen(fd, "wb") as fh:
            fh.write(payload)
    except BaseException:
        target.unlink(missing_ok=True)
        raise
    return target


class CommitConflict(Exception):
    """发布时目标版本已被其他写入方占用。"""

    def __init__(self, version: int):
        super().__init__(f"v{version} 已被其他提交占用")
        self.version = version


def enable_constraint(table_dir: Path) -> dict[str, Any]:
    """体检通过后发布金额非负约束；返回发布结果摘要。

    流程（全程不阻塞其他只增删文件的并发提交）：
      1. 以最新快照体检；
      2. 已启用则直接返回现有启用版号，不再提交；
      3. 有负金额 / 缺失 / 不可读文件则拒绝发布，逐项列出路径与订单号；
      4. 构造 protocol(writer=3)+metaData(带约束) 提交，排他原子创建；
      5. 撞版本（并发新提交）则丢弃本次成果，回到第 1 步整体重验。
    """
    table_dir = Path(table_dir)
    log_dir = table_dir / "_delta_log"
    attempts = 0

    while True:
        attempts += 1
        report = scan_table(table_dir)

        if report.constrained:
            return {
                "published": False,
                "already_enabled": True,
                "version": report.constraint_version,
                "snapshot_version": report.snapshot_version,
                "attempts": attempts,
            }

        if report.blocked:
            return {
                "published": False,
                "already_enabled": False,
                "blocked": True,
                "snapshot_version": report.snapshot_version,
                "negatives": [
                    {"order_id": r.order_id, "amount_cents": r.amount_cents,
                     "amount_null": r.amount_null, "path": r.path}
                    for r in report.negatives
                ],
                "missing_files": report.missing_files,
                "unreadable_files": report.unreadable_files,
                "attempts": attempts,
            }

        snapshot = load_snapshot(table_dir)
        if snapshot.version != report.snapshot_version:
            # 扫描期间已有新提交：以最新快照为准重验
            continue
        if snapshot.constrained:
            return {
                "published": False,
                "already_enabled": True,
                "version": snapshot.constraint_version,
                "snapshot_version": snapshot.version,
                "attempts": attempts,
            }

        next_version = snapshot.version + 1
        actions = _build_constraint_actions(snapshot, now_millis=int(time.time() * 1000))
        try:
            target = _publish_commit_atomic(log_dir, next_version, actions)
        except CommitConflict:
            if attempts > MAX_CONFLICT_RETRIES:
                raise DeltaError(
                    f"连续 {MAX_CONFLICT_RETRIES} 次发布都撞上并发新版本，"
                    "请稍后重试")
            continue

        # 发布后再读一次确认落盘状态
        confirm = load_snapshot(table_dir)
        return {
            "published": True,
            "already_enabled": False,
            "version": next_version,
            "snapshot_version": confirm.version,
            "commit_file": target.name,
            "attempts": attempts,
        }
