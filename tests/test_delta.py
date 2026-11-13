"""app.delta 核心逻辑测试。

运行（仓库根目录，隔离语义）：
    python -I -m pytest tests -q
"""

from __future__ import annotations

import io
import json
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from app.delta import (
    CONSTRAINT_EXPR,
    CONSTRAINT_KEY,
    DeltaError,
    ScanReport,
    TARGET_WRITER_VERSION,
    _publish_commit_atomic,
    enable_constraint,
    load_snapshot,
    scan_table,
)

ORDER_SCHEMA = pa.schema([
    pa.field("order_id", pa.int64(), nullable=False),
    pa.field("amount_cents", pa.int64(), nullable=False),
])


# --------------------------------------------------------------------------- #
# 测试表构造工具
# --------------------------------------------------------------------------- #

def write_parquet(path: Path, rows: list[tuple[int, int]],
                  schema: pa.Schema = ORDER_SCHEMA) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    ids = pa.array([r[0] for r in rows], type=pa.int64())
    amounts = pa.array([r[1] for r in rows], type=pa.int64())
    pq.write_table(pa.Table.from_arrays([ids, amounts], schema=schema), path)


def commit_name(version: int) -> str:
    return f"{version:020d}.json"


def add_action(path: str) -> dict:
    return {"add": {"path": path, "partitionValues": {}, "size": 1024,
                    "modificationTime": 1700000000000, "dataChange": True}}


def remove_action(path: str) -> dict:
    return {"remove": {"path": path, "deletionTimestamp": 1700000100000,
                       "dataChange": True, "partitionValues": {}}}


def metadata_action(configuration: dict | None = None,
                    table_id: str = "test-id") -> dict:
    schema = {"type": "struct", "fields": [
        {"name": "order_id", "type": "long", "nullable": False, "metadata": {}},
        {"name": "amount_cents", "type": "long", "nullable": False, "metadata": {}},
    ]}
    return {"metaData": {
        "id": table_id, "name": "sales_orders", "description": "t",
        "format": {"provider": "parquet", "options": {}},
        "schemaString": json.dumps(schema), "partitionColumns": [],
        "createdTime": 1700000000000, "configuration": dict(configuration or {}),
    }}


PROTOCOL_12 = {"protocol": {"minReaderVersion": 1, "minWriterVersion": 2}}


def write_commit(log_dir: Path, version: int, actions: list[dict]) -> None:
    log_dir.mkdir(parents=True, exist_ok=True)
    (log_dir / commit_name(version)).write_text(
        "\n".join(json.dumps(a) for a in actions) + "\n", encoding="utf-8")


def write_checkpoint(log_dir: Path, version: int,
                     active_paths: list[str], table_id: str = "test-id") -> None:
    n = max(len(active_paths), 1)
    add_type = pa.struct([
        ("path", pa.string()),
        ("partitionValues", pa.struct([("__empty__", pa.bool_())])),
        ("size", pa.int64()),
        ("modificationTime", pa.timestamp("us")),
        ("dataChange", pa.bool_()),
    ])
    add_rows = [{"path": p, "partitionValues": {"__empty__": None}, "size": 1024,
                 "modificationTime": 1700000000000, "dataChange": False}
                for p in active_paths] + [None] * (n - len(active_paths))
    proto_rows = [{"minReaderVersion": 1, "minWriterVersion": 2}] + [None] * (n - 1)
    meta = metadata_action(table_id=table_id)["metaData"]
    meta_type = pa.struct([
        ("id", pa.string()), ("name", pa.string()),
        ("description", pa.string()),
        ("format", pa.struct([("provider", pa.string())])),
        ("schemaString", pa.string()),
        ("partitionColumns", pa.list_(pa.string())),
        ("createdTime", pa.timestamp("us")),
        ("configuration", pa.map_(pa.string(), pa.string())),
    ])
    meta_rows = [{
        "id": meta["id"], "name": meta["name"], "description": meta["description"],
        "format": {"provider": "parquet"}, "schemaString": meta["schemaString"],
        "partitionColumns": [], "createdTime": 1700000000000,
        "configuration": list(meta["configuration"].items()),
    }] + [None] * (n - 1)
    pq.write_table(pa.table({
        "add": pa.array(add_rows, type=add_type),
        "protocol": pa.array(proto_rows, type=pa.struct([
            ("minReaderVersion", pa.int32()),
            ("minWriterVersion", pa.int32())])),
        "metaData": pa.array(meta_rows, type=meta_type),
    }), log_dir / f"{version:020d}.checkpoint.parquet")


def build_basic_table(root: Path, files: dict[str, list[tuple[int, int]]],
                      commits: list[list[dict]],
                      with_checkpoint: int | None = None) -> Path:
    table = root / "orders"
    log_dir = table / "_delta_log"
    for rel, rows in files.items():
        write_parquet(table / rel, rows)
    for version, actions in enumerate(commits):
        write_commit(log_dir, version, actions)
    if with_checkpoint is not None:
        active = []
        seen: dict[str, None] = {}
        for version, actions in enumerate(commits):
            if version > with_checkpoint:
                break
            for a in actions:
                if "add" in a:
                    seen[a["add"]["path"]] = None
                if "remove" in a:
                    seen.pop(a["remove"]["path"], None)
        write_checkpoint(log_dir, with_checkpoint, list(seen))
    return table


def healthy_commits(f) -> list[list[dict]]:
    return [
        [PROTOCOL_12, metadata_action(), add_action(f(0)), add_action(f(1))],
        [add_action(f(2))],
        [add_action(f(3)), remove_action(f(1))],
        [add_action(f(1))],
    ]


@pytest.fixture()
def healthy_table(tmp_path: Path) -> Path:
    f = lambda n: f"data/part-{n:05d}.parquet"
    files = {
        f(0): [(1, 100), (2, 200)],
        f(1): [(3, 300)],
        f(2): [(4, 0)],
        f(3): [(5, 500)],
    }
    return build_basic_table(tmp_path, files, healthy_commits(f),
                             with_checkpoint=2)


# --------------------------------------------------------------------------- #
# 快照与前置校验
# --------------------------------------------------------------------------- #

def test_snapshot_replays_commits_and_checkpoint(healthy_table: Path):
    snap = load_snapshot(healthy_table)
    assert snap.version == 3
    assert (snap.protocol.min_reader_version,
            snap.protocol.min_writer_version) == (1, 2)
    # f1 在 v2 被 remove、v3 重新 add；f0/f2/f3 一直存活
    assert sorted(snap.active_files) == sorted([
        "data/part-00000.parquet", "data/part-00001.parquet",
        "data/part-00002.parquet", "data/part-00003.parquet"])
    assert snap.metadata["partitionColumns"] == []


def test_missing_commit_detected(tmp_path: Path):
    f = lambda n: f"data/part-{n:05d}.parquet"
    # 无检查点的独立表，制造 v0..v2 后删掉中间的 v1：断档必须报错
    table = build_basic_table(
        tmp_path,
        {f(0): [(1, 100)], f(1): [(2, 200)], f(2): [(3, 300)]},
        [[PROTOCOL_12, metadata_action(), add_action(f(0))],
         [add_action(f(1))],
         [add_action(f(2))]],
        with_checkpoint=None)
    (table / "_delta_log" / commit_name(1)).unlink()
    with pytest.raises(DeltaError, match="JSON 提交不连续"):
        load_snapshot(table)


def test_trailing_gap_exposed_by_checkpoint(tmp_path: Path):
    """最新检查点版本高于现存最高 JSON 时，尾部断档同样必须报错。"""
    f = lambda n: f"data/part-{n:05d}.parquet"
    table = build_basic_table(
        tmp_path,
        {f(0): [(1, 100)], f(1): [(2, 200)]},
        [[PROTOCOL_12, metadata_action(), add_action(f(0))],
         [add_action(f(1))]],
        with_checkpoint=1)
    (table / "_delta_log" / commit_name(1)).unlink()
    with pytest.raises(DeltaError, match="JSON 提交不连续"):
        load_snapshot(table)


def test_missing_version_zero(tmp_path: Path):
    table = tmp_path / "t"
    log = table / "_delta_log"
    log.mkdir(parents=True)
    write_commit(log, 1, [add_action("data/x.parquet")])
    with pytest.raises(DeltaError, match="版本 0"):
        load_snapshot(table)


def test_no_delta_log(tmp_path: Path):
    with pytest.raises(DeltaError, match="_delta_log"):
        load_snapshot(tmp_path / "nope")


def test_deletion_vector_in_commit_rejected(tmp_path: Path):
    f = lambda n: f"data/part-{n:05d}.parquet"
    commits = [[
        {"protocol": {"minReaderVersion": 1, "minWriterVersion": 3,
                      "readerFeatures": ["deletionVectors"],
                      "writerFeatures": ["deletionVectors"]}},
        metadata_action(), add_action(f(0)),
    ]]
    table = build_basic_table(tmp_path, {f(0): [(1, 100)]}, commits)
    with pytest.raises(DeltaError, match="删除向量"):
        load_snapshot(table)


def test_deletion_vector_on_add_action_rejected(tmp_path: Path):
    f = lambda n: f"data/part-{n:05d}.parquet"
    add = add_action(f(0))
    add["add"]["deletionVector"] = {"storageType": "u", "pathOrInlineDv": "x"}
    table = build_basic_table(
        tmp_path, {f(0): [(1, 100)]},
        [[PROTOCOL_12, metadata_action(), add]])
    with pytest.raises(DeltaError, match="删除向量"):
        load_snapshot(table)


def test_partitioned_table_rejected(tmp_path: Path):
    f = lambda n: f"data/part-{n:05d}.parquet"
    meta = metadata_action()
    meta["metaData"]["partitionColumns"] = ["region"]
    table = build_basic_table(
        tmp_path, {f(0): [(1, 100)]},
        [[PROTOCOL_12, meta, add_action(f(0))]])
    with pytest.raises(DeltaError, match="无分区"):
        load_snapshot(table)


def test_partition_values_on_add_rejected(tmp_path: Path):
    f = lambda n: f"data/part-{n:05d}.parquet"
    add = add_action(f(0))
    add["add"]["partitionValues"] = {"region": "cn"}
    table = build_basic_table(
        tmp_path, {f(0): [(1, 100)]},
        [[PROTOCOL_12, metadata_action(), add]])
    with pytest.raises(DeltaError, match="partitionValues"):
        load_snapshot(table)


def test_wrong_column_type_rejected(tmp_path: Path):
    f = lambda n: f"data/part-{n:05d}.parquet"
    schema = {"type": "struct", "fields": [
        {"name": "order_id", "type": "integer", "nullable": False, "metadata": {}},
        {"name": "amount_cents", "type": "long", "nullable": False, "metadata": {}},
    ]}
    meta = metadata_action()
    meta["metaData"]["schemaString"] = json.dumps(schema)
    table = build_basic_table(
        tmp_path, {f(0): [(1, 100)]},
        [[PROTOCOL_12, meta, add_action(f(0))]])
    with pytest.raises(DeltaError, match="long（int64）"):
        load_snapshot(table)


def test_nullable_column_rejected(tmp_path: Path):
    f = lambda n: f"data/part-{n:05d}.parquet"
    schema = {"type": "struct", "fields": [
        {"name": "order_id", "type": "long", "nullable": False, "metadata": {}},
        {"name": "amount_cents", "type": "long", "nullable": True, "metadata": {}},
    ]}
    meta = metadata_action()
    meta["metaData"]["schemaString"] = json.dumps(schema)
    table = build_basic_table(
        tmp_path, {f(0): [(1, 100)]},
        [[PROTOCOL_12, meta, add_action(f(0))]])
    with pytest.raises(DeltaError, match="非空"):
        load_snapshot(table)


def test_missing_column_rejected(tmp_path: Path):
    schema = {"type": "struct", "fields": [
        {"name": "order_id", "type": "long", "nullable": False, "metadata": {}},
    ]}
    meta = metadata_action()
    meta["metaData"]["schemaString"] = json.dumps(schema)
    table = build_basic_table(
        tmp_path, {"data/x.parquet": [(1, 100)]},
        [[PROTOCOL_12, meta, add_action("data/x.parquet")]])
    with pytest.raises(DeltaError, match="amount_cents"):
        load_snapshot(table)


def test_unsupported_protocol_rejected(tmp_path: Path):
    f = lambda n: f"data/part-{n:05d}.parquet"
    table = build_basic_table(
        tmp_path, {f(0): [(1, 100)]},
        [[{"protocol": {"minReaderVersion": 2, "minWriterVersion": 5}},
          metadata_action(), add_action(f(0))]])
    with pytest.raises(DeltaError, match="读协议"):
        load_snapshot(table)


# --------------------------------------------------------------------------- #
# 体检：负金额 / 缺失 / 不可读
# --------------------------------------------------------------------------- #

def test_scan_clean_table(healthy_table: Path):
    report = scan_table(healthy_table)
    assert isinstance(report, ScanReport)
    assert report.blocked is False
    assert report.negatives == []
    assert report.missing_files == []
    assert report.unreadable_files == []
    assert report.snapshot_version == 3


def test_negative_amounts_list_order_id_and_path(tmp_path: Path):
    f = lambda n: f"data/part-{n:05d}.parquet"
    table = build_basic_table(
        tmp_path,
        {f(0): [(1, 100), (2, -50)], f(1): [(3, -9), (4, 0)]},
        [[PROTOCOL_12, metadata_action(), add_action(f(0)), add_action(f(1))]])
    report = scan_table(table)
    assert report.blocked is True
    negatives = [(r.order_id, r.amount_cents, r.path) for r in report.negatives]
    assert negatives == [
        (2, -50, f(0)),
        (3, -9, f(1)),
    ]


def test_missing_active_file_reported_by_path(tmp_path: Path):
    f = lambda n: f"data/part-{n:05d}.parquet"
    table = build_basic_table(
        tmp_path, {f(0): [(1, 100)], f(1): [(2, 200)]},
        [[PROTOCOL_12, metadata_action(), add_action(f(0)), add_action(f(1))]])
    (table / f(1)).unlink()
    report = scan_table(table)
    assert report.missing_files == [f(1)]
    assert report.blocked is True


def test_unreadable_parquet_reported(tmp_path: Path):
    f = lambda n: f"data/part-{n:05d}.parquet"
    table = build_basic_table(
        tmp_path, {f(0): [(1, 100)]},
        [[PROTOCOL_12, metadata_action(), add_action(f(0))]])
    (table / f(0)).write_bytes(b"corrupt-not-parquet")
    report = scan_table(table)
    assert report.unreadable_files == [f(0)]
    assert report.blocked is True


def test_null_amount_flagged_as_violation(tmp_path: Path):
    f = lambda n: f"data/part-{n:05d}.parquet"
    nullable_schema = pa.schema([
        pa.field("order_id", pa.int64(), nullable=False),
        pa.field("amount_cents", pa.int64(), nullable=True),
    ])
    table = build_basic_table(
        tmp_path, {f(0): [(5, 10)]},
        [[PROTOCOL_12, metadata_action(), add_action(f(0))]],
        with_checkpoint=None)
    # 物理文件允许 null（Delta 元数据仍声明非空）：null 金额过不了 >= 0
    pq.write_table(pa.Table.from_arrays([
        pa.array([6], type=pa.int64()), pa.array([None], type=pa.int64())],
        schema=nullable_schema), table / f(1))
    write_commit(table / "_delta_log", 1, [add_action(f(1))])
    report = scan_table(table)
    assert report.blocked is True
    row = next(r for r in report.negatives if r.order_id == 6)
    assert row.amount_null is True
    assert row.amount_cents is None
    assert row.path == f(1)


def test_removed_files_are_not_scanned(tmp_path: Path):
    """已退出快照的文件即使物理缺失也不算问题。"""
    f = lambda n: f"data/part-{n:05d}.parquet"
    table = build_basic_table(
        tmp_path, {f(0): [(1, 100)], f(1): [(2, -777)]},
        [[PROTOCOL_12, metadata_action(), add_action(f(0)), add_action(f(1))],
         [remove_action(f(1))]])
    # f1 已 remove：物理文件删掉且内含负金额，都不应影响体检
    (table / f(1)).unlink()
    report = scan_table(table)
    assert report.blocked is False
    assert report.negatives == []
    assert report.active_files == [f(0)]


def test_newly_added_file_is_scanned(tmp_path: Path):
    """并发新提交引入的新文件，重新扫描时必须覆盖到。"""
    f = lambda n: f"data/part-{n:05d}.parquet"
    table = build_basic_table(
        tmp_path, {f(0): [(1, 100)]},
        [[PROTOCOL_12, metadata_action(), add_action(f(0))]])
    write_parquet(table / f(1), [(9, -1)])
    write_commit(table / "_delta_log", 1, [add_action(f(1))])
    report = scan_table(table)
    assert report.snapshot_version == 1
    assert [(r.order_id, r.path) for r in report.negatives] == [(9, f(1))]


# --------------------------------------------------------------------------- #
# 发布：提交内容、原子性、幂等与并发冲突
# --------------------------------------------------------------------------- #

def test_enable_publishes_protocol_and_metadata(healthy_table: Path):
    result = enable_constraint(healthy_table)
    assert result["published"] is True
    assert result["version"] == 4
    commit = json.loads((healthy_table / "_delta_log" /
                         commit_name(4)).read_text().splitlines()[0])
    assert commit == {"protocol": {"minReaderVersion": 1,
                                   "minWriterVersion": TARGET_WRITER_VERSION}}
    meta_line = json.loads((healthy_table / "_delta_log" /
                            commit_name(4)).read_text().splitlines()[1])
    meta = meta_line["metaData"]
    assert meta["configuration"][CONSTRAINT_KEY] == CONSTRAINT_EXPR
    # 其余元数据保留
    assert meta["id"] == "test-id"
    assert meta["partitionColumns"] == []
    assert meta["schemaString"].startswith('{"type": "struct"')
    assert "createdTime" in meta and "lastUpdate" in meta
    # 不含任何 add/remove，Parquet 不被改写
    text = (healthy_table / "_delta_log" / commit_name(4)).read_text()
    assert '"add"' not in text and '"remove"' not in text

    snap = load_snapshot(healthy_table)
    assert snap.protocol.min_writer_version == 3
    assert snap.protocol.min_reader_version == 1
    assert snap.constrained is True
    assert snap.constraint_version == 4


def test_enable_is_idempotent(healthy_table: Path):
    first = enable_constraint(healthy_table)
    assert first["published"] is True
    second = enable_constraint(healthy_table)
    assert second["published"] is False
    assert second["already_enabled"] is True
    assert second["version"] == 4
    # 没有产生额外提交
    logs = list((healthy_table / "_delta_log").glob("*.json"))
    assert sorted(p.name for p in logs)[-1] == commit_name(4)


def test_enable_blocked_by_negative_or_file_issues_publishes_nothing(tmp_path: Path):
    f = lambda n: f"data/part-{n:05d}.parquet"
    table = build_basic_table(
        tmp_path, {f(0): [(1, -10)], f(1): [(2, 200)]},
        [[PROTOCOL_12, metadata_action(), add_action(f(0)), add_action(f(1))]])
    (table / f(1)).unlink()
    before = sorted(p.name for p in (table / "_delta_log").iterdir())
    result = enable_constraint(table)
    after = sorted(p.name for p in (table / "_delta_log").iterdir())
    assert result["published"] is False
    assert result["blocked"] is True
    assert before == after  # 没有任何新文件
    assert result["negatives"][0]["order_id"] == 1
    assert result["missing_files"] == [f(1)]


def test_publish_never_overwrites_existing_commit(healthy_table: Path,
                                                   monkeypatch: pytest.MonkeyPatch):
    log_dir = healthy_table / "_delta_log"
    # 先占住 v4，模拟并发写入方抢先提交了一个只增文件的提交
    write_commit(log_dir, 4, [add_action("data/part-99999.parquet")])
    # 该新文件真实存在且数据干净，保证重验能通过
    write_parquet(healthy_table / "data/part-99999.parquet", [(99, 99)])

    result = enable_constraint(healthy_table)
    assert result["published"] is True
    assert result["version"] == 5  # 重验后改发 v5，未覆盖 v4
    v4 = (log_dir / commit_name(4)).read_text()
    assert "part-99999" in v4 and "protocol" not in v4
    snap = load_snapshot(healthy_table)
    assert snap.constraint_version == 5
    assert snap.protocol.min_writer_version == 3


def test_publish_conflict_retries_and_revalidates(tmp_path: Path,
                                                   monkeypatch: pytest.MonkeyPatch):
    """扫描通过后、发布前出现并发新提交且引入负金额：必须重验并拦截。"""
    f = lambda n: f"data/part-{n:05d}.parquet"
    table = build_basic_table(
        tmp_path, {f(0): [(1, 100)]},
        [[PROTOCOL_12, metadata_action(), add_action(f(0))]])
    log_dir = table / "_delta_log"

    import app.delta as delta_mod
    real_publish = delta_mod._publish_commit_atomic
    calls = {"n": 0}

    def fake_publish(log_dir_arg, version, actions):
        calls["n"] += 1
        if calls["n"] == 1:
            # 并发写入方抢先占掉 v1，并引入带负金额的新文件
            write_parquet(table / f(1), [(8, -3)])
            write_commit(log_dir, 1, [add_action(f(1))])
            return real_publish(log_dir_arg, version, actions)  # 必然撞名
        return real_publish(log_dir_arg, version, actions)

    monkeypatch.setattr(delta_mod, "_publish_commit_atomic", fake_publish)
    result = enable_constraint(table)
    assert result["published"] is False
    assert result["blocked"] is True
    assert result["negatives"][0]["order_id"] == 8
    # 只有并发方的 v1，没有我们的约束提交
    assert sorted(p.name for p in log_dir.glob("*.json")) == [
        commit_name(0), commit_name(1)]


def test_atomic_publish_raises_on_existing(tmp_path: Path):
    log_dir = tmp_path / "_delta_log"
    log_dir.mkdir()
    _publish_commit_atomic(log_dir, 0, [{"a": 1}])
    from app.delta import CommitConflict
    with pytest.raises(CommitConflict):
        _publish_commit_atomic(log_dir, 0, [{"a": 2}])
    # 原文件内容未被覆盖
    assert json.loads((log_dir / commit_name(0)).read_text()) == {"a": 1}


def test_extra_configuration_is_preserved(tmp_path: Path):
    f = lambda n: f"data/part-{n:05d}.parquet"
    meta = metadata_action(configuration={"delta.enableChangeDataFeed": "true"})
    table = build_basic_table(
        tmp_path, {f(0): [(1, 100)]},
        [[PROTOCOL_12, meta, add_action(f(0))]])
    enable_constraint(table)
    snap = load_snapshot(table)
    config = snap.metadata["configuration"]
    assert config[CONSTRAINT_KEY] == CONSTRAINT_EXPR
    assert config["delta.enableChangeDataFeed"] == "true"


def test_constraint_detected_when_already_in_metadata(tmp_path: Path):
    f = lambda n: f"data/part-{n:05d}.parquet"
    meta = metadata_action(configuration={CONSTRAINT_KEY: CONSTRAINT_EXPR})
    table = build_basic_table(
        tmp_path, {f(0): [(1, 100)]},
        [[{"protocol": {"minReaderVersion": 1, "minWriterVersion": 3}},
          meta, add_action(f(0))]])
    snap = load_snapshot(table)
    assert snap.constrained is True
    assert snap.constraint_version == 0
    result = enable_constraint(table)
    assert result["already_enabled"] is True
    assert result["version"] == 0
