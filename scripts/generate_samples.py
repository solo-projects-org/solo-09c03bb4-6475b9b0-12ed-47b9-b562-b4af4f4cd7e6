"""生成本地 Delta Lake 销售订单样表，供网页一键操作。

两张表均为 Delta 协议 1/2 起步、无分区、无删除向量，且只有
``order_id``、``amount_cents`` 两列（均为非空 int64）。不依赖
Spark / Delta Lake 运行时，提交为 ndjson，检查点为经典单文件 Parquet。

- ``healthy_table``：全部在用文件可读且金额非负，体检通过后可成功发布
  协议升级（writer=3）+ ``delta.constraints.nonnegative_amount`` 提交；
- ``incident_table``：保留退款误记账的负金额订单、一个在用但缺失的数据
  文件和一个损坏（不可读）的数据文件，启用约束必须被逐项拦截、不得发布。

运行：python -I scripts/generate_samples.py
"""

from __future__ import annotations

import json
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

ROOT = Path(__file__).resolve().parent.parent
SAMPLES = ROOT / "samples"

ORDER_SCHEMA = pa.schema([
    pa.field("order_id", pa.int64(), nullable=False),
    pa.field("amount_cents", pa.int64(), nullable=False),
])

# ---- Delta 检查点列结构（仅保留快照所需字段，无 deletionVector 列） ----
# Parquet 不支持零字段 struct，空 partitionValues 用一个占位子字段表达
_EMPTY_PARTITION = pa.struct([("__empty__", pa.bool_())])
ADD_TYPE = pa.struct([
    ("path", pa.string()),
    ("partitionValues", _EMPTY_PARTITION),
    ("size", pa.int64()),
    ("modificationTime", pa.timestamp("us")),
    ("dataChange", pa.bool_()),
])
REMOVE_TYPE = pa.struct([
    ("path", pa.string()),
    ("deletionTimestamp", pa.timestamp("us")),
    ("dataChange", pa.bool_()),
])
PROTOCOL_TYPE = pa.struct([
    ("minReaderVersion", pa.int32()),
    ("minWriterVersion", pa.int32()),
])
METADATA_TYPE = pa.struct([
    ("id", pa.string()),
    ("name", pa.string()),
    ("description", pa.string()),
    ("format", pa.struct([("provider", pa.string())])),
    ("schemaString", pa.string()),
    ("partitionColumns", pa.list_(pa.string())),
    ("createdTime", pa.timestamp("us")),
    ("configuration", pa.map_(pa.string(), pa.string())),
])

PROTOCOL_12 = {"minReaderVersion": 1, "minWriterVersion": 2}
BASE_TIME = 1700000000000


def commit_name(version: int) -> str:
    return f"{version:020d}.json"


def checkpoint_name(version: int) -> str:
    return f"{version:020d}.checkpoint.parquet"


def write_commit(log_dir: Path, version: int, actions: list[dict]) -> None:
    lines = [json.dumps(action, ensure_ascii=False) for action in actions]
    (log_dir / commit_name(version)).write_text(
        "\n".join(lines) + "\n", encoding="utf-8")


def add_action(path: str) -> dict:
    return {"add": {"path": path, "partitionValues": {}, "size": 1024,
                    "modificationTime": BASE_TIME, "dataChange": True}}


def remove_action(path: str) -> dict:
    return {"remove": {"path": path, "deletionTimestamp": BASE_TIME + 100_000,
                       "dataChange": True, "partitionValues": {}}}


def metadata_action(table_id: str, configuration: dict | None = None) -> dict:
    schema = {"type": "struct", "fields": [
        {"name": "order_id", "type": "long", "nullable": False, "metadata": {}},
        {"name": "amount_cents", "type": "long", "nullable": False, "metadata": {}},
    ]}
    return {"metaData": {
        "id": table_id,
        "name": "sales_orders",
        "description": "Delta Lake 销售订单样表",
        "format": {"provider": "parquet", "options": {}},
        "schemaString": json.dumps(schema, ensure_ascii=False),
        "partitionColumns": [],
        "createdTime": BASE_TIME,
        "configuration": dict(configuration or {}),
    }}


def write_data_file(table_dir: Path, rel_path: str,
                    rows: list[tuple[int, int]]) -> Path:
    target = table_dir / rel_path
    target.parent.mkdir(parents=True, exist_ok=True)
    order_ids = pa.array([r[0] for r in rows], type=pa.int64())
    amounts = pa.array([r[1] for r in rows], type=pa.int64())
    table = pa.Table.from_arrays([order_ids, amounts], schema=ORDER_SCHEMA)
    pq.write_table(table, target)
    return target


def write_checkpoint(log_dir: Path, version: int,
                     active_paths: list[str], tombstone_paths: list[str],
                     table_id: str) -> None:
    """写经典单文件 Parquet 检查点（含 protocol / metaData / add / remove）。"""
    n_add, n_rm = len(active_paths), len(tombstone_paths)
    n_rows = max(n_add, n_rm, 1)

    add_rows = [
        {"path": p, "partitionValues": {"__empty__": None}, "size": 1024,
         "modificationTime": BASE_TIME, "dataChange": False}
        for p in active_paths
    ] + [None] * (n_rows - n_add)
    rm_rows = [
        {"path": p, "deletionTimestamp": BASE_TIME + 100_000,
         "dataChange": False}
        for p in tombstone_paths
    ] + [None] * (n_rows - n_rm)
    protocol_rows = [dict(PROTOCOL_12)] + [None] * (n_rows - 1)
    meta = metadata_action(table_id)["metaData"]
    metadata_rows = [{
        "id": meta["id"], "name": meta["name"],
        "description": meta["description"],
        "format": {"provider": "parquet"},
        "schemaString": meta["schemaString"],
        "partitionColumns": [], "createdTime": BASE_TIME,
        "configuration": list(meta["configuration"].items()),
    }] + [None] * (n_rows - 1)

    table = pa.table({
        "add": pa.array(add_rows, type=ADD_TYPE),
        "remove": pa.array(rm_rows, type=REMOVE_TYPE),
        "protocol": pa.array(protocol_rows, type=PROTOCOL_TYPE),
        "metaData": pa.array(metadata_rows, type=METADATA_TYPE),
    })
    pq.write_table(table, log_dir / checkpoint_name(version))
    (log_dir / "_last_checkpoint").write_text(
        json.dumps({"version": version, "size": 0}), encoding="utf-8")


def reset_dir(path: Path) -> None:
    if path.exists():
        for child in sorted(path.rglob("*"), reverse=True):
            if child.is_file() or child.is_symlink():
                child.unlink()
            else:
                child.rmdir()
    path.mkdir(parents=True, exist_ok=True)


def build_healthy() -> None:
    """健康表：v0-v3 JSON 连续，v2 检查点；4 个在用文件全部可读且金额非负。"""
    table_dir = SAMPLES / "healthy_table"
    reset_dir(table_dir)
    log_dir = table_dir / "_delta_log"
    log_dir.mkdir(parents=True)
    table_id = "healthy-table-id-0001"

    f = lambda n: f"data/part-{n:05d}-healthy.parquet"
    write_data_file(table_dir, f(0), [(1001, 12900), (1002, 4500)])
    write_data_file(table_dir, f(1), [(1003, 8800)])
    write_data_file(table_dir, f(2), [(1004, 0), (1005, 19900)])
    write_data_file(table_dir, f(3), [(1006, 3200)])

    write_commit(log_dir, 0, [
        {"protocol": dict(PROTOCOL_12)}, metadata_action(table_id),
        add_action(f(0)), add_action(f(1)),
    ])
    write_commit(log_dir, 1, [add_action(f(2))])
    write_commit(log_dir, 2, [add_action(f(3)), remove_action(f(1))])
    # v2 快照在用文件：f0, f2, f3；f1 为墓碑（物理文件保留也不计入扫描）
    write_checkpoint(log_dir, 2, [f(0), f(2), f(3)], [f(1)], table_id)
    write_commit(log_dir, 3, [add_action(f(1))])  # 重新加回 f1（新动作、新快照引用）


def build_incident() -> None:
    """事故表：负金额订单 + 在用文件缺失 + 在用文件损坏，三类阻塞各一。"""
    table_dir = SAMPLES / "incident_table"
    reset_dir(table_dir)
    log_dir = table_dir / "_delta_log"
    log_dir.mkdir(parents=True)
    table_id = "incident-table-id-0002"

    f = lambda n: f"data/part-{n:05d}-incident.parquet"
    # 正常文件
    write_data_file(table_dir, f(0), [(2001, 15800), (2002, 7600)])
    write_data_file(table_dir, f(1), [(2003, 4200)])
    # 负金额：退款已单独冲减销售额，却又误写进订单表（会再扣一次）
    write_data_file(table_dir, f(2), [(2004, -9900), (2005, 6300)])
    # f3：在用但物理文件缺失（模拟对象存储误删）
    # f4：在用但内容损坏，Parquet 读不出来
    corrupt = table_dir / f(4)
    corrupt.parent.mkdir(parents=True, exist_ok=True)
    corrupt.write_bytes(b"this is not a parquet file anymore")
    # f5：正常文件，保证最新快照里同时存在健康数据
    write_data_file(table_dir, f(5), [(2006, 1200), (2007, 0)])

    write_commit(log_dir, 0, [
        {"protocol": dict(PROTOCOL_12)}, metadata_action(table_id),
        add_action(f(0)), add_action(f(1)),
    ])
    write_commit(log_dir, 1, [add_action(f(2))])
    write_commit(log_dir, 2, [add_action(f(3))])
    write_checkpoint(log_dir, 2, [f(0), f(1), f(2), f(3)], [], table_id)
    write_commit(log_dir, 3, [add_action(f(4))])
    write_commit(log_dir, 4, [add_action(f(5)), remove_action(f(1))])
    # 最新快照在用：f0, f2, f3(缺失), f4(损坏), f5；f1 已退出，不参与扫描


def main() -> None:
    build_healthy()
    build_incident()
    print(f"样表已生成于 {SAMPLES}")
    for table in sorted(SAMPLES.iterdir()):
        if table.is_dir():
            logs = sorted(p.name for p in (table / "_delta_log").iterdir())
            print(f"\n[{table.name}] _delta_log:")
            for name in logs:
                print(f"  - {name}")


if __name__ == "__main__":
    main()
