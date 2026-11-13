"""Flask 网页：Delta Lake 销售订单表金额非负约束启用台。

湖仓值班员在首页选择一张**本地可写样表**，先查看最新快照体检结论
（负金额订单、在用文件缺失 / 不可读清单），确认无阻塞项后一键发布标准
Delta 提交，把 ``delta.constraints.nonnegative_amount``（amount_cents >= 0）
写入元数据并把写协议升到 3。

只允许操作 SAMPLE_ROOT 白名单中的表目录；扫描不阻塞其他只增删文件的并发
提交，发布撞版本时后端自动以最新快照重验，绝不覆盖已有提交。
"""

from __future__ import annotations

import os
import threading
from pathlib import Path

from flask import Flask, jsonify, render_template, request

from .delta import (
    CONSTRAINT_EXPR,
    CONSTRAINT_KEY,
    DeltaError,
    ScanReport,
    enable_constraint,
    scan_table,
)

app = Flask(__name__)
app.config["MAX_CONTENT_LENGTH"] = 16 * 1024 * 1024

SAMPLE_ROOT = Path(os.environ.get(
    "SAMPLE_ROOT", Path(__file__).resolve().parent.parent / "samples"))

# 允许在网页操作的本地样表白名单（目录名 -> 展示信息）
SAMPLES = {
    "healthy_table": {
        "label": "健康销售订单表（数据齐全、金额非负，可直接启用约束）",
    },
    "incident_table": {
        "label": "退款冲减事故表（含负金额订单 / 缺失或不可读文件，启用应被拦截）",
    },
}

# 同进程内同一张表的发布串行化（跨进程 / 外部写入仍由 O_EXCL 提交兜底）
_table_locks: dict[str, threading.Lock] = {}
_locks_guard = threading.Lock()


def _table_lock(name: str) -> threading.Lock:
    with _locks_guard:
        lock = _table_locks.get(name)
        if lock is None:
            lock = threading.Lock()
            _table_locks[name] = lock
        return lock


def _resolve_table(name: str) -> Path:
    """白名单 + 路径收敛校验，杜绝越权访问任意目录。"""
    if name not in SAMPLES:
        raise DeltaError(f"未知样表：{name}")
    root = SAMPLE_ROOT.resolve()
    table_dir = (root / name).resolve()
    # 防御符号链接逃逸：解析后仍必须位于 SAMPLE_ROOT 之下
    if root not in table_dir.parents and table_dir != root:
        raise DeltaError(f"样表路径越界：{name}")
    if not table_dir.is_dir():
        raise DeltaError(f"样表目录不存在：{table_dir}")
    return table_dir


def _report_payload(name: str, report: ScanReport) -> dict:
    return {
        "table": name,
        "label": SAMPLES[name]["label"],
        "snapshot_version": report.snapshot_version,
        "constrained": report.constrained,
        "constraint_version": report.constraint_version,
        "constraint_key": CONSTRAINT_KEY,
        "constraint_expr": CONSTRAINT_EXPR,
        "protocol": {
            "minReaderVersion": report.protocol.min_reader_version,
            "minWriterVersion": report.protocol.min_writer_version,
            "readerFeatures": report.protocol.reader_features,
            "writerFeatures": report.protocol.writer_features,
        },
        "schema_fields": report.schema_fields,
        "active_file_count": len(report.active_files),
        "active_files": report.active_files,
        "negatives": [
            {"order_id": r.order_id, "amount_cents": r.amount_cents,
             "amount_null": r.amount_null, "path": r.path}
            for r in report.negatives
        ],
        "missing_files": report.missing_files,
        "unreadable_files": report.unreadable_files,
        "blocked": report.blocked,
        "extra_config": report.extra_config,
    }


@app.get("/")
def index():
    return render_template("index.html", samples=SAMPLES,
                           constraint_key=CONSTRAINT_KEY,
                           constraint_expr=CONSTRAINT_EXPR)


@app.get("/api/samples")
def list_samples():
    return jsonify({key: {"label": meta["label"]}
                    for key, meta in SAMPLES.items()})


@app.get("/api/table/<name>")
def table_status(name: str):
    """以最新快照为准返回一张表的体检结论（只读，不产生提交）。"""
    try:
        table_dir = _resolve_table(name)
        report = scan_table(table_dir)
    except DeltaError as exc:
        return jsonify({"error": str(exc)}), 400
    return jsonify(_report_payload(name, report))


@app.post("/api/table/<name>/enable")
def table_enable(name: str):
    """体检通过后原子发布约束提交；已启用则幂等返回启用版号。"""
    try:
        table_dir = _resolve_table(name)
        with _table_lock(name):
            result = enable_constraint(table_dir)
        # 无论发布、幂等还是被拦截，都附带最新体检结论供页面展示
        report = scan_table(table_dir)
        payload = _report_payload(name, report)
        payload["result"] = result
        status = 200
    except DeltaError as exc:
        return jsonify({"error": str(exc)}), 400
    return jsonify(payload), status


@app.errorhandler(413)
def too_large(_err):
    return jsonify({"error": "请求体超过大小上限"}), 413


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8000)
