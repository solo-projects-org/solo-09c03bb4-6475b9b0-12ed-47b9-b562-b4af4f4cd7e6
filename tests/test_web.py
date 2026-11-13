"""Flask 路由层测试：样表白名单、体检 API、启用 / 幂等 / 拦截行为。"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from app import web
from app.delta import CONSTRAINT_EXPR, CONSTRAINT_KEY


def test_index_renders():
    client = web.app.test_client()
    resp = client.get("/")
    assert resp.status_code == 200
    assert "金额非负约束".encode() in resp.data
    assert CONSTRAINT_KEY.encode() in resp.data


def test_sample_listing():
    client = web.app.test_client()
    resp = client.get("/api/samples")
    assert resp.status_code == 200
    data = resp.get_json()
    assert set(data) == {"healthy_table", "incident_table"}


def test_unknown_sample_rejected():
    client = web.app.test_client()
    assert client.get("/api/table/../etc_passwd").status_code in (400, 404)
    resp = client.get("/api/table/not_a_sample")
    assert resp.status_code == 400
    assert "未知样表" in resp.get_json()["error"]


def test_healthy_table_status_then_enable():
    client = web.app.test_client()
    status = client.get("/api/table/healthy_table").get_json()
    assert status["constrained"] is False
    assert status["blocked"] is False
    assert status["protocol"] == {
        "minReaderVersion": 1, "minWriterVersion": 2,
        "readerFeatures": [], "writerFeatures": []}
    assert status["snapshot_version"] == 3

    resp = client.post("/api/table/healthy_table/enable")
    assert resp.status_code == 200
    payload = resp.get_json()
    assert payload["result"]["published"] is True
    assert payload["result"]["version"] == 4
    assert payload["constrained"] is True
    assert payload["constraint_version"] == 4
    assert payload["protocol"]["minWriterVersion"] == 3
    assert payload["protocol"]["minReaderVersion"] == 1

    # 页面展示启用版号：再次调用不再提交
    again = client.post("/api/table/healthy_table/enable").get_json()
    assert again["result"]["already_enabled"] is True
    assert again["result"]["published"] is False
    assert again["constraint_version"] == 4


def test_incident_table_blocks_enable_with_details():
    client = web.app.test_client()
    status = client.get("/api/table/incident_table").get_json()
    assert status["blocked"] is True
    neg_paths = {n["path"] for n in status["negatives"]}
    assert any(n["order_id"] == 2004 and n["amount_cents"] == -9900
               for n in status["negatives"])
    assert status["missing_files"] == ["data/part-00003-incident.parquet"]
    assert status["unreadable_files"] == ["data/part-00004-incident.parquet"]
    assert neg_paths == {"data/part-00002-incident.parquet"}

    resp = client.post("/api/table/incident_table/enable")
    assert resp.status_code == 200
    payload = resp.get_json()
    assert payload["result"]["published"] is False
    assert payload["result"]["blocked"] is True

    # 事故表目录不得出现任何新提交
    log_dir = web.SAMPLE_ROOT / "incident_table" / "_delta_log"
    jsons = sorted(p.name for p in log_dir.glob("*.json"))
    assert jsons[-1] == "00000000000000000004.json"


def test_enable_commit_is_standard_delta_shape():
    client = web.app.test_client()
    client.post("/api/table/healthy_table/enable")
    commit_path = (web.SAMPLE_ROOT / "healthy_table" / "_delta_log" /
                   "00000000000000000004.json")
    lines = [json.loads(line) for line in
             commit_path.read_text(encoding="utf-8").splitlines()]
    assert lines[0] == {"protocol": {"minReaderVersion": 1,
                                     "minWriterVersion": 3}}
    meta = lines[1]["metaData"]
    assert meta["configuration"][CONSTRAINT_KEY] == CONSTRAINT_EXPR
    # 保留其余元数据键
    for key in ("id", "name", "description", "format", "schemaString",
                "partitionColumns", "createdTime"):
        assert key in meta
    assert meta["partitionColumns"] == []
    assert all(("add" not in obj and "remove" not in obj) for obj in lines)


def test_concurrent_commit_during_enable_is_not_overwritten(tmp_path: Path,
                                                            monkeypatch):
    # 用独立样表根目录搭一张可写表，并把 web 白名单临时指向它
    import app.delta as delta_mod
    from tests.test_delta import (build_basic_table, healthy_commits,
                                  write_commit, write_parquet)
    f = lambda n: f"data/part-{n:05d}.parquet"
    table = build_basic_table(
        tmp_path / "root",
        {f(0): [(1, 100)], f(1): [(2, 200)], f(2): [(3, 300)], f(3): [(4, 0)]},
        healthy_commits(f), with_checkpoint=2)
    sample_root = tmp_path / "root"
    monkeypatch.setattr(web, "SAMPLE_ROOT", sample_root)
    monkeypatch.setattr(web, "SAMPLES", {"orders": {"label": "t"}})

    real_publish = delta_mod._publish_commit_atomic

    def racing_publish(log_dir, version, actions):
        # 并发方先占 v4（只新增一个干净文件）
        write_parquet(table / "data/part-00009.parquet", [(9, 900)])
        write_commit(log_dir, 4, [{"add": {
            "path": "data/part-00009.parquet", "partitionValues": {},
            "size": 1024, "modificationTime": 1700000000000,
            "dataChange": True}}])
        return real_publish(log_dir, version, actions)

    monkeypatch.setattr(delta_mod, "_publish_commit_atomic", racing_publish)
    resp = web.app.test_client().post("/api/table/orders/enable")
    payload = resp.get_json()
    assert resp.status_code == 200
    assert payload["result"]["published"] is True
    assert payload["result"]["version"] == 5
    v4 = (table / "_delta_log" / "00000000000000000004.json").read_text()
    assert "part-00009" in v4 and '"protocol"' not in v4
    assert (table / "_delta_log" / "00000000000000000005.json").is_file()
