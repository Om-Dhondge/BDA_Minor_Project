import pytest

from lib.bench import (
    NX_STEPS,
    RECORD_FIELDS,
    append_record,
    cross_checks,
    mark_unfinished,
    read_records,
    run_step,
)


def test_append_record_fills_every_field(tmp_path):
    path = tmp_path / "r.jsonl"
    append_record(path, {"engine": "spark", "step": "etl"})
    [rec] = read_records(path)
    assert set(rec) == set(RECORD_FIELDS)
    assert rec["engine"] == "spark"
    assert rec["seconds"] is None


def test_append_record_rejects_unknown_fields(tmp_path):
    with pytest.raises(ValueError):
        append_record(tmp_path / "r.jsonl", {"engin": "spark"})


def test_run_step_records_failure_and_continues(tmp_path):
    path = tmp_path / "r.jsonl"

    def record(**fields):
        append_record(path, {"engine": "networkx", **fields})

    def blow_up():
        raise MemoryError("graph too large")

    assert run_step(record, "load", "bipartite", blow_up) is False
    [rec] = read_records(path)
    assert rec["status"] == "error"
    assert "MemoryError" in rec["error"]
    assert rec["seconds"] is not None


def test_mark_unfinished_blames_the_first_missing_step(tmp_path):
    path = tmp_path / "r.jsonl"
    base = {"engine": "networkx", "scale_pct": 100, "repeat": 1}
    for step in ("load", "connected_components"):
        append_record(path, {**base, "graph": "projection", "step": step, "status": "ok"})

    missing = mark_unfinished(path, base, "projection", limit_seconds=1800)

    assert missing == ["pagerank", "label_propagation", "triangle_count"]
    statuses = {r["step"]: r["status"] for r in read_records(path)}
    assert statuses["pagerank"] == "timeout"
    assert statuses["label_propagation"] == "not_run"
    assert statuses["triangle_count"] == "not_run"


def test_mark_unfinished_ignores_other_scales(tmp_path):
    path = tmp_path / "r.jsonl"
    small = {"engine": "networkx", "scale_pct": 10, "repeat": 1}
    append_record(path, {**small, "graph": "bipartite", "step": "load", "status": "ok"})
    full = {**small, "scale_pct": 100}
    assert mark_unfinished(path, full, "bipartite", limit_seconds=60) == list(NX_STEPS["bipartite"])


def test_cross_checks_flag_mismatch_and_skip_missing():
    def rec(engine, graph, step, **fields):
        return {"engine": engine, "scale_pct": 10, "repeat": 1, "graph": graph,
                "step": step, "status": "ok", **fields}

    records = [
        rec("spark", "bipartite", "connected_components", result=5),
        rec("networkx", "bipartite", "connected_components", result=5),
        rec("spark", "projection", "triangle_count", result=100),
        rec("networkx", "projection", "triangle_count", result=99),
    ]
    outcomes = {row["check"]: row["outcome"] for row in cross_checks(records)}
    assert outcomes["bipartite.connected_components.result"] == "match"
    assert outcomes["projection.triangle_count.result"] == "MISMATCH"
    assert outcomes["projection.load.vertices"] == "unchecked"
