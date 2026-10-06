import subprocess
import sys
from pathlib import Path

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from lib.bench import (
    NX_STEPS,
    RECORD_FIELDS,
    append_record,
    cross_checks,
    mark_unfinished,
    read_parquet_frame,
    read_records,
    run_step,
)

GRAPH_DIR = Path(__file__).resolve().parent.parent


def spark_style_dir(root):
    """Two part files plus the marker files Spark writes beside them."""
    out = root / "edges.parquet"
    out.mkdir()
    pq.write_table(pa.table({"src": ["a", "b"], "dst": ["b", "c"]}), out / "part-00000-x.snappy.parquet")
    pq.write_table(pa.table({"src": ["c"], "dst": ["d"]}), out / "part-00001-x.snappy.parquet")
    (out / "_SUCCESS").touch()
    (out / ".part-00000-x.snappy.parquet.crc").write_bytes(b"\x00")
    return out


def test_read_parquet_frame_concatenates_spark_part_files(tmp_path):
    frame = read_parquet_frame(spark_style_dir(tmp_path))
    assert sorted(zip(frame["src"], frame["dst"])) == [("a", "b"), ("b", "c"), ("c", "d")]


def test_read_parquet_frame_reads_a_single_file(tmp_path):
    path = tmp_path / "pagerank.parquet"
    pd.DataFrame({"tag": ["x", "y"], "pagerank": [0.4, 0.6]}).to_parquet(path, index=False)
    assert list(read_parquet_frame(path)["tag"]) == ["x", "y"]


def test_read_parquet_frame_never_loads_pyarrow_json(tmp_path):
    # Smart App Control intermittently blocks pyarrow's unsigned _json
    # extension, which pd.read_parquet loads via pyarrow.dataset. A fresh
    # process shows what the reader itself imports.
    directory = spark_style_dir(tmp_path)
    single = tmp_path / "one.parquet"
    pd.DataFrame({"tag": ["x"]}).to_parquet(single, index=False)
    code = (
        "import sys; from lib.bench import read_parquet_frame; "
        f"read_parquet_frame(r'{directory}'); read_parquet_frame(r'{single}'); "
        "print(sorted(m for m in ('pyarrow._json', 'pyarrow.dataset') if m in sys.modules))"
    )
    run = subprocess.run([sys.executable, "-c", code], cwd=GRAPH_DIR,
                         capture_output=True, text=True, check=True)
    assert run.stdout.strip() == "[]"


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


def test_run_step_records_the_underlying_cause(tmp_path):
    # pyarrow re-raises a failed `import pyarrow.dataset` as a generic
    # ValueError; only the chained ImportError says why it failed.
    path = tmp_path / "r.jsonl"

    def record(**fields):
        append_record(path, {"engine": "networkx", **fields})

    def wrapped_failure():
        try:
            raise ImportError("DLL load failed while importing _dataset")
        except ImportError:
            raise ValueError("pyarrow.dataset module is not available")

    run_step(record, "load", "bipartite", wrapped_failure)
    [rec] = read_records(path)
    assert rec["error"].startswith("ValueError: pyarrow.dataset")
    assert "ImportError: DLL load failed" in rec["error"]


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
