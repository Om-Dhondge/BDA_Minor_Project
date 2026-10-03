"""Benchmark plumbing shared by the orchestrator and both workers.

Imports neither Spark nor NetworkX: the NetworkX worker must not pay for a
PySpark import, and the orchestrator must not hold either engine's memory
while the workers run.
"""
import json
import time
from pathlib import Path

SCALES = (10, 25, 50, 100)

RECORD_FIELDS = (
    "engine", "scale_pct", "repeat", "graph", "step", "status",
    "seconds", "vertices", "edges", "result", "peak_mb", "error",
)

# NetworkX steps per graph, in the order bench_networkx.py runs them. The
# orchestrator uses this to tell which step a killed worker was inside.
NX_STEPS = {
    "bipartite": ("load", "connected_components"),
    "projection": ("load", "connected_components", "pagerank",
                   "label_propagation", "triangle_count"),
}

# (graph, step, field) triples that must be identical when both engines ran
# the same graph. PageRank is compared by rank correlation in the
# orchestrator; LPA is not compared at all (spec §12).
EXACT_CHECKS = (
    ("bipartite", "load", "vertices"),
    ("bipartite", "load", "edges"),
    ("bipartite", "connected_components", "result"),
    ("projection", "load", "vertices"),
    ("projection", "load", "edges"),
    ("projection", "connected_components", "result"),
    ("projection", "triangle_count", "result"),
)


def append_record(path, record):
    """Append one timed step as a JSON line, with every field present."""
    unknown = set(record) - set(RECORD_FIELDS)
    if unknown:
        raise ValueError(f"unknown record fields: {sorted(unknown)}")
    line = json.dumps({field: record.get(field) for field in RECORD_FIELDS})
    with open(path, "a", encoding="utf-8") as f:
        f.write(line + "\n")


def read_records(path):
    """Every record in the log, oldest first. A missing log reads as empty."""
    path = Path(path)
    if not path.exists():
        return []
    with open(path, encoding="utf-8") as f:
        return [json.loads(line) for line in f if line.strip()]


def run_step(record, step, graph, fn):
    """Time fn(), record the outcome, and return True if it succeeded.

    fn returns a dict of extra record fields, e.g. {"result": 119}. A failure
    is recorded rather than raised, so one algorithm blowing up does not hide
    the timings of the ones after it.
    """
    start = time.perf_counter()
    try:
        fields = fn() or {}
        status, error = "ok", None
    except Exception as exc:  # recorded in the results, not swallowed
        fields, status, error = {}, "error", f"{type(exc).__name__}: {exc}"[:500]
    record(step=step, graph=graph, status=status, error=error,
           seconds=round(time.perf_counter() - start, 3), **fields)
    return status == "ok"


def mark_unfinished(path, base, graph, limit_seconds):
    """Record the steps a killed NetworkX worker never reported.

    The first missing step is the one it was inside when the limit hit; the
    rest never started. Returns the missing step names.
    """
    done = {
        r["step"] for r in read_records(path)
        if r["graph"] == graph and all(r[k] == v for k, v in base.items())
    }
    missing = [step for step in NX_STEPS[graph] if step not in done]
    for i, step in enumerate(missing):
        append_record(path, {
            **base, "graph": graph, "step": step,
            "status": "timeout" if i == 0 else "not_run",
            "error": f"killed at the {limit_seconds:.0f}s run limit" if i == 0 else None,
        })
    return missing


def cross_checks(records):
    """Compare the engines per scale on quantities that must match exactly.

    Uses repeat 1 only; later repeats run the same graphs. A check with a
    missing side (timeout, error) is "unchecked", never a mismatch.
    """
    found = {
        (r["engine"], r["scale_pct"], r["graph"], r["step"]): r
        for r in records if r["repeat"] == 1 and r["status"] == "ok"
    }
    rows = []
    for pct in sorted({r["scale_pct"] for r in records}):
        for graph, step, field in EXACT_CHECKS:
            spark = found.get(("spark", pct, graph, step), {}).get(field)
            networkx = found.get(("networkx", pct, graph, step), {}).get(field)
            if spark is None or networkx is None:
                outcome = "unchecked"
            else:
                outcome = "match" if spark == networkx else "MISMATCH"
            rows.append({"scale_pct": pct, "check": f"{graph}.{step}.{field}",
                         "spark": spark, "networkx": networkx, "outcome": outcome})
    return rows
