"""Stage 6: Spark vs NetworkX across data scales (synopsis objective 6).

Every (engine, scale, graph) runs in its own process, one at a time:
  spark build -> spark bipartite -> spark projection
  -> networkx bipartite -> networkx projection
NetworkX runs are killed after --nx-timeout-min and recorded as timeouts.
Spark runs have no limit: killing the Python driver would orphan its JVM.

Spark here is local mode on one machine's cores, not a cluster. Report it
that way.
"""
import argparse
import subprocess
import sys
from pathlib import Path

import pandas as pd

from lib.bench import (
    SCALES,
    cross_checks,
    mark_unfinished,
    read_parquet_frame,
    read_records,
)

HERE = Path(__file__).resolve().parent


def log(msg):
    print(f"[stage6] {msg}", flush=True)


def run_worker(script, args, timeout=None):
    """Run one worker to completion. Returns False if it hit the timeout."""
    cmd = [sys.executable, str(HERE / script), *map(str, args)]
    log(" ".join(cmd[1:]))
    try:
        code = subprocess.run(cmd, cwd=HERE, timeout=timeout, check=False).returncode
    except subprocess.TimeoutExpired:
        return False
    if code != 0:
        log(f"  exited with {code}; its records say which step failed")
    return True


def pagerank_agreement(bench_dir, pct):
    """Spearman correlation of the two engines' PageRank, or None if either
    side did not finish."""
    spark_path = bench_dir / f"scale_{pct}" / "pagerank_spark.parquet"
    nx_path = bench_dir / f"scale_{pct}" / "pagerank_networkx.parquet"
    if not (spark_path.exists() and nx_path.exists()):
        return None
    merged = read_parquet_frame(spark_path).merge(
        read_parquet_frame(nx_path), on="tag", suffixes=("_spark", "_nx"))
    return round(merged["pagerank_spark"].corr(merged["pagerank_nx"],
                                               method="spearman"), 4)


def main():
    parser = argparse.ArgumentParser(description="Stage 6: Spark vs NetworkX")
    parser.add_argument("--scales", type=int, nargs="+", default=list(SCALES),
                        help="Percent of questions, e.g. --scales 10 25")
    parser.add_argument("--repeats", type=int, default=1)
    parser.add_argument("--nx-timeout-min", type=float, default=30)
    parser.add_argument("--input", help="Override bench_spark.py's input Parquet")
    parser.add_argument("--out-dir", default="out")
    parser.add_argument("--append", action="store_true",
                        help="Keep earlier records instead of starting fresh")
    args = parser.parse_args()

    out = Path(args.out_dir).resolve()
    bench_dir = out / "bench"
    bench_dir.mkdir(parents=True, exist_ok=True)
    log_path = bench_dir / "records.jsonl"
    if log_path.exists() and not args.append:
        log_path.unlink()

    limit = args.nx_timeout_min * 60
    input_args = ["--input", Path(args.input).resolve()] if args.input else []

    for repeat in range(1, args.repeats + 1):
        for pct in args.scales:
            log(f"=== repeat {repeat}, scale {pct}% ===")
            common = ["--scale", pct, "--repeat", repeat, "--bench-dir", bench_dir]
            run_worker("bench_spark.py", [*common, "--part", "build", *input_args])
            for part in ("bipartite", "projection"):
                run_worker("bench_spark.py", [*common, "--part", part])
            for graph in ("bipartite", "projection"):
                if not run_worker("bench_networkx.py", [*common, "--graph", graph],
                                  timeout=limit):
                    base = {"engine": "networkx", "scale_pct": pct, "repeat": repeat}
                    missing = mark_unfinished(log_path, base, graph, limit)
                    if missing:
                        log(f"  networkx {graph} hit the {args.nx_timeout_min:g} min "
                            f"limit during {missing[0]}")

    records = read_records(log_path)
    results_dir = out / "results"
    results_dir.mkdir(parents=True, exist_ok=True)
    frame = pd.DataFrame(records)
    frame.to_csv(results_dir / "benchmark.csv", index=False)

    checks = cross_checks(records)
    for pct in sorted(frame["scale_pct"].unique()):
        rho = pagerank_agreement(bench_dir, pct)
        checks.append({"scale_pct": int(pct), "check": "projection.pagerank.spearman",
                       "spark": None, "networkx": None,
                       "outcome": "unchecked" if rho is None else f"spearman {rho}"})
    pd.DataFrame(checks).to_csv(results_dir / "benchmark_checks.csv", index=False)

    ok = frame[frame["status"] == "ok"]
    log("median seconds:\n" + ok.pivot_table(
        index=["graph", "step"], columns=["engine", "scale_pct"],
        values="seconds", aggfunc="median").round(1).to_string())
    for row in checks:
        log(f"{row['scale_pct']:>3}%  {row['check']:<40} {row['outcome']}")
    mismatches = sum(row["outcome"] == "MISMATCH" for row in checks)
    if mismatches:
        log(f"WARNING: {mismatches} cross-engine mismatches - the engines did not "
            "run the same graph, or one of them is wrong. Timings are not "
            "comparable until this is resolved.")


if __name__ == "__main__":
    main()
