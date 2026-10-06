"""Benchmark worker: NetworkX on one graph at one data scale.

Reads the edge lists the Spark worker wrote for this scale, so both engines
run on identical graphs. stage6_benchmark.py runs it in its own process under
a time limit and never beside a Spark JVM - the two would contend for 15.7 GB
of RAM and corrupt each other's timings.
"""
import argparse
import os
from pathlib import Path

import psutil

from lib import nx_algorithms as nxa
from lib.bench import append_record, read_parquet_frame, run_step


def peak_mb():
    """Peak working set so far. psutil reports it on Windows only; elsewhere
    this falls back to current RSS, which understates the peak."""
    info = psutil.Process(os.getpid()).memory_info()
    return round(getattr(info, "peak_wset", info.rss) / 2**20, 1)


def main():
    parser = argparse.ArgumentParser(description="Benchmark worker: NetworkX")
    parser.add_argument("--scale", type=int, required=True, help="Percent of questions")
    parser.add_argument("--graph", choices=("bipartite", "projection"), required=True)
    parser.add_argument("--repeat", type=int, default=1)
    parser.add_argument("--bench-dir", default=str(Path("out") / "bench"))
    args = parser.parse_args()

    bench_dir = Path(args.bench_dir)
    scale_dir = bench_dir / f"scale_{args.scale}"
    base = {"engine": "networkx", "scale_pct": args.scale, "repeat": args.repeat}

    def record(**fields):
        append_record(bench_dir / "records.jsonl",
                      {**base, **fields, "peak_mb": peak_mb()})

    state = {}

    def load(file_name, build):
        def step():
            state["g"] = build(read_parquet_frame(scale_dir / file_name))
            return {"vertices": state["g"].number_of_nodes(),
                    "edges": state["g"].number_of_edges()}
        return step

    def components():
        return {"result": nxa.component_count(state["g"])}

    if args.graph == "bipartite":
        if run_step(record, "load", "bipartite",
                    load("edges_question_tag.parquet", nxa.build_bipartite)):
            run_step(record, "connected_components", "bipartite", components)
        return

    def pagerank():
        nxa.pagerank_scores(state["g"]).to_parquet(
            scale_dir / "pagerank_networkx.parquet", index=False)
        return {}

    # Same order as NX_STEPS["projection"] - mark_unfinished relies on it.
    if run_step(record, "load", "projection",
                load("edges_tag_tag.parquet", nxa.build_projection)):
        run_step(record, "connected_components", "projection", components)
        run_step(record, "pagerank", "projection", pagerank)
        run_step(record, "label_propagation", "projection",
                 lambda: {"result": nxa.community_count(state["g"])})
        run_step(record, "triangle_count", "projection",
                 lambda: {"result": nxa.triangle_total(state["g"])})


if __name__ == "__main__":
    main()
