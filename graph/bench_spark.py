"""Benchmark worker: one Spark part at one data scale, in its own process.

Parts, run in this order by stage6_benchmark.py:
  build       sample the input, run the stage 1 and 2 transforms, and write
              this scale's graphs to <bench-dir>/scale_<pct>/
  bipartite   Connected Components on the question-tag graph
  projection  CC, PageRank, Label Propagation and Triangle Count on the tag
              projection at weight >= 1 (unthresholded - see spec §12)

Each part gets a fresh process and SparkSession: successive algorithm suites
in one session exhaust the driver heap (see stage3_algorithms.py).
"""
import argparse
import sys
from pathlib import Path

from graphframes import GraphFrame
from pyspark.sql import functions as F

from lib.algorithms import bipartite_graph_frames, symmetrize, tag_vertices
from lib.bench import append_record, run_step
from lib.etl import (
    aggregate_questions,
    explode_question_tags,
    read_posts,
    sample_questions,
)
from lib.projection import build_cooccurrence
from lib.session import build_session

DEFAULT_INPUT = r"..\dataset\Datasets_2nd run\preprocessed_posts.parquet"
LPA_MAX_ITER = 10
PAGERANK_MAX_ITER = 20
PAGERANK_RESET = 0.15


def run_build(spark, record, args, scale_dir):
    qt_path = str(scale_dir / "edges_question_tag.parquet")
    tt_path = str(scale_dir / "edges_tag_tag.parquet")

    def etl():
        posts = sample_questions(read_posts(spark, args.input), args.scale)
        explode_question_tags(aggregate_questions(posts)) \
            .write.mode("overwrite").parquet(qt_path)
        return {"edges": spark.read.parquet(qt_path).count()}

    def projection_build():
        edges = spark.read.parquet(qt_path)
        build_cooccurrence(edges).write.mode("overwrite").parquet(tt_path)
        return {"edges": spark.read.parquet(tt_path).count()}

    if run_step(record, "etl", "bipartite", etl):
        run_step(record, "projection_build", "projection", projection_build)


def run_bipartite(spark, record, args, scale_dir):
    state = {}

    def load():
        edges = spark.read.parquet(str(scale_dir / "edges_question_tag.parquet"))
        vertices, bip_edges = bipartite_graph_frames(edges)
        vertices, bip_edges = vertices.cache(), bip_edges.cache()
        state["graph"] = GraphFrame(vertices, bip_edges)
        return {"vertices": vertices.count(), "edges": bip_edges.count()}

    def connected_components():
        components = state["graph"].connectedComponents()
        return {"result": components.select("component").distinct().count()}

    if run_step(record, "load", "bipartite", load):
        run_step(record, "connected_components", "bipartite", connected_components)


def run_projection(spark, record, args, scale_dir):
    state = {}

    def load():
        edges = spark.read.parquet(str(scale_dir / "edges_tag_tag.parquet")).cache()
        vertices = tag_vertices(edges).cache()
        # PageRank needs both edge directions (spec §6.2). Building them here
        # charges that cost to load, as on the NetworkX side.
        both_ways = symmetrize(edges).cache()
        both_ways.count()
        state["undirected"] = GraphFrame(vertices, edges)
        state["directed"] = GraphFrame(vertices, both_ways)
        return {"vertices": vertices.count(), "edges": edges.count()}

    def connected_components():
        components = state["undirected"].connectedComponents()
        return {"result": components.select("component").distinct().count()}

    def pagerank():
        ranks = state["directed"].pageRank(
            resetProbability=PAGERANK_RESET, maxIter=PAGERANK_MAX_ITER
        ).vertices
        ranks.select(F.col("id").alias("tag"), "pagerank") \
            .write.mode("overwrite").parquet(str(scale_dir / "pagerank_spark.parquet"))
        return {}

    def label_propagation():
        labels = state["undirected"].labelPropagation(maxIter=LPA_MAX_ITER)
        return {"result": labels.select("label").distinct().count()}

    def triangle_count():
        total = state["undirected"].triangleCount().agg(F.sum("count")).collect()[0][0]
        # Each triangle is counted once at each of its three corners.
        return {"result": (total or 0) // 3}

    if run_step(record, "load", "projection", load):
        run_step(record, "connected_components", "projection", connected_components)
        run_step(record, "pagerank", "projection", pagerank)
        run_step(record, "label_propagation", "projection", label_propagation)
        run_step(record, "triangle_count", "projection", triangle_count)


PARTS = {"build": run_build, "bipartite": run_bipartite, "projection": run_projection}


def main():
    parser = argparse.ArgumentParser(description="Benchmark worker: Spark")
    parser.add_argument("--scale", type=int, required=True, help="Percent of questions")
    parser.add_argument("--part", choices=sorted(PARTS), required=True)
    parser.add_argument("--repeat", type=int, default=1)
    parser.add_argument("--input", default=DEFAULT_INPUT)
    parser.add_argument("--bench-dir", default=str(Path("out") / "bench"))
    args = parser.parse_args()

    if args.part == "build" and not Path(args.input).exists():
        print(f"[bench_spark] ERROR: input not found: {args.input}", file=sys.stderr)
        sys.exit(1)

    bench_dir = Path(args.bench_dir)
    scale_dir = bench_dir / f"scale_{args.scale}"
    scale_dir.mkdir(parents=True, exist_ok=True)
    base = {"engine": "spark", "scale_pct": args.scale, "repeat": args.repeat}

    def record(**fields):
        append_record(bench_dir / "records.jsonl", {**base, **fields})

    # For session_start, `graph` names the part the session was started for.
    state = {}

    def session_start():
        state["spark"] = build_session(f"bench-{args.part}-{args.scale}")
        return {}

    if not run_step(record, "session_start", args.part, session_start):
        sys.exit(1)
    PARTS[args.part](state["spark"], record, args, scale_dir)
    state["spark"].stop()


if __name__ == "__main__":
    main()
