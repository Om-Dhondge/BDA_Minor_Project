"""Stage 3: run GraphFrames algorithms over the tag projection.

PageRank here ranks TECHNOLOGIES, not users. The synopsis frames PageRank as
an expert-ranking method; that version needs OwnerUserId, which the current
dataset does not carry. Column names say "authority", never "expert".
"""
import argparse
import shutil
import sys
from pathlib import Path

from graphframes import GraphFrame
from pyspark.sql import functions as F

from lib.algorithms import (
    PRIMARY_THRESHOLD,
    SWEEP_THRESHOLDS,
    join_tag_metrics,
    symmetrize,
    tag_vertices,
    threshold_edges,
)
from lib.session import build_session

LPA_MAX_ITER = 10
PAGERANK_MAX_ITER = 20
PAGERANK_RESET = 0.15


def log(msg):
    print(f"[stage3] {msg}", flush=True)


def run_projection_algorithms(edges, min_weight, attributes):
    """Run all four algorithms at one threshold. Returns one row per tag."""
    filtered = threshold_edges(edges, min_weight).cache()
    vertices = tag_vertices(filtered)

    undirected = GraphFrame(vertices, filtered)
    lpa = undirected.labelPropagation(maxIter=LPA_MAX_ITER) \
        .select(F.col("id").alias("tag"), F.col("label").alias("community"))
    cc = undirected.connectedComponents() \
        .select(F.col("id").alias("tag"), F.col("component"))
    tri = undirected.triangleCount() \
        .select(F.col("id").alias("tag"), F.col("count").alias("triangle_count"))

    # PageRank is directional and needs both edge directions.
    directed = GraphFrame(vertices, symmetrize(filtered))
    pr = directed.pageRank(
        resetProbability=PAGERANK_RESET, maxIter=PAGERANK_MAX_ITER
    ).vertices.select(F.col("id").alias("tag"),
                      F.col("pagerank").alias("tag_authority"))

    return join_tag_metrics(pr, lpa, cc, tri, attributes)


def run_bipartite_components(spark, edges_path):
    """Connected Components over the question-tag bipartite graph.

    Triangle Count is deliberately NOT run here. Bipartite graphs contain no
    odd-length cycles, so their triangle count is identically zero - a
    definition, not a defect.
    """
    from lib.algorithms import bipartite_graph_frames

    edges = spark.read.parquet(str(edges_path))
    vertices, bip_edges = bipartite_graph_frames(edges)
    graph = GraphFrame(vertices, bip_edges)

    components = graph.connectedComponents()
    return (
        components.groupBy("component")
        .agg(
            F.count(F.lit(1)).alias("size"),
            F.sum(F.when(F.col("id").startswith("t:"), 1).otherwise(0))
             .alias("tag_count"),
            F.sum(F.when(F.col("id").startswith("q:"), 1).otherwise(0))
             .alias("question_count"),
        )
        .orderBy(F.col("size").desc())
    )


def main():
    parser = argparse.ArgumentParser(description="Stage 3: graph algorithms")
    parser.add_argument("--out-dir", default="out")
    parser.add_argument("--skip-sweep", action="store_true",
                        help="Run only the primary threshold")
    parser.add_argument("--bipartite", action="store_true",
                        help="Also run Connected Components on the bipartite graph")
    args = parser.parse_args()

    out = Path(args.out_dir)
    edges_path = out / "edges_tag_tag.parquet"
    tags_path = out / "nodes_tag.parquet"
    for p in (edges_path, tags_path):
        if not p.exists():
            print(f"[stage3] ERROR: {p} not found. Run stage1 and stage2 first.",
                  file=sys.stderr)
            sys.exit(1)

    results_dir = out / "results"
    results_dir.mkdir(parents=True, exist_ok=True)

    spark = build_session("stage3-algorithms")
    edges = spark.read.parquet(str(edges_path)).cache()
    attrs = spark.read.parquet(str(tags_path))

    log(f"primary threshold: weight >= {PRIMARY_THRESHOLD}")
    metrics = run_projection_algorithms(edges, PRIMARY_THRESHOLD, attrs)
    metrics.write.mode("overwrite").parquet(str(results_dir / "tag_metrics.parquet"))
    log(f"tag_metrics rows: {metrics.count()}")

    if not args.skip_sweep:
        # Each threshold is evaluated and released before the next one starts.
        # Collecting the frames lazily and unioning at the end instead keeps
        # every threshold's full iterative lineage - LPA, Connected
        # Components, Triangle Count and a 20-iteration PageRank - alive in
        # the driver at once, on top of a `filtered` cache per run that is
        # never freed. That accumulation, not data volume, exhausts the heap:
        # threshold 5 completes as the first run of a session and then dies on
        # identical input as the third, inside AQE's plan-to-string listener.
        parts = []
        for threshold in SWEEP_THRESHOLDS:
            log(f"sweep threshold: weight >= {threshold}")
            df = run_projection_algorithms(edges, threshold, attrs)
            long = df.select(
                F.lit(threshold).alias("threshold"),
                "tag",
                F.col("tag_authority").cast("double").alias("tag_authority"),
                F.col("triangle_count").cast("long").alias("triangle_count"),
                F.col("community").cast("long").alias("community"),
            )
            part = results_dir / f"_sweep_t{threshold}.parquet"
            long.write.mode("overwrite").parquet(str(part))
            parts.append(part)
            # Drops the per-run `filtered` cache and the `edges` cache with
            # it; edges is re-read from Parquet next iteration, which is far
            # cheaper than carrying the lineage forward.
            spark.catalog.clearCache()
            log(f"  threshold {threshold} materialized")

        sweep = spark.read.parquet(str(parts[0]))
        for extra in parts[1:]:
            sweep = sweep.unionByName(spark.read.parquet(str(extra)))
        sweep.write.mode("overwrite").parquet(
            str(results_dir / "tag_metrics_sweep.parquet"))
        log(f"sweep written: {spark.read.parquet(str(results_dir / 'tag_metrics_sweep.parquet')).count()} rows")
        for part in parts:
            shutil.rmtree(part, ignore_errors=True)

    if args.bipartite:
        # Release the projection runs' caches and lineage first - this is the
        # one remaining place a second algorithm suite starts inside a live
        # session, which is what exhausted the heap during the sweep.
        spark.catalog.clearCache()
        log("bipartite connected components (7.78M edges, this is the slow one)...")
        comps = run_bipartite_components(spark, out / "edges_question_tag.parquet")
        comps.write.mode("overwrite").parquet(
            str(results_dir / "bipartite_components.parquet"))
        written = spark.read.parquet(str(results_dir / "bipartite_components.parquet"))
        log(f"components: {written.count()}")
        log(f"largest component size: {written.agg(F.max('size')).collect()[0][0]}")

    spark.stop()


if __name__ == "__main__":
    main()
