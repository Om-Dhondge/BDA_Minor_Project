"""Stage 0: validate JDK, Spark, and GraphFrames before any real work."""
import sys

from graphframes import GraphFrame

from lib.session import build_session


def build_toy_graph(spark):
    """Triangle a-b-c plus path d-e-f. Edges stored once, canonical src < dst."""
    vertices = spark.createDataFrame(
        [("a",), ("b",), ("c",), ("d",), ("e",), ("f",)], ["id"]
    )
    edges = spark.createDataFrame(
        [
            ("a", "b", 10),
            ("b", "c", 10),
            ("a", "c", 10),
            ("d", "e", 1),
            ("e", "f", 1),
        ],
        ["src", "dst", "weight"],
    )
    return GraphFrame(vertices, edges)


def symmetrize(edges):
    from pyspark.sql import functions as F

    return edges.select("src", "dst", "weight").union(
        edges.select(
            F.col("dst").alias("src"), F.col("src").alias("dst"), F.col("weight")
        )
    )


def main():
    spark = build_session("stage0-smoke", driver_memory="2g")
    print(f"[stage0] driver memory: {spark.conf.get('spark.driver.memory')}", flush=True)
    failures = []

    g = build_toy_graph(spark)

    # Connected Components: exactly 2 components, sizes 3 and 3.
    cc = g.connectedComponents()
    sizes = sorted(r["count"] for r in cc.groupBy("component").count().collect())
    if sizes != [3, 3]:
        failures.append(f"connectedComponents: expected sizes [3, 3], got {sizes}")

    # Triangle Count: a, b, c each in 1 triangle; d, e, f in 0.
    tri = {r["id"]: r["count"] for r in g.triangleCount().collect()}
    expected_tri = {"a": 1, "b": 1, "c": 1, "d": 0, "e": 0, "f": 0}
    if tri != expected_tri:
        failures.append(f"triangleCount: expected {expected_tri}, got {tri}")

    # Label Propagation. NOT one label per component - LPA is not Connected
    # Components. Under synchronous updates the path d-e-f settles into
    # {d, f} and {e}: d and f see only e, e sees both, and the labels flip
    # across the bipartition. That is stable from iteration 2 onward, so the
    # invariants worth asserting are the ones LPA actually guarantees.
    lpa = {r["id"]: r["label"] for r in g.labelPropagation(maxIter=5).collect()}
    if set(lpa) != {"a", "b", "c", "d", "e", "f"}:
        failures.append(f"labelPropagation: expected all 6 vertices, got {sorted(lpa)}")
    else:
        # A label can never cross a component boundary - no edges to carry it.
        triangle_labels = {lpa["a"], lpa["b"], lpa["c"]}
        path_labels = {lpa["d"], lpa["e"], lpa["f"]}
        if triangle_labels & path_labels:
            failures.append(
                "labelPropagation: a label spans both components, which is "
                f"impossible: {lpa}"
            )
        # The triangle is a clique, so it must collapse to a single label.
        if len(triangle_labels) != 1:
            failures.append(
                f"labelPropagation: triangle a/b/c should share one label, got {lpa}"
            )

    # PageRank needs symmetric edges. On the symmetrized graph a, b, c are
    # interchangeable and must score equally.
    g_sym = GraphFrame(g.vertices, symmetrize(g.edges))
    pr = {r["id"]: r["pagerank"] for r in g_sym.pageRank(
        resetProbability=0.15, maxIter=20).vertices.collect()}
    triangle_scores = [pr["a"], pr["b"], pr["c"]]
    if max(triangle_scores) - min(triangle_scores) > 1e-6:
        failures.append(f"pageRank: a/b/c should tie, got {triangle_scores}")

    # Confirm PageRank ignores edge weights (spec section 6.3). The triangle
    # edges have weight 10 and the path edges weight 1; re-running with all
    # weights set to 1 must produce identical scores.
    from pyspark.sql import functions as F
    flat = g.edges.withColumn("weight", F.lit(1))
    pr_flat = {r["id"]: r["pagerank"] for r in GraphFrame(
        g.vertices, symmetrize(flat)).pageRank(
        resetProbability=0.15, maxIter=20).vertices.collect()}
    if any(abs(pr[k] - pr_flat[k]) > 1e-9 for k in pr):
        failures.append(
            "pageRank appears to USE edge weights - spec section 6.3 assumption "
            "is wrong, revisit the thresholding decision"
        )

    spark.stop()

    if failures:
        print("STAGE 0 FAILED:")
        for f in failures:
            print(f"  - {f}")
        sys.exit(1)
    print("STAGE 0 PASSED: JDK, Spark, and all four GraphFrames algorithms work.")


if __name__ == "__main__":
    main()
