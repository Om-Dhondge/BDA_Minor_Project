from pyspark.sql import DataFrame, functions as F

PRIMARY_THRESHOLD = 5
SWEEP_THRESHOLDS = (1, 5, 20)


def threshold_edges(edges: DataFrame, min_weight: int) -> DataFrame:
    """Drop low-weight co-occurrence.

    GraphFrames' pageRank and labelPropagation ignore edge weights, so
    weight-1 accidental co-occurrence would count equally with genuine
    coupling. Thresholding is how weight influences the result at all, and it
    gates all four projection algorithms - not PageRank alone.
    """
    return edges.filter(F.col("weight") >= min_weight)


def symmetrize(edges: DataFrame) -> DataFrame:
    """Add the reverse of every edge.

    Stage 2 stores each undirected edge once as src < dst. Connected
    Components and Label Propagation treat edges as undirected internally,
    but PageRank is genuinely directional and produces meaningless scores on
    a half-stored graph.
    """
    return edges.select("src", "dst", "weight").union(
        edges.select(
            F.col("dst").alias("src"),
            F.col("src").alias("dst"),
            F.col("weight"),
        )
    )


def tag_vertices(edges: DataFrame) -> DataFrame:
    """Distinct vertex ids implied by a tag-tag edge list."""
    return (
        edges.select(F.col("src").alias("id"))
        .union(edges.select(F.col("dst").alias("id")))
        .distinct()
    )


def bipartite_graph_frames(edges: DataFrame) -> tuple[DataFrame, DataFrame]:
    """Build GraphFrames vertices/edges for the question-tag bipartite graph.

    Prefixes keep the two node types distinct in one id space.
    """
    bip_edges = edges.select(
        F.concat(F.lit("q:"), F.col("question_id").cast("string")).alias("src"),
        F.concat(F.lit("t:"), F.col("tag")).alias("dst"),
    )
    vertices = (
        bip_edges.select(F.col("src").alias("id"))
        .union(bip_edges.select(F.col("dst").alias("id")))
        .distinct()
    )
    return vertices, bip_edges


def join_tag_metrics(pagerank, communities, components, triangles, attributes):
    """Fold the four per-tag algorithm outputs plus attributes into one row per tag.

    Outer joins between the algorithm outputs: a tag present in one must not be
    silently dropped. Left join for attributes, which are supplementary - a tag
    surviving the weight threshold keeps its metrics even if attributes are
    somehow absent.
    """
    return (
        pagerank
        .join(communities, on="tag", how="outer")
        .join(components, on="tag", how="outer")
        .join(triangles, on="tag", how="outer")
        .join(attributes, on="tag", how="left")
    )
