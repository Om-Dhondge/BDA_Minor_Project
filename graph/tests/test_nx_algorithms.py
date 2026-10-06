import pandas as pd
from graphframes import GraphFrame

from lib.algorithms import tag_vertices
from lib.nx_algorithms import (
    build_bipartite,
    build_projection,
    community_count,
    component_count,
    pagerank_scores,
    triangle_total,
)

# a-b-c is a triangle, c-d hangs off it, e-f is a separate component.
TOY_EDGES = [("a", "b"), ("a", "c"), ("b", "c"), ("c", "d"), ("e", "f")]


def toy_projection():
    return pd.DataFrame(TOY_EDGES, columns=["src", "dst"]).assign(weight=1)


def test_bipartite_keeps_question_ids_and_tags_distinct():
    # A tag literally named "1" must not merge with question 1.
    edges = pd.DataFrame({"question_id": [1, 1, 2], "tag": ["python", "1", "python"]})
    graph = build_bipartite(edges)
    assert graph.number_of_nodes() == 4
    assert graph.number_of_edges() == 3


def test_projection_counts_on_toy_graph():
    graph = build_projection(toy_projection())
    assert component_count(graph) == 2
    assert triangle_total(graph) == 1


def test_pagerank_covers_every_tag_and_sums_to_one():
    scores = pagerank_scores(build_projection(toy_projection()))
    assert set(scores["tag"]) == {"a", "b", "c", "d", "e", "f"}
    assert abs(scores["pagerank"].sum() - 1.0) < 1e-6


def test_lpa_never_merges_separate_components():
    assert community_count(build_projection(toy_projection())) >= 2


def test_engines_agree_on_toy_graph(spark):
    """Same graph, both engines: the exact checks the benchmark makes at scale."""
    edges_pd = toy_projection()
    edges = spark.createDataFrame(edges_pd)
    g = GraphFrame(tag_vertices(edges), edges)
    spark_components = g.connectedComponents().select("component").distinct().count()
    spark_triangles = g.triangleCount().agg({"count": "sum"}).collect()[0][0] // 3

    graph = build_projection(edges_pd)
    assert spark_components == component_count(graph) == 2
    assert spark_triangles == triangle_total(graph) == 1
