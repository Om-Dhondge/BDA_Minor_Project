import pytest

from lib.algorithms import (
    bipartite_graph_frames,
    join_tag_metrics,
    symmetrize,
    tag_vertices,
    threshold_edges,
)


@pytest.fixture
def tag_tag(spark):
    return spark.createDataFrame(
        [
            ("django", "python", 20),
            ("flask", "python", 7),
            ("numpy", "python", 4),
            ("obscure", "python", 1),
        ],
        ["src", "dst", "weight"],
    )


def test_threshold_keeps_edges_at_or_above_min(tag_tag):
    assert threshold_edges(tag_tag, 1).count() == 4
    assert threshold_edges(tag_tag, 5).count() == 2
    assert threshold_edges(tag_tag, 20).count() == 1


def test_symmetrize_doubles_edge_count(tag_tag):
    assert symmetrize(tag_tag).count() == 8


def test_symmetrize_adds_the_reverse_of_every_edge(tag_tag):
    pairs = {(r["src"], r["dst"]) for r in symmetrize(tag_tag).collect()}
    assert ("django", "python") in pairs
    assert ("python", "django") in pairs


def test_symmetrize_preserves_weight_on_reversed_edge(tag_tag):
    weights = {(r["src"], r["dst"]): r["weight"] for r in symmetrize(tag_tag).collect()}
    assert weights[("python", "django")] == 20


def test_symmetrize_introduces_no_self_loops(tag_tag):
    for row in symmetrize(tag_tag).collect():
        assert row["src"] != row["dst"]


def test_tag_vertices_are_unique_and_complete(tag_tag):
    ids = [r["id"] for r in tag_vertices(tag_tag).collect()]
    assert len(ids) == len(set(ids))
    assert set(ids) == {"django", "python", "flask", "numpy", "obscure"}


def test_bipartite_prefixes_distinguish_node_types(spark):
    edges = spark.createDataFrame([(1, "python"), (2, "java")],
                                  ["question_id", "tag"])
    vertices, bip_edges = bipartite_graph_frames(edges)
    ids = {r["id"] for r in vertices.collect()}
    assert ids == {"q:1", "q:2", "t:python", "t:java"}
    for row in bip_edges.collect():
        assert row["src"].startswith("q:")
        assert row["dst"].startswith("t:")


def test_bipartite_vertex_count_is_questions_plus_tags(spark):
    edges = spark.createDataFrame([(1, "python"), (1, "java"), (2, "python")],
                                  ["question_id", "tag"])
    vertices, _ = bipartite_graph_frames(edges)
    assert vertices.count() == 4   # q:1, q:2, t:python, t:java


@pytest.fixture
def metric_frames(spark):
    """Four per-tag algorithm outputs plus attributes, all covering the same tags."""
    pagerank = spark.createDataFrame(
        [("python", 2.5), ("django", 1.1)], ["tag", "tag_authority"])
    communities = spark.createDataFrame(
        [("python", 7), ("django", 7)], ["tag", "community"])
    components = spark.createDataFrame(
        [("python", 0), ("django", 0)], ["tag", "component"])
    triangles = spark.createDataFrame(
        [("python", 4), ("django", 2)], ["tag", "triangle_count"])
    attributes = spark.createDataFrame(
        [("python", 100), ("django", 30)], ["tag", "question_count"])
    return pagerank, communities, components, triangles, attributes


def test_join_produces_exactly_one_row_per_tag(metric_frames):
    result = join_tag_metrics(*metric_frames)
    assert result.count() == 2
    assert result.select("tag").distinct().count() == 2


def test_join_leaves_no_nulls_when_inputs_align(metric_frames):
    row = {k: v for k, v in
           join_tag_metrics(*metric_frames).filter("tag = 'python'")
           .collect()[0].asDict().items()}
    assert None not in row.values()
    assert row["tag_authority"] == 2.5
    assert row["triangle_count"] == 4
    assert row["question_count"] == 100


def test_join_keeps_tag_when_attributes_are_missing(spark, metric_frames):
    pagerank, communities, components, triangles, _ = metric_frames
    sparse_attrs = spark.createDataFrame([("python", 100)], ["tag", "question_count"])
    result = join_tag_metrics(pagerank, communities, components,
                              triangles, sparse_attrs)
    assert result.count() == 2
    django = result.filter("tag = 'django'").collect()[0]
    assert django["question_count"] is None
    assert django["tag_authority"] == 1.1
