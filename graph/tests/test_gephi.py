import networkx as nx
import pandas as pd
import pytest

from lib.gephi import (
    MIN_WEIGHT,
    build_tag_graph,
    edge_segments,
    louvain_clusters,
    missing_attributes,
    network_view,
    read_gephi_layout,
)


@pytest.fixture
def metrics():
    """Six tags. The python/django/flask labels exceed 2**31, like real LPA
    labels; c# has no edge at weight >= 5."""
    return pd.DataFrame({
        "tag": ["python", "django", "flask", "java", "spring", "c#"],
        "tag_authority": [9.5, 3.1, 2.2, 8.0, 2.9, 7.7],
        "community": [404_000_000_000] * 3 + [7] * 3,
        "component": [0] * 6,
        "triangle_count": [1, 1, 1, 0, 0, 0],
        "question_count": [1000, 300, 200, 900, 250, 800],
        "mean_question_score": [1.0] * 6,
    })


@pytest.fixture
def edges():
    """Stored once per pair, src < dst, as stage 2 writes them."""
    return pd.DataFrame({
        "src": ["django", "django", "flask", "java", "java", "c#", "c#", "rails"],
        "dst": ["python", "flask", "python", "spring", "python", "java", "python", "ruby"],
        "weight": [50, 9, 40, 60, 6, 3, 4, 99],
    })


def test_graph_keeps_every_tag_and_only_edges_at_threshold(metrics, edges):
    graph = build_tag_graph(metrics, edges, MIN_WEIGHT)
    assert set(graph.nodes) == set(metrics["tag"])
    assert graph.number_of_edges() == 5
    assert all(w >= MIN_WEIGHT for _, _, w in graph.edges(data="weight"))


def test_graph_drops_edges_to_tags_without_metrics(metrics, edges):
    graph = build_tag_graph(metrics, edges, MIN_WEIGHT)
    assert "ruby" not in graph and "rails" not in graph


def test_graph_attributes_are_python_numbers(metrics, edges):
    node = build_tag_graph(metrics, edges, MIN_WEIGHT).nodes["python"]
    assert type(node["lpa_community"]) is int
    assert type(node["tag_authority"]) is float
    assert node["lpa_community"] == 404_000_000_000


def test_written_gexf_declares_long_and_double(metrics, edges, tmp_path):
    # NetworkX writes a NumPy int64 as type "int", which GEXF does not
    # define, and a float64 as 32-bit "float".
    path = tmp_path / "tag_graph.gexf"
    nx.write_gexf(build_tag_graph(metrics, edges, MIN_WEIGHT), path)
    text = path.read_text(encoding="utf-8")
    assert 'title="lpa_community" type="long"' in text
    assert 'title="tag_authority" type="double"' in text
    assert "404000000000" in text


def test_louvain_is_deterministic_and_covers_every_tag(metrics, edges):
    graph = build_tag_graph(metrics, edges, MIN_WEIGHT)
    first, second = louvain_clusters(graph), louvain_clusters(graph)
    assert first == second
    assert set(first) == set(graph.nodes)


def test_louvain_clusters_are_numbered_by_size(metrics, edges):
    clusters = louvain_clusters(build_tag_graph(metrics, edges, MIN_WEIGHT))
    sizes = pd.Series(clusters).value_counts().sort_index()
    assert list(sizes.index) == list(range(len(sizes)))
    assert list(sizes) == sorted(sizes, reverse=True)


def test_missing_attributes_names_an_unset_louvain_cluster(metrics, edges):
    graph = build_tag_graph(metrics, edges, MIN_WEIGHT)
    assert missing_attributes(graph) == ["louvain_cluster"]
    nx.set_node_attributes(graph, louvain_clusters(graph), "louvain_cluster")
    assert missing_attributes(graph) == []


@pytest.mark.parametrize("version", ["1.2draft", "1.3"])
def test_layout_reader_reads_both_gexf_versions(tmp_path, version):
    # The two versions use different XML namespaces. Tag names include the
    # real Stack Overflow tag "nan" and characters XML must escape.
    graph = nx.Graph()
    graph.add_node("c#", viz={"position": {"x": 1.5, "y": -2.0, "z": 0.0},
                              "color": {"r": 255, "g": 0, "b": 128, "a": 1.0},
                              "size": 12.0})
    graph.add_node("nan", viz={"position": {"x": 0.0, "y": 3.0, "z": 0.0},
                               "color": {"r": 1, "g": 2, "b": 3, "a": 1.0},
                               "size": 4.0})
    graph.add_node("c++")
    graph.add_edge("c#", "nan")
    path = tmp_path / "layout.gexf"
    nx.write_gexf(graph, path, version=version)

    layout = read_gephi_layout(path).set_index("tag")

    assert list(layout.index) == ["c#", "nan", "c++"]
    assert (layout.loc["c#", "x"], layout.loc["c#", "y"]) == (1.5, -2.0)
    assert layout.loc["c#", "color"] == "#ff0080"
    assert layout.loc["nan", "size"] == 4.0
    assert pd.isna(layout.loc["c++", "x"])


def test_min_weight_matches_primary_threshold():
    # lib.gephi keeps its own copy so the dashboard never imports Spark.
    from lib.algorithms import PRIMARY_THRESHOLD
    assert MIN_WEIGHT == PRIMARY_THRESHOLD


def placed(tags, **overrides):
    """A layout frame with every tag placed on a line."""
    rows = [{"tag": t, "x": float(i), "y": float(-i), "size": 10.0, "color": "#112233"}
            for i, t in enumerate(tags)]
    for row in rows:
        row.update(overrides.get(row["tag"], {}))
    return pd.DataFrame(rows)


def test_view_keeps_the_top_tags_by_question_count(metrics, edges):
    nodes, _ = network_view(placed(metrics["tag"]), metrics, edges,
                            top_n=3, per_tag=3)
    assert list(nodes["tag"]) == ["python", "java", "c#"]


def test_view_keeps_each_tags_strongest_edges(metrics, edges):
    _, kept = network_view(placed(metrics["tag"]), metrics, edges,
                           top_n=6, per_tag=1)
    # java-python (6) is no endpoint's strongest; django-flask (9) neither.
    assert set(zip(kept["src"], kept["dst"])) == {
        ("django", "python"), ("flask", "python"), ("java", "spring")}


def test_view_never_draws_an_edge_to_a_hidden_tag(metrics, edges):
    nodes, kept = network_view(placed(metrics["tag"]), metrics, edges,
                               top_n=2, per_tag=3)
    shown = set(nodes["tag"])
    assert shown == {"python", "java"}
    assert set(kept["src"]) | set(kept["dst"]) <= shown
    assert len(kept) == 1


def test_view_skips_unplaced_and_unknown_tags(metrics, edges):
    # A stale layout: "ruby" is not a current tag, and Gephi never placed "java".
    layout = placed(["python", "java", "ruby"], java={"x": None, "y": None})
    nodes, _ = network_view(layout, metrics, edges, top_n=10, per_tag=3)
    assert list(nodes["tag"]) == ["python"]


def test_edge_segments_break_between_edges():
    nodes = placed(["a", "b", "c"])
    xs, ys = edge_segments(nodes, pd.DataFrame({"src": ["a", "b"], "dst": ["b", "c"]}))
    assert xs == [0.0, 1.0, None, 1.0, 2.0, None]
    assert ys == [0.0, -1.0, None, -1.0, -2.0, None]
