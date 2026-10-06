"""NetworkX counterparts of the GraphFrames algorithms, for the benchmark.

Single-threaded and in-memory by design: this is the baseline objective 6
measures Spark against.
"""
import networkx as nx
import pandas as pd

PAGERANK_ALPHA = 0.85  # = 1 - GraphFrames' resetProbability of 0.15


def build_bipartite(edges: pd.DataFrame) -> nx.Graph:
    """Question-tag graph.

    Question ids stay ints and tags stay strs, so the node types cannot
    collide without Spark's q:/t: prefixes - which would cost ~2.5M extra
    string objects here.
    """
    graph = nx.Graph()
    graph.add_edges_from(zip(edges["question_id"].tolist(), edges["tag"].tolist()))
    return graph


def build_projection(edges: pd.DataFrame) -> nx.Graph:
    """Tag-tag graph. Weight is dropped: the GraphFrames runs ignore it too
    (spec §6.3), and both engines must run the same algorithm."""
    graph = nx.Graph()
    graph.add_edges_from(zip(edges["src"].tolist(), edges["dst"].tolist()))
    return graph


def component_count(graph: nx.Graph) -> int:
    return nx.number_connected_components(graph)


def pagerank_scores(graph: nx.Graph) -> pd.DataFrame:
    """Runs to convergence, unlike GraphFrames' fixed 20 iterations, so the
    two are compared by rank correlation, not by value."""
    scores = nx.pagerank(graph, alpha=PAGERANK_ALPHA)
    return pd.DataFrame({"tag": list(scores), "pagerank": list(scores.values())})


def community_count(graph: nx.Graph) -> int:
    """Semi-synchronous LPA to convergence. GraphFrames runs synchronous LPA
    for a fixed 10 iterations, so the counts are reported, not expected to
    match."""
    return sum(1 for _ in nx.community.label_propagation_communities(graph))


def triangle_total(graph: nx.Graph) -> int:
    """Each triangle is counted once at each of its three corners."""
    return sum(nx.triangles(graph).values()) // 3
