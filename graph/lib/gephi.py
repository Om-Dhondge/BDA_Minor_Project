"""Stage 7: the tag graph for Gephi, and Gephi's layout back for the dashboard.

pandas and NetworkX, not Spark: the graph is 25,597 tags, and Gephi is a
desktop application that reads a GEXF file (spec §13).
"""
import xml.etree.ElementTree as ET

import networkx as nx
import pandas as pd

# Equals PRIMARY_THRESHOLD in lib/algorithms.py, which imports Spark; the
# dashboard imports this module and must not. A test pins the two together.
MIN_WEIGHT = 5
LOUVAIN_SEED = 42

# GEXF attribute name -> tag_metrics column.
NODE_ATTRIBUTES = {
    "question_count": "question_count",
    "tag_authority": "tag_authority",
    "lpa_community": "community",
    "component": "component",
    "triangle_count": "triangle_count",
}
REQUIRED_ATTRIBUTES = (*NODE_ATTRIBUTES, "louvain_cluster")


def build_tag_graph(metrics: pd.DataFrame, edges: pd.DataFrame,
                    min_weight: int) -> nx.Graph:
    """Every tag in metrics, with the edges at or above min_weight between them.

    Values go in as Python int and float via tolist(). NetworkX writes those
    as GEXF `long` and `double`; NumPy int64 would come out as type "int",
    which GEXF does not define, and float64 as 32-bit "float". LPA labels
    reach ~4e11, beyond a 32-bit integer.
    """
    tags = metrics["tag"].tolist()
    columns = {name: metrics[column].tolist()
               for name, column in NODE_ATTRIBUTES.items()}
    graph = nx.Graph()
    graph.add_nodes_from(
        (tag, {name: values[i] for name, values in columns.items()})
        for i, tag in enumerate(tags)
    )
    known = set(tags)
    kept = edges[(edges["weight"] >= min_weight)
                 & edges["src"].isin(known) & edges["dst"].isin(known)]
    graph.add_edges_from(
        (src, dst, {"weight": weight}) for src, dst, weight in
        zip(kept["src"].tolist(), kept["dst"].tolist(), kept["weight"].tolist())
    )
    return graph


def louvain_clusters(graph: nx.Graph, seed: int = LOUVAIN_SEED) -> dict:
    """Weighted Louvain, renumbered by size so the largest cluster is 0.

    A clustering for colouring the picture only. LPA stays the pipeline's
    community result; it leaves 99% of these tags in one community, which
    would draw the graph in a single colour.
    """
    parts = nx.community.louvain_communities(graph, weight="weight", seed=seed)
    ordered = sorted(parts, key=lambda part: (-len(part), min(part)))
    return {tag: cluster for cluster, part in enumerate(ordered) for tag in part}


def missing_attributes(graph: nx.Graph) -> list:
    """Required attributes that are absent or null on at least one node."""
    missing = set()
    for _, data in graph.nodes(data=True):
        for name in REQUIRED_ATTRIBUTES:
            value = data.get(name)
            if value is None or value != value:  # value != value: NaN
                missing.add(name)
    return sorted(missing)


def read_gephi_layout(path) -> pd.DataFrame:
    """Each node's id, position, colour and size from a GEXF that Gephi saved.

    Matches elements by local name, so GEXF 1.2draft and 1.3, whose
    namespaces differ, read alike. Edges are skipped: the dashboard takes
    them from the pipeline's own artifacts.
    """
    rows = []
    for _, elem in ET.iterparse(path, events=("end",)):
        name = elem.tag.rsplit("}", 1)[-1]
        if name == "node":
            row = {"tag": elem.get("id"), "x": None, "y": None,
                   "size": None, "color": None}
            for child in elem:
                part = child.tag.rsplit("}", 1)[-1]
                if part == "position":
                    row["x"], row["y"] = float(child.get("x")), float(child.get("y"))
                elif part == "size":
                    row["size"] = float(child.get("value"))
                elif part == "color":
                    row["color"] = child.get("hex") or "#{:02x}{:02x}{:02x}".format(
                        int(child.get("r")), int(child.get("g")), int(child.get("b")))
            rows.append(row)
            elem.clear()
        elif name == "edge":
            elem.clear()
    return pd.DataFrame(rows, columns=["tag", "x", "y", "size", "color"])


def network_view(layout: pd.DataFrame, metrics: pd.DataFrame,
                 edges: pd.DataFrame, top_n: int, per_tag: int,
                 min_weight: int = MIN_WEIGHT):
    """The tags and edges the Network tab draws.

    Tags: the top_n by question_count among those Gephi placed and stage 3
    knows; a stale layout's unknown ids drop out. Edges: each shown tag's
    per_tag strongest edges to other shown tags. An edge is kept if it is in
    either endpoint's top per_tag, so popular tags cannot crowd out the
    rest: the 300 most-asked tags alone share 17,955 edges.
    """
    placed = layout.dropna(subset=["x", "y"]).merge(
        metrics[["tag", "question_count", "tag_authority"]], on="tag")
    nodes = (placed.sort_values(["question_count", "tag"], ascending=[False, True])
             .head(top_n).reset_index(drop=True))
    shown = set(nodes["tag"])
    among = edges[(edges["weight"] >= min_weight)
                  & edges["src"].isin(shown)
                  & edges["dst"].isin(shown)].reset_index(drop=True)
    ends = pd.concat([
        pd.DataFrame({"end": among["src"], "edge": among.index, "weight": among["weight"]}),
        pd.DataFrame({"end": among["dst"], "edge": among.index, "weight": among["weight"]}),
    ])
    best = (ends.sort_values(["end", "weight", "edge"], ascending=[True, False, True])
            .groupby("end").head(per_tag))
    kept = among.loc[sorted(set(best["edge"]))].reset_index(drop=True)
    return nodes, kept


def edge_segments(nodes: pd.DataFrame, edges: pd.DataFrame):
    """x and y lists for one Plotly line trace: each edge, then a None break."""
    position = nodes.set_index("tag")[["x", "y"]]
    xs, ys = [], []
    for src, dst in zip(edges["src"], edges["dst"]):
        xs += [position.at[src, "x"], position.at[dst, "x"], None]
        ys += [position.at[src, "y"], position.at[dst, "y"], None]
    return xs, ys
