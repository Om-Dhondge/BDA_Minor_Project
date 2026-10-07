"""Stage 7: write the weight >= 5 tag graph as GEXF for Gephi (spec §13).

Reads stage 2-3 artifacts with pandas - no Spark session. Laying the graph
out is a manual step in Gephi (plan Task 15, Part C).
"""
import argparse
import sys
from pathlib import Path

import networkx as nx
import pandas as pd

from lib.bench import read_parquet_frame
from lib.gephi import MIN_WEIGHT, build_tag_graph, louvain_clusters, missing_attributes

EXPECTED_NODES = 25_597
EXPECTED_EDGES = 215_968


def log(msg):
    print(f"[stage7] {msg}", flush=True)


def main():
    parser = argparse.ArgumentParser(description="Stage 7: GEXF for Gephi")
    parser.add_argument("--out-dir", default="out")
    args = parser.parse_args()

    out = Path(args.out_dir)
    metrics_path = out / "results" / "tag_metrics.parquet"
    edges_path = out / "edges_tag_tag.parquet"
    for path in (metrics_path, edges_path):
        if not path.exists():
            print(f"[stage7] ERROR: {path} not found. Run stages 1-3 first.",
                  file=sys.stderr)
            sys.exit(1)

    metrics = read_parquet_frame(metrics_path)
    graph = build_tag_graph(metrics, read_parquet_frame(edges_path), MIN_WEIGHT)
    clusters = louvain_clusters(graph)
    nx.set_node_attributes(graph, clusters, "louvain_cluster")

    nodes, edges = graph.number_of_nodes(), graph.number_of_edges()
    missing = missing_attributes(graph)
    log(f"nodes: {nodes:,} (expected {EXPECTED_NODES:,})")
    log(f"edges: {edges:,} (expected {EXPECTED_EDGES:,})")
    log(f"attributes missing on some node: {missing or 'none'}")
    if (nodes, edges) != (EXPECTED_NODES, EXPECTED_EDGES) or missing:
        print("[stage7] ERROR: graph check failed; nothing written.", file=sys.stderr)
        sys.exit(1)

    gephi_dir = out / "gephi"
    gephi_dir.mkdir(parents=True, exist_ok=True)
    nx.write_gexf(graph, gephi_dir / "tag_graph.gexf")
    # Parquet, not CSV: Stack Overflow has a real tag named "nan".
    pd.DataFrame({"tag": list(clusters), "louvain_cluster": list(clusters.values())}) \
        .to_parquet(gephi_dir / "louvain_clusters.parquet", index=False)

    # Louvain's result depends on node order, so the count is logged, not asserted.
    sizes = pd.Series(clusters).value_counts().sort_index()
    log(f"louvain clusters: {len(sizes)} (largest {sizes.iloc[0]:,} tags)")
    counts = metrics.set_index("tag")["question_count"]
    for cluster in sizes.index[:8]:
        members = [tag for tag, c in clusters.items() if c == cluster]
        log(f"  cluster {cluster} ({len(members):,} tags): "
            + ", ".join(counts[members].nlargest(5).index))
    log(f"written to {gephi_dir.resolve()}")


if __name__ == "__main__":
    main()
