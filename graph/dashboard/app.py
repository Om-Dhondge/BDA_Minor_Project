"""Stage 4: Streamlit dashboard. Reads Parquet directly - no Spark session.

PageRank here is TAG AUTHORITY: the structural centrality of a technology.
It is not an expert ranking; that requires OwnerUserId, which the current
dataset does not carry.
"""
import sys
from pathlib import Path

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st

# `streamlit run` puts dashboard/ on sys.path, not graph/, so lib/ needs this.
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from lib.bench import read_parquet_frame  # noqa: E402
from lib.gephi import edge_segments, network_view, read_gephi_layout  # noqa: E402

OUT = Path(__file__).resolve().parent.parent / "out"
GEPHI = OUT / "gephi"

st.set_page_config(page_title="Stack Overflow Tag Graph", layout="wide")


@st.cache_data
def load_metrics():
    return pd.read_parquet(OUT / "results" / "tag_metrics.parquet")


@st.cache_data
def load_edges():
    return pd.read_parquet(OUT / "edges_tag_tag.parquet")


# Gephi's files are rewritten by hand. Each loader takes the file's mtime so a
# fresh export replaces the cached copy without restarting the app.
@st.cache_data
def load_gephi_layout(path, modified):
    return read_gephi_layout(path)


@st.cache_data
def load_louvain(path, modified):
    return read_parquet_frame(path)


st.title("Stack Overflow Technology Graph")
st.caption(
    "Tag authority is PageRank over the tag co-occurrence graph - a measure of "
    "structural centrality among technologies. It is not a ranking of users."
)

metrics = load_metrics()

tab1, tab2, tab3, tab4, tab5 = st.tabs(
    ["Tag authority", "Communities", "Co-occurrence", "Network", "Benchmark"]
)

with tab1:
    st.subheader("Authority vs. raw popularity")
    df = metrics.dropna(subset=["tag_authority", "question_count"]).copy()

    # Tags with only a handful of questions sit at the PageRank floor of 1.0,
    # and thousands of them tie there. A tie assigns every one the same
    # authority_rank, which beats their volume rank by ~20,000 places and
    # floods the table with 5-question tags that are not central to anything.
    # A volume floor removes the artifact and lets rank_delta mean what it is
    # supposed to mean.
    MIN_QUESTIONS = 100
    df = df[df["question_count"] >= MIN_QUESTIONS].copy()

    df["authority_rank"] = df["tag_authority"].rank(ascending=False).astype(int)
    df["volume_rank"] = df["question_count"].rank(ascending=False).astype(int)
    df["rank_delta"] = df["volume_rank"] - df["authority_rank"]

    st.markdown("**Punching above their volume** - structurally central "
                "relative to how many questions they carry, among the "
                f"{len(df):,} tags with at least {MIN_QUESTIONS} questions:")
    st.dataframe(
        df.nlargest(20, "rank_delta")[
            ["tag", "authority_rank", "volume_rank", "rank_delta",
             "tag_authority", "question_count"]
        ],
        use_container_width=True,
    )

    st.plotly_chart(
        px.scatter(
            df.nlargest(300, "question_count"),
            x="question_count", y="tag_authority", hover_name="tag",
            log_x=True, log_y=True,
            labels={"question_count": "Questions", "tag_authority": "Tag authority"},
            title="Authority against volume (top 300 tags)",
        ),
        use_container_width=True,
    )

with tab2:
    st.subheader("Technology communities (Label Propagation)")
    sizes = (metrics.groupby("community").size()
             .reset_index(name="size").sort_values("size", ascending=False))
    st.markdown(f"**{len(sizes)} communities** across {len(metrics)} tags.")

    choice = st.selectbox(
        "Community",
        sizes["community"].head(50),
        format_func=lambda c: (
            f"{int(c)} ({int(sizes.loc[sizes['community'] == c, 'size'].iloc[0])} tags)"
        ),
    )
    members = metrics[metrics["community"] == choice].nlargest(50, "tag_authority")
    st.dataframe(
        members[["tag", "tag_authority", "question_count", "triangle_count",
                 "mean_question_score", "mean_answers_per_question"]],
        use_container_width=True,
    )

with tab3:
    st.subheader("Tag co-occurrence")
    top_n = st.slider("Number of tags", 10, 50, 25)
    edges = load_edges()
    top_tags = set(metrics.nlargest(top_n, "question_count")["tag"])
    sub = edges[edges["src"].isin(top_tags) & edges["dst"].isin(top_tags)]

    # Stage 2 stores each undirected edge once as src < dst, so the raw pivot
    # fills only one triangle and omits any tag that never appears on a given
    # side. Reindex onto the full tag set and mirror: co-occurrence is
    # genuinely symmetric, the storage convention is not.
    axis = sorted(top_tags)
    matrix = (
        sub.pivot(index="src", columns="dst", values="weight")
        .reindex(index=axis, columns=axis)
        .fillna(0)
    )
    matrix = matrix + matrix.T
    st.plotly_chart(
        px.imshow(matrix, labels={"color": "Co-occurrence"},
                  title=f"Co-occurrence among the top {top_n} tags"),
        use_container_width=True,
    )

with tab4:
    st.subheader("Network view")

    # Names below stay distinct from tabs 1-3: tab 2's format_func reads the
    # module-level `sizes` after the script finishes (see the tab 5 note).
    gephi_image = GEPHI / "tag_graph.png"
    if gephi_image.exists():
        st.image(str(gephi_image), width="stretch", caption=(
            "Gephi ForceAtlas2 layout of the tag graph at weight >= 5 "
            "(25,597 tags). Colour: Louvain cluster. Size: tag authority."))
    else:
        st.info("No Gephi image yet. Run `stage7_gephi_export.py`, lay the graph "
                "out in Gephi and export `out/gephi/tag_graph.png` "
                "(plan Task 15, Part C).")

    layout_path = GEPHI / "tag_graph_layout.gexf"
    if not layout_path.exists():
        st.info("No Gephi layout yet. Export `out/gephi/tag_graph_layout.gexf` "
                "from Gephi with positions, colours and sizes "
                "(plan Task 15, Part C).")
    else:
        layout = load_gephi_layout(str(layout_path), layout_path.stat().st_mtime)
        gephi_n = st.slider("Tags in the layout view", 100, 5000, 1000,
                            step=100, key="gephi_n")
        gephi_k = st.slider("Strongest edges per tag", 1, 10, 3, key="gephi_k")
        view_nodes, view_edges = network_view(layout, metrics, load_edges(),
                                              gephi_n, gephi_k)
        matched = int(layout["tag"].isin(metrics["tag"]).sum())
        if view_nodes.empty:
            st.warning(f"None of the {len(layout):,} tags in the Gephi layout "
                       "match the current stage 3 output. Re-run stage 7 and "
                       "redo the Gephi step.")
        else:
            louvain_path = GEPHI / "louvain_clusters.parquet"
            if louvain_path.exists():
                view_nodes = view_nodes.merge(
                    load_louvain(str(louvain_path), louvain_path.stat().st_mtime),
                    on="tag", how="left")
            else:
                view_nodes["louvain_cluster"] = None
            hover = (
                view_nodes["tag"]
                + "<br>" + view_nodes["question_count"].map("{:,} questions".format)
                + "<br>tag authority " + view_nodes["tag_authority"].map("{:.1f}".format)
                + "<br>Louvain cluster " + view_nodes["louvain_cluster"].map(
                    lambda c: "?" if pd.isna(c) else str(int(c)))
            )
            xs, ys = edge_segments(view_nodes, view_edges)
            network = go.Figure([
                go.Scattergl(x=xs, y=ys, mode="lines", hoverinfo="skip",
                             line={"width": 0.5, "color": "rgba(140, 140, 140, 0.35)"}),
                go.Scattergl(x=view_nodes["x"], y=view_nodes["y"], mode="markers",
                             text=hover, hoverinfo="text",
                             marker={"color": view_nodes["color"].fillna("#888888"),
                                     "size": 2 * view_nodes["size"].fillna(4.0)
                                     .clip(lower=1.0) ** 0.5,
                                     "line": {"width": 0}}),
            ])
            network.update_layout(
                showlegend=False, height=700,
                margin={"l": 0, "r": 0, "t": 40, "b": 0},
                title=(f"{len(view_nodes):,} most-asked tags and {len(view_edges):,} "
                       "edges, at Gephi's positions"))
            network.update_xaxes(visible=False)
            network.update_yaxes(visible=False, scaleanchor="x")
            st.plotly_chart(network)
            if matched < len(layout):
                st.caption(f"{matched:,} of the {len(layout):,} tags in the layout "
                           "match the current stage 3 output.")

    st.divider()
    top_n = st.slider("Tags to plot", 20, 150, 60, key="net")
    min_w = st.slider("Minimum edge weight", 5, 500, 50)

    edges = load_edges()
    top_tags = set(metrics.nlargest(top_n, "tag_authority")["tag"])
    sub = edges[
        edges["src"].isin(top_tags)
        & edges["dst"].isin(top_tags)
        & (edges["weight"] >= min_w)
    ]
    st.markdown(f"{len(sub)} edges among {top_n} tags at weight >= {min_w}.")
    # Look the community up on both endpoints: src < dst is a storage
    # convention, so a src-only join leaves any tag that happens to sort last
    # without a community.
    communities = metrics[["tag", "community"]]
    table = (
        sub.nlargest(50, "weight")
        .merge(communities.rename(columns={"community": "src_community"}),
               left_on="src", right_on="tag", how="left")
        .merge(communities.rename(columns={"community": "dst_community"}),
               left_on="dst", right_on="tag", how="left")
    )
    st.dataframe(
        table[["src", "dst", "weight", "src_community", "dst_community"]],
        use_container_width=True,
    )

BENCH_STEPS = ["load", "connected_components", "pagerank",
               "label_propagation", "triangle_count"]

with tab5:
    st.subheader("Spark vs NetworkX")
    st.caption(
        "Both engines run the same graphs at each scale. Spark runs in local "
        "mode on one machine's 12 cores; NetworkX is single-threaded. The "
        "projection is benchmarked unthresholded (weight >= 1)."
    )
    bench_path = OUT / "results" / "benchmark.csv"
    if not bench_path.exists():
        st.info("No benchmark results yet. Run `stage6_benchmark.py` first.")
    else:
        bench = pd.read_csv(bench_path)
        graph = st.radio("Graph", ["projection", "bipartite"], horizontal=True)
        timed = bench[
            (bench["status"] == "ok")
            & (bench["graph"] == graph)
            & bench["step"].isin(BENCH_STEPS)
        ]
        medians = (timed.groupby(["engine", "scale_pct", "step"], as_index=False)
                   ["seconds"].median())
        if medians.empty:
            st.warning("No completed runs for this graph yet.")
        else:
            st.plotly_chart(px.line(
                medians, x="scale_pct", y="seconds", color="engine",
                facet_col="step", markers=True, log_y=True,
                category_orders={"step": [s for s in BENCH_STEPS
                                          if s in set(medians["step"])]},
                labels={"scale_pct": "% of questions", "seconds": "Seconds"},
                title=f"Time by data scale, {graph} graph (median over repeats)",
            ))

        # Not `sizes`: tab 2's format_func closes over that module-level name,
        # and Streamlit calls it again after the script finishes.
        scale_sizes = bench[
            (bench["engine"] == "spark") & (bench["graph"] == graph)
            & (bench["step"] == "load") & (bench["status"] == "ok")
        ].drop_duplicates("scale_pct").sort_values("scale_pct")
        st.markdown("**Graph size at each scale**")
        st.dataframe(scale_sizes[["scale_pct", "vertices", "edges"]],
                     hide_index=True)

        unfinished = bench[bench["status"] != "ok"]
        if len(unfinished):
            st.markdown("**Runs that did not finish.** A NetworkX timeout at "
                        "full scale is a result, not a failure.")
            st.dataframe(unfinished[["engine", "scale_pct", "graph", "step",
                                     "status", "error"]], hide_index=True)

        checks_path = OUT / "results" / "benchmark_checks.csv"
        if checks_path.exists():
            st.markdown("**Cross-engine checks**")
            st.dataframe(pd.read_csv(checks_path), hide_index=True)
