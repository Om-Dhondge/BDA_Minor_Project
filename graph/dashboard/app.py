"""Stage 4: Streamlit dashboard. Reads Parquet directly - no Spark session.

PageRank here is TAG AUTHORITY: the structural centrality of a technology.
It is not an expert ranking; that requires OwnerUserId, which the current
dataset does not carry.
"""
from pathlib import Path

import pandas as pd
import plotly.express as px
import streamlit as st

OUT = Path(__file__).resolve().parent.parent / "out"

st.set_page_config(page_title="Stack Overflow Tag Graph", layout="wide")


@st.cache_data
def load_metrics():
    return pd.read_parquet(OUT / "results" / "tag_metrics.parquet")


@st.cache_data
def load_edges():
    return pd.read_parquet(OUT / "edges_tag_tag.parquet")


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
