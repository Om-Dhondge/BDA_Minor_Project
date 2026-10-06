# Stack Overflow Knowledge Graph Analytics
### Using Distributed Graph Processing

**Minor Specialization:** Big Data Analytics
**Guide:** Dr. Mayur Pandya
**Team:** Om Dhondge (230962282) · Ayush Kumar (230962170)

**Progress Review — 27 August 2026**

---

## Agenda

1. Where the project stands in one slide
2. What we built: preprocessing → graph pipeline
3. Data at scale — verified counts
4. Results: communities, authority, structure
5. What we found that we did not expect
6. Blockers and scope decisions (with reasons)
7. Remaining work and what we need from you

---

## Status in One Slide

| Synopsis objective | Status |
|---|---|
| **1.** ETL pipeline: XML → cleaned Parquet | ✅ Done |
| **2.** Multi-relational graph | ⚠️ **Partial** — Question–Tag built; User edges blocked |
| **3.** GraphFrames analytics (PageRank, LPA, CC, Triangles) | ✅ Done — all four, on tag graph |
| **4.** Neo4j interactive querying | ⛔ Descoped (rationale on slide 12) |
| **5.** Gephi + Streamlit visualization | ⚠️ Streamlit done; Gephi export pending |
| **6.** Spark vs. NetworkX benchmark | ⛔ Deferred |

**Headline:** the full distributed pipeline runs end to end on **7.0 M records**, producing a **2.58 M-node graph** and a working dashboard. One data limitation removed the user-centric half of the graph.

---

## What Is Actually Built

Two complete pipelines, both runnable and tested.

**A. Preprocessing (`dataset/`)** — 4-stage parallel XML pipeline
- Byte-range chunker over a **104 GB** `Posts.xml` (never a DOM parse)
- Two parallel scans: questions → in-memory index, then answers → join
- HTML split into prose / code snippets / image URLs
- Output: **`preprocessed_posts.parquet`**, 7.0 M rows · 36 tests passing

**B. Graph analytics (`graph/`)** — 4 Spark stages on PySpark 3.5.3 + GraphFrames 0.8.4
- `stage0_smoke.py` — environment + algorithm-semantics proof
- `stage1_etl.py` — node and edge extraction
- `stage2_projection.py` — tag co-occurrence projection
- `stage3_algorithms.py` — LPA, Connected Components, Triangle Count, PageRank
- `dashboard/app.py` — Streamlit, 4 tabs · **25 tests passing**

Every stage is independently re-runnable and idempotent (`mode("overwrite")`).

---

## Architecture

```
Stack Overflow Posts.xml  (104 GB, seekable)
        │
        ▼  chunker → phase1 (questions) → phase2 (answers + HTML split)
preprocessed_posts.parquet          6,997,182 rows
        │
        ▼  Stage 1 — ETL
nodes_question · nodes_tag · edges_question_tag      (bipartite graph)
        │
        ▼  Stage 2 — one-mode projection (self-join on question, src < dst)
edges_tag_tag                        1,469,065 weighted co-occurrence edges
        │
        ▼  Stage 3 — GraphFrames: LPA · CC · TriangleCount · PageRank
tag_metrics · tag_metrics_sweep · bipartite_components
        │
        ▼  Stage 4
Streamlit dashboard (localhost:8501)
```

Runtime: full pipeline ≈ **11 min** (Stage 1: 4 min, Stage 2: 2 min, Stage 3: 5 min), on a **15.7 GB / 12-core** laptop.

---

## The Data — Verified, Not Estimated

| Quantity | Value |
|---|---|
| Answer records processed | **6,997,182** |
| Distinct questions | 2,532,514 |
| Distinct tags | 49,372 |
| Question–tag edges | 7,775,852 |
| Tag–tag co-occurrence edges | 1,469,065 |
| **Bipartite graph vertices** | **2,581,886** = 2,532,514 q + 49,372 t (exact) |

We chose a **weight ≥ 5** threshold on co-occurrence (an edge must appear on ≥ 5 shared questions) — this keeps **25,597 tags** and cuts incidental noise.

> **Scale check vs. the synopsis:** the plan proposed a 5–10 GB subset over 50 tags. We processed the **full dump across all 49,372 tags**. Actual scale exceeded the proposal.

---

## Result 1 — Structure of the Tag Graph

**Connected Components (bipartite, 2.58 M vertices):**
- 119 components; the giant component holds **2,581,620 vertices — 99.99%**
- The remaining 106 are size-2 isolates (a lone tag on a lone question)

**→ Interpretation:** Stack Overflow's technology space is *one* knowledge network, not a set of islands. There is no meaningfully disconnected niche.

**Label Propagation (tag graph, w ≥ 5):** 111 communities over 25,597 tags — but one community absorbs **25,348 of them (99%)**.

**→ Interpretation:** at this density, LPA gives a *near-trivial* partition. That is a genuine finding about the graph, not a failure of the algorithm — and it motivated the threshold sweep on the next slide.

---

## Result 2 — Threshold Sweep

We re-ran the full algorithm suite at three co-occurrence thresholds:

| Threshold | Tags retained | Communities | Largest community |
|---|---|---|---|
| w ≥ 1 | 49,260 | 24 | 49,189 (99.9%) |
| **w ≥ 5** | **25,597** | **111** | 25,348 (99.0%) |
| w ≥ 20 | 11,768 | 116 | 10,874 (92.4%) |

Two things to note:
- w ≥ 1 keeps 49,260 of 49,372 tags. The **112 missing** tags only ever appeared *alone* on a question, so they have no co-occurrence edge at all — an exact, explainable reconciliation.
- Community structure only starts to resolve as the threshold rises. Even at w ≥ 20 the core stays fused, which argues for **Louvain/modularity-based detection** rather than LPA as a next step.

---

## Result 3 — Tag Authority (PageRank)

Top technologies by structural centrality, weight ≥ 5:

| Rank | Tag | Authority | Triangles | Questions |
|---|---|---|---|---|
| 1 | java | 358.4 | 111,337 | 213,850 |
| 2 | python | 340.6 | 104,232 | 229,355 |
| 3 | c# | 323.8 | 103,478 | 215,512 |
| 4 | javascript | 295.0 | 101,897 | 222,067 |
| 5 | android | 264.4 | 78,080 | 136,833 |
| 6 | c++ | 215.0 | 74,857 | 132,053 |
| 7 | php | 177.3 | 75,308 | 107,364 |
| 8 | ios | 155.8 | 53,706 | 76,911 |

**Terminology note:** we label this **"tag authority" / technology centrality — never "expert ranking."** PageRank here runs over *technologies*, not users. Calling it expertise would overstate what the graph contains (see slide 12).

---

## Result 4 — Connectors: Punching Above Their Volume

The interesting result is not the top of the list — it is where **authority rank ≫ volume rank**. These are low-traffic tags that sit on structural bridges:

| Tag | Questions | Authority rank | Volume rank | Gain |
|---|---|---|---|---|
| openstack | 101 | 2,232 | 5,901 | **+3,669** |
| theorem-proving | 102 | 2,355 | 5,859 | +3,504 |
| microsoft-dynamics | 105 | 2,713 | 5,748 | +3,035 |
| bittorrent | 138 | 2,145 | 4,824 | +2,679 |
| olap | 128 | 2,578 | 5,057 | +2,479 |
| biztalk | 172 | 1,660 | 4,138 | +2,478 |

This directly delivers the synopsis's thesis — that **structural importance diverges from raw volume** — at the technology level rather than the user level.

*Methodological caveat we had to add:* the table needs a `question_count ≥ 100` floor (5,956 tags survive). Without it, thousands of tags tie at the PageRank floor, share a rank, and produce meaningless deltas. **Never rank on authority alone without a volume filter.**

---

## The Streamlit Dashboard

Four tabs, running on the Stage 3 outputs:

1. **Tag authority** — ranked table + the connector view above (volume floor applied)
2. **Communities** — LPA community sizes and membership
3. **Co-occurrence heatmap** — top-25 × 25, verified symmetric with zero diagonal
4. **Network view** — filtered edge table

**Honest caveat:** tab 4 is a *table*, not a node-link diagram. Network drawing is routed to Gephi via a GEXF export that is still pending — no layout library (`networkx`, `igraph`, `scipy`) is installed. We would rather state this than call a table a network visualization.

---

## Blocker — No User Identifier

**The single biggest deviation from the synopsis.**

The processed Parquet schema has **no `OwnerUserId`** and **no `CreationDate`**. The preprocessing code was originally written for a different pipeline (a Q&A retrieval dataset) and did not carry those fields through.

Consequently these synopsis items are **unbuildable from the current artifact**:
- User–answers–Question edges (Objective 2)
- User–votes–Post edges (Objective 2)
- PageRank over *users* → the expert leaderboard (Objectives 3, 5)
- PageRank vs. reputation comparison (Objective 3)
- All temporal / trend analysis (Objective 5)

**Also known:** the committed preprocessing code can no longer reproduce that exact Parquet — the version that generated it was not saved.

**Options we see:** (a) re-run preprocessing with the user columns added — requires re-decompressing 104 GB to C:; (b) extract the already-downloaded `Users.7z` / `Votes.7z` and join on a re-derived key; (c) formally re-scope Objectives 2/3 to the tag graph. **We would like your call on this.**

---

## Deliberate Scope Decisions

| Item | Decision | Reason |
|---|---|---|
| **Neo4j (Obj. 4)** | Descoped | Adds a database layer without adding analytical findings; Parquet + Spark SQL already answers the same queries. Install cost not justified. |
| **Spark vs. NetworkX benchmark (Obj. 6)** | Deferred | Meaningful only once the user graph exists; the tag graph is small enough that NetworkX would not lose convincingly. |
| **Gephi export** | Pending, not dropped | The GEXF writer is a small, well-defined task — planned next. |

Both descopes are recorded with rationale in the project design spec, not silently dropped.

---

## Engineering Lessons Worth Reporting

Three problems that cost real time and are worth documenting in the report:

**1. PySpark 3.5.3 is incompatible with Python 3.12.** Its bundled cloudpickle predates 3.12; every executor dies with *no traceback*. Fixed by pinning a Python 3.11 venv. This was the most expensive trap in the project.

**2. Successive algorithm suites exhaust the driver heap — it is lineage accumulation, not data volume.** The original sweep held every threshold's iterative lineage (LPA×10 + CC + Triangles + PageRank×20) alive at once and died serialising a *query plan to a string*. Proof it was not volume: weight ≥ 5 finished in 4m48s as the session's first run, then OOM'd on identical input as its third. Fixed by materialising each threshold to staging Parquet and clearing the cache between runs — not by raising driver memory.

**3. Algorithm semantics we had to verify, not assume:**
- Triangle Count on a bipartite graph is **identically zero** — no odd cycles. A definition, not a bug.
- PageRank needs **symmetrized** edges; our store keeps each edge once as `src < dst`.
- GraphFrames' PageRank and LPA **ignore edge weights** — weight enters only via the pre-filter threshold. Confirmed empirically: flattening all weights to 1 gives bit-identical scores.
- **LPA is not Connected Components** and must never be asserted to return one label per component.

---

## Remaining Work

| # | Task | Effort |
|---|---|---|
| 1 | **Resolve the user-identifier blocker** — pending your decision | Large / TBD |
| 2 | GEXF export → Gephi force-directed layout, sized by authority, colored by community | Small |
| 3 | Swap LPA for **Louvain / modularity** to get a non-trivial partition | Medium |
| 4 | Git hygiene: `.gitignore`, commit the pipeline (currently untracked) | Small |
| 5 | Final report + reproducibility appendix | Medium |

---

## What We Need From You

1. **Objectives 2 & 3 — user graph.** Re-run preprocessing to recover `OwnerUserId`, or formally re-scope to the tag graph? Re-running costs a 104 GB decompression and a full re-parse.
2. **Objective 4 — Neo4j.** Is descoping acceptable given it adds no new findings, or is the graph-database component required for the deliverable?
3. **Community detection.** Is the LPA-gives-one-giant-community result acceptable as a *finding*, or should we implement Louvain to produce a more readable partition?
4. **Report emphasis.** Lead with the scale/engineering achievement, or with the analytical findings (connector tags, single-component structure)?

---

## Summary

- Two full pipelines built, tested (**61 tests passing**), and reproducible end to end
- **7.0 M records → 2.58 M-node graph → 1.47 M weighted edges**, all four graph algorithms run at full dump scale — beyond the 50-tag subset the synopsis proposed
- Real analytical findings: a **single 99.99% component**, a fused community core resistant to thresholding, and **quantified connector technologies**
- One data limitation blocks the user-centric objectives; scope decisions are documented with reasons rather than quietly dropped

**Thank you — questions welcome.**