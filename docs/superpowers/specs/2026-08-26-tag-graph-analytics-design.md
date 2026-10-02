# Tag Graph Analytics Pipeline — Design

**Date:** 2026-08-26
**Amended:** 2026-10-03. Objectives 4 (Neo4j) and 6 (Spark vs NetworkX benchmark) are back in scope; see §11 and §12.
**Status:** Approved for planning
**Project:** Stack Overflow Knowledge Graph Analytics (BDA Minor Project)

## 1. Context

The synopsis proposes a multi-relational knowledge graph over Stack Overflow with three
edge types: User-answers-Question, Question-tagged-with-Tag, and User-votes-on-Post.

The preprocessed Parquet dataset that exists today was built for a different project (a
tri-modal RAG pipeline) and carries **no user identifier**. Two of the three edge types are
therefore unbuildable from it, and with them go PageRank-over-users, the expert leaderboard,
and the PageRank-vs-reputation comparison. `CreationDate` was also not retained, so temporal
trend charts are out.

This design covers the objectives that the existing Parquet *does* support, so that work can
proceed immediately without waiting on a re-extraction of `Posts.xml`. Recovering
`OwnerUserId` and `CreationDate` is tracked separately as future work; the staged structure
here is designed so that user-graph stages slot in beside these rather than replacing them.

### Objective coverage

| Synopsis objective | This design |
|---|---|
| 2 — Question-tagged-with-Tag edges | In scope |
| 2 — User-answers-Question edges | Out (no user column) |
| 2 — User-votes-on-Post edges | Out (Votes never extracted) |
| 3 — Label Propagation | In scope |
| 3 — Connected Components | In scope |
| 3 — Triangle Count | In scope (projection only, see §6.1) |
| 3 — PageRank | In scope, over **tags** not users (see §6.2) |
| 4 — Neo4j / Cypher | In scope, over tags and questions (see §11) |
| 5 — Streamlit dashboard | In scope |
| 5 — Gephi network layouts | Deferred (see §10) |
| 5 — Temporal trend charts | Out (no `CreationDate`) |
| 6 — Spark-vs-NetworkX benchmark | In scope, scaled by question sample, not raw GB (see §12) |

## 2. Input data

**Required input:** `dataset/Datasets_2nd run/preprocessed_posts.parquet` (6.93 GB).

The 2nd run must be used, not the 1st. The 2nd run captured *all* answers (6,997,182 rows,
~3.17 answers per question, minimum answer score −89), whereas the 1st run contains only
accepted answers — one per question — which would make the answer-count enrichment in §5
uniformly 1 and therefore meaningless.

Verified figures, measured directly from the 2nd-run artifact:

| Quantity | Value |
|---|---|
| Answer rows | 6,997,182 |
| Distinct questions | 2,532,514 |
| Distinct tags | 49,372 |
| Question–tag edges | 7,775,852 |
| Tag–tag co-occurrence edges (unique pairs) | 1,469,065 |
| …of those, with weight ≥ 5 | 215,968 |
| Bipartite graph vertices | 2,581,886 |

**Known artifact drift.** The 2nd-run Parquet contains an `answer_id` column that the current
`dataset/lib/phase2.py` `OUTPUT_SCHEMA` does not define, and it includes non-accepted answers
that `phase2.py:69` would now filter out. The committed preprocessing code cannot reproduce
this artifact. To avoid depending on that drift, stage 1 derives answer counts from **row
counts per `question_id`**, not from `answer_id`. The artifact must not be regenerated with
the current code without first restoring the missing behaviour.

Only `question_id`, `tags`, `score`, and `answer_score` are read. Parquet is columnar, so
this projection reads a few hundred MB rather than 6.93 GB.

## 3. Environment constraints

Measured on the target machine:

- 15.7 GB RAM, 12 logical cores
- No JDK, Spark, Neo4j, or Docker installed — the whole runtime is stood up by stage 0
- D: has 30 GB free; C: has 255 GB free

Spark scratch space (`spark.local.dir`) and checkpoint directories are therefore placed on
**C:**, under a configurable root defaulting to `C:\spark-tmp`. Shuffle spill onto D: could
exhaust the disk mid-job. Materialized artifacts are small and stay in `graph/out/` on D:.

## 4. Architecture

A new top-level `graph/` directory, mirroring the conventions already used in `dataset/`:
importable units in `lib/`, thin runnable stage scripts, a parallel `tests/`.

```
graph/
  lib/
    session.py       # SparkSession factory: graphframes package, checkpoint dir, local[*]
    etl.py           # stage 1 transforms
    projection.py    # stage 2 transforms
    algorithms.py    # stage 3 transforms
    neo4j_export.py  # stage 5: artifacts -> bulk-import CSVs (pandas)
    cypher.py        # stage 5: count checks and objective-4 queries
    bench.py         # stage 6: record format, step timing, cross-checks (no engine imports)
    nx_algorithms.py # stage 6: NetworkX baseline
  stage0_smoke.py    # environment validation
  stage1_etl.py
  stage2_projection.py
  stage3_algorithms.py
  stage5_neo4j_export.py
  stage5_neo4j_queries.py
  stage6_benchmark.py  # orchestrator; spawns the two workers below
  bench_spark.py       # one Spark part at one scale, own process
  bench_networkx.py    # one NetworkX graph at one scale, own process
  dashboard/app.py   # Streamlit, no Spark
  out/               # materialized artifacts
  tests/
```

Stages 5 and 6 are terminal: nothing downstream reads Neo4j or the benchmark records except
the dashboard's benchmark tab. Stages 0–4 stay runnable without either.

Each stage script is a thin `main()` over `lib/` functions — the same split that makes the
existing preprocessing code testable. Data flows strictly one way through `out/`, so any
stage can be re-run in isolation without re-running the ones before it.

GraphFrames' JAR is resolved through `spark.jars.packages` configured **inside** the
SparkSession builder rather than via `spark-submit` command-line flags, so every stage runs
as a plain `python stage1_etl.py`.

## 5. Stages

### Stage 0 — Environment

Installs JDK 17 (Temurin), creates a project virtualenv, installs `pyspark` 3.5.x and the
GraphFrames Python wrapper, and configures `HADOOP_HOME` with `winutils.exe` — the
Windows-specific component Spark's Hadoop IO layer requires.

The smoke test runs **all four algorithms** — PageRank, Label Propagation, Connected
Components, Triangle Count — against a 6-node toy graph with hand-verifiable expected
results. All four, not one: `connectedComponents` fails distinctly when the checkpoint
directory is unset, and `triangleCount` has its own edge-direction semantics. The smoke test
additionally confirms empirically that `pageRank` and `labelPropagation` ignore edge weights
(§6.3), so that behaviour is verified rather than assumed.

Stage 0 is the project's highest-risk step. GraphFrames is a third-party Scala package whose
Maven coordinate must match the Spark and Scala versions exactly, and the combination is
historically fragile on Windows. It is deliberately sequenced first and kept small so failure
surfaces in minutes rather than after the ETL is written.

### Stage 1 — ETL

Reads `question_id, tags, score, answer_score` from the input Parquet.

Groups by `question_id`, collapsing 6,997,182 answer rows to 2,532,514 question rows. That
grouping yields the node-attribute enrichment for free: `count(*)` per question gives
`answer_count`, `avg(answer_score)` gives `mean_answer_score`. `tags` and `score` are
invariant within a `question_id` group, so `first()` is safe for both.

Then explodes `tags` to produce the question–tag edge list.

Artifacts:

| Path | Rows | Contents |
|---|---|---|
| `out/edges_question_tag.parquet` | 7,775,852 | `question_id`, `tag` |
| `out/nodes_question.parquet` | 2,532,514 | `question_id`, `score`, `answer_count`, `mean_answer_score`, `tag_count` |
| `out/nodes_tag.parquet` | 49,372 | `tag`, `question_count`, `mean_question_score`, `mean_answers_per_question`, `mean_answer_score` |

### Stage 2 — Tag co-occurrence projection

Self-joins the exploded edge list on `question_id`, retaining pairs where `t1 < t2` for a
canonical ordering, then counts occurrences to produce weights.

This is cheap and free of skew for a non-obvious reason worth recording: Stack Overflow caps
questions at 5 tags, so every join key has at most 5 rows and emits at most 10 pairs. There
is no heavy-hitter key to degrade the join.

Artifact: `out/edges_tag_tag.parquet` — 1,469,065 rows of `src`, `dst`, `weight`.

All edges are materialized. The weight threshold is a **stage 3 parameter**, not a stage 2
filter, so thresholds can be swept without re-running the projection.

### Stage 3 — Algorithms

Two GraphFrames graphs share a vertex-ID scheme (`q:12345`, `t:python`) so the bipartite
graph can mix node types.

**Bipartite graph** (2,581,886 vertices / 7,775,852 edges): Connected Components. This is the
scale artifact, and it answers a genuine synopsis question — which technology niches are
self-contained versus part of the giant component.

**Tag projection** (49,372 vertices / up to 1,469,065 edges, weight-thresholded): Label
Propagation, Connected Components, Triangle Count, PageRank.

The weight threshold applies uniformly to **all four** projection algorithms, not to PageRank
alone — Label Propagation and Triangle Count are equally distorted by weight-1 accidental
co-occurrence. The **primary threshold is weight ≥ 5** (215,968 edges), and
`tag_metrics.parquet` is always the primary-threshold result.

The sensitivity sweep runs the same four algorithms at thresholds **{1, 5, 20}** and writes a
separate long-format artifact, leaving the primary result unambiguous.

Outputs:

| Path | Contents |
|---|---|
| `out/results/tag_metrics.parquet` | One row per tag: all four metrics at weight ≥ 5, plus stage 1 attributes. Dashboard's single source of truth. |
| `out/results/tag_metrics_sweep.parquet` | Long format: `threshold`, `tag`, `metric`, `value` across thresholds {1, 5, 20}. |
| `out/results/bipartite_components.parquet` | Component-size summary for the bipartite graph. |

### Stage 4 — Dashboard

Streamlit reading Parquet via pandas. **No Spark session at runtime**, so startup is roughly
a second. Four views:

1. **Tag authority leaderboard** — PageRank rank against raw question-count rank, surfacing
   tags that punch above their volume.
2. **Community explorer** — select an LPA community, inspect member tags and enrichment stats.
3. **Co-occurrence heatmap** — top-N tags by edge weight.
4. **Network view** — top-N subgraph, coloured by community, sized by PageRank.

## 6. Algorithm decisions

### 6.1 Triangle Count runs on the projection only

A bipartite graph contains no odd-length cycles, so its triangle count is identically zero.
This is a definition, not a defect, and a zero result there must not be mistaken for a bug.

The projection is the correct home for it regardless. There, a triangle is three technologies
that all pairwise co-occur — a real stack, such as `html`/`css`/`javascript` or
`docker`/`kubernetes`/`nginx`. Tags with high triangle counts sit inside dense, tightly
coupled ecosystems; tags with high degree but few triangles are connectors that touch many
communities without binding them together. This maps directly onto "collaboration density" in
objective 3.

### 6.2 PageRank is symmetrized at point of use, and ranks tags not users

Stage 2 stores each undirected edge once in canonical `src < dst` order. Connected Components
and Label Propagation treat edges as undirected internally, but PageRank is genuinely
directional and would produce meaningless results on a half-stored graph.

`algorithms.py` therefore symmetrizes explicitly when constructing the PageRank graph:

```python
undirected = edges.union(
    edges.select(col("dst").alias("src"), col("src").alias("dst"), col("weight"))
)
```

The rejected alternative — storing both directions in stage 2 — doubles the artifact and
hands Triangle Count a duplicate-edge hazard, since that is the algorithm most likely to
silently double-count. One canonical source of truth, with the asymmetry explicit in the code
that needs it, is safer.

**Interpretation.** PageRank here ranks *technologies*, not users. The synopsis frames
PageRank as an expert-ranking method, and that version requires `OwnerUserId`. All dashboard
labelling and reporting must say "tag authority" or "technology centrality". The underlying
thesis — that structural importance diverges from raw popularity — still holds, but must be
presented as a claim about technologies so that the user graph is not assumed to be done.

### 6.3 Edge weights are handled by thresholding

GraphFrames' `pageRank` wraps GraphX's unweighted implementation and ignores edge weights,
treating all out-edges of a vertex as equal. Its `labelPropagation` is likewise unweighted.
The `weight` column from stage 2 therefore has no effect on either by default.

This matters, because weight carries the signal. `python`–`pandas` co-occur tens of thousands
of times; `python`–`some-abandoned-library` co-occur once. Unweighted, those count equally,
and PageRank rewards tags with many incidental neighbours over tags with strong ones. Across
1,469,065 edges with a long tail of weight-1 accidents, this visibly distorts the leaderboard.

**Resolution:** filter edges by weight before graph construction, using the stage 3 threshold
parameter. At weight ≥ 5 the graph drops from 1,469,065 to 215,968 edges, and what is removed
is almost entirely one-off co-occurrence. Weight ≥ 5 is the primary threshold; the algorithms
are additionally swept across {1, 5, 20} and the sensitivity reported, which converts a
library limitation into stated methodology.

The same reasoning applies to Label Propagation, which is also unweighted, and to Triangle
Count, where weight-1 edges manufacture triangles between technologies that never genuinely
co-occur. Hence the threshold gates all four projection algorithms rather than PageRank alone.

True weighted PageRank is implementable in GraphFrames via `aggregateMessages`/Pregel. It is
recorded as future work (§10), not built now — substantial effort for a second-order gain.

## 7. Testing

`graph/tests/` mirrors `dataset/tests/`, with a session-scoped SparkSession fixture in
`conftest.py`.

Tests target the pure transforms over small synthetic DataFrames:

- stage 1 dedup-and-aggregate: answer counts, mean answer score, tag invariance within group
- stage 1 explode: edge count matches sum of tag-list lengths
- stage 2 pair generation: canonical ordering, no self-pairs, no duplicate pairs, correct weights
- stage 3 symmetrization: edge count doubles, no self-loops introduced
- stage 3 join: `tag_metrics` has exactly one row per tag, no nulls from failed joins
- toy-graph assertions for all four algorithms

The projection logic gets the heaviest coverage — double-counted pairs, self-pairs, and
wrong-ordered pairs are the most likely defects and the least visible in aggregate output.

Added with §11 and §12:

- stage 5 export: importer header syntax, tags without metrics written as empty fields,
  large integer labels written without a decimal point, relationships not symmetrized
- stage 6 sampling: deterministic, whole questions only, smaller scales nested in larger
- stage 6 plumbing: failed steps recorded rather than raised, timeouts attributed to the
  step that was running, cross-checks flag mismatches and skip missing sides
- one cross-engine test: GraphFrames and NetworkX agree on a toy graph's component and
  triangle counts — the same exact checks the benchmark makes at scale

Neo4j itself is checked by integration only: the query runner refuses to run until node and
relationship counts equal the reference figures.

## 8. Failure behaviour

Each stage script fails fast if its input artifact is missing, with a message naming the
stage that must be run first. Stages overwrite their outputs, so re-running is always safe
and no stage accumulates state across runs.

## 9. Out of scope

- The synopsis's 1/5/10 GB raw-XML benchmark subsets — `Posts.xml` is no longer on disk, and
  XML parsing is not done by Spark. Stage 6 scales by question sample instead (§12).
- Benchmarking XML parsing or a single-machine pandas ETL baseline — only graph construction
  and algorithms are compared
- Any user-derived edge, metric, or view — blocked on `OwnerUserId`
- Temporal analysis — blocked on `CreationDate`

## 10. Future work

- **Re-extract with `OwnerUserId` and `CreationDate`.** Unlocks the user graph, the
  PageRank-vs-reputation comparison, and temporal charts. Requires decompressing `Posts.xml`
  to C: (255 GB free; D: cannot hold it) and restoring the non-accepted-answer behaviour lost
  from `phase2.py`. `Users.7z` and `Votes.7z` are already on disk.
- **GEXF export for Gephi**, covering the other half of objective 5. Roughly thirty lines
  from `tag_metrics` plus the edge list.
- **Weighted PageRank** via `aggregateMessages`/Pregel (§6.3).

## 11. Stage 5 — Neo4j load and Cypher queries (objective 4)

*Added 2026-10-03.* This was first deferred because it adds no finding that Spark SQL cannot
produce. It is back because the synopsis treats a graph database as part of the core
contribution ("storing results in a proper graph database", Synopsis §2). Objective 4's own
example query, the shortest path between technology communities, is also a traversal that
Cypher expresses directly and Spark SQL does not.

**Runtime.** Neo4j Community **5.26 LTS**, installed from the Windows zip under `C:\neo4j\`.
5.26 is the last line that runs on Java 17, so it reuses the Temurin JDK from stage 0. The
2025.x releases need Java 21. No Docker, no Neo4j Desktop. `conf/neo4j.conf` caps heap and
page cache at 2 GB each. The server is stopped whenever a Spark stage or the benchmark runs:
15.7 GB of RAM cannot hold an 8 GB Spark driver, a 4 GB Neo4j and NetworkX at once.

**What is loaded: the whole graph, not a sample.**

| Element | Count | Properties |
|---|---|---|
| `(:Tag)` | 49,372 | `name`, stage 1 attributes, primary-threshold metrics where present |
| `(:Question)` | 2,532,514 | `question_id`, `score`, `answer_count`, `mean_answer_score`, `tag_count` |
| `(:Question)-[:TAGGED_WITH]->(:Tag)` | 7,775,852 | none |
| `(:Tag)-[:CO_OCCURS]->(:Tag)` | 1,469,065 | `weight` |

Only the 25,597 tags that survive weight ≥ 5 carry metrics (`tag_authority`, `community`,
`component`, `triangle_count`). The other 23,775 are imported with those properties absent,
not zero. Every co-occurrence edge is loaded with its weight, and queries apply the threshold
themselves. Stage 2 makes the same choice: the threshold stays a parameter instead of being
baked into storage. `CO_OCCURS` is stored once per pair in `src < dst` direction, as in stage
2. Cypher matches it undirected (`-[:CO_OCCURS]-`), so no symmetrization is needed.

**Load path.** The graph is loaded with `neo4j-admin database import full`, the bulk importer
the synopsis names, from four CSVs. pandas writes the CSVs, not Spark. This is a format
conversion over artifacts that already exist, and Spark would write a directory of part files
that each carry a header line, which the importer would read as data rows. Two formatting
rules are load-bearing:

- **Nullable integer columns use pandas' `Int64`.** These are `community`, `component` and
  `triangle_count`. As float64 they would print as `180388626595.0`, which the importer
  rejects for a `long` property, and LPA labels reach about 4×10¹¹.
- **Question nodes have an unnamed `:ID(Question)` column plus a separate `question_id:long`
  property.** A named ID column would be stored as a string, because tag names force the
  importer's global ID type to string.

**Correctness signal.** After import, node and relationship counts must equal the table
above exactly, and exactly 23,775 tags must lack `community`. The second check proves that
empty CSV fields became absent properties. The query runner checks both before it runs any
query.

**Queries.** They live in `lib/cypher.py`. `stage5_neo4j_queries.py` runs them and saves the
results to `out/results/neo4j_queries.json`. All are parameterized:

1. **Shortest path between technologies in different communities.** This is objective 4's
   own example, e.g. `solidity` (the blockchain community) to `haskell`, over edges of
   weight ≥ 5.
2. **Bridge tags.** These are the tags whose weight ≥ 5 neighbours span the most *other*
   communities: the tag-level counterpart of the synopsis's "users who bridge multiple
   domains", which stays blocked on `OwnerUserId`.
3. **Questions that carry two given tags.** A bipartite traversal, ordered by score.
4. **Stack completion.** The tags that co-occur most strongly with both of two given tags.

Neo4j Browser (`http://localhost:7474`) is the interactive surface the synopsis asks for. No
other stage, and not the dashboard, reads from Neo4j.

## 12. Stage 6 — Spark vs NetworkX benchmark (objective 6)

*Added 2026-10-03.* This was first dropped because it was thought to mean something only once
the user graph existed. That reasoning was wrong. The question–tag bipartite graph (2.58M
vertices, 7.78M edges) is already far beyond the ~100,000 nodes at which the synopsis says
single-machine tools struggle.

**Scale axis.** The synopsis's 1/5/10 GB raw-XML subsets cannot be reproduced: `Posts.xml` is
no longer on disk, and XML parsing is done by `dataset/`'s multiprocess Python, not by Spark.
Scale is instead a deterministic **fraction of questions**, {10, 25, 50, 100}%, selected by
`pmod(xxhash64(question_id), 100) < pct`. Hashing instead of calling `.sample()` keeps the
subsets identical across processes and runs, and nests them (the 10% subset sits inside the
25% one). It also samples whole questions, so `answer_count` is not distorted. Scale is
reported as rows, vertices and edges, not as GB.

**What is timed.**

| Engine | Steps |
|---|---|
| Spark | `session_start`; `etl` (read, sample, aggregate, explode, write); `projection_build`; then per graph: `load` (build and cache vertices and edges), then the algorithms |
| NetworkX | per graph: `load` (read Parquet, build `nx.Graph`), then the algorithms |

The algorithms depend on the graph:

- **Bipartite:** Connected Components only. PageRank and LPA are not meaningful on it, and
  Triangle Count is identically zero there (§6.1).
- **Projection:** Connected Components, PageRank, Label Propagation and Triangle Count.

The projection is benchmarked at **weight ≥ 1**, not the primary threshold of 5. Sampling
shrinks every weight roughly in proportion, so a fixed threshold of 5 would remove a growing
share of edges at smaller scales, mixing up graph size with sampling rate. The benchmark
measures cost, not findings, so the unthresholded graph is the honest choice.

Every Spark step is forced, either by a write or by an aggregate the step's result needs. An
unforced lazy DataFrame would time nothing.

**Identical inputs.** Spark builds each scale's graphs and writes them to
`out/bench/scale_<pct>/`, and NetworkX reads those exact files. That lets the two engines be
checked against each other:

| Check | Expected |
|---|---|
| Vertices and edges per graph | Equal |
| Connected component count (both graphs) | Equal |
| Total triangles | Equal |
| PageRank | Spearman rank correlation is reported, expected > 0.95. GraphFrames runs a fixed 20 iterations; NetworkX runs to convergence. |
| Label Propagation | Community counts are reported but **not** expected to match. GraphFrames is synchronous with 10 iterations; NetworkX's `label_propagation_communities` is semi-synchronous and runs to convergence. |

At 100% the Spark figures must also reproduce the reference counts: 7,775,852 question–tag
edges, 1,469,065 tag–tag edges, 2,581,886 bipartite vertices, 49,260 projection vertices and
119 bipartite components.

**Isolation.** Every (engine, scale, graph) runs in its own process, one after another:

- **A fresh SparkSession per process** avoids the heap-accumulation failure documented in
  stage 3.
- **A Spark JVM never runs beside NetworkX,** so the two cannot compete for RAM and distort
  each other's timings.
- **Each NetworkX run has a time limit** (default 30 min). A step that does not finish is
  recorded as `timeout` rather than treated as a crash, and the next run goes ahead. A
  NetworkX timeout or memory failure at full scale is a legitimate result: it is the effect
  the objective sets out to show.
- **Spark runs have no time limit,** because killing the Python driver would orphan its JVM.

**Fairness caveat, stated in the report.** Spark runs in local mode on the 12 cores of one
machine, and NetworkX is single-threaded. The comparison is a parallel dataflow engine
against a single-threaded in-memory library on the same hardware, not a cluster against a
laptop. Spark's fixed costs (JVM start, task scheduling) are timed separately so that the
crossover point shows. NetworkX is expected to win on small graphs.

**Outputs.**

| Path | Contents |
|---|---|
| `out/bench/records.jsonl` | One raw record per timed step |
| `out/results/benchmark.csv` | Records flattened: `engine, scale_pct, repeat, graph, step, status, seconds, vertices, edges, result, peak_mb, error` |
| `out/results/benchmark_checks.csv` | Cross-engine checks per scale |

`result` holds the component count, triangle total or community count, depending on the
step. NetworkX peak working-set memory is recorded via `psutil`. Spark's is not: its heap is
fixed by `driver_memory`, so the numbers would not be comparable. Runs repeat once by
default; with `--repeats N`, the dashboard shows medians.

A fifth dashboard tab plots seconds against scale for each step and engine, and lists runs
that did not finish.
