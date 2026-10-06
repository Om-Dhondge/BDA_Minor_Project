# Tag Graph Analytics Pipeline Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build a staged PySpark + GraphFrames pipeline that turns the existing preprocessed Stack Overflow Parquet into tag-graph analytics (Label Propagation, Connected Components, Triangle Count, PageRank) surfaced through a Streamlit dashboard. Then load the graph into Neo4j for Cypher queries, and benchmark Spark against NetworkX across data scales.

**Architecture:** Seven stages, each writing materialized artifacts to `graph/out/` and each independently re-runnable. Stage 0 stands up the runtime and validates GraphFrames. Stages 1–2 build the question–tag bipartite edge list and the tag–tag co-occurrence projection. Stage 3 runs the four algorithms. Stage 4 is a Streamlit app that reads only the small result tables, with no Spark session at runtime. Stage 5 bulk-imports the full graph into Neo4j and runs the objective-4 queries. Stage 6 times Spark and NetworkX on identical graphs at four data scales, each run in its own process.

**Tech Stack:** Python 3.11, PySpark 3.5.3, GraphFrames, PyArrow, pandas 2.x, Streamlit, Plotly, pytest. JDK 17 (Temurin). Added for Tasks 10–14: Neo4j Community 5.26 LTS, the `neo4j` Python driver 5.x, NetworkX 3.4–3.6, SciPy, psutil.

**Spec:** `docs/superpowers/specs/2026-08-26-tag-graph-analytics-design.md` (Stage 5 is §11, Stage 6 is §12)

**Progress (2026-10-03):** Tasks 1–8 are complete and verified. Tasks 10–14 were added on 2026-10-03, when objectives 4 and 6 came back into scope. Task 9 (git) was run early, on 2026-10-03. **Next is Task 10.** Each of Tasks 10–14 ends with its own commit.

## Global Constraints

- **Input artifact is `dataset/Datasets_2nd run/preprocessed_posts.parquet`** (6.93 GB, 6,997,182 rows). Never the 1st run — it holds one accepted answer per question, which would make `answer_count` uniformly 1.
- **Never read the text columns.** Only `question_id`, `tags`, `score`, `answer_score`. Parquet is columnar; this projection reads a few hundred MB rather than 6.93 GB.
- **Never depend on the `answer_id` column.** It exists in the 2nd-run artifact but is absent from the committed `dataset/lib/phase2.py` schema. Derive answer counts from row counts per `question_id`.
- **Python must be 3.11, not 3.12.** PySpark 3.5.3 supports 3.8–3.11 only and bundles cloudpickle 2.2.1, which predates 3.12. On 3.12 every executor Python worker dies with no traceback — `Python worker exited unexpectedly (crashed)` / `EOFException` — surviving both `spark.python.worker.faulthandler` and a stderr-capturing wrapper. Verified in Task 1: identical repro fails on 3.12.10, passes on 3.11.9.
- **pandas is pinned below 3.0.** PySpark 3.5.3 predates pandas 3.0 and is tested against 2.x; an unpinned `pandas>=2.2.0` resolves to 3.0.x. Stages 0–3 never touch pandas, so the blast radius is Stage 4 plus any `toPandas()` call.
- **Spark scratch space goes on C:** (`C:\spark-tmp` by default, override with `SPARK_SCRATCH`). D: has only 30 GB free and shuffle spill will exhaust it.
- **Primary weight threshold is 5.** `tag_metrics.parquet` is always the weight ≥ 5 result. Sweep thresholds are exactly `{1, 5, 20}`.
- **The weight threshold gates all four projection algorithms**, not PageRank alone.
- **Triangle Count never runs on the bipartite graph** — bipartite graphs have no odd cycles, so the result is identically zero.
- **PageRank is symmetrized at point of use.** Stage 2 stores each edge once, canonically `src < dst`.
- **PageRank ranks technologies, not users.** All labels and column names say "tag authority" / "technology centrality". Never "expert" or "reputation".
- **Tests run from inside `graph/`** (`cd graph && pytest tests/ -v`), so imports are `from lib.etl import ...`. This mirrors the existing `dataset/` convention.
- **Git: commit at the end of each task, on `feat/tag-graph-analytics`.** Task 9 set up the branch and `.gitignore` on 2026-10-03. Stage only the paths the task's commit step lists, never `git add .` or `-A`. **Never stage `CLAUDE.md`**: it stays untracked by user instruction. **The user runs every git write command (add, commit, branch, push) themselves.** At a commit step, print the exact commands for PowerShell and wait. Do not execute them.
- **Neo4j is Community 5.26 LTS, nothing newer.** It is the last line on Java 17; 2025.x needs Java 21. Install it under `C:\neo4j\`, never D:.
- **Never run Neo4j alongside Spark or the benchmark.** 15.7 GB of RAM cannot hold an 8 GB Spark driver, a 4 GB Neo4j and NetworkX at once. Stop the server first.
- **The Neo4j password lives only in `$env:NEO4J_PASSWORD`.** It is never committed or hardcoded.
- **NetworkX is capped `<3.7`.** NetworkX 3.7 requires Python 3.12, and this venv must stay on 3.11.
- **The benchmark projection runs at weight ≥ 1, not 5.** Sampling scales every weight down, so a fixed threshold of 5 would mix up graph size with sampling rate (spec §12).
- **Benchmark scales are `{10, 25, 50, 100}`% of questions**, selected by `pmod(xxhash64(question_id), 100) < pct`.
- **Spark in the benchmark is local mode on one machine.** Report it as 12 cores against single-threaded NetworkX, never as a cluster.

**Expected reference figures** (verified against the real artifact — use these to check stage output):

| Quantity | Value |
|---|---|
| Answer rows in | 6,997,182 |
| Distinct questions | 2,532,514 |
| Distinct tags | 49,372 |
| Question–tag edges | 7,775,852 |
| Tag–tag edges (weight ≥ 1) | 1,469,065 |
| Tag–tag edges (weight ≥ 5) | 215,968 |
| Bipartite vertices | 2,581,886 |
| Bipartite components (stage 3) | 119 |
| Projection vertices at weight ≥ 1 | 49,260 |
| Tags with metrics (weight ≥ 5) / without | 25,597 / 23,775 |

## Review Focus (Tasks 10–14)

These are the failure modes the spec implies that are most likely to bite. Each is pinned by a test in the task that owns the code:

1. **Tags that fell below the weight threshold.** They must import with metric properties absent, not as `nan` or `0`. Test: `test_missing_metrics_are_empty_fields_not_nan` (Task 10).
2. **LPA community labels near 4×10¹¹.** They must be written as integers. A float column prints `180388626595.0`, which aborts the import. Test: `test_large_community_labels_are_written_as_integers` (Task 10).
3. **A NetworkX run that exceeds its time limit.** It must be recorded as `timeout` against the step it was inside, and the benchmark must carry on. Test: `test_mark_unfinished_blames_the_first_missing_step` (Task 12).
4. **A NetworkX run that raises** (`MemoryError`, convergence failure). It must be recorded as `error`, and the worker must go on to the next algorithm. Test: `test_run_step_records_failure_and_continues` (Task 12).
5. **The engines running on different graphs**, for example from a stale scale directory. Exact cross-checks must flag it as `MISMATCH` instead of silently comparing timings. Tests: `test_cross_checks_flag_mismatch_and_skip_missing` (Task 12) and `test_engines_agree_on_toy_graph` (Task 13).

## File Structure

| File | Responsibility |
|---|---|
| `graph/lib/__init__.py` | Package marker (empty) |
| `graph/lib/session.py` | SparkSession factory: GraphFrames package, scratch dirs, checkpointing |
| `graph/lib/etl.py` | Stage 1 transforms: question aggregation, tag explode, tag attributes |
| `graph/lib/projection.py` | Stage 2 transform: tag–tag co-occurrence self-join |
| `graph/lib/algorithms.py` | Stage 3 transforms: thresholding, symmetrization, vertex derivation, algorithm runners |
| `graph/stage0_smoke.py` | Environment validation against a 6-node toy graph |
| `graph/stage1_etl.py` | Stage 1 runner |
| `graph/stage2_projection.py` | Stage 2 runner |
| `graph/stage3_algorithms.py` | Stage 3 runner |
| `graph/dashboard/app.py` | Streamlit dashboard (no Spark) |
| `graph/tests/__init__.py` | Package marker (empty) |
| `graph/tests/conftest.py` | Session-scoped SparkSession fixture |
| `graph/tests/test_etl.py` | Stage 1 transform tests |
| `graph/tests/test_projection.py` | Stage 2 transform tests |
| `graph/tests/test_algorithms.py` | Stage 3 transform tests |
| `graph/requirements.txt` | Pinned Python dependencies |
| `.gitignore` | Excludes data, artifacts, venv (Task 9) |
| `graph/lib/neo4j_export.py` | Stage 5: artifacts → bulk-import CSV frames (Task 10) |
| `graph/stage5_neo4j_export.py` | Stage 5a runner: writes `out/neo4j_import/*.csv` (Task 10) |
| `graph/tests/test_neo4j_export.py` | Export tests: headers, nulls, integer formatting (Task 10) |
| `graph/lib/cypher.py` | Stage 5: count checks, schema, the four queries (Task 11) |
| `graph/stage5_neo4j_queries.py` | Stage 5b runner: verify import, run queries (Task 11) |
| `graph/lib/bench.py` | Stage 6: record format, step timing, timeout attribution, cross-checks (Task 12) |
| `graph/bench_spark.py` | Stage 6 worker: one Spark part at one scale (Task 12) |
| `graph/tests/test_bench.py` | Benchmark plumbing tests (Task 12) |
| `graph/lib/nx_algorithms.py` | Stage 6: NetworkX baseline algorithms (Task 13) |
| `graph/bench_networkx.py` | Stage 6 worker: one NetworkX graph at one scale (Task 13) |
| `graph/stage6_benchmark.py` | Stage 6 orchestrator: runs workers, writes results (Task 13) |
| `graph/tests/test_nx_algorithms.py` | NetworkX baseline and cross-engine tests (Task 13) |

---

### Task 1: Stage 0 — Runtime and GraphFrames validation

This is the highest-risk task in the plan. GraphFrames is a third-party Scala package whose Maven coordinate must match the Spark and Scala versions exactly, and the combination is historically fragile on Windows. It is sequenced first and kept small so failure surfaces in minutes.

> **STATUS: COMPLETE (2026-08-26).** `STAGE 0 PASSED`, exit 0, reproduced across three runs. All four GraphFrames algorithms work. Outcomes worth carrying forward:
>
> - **Coordinate 1 resolved** — `graphframes:graphframes:0.8.4-spark3.5-s_2.12`, from the spark-packages repo rather than Maven Central. `pip install graphframes` (0.6) drives the 0.8.4 JAR fine; no JAR extraction was needed.
> - **PageRank confirmed to ignore edge weights**, so the spec §6.3 thresholding decision stands as necessary. Flattening all weights to 1 gave bit-identical scores.
> - **Python 3.12 does not work** — see Global Constraints. Cost most of the task; the venv is now 3.11.9.
> - **Two Windows prerequisites the plan missed**: `winutils.exe` and a pinned `PYSPARK_PYTHON`. Both are handled inside `build_session`, so Stages 1–4 need no action.
> - **Step 5's Label Propagation assertion was wrong** and has been corrected below. LPA is not Connected Components.
>
> The shipped `graph/lib/session.py` carries two guards beyond the Step 4 listing (`_ensure_hadoop_home`, `_ensure_python_workers`). The file on disk is the source of truth; do not revert it to the block below.

**Files:**
- Create: `graph/lib/__init__.py`
- Create: `graph/lib/session.py`
- Create: `graph/stage0_smoke.py`
- Create: `graph/requirements.txt`

**Interfaces:**
- Consumes: nothing
- Produces: `build_session(app_name: str, scratch_root: str | Path | None = None, driver_memory: str = "8g") -> SparkSession` — every later stage calls this.

- [x] **Step 1: Install JDK 17**

```powershell
winget install --id EclipseAdoptium.Temurin.17.JDK -e
```

Then set `JAVA_HOME` permanently and confirm. The exact install path varies by version; discover it rather than assuming:

```powershell
$jdk = (Get-ChildItem 'C:\Program Files\Eclipse Adoptium' -Directory | Where-Object Name -like 'jdk-17*' | Select-Object -First 1).FullName
[Environment]::SetEnvironmentVariable('JAVA_HOME', $jdk, 'User')
$env:JAVA_HOME = $jdk
& "$env:JAVA_HOME\bin\java.exe" -version
```

Expected: `openjdk version "17.0.x"`. If `winget` is unavailable, download the Temurin 17 MSI from adoptium.net and install manually.

- [x] **Step 2: Create the project virtualenv and requirements file**

Create `graph/requirements.txt`:

```
# Requires Python 3.11. PySpark 3.5.3 does not support 3.12: its bundled
# cloudpickle 2.2.1 predates 3.12 and the executor's Python worker dies
# without a traceback ("Python worker exited unexpectedly (crashed)").
pyspark==3.5.3
pyarrow>=17.0.0
# Capped below 3.0: PySpark 3.5.3 predates pandas 3.0 and is tested against 2.x.
pandas>=2.2.0,<3.0
streamlit>=1.38.0
plotly>=5.24.0
pytest>=8.3.0
```

The venv **must** be built on Python 3.11 — a bare `python` here picks up whatever
is on PATH, which on this machine is 3.12 and produces a Spark that starts
cleanly and then fails every job. Install 3.11 if absent:

```powershell
winget install --id Python.Python.3.11 -e
```

Then build the venv against it explicitly, and confirm the version before
installing anything:

```powershell
& "$env:LOCALAPPDATA\Programs\Python\Python311\python.exe" -m venv .venv
.\.venv\Scripts\python.exe --version     # must print 3.11.x
.\.venv\Scripts\python.exe -m pip install --upgrade pip
.\.venv\Scripts\python.exe -m pip install -r graph\requirements.txt
```

Note `graphframes` is deliberately absent — its Python wrapper is resolved in Step 3, because the correct package name depends on what works.

- [x] **Step 3: Resolve the GraphFrames coordinate and Python wrapper**

This step is an investigation, not a fixed recipe. Work through the coordinates in order and record which one resolves:

1. `graphframes:graphframes:0.8.4-spark3.5-s_2.12` ← **RESOLVED, in use.** 2 and 3 were never needed.
2. `graphframes:graphframes:0.8.3-spark3.5-s_2.12`
3. `graphframes:graphframes:0.8.2-spark3.2-s_2.12`

Test resolution directly — this downloads the JAR from Maven Central and needs network access:

```powershell
.\.venv\Scripts\python.exe -c "from pyspark.sql import SparkSession; s=SparkSession.builder.master('local[2]').config('spark.jars.packages','graphframes:graphframes:0.8.4-spark3.5-s_2.12').getOrCreate(); print('RESOLVED'); s.stop()"
```

Expected: Ivy download output, then `RESOLVED`. On failure the error names an unresolved dependency — move to the next coordinate.

For the Python wrapper, try `pip install graphframes` first. If `from graphframes import GraphFrame` fails at import, extract the bundled Python package from the downloaded JAR instead (it ships inside it) and add it to `PYTHONPATH`; the JAR lands under `~\.ivy2\jars\`.

**Record the working coordinate — Step 4 hardcodes it.**

- [x] **Step 4: Write the SparkSession factory**

Create `graph/lib/__init__.py` as an empty file, then `graph/lib/session.py`:

```python
import os
from pathlib import Path

from pyspark.sql import SparkSession

# Coordinate confirmed working in Task 1 Step 3. Update here if it changes.
GRAPHFRAMES_PACKAGE = "graphframes:graphframes:0.8.4-spark3.5-s_2.12"

DEFAULT_SCRATCH = Path(os.environ.get("SPARK_SCRATCH", r"C:\spark-tmp"))


def build_session(app_name, scratch_root=None, driver_memory="8g"):
    """Create a local-mode SparkSession with GraphFrames and checkpointing ready.

    Scratch directories default to C: because D: has insufficient free space
    for shuffle spill.
    """
    scratch = Path(scratch_root) if scratch_root else DEFAULT_SCRATCH
    local_dir = scratch / "local"
    checkpoint_dir = scratch / "checkpoints"
    local_dir.mkdir(parents=True, exist_ok=True)
    checkpoint_dir.mkdir(parents=True, exist_ok=True)

    spark = (
        SparkSession.builder
        .appName(app_name)
        .master("local[*]")
        .config("spark.jars.packages", GRAPHFRAMES_PACKAGE)
        .config("spark.local.dir", str(local_dir))
        .config("spark.driver.memory", driver_memory)
        .config("spark.sql.shuffle.partitions", "48")
        .getOrCreate()
    )
    spark.sparkContext.setCheckpointDir(str(checkpoint_dir))
    spark.sparkContext.setLogLevel("WARN")
    return spark
```

`spark.driver.memory` must be set before the JVM launches. In local mode PySpark forwards builder configs to the launcher, but verify it took effect in Step 6. If it did not, set it before importing pyspark instead:

```python
os.environ["PYSPARK_SUBMIT_ARGS"] = "--driver-memory 8g pyspark-shell"
```

- [x] **Step 5: Write the smoke test**

Create `graph/stage0_smoke.py`. The toy graph is two disjoint components: a triangle `a-b-c`, and a path `d-e-f`. Every expected value below is hand-derived.

```python
"""Stage 0: validate JDK, Spark, and GraphFrames before any real work."""
import sys

from graphframes import GraphFrame

from lib.session import build_session


def build_toy_graph(spark):
    """Triangle a-b-c plus path d-e-f. Edges stored once, canonical src < dst."""
    vertices = spark.createDataFrame(
        [("a",), ("b",), ("c",), ("d",), ("e",), ("f",)], ["id"]
    )
    edges = spark.createDataFrame(
        [
            ("a", "b", 10),
            ("b", "c", 10),
            ("a", "c", 10),
            ("d", "e", 1),
            ("e", "f", 1),
        ],
        ["src", "dst", "weight"],
    )
    return GraphFrame(vertices, edges)


def symmetrize(edges):
    from pyspark.sql import functions as F

    return edges.select("src", "dst", "weight").union(
        edges.select(
            F.col("dst").alias("src"), F.col("src").alias("dst"), F.col("weight")
        )
    )


def main():
    spark = build_session("stage0-smoke", driver_memory="2g")
    failures = []

    g = build_toy_graph(spark)

    # Connected Components: exactly 2 components, sizes 3 and 3.
    cc = g.connectedComponents()
    sizes = sorted(r["count"] for r in cc.groupBy("component").count().collect())
    if sizes != [3, 3]:
        failures.append(f"connectedComponents: expected sizes [3, 3], got {sizes}")

    # Triangle Count: a, b, c each in 1 triangle; d, e, f in 0.
    tri = {r["id"]: r["count"] for r in g.triangleCount().collect()}
    expected_tri = {"a": 1, "b": 1, "c": 1, "d": 0, "e": 0, "f": 0}
    if tri != expected_tri:
        failures.append(f"triangleCount: expected {expected_tri}, got {tri}")

    # Label Propagation. NOT one label per component - LPA is not Connected
    # Components. Under synchronous updates the path d-e-f settles into
    # {d, f} and {e}: d and f see only e, e sees both, and the labels flip
    # across the bipartition. That is stable from iteration 2 onward, so the
    # invariants worth asserting are the ones LPA actually guarantees.
    lpa = {r["id"]: r["label"] for r in g.labelPropagation(maxIter=5).collect()}
    if set(lpa) != {"a", "b", "c", "d", "e", "f"}:
        failures.append(f"labelPropagation: expected all 6 vertices, got {sorted(lpa)}")
    else:
        # A label can never cross a component boundary - no edges to carry it.
        triangle_labels = {lpa["a"], lpa["b"], lpa["c"]}
        path_labels = {lpa["d"], lpa["e"], lpa["f"]}
        if triangle_labels & path_labels:
            failures.append(
                "labelPropagation: a label spans both components, which is "
                f"impossible: {lpa}"
            )
        # The triangle is a clique, so it must collapse to a single label.
        if len(triangle_labels) != 1:
            failures.append(
                f"labelPropagation: triangle a/b/c should share one label, got {lpa}"
            )

    # PageRank needs symmetric edges. On the symmetrized graph a, b, c are
    # interchangeable and must score equally.
    g_sym = GraphFrame(g.vertices, symmetrize(g.edges))
    pr = {r["id"]: r["pagerank"] for r in g_sym.pageRank(
        resetProbability=0.15, maxIter=20).vertices.collect()}
    triangle_scores = [pr["a"], pr["b"], pr["c"]]
    if max(triangle_scores) - min(triangle_scores) > 1e-6:
        failures.append(f"pageRank: a/b/c should tie, got {triangle_scores}")

    # Confirm PageRank ignores edge weights (spec section 6.3). The triangle
    # edges have weight 10 and the path edges weight 1; re-running with all
    # weights set to 1 must produce identical scores.
    from pyspark.sql import functions as F
    flat = g.edges.withColumn("weight", F.lit(1))
    pr_flat = {r["id"]: r["pagerank"] for r in GraphFrame(
        g.vertices, symmetrize(flat)).pageRank(
        resetProbability=0.15, maxIter=20).vertices.collect()}
    if any(abs(pr[k] - pr_flat[k]) > 1e-9 for k in pr):
        failures.append(
            "pageRank appears to USE edge weights - spec section 6.3 assumption "
            "is wrong, revisit the thresholding decision"
        )

    spark.stop()

    if failures:
        print("STAGE 0 FAILED:")
        for f in failures:
            print(f"  - {f}")
        sys.exit(1)
    print("STAGE 0 PASSED: JDK, Spark, and all four GraphFrames algorithms work.")


if __name__ == "__main__":
    main()
```

- [x] **Step 6: Run the smoke test**

```powershell
cd graph
..\.venv\Scripts\python.exe stage0_smoke.py
```

Expected: `STAGE 0 PASSED: JDK, Spark, and all four GraphFrames algorithms work.`

Also confirm driver memory took effect — add a temporary print of `spark.conf.get("spark.driver.memory")` if unsure, or check the Spark UI at `http://localhost:4040` while the job runs.

**If `connectedComponents` fails with a checkpoint error**, `setCheckpointDir` did not take — verify the path exists and is writable.

**If GraphFrames cannot be resolved at all after all three coordinates**, STOP and report. The fallback is hand-rolled DataFrame implementations of the four algorithms, which keeps the work in Spark but is a significant scope change requiring a spec amendment.

**If the weight-ignoring check fails**, that is a genuinely good outcome — it means weighted PageRank is available. Report it; the thresholding decision in spec §6.3 becomes optional rather than necessary.

---

### Task 2: Stage 1 transforms — question aggregation and tag explode

**Files:**
- Create: `graph/lib/etl.py`
- Create: `graph/tests/__init__.py`
- Create: `graph/tests/conftest.py`
- Create: `graph/tests/test_etl.py`

**Interfaces:**
- Consumes: `build_session` from Task 1
- Produces:
  - `read_posts(spark, path: str) -> DataFrame` — columns `question_id, tags, score, answer_score`
  - `aggregate_questions(posts: DataFrame) -> DataFrame` — columns `question_id, tags, score, answer_count, mean_answer_score, tag_count`
  - `explode_question_tags(questions: DataFrame) -> DataFrame` — columns `question_id, tag`
  - `build_tag_attributes(edges: DataFrame, questions: DataFrame) -> DataFrame` — columns `tag, question_count, mean_question_score, mean_answers_per_question, mean_answer_score`

- [ ] **Step 1: Write the test fixture**

Create `graph/tests/__init__.py` as an empty file, then `graph/tests/conftest.py`:

```python
import pytest

from lib.session import build_session


@pytest.fixture(scope="session")
def spark(tmp_path_factory):
    """One SparkSession for the whole test run - JVM startup is expensive."""
    scratch = tmp_path_factory.mktemp("spark_scratch")
    session = build_session("tests", scratch_root=scratch, driver_memory="2g")
    yield session
    session.stop()


@pytest.fixture
def posts(spark):
    """Three questions. q1 has 3 answers, q2 has 1, q3 has 2.

    Mirrors the real input: one row per answer, question fields repeated.
    """
    return spark.createDataFrame(
        [
            (1, ["python", "django"], 10, 5),
            (1, ["python", "django"], 10, 3),
            (1, ["python", "django"], 10, 1),
            (2, ["java"], 4, 8),
            (3, ["python", "pandas", "numpy"], 7, 2),
            (3, ["python", "pandas", "numpy"], 7, 4),
        ],
        ["question_id", "tags", "score", "answer_score"],
    )
```

- [ ] **Step 2: Write the failing tests**

Create `graph/tests/test_etl.py`:

```python
from lib.etl import (
    aggregate_questions,
    build_tag_attributes,
    explode_question_tags,
)


def test_aggregate_collapses_answers_to_one_row_per_question(posts):
    result = aggregate_questions(posts)
    assert result.count() == 3


def test_aggregate_counts_answers_per_question(posts):
    result = {r["question_id"]: r["answer_count"]
              for r in aggregate_questions(posts).collect()}
    assert result == {1: 3, 2: 1, 3: 2}


def test_aggregate_means_answer_score(posts):
    result = {r["question_id"]: r["mean_answer_score"]
              for r in aggregate_questions(posts).collect()}
    assert result[1] == 3.0   # (5 + 3 + 1) / 3
    assert result[2] == 8.0
    assert result[3] == 3.0   # (2 + 4) / 2


def test_aggregate_preserves_invariant_question_fields(posts):
    result = {r["question_id"]: (r["score"], sorted(r["tags"]), r["tag_count"])
              for r in aggregate_questions(posts).collect()}
    assert result[1] == (10, ["django", "python"], 2)
    assert result[2] == (4, ["java"], 1)
    assert result[3] == (7, ["numpy", "pandas", "python"], 3)


def test_explode_produces_one_row_per_question_tag_pair(posts):
    edges = explode_question_tags(aggregate_questions(posts))
    assert edges.count() == 6   # 2 + 1 + 3


def test_explode_drops_empty_tags(spark):
    from lib.etl import explode_question_tags
    df = spark.createDataFrame([(1, ["python", "", None])], ["question_id", "tags"])
    assert explode_question_tags(df).count() == 1


def test_tag_attributes_count_questions_per_tag(posts):
    questions = aggregate_questions(posts)
    edges = explode_question_tags(questions)
    result = {r["tag"]: r["question_count"]
              for r in build_tag_attributes(edges, questions).collect()}
    assert result == {"python": 2, "django": 1, "java": 1, "pandas": 1, "numpy": 1}


def test_tag_attributes_average_across_questions(posts):
    questions = aggregate_questions(posts)
    edges = explode_question_tags(questions)
    rows = {r["tag"]: r for r in build_tag_attributes(edges, questions).collect()}
    # python appears on q1 (score 10, 3 answers) and q3 (score 7, 2 answers)
    assert rows["python"]["mean_question_score"] == 8.5
    assert rows["python"]["mean_answers_per_question"] == 2.5
    # java appears only on q2
    assert rows["java"]["mean_question_score"] == 4.0
```

- [ ] **Step 3: Run tests to verify they fail**

```powershell
cd graph
..\.venv\Scripts\python.exe -m pytest tests/test_etl.py -v
```

Expected: collection error — `ModuleNotFoundError: No module named 'lib.etl'`.

- [ ] **Step 4: Write the implementation**

Create `graph/lib/etl.py`:

```python
from pyspark.sql import DataFrame, functions as F

INPUT_COLUMNS = ["question_id", "tags", "score", "answer_score"]


def read_posts(spark, path: str) -> DataFrame:
    """Read only the columns the graph needs.

    Parquet is columnar, so this touches a few hundred MB of a 6.93 GB file.
    Never widen this projection to the text columns.
    """
    return spark.read.parquet(path).select(*INPUT_COLUMNS)


def aggregate_questions(posts: DataFrame) -> DataFrame:
    """Collapse one-row-per-answer into one-row-per-question.

    `tags` and `score` are invariant within a question_id group, so `first`
    is safe. Answer count comes from row count, never from `answer_id`, which
    the committed preprocessing schema does not define.
    """
    return (
        posts.groupBy("question_id")
        .agg(
            F.first("tags").alias("tags"),
            F.first("score").alias("score"),
            F.count(F.lit(1)).alias("answer_count"),
            F.avg("answer_score").alias("mean_answer_score"),
        )
        .withColumn("tag_count", F.size("tags"))
    )


def explode_question_tags(questions: DataFrame) -> DataFrame:
    """One row per (question, tag). Drops null and empty tag strings."""
    return (
        questions.select("question_id", F.explode("tags").alias("tag"))
        .filter(F.col("tag").isNotNull() & (F.length(F.col("tag")) > 0))
    )


def build_tag_attributes(edges: DataFrame, questions: DataFrame) -> DataFrame:
    """Per-tag node attributes, averaged over the questions carrying that tag."""
    return (
        edges.join(questions, on="question_id", how="inner")
        .groupBy("tag")
        .agg(
            F.count(F.lit(1)).alias("question_count"),
            F.avg("score").alias("mean_question_score"),
            F.avg("answer_count").alias("mean_answers_per_question"),
            F.avg("mean_answer_score").alias("mean_answer_score"),
        )
    )
```

- [ ] **Step 5: Run tests to verify they pass**

```powershell
cd graph
..\.venv\Scripts\python.exe -m pytest tests/test_etl.py -v
```

Expected: 8 passed.

---

### Task 3: Stage 1 runner — materialize ETL artifacts

**Files:**
- Create: `graph/stage1_etl.py`

**Interfaces:**
- Consumes: `read_posts`, `aggregate_questions`, `explode_question_tags`, `build_tag_attributes` from Task 2; `build_session` from Task 1
- Produces: `graph/out/edges_question_tag.parquet`, `graph/out/nodes_question.parquet`, `graph/out/nodes_tag.parquet`

- [ ] **Step 1: Write the runner**

Create `graph/stage1_etl.py`:

```python
"""Stage 1: Parquet -> question-tag edge list plus node attributes."""
import argparse
import sys
from pathlib import Path

from lib.etl import (
    aggregate_questions,
    build_tag_attributes,
    explode_question_tags,
    read_posts,
)
from lib.session import build_session

DEFAULT_INPUT = r"..\dataset\Datasets_2nd run\preprocessed_posts.parquet"


def log(msg):
    print(f"[stage1] {msg}", flush=True)


def main():
    parser = argparse.ArgumentParser(description="Stage 1: ETL to graph edge lists")
    parser.add_argument("--input", default=DEFAULT_INPUT,
                        help="Path to preprocessed_posts.parquet (2nd run)")
    parser.add_argument("--out-dir", default="out", help="Artifact output directory")
    args = parser.parse_args()

    if not Path(args.input).exists():
        print(f"[stage1] ERROR: input not found: {args.input}", file=sys.stderr)
        sys.exit(1)

    out = Path(args.out_dir)
    out.mkdir(parents=True, exist_ok=True)

    spark = build_session("stage1-etl")

    posts = read_posts(spark, args.input)
    questions = aggregate_questions(posts).cache()
    edges = explode_question_tags(questions).cache()
    tags = build_tag_attributes(edges, questions)

    log("writing nodes_question...")
    questions.write.mode("overwrite").parquet(str(out / "nodes_question.parquet"))
    log("writing edges_question_tag...")
    edges.write.mode("overwrite").parquet(str(out / "edges_question_tag.parquet"))
    log("writing nodes_tag...")
    tags.write.mode("overwrite").parquet(str(out / "nodes_tag.parquet"))

    log(f"questions: {questions.count()}")
    log(f"question-tag edges: {edges.count()}")
    log(f"tags: {spark.read.parquet(str(out / 'nodes_tag.parquet')).count()}")

    spark.stop()


if __name__ == "__main__":
    main()
```

- [ ] **Step 2: Run against the real dataset**

```powershell
cd graph
..\.venv\Scripts\python.exe stage1_etl.py
```

- [ ] **Step 3: Verify the counts match the reference figures**

Expected output, which must match the Global Constraints table exactly:

```
[stage1] questions: 2532514
[stage1] question-tag edges: 7775852
[stage1] tags: 49372
```

If `questions` reads 2,532,450 you are on the 1st-run artifact — switch to the 2nd run. Any other mismatch means a filter or join is wrong; stop and diagnose before continuing.

---

### Task 4: Stage 2 — tag co-occurrence projection

**Files:**
- Create: `graph/lib/projection.py`
- Create: `graph/tests/test_projection.py`
- Create: `graph/stage2_projection.py`

**Interfaces:**
- Consumes: `graph/out/edges_question_tag.parquet` from Task 3
- Produces:
  - `build_cooccurrence(edges: DataFrame) -> DataFrame` — columns `src, dst, weight`, canonical `src < dst`
  - `graph/out/edges_tag_tag.parquet`

- [ ] **Step 1: Write the failing tests**

Create `graph/tests/test_projection.py`:

```python
import pytest

from lib.projection import build_cooccurrence


@pytest.fixture
def tag_edges(spark):
    """q1: python+django. q2: python+django+flask. q3: java."""
    return spark.createDataFrame(
        [
            (1, "python"), (1, "django"),
            (2, "python"), (2, "django"), (2, "flask"),
            (3, "java"),
        ],
        ["question_id", "tag"],
    )


def test_pairs_are_canonically_ordered(tag_edges):
    for row in build_cooccurrence(tag_edges).collect():
        assert row["src"] < row["dst"]


def test_no_self_pairs(tag_edges):
    for row in build_cooccurrence(tag_edges).collect():
        assert row["src"] != row["dst"]


def test_each_pair_appears_once(tag_edges):
    result = build_cooccurrence(tag_edges)
    assert result.count() == result.select("src", "dst").distinct().count()


def test_weights_count_shared_questions(tag_edges):
    result = {(r["src"], r["dst"]): r["weight"]
              for r in build_cooccurrence(tag_edges).collect()}
    assert result[("django", "python")] == 2   # q1 and q2
    assert result[("django", "flask")] == 1    # q2 only
    assert result[("flask", "python")] == 1    # q2 only


def test_single_tag_question_produces_no_edges(tag_edges):
    result = {(r["src"], r["dst"]) for r in build_cooccurrence(tag_edges).collect()}
    assert not any("java" in pair for pair in result)


def test_pair_count_is_n_choose_2_per_question(tag_edges):
    # q1 -> 1 pair, q2 -> 3 pairs, q3 -> 0 pairs, with (django, python)
    # shared between q1 and q2, giving 3 distinct pairs.
    assert build_cooccurrence(tag_edges).count() == 3
```

- [ ] **Step 2: Run tests to verify they fail**

```powershell
cd graph
..\.venv\Scripts\python.exe -m pytest tests/test_projection.py -v
```

Expected: `ModuleNotFoundError: No module named 'lib.projection'`.

- [ ] **Step 3: Write the implementation**

Create `graph/lib/projection.py`:

```python
from pyspark.sql import DataFrame, functions as F


def build_cooccurrence(edges: DataFrame) -> DataFrame:
    """Self-join question-tag edges into a weighted tag-tag graph.

    Keeps only src < dst, so each undirected edge is stored exactly once.
    Algorithms needing directed edges symmetrize at point of use.

    This join is free of skew because Stack Overflow caps questions at 5 tags,
    so every join key has at most 5 rows and emits at most 10 pairs.
    """
    left = edges.select("question_id", F.col("tag").alias("src"))
    right = edges.select("question_id", F.col("tag").alias("dst"))
    return (
        left.join(right, on="question_id")
        .filter(F.col("src") < F.col("dst"))
        .groupBy("src", "dst")
        .agg(F.count(F.lit(1)).alias("weight"))
    )
```

- [ ] **Step 4: Run tests to verify they pass**

```powershell
cd graph
..\.venv\Scripts\python.exe -m pytest tests/test_projection.py -v
```

Expected: 6 passed.

- [ ] **Step 5: Write the runner**

Create `graph/stage2_projection.py`:

```python
"""Stage 2: question-tag edges -> weighted tag-tag co-occurrence graph."""
import argparse
import sys
from pathlib import Path

from lib.projection import build_cooccurrence
from lib.session import build_session


def log(msg):
    print(f"[stage2] {msg}", flush=True)


def main():
    parser = argparse.ArgumentParser(description="Stage 2: tag co-occurrence")
    parser.add_argument("--out-dir", default="out")
    args = parser.parse_args()

    out = Path(args.out_dir)
    edges_path = out / "edges_question_tag.parquet"
    if not edges_path.exists():
        print(f"[stage2] ERROR: {edges_path} not found. Run stage1_etl.py first.",
              file=sys.stderr)
        sys.exit(1)

    spark = build_session("stage2-projection")

    edges = spark.read.parquet(str(edges_path))
    cooc = build_cooccurrence(edges)

    log("writing edges_tag_tag...")
    cooc.write.mode("overwrite").parquet(str(out / "edges_tag_tag.parquet"))

    written = spark.read.parquet(str(out / "edges_tag_tag.parquet"))
    log(f"tag-tag edges (weight >= 1): {written.count()}")
    log(f"tag-tag edges (weight >= 5): {written.filter('weight >= 5').count()}")

    spark.stop()


if __name__ == "__main__":
    main()
```

- [ ] **Step 6: Run and verify against the reference figures**

```powershell
cd graph
..\.venv\Scripts\python.exe stage2_projection.py
```

Expected:

```
[stage2] tag-tag edges (weight >= 1): 1469065
[stage2] tag-tag edges (weight >= 5): 215968
```

Both must match exactly. A higher weight-≥-1 count usually means the `src < dst` filter is wrong and pairs are being double-counted.

---

### Task 5: Stage 3 transforms — thresholding, symmetrization, vertices

**Files:**
- Create: `graph/lib/algorithms.py`
- Create: `graph/tests/test_algorithms.py`

**Interfaces:**
- Consumes: `graph/out/edges_tag_tag.parquet` from Task 4
- Produces:
  - `threshold_edges(edges: DataFrame, min_weight: int) -> DataFrame`
  - `symmetrize(edges: DataFrame) -> DataFrame` — columns `src, dst, weight`
  - `tag_vertices(edges: DataFrame) -> DataFrame` — single column `id`
  - `bipartite_graph_frames(edges: DataFrame) -> tuple[DataFrame, DataFrame]` — `(vertices, edges)` with `q:`/`t:` prefixed ids
  - `join_tag_metrics(pagerank, communities, components, triangles, attributes) -> DataFrame` — one row per tag

- [ ] **Step 1: Write the failing tests**

Create `graph/tests/test_algorithms.py`:

```python
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
```

- [ ] **Step 2: Run tests to verify they fail**

```powershell
cd graph
..\.venv\Scripts\python.exe -m pytest tests/test_algorithms.py -v
```

Expected: `ModuleNotFoundError: No module named 'lib.algorithms'`.

- [ ] **Step 3: Write the implementation**

Create `graph/lib/algorithms.py`:

```python
from pyspark.sql import DataFrame, functions as F

PRIMARY_THRESHOLD = 5
SWEEP_THRESHOLDS = (1, 5, 20)


def threshold_edges(edges: DataFrame, min_weight: int) -> DataFrame:
    """Drop low-weight co-occurrence.

    GraphFrames' pageRank and labelPropagation ignore edge weights, so
    weight-1 accidental co-occurrence would count equally with genuine
    coupling. Thresholding is how weight influences the result at all, and it
    gates all four projection algorithms - not PageRank alone.
    """
    return edges.filter(F.col("weight") >= min_weight)


def symmetrize(edges: DataFrame) -> DataFrame:
    """Add the reverse of every edge.

    Stage 2 stores each undirected edge once as src < dst. Connected
    Components and Label Propagation treat edges as undirected internally,
    but PageRank is genuinely directional and produces meaningless scores on
    a half-stored graph.
    """
    return edges.select("src", "dst", "weight").union(
        edges.select(
            F.col("dst").alias("src"),
            F.col("src").alias("dst"),
            F.col("weight"),
        )
    )


def tag_vertices(edges: DataFrame) -> DataFrame:
    """Distinct vertex ids implied by a tag-tag edge list."""
    return (
        edges.select(F.col("src").alias("id"))
        .union(edges.select(F.col("dst").alias("id")))
        .distinct()
    )


def bipartite_graph_frames(edges: DataFrame) -> tuple[DataFrame, DataFrame]:
    """Build GraphFrames vertices/edges for the question-tag bipartite graph.

    Prefixes keep the two node types distinct in one id space.
    """
    bip_edges = edges.select(
        F.concat(F.lit("q:"), F.col("question_id").cast("string")).alias("src"),
        F.concat(F.lit("t:"), F.col("tag")).alias("dst"),
    )
    vertices = (
        bip_edges.select(F.col("src").alias("id"))
        .union(bip_edges.select(F.col("dst").alias("id")))
        .distinct()
    )
    return vertices, bip_edges


def join_tag_metrics(pagerank, communities, components, triangles, attributes):
    """Fold the four per-tag algorithm outputs plus attributes into one row per tag.

    Outer joins between the algorithm outputs: a tag present in one must not be
    silently dropped. Left join for attributes, which are supplementary - a tag
    surviving the weight threshold keeps its metrics even if attributes are
    somehow absent.
    """
    return (
        pagerank
        .join(communities, on="tag", how="outer")
        .join(components, on="tag", how="outer")
        .join(triangles, on="tag", how="outer")
        .join(attributes, on="tag", how="left")
    )
```

- [ ] **Step 4: Run tests to verify they pass**

```powershell
cd graph
..\.venv\Scripts\python.exe -m pytest tests/test_algorithms.py -v
```

Expected: 12 passed.

- [ ] **Step 5: Run the whole suite**

```powershell
cd graph
..\.venv\Scripts\python.exe -m pytest tests/ -v
```

Expected: 26 passed (8 ETL + 6 projection + 12 algorithms).

---

### Task 6: Stage 3 runner — projection algorithms and tag_metrics

**Files:**
- Create: `graph/stage3_algorithms.py`

**Interfaces:**
- Consumes: `threshold_edges`, `symmetrize`, `tag_vertices`, `join_tag_metrics`, `PRIMARY_THRESHOLD`, `SWEEP_THRESHOLDS` from Task 5; `graph/out/edges_tag_tag.parquet` and `graph/out/nodes_tag.parquet`
- Produces: `graph/out/results/tag_metrics.parquet`, `graph/out/results/tag_metrics_sweep.parquet`

- [ ] **Step 1: Write the runner**

Create `graph/stage3_algorithms.py`:

```python
"""Stage 3: run GraphFrames algorithms over the tag projection.

PageRank here ranks TECHNOLOGIES, not users. The synopsis frames PageRank as
an expert-ranking method; that version needs OwnerUserId, which the current
dataset does not carry. Column names say "authority", never "expert".
"""
import argparse
import sys
from pathlib import Path

from graphframes import GraphFrame
from pyspark.sql import functions as F

from lib.algorithms import (
    PRIMARY_THRESHOLD,
    SWEEP_THRESHOLDS,
    join_tag_metrics,
    symmetrize,
    tag_vertices,
    threshold_edges,
)
from lib.session import build_session

LPA_MAX_ITER = 10
PAGERANK_MAX_ITER = 20
PAGERANK_RESET = 0.15


def log(msg):
    print(f"[stage3] {msg}", flush=True)


def run_projection_algorithms(edges, min_weight, attributes):
    """Run all four algorithms at one threshold. Returns one row per tag."""
    filtered = threshold_edges(edges, min_weight).cache()
    vertices = tag_vertices(filtered)

    undirected = GraphFrame(vertices, filtered)
    lpa = undirected.labelPropagation(maxIter=LPA_MAX_ITER) \
        .select(F.col("id").alias("tag"), F.col("label").alias("community"))
    cc = undirected.connectedComponents() \
        .select(F.col("id").alias("tag"), F.col("component"))
    tri = undirected.triangleCount() \
        .select(F.col("id").alias("tag"), F.col("count").alias("triangle_count"))

    # PageRank is directional and needs both edge directions.
    directed = GraphFrame(vertices, symmetrize(filtered))
    pr = directed.pageRank(
        resetProbability=PAGERANK_RESET, maxIter=PAGERANK_MAX_ITER
    ).vertices.select(F.col("id").alias("tag"),
                      F.col("pagerank").alias("tag_authority"))

    return join_tag_metrics(pr, lpa, cc, tri, attributes)


def main():
    parser = argparse.ArgumentParser(description="Stage 3: graph algorithms")
    parser.add_argument("--out-dir", default="out")
    parser.add_argument("--skip-sweep", action="store_true",
                        help="Run only the primary threshold")
    args = parser.parse_args()

    out = Path(args.out_dir)
    edges_path = out / "edges_tag_tag.parquet"
    tags_path = out / "nodes_tag.parquet"
    for p in (edges_path, tags_path):
        if not p.exists():
            print(f"[stage3] ERROR: {p} not found. Run stage1 and stage2 first.",
                  file=sys.stderr)
            sys.exit(1)

    results_dir = out / "results"
    results_dir.mkdir(parents=True, exist_ok=True)

    spark = build_session("stage3-algorithms")
    edges = spark.read.parquet(str(edges_path)).cache()
    attrs = spark.read.parquet(str(tags_path))

    log(f"primary threshold: weight >= {PRIMARY_THRESHOLD}")
    metrics = run_projection_algorithms(edges, PRIMARY_THRESHOLD, attrs)
    metrics.write.mode("overwrite").parquet(str(results_dir / "tag_metrics.parquet"))
    log(f"tag_metrics rows: {metrics.count()}")

    if not args.skip_sweep:
        frames = []
        for threshold in SWEEP_THRESHOLDS:
            log(f"sweep threshold: weight >= {threshold}")
            df = run_projection_algorithms(edges, threshold, attrs)
            long = df.select(
                F.lit(threshold).alias("threshold"),
                "tag",
                F.col("tag_authority").cast("double").alias("tag_authority"),
                F.col("triangle_count").cast("long").alias("triangle_count"),
                F.col("community").cast("long").alias("community"),
            )
            frames.append(long)

        sweep = frames[0]
        for extra in frames[1:]:
            sweep = sweep.unionByName(extra)
        sweep.write.mode("overwrite").parquet(
            str(results_dir / "tag_metrics_sweep.parquet"))
        log("sweep written")

    spark.stop()


if __name__ == "__main__":
    main()
```

- [ ] **Step 2: Run the primary threshold only, first**

The sweep at threshold 1 runs Triangle Count over 1.47M edges and is the slowest path. Validate the primary result before paying for it:

```powershell
cd graph
..\.venv\Scripts\python.exe stage3_algorithms.py --skip-sweep
```

Expected: `[stage3] tag_metrics rows:` a number at or below 49,372. It will be *lower* than 49,372 — thresholding at weight ≥ 5 drops tags whose every edge is weak, and those tags leave the graph entirely. That is correct behaviour, not a bug.

- [ ] **Step 3: Sanity-check the results**

```powershell
cd graph
..\.venv\Scripts\python.exe -c "import pandas as pd, glob; df=pd.read_parquet('out/results/tag_metrics.parquet'); print(df.nlargest(15,'tag_authority')[['tag','tag_authority','question_count','triangle_count']]); print('communities:', df['community'].nunique())"
```

Expected: mainstream tags (`python`, `javascript`, `java`, `c#`, `android`) dominate the top of `tag_authority`, and community count is in the tens-to-low-hundreds rather than 1 or 10,000. A single community means the threshold is too low and everything is connected; thousands of singletons means it is too high.

- [ ] **Step 4: Run the full sweep**

```powershell
cd graph
..\.venv\Scripts\python.exe stage3_algorithms.py
```

Expected: three `sweep threshold` lines then `sweep written`. If threshold 1 exhausts memory, lower `spark.sql.shuffle.partitions` is the wrong lever — raise driver memory in `build_session` instead, or drop 1 from `SWEEP_THRESHOLDS` and record the omission.

---

### Task 7: Stage 3 — bipartite connected components

Kept separate from Task 6 because it runs on a 7.78M-edge graph and is the one step likely to need memory tuning. A reviewer could reasonably accept Task 6 and reject this.

**Files:**
- Modify: `graph/stage3_algorithms.py`

**Interfaces:**
- Consumes: `bipartite_graph_frames` from Task 5; `graph/out/edges_question_tag.parquet`
- Produces: `graph/out/results/bipartite_components.parquet` — columns `component, size, tag_count, question_count`

- [ ] **Step 1: Add the bipartite function**

Add to `graph/stage3_algorithms.py`, above `main()`:

```python
def run_bipartite_components(spark, edges_path):
    """Connected Components over the question-tag bipartite graph.

    Triangle Count is deliberately NOT run here. Bipartite graphs contain no
    odd-length cycles, so their triangle count is identically zero - a
    definition, not a defect.
    """
    from lib.algorithms import bipartite_graph_frames

    edges = spark.read.parquet(str(edges_path))
    vertices, bip_edges = bipartite_graph_frames(edges)
    graph = GraphFrame(vertices, bip_edges)

    components = graph.connectedComponents()
    return (
        components.groupBy("component")
        .agg(
            F.count(F.lit(1)).alias("size"),
            F.sum(F.when(F.col("id").startswith("t:"), 1).otherwise(0))
             .alias("tag_count"),
            F.sum(F.when(F.col("id").startswith("q:"), 1).otherwise(0))
             .alias("question_count"),
        )
        .orderBy(F.col("size").desc())
    )
```

- [ ] **Step 2: Wire it into main()**

Add the CLI flag alongside the existing arguments in `main()`:

```python
    parser.add_argument("--bipartite", action="store_true",
                        help="Also run Connected Components on the bipartite graph")
```

And insert this block immediately before `spark.stop()`:

```python
    if args.bipartite:
        log("bipartite connected components (7.78M edges, this is the slow one)...")
        comps = run_bipartite_components(spark, out / "edges_question_tag.parquet")
        comps.write.mode("overwrite").parquet(
            str(results_dir / "bipartite_components.parquet"))
        written = spark.read.parquet(str(results_dir / "bipartite_components.parquet"))
        log(f"components: {written.count()}")
        log(f"largest component size: {written.agg(F.max('size')).collect()[0][0]}")
```

- [ ] **Step 3: Run it**

```powershell
cd graph
..\.venv\Scripts\python.exe stage3_algorithms.py --skip-sweep --bipartite
```

Expected: a giant component holding the large majority of the 2,581,886 vertices, plus a tail of small isolated components. That giant component dominating is the finding, not a failure — it says the technology ecosystem is overwhelmingly interconnected, and the small components are the genuinely isolated niches worth reporting.

If this fails on memory, raise `driver_memory` in the `build_session` call inside `main()` to `12g` and re-run. Do not lower `spark.sql.shuffle.partitions` — connected components needs the parallelism.

---

### Task 8: Stage 4 — Streamlit dashboard

**Files:**
- Create: `graph/dashboard/app.py`

**Interfaces:**
- Consumes: `graph/out/results/tag_metrics.parquet`, `graph/out/edges_tag_tag.parquet`
- Produces: a running Streamlit app. No Spark.

- [ ] **Step 1: Write the app**

Create `graph/dashboard/app.py`:

```python
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

tab1, tab2, tab3, tab4 = st.tabs(
    ["Tag authority", "Communities", "Co-occurrence", "Network"]
)

with tab1:
    st.subheader("Authority vs. raw popularity")
    df = metrics.dropna(subset=["tag_authority", "question_count"]).copy()
    df["authority_rank"] = df["tag_authority"].rank(ascending=False).astype(int)
    df["volume_rank"] = df["question_count"].rank(ascending=False).astype(int)
    df["rank_delta"] = df["volume_rank"] - df["authority_rank"]

    st.markdown("**Punching above their volume** - structurally central "
                "relative to how many questions they carry:")
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

    matrix = sub.pivot(index="src", columns="dst", values="weight").fillna(0)
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
    st.dataframe(
        sub.nlargest(50, "weight").merge(
            metrics[["tag", "community"]], left_on="src", right_on="tag", how="left"
        )[["src", "dst", "weight", "community"]],
        use_container_width=True,
    )
```

- [ ] **Step 2: Run the dashboard**

```powershell
cd graph
..\.venv\Scripts\streamlit.exe run dashboard\app.py
```

Expected: opens on `http://localhost:8501`, loads in about a second, all four tabs render without error.

- [ ] **Step 3: Verify each tab**

Click through all four. Confirm: the authority tab's "punching above their volume" table contains plausible connector technologies; the community selector changes the member list; the heatmap renders a symmetric-looking block structure; the network tab's edge count responds to both sliders.

---

### Task 9: Git hygiene and commits

> **STATUS: COMPLETE (2026-10-03).** At the user's request, this ran before Tasks 10–14. It made one commit on `feat/tag-graph-analytics` tracking everything to date except `CLAUDE.md`, which stays untracked by choice. The steps below record what was actually run. Beyond the original `docs/` and `graph/`, the commit also tracks `Synopsis.md`, the `dataset/` source (its data folders are ignored) and `submissions/`, and removes the placeholder `initial.md`. From Task 10 on, each task commits its own files.

**Files:**
- Create: `.gitignore`

- [ ] **Step 1: Write the .gitignore**

The repo currently has none, so `dataset/` — roughly 41 GB of archives, Parquet, and images — is untracked but not ignored. A stray `git add .` would try to stage all of it.

Create `.gitignore` at the repo root:

```gitignore
# Data - never commit, tens of GB
dataset/data/
dataset/Datasets_1st run/
dataset/Datasets_2nd run/
graph/out/

# Python
.venv/
__pycache__/
*.pyc
.pytest_cache/

# Spark
spark-warehouse/
metastore_db/
derby.log

# IDE
.vscode/
```

- [ ] **Step 2: Verify nothing large is staged**

```bash
git add -A --dry-run | head -50
```

Expected: only source files, docs, and `.gitignore`. If any path under `dataset/data/`, `Datasets_*`, or `graph/out/` appears, fix `.gitignore` before going further.

- [ ] **Step 3: Branch and commit**

```bash
git checkout -b feat/tag-graph-analytics
git add .gitignore Synopsis.md docs/ graph/ dataset/ submissions/ initial.md
git commit -m "feat: track tag graph pipeline, preprocessing source and docs

Staged PySpark + GraphFrames pipeline over the existing preprocessed
Stack Overflow Parquet: question-tag edge list, tag co-occurrence
projection, Label Propagation / Connected Components / Triangle Count /
PageRank, and a Streamlit dashboard (stages 0-4, 25 tests).

Also tracks the dataset/ preprocessing source, the design spec and plan
(Tasks 10-14, Neo4j and the Spark vs NetworkX benchmark, still to do),
the synopsis and the coursework deliverables. .gitignore keeps ~41 GB
of dump archives, Parquet and graph/out artifacts out. Removes the
placeholder initial.md. CLAUDE.md stays untracked by choice.

PageRank ranks technologies rather than users - the user graph is
blocked on OwnerUserId, which the current dataset does not carry.

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

- [ ] **Step 4: Confirm nothing from the pipeline is left over**

```bash
git status --short
```

Expected: exactly one line, `?? CLAUDE.md`, which is untracked by choice. Never stage it. Nothing under `dataset/data/`, `Datasets_*`, `graph/out/` or `.venv/` may appear.

Later tasks write `graph/out/neo4j_import/` and `graph/out/bench/`. Both are covered by the `graph/out/` ignore rule. The Neo4j install lives in `C:\neo4j\`, outside the repo.

---

### Task 10: Stage 5a — Neo4j import CSVs

Spec §11. This task needs only pandas: no Spark and no Neo4j. It is therefore fully testable on its own, and a reviewer can accept the export without the database existing yet.

**Files:**
- Create: `graph/lib/neo4j_export.py`
- Create: `graph/stage5_neo4j_export.py`
- Test: `graph/tests/test_neo4j_export.py`

**Interfaces:**
- Consumes: `out/nodes_tag.parquet`, `out/results/tag_metrics.parquet`, `out/nodes_question.parquet`, `out/edges_question_tag.parquet`, `out/edges_tag_tag.parquet`
- Produces, in `lib/neo4j_export.py`:
  - `QUESTION_HEADER: dict`
  - `tag_nodes(tags: pd.DataFrame, metrics: pd.DataFrame) -> pd.DataFrame`
  - `question_nodes(questions: pd.DataFrame) -> pd.DataFrame`
  - `tagged_with_rels(edges: pd.DataFrame) -> pd.DataFrame`
  - `co_occurs_rels(edges: pd.DataFrame) -> pd.DataFrame`
  - `to_import_csv(frame: pd.DataFrame, path) -> int` (returns rows written)
- Produces, on disk: `out/neo4j_import/{tags,questions,tagged_with,co_occurs}.csv`, each one file with a single header line

- [ ] **Step 1: Write the failing tests**

Create `graph/tests/test_neo4j_export.py`:

```python
import pandas as pd
import pytest

from lib.neo4j_export import (
    co_occurs_rels,
    question_nodes,
    tag_nodes,
    tagged_with_rels,
    to_import_csv,
)


@pytest.fixture
def tags():
    return pd.DataFrame({
        "tag": ["python", "c#", "rare-tag"],
        "question_count": [100, 50, 2],
        "mean_question_score": [1.5, 2.0, 0.0],
        "mean_answers_per_question": [2.0, 1.5, 1.0],
        "mean_answer_score": [3.0, 2.5, 0.5],
    })


@pytest.fixture
def metrics():
    """rare-tag fell below the weight threshold, so it has no metrics row.

    The real tag_metrics also carries the attribute columns; question_count
    rides along here to prove the export does not pick up a duplicate.
    """
    return pd.DataFrame({
        "tag": ["python", "c#"],
        "tag_authority": [340.5, 323.7],
        "community": [180388626595, 180388626595],
        "component": [0, 0],
        "triangle_count": [9000, 7000],
        "question_count": [100, 50],
    })


def csv_lines(frame, tmp_path):
    path = tmp_path / "out.csv"
    to_import_csv(frame, path)
    return path.read_text(encoding="utf-8").splitlines()


def test_tag_nodes_keep_tags_without_metrics(tags, metrics):
    nodes = tag_nodes(tags, metrics)
    assert len(nodes) == 3
    assert set(nodes["name:ID(Tag)"]) == {"python", "c#", "rare-tag"}


def test_tag_header_is_importer_syntax(tags, metrics, tmp_path):
    header = csv_lines(tag_nodes(tags, metrics), tmp_path)[0]
    assert header == (
        "name:ID(Tag),question_count:long,mean_question_score:double,"
        "mean_answers_per_question:double,mean_answer_score:double,"
        "tag_authority:double,community:long,component:long,triangle_count:long"
    )


def test_large_community_labels_are_written_as_integers(tags, metrics, tmp_path):
    lines = csv_lines(tag_nodes(tags, metrics), tmp_path)
    python_row = next(line for line in lines if line.startswith("python,"))
    assert ",180388626595,0,9000" in python_row
    assert "180388626595.0" not in python_row


def test_missing_metrics_are_empty_fields_not_nan(tags, metrics, tmp_path):
    lines = csv_lines(tag_nodes(tags, metrics), tmp_path)
    rare_row = next(line for line in lines if line.startswith("rare-tag,"))
    assert rare_row.endswith(",,,,")
    assert "nan" not in rare_row.lower()


def test_tag_names_with_csv_sensitive_characters_survive(tags, metrics, tmp_path):
    path = tmp_path / "tags.csv"
    to_import_csv(tag_nodes(tags, metrics), path)
    assert "c#" in set(pd.read_csv(path)["name:ID(Tag)"])


def test_question_nodes_store_id_as_long_property():
    questions = pd.DataFrame({
        "question_id": [11, 12], "score": [5, -1], "answer_count": [3, 1],
        "mean_answer_score": [2.0, 0.0], "tag_count": [2, 1],
    })
    nodes = question_nodes(questions)
    assert list(nodes.columns) == [
        ":ID(Question)", "question_id:long", "score:long",
        "answer_count:long", "mean_answer_score:double", "tag_count:long",
    ]
    assert nodes[":ID(Question)"].tolist() == [11, 12]
    assert nodes["question_id:long"].tolist() == [11, 12]


def test_relationship_headers_reference_the_right_id_spaces():
    tagged = tagged_with_rels(pd.DataFrame({"question_id": [11], "tag": ["python"]}))
    assert list(tagged.columns) == [":START_ID(Question)", ":END_ID(Tag)"]
    cooc = co_occurs_rels(
        pd.DataFrame({"src": ["django"], "dst": ["python"], "weight": [20]})
    )
    assert list(cooc.columns) == [":START_ID(Tag)", ":END_ID(Tag)", "weight:long"]


def test_co_occurs_is_not_symmetrized():
    edges = pd.DataFrame({
        "src": ["django", "flask"], "dst": ["python", "python"], "weight": [20, 7],
    })
    assert len(co_occurs_rels(edges)) == 2
```

- [ ] **Step 2: Run the tests and confirm they fail**

```powershell
cd graph
..\.venv\Scripts\python.exe -m pytest tests/test_neo4j_export.py -v
```

Expected: collection error, `ModuleNotFoundError: No module named 'lib.neo4j_export'`.

- [ ] **Step 3: Implement the export module**

Create `graph/lib/neo4j_export.py`:

```python
"""Shape graph artifacts into CSVs for `neo4j-admin database import full`.

pandas, not Spark: this is a format conversion over artifacts that already
exist. Spark writes a directory of part files, each with its own header line,
and the importer reads every line after the first as data.

Column names ARE the importer's header syntax: `name:ID(Tag)` declares the id
column of the Tag id space, `:START_ID(Question)` a relationship's start node,
`weight:long` a typed property.
"""
import pandas as pd

METRIC_COLUMNS = ["tag_authority", "community", "component", "triangle_count"]

# Only the 25,597 tags that survive weight >= 5 have metrics, so these columns
# carry nulls. As float64 they print 180388626595.0, which the importer
# rejects for a long property - and LPA labels reach ~4e11. Int64 is pandas'
# nullable integer: whole numbers print bare, nulls print as empty fields.
NULLABLE_INT_METRICS = ["community", "component", "triangle_count"]

TAG_HEADER = {
    "tag": "name:ID(Tag)",
    "question_count": "question_count:long",
    "mean_question_score": "mean_question_score:double",
    "mean_answers_per_question": "mean_answers_per_question:double",
    "mean_answer_score": "mean_answer_score:double",
    "tag_authority": "tag_authority:double",
    "community": "community:long",
    "component": "component:long",
    "triangle_count": "triangle_count:long",
}

QUESTION_HEADER = {
    "question_id": "question_id:long",
    "score": "score:long",
    "answer_count": "answer_count:long",
    "mean_answer_score": "mean_answer_score:double",
    "tag_count": "tag_count:long",
}


def tag_nodes(tags: pd.DataFrame, metrics: pd.DataFrame) -> pd.DataFrame:
    """Every tag, with the primary-threshold metrics where it has them."""
    merged = tags.merge(metrics[["tag", *METRIC_COLUMNS]], on="tag", how="left")
    for column in NULLABLE_INT_METRICS:
        merged[column] = merged[column].astype("Int64")
    return merged[list(TAG_HEADER)].rename(columns=TAG_HEADER)


def question_nodes(questions: pd.DataFrame) -> pd.DataFrame:
    """Question nodes keyed by an unnamed id column.

    An unnamed :ID column is used to match relationships but not stored. A
    named one would be stored as a string, because tag names force the
    importer's global id type to string - so question_id is stored separately
    as a long.
    """
    nodes = questions[list(QUESTION_HEADER)].rename(columns=QUESTION_HEADER)
    nodes.insert(0, ":ID(Question)", questions["question_id"].to_numpy())
    return nodes


def tagged_with_rels(edges: pd.DataFrame) -> pd.DataFrame:
    return pd.DataFrame({
        ":START_ID(Question)": edges["question_id"].to_numpy(),
        ":END_ID(Tag)": edges["tag"].to_numpy(),
    })


def co_occurs_rels(edges: pd.DataFrame) -> pd.DataFrame:
    """One relationship per pair, src < dst, as stage 2 stores it.

    Cypher matches CO_OCCURS undirected, so reverse edges would only double
    every count and weight sum.
    """
    return pd.DataFrame({
        ":START_ID(Tag)": edges["src"].to_numpy(),
        ":END_ID(Tag)": edges["dst"].to_numpy(),
        "weight:long": edges["weight"].to_numpy(),
    })


def to_import_csv(frame: pd.DataFrame, path) -> int:
    """Write one header line, then data. Nulls become empty fields, which the
    importer leaves as absent properties."""
    frame.to_csv(path, index=False, na_rep="")
    return len(frame)
```

- [ ] **Step 4: Run the tests and confirm they pass**

```powershell
..\.venv\Scripts\python.exe -m pytest tests/test_neo4j_export.py -v
```

Expected: 8 passed, in seconds. There is no Spark fixture in this file.

- [ ] **Step 5: Write the runner**

Create `graph/stage5_neo4j_export.py`:

```python
"""Stage 5a: write the graph as CSVs for Neo4j's bulk importer.

Reads stage 1-3 artifacts with pandas - no Spark session.
"""
import argparse
import sys
from pathlib import Path

import pandas as pd

from lib.neo4j_export import (
    QUESTION_HEADER,
    co_occurs_rels,
    question_nodes,
    tag_nodes,
    tagged_with_rels,
    to_import_csv,
)


def log(msg):
    print(f"[stage5] {msg}", flush=True)


def main():
    parser = argparse.ArgumentParser(description="Stage 5a: Neo4j import CSVs")
    parser.add_argument("--out-dir", default="out")
    args = parser.parse_args()

    out = Path(args.out_dir)
    inputs = {
        "tags": out / "nodes_tag.parquet",
        "metrics": out / "results" / "tag_metrics.parquet",
        "questions": out / "nodes_question.parquet",
        "tagged_with": out / "edges_question_tag.parquet",
        "co_occurs": out / "edges_tag_tag.parquet",
    }
    for path in inputs.values():
        if not path.exists():
            print(f"[stage5] ERROR: {path} not found. Run stages 1-3 first.",
                  file=sys.stderr)
            sys.exit(1)

    import_dir = out / "neo4j_import"
    import_dir.mkdir(parents=True, exist_ok=True)

    nodes = tag_nodes(pd.read_parquet(inputs["tags"]),
                      pd.read_parquet(inputs["metrics"]))
    log(f"tags.csv: {to_import_csv(nodes, import_dir / 'tags.csv'):,} rows")

    # Never read the `tags` list column - it is not a node property.
    questions = pd.read_parquet(inputs["questions"], columns=list(QUESTION_HEADER))
    rows = to_import_csv(question_nodes(questions), import_dir / "questions.csv")
    log(f"questions.csv: {rows:,} rows")
    del questions

    edges = pd.read_parquet(inputs["tagged_with"])
    rows = to_import_csv(tagged_with_rels(edges), import_dir / "tagged_with.csv")
    log(f"tagged_with.csv: {rows:,} rows")
    del edges

    cooc = pd.read_parquet(inputs["co_occurs"])
    rows = to_import_csv(co_occurs_rels(cooc), import_dir / "co_occurs.csv")
    log(f"co_occurs.csv: {rows:,} rows")
    log(f"written to {import_dir.resolve()}")


if __name__ == "__main__":
    main()
```

- [ ] **Step 6: Run it against the real artifacts**

```powershell
..\.venv\Scripts\python.exe stage5_neo4j_export.py
```

Expected, in about a minute:

```
[stage5] tags.csv: 49,372 rows
[stage5] questions.csv: 2,532,514 rows
[stage5] tagged_with.csv: 7,775,852 rows
[stage5] co_occurs.csv: 1,469,065 rows
```

Then confirm that nulls were written as empty fields, not zeros:

```powershell
..\.venv\Scripts\python.exe -c "import pandas as pd; t = pd.read_csv('out/neo4j_import/tags.csv'); print(t['community:long'].isna().sum())"
```

Expected: `23775`. Any other count means the metrics join is wrong. Do not import.

- [ ] **Step 7: Commit**

```bash
git add graph/lib/neo4j_export.py graph/stage5_neo4j_export.py graph/tests/test_neo4j_export.py
git commit -m "feat(stage5): write Neo4j bulk-import CSVs

pandas over existing artifacts: 49,372 tags, 2,532,514 questions,
7,775,852 TAGGED_WITH and 1,469,065 CO_OCCURS. Nullable Int64 keeps
~4e11 LPA labels out of float notation, which the importer rejects.

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 11: Stage 5b — Neo4j install, bulk import, Cypher queries

Spec §11. There are no unit tests here: the runner refuses to query until every count matches the reference figures, and that gate is the test. Most steps are setup outside the repo. The repo gains one dependency and two files.

**Files:**
- Modify: `graph/requirements.txt`
- Create: `graph/lib/cypher.py`
- Create: `graph/stage5_neo4j_queries.py`
- Outside the repo: `C:\neo4j\neo4j-community-5.26.<patch>\`

**Interfaces:**
- Consumes: `out/neo4j_import/*.csv` from Task 10
- Produces:
  - Neo4j's default database `neo4j`, holding the full graph
  - `out/results/neo4j_queries.json`, with keys `shortest_path`, `bridge_tags`, `tagged_both` and `stack_completion`, each `{query, params, rows}`

- [ ] **Step 1: Add and install the driver**

Append to `graph/requirements.txt`:

```
# Stage 5 query runner. Driver 5.x pairs with the 5.26 LTS server.
neo4j>=5.28,<6
```

```powershell
cd graph
..\.venv\Scripts\python.exe -m pip install "neo4j>=5.28,<6"
..\.venv\Scripts\python.exe -c "import neo4j; print(neo4j.__version__)"
```

Expected: `5.28.x`.

- [ ] **Step 2: Install Neo4j Community 5.26 LTS**

1. Open https://neo4j.com/deployment-center/ and choose **Community Edition**, the newest **5.26.x** release, **Windows** zip. Do not take 2025.x, which needs Java 21.
2. Unzip it to `C:\neo4j\`, so that `C:\neo4j\neo4j-community-5.26.<patch>\bin\neo4j.bat` exists.
3. Point the shell at it:

```powershell
$env:NEO4J_HOME = (Get-ChildItem C:\neo4j -Directory -Filter "neo4j-community-5.26*" | Select-Object -Last 1).FullName
if (-not $env:JAVA_HOME) { $env:JAVA_HOME = [Environment]::GetEnvironmentVariable('JAVA_HOME', 'User') }
& "$env:NEO4J_HOME\bin\neo4j.bat" version
```

Expected: `neo4j 5.26.x`. If it reports no Java, the shell predates the JDK install (see CLAUDE.md, "A shell opened before the JDK install"). The `JAVA_HOME` line above recovers it. Every new terminal needs both `$env:` lines again.

- [ ] **Step 3: Cap Neo4j's memory**

The shipped `neo4j.conf` has these settings commented out, so appending them does not create duplicates:

```powershell
Add-Content "$env:NEO4J_HOME\conf\neo4j.conf" @"

# BDA project: about 4 GB total, so a stopped Spark job can restart beside it
server.memory.heap.initial_size=2g
server.memory.heap.max_size=2g
server.memory.pagecache.size=2g
"@
```

- [ ] **Step 4: Set the initial password, before the first start**

```powershell
$env:NEO4J_PASSWORD = "<choose one, at least 8 characters>"
& "$env:NEO4J_HOME\bin\neo4j-admin.bat" dbms set-initial-password $env:NEO4J_PASSWORD
```

This works only before the server has ever started. Keep the password out of the repo. Every terminal that runs Step 9 needs `$env:NEO4J_PASSWORD` set.

- [ ] **Step 5: Bulk import, with the server stopped**

The server has not started yet, which the importer requires. To re-import later, stop the server first. `--overwrite-destination=true` replaces the existing database.

```powershell
$imp = (Resolve-Path out\neo4j_import).Path
& "$env:NEO4J_HOME\bin\neo4j-admin.bat" database import full neo4j `
    --nodes=Tag="$imp\tags.csv" `
    --nodes=Question="$imp\questions.csv" `
    --relationships=TAGGED_WITH="$imp\tagged_with.csv" `
    --relationships=CO_OCCURS="$imp\co_occurs.csv" `
    --overwrite-destination=true
```

Expected: `IMPORT DONE`, with 2,581,886 nodes (49,372 + 2,532,514) and 9,244,917 relationships (7,775,852 + 1,469,065). The importer tolerates up to 1,000 bad rows by default and only logs them to an `import.report` file. A clean import is therefore not proven by `IMPORT DONE`. It is proven by the count gate in Step 9.

- [ ] **Step 6: Start the server in a second terminal**

```powershell
$env:NEO4J_HOME = (Get-ChildItem C:\neo4j -Directory -Filter "neo4j-community-5.26*" | Select-Object -Last 1).FullName
if (-not $env:JAVA_HOME) { $env:JAVA_HOME = [Environment]::GetEnvironmentVariable('JAVA_HOME', 'User') }
& "$env:NEO4J_HOME\bin\neo4j.bat" console
```

Expected: the log ends with `Started.`. If Windows Firewall prompts for Java, allow private networks only. Leave this terminal running.

- [ ] **Step 7: Write the Cypher module**

Create `graph/lib/cypher.py`:

```python
"""Cypher for stage 5. Every query is parameterized: tag names arrive from the
command line and are never formatted into query text.

These connect and rank TECHNOLOGIES. The synopsis's user-centric query -
"users who bridge multiple domains" - stays blocked on OwnerUserId; BRIDGE_TAGS
is its tag-level counterpart.
"""

EXPECTED_COUNTS = {
    "Tag nodes": 49_372,
    "Question nodes": 2_532_514,
    "TAGGED_WITH relationships": 7_775_852,
    "CO_OCCURS relationships": 1_469_065,
    "Tags without metrics": 23_775,
}

COUNT_QUERIES = {
    "Tag nodes": "MATCH (t:Tag) RETURN count(t) AS n",
    "Question nodes": "MATCH (q:Question) RETURN count(q) AS n",
    "TAGGED_WITH relationships": "MATCH ()-[r:TAGGED_WITH]->() RETURN count(r) AS n",
    "CO_OCCURS relationships": "MATCH ()-[r:CO_OCCURS]->() RETURN count(r) AS n",
    # Proves empty CSV fields became absent properties rather than zeros.
    "Tags without metrics": "MATCH (t:Tag) WHERE t.community IS NULL RETURN count(t) AS n",
}

# Backs every {name: $tag} lookup below. The bulk importer creates no indexes.
SCHEMA = [
    "CREATE CONSTRAINT tag_name IF NOT EXISTS FOR (t:Tag) REQUIRE t.name IS UNIQUE",
]

# Objective 4's own example: how a niche connects to the rest of the graph.
SHORTEST_PATH = """
MATCH (a:Tag {name: $source}), (b:Tag {name: $target})
MATCH p = shortestPath((a)-[:CO_OCCURS*..6]-(b))
WHERE all(r IN relationships(p) WHERE r.weight >= $min_weight)
RETURN [n IN nodes(p) | n.name] AS path,
       [n IN nodes(p) | n.community] AS communities,
       [r IN relationships(p) | r.weight] AS weights
"""

BRIDGE_TAGS = """
MATCH (t:Tag)-[r:CO_OCCURS]-(n:Tag)
WHERE r.weight >= $min_weight
  AND t.community IS NOT NULL AND n.community IS NOT NULL
  AND n.community <> t.community
RETURN t.name AS tag, t.community AS community,
       count(DISTINCT n.community) AS other_communities,
       sum(r.weight) AS cross_weight
ORDER BY other_communities DESC, cross_weight DESC
LIMIT $limit
"""

TAGGED_BOTH = """
MATCH (a:Tag {name: $tag_a})<-[:TAGGED_WITH]-(q:Question)-[:TAGGED_WITH]->(b:Tag {name: $tag_b})
RETURN q.question_id AS question_id, q.score AS score, q.answer_count AS answers
ORDER BY score DESC
LIMIT $limit
"""

# Ranked by the weaker of the two links, so a tag tied strongly to only one
# side of the pair does not float to the top.
STACK_COMPLETION = """
MATCH (a:Tag {name: $tag_a})-[r1:CO_OCCURS]-(c:Tag)-[r2:CO_OCCURS]-(b:Tag {name: $tag_b})
WITH c, r1.weight AS with_a, r2.weight AS with_b
RETURN c.name AS tag, with_a, with_b,
       CASE WHEN with_a < with_b THEN with_a ELSE with_b END AS weaker_link
ORDER BY weaker_link DESC
LIMIT $limit
"""
```

- [ ] **Step 8: Write the query runner**

Create `graph/stage5_neo4j_queries.py`:

```python
"""Stage 5b: verify the Neo4j import, then run the objective-4 queries.

Needs a running Neo4j 5.26 server loaded by `neo4j-admin database import
full` (plan Task 11). The password comes from NEO4J_PASSWORD and is never
stored in the repo.
"""
import argparse
import json
import os
import sys
from pathlib import Path

from neo4j import GraphDatabase

from lib.cypher import (
    BRIDGE_TAGS,
    COUNT_QUERIES,
    EXPECTED_COUNTS,
    SCHEMA,
    SHORTEST_PATH,
    STACK_COMPLETION,
    TAGGED_BOTH,
)


def log(msg):
    print(f"[stage5] {msg}", flush=True)


def verify_counts(driver):
    """True only if every count matches - checked before any query runs."""
    ok = True
    for name, query in COUNT_QUERIES.items():
        records, _, _ = driver.execute_query(query)
        actual, expected = records[0]["n"], EXPECTED_COUNTS[name]
        log(f"{name}: {actual:,} (expected {expected:,}) "
            f"{'ok' if actual == expected else 'MISMATCH'}")
        ok = ok and actual == expected
    return ok


def run(driver, name, query, params):
    records, _, _ = driver.execute_query(query, params)
    rows = [record.data() for record in records]
    log(f"--- {name} {params}")
    for row in rows:
        log(f"    {row}")
    if not rows:
        log("    (no rows)")
    return {"query": query.strip(), "params": params, "rows": rows}


def main():
    parser = argparse.ArgumentParser(description="Stage 5b: Neo4j queries")
    parser.add_argument("--out-dir", default="out")
    parser.add_argument("--uri", default=os.environ.get("NEO4J_URI", "bolt://localhost:7687"))
    parser.add_argument("--user", default=os.environ.get("NEO4J_USER", "neo4j"))
    # Defaults sit in different LPA communities: solidity is in the
    # blockchain community, haskell in the giant one.
    parser.add_argument("--source", default="solidity")
    parser.add_argument("--target", default="haskell")
    parser.add_argument("--tag-a", default="python")
    parser.add_argument("--tag-b", default="docker")
    parser.add_argument("--min-weight", type=int, default=5)
    parser.add_argument("--limit", type=int, default=10)
    args = parser.parse_args()

    password = os.environ.get("NEO4J_PASSWORD")
    if not password:
        print("[stage5] ERROR: set NEO4J_PASSWORD to the password given to "
              "`neo4j-admin dbms set-initial-password`.", file=sys.stderr)
        sys.exit(1)

    with GraphDatabase.driver(args.uri, auth=(args.user, password)) as driver:
        driver.verify_connectivity()
        if not verify_counts(driver):
            print("[stage5] ERROR: the import is incomplete. Stop the server, "
                  "re-run stage5_neo4j_export.py and the import, then retry.",
                  file=sys.stderr)
            sys.exit(1)
        for statement in SCHEMA:
            driver.execute_query(statement)
        driver.execute_query("CALL db.awaitIndexes(300)")

        pair = {"tag_a": args.tag_a, "tag_b": args.tag_b, "limit": args.limit}
        results = {
            "shortest_path": run(driver, "shortest path", SHORTEST_PATH, {
                "source": args.source, "target": args.target,
                "min_weight": args.min_weight}),
            "bridge_tags": run(driver, "bridge tags", BRIDGE_TAGS, {
                "min_weight": args.min_weight, "limit": args.limit}),
            "tagged_both": run(driver, "questions tagged with both",
                               TAGGED_BOTH, pair),
            "stack_completion": run(driver, "stack completion",
                                    STACK_COMPLETION, pair),
        }

    path = Path(args.out_dir) / "results" / "neo4j_queries.json"
    path.write_text(json.dumps(results, indent=2), encoding="utf-8")
    log(f"results written to {path}")


if __name__ == "__main__":
    main()
```

- [ ] **Step 9: Run it**

In the first terminal, with `$env:NEO4J_PASSWORD` set:

```powershell
..\.venv\Scripts\python.exe stage5_neo4j_queries.py
```

Expected:
- All five count lines end in `ok`. A single `MISMATCH` stops the run, and that is a failure: re-export and re-import before going on.
- Then four result blocks:
  - **Shortest path:** a non-empty path from `solidity` to `haskell` whose `communities` list changes along the way.
  - **Bridge tags:** a non-empty list.
  - **Questions:** up to 10 questions carrying both `python` and `docker`.
  - **Stack completion:** up to 10 tags.
- `out/results/neo4j_queries.json` is written.

- [ ] **Step 10: Check it interactively in Neo4j Browser**

Open http://localhost:7474 and log in as `neo4j` with your password. Run `:params {source: 'solidity', target: 'haskell', min_weight: 5}`, then paste the `SHORTEST_PATH` query from `lib/cypher.py`. Browser draws the path as a graph. Screenshot it for the report: this is the "interactive exploration through Cypher" that objective 4 promises.

- [ ] **Step 11: Stop the server and commit**

Press Ctrl+C in the server terminal and wait for it to exit. The server must be down before any Spark or benchmark work.

```bash
git add graph/requirements.txt graph/lib/cypher.py graph/stage5_neo4j_queries.py
git commit -m "feat(stage5): Neo4j bulk import verification and Cypher queries

Count gate refuses to query unless all nodes, relationships and the
23,775 metric-less tags match. Four parameterized queries: shortest
path across communities, bridge tags, questions with two tags, and
stack completion. Neo4j 5.26 LTS reuses the existing JDK 17.

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 12: Stage 6 — benchmark plumbing and the Spark worker

Spec §12. The plumbing sits in `lib/bench.py` and imports neither engine, so its tests are plain Python. The Spark worker is checked by a 10% smoke run.

**Files:**
- Modify: `graph/lib/etl.py` (add `sample_questions`)
- Modify: `graph/tests/test_etl.py`
- Create: `graph/lib/bench.py`
- Create: `graph/tests/test_bench.py`
- Create: `graph/bench_spark.py`

**Interfaces:**
- Consumes:
  - `read_posts`, `aggregate_questions`, `explode_question_tags` from `lib/etl.py`
  - `build_cooccurrence` from `lib/projection.py`
  - `bipartite_graph_frames`, `symmetrize`, `tag_vertices` from `lib/algorithms.py`
  - `build_session` from `lib/session.py`
- Produces, in `lib/etl.py`: `sample_questions(posts: DataFrame, pct: int) -> DataFrame`
- Produces, in `lib/bench.py`:
  - constants `SCALES`, `RECORD_FIELDS`, `NX_STEPS`, `EXACT_CHECKS`
  - `append_record(path, record: dict) -> None`
  - `read_records(path) -> list[dict]`
  - `run_step(record, step: str, graph: str, fn) -> bool`, where `record` is a `**fields` callable and `fn` returns a dict of extra fields
  - `mark_unfinished(path, base: dict, graph: str, limit_seconds: float) -> list[str]`
  - `cross_checks(records: list[dict]) -> list[dict]`, whose rows have keys `scale_pct, check, spark, networkx, outcome`
- Produces, from the worker:
  - CLI: `bench_spark.py --scale N --part {build,bipartite,projection} [--repeat R] [--input P] [--bench-dir D]`
  - writes `D/scale_N/{edges_question_tag,edges_tag_tag,pagerank_spark}.parquet`
  - appends to `D/records.jsonl`

- [ ] **Step 1: Write the failing sampling tests**

Add `import pytest` as the first line of `graph/tests/test_etl.py`. Add `sample_questions` to its `from lib.etl import (...)` list, then append:

```python
@pytest.fixture
def many_posts(spark):
    """200 questions with two answer rows each."""
    rows = [(q, ["t"], 1, a) for q in range(200) for a in (1, 2)]
    return spark.createDataFrame(rows, ["question_id", "tags", "score", "answer_score"])


def test_sample_100_keeps_every_row(many_posts):
    assert sample_questions(many_posts, 100).count() == 400


def test_sample_keeps_whole_questions(many_posts):
    counts = sample_questions(many_posts, 25).groupBy("question_id").count().collect()
    assert counts
    assert all(r["count"] == 2 for r in counts)


def test_sample_is_deterministic(many_posts):
    first = {r["question_id"] for r in sample_questions(many_posts, 25).collect()}
    second = {r["question_id"] for r in sample_questions(many_posts, 25).collect()}
    assert first == second


def test_smaller_samples_nest_inside_larger_ones(many_posts):
    small = {r["question_id"] for r in sample_questions(many_posts, 10).collect()}
    large = {r["question_id"] for r in sample_questions(many_posts, 50).collect()}
    assert small <= large
    assert len(small) < len(large)


def test_sample_rejects_out_of_range_percent(many_posts):
    with pytest.raises(ValueError):
        sample_questions(many_posts, 0)
```

- [ ] **Step 2: Run them and confirm they fail**

```powershell
cd graph
..\.venv\Scripts\python.exe -m pytest tests/test_etl.py -v
```

Expected: collection error, `ImportError: cannot import name 'sample_questions'`.

- [ ] **Step 3: Implement sampling**

Append to `graph/lib/etl.py`:

```python
def sample_questions(posts: DataFrame, pct: int) -> DataFrame:
    """Keep pct% of questions, with every answer row of each kept question.

    Hashing question_id instead of calling .sample() makes the subset
    identical across processes and runs, and nests the scales: the 10% subset
    sits inside the 25% one. Sampling rows instead of questions would drop
    some answers of a kept question and understate answer_count.
    """
    if not 0 < pct <= 100:
        raise ValueError(f"pct must be in (0, 100], got {pct}")
    return posts.filter(F.pmod(F.xxhash64("question_id"), F.lit(100)) < pct)
```

- [ ] **Step 4: Run them and confirm they pass**

```powershell
..\.venv\Scripts\python.exe -m pytest tests/test_etl.py -v
```

Expected: 13 passed (8 existing plus 5 new).

- [ ] **Step 5: Write the failing plumbing tests**

Create `graph/tests/test_bench.py`:

```python
import pytest

from lib.bench import (
    NX_STEPS,
    RECORD_FIELDS,
    append_record,
    cross_checks,
    mark_unfinished,
    read_records,
    run_step,
)


def test_append_record_fills_every_field(tmp_path):
    path = tmp_path / "r.jsonl"
    append_record(path, {"engine": "spark", "step": "etl"})
    [rec] = read_records(path)
    assert set(rec) == set(RECORD_FIELDS)
    assert rec["engine"] == "spark"
    assert rec["seconds"] is None


def test_append_record_rejects_unknown_fields(tmp_path):
    with pytest.raises(ValueError):
        append_record(tmp_path / "r.jsonl", {"engin": "spark"})


def test_run_step_records_failure_and_continues(tmp_path):
    path = tmp_path / "r.jsonl"

    def record(**fields):
        append_record(path, {"engine": "networkx", **fields})

    def blow_up():
        raise MemoryError("graph too large")

    assert run_step(record, "load", "bipartite", blow_up) is False
    [rec] = read_records(path)
    assert rec["status"] == "error"
    assert "MemoryError" in rec["error"]
    assert rec["seconds"] is not None


def test_mark_unfinished_blames_the_first_missing_step(tmp_path):
    path = tmp_path / "r.jsonl"
    base = {"engine": "networkx", "scale_pct": 100, "repeat": 1}
    for step in ("load", "connected_components"):
        append_record(path, {**base, "graph": "projection", "step": step, "status": "ok"})

    missing = mark_unfinished(path, base, "projection", limit_seconds=1800)

    assert missing == ["pagerank", "label_propagation", "triangle_count"]
    statuses = {r["step"]: r["status"] for r in read_records(path)}
    assert statuses["pagerank"] == "timeout"
    assert statuses["label_propagation"] == "not_run"
    assert statuses["triangle_count"] == "not_run"


def test_mark_unfinished_ignores_other_scales(tmp_path):
    path = tmp_path / "r.jsonl"
    small = {"engine": "networkx", "scale_pct": 10, "repeat": 1}
    append_record(path, {**small, "graph": "bipartite", "step": "load", "status": "ok"})
    full = {**small, "scale_pct": 100}
    assert mark_unfinished(path, full, "bipartite", limit_seconds=60) == list(NX_STEPS["bipartite"])


def test_cross_checks_flag_mismatch_and_skip_missing():
    def rec(engine, graph, step, **fields):
        return {"engine": engine, "scale_pct": 10, "repeat": 1, "graph": graph,
                "step": step, "status": "ok", **fields}

    records = [
        rec("spark", "bipartite", "connected_components", result=5),
        rec("networkx", "bipartite", "connected_components", result=5),
        rec("spark", "projection", "triangle_count", result=100),
        rec("networkx", "projection", "triangle_count", result=99),
    ]
    outcomes = {row["check"]: row["outcome"] for row in cross_checks(records)}
    assert outcomes["bipartite.connected_components.result"] == "match"
    assert outcomes["projection.triangle_count.result"] == "MISMATCH"
    assert outcomes["projection.load.vertices"] == "unchecked"
```

- [ ] **Step 6: Run them and confirm they fail**

```powershell
..\.venv\Scripts\python.exe -m pytest tests/test_bench.py -v
```

Expected: collection error, `ModuleNotFoundError: No module named 'lib.bench'`.

- [ ] **Step 7: Implement the plumbing**

Create `graph/lib/bench.py`:

```python
"""Benchmark plumbing shared by the orchestrator and both workers.

Imports neither Spark nor NetworkX: the NetworkX worker must not pay for a
PySpark import, and the orchestrator must not hold either engine's memory
while the workers run.
"""
import json
import time
from pathlib import Path

SCALES = (10, 25, 50, 100)

RECORD_FIELDS = (
    "engine", "scale_pct", "repeat", "graph", "step", "status",
    "seconds", "vertices", "edges", "result", "peak_mb", "error",
)

# NetworkX steps per graph, in the order bench_networkx.py runs them. The
# orchestrator uses this to tell which step a killed worker was inside.
NX_STEPS = {
    "bipartite": ("load", "connected_components"),
    "projection": ("load", "connected_components", "pagerank",
                   "label_propagation", "triangle_count"),
}

# (graph, step, field) triples that must be identical when both engines ran
# the same graph. PageRank is compared by rank correlation in the
# orchestrator; LPA is not compared at all (spec §12).
EXACT_CHECKS = (
    ("bipartite", "load", "vertices"),
    ("bipartite", "load", "edges"),
    ("bipartite", "connected_components", "result"),
    ("projection", "load", "vertices"),
    ("projection", "load", "edges"),
    ("projection", "connected_components", "result"),
    ("projection", "triangle_count", "result"),
)


def append_record(path, record):
    """Append one timed step as a JSON line, with every field present."""
    unknown = set(record) - set(RECORD_FIELDS)
    if unknown:
        raise ValueError(f"unknown record fields: {sorted(unknown)}")
    line = json.dumps({field: record.get(field) for field in RECORD_FIELDS})
    with open(path, "a", encoding="utf-8") as f:
        f.write(line + "\n")


def read_records(path):
    """Every record in the log, oldest first. A missing log reads as empty."""
    path = Path(path)
    if not path.exists():
        return []
    with open(path, encoding="utf-8") as f:
        return [json.loads(line) for line in f if line.strip()]


def run_step(record, step, graph, fn):
    """Time fn(), record the outcome, and return True if it succeeded.

    fn returns a dict of extra record fields, e.g. {"result": 119}. A failure
    is recorded rather than raised, so one algorithm blowing up does not hide
    the timings of the ones after it.
    """
    start = time.perf_counter()
    try:
        fields = fn() or {}
        status, error = "ok", None
    except Exception as exc:  # recorded in the results, not swallowed
        fields, status, error = {}, "error", f"{type(exc).__name__}: {exc}"[:500]
    record(step=step, graph=graph, status=status, error=error,
           seconds=round(time.perf_counter() - start, 3), **fields)
    return status == "ok"


def mark_unfinished(path, base, graph, limit_seconds):
    """Record the steps a killed NetworkX worker never reported.

    The first missing step is the one it was inside when the limit hit; the
    rest never started. Returns the missing step names.
    """
    done = {
        r["step"] for r in read_records(path)
        if r["graph"] == graph and all(r[k] == v for k, v in base.items())
    }
    missing = [step for step in NX_STEPS[graph] if step not in done]
    for i, step in enumerate(missing):
        append_record(path, {
            **base, "graph": graph, "step": step,
            "status": "timeout" if i == 0 else "not_run",
            "error": f"killed at the {limit_seconds:.0f}s run limit" if i == 0 else None,
        })
    return missing


def cross_checks(records):
    """Compare the engines per scale on quantities that must match exactly.

    Uses repeat 1 only; later repeats run the same graphs. A check with a
    missing side (timeout, error) is "unchecked", never a mismatch.
    """
    found = {
        (r["engine"], r["scale_pct"], r["graph"], r["step"]): r
        for r in records if r["repeat"] == 1 and r["status"] == "ok"
    }
    rows = []
    for pct in sorted({r["scale_pct"] for r in records}):
        for graph, step, field in EXACT_CHECKS:
            spark = found.get(("spark", pct, graph, step), {}).get(field)
            networkx = found.get(("networkx", pct, graph, step), {}).get(field)
            if spark is None or networkx is None:
                outcome = "unchecked"
            else:
                outcome = "match" if spark == networkx else "MISMATCH"
            rows.append({"scale_pct": pct, "check": f"{graph}.{step}.{field}",
                         "spark": spark, "networkx": networkx, "outcome": outcome})
    return rows
```

- [ ] **Step 8: Run the plumbing tests**

```powershell
..\.venv\Scripts\python.exe -m pytest tests/test_bench.py -v
```

Expected: 6 passed, in under a second.

- [ ] **Step 9: Write the Spark worker**

Create `graph/bench_spark.py`:

```python
"""Benchmark worker: one Spark part at one data scale, in its own process.

Parts, run in this order by stage6_benchmark.py:
  build       sample the input, run the stage 1 and 2 transforms, and write
              this scale's graphs to <bench-dir>/scale_<pct>/
  bipartite   Connected Components on the question-tag graph
  projection  CC, PageRank, Label Propagation and Triangle Count on the tag
              projection at weight >= 1 (unthresholded - see spec §12)

Each part gets a fresh process and SparkSession: successive algorithm suites
in one session exhaust the driver heap (see stage3_algorithms.py).
"""
import argparse
import sys
from pathlib import Path

from graphframes import GraphFrame
from pyspark.sql import functions as F

from lib.algorithms import bipartite_graph_frames, symmetrize, tag_vertices
from lib.bench import append_record, run_step
from lib.etl import (
    aggregate_questions,
    explode_question_tags,
    read_posts,
    sample_questions,
)
from lib.projection import build_cooccurrence
from lib.session import build_session

DEFAULT_INPUT = r"..\dataset\Datasets_2nd run\preprocessed_posts.parquet"
LPA_MAX_ITER = 10
PAGERANK_MAX_ITER = 20
PAGERANK_RESET = 0.15


def run_build(spark, record, args, scale_dir):
    qt_path = str(scale_dir / "edges_question_tag.parquet")
    tt_path = str(scale_dir / "edges_tag_tag.parquet")

    def etl():
        posts = sample_questions(read_posts(spark, args.input), args.scale)
        explode_question_tags(aggregate_questions(posts)) \
            .write.mode("overwrite").parquet(qt_path)
        return {"edges": spark.read.parquet(qt_path).count()}

    def projection_build():
        edges = spark.read.parquet(qt_path)
        build_cooccurrence(edges).write.mode("overwrite").parquet(tt_path)
        return {"edges": spark.read.parquet(tt_path).count()}

    if run_step(record, "etl", "bipartite", etl):
        run_step(record, "projection_build", "projection", projection_build)


def run_bipartite(spark, record, args, scale_dir):
    state = {}

    def load():
        edges = spark.read.parquet(str(scale_dir / "edges_question_tag.parquet"))
        vertices, bip_edges = bipartite_graph_frames(edges)
        vertices, bip_edges = vertices.cache(), bip_edges.cache()
        state["graph"] = GraphFrame(vertices, bip_edges)
        return {"vertices": vertices.count(), "edges": bip_edges.count()}

    def connected_components():
        components = state["graph"].connectedComponents()
        return {"result": components.select("component").distinct().count()}

    if run_step(record, "load", "bipartite", load):
        run_step(record, "connected_components", "bipartite", connected_components)


def run_projection(spark, record, args, scale_dir):
    state = {}

    def load():
        edges = spark.read.parquet(str(scale_dir / "edges_tag_tag.parquet")).cache()
        vertices = tag_vertices(edges).cache()
        # PageRank needs both edge directions (spec §6.2). Building them here
        # charges that cost to load, as on the NetworkX side.
        both_ways = symmetrize(edges).cache()
        both_ways.count()
        state["undirected"] = GraphFrame(vertices, edges)
        state["directed"] = GraphFrame(vertices, both_ways)
        return {"vertices": vertices.count(), "edges": edges.count()}

    def connected_components():
        components = state["undirected"].connectedComponents()
        return {"result": components.select("component").distinct().count()}

    def pagerank():
        ranks = state["directed"].pageRank(
            resetProbability=PAGERANK_RESET, maxIter=PAGERANK_MAX_ITER
        ).vertices
        ranks.select(F.col("id").alias("tag"), "pagerank") \
            .write.mode("overwrite").parquet(str(scale_dir / "pagerank_spark.parquet"))
        return {}

    def label_propagation():
        labels = state["undirected"].labelPropagation(maxIter=LPA_MAX_ITER)
        return {"result": labels.select("label").distinct().count()}

    def triangle_count():
        total = state["undirected"].triangleCount().agg(F.sum("count")).collect()[0][0]
        # Each triangle is counted once at each of its three corners.
        return {"result": (total or 0) // 3}

    if run_step(record, "load", "projection", load):
        run_step(record, "connected_components", "projection", connected_components)
        run_step(record, "pagerank", "projection", pagerank)
        run_step(record, "label_propagation", "projection", label_propagation)
        run_step(record, "triangle_count", "projection", triangle_count)


PARTS = {"build": run_build, "bipartite": run_bipartite, "projection": run_projection}


def main():
    parser = argparse.ArgumentParser(description="Benchmark worker: Spark")
    parser.add_argument("--scale", type=int, required=True, help="Percent of questions")
    parser.add_argument("--part", choices=sorted(PARTS), required=True)
    parser.add_argument("--repeat", type=int, default=1)
    parser.add_argument("--input", default=DEFAULT_INPUT)
    parser.add_argument("--bench-dir", default=str(Path("out") / "bench"))
    args = parser.parse_args()

    if args.part == "build" and not Path(args.input).exists():
        print(f"[bench_spark] ERROR: input not found: {args.input}", file=sys.stderr)
        sys.exit(1)

    bench_dir = Path(args.bench_dir)
    scale_dir = bench_dir / f"scale_{args.scale}"
    scale_dir.mkdir(parents=True, exist_ok=True)
    base = {"engine": "spark", "scale_pct": args.scale, "repeat": args.repeat}

    def record(**fields):
        append_record(bench_dir / "records.jsonl", {**base, **fields})

    # For session_start, `graph` names the part the session was started for.
    state = {}

    def session_start():
        state["spark"] = build_session(f"bench-{args.part}-{args.scale}")
        return {}

    if not run_step(record, "session_start", args.part, session_start):
        sys.exit(1)
    PARTS[args.part](state["spark"], record, args, scale_dir)
    state["spark"].stop()


if __name__ == "__main__":
    main()
```

- [ ] **Step 10: Smoke-test the worker at 10%**

Stop Neo4j first if it is running.

```powershell
$b = "$env:TEMP\bench-smoke"
..\.venv\Scripts\python.exe bench_spark.py --scale 10 --part build --bench-dir $b
..\.venv\Scripts\python.exe bench_spark.py --scale 10 --part bipartite --bench-dir $b
..\.venv\Scripts\python.exe bench_spark.py --scale 10 --part projection --bench-dir $b
Get-Content "$b\records.jsonl"
```

Expected: 12 lines (3 `session_start`, `etl`, `projection_build`, 2 bipartite, 5 projection), every one with `"status": "ok"`. The `etl` edge count should be roughly 10% of 7,775,852. Any `"error"` line carries its exception text; fix it before going on. Then clean up:

```powershell
Remove-Item -Recurse -Force $b
```

- [ ] **Step 11: Commit**

```bash
git add graph/lib/etl.py graph/tests/test_etl.py graph/lib/bench.py graph/tests/test_bench.py graph/bench_spark.py
git commit -m "feat(stage6): benchmark plumbing and Spark worker

Deterministic hashed question sampling (scales nest), JSONL step
records, timeout attribution and exact cross-engine checks. The Spark
worker runs build / bipartite / projection as separate processes so
no session accumulates algorithm lineage.

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 13: Stage 6 — NetworkX worker and the benchmark orchestrator

Spec §12. Depends on Task 12.

**Files:**
- Modify: `graph/requirements.txt`
- Create: `graph/lib/nx_algorithms.py`
- Create: `graph/tests/test_nx_algorithms.py`
- Create: `graph/bench_networkx.py`
- Create: `graph/stage6_benchmark.py`

**Interfaces:**
- Consumes:
  - from Task 12: `lib/bench.py` (`SCALES`, `append_record`, `read_records`, `run_step`, `mark_unfinished`, `cross_checks`) and `bench_spark.py`'s CLI and its `scale_N/` files
  - from Task 5: `tag_vertices` (test only)
- Produces, in `lib/nx_algorithms.py`:
  - `build_bipartite(edges: pd.DataFrame) -> nx.Graph`
  - `build_projection(edges: pd.DataFrame) -> nx.Graph`
  - `component_count(g) -> int`
  - `pagerank_scores(g) -> pd.DataFrame`, with columns `tag, pagerank`
  - `community_count(g) -> int`
  - `triangle_total(g) -> int`
- Produces, on disk:
  - `out/bench/records.jsonl`
  - `out/bench/scale_N/pagerank_networkx.parquet`
  - `out/results/benchmark.csv`
  - `out/results/benchmark_checks.csv`

- [ ] **Step 1: Add and install the dependencies**

Append to `graph/requirements.txt`:

```
# Stage 6 benchmark baseline. networkx 3.7+ requires Python 3.12.
networkx>=3.4,<3.7
scipy>=1.13   # nx.pagerank runs its power iteration on scipy.sparse
psutil>=6.0   # NetworkX peak working-set memory
```

```powershell
cd graph
..\.venv\Scripts\python.exe -m pip install "networkx>=3.4,<3.7" "scipy>=1.13" "psutil>=6.0"
..\.venv\Scripts\python.exe -c "import networkx, scipy, psutil; print(networkx.__version__, scipy.__version__, psutil.__version__)"
```

Expected: networkx 3.6.x. If pip picks 3.7, the venv is not on 3.11; stop and check `..\.venv\Scripts\python.exe --version`.

- [ ] **Step 2: Write the failing tests**

Create `graph/tests/test_nx_algorithms.py`:

```python
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
```

- [ ] **Step 3: Run them and confirm they fail**

```powershell
..\.venv\Scripts\python.exe -m pytest tests/test_nx_algorithms.py -v
```

Expected: collection error, `ModuleNotFoundError: No module named 'lib.nx_algorithms'`.

- [ ] **Step 4: Implement the NetworkX baseline**

Create `graph/lib/nx_algorithms.py`:

```python
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
```

- [ ] **Step 5: Run the tests and confirm they pass**

```powershell
..\.venv\Scripts\python.exe -m pytest tests/test_nx_algorithms.py -v
```

Expected: 5 passed. The Spark fixture adds about a minute of startup.

- [ ] **Step 6: Write the NetworkX worker**

Create `graph/bench_networkx.py`:

```python
"""Benchmark worker: NetworkX on one graph at one data scale.

Reads the edge lists the Spark worker wrote for this scale, so both engines
run on identical graphs. stage6_benchmark.py runs it in its own process under
a time limit and never beside a Spark JVM - the two would contend for 15.7 GB
of RAM and corrupt each other's timings.
"""
import argparse
import os
from pathlib import Path

import pandas as pd
import psutil

from lib import nx_algorithms as nxa
from lib.bench import append_record, run_step


def peak_mb():
    """Peak working set so far. psutil reports it on Windows only; elsewhere
    this falls back to current RSS, which understates the peak."""
    info = psutil.Process(os.getpid()).memory_info()
    return round(getattr(info, "peak_wset", info.rss) / 2**20, 1)


def main():
    parser = argparse.ArgumentParser(description="Benchmark worker: NetworkX")
    parser.add_argument("--scale", type=int, required=True, help="Percent of questions")
    parser.add_argument("--graph", choices=("bipartite", "projection"), required=True)
    parser.add_argument("--repeat", type=int, default=1)
    parser.add_argument("--bench-dir", default=str(Path("out") / "bench"))
    args = parser.parse_args()

    bench_dir = Path(args.bench_dir)
    scale_dir = bench_dir / f"scale_{args.scale}"
    base = {"engine": "networkx", "scale_pct": args.scale, "repeat": args.repeat}

    def record(**fields):
        append_record(bench_dir / "records.jsonl",
                      {**base, **fields, "peak_mb": peak_mb()})

    state = {}

    def load(file_name, build):
        def step():
            state["g"] = build(pd.read_parquet(scale_dir / file_name))
            return {"vertices": state["g"].number_of_nodes(),
                    "edges": state["g"].number_of_edges()}
        return step

    def components():
        return {"result": nxa.component_count(state["g"])}

    if args.graph == "bipartite":
        if run_step(record, "load", "bipartite",
                    load("edges_question_tag.parquet", nxa.build_bipartite)):
            run_step(record, "connected_components", "bipartite", components)
        return

    def pagerank():
        nxa.pagerank_scores(state["g"]).to_parquet(
            scale_dir / "pagerank_networkx.parquet", index=False)
        return {}

    # Same order as NX_STEPS["projection"] - mark_unfinished relies on it.
    if run_step(record, "load", "projection",
                load("edges_tag_tag.parquet", nxa.build_projection)):
        run_step(record, "connected_components", "projection", components)
        run_step(record, "pagerank", "projection", pagerank)
        run_step(record, "label_propagation", "projection",
                 lambda: {"result": nxa.community_count(state["g"])})
        run_step(record, "triangle_count", "projection",
                 lambda: {"result": nxa.triangle_total(state["g"])})


if __name__ == "__main__":
    main()
```

- [ ] **Step 7: Write the orchestrator**

Create `graph/stage6_benchmark.py`:

```python
"""Stage 6: Spark vs NetworkX across data scales (synopsis objective 6).

Every (engine, scale, graph) runs in its own process, one at a time:
  spark build -> spark bipartite -> spark projection
  -> networkx bipartite -> networkx projection
NetworkX runs are killed after --nx-timeout-min and recorded as timeouts.
Spark runs have no limit: killing the Python driver would orphan its JVM.

Spark here is local mode on one machine's cores, not a cluster. Report it
that way.
"""
import argparse
import subprocess
import sys
from pathlib import Path

import pandas as pd

from lib.bench import SCALES, cross_checks, mark_unfinished, read_records

HERE = Path(__file__).resolve().parent


def log(msg):
    print(f"[stage6] {msg}", flush=True)


def run_worker(script, args, timeout=None):
    """Run one worker to completion. Returns False if it hit the timeout."""
    cmd = [sys.executable, str(HERE / script), *map(str, args)]
    log(" ".join(cmd[1:]))
    try:
        code = subprocess.run(cmd, cwd=HERE, timeout=timeout, check=False).returncode
    except subprocess.TimeoutExpired:
        return False
    if code != 0:
        log(f"  exited with {code}; its records say which step failed")
    return True


def pagerank_agreement(bench_dir, pct):
    """Spearman correlation of the two engines' PageRank, or None if either
    side did not finish."""
    spark_path = bench_dir / f"scale_{pct}" / "pagerank_spark.parquet"
    nx_path = bench_dir / f"scale_{pct}" / "pagerank_networkx.parquet"
    if not (spark_path.exists() and nx_path.exists()):
        return None
    merged = pd.read_parquet(spark_path).merge(
        pd.read_parquet(nx_path), on="tag", suffixes=("_spark", "_nx"))
    return round(merged["pagerank_spark"].corr(merged["pagerank_nx"],
                                               method="spearman"), 4)


def main():
    parser = argparse.ArgumentParser(description="Stage 6: Spark vs NetworkX")
    parser.add_argument("--scales", type=int, nargs="+", default=list(SCALES),
                        help="Percent of questions, e.g. --scales 10 25")
    parser.add_argument("--repeats", type=int, default=1)
    parser.add_argument("--nx-timeout-min", type=float, default=30)
    parser.add_argument("--input", help="Override bench_spark.py's input Parquet")
    parser.add_argument("--out-dir", default="out")
    parser.add_argument("--append", action="store_true",
                        help="Keep earlier records instead of starting fresh")
    args = parser.parse_args()

    out = Path(args.out_dir).resolve()
    bench_dir = out / "bench"
    bench_dir.mkdir(parents=True, exist_ok=True)
    log_path = bench_dir / "records.jsonl"
    if log_path.exists() and not args.append:
        log_path.unlink()

    limit = args.nx_timeout_min * 60
    input_args = ["--input", Path(args.input).resolve()] if args.input else []

    for repeat in range(1, args.repeats + 1):
        for pct in args.scales:
            log(f"=== repeat {repeat}, scale {pct}% ===")
            common = ["--scale", pct, "--repeat", repeat, "--bench-dir", bench_dir]
            run_worker("bench_spark.py", [*common, "--part", "build", *input_args])
            for part in ("bipartite", "projection"):
                run_worker("bench_spark.py", [*common, "--part", part])
            for graph in ("bipartite", "projection"):
                if not run_worker("bench_networkx.py", [*common, "--graph", graph],
                                  timeout=limit):
                    base = {"engine": "networkx", "scale_pct": pct, "repeat": repeat}
                    missing = mark_unfinished(log_path, base, graph, limit)
                    if missing:
                        log(f"  networkx {graph} hit the {args.nx_timeout_min:g} min "
                            f"limit during {missing[0]}")

    records = read_records(log_path)
    results_dir = out / "results"
    results_dir.mkdir(parents=True, exist_ok=True)
    frame = pd.DataFrame(records)
    frame.to_csv(results_dir / "benchmark.csv", index=False)

    checks = cross_checks(records)
    for pct in sorted(frame["scale_pct"].unique()):
        rho = pagerank_agreement(bench_dir, pct)
        checks.append({"scale_pct": int(pct), "check": "projection.pagerank.spearman",
                       "spark": None, "networkx": None,
                       "outcome": "unchecked" if rho is None else f"spearman {rho}"})
    pd.DataFrame(checks).to_csv(results_dir / "benchmark_checks.csv", index=False)

    ok = frame[frame["status"] == "ok"]
    log("median seconds:\n" + ok.pivot_table(
        index=["graph", "step"], columns=["engine", "scale_pct"],
        values="seconds", aggfunc="median").round(1).to_string())
    for row in checks:
        log(f"{row['scale_pct']:>3}%  {row['check']:<40} {row['outcome']}")
    mismatches = sum(row["outcome"] == "MISMATCH" for row in checks)
    if mismatches:
        log(f"WARNING: {mismatches} cross-engine mismatches - the engines did not "
            "run the same graph, or one of them is wrong. Timings are not "
            "comparable until this is resolved.")


if __name__ == "__main__":
    main()
```

- [ ] **Step 8: Smoke run at 10%**

Stop Neo4j and close anything heavy. Then:

```powershell
..\.venv\Scripts\python.exe stage6_benchmark.py --scales 10
```

Expected, in roughly 10–15 minutes:
- Every exact check reads `match`.
- `projection.pagerank.spearman` is above 0.95.
- `out/results/benchmark.csv` and `benchmark_checks.csv` exist.

A `MISMATCH` here is a bug, not a finding, because both engines read the same files. Use `superpowers:systematic-debugging` before going further.

- [ ] **Step 9: Full run**

```powershell
..\.venv\Scripts\python.exe stage6_benchmark.py
```

Run it in the background and leave the machine alone: other load corrupts the timings. Expect roughly 1.5–3 hours. Spark at 100% takes about 20 minutes across its three processes, and any NetworkX run can take up to its 30-minute limit.

Expected at 100%: the Spark `etl` edges are 7,775,852, the `projection_build` edges are 1,469,065, the bipartite `load` vertices are 2,581,886, the bipartite components number 119, and the projection `load` vertices are 49,260. These are the reference figures, so any difference is a sampling bug at 100%.

NetworkX `timeout` or `error` rows at 50–100% are results, not failures. Report them as such.

- [ ] **Step 10: Run the whole suite**

```powershell
..\.venv\Scripts\python.exe -m pytest tests/ -v
```

Expected: 49 passed (the 25 original, plus 8 from Task 10, 11 from Task 12 and 5 from Task 13).

- [ ] **Step 11: Commit**

```bash
git add graph/requirements.txt graph/lib/nx_algorithms.py graph/tests/test_nx_algorithms.py graph/bench_networkx.py graph/stage6_benchmark.py
git commit -m "feat(stage6): NetworkX baseline and benchmark orchestrator

Same graphs, both engines, one process per run, NetworkX under a time
limit. Exact cross-checks on vertices, edges, component and triangle
counts; PageRank compared by Spearman. Spark is local mode on one
machine and is reported that way.

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 14: Stage 4 addition — dashboard benchmark tab

Spec §12. This is a separate task because a reviewer could accept the benchmark numbers and still reject how they are shown.

**Files:**
- Modify: `graph/dashboard/app.py:36-38` (tab list) and append a new block at the end

**Interfaces:**
- Consumes: `out/results/benchmark.csv` and `out/results/benchmark_checks.csv` from Task 13. If they are absent, the tab shows a notice instead.
- Produces: a fifth tab, "Benchmark". No Spark.

- [ ] **Step 1: Add the tab**

In `graph/dashboard/app.py`, replace:

```python
tab1, tab2, tab3, tab4 = st.tabs(
    ["Tag authority", "Communities", "Co-occurrence", "Network"]
)
```

with:

```python
tab1, tab2, tab3, tab4, tab5 = st.tabs(
    ["Tag authority", "Communities", "Co-occurrence", "Network", "Benchmark"]
)
```

- [ ] **Step 2: Append the tab body**

Append to the end of `graph/dashboard/app.py`. Do not pass `use_container_width`: it is deprecated in Streamlit 1.62, and `width="stretch"` is already the default for both `st.plotly_chart` and `st.dataframe`.

```python
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

        sizes = bench[
            (bench["engine"] == "spark") & (bench["graph"] == graph)
            & (bench["step"] == "load") & (bench["status"] == "ok")
        ].drop_duplicates("scale_pct").sort_values("scale_pct")
        st.markdown("**Graph size at each scale**")
        st.dataframe(sizes[["scale_pct", "vertices", "edges"]], hide_index=True)

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
```

- [ ] **Step 3: Run and check it**

```powershell
..\.venv\Scripts\streamlit.exe run dashboard\app.py
```

On the Benchmark tab, check:
- Both radio choices render a faceted chart with one line per engine.
- The size table shows four scales.
- The cross-engine table matches `out/results/benchmark_checks.csv`.

Tabs 1–4 must be unchanged. If Task 13 has not run yet, the tab shows only the info notice. That is the expected behaviour too.

- [ ] **Step 4: Commit**

```bash
git add graph/dashboard/app.py
git commit -m "feat(dashboard): Spark vs NetworkX benchmark tab

Median seconds by scale per step and engine, graph sizes, unfinished
runs and cross-engine checks. Labels Spark as local mode on one machine.

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

## Notes for the executor

**Resume point: Task 10.** Tasks 1–9 are complete. Task 9 ran early and committed everything to date on `feat/tag-graph-analytics`. Each of Tasks 10–14 ends with its own commit. Before anything else, re-run the Stage 0 smoke test. It takes about 2 minutes and proves the runtime is still intact:

```powershell
cd graph
..\.venv\Scripts\python.exe stage0_smoke.py     # must print STAGE 0 PASSED
```

If that fails, fix it before going further: every stage runs on the same session factory. `CLAUDE.md` is untracked by choice; never stage it.

**Tasks 10–11 (Neo4j) and 12–14 (benchmark) are independent of each other.** Either pair can go first. Within each pair, keep the order.

**If stage 0 fails on GraphFrames**, stop and report before writing any other code. Every later task depends on it, and the fallback (hand-rolled DataFrame implementations of the four algorithms) is a spec-level change, not something to improvise.

**Counts are the primary correctness signal.** Stages 1 and 2 have exact expected row counts drawn from direct measurement of the real artifact. A mismatch means a filter or join is wrong. Do not proceed past a mismatch.

**Do not widen the input column projection.** Reading the text columns turns a few-hundred-MB read into a 6.93 GB one and will likely exhaust the 15.7 GB of RAM.

**Deferred to future work** (recorded in spec §10, not in this plan): re-extraction with `OwnerUserId` and `CreationDate`, GEXF export for Gephi, and weighted PageRank via `aggregateMessages`. The Neo4j load was on this list until 2026-10-03 and is now Tasks 10–11.
