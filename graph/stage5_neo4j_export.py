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
