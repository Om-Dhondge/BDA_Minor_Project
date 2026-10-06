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
