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
