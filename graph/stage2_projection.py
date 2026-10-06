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
