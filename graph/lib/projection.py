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
