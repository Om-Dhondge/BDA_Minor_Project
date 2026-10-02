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
