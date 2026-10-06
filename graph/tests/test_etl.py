import pytest

from lib.etl import (
    aggregate_questions,
    build_tag_attributes,
    explode_question_tags,
    sample_questions,
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
