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
