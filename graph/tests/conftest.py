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
