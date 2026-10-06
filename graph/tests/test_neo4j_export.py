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
