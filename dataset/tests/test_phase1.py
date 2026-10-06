import os
import pyarrow.parquet as pq
import pytest
from lib.chunker import chunk_file
from lib.phase1 import extract_questions_worker, merge_questions, parse_tags


def test_parse_tags_pipe_delimited():
    assert parse_tags('|python|django|') == ['python', 'django']
    assert parse_tags('|css|') == ['css']
    assert parse_tags('') == []
    assert parse_tags('||') == []


def test_worker_filters_low_score(tmp_dir, sample_posts_xml):
    chunks = chunk_file(sample_posts_xml, 1)
    result_path, errors = extract_questions_worker(
        (sample_posts_xml, chunks[0][0], chunks[0][1], 0, tmp_dir)
    )
    table = pq.read_table(result_path)
    ids = table['question_id'].to_pylist()
    assert 101 not in ids  # Score=1, below threshold


def test_worker_filters_no_accepted_answer(tmp_dir, sample_posts_xml):
    chunks = chunk_file(sample_posts_xml, 1)
    result_path, errors = extract_questions_worker(
        (sample_posts_xml, chunks[0][0], chunks[0][1], 0, tmp_dir)
    )
    table = pq.read_table(result_path)
    ids = table['question_id'].to_pylist()
    assert 102 not in ids  # no AcceptedAnswerId


def test_worker_includes_qualifying_questions(tmp_dir, sample_posts_xml):
    chunks = chunk_file(sample_posts_xml, 1)
    result_path, errors = extract_questions_worker(
        (sample_posts_xml, chunks[0][0], chunks[0][1], 0, tmp_dir)
    )
    table = pq.read_table(result_path)
    ids = table['question_id'].to_pylist()
    assert 100 in ids   # Score=5, has AcceptedAnswerId=200
    assert 103 in ids   # Score=8, has AcceptedAnswerId=203


def test_worker_output_schema(tmp_dir, sample_posts_xml):
    chunks = chunk_file(sample_posts_xml, 1)
    result_path, _ = extract_questions_worker(
        (sample_posts_xml, chunks[0][0], chunks[0][1], 0, tmp_dir)
    )
    table = pq.read_table(result_path)
    assert set(table.column_names) == {
        'question_id', 'accepted_answer_id', 'title', 'tags', 'score', 'body'
    }


def test_worker_parses_tags(tmp_dir, sample_posts_xml):
    chunks = chunk_file(sample_posts_xml, 1)
    result_path, _ = extract_questions_worker(
        (sample_posts_xml, chunks[0][0], chunks[0][1], 0, tmp_dir)
    )
    table = pq.read_table(result_path)
    import pyarrow.compute as pc
    row = table.filter(pc.equal(table['question_id'], 100))
    tags = row['tags'][0].as_py()
    assert set(tags) == {'css', 'html'}


def test_merge_questions_builds_lookup(tmp_dir, sample_posts_xml):
    chunks = chunk_file(sample_posts_xml, 1)
    result_path, _ = extract_questions_worker(
        (sample_posts_xml, chunks[0][0], chunks[0][1], 0, tmp_dir)
    )
    questions_dict, accepted_ids = merge_questions([result_path])
    # Key is accepted_answer_id
    assert 200 in questions_dict
    assert 203 in questions_dict
    assert questions_dict[200]['question_id'] == 100
    assert 200 in accepted_ids
    assert 203 in accepted_ids
