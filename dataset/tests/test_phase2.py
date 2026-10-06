# tests/test_phase2.py
import os
import pyarrow.parquet as pq
import pyarrow.compute as pc
import pytest
from lib.chunker import chunk_file
from lib.phase1 import extract_questions_worker, merge_questions
from lib.phase2 import extract_answers_worker, OUTPUT_SCHEMA


def _build_questions(sample_posts_xml, tmp_dir):
    chunks = chunk_file(sample_posts_xml, 1)
    result_path, _ = extract_questions_worker(
        (sample_posts_xml, chunks[0][0], chunks[0][1], 0, tmp_dir)
    )
    return merge_questions([result_path])


def test_worker_matches_accepted_answers(tmp_dir, sample_posts_xml):
    questions_dict, accepted_ids = _build_questions(sample_posts_xml, tmp_dir)
    chunks = chunk_file(sample_posts_xml, 1)
    result_path, errors = extract_answers_worker(
        (sample_posts_xml, chunks[0][0], chunks[0][1], 0, tmp_dir, questions_dict, accepted_ids)
    )
    table = pq.read_table(result_path)
    q_ids = table['question_id'].to_pylist()
    # Question 100 (accepted answer 200) and 103 (accepted answer 203) should be matched
    assert 100 in q_ids
    assert 103 in q_ids


def test_worker_excludes_non_accepted_answers(tmp_dir, sample_posts_xml):
    questions_dict, accepted_ids = _build_questions(sample_posts_xml, tmp_dir)
    chunks = chunk_file(sample_posts_xml, 1)
    result_path, _ = extract_answers_worker(
        (sample_posts_xml, chunks[0][0], chunks[0][1], 0, tmp_dir, questions_dict, accepted_ids)
    )
    table = pq.read_table(result_path)
    a_ids = table['accepted_answer_id'].to_pylist()
    assert 201 not in a_ids  # answer 201 is accepted for low-score question 101


def test_worker_output_schema(tmp_dir, sample_posts_xml):
    questions_dict, accepted_ids = _build_questions(sample_posts_xml, tmp_dir)
    chunks = chunk_file(sample_posts_xml, 1)
    result_path, _ = extract_answers_worker(
        (sample_posts_xml, chunks[0][0], chunks[0][1], 0, tmp_dir, questions_dict, accepted_ids)
    )
    table = pq.read_table(result_path)
    expected_cols = {
        'question_id', 'accepted_answer_id', 'title', 'question_text',
        'question_code_snippets', 'tags', 'score', 'answer_text',
        'answer_code_snippets', 'image_urls', 'answer_score',
    }
    assert set(table.column_names) == expected_cols


def test_worker_extracts_question_code_snippets(tmp_dir, sample_posts_xml):
    questions_dict, accepted_ids = _build_questions(sample_posts_xml, tmp_dir)
    chunks = chunk_file(sample_posts_xml, 1)
    result_path, _ = extract_answers_worker(
        (sample_posts_xml, chunks[0][0], chunks[0][1], 0, tmp_dir, questions_dict, accepted_ids)
    )
    table = pq.read_table(result_path)
    row = table.filter(pc.equal(table['question_id'], 100))
    snippets = row['question_code_snippets'][0].as_py()
    assert any('margin: auto' in s for s in snippets)


def test_worker_extracts_answer_code_snippets(tmp_dir, sample_posts_xml):
    questions_dict, accepted_ids = _build_questions(sample_posts_xml, tmp_dir)
    chunks = chunk_file(sample_posts_xml, 1)
    result_path, _ = extract_answers_worker(
        (sample_posts_xml, chunks[0][0], chunks[0][1], 0, tmp_dir, questions_dict, accepted_ids)
    )
    table = pq.read_table(result_path)
    row = table.filter(pc.equal(table['question_id'], 103))
    snippets = row['answer_code_snippets'][0].as_py()
    assert any('list comprehension' in s or 'for x in lst' in s for s in snippets)


def test_worker_extracts_image_urls(tmp_dir, sample_posts_xml):
    questions_dict, accepted_ids = _build_questions(sample_posts_xml, tmp_dir)
    chunks = chunk_file(sample_posts_xml, 1)
    result_path, _ = extract_answers_worker(
        (sample_posts_xml, chunks[0][0], chunks[0][1], 0, tmp_dir, questions_dict, accepted_ids)
    )
    table = pq.read_table(result_path)
    row = table.filter(pc.equal(table['question_id'], 103))
    urls = row['image_urls'][0].as_py()
    assert 'https://i.stack.imgur.com/abc12.png' in urls
