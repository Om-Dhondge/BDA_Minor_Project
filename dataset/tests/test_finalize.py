import json
import os
import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from lib.finalize import merge_parts


def _make_output_part(tmp_dir, name, q_ids, image_urls_list):
    path = os.path.join(tmp_dir, name)
    table = pa.table({
        'question_id': pa.array(q_ids, pa.int64()),
        'accepted_answer_id': pa.array(q_ids, pa.int64()),
        'title': pa.array([f'Q{i}' for i in q_ids], pa.string()),
        'question_text': pa.array([f'text {i}' for i in q_ids], pa.string()),
        'question_code_snippets': pa.array([[] for _ in q_ids], pa.list_(pa.string())),
        'tags': pa.array([['python'] for _ in q_ids], pa.list_(pa.string())),
        'score': pa.array([5] * len(q_ids), pa.int32()),
        'answer_text': pa.array([f'answer {i}' for i in q_ids], pa.string()),
        'answer_code_snippets': pa.array([[] for _ in q_ids], pa.list_(pa.string())),
        'image_urls': pa.array(image_urls_list, pa.list_(pa.string())),
        'answer_score': pa.array([10] * len(q_ids), pa.int32()),
    })
    pq.write_table(table, path)
    return path


def test_merge_combines_all_parts(tmp_dir):
    p1 = _make_output_part(tmp_dir, 'part_0.parquet', [1, 2], [[], []])
    p2 = _make_output_part(tmp_dir, 'part_1.parquet', [3, 4], [[], []])
    out_parquet = os.path.join(tmp_dir, 'out.parquet')
    out_jsonl = os.path.join(tmp_dir, 'out.jsonl')

    merge_parts([p1, p2], {}, out_parquet, out_jsonl)

    table = pq.read_table(out_parquet)
    assert len(table) == 4
    assert set(table['question_id'].to_pylist()) == {1, 2, 3, 4}


def test_merge_adds_image_fields(tmp_dir):
    url = 'https://i.stack.imgur.com/abc.png'
    p1 = _make_output_part(tmp_dir, 'part_0.parquet', [1], [[url]])
    out_parquet = os.path.join(tmp_dir, 'out.parquet')
    out_jsonl = os.path.join(tmp_dir, 'out.jsonl')

    image_results = {url: ('images/abc.png', 'ok')}
    merge_parts([p1], image_results, out_parquet, out_jsonl)

    table = pq.read_table(out_parquet)
    assert 'image_local_paths' in table.column_names
    assert 'image_statuses' in table.column_names
    assert table['image_local_paths'][0].as_py() == ['images/abc.png']
    assert table['image_statuses'][0].as_py() == ['ok']


def test_merge_marks_broken_images(tmp_dir):
    url = 'https://broken.example.com/img.png'
    p1 = _make_output_part(tmp_dir, 'part_0.parquet', [1], [[url]])
    out_parquet = os.path.join(tmp_dir, 'out.parquet')
    out_jsonl = os.path.join(tmp_dir, 'out.jsonl')

    image_results = {url: (None, '404')}
    merge_parts([p1], image_results, out_parquet, out_jsonl)

    table = pq.read_table(out_parquet)
    assert table['image_local_paths'][0].as_py() == [None]
    assert table['image_statuses'][0].as_py() == ['404']


def test_merge_writes_jsonl_sample(tmp_dir):
    parts = [_make_output_part(tmp_dir, f'part_{i}.parquet', list(range(i*100, i*100+100)), [[] for _ in range(100)]) for i in range(2)]
    out_parquet = os.path.join(tmp_dir, 'out.parquet')
    out_jsonl = os.path.join(tmp_dir, 'out.jsonl')

    merge_parts(parts, {}, out_parquet, out_jsonl, sample_size=50)

    lines = open(out_jsonl).readlines()
    assert len(lines) == 50
    record = json.loads(lines[0])
    assert 'question_id' in record
    assert 'image_local_paths' in record
