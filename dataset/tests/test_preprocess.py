import subprocess
import sys
import os
import pathlib
import pyarrow.parquet as pq
import pytest

PROJECT_ROOT = pathlib.Path(__file__).parent.parent


def test_preprocess_end_to_end(tmp_dir, sample_posts_xml):
    result = subprocess.run(
        [sys.executable, 'preprocess.py',
         '--input', sample_posts_xml,
         '--output-dir', tmp_dir,
         '--workers', '1',
         '--skip-images'],
        capture_output=True, text=True,
        cwd=str(PROJECT_ROOT)
    )
    assert result.returncode == 0, f"STDERR: {result.stderr}"

    out_parquet = os.path.join(tmp_dir, 'preprocessed_posts.parquet')
    out_jsonl = os.path.join(tmp_dir, 'preprocessed_posts_sample.jsonl')
    assert os.path.exists(out_parquet)
    assert os.path.exists(out_jsonl)

    table = pq.read_table(out_parquet)
    assert len(table) == 2   # Questions 100 and 103 qualify
    assert 'image_local_paths' in table.column_names
    assert 'image_statuses' in table.column_names
