import os
import pytest
from lib.chunker import chunk_file


def test_chunks_cover_entire_file(tmp_dir):
    path = os.path.join(tmp_dir, 'test.xml')
    content = '\n'.join(f'  <row Id="{i}" />' for i in range(1000)) + '\n'
    with open(path, 'w') as f:
        f.write(content)

    chunks = chunk_file(path, 4)

    assert chunks[0][0] == 0
    assert chunks[-1][1] == os.path.getsize(path)
    for i in range(len(chunks) - 1):
        assert chunks[i][1] == chunks[i + 1][0]


def test_chunks_start_at_line_boundaries(tmp_dir):
    path = os.path.join(tmp_dir, 'test.xml')
    lines = [f'line_{i}\n' for i in range(200)]
    with open(path, 'w') as f:
        f.writelines(lines)

    chunks = chunk_file(path, 4)

    with open(path, 'rb') as f:
        for start, end in chunks:
            if start == 0:
                continue
            f.seek(start - 1)
            prev_byte = f.read(1)
            assert prev_byte == b'\n', f"Chunk at {start} doesn't start after newline"


def test_single_chunk_returns_full_file(tmp_dir):
    path = os.path.join(tmp_dir, 'test.xml')
    with open(path, 'w') as f:
        f.write('hello\nworld\n')

    chunks = chunk_file(path, 1)

    assert len(chunks) == 1
    assert chunks[0] == (0, os.path.getsize(path))


def test_chunk_content_readable(tmp_dir, sample_posts_xml):
    chunks = chunk_file(sample_posts_xml, 2)

    all_lines = []
    with open(sample_posts_xml, 'rb') as f:
        for start, end in chunks:
            f.seek(start)
            data = f.read(end - start)
            all_lines.extend(data.splitlines())

    with open(sample_posts_xml, 'rb') as f:
        expected = f.read().splitlines()

    assert all_lines == expected
