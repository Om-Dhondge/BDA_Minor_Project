# tests/test_phase3.py
import asyncio
import json
import os
import pytest
import pyarrow as pa
import pyarrow.parquet as pq
from unittest.mock import AsyncMock, MagicMock, patch
from lib.phase3 import collect_image_urls, download_images, load_resume, save_resume


def _make_parquet(tmp_dir, urls_per_record):
    path = os.path.join(tmp_dir, 'test.parquet')
    n = len(urls_per_record)
    table = pa.table({
        'question_id': pa.array(list(range(1, n + 1)), pa.int64()),
        'image_urls': pa.array(urls_per_record, pa.list_(pa.string())),
    })
    pq.write_table(table, path)
    return path


def test_collect_image_urls_deduplicates(tmp_dir):
    path = _make_parquet(tmp_dir, [
        ['https://i.stack.imgur.com/a.png', 'https://i.stack.imgur.com/b.png'],
        ['https://i.stack.imgur.com/a.png'],  # duplicate
    ])
    urls = collect_image_urls([path])
    assert len(urls) == 2
    assert 'https://i.stack.imgur.com/a.png' in urls
    assert 'https://i.stack.imgur.com/b.png' in urls


def test_collect_image_urls_skips_empty(tmp_dir):
    path = _make_parquet(tmp_dir, [[], []])
    urls = collect_image_urls([path])
    assert urls == set()


def test_load_resume_returns_empty_dict_if_missing(tmp_dir):
    result = load_resume(os.path.join(tmp_dir, 'resume.json'))
    assert result == {}


def test_save_and_load_resume(tmp_dir):
    path = os.path.join(tmp_dir, 'resume.json')
    data = {'https://example.com/a.png': ('images/a.png', 'ok')}
    save_resume(path, data)
    loaded = load_resume(path)
    assert loaded == data


def test_download_images_skips_already_done(tmp_dir):
    resume_path = os.path.join(tmp_dir, 'resume.json')
    images_dir = os.path.join(tmp_dir, 'images')
    os.makedirs(images_dir)

    url = 'https://i.stack.imgur.com/done.png'
    existing = {url: ('images/done.png', 'ok')}
    save_resume(resume_path, existing)

    path = _make_parquet(tmp_dir, [[url]])
    results = asyncio.run(download_images(
        parquet_paths=[path],
        images_dir=images_dir,
        resume_path=resume_path,
        concurrency=2,
    ))
    assert results[url] == ('images/done.png', 'ok')


@pytest.mark.asyncio
async def test_download_images_handles_404(tmp_dir):
    resume_path = os.path.join(tmp_dir, 'resume.json')
    images_dir = os.path.join(tmp_dir, 'images')
    os.makedirs(images_dir)

    url = 'https://i.stack.imgur.com/notfound.png'
    path = _make_parquet(tmp_dir, [[url]])

    mock_resp = MagicMock()
    mock_resp.status = 404
    mock_resp.__aenter__ = AsyncMock(return_value=mock_resp)
    mock_resp.__aexit__ = AsyncMock(return_value=False)

    with patch('aiohttp.ClientSession') as mock_session_cls:
        mock_session = MagicMock()
        mock_session.__aenter__ = AsyncMock(return_value=mock_session)
        mock_session.__aexit__ = AsyncMock(return_value=False)
        mock_session.head = MagicMock(return_value=mock_resp)
        mock_session_cls.return_value = mock_session

        results = await download_images(
            parquet_paths=[path],
            images_dir=images_dir,
            resume_path=resume_path,
            concurrency=2,
        )

    assert results[url][1] == '404'
