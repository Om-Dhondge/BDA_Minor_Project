# lib/phase3.py
import asyncio
import hashlib
import json
import os
from pathlib import Path
import aiohttp
import pyarrow.parquet as pq

CHECKPOINT_EVERY = 10_000
MAX_IMAGE_BYTES = 5 * 1024 * 1024  # 5MB
REQUEST_TIMEOUT = 5


def collect_image_urls(parquet_paths: list[str]) -> set[str]:
    """Read all image_urls columns from Parquet files and return deduplicated set."""
    urls = set()
    for path in parquet_paths:
        if not Path(path).exists():
            continue
        table = pq.read_table(path, columns=['image_urls'])
        for cell in table['image_urls'].to_pylist():
            if cell:
                urls.update(u for u in cell if u)
    return urls


def load_resume(resume_path: str) -> dict:
    """Load checkpoint dict from resume.json. Returns {} if file doesn't exist."""
    if not Path(resume_path).exists():
        return {}
    with open(resume_path) as f:
        raw = json.load(f)
    # JSON serializes tuples as lists; restore to (local_path, status) tuples
    return {url: tuple(val) for url, val in raw.items()}


def save_resume(resume_path: str, results: dict) -> None:
    """Persist results dict to resume.json atomically."""
    tmp = resume_path + '.tmp'
    with open(tmp, 'w') as f:
        json.dump(results, f)
    os.replace(tmp, resume_path)


async def _download_one(
    session: aiohttp.ClientSession,
    url: str,
    images_dir: str,
    semaphore: asyncio.Semaphore,
) -> tuple[str, str | None, str]:
    """Download a single image URL. Returns (url, local_path_or_None, status)."""
    async with semaphore:
        timeout = aiohttp.ClientTimeout(total=REQUEST_TIMEOUT)
        # HEAD check
        try:
            async with session.head(url, timeout=timeout, allow_redirects=True) as resp:
                if resp.status != 200:
                    return url, None, '404'
                ct = resp.headers.get('Content-Type', '')
                if not ct.startswith('image/'):
                    return url, None, 'not_image'
        except asyncio.TimeoutError:
            return url, None, 'timeout'
        except Exception:
            return url, None, '404'

        # GET with size cap
        try:
            async with session.get(url, timeout=timeout, allow_redirects=True) as resp:
                if resp.status != 200:
                    return url, None, '404'
                chunks = []
                size = 0
                async for chunk in resp.content.iter_chunked(65536):
                    size += len(chunk)
                    if size > MAX_IMAGE_BYTES:
                        return url, None, 'too_large'
                    chunks.append(chunk)
                data = b''.join(chunks)
        except asyncio.TimeoutError:
            # One retry with chunked reading
            try:
                async with session.get(url, timeout=timeout, allow_redirects=True) as resp:
                    chunks = []
                    size = 0
                    async for chunk in resp.content.iter_chunked(65536):
                        size += len(chunk)
                        if size > MAX_IMAGE_BYTES:
                            return url, None, 'too_large'
                        chunks.append(chunk)
                    data = b''.join(chunks)
            except Exception:
                return url, None, 'timeout'
        except Exception:
            return url, None, '404'

    url_hash = hashlib.md5(url.encode()).hexdigest()[:16]
    ext = url.split('/')[-1].split('?')[0].rsplit('.', 1)
    extension = ('.' + ext[1]) if len(ext) == 2 and len(ext[1]) <= 6 else ''
    filename = url_hash + extension
    local_path = str(Path(images_dir) / filename)
    await asyncio.to_thread(Path(local_path).write_bytes, data)
    return url, local_path, 'ok'


async def download_images(
    parquet_paths: list[str],
    images_dir: str,
    resume_path: str,
    concurrency: int = 200,
) -> dict:
    """Download all images referenced in parquet_paths.

    Skips URLs already in resume.json. Checkpoints every CHECKPOINT_EVERY downloads.

    Returns:
        dict mapping url -> (local_path_or_None, status)
    """
    Path(images_dir).mkdir(parents=True, exist_ok=True)
    results = load_resume(resume_path)

    all_urls = collect_image_urls(parquet_paths)
    pending = [u for u in all_urls if u not in results]

    if not pending:
        return results

    semaphore = asyncio.Semaphore(concurrency)
    connector = aiohttp.TCPConnector(limit=concurrency)

    async with aiohttp.ClientSession(connector=connector) as session:
        tasks = [
            _download_one(session, url, images_dir, semaphore)
            for url in pending
        ]
        completed = 0
        for coro in asyncio.as_completed(tasks):
            url, local_path, status = await coro
            results[url] = (local_path, status)
            completed += 1
            if completed % CHECKPOINT_EVERY == 0:
                save_resume(resume_path, results)
                print(f'  [phase3] {completed}/{len(pending)} images processed')

    save_resume(resume_path, results)
    return results
