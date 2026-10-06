# Stack Overflow Preprocessing Pipeline

Transforms `Posts.xml` (104 GB) into a clean Parquet dataset of Q&A pairs with separated prose, code snippets, and downloaded images for a tri-modal RAG pipeline.

## Requirements

Python 3.12+

```bash
pip install -r requirements.txt
```

## Running the Pipeline

### Full run (with image downloading)

```bash
python preprocess.py \
  --input Datasets/Posts.xml \
  --output-dir Datasets \
  --workers 4 \
  --concurrency 50
```

### Skip image downloading

```bash
python preprocess.py \
  --input Datasets/Posts.xml \
  --output-dir Datasets \
  --workers 4 \
  --skip-images
```

### Options

| Flag | Default | Description |
|------|---------|-------------|
| `--input` | required | Path to `Posts.xml` |
| `--output-dir` | required | Directory to write output files |
| `--workers` | 4 | Parallel worker processes for Phases 1 and 2 |
| `--concurrency` | 50 | Concurrent connections for image downloading (Phase 3) |
| `--skip-images` | off | Skip Phase 3 image downloading entirely |

## Output Files

| File | Description |
|------|-------------|
| `preprocessed_posts.parquet` | Final merged dataset (Snappy compressed) |
| `preprocessed_posts_sample.jsonl` | Random 10,000-record sample for inspection |
| `images/` | Downloaded images (only when `--skip-images` is not set) |
| `resume.json` | Image download checkpoint (resumes interrupted Phase 3) |

## Output Schema

| Column | Type | Description |
|--------|------|-------------|
| `question_id` | int64 | Stack Overflow post ID |
| `accepted_answer_id` | int64 | Accepted answer post ID |
| `title` | string | Question title |
| `question_text` | string | Clean prose from question body |
| `question_code_snippets` | list[string] | Code blocks from question |
| `tags` | list[string] | Question tags |
| `score` | int32 | Question score |
| `answer_text` | string | Clean prose from accepted answer |
| `answer_code_snippets` | list[string] | Code blocks from accepted answer |
| `image_urls` | list[string] | Image URLs referenced in question + answer |
| `answer_score` | int32 | Accepted answer score |
| `image_local_paths` | list[string\|null] | Local paths to downloaded images |
| `image_statuses` | list[string] | Per-image status: `ok`, `404`, `timeout`, `too_large`, `not_image`, `not_downloaded` |

## Filtering

Only Q&A pairs meeting all conditions are included:
- Question score ≥ 3
- Question has an accepted answer
- Question body is non-empty

## Performance (on 104 GB Posts.xml)

- Phase 1 + 2 (extraction): ~5 minutes with 8 workers
- Phase 3 (image download): ~2–3 hours with 200 concurrency
- Total: ~3 hours

## Resuming an Interrupted Run

Phase 3 checkpoints progress to `resume.json`. Re-running with the same `--output-dir` automatically skips already-downloaded images.

## Running Tests

```bash
pytest tests/ -v
```

Expected: 37 tests passing.
