#!/usr/bin/env python3
# preprocess.py
import argparse
import asyncio
import os
import shutil
import sys
import time
from multiprocessing import Pool
from pathlib import Path

from lib.chunker import chunk_file
from lib.phase1 import extract_questions_worker, merge_questions
from lib.phase2 import extract_answers_worker
from lib.phase3 import download_images
from lib.finalize import merge_parts


def log(msg: str) -> None:
    print(f'[preprocess] {msg}', flush=True)


def run_phase1(input_path: str, chunks: list, temp_dir: str, workers: int) -> tuple[dict, set]:
    log(f'Phase 1: extracting questions with {workers} workers...')
    t0 = time.time()
    args = [
        (input_path, start, end, i, temp_dir)
        for i, (start, end) in enumerate(chunks)
    ]
    with Pool(workers) as pool:
        results = pool.map(extract_questions_worker, args)

    part_paths = [r[0] for r in results]
    total_errors = sum(r[1] for r in results)
    questions_dict, accepted_ids = merge_questions(part_paths)
    log(f'Phase 1 done: {len(questions_dict)} qualifying questions, {total_errors} parse errors [{time.time()-t0:.1f}s]')
    return questions_dict, accepted_ids


def run_phase2(input_path: str, chunks: list, temp_dir: str, workers: int,
               questions_dict: dict, accepted_ids: set) -> list[str]:
    log(f'Phase 2: extracting answers with {workers} workers...')
    t0 = time.time()
    args = [
        (input_path, start, end, i, temp_dir, questions_dict, accepted_ids)
        for i, (start, end) in enumerate(chunks)
    ]
    with Pool(workers) as pool:
        results = pool.map(extract_answers_worker, args)

    part_paths = [r[0] for r in results]
    total_errors = sum(r[1] for r in results)
    log(f'Phase 2 done: {total_errors} parse errors [{time.time()-t0:.1f}s]')
    return part_paths


def main():
    parser = argparse.ArgumentParser(description='Preprocess Stack Overflow Posts.xml for RAG pipeline')
    parser.add_argument('--input', required=True, help='Path to Posts.xml')
    parser.add_argument('--output-dir', required=True, help='Directory to write output files')
    parser.add_argument('--workers', type=int, default=4, help='Number of parallel workers (default: 4)')
    parser.add_argument('--concurrency', type=int, default=50, help='Image download concurrency (default: 50)')
    parser.add_argument('--skip-images', action='store_true', help='Skip Phase 3 image downloading')
    args = parser.parse_args()

    input_path = args.input
    output_dir = args.output_dir
    temp_dir = os.path.join(output_dir, 'temp')
    images_dir = os.path.join(output_dir, 'images')
    resume_path = os.path.join(output_dir, 'resume.json')
    out_parquet = os.path.join(output_dir, 'preprocessed_posts.parquet')
    out_jsonl = os.path.join(output_dir, 'preprocessed_posts_sample.jsonl')

    # Input validation
    if not os.path.isfile(input_path):
        print(f'[preprocess] ERROR: input file not found: {input_path}', file=sys.stderr)
        sys.exit(1)
    if args.workers < 1:
        print(f'[preprocess] ERROR: --workers must be >= 1', file=sys.stderr)
        sys.exit(1)

    Path(output_dir).mkdir(parents=True, exist_ok=True)
    Path(temp_dir).mkdir(parents=True, exist_ok=True)

    log(f'Input: {input_path} ({os.path.getsize(input_path) / 1e9:.1f} GB)')
    log(f'Output: {output_dir}')
    log(f'Workers: {args.workers}')

    try:
        # Chunk the file
        log('Chunking input file...')
        chunks = chunk_file(input_path, args.workers)
        log(f'Created {len(chunks)} chunks')

        # Phase 1
        questions_dict, accepted_ids = run_phase1(input_path, chunks, temp_dir, args.workers)

        # Phase 2
        output_parts = run_phase2(input_path, chunks, temp_dir, args.workers, questions_dict, accepted_ids)

        # Phase 3
        image_results = {}
        if not args.skip_images:
            log(f'Phase 3: downloading images (concurrency={args.concurrency})...')
            t0 = time.time()
            image_results = asyncio.run(download_images(
                parquet_paths=output_parts,
                images_dir=images_dir,
                resume_path=resume_path,
                concurrency=args.concurrency,
            ))
            ok = sum(1 for _, s in image_results.values() if s == 'ok')
            log(f'Phase 3 done: {ok}/{len(image_results)} images downloaded [{time.time()-t0:.1f}s]')
        else:
            log('Phase 3: skipped (--skip-images)')

        # Finalize
        log('Finalizing: merging parts and writing output...')
        t0 = time.time()
        merge_parts(output_parts, image_results, out_parquet, out_jsonl)
        log(f'Finalize done [{time.time()-t0:.1f}s]')
    finally:
        shutil.rmtree(temp_dir, ignore_errors=True)
        log(f'Done. Output: {out_parquet}')


if __name__ == '__main__':
    main()
