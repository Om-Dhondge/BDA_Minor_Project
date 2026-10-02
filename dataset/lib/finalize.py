import json
import random
from pathlib import Path
import pyarrow as pa
import pyarrow.parquet as pq


def merge_parts(
    output_parts: list[str],
    image_results: dict,
    out_parquet: str,
    out_jsonl: str,
    sample_size: int = 10_000,
) -> None:
    """Merge output Parquet parts, attach image results, write final Parquet + JSONL sample.

    Args:
        output_parts: paths to output_part_N.parquet files
        image_results: {url: (local_path_or_None, status)} from Phase 3
        out_parquet: destination path for merged preprocessed_posts.parquet
        out_jsonl: destination path for JSONL sample
        sample_size: number of records to sample into JSONL
    """
    tables = [pq.read_table(p) for p in output_parts if Path(p).exists()]
    if not tables:
        raise ValueError("No output parts found to merge")

    combined = pa.concat_tables(tables)
    n = len(combined)

    # Build image_local_paths and image_statuses columns
    local_paths_col = []
    statuses_col = []

    image_urls_col = combined['image_urls'].to_pylist()
    for urls in image_urls_col:
        lpaths = []
        statuses = []
        for url in (urls or []):
            result = image_results.get(url, (None, 'not_downloaded'))
            lpaths.append(result[0])
            statuses.append(result[1])
        local_paths_col.append(lpaths)
        statuses_col.append(statuses)

    final = combined.append_column(
        'image_local_paths',
        pa.array(local_paths_col, pa.list_(pa.string()))
    ).append_column(
        'image_statuses',
        pa.array(statuses_col, pa.list_(pa.string()))
    )

    pq.write_table(final, out_parquet, compression='snappy')

    # Write JSONL sample
    indices = random.sample(range(n), min(sample_size, n))
    sample = final.take(indices)
    with open(out_jsonl, 'w', encoding='utf-8') as f:
        for i in range(len(sample)):
            row = {col: sample[col][i].as_py() for col in sample.column_names}
            f.write(json.dumps(row, ensure_ascii=False) + '\n')
