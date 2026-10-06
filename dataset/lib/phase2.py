# lib/phase2.py
import xml.etree.ElementTree as ET
from pathlib import Path
import pyarrow as pa
import pyarrow.parquet as pq
from lib.html_parser import parse_body

BATCH_SIZE = 50_000

OUTPUT_SCHEMA = pa.schema([
    ('question_id', pa.int64()),
    ('accepted_answer_id', pa.int64()),
    ('title', pa.string()),
    ('question_text', pa.string()),
    ('question_code_snippets', pa.list_(pa.string())),
    ('tags', pa.list_(pa.string())),
    ('score', pa.int32()),
    ('answer_text', pa.string()),
    ('answer_code_snippets', pa.list_(pa.string())),
    ('image_urls', pa.list_(pa.string())),
    ('answer_score', pa.int32()),
])


def _flush_batch(rows: dict, writer, schema) -> None:
    if not rows['question_id']:
        return
    table = pa.table(rows, schema=schema)
    writer.write_table(table)
    for key in rows:
        rows[key].clear()


def extract_answers_worker(args: tuple) -> tuple[str, int]:
    """Multiprocessing worker: extract accepted answers, join with questions, parse HTML.

    Args:
        args: (path, start_byte, end_byte, worker_id, temp_dir, questions_dict, accepted_ids_set)

    Returns:
        (output_parquet_path, parse_error_count)
    """
    path, start_byte, end_byte, worker_id, temp_dir, questions_dict, accepted_ids_set = args

    out_path = str(Path(temp_dir) / f'output_part_{worker_id}.parquet')
    parse_errors = 0

    rows = {
        'question_id': [], 'accepted_answer_id': [], 'title': [],
        'question_text': [], 'question_code_snippets': [], 'tags': [],
        'score': [], 'answer_text': [], 'answer_code_snippets': [],
        'image_urls': [], 'answer_score': [],
    }

    with open(path, 'rb') as f, pq.ParquetWriter(out_path, OUTPUT_SCHEMA) as writer:
        f.seek(start_byte)
        while f.tell() < end_byte:
            line = f.readline()
            if not line:
                break
            try:
                line_str = line.decode('utf-8', errors='ignore').strip()
                if not line_str.startswith('<row'):
                    continue
                elem = ET.fromstring(line_str)
                if elem.get('PostTypeId') != '2':
                    continue
                answer_id = int(elem.get('Id', '0'))
                if answer_id not in accepted_ids_set:
                    continue

                q = questions_dict[answer_id]
                answer_body = elem.get('Body', '')
                answer_score = int(elem.get('Score', '0'))

                q_parsed = parse_body(q['body'])
                a_parsed = parse_body(answer_body)

                all_image_urls = q_parsed.image_urls + a_parsed.image_urls

                rows['question_id'].append(q['question_id'])
                rows['accepted_answer_id'].append(answer_id)
                rows['title'].append(q['title'])
                rows['question_text'].append(q_parsed.text)
                rows['question_code_snippets'].append(q_parsed.code_snippets)
                rows['tags'].append(q['tags'])
                rows['score'].append(q['score'])
                rows['answer_text'].append(a_parsed.text)
                rows['answer_code_snippets'].append(a_parsed.code_snippets)
                rows['image_urls'].append(all_image_urls)
                rows['answer_score'].append(answer_score)

                if len(rows['question_id']) >= BATCH_SIZE:
                    _flush_batch(rows, writer, OUTPUT_SCHEMA)

            except Exception:
                parse_errors += 1

        _flush_batch(rows, writer, OUTPUT_SCHEMA)

    return out_path, parse_errors
