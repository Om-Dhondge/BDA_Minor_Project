import xml.etree.ElementTree as ET
from pathlib import Path
import pyarrow as pa
import pyarrow.parquet as pq


def parse_tags(raw: str) -> list[str]:
    """Parse '|python|django|' → ['python', 'django']"""
    return [t for t in raw.strip('|').split('|') if t]


def extract_questions_worker(args: tuple) -> tuple[str, int]:
    """Multiprocessing worker: extract qualifying questions from a file chunk.

    Args:
        args: (path, start_byte, end_byte, worker_id, temp_dir)

    Returns:
        (output_parquet_path, parse_error_count)
    """
    path, start_byte, end_byte, worker_id, temp_dir = args

    rows = {
        'question_id': [], 'accepted_answer_id': [], 'title': [],
        'tags': [], 'score': [], 'body': [],
    }
    parse_errors = 0

    with open(path, 'rb') as f:
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
                if elem.get('PostTypeId') != '1':
                    continue
                score = int(elem.get('Score', '0'))
                if score < 3:
                    continue
                accepted_answer_id = elem.get('AcceptedAnswerId')
                if not accepted_answer_id:
                    continue
                body = elem.get('Body', '')
                if not body:
                    continue
                rows['question_id'].append(int(elem.get('Id')))
                rows['accepted_answer_id'].append(int(accepted_answer_id))
                rows['title'].append(elem.get('Title', ''))
                rows['tags'].append(parse_tags(elem.get('Tags', '')))
                rows['score'].append(score)
                rows['body'].append(body)
            except Exception:
                parse_errors += 1

    out_path = str(Path(temp_dir) / f'questions_part_{worker_id}.parquet')
    table = pa.table({
        'question_id': pa.array(rows['question_id'], pa.int64()),
        'accepted_answer_id': pa.array(rows['accepted_answer_id'], pa.int64()),
        'title': pa.array(rows['title'], pa.string()),
        'tags': pa.array(rows['tags'], pa.list_(pa.string())),
        'score': pa.array(rows['score'], pa.int32()),
        'body': pa.array(rows['body'], pa.string()),
    })
    pq.write_table(table, out_path)
    return out_path, parse_errors


def merge_questions(part_paths: list[str]) -> tuple[dict, set]:
    """Merge question part Parquet files into a lookup dict and accepted IDs set.

    Returns:
        questions_dict: {accepted_answer_id → question row dict}
        accepted_ids_set: set of all accepted answer IDs (for O(1) lookup)
    """
    tables = [pq.read_table(p) for p in part_paths if Path(p).exists()]
    if not tables:
        return {}, set()

    combined = pa.concat_tables(tables)
    questions_dict = {}
    accepted_ids_set = set()

    q_ids = combined['question_id'].to_pylist()
    a_ids = combined['accepted_answer_id'].to_pylist()
    titles = combined['title'].to_pylist()
    tags = combined['tags'].to_pylist()
    scores = combined['score'].to_pylist()
    bodies = combined['body'].to_pylist()

    for i in range(len(combined)):
        aid = a_ids[i]
        questions_dict[aid] = {
            'question_id': q_ids[i],
            'accepted_answer_id': aid,
            'title': titles[i],
            'tags': tags[i],
            'score': scores[i],
            'body': bodies[i],
        }
        accepted_ids_set.add(aid)

    return questions_dict, accepted_ids_set
