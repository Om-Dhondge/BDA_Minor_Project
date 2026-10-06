import os


def chunk_file(path: str, n: int) -> list[tuple[int, int]]:
    """Split file into n byte-aligned chunks, each ending at a newline boundary.

    Returns list of (start_byte, end_byte) pairs covering the entire file.
    """
    file_size = os.path.getsize(path)
    if n == 1:
        return [(0, file_size)]

    chunk_size = file_size // n
    chunks = []
    start = 0

    with open(path, 'rb') as f:
        for i in range(n):
            if i == n - 1:
                end = file_size
            else:
                approx_end = start + chunk_size
                f.seek(approx_end)
                f.readline()  # advance to next newline
                end = f.tell()
                if end >= file_size:
                    end = file_size
            chunks.append((start, end))
            start = end
            if start >= file_size:
                break

    return chunks
