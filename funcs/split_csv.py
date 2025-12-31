# funcs/split_csv.py
from pathlib import Path

def stdlib(file_path: Path, rows_per_chunk: int, output_dir: Path):
    try:
        with open(file_path, 'r', encoding='utf-8', newline='', errors='replace') as inf:
            header = inf.readline()
            if not header:
                return file_path.name, 0
            base_name = file_path.stem
            chunk_num = 1
            chunks = 0
            current = []
            for line in inf:
                current.append(line)
                if len(current) >= rows_per_chunk:
                    out_file = output_dir / f"{base_name}_part_{chunk_num:04d}.csv"
                    with open(out_file, 'w', encoding='utf-8', newline='', buffering=8192*128) as outf:
                        outf.write(header)
                        outf.writelines(current)
                    chunks += 1
                    chunk_num += 1
                    current.clear()
            if current:
                out_file = output_dir / f"{base_name}_part_{chunk_num:04d}.csv"
                with open(out_file, 'w', encoding='utf-8', newline='', buffering=8192*128) as outf:
                    outf.write(header)
                    outf.writelines(current)
                chunks += 1
            return file_path.name, chunks
    except Exception as e:
        raise Exception(f"error {file_path.name}: {e}")

def polars(file_path: Path, rows_per_chunk: int, output_dir: Path):
    import polars as pl
    df = pl.read_csv(str(file_path), encoding='utf-8', ignore_errors=True, infer_schema_length=10000)
    total = len(df)
    if total <= rows_per_chunk:
        return file_path.name, 0
    base_name = file_path.stem
    chunks = 0
    for i in range(0, total, rows_per_chunk):
        chunk = df.slice(i, rows_per_chunk)
        chunk_num = chunks + 1
        out_file = output_dir / f"{base_name}_part_{chunk_num:04d}.csv"
        chunk.write_csv(str(out_file))
        chunks += 1
    return file_path.name, chunks

def process(file_path: Path, rows_per_chunk: int, output_dir: Path, engine: str):
    if engine == 'cudf':
        from gpu.cudf_funcs import split_file
        return split_file(file_path, rows_per_chunk, output_dir)
    if engine == 'polars':
        return polars(file_path, rows_per_chunk, output_dir)
    return stdlib(file_path, rows_per_chunk, output_dir)