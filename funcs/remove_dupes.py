# funcs/remove_dupes.py
from pathlib import Path

def stdlib(file_path: Path):
    temp = file_path.with_suffix('.tmp')
    seen = set()
    total_rows = 0
    unique_rows = 0
    try:
        with open(file_path, 'r', encoding='utf-8', newline='', errors='replace') as inf:
            lines = inf.readlines()
        if not lines:
            return file_path.name, 0, 0
        header = lines[0]
        total_rows = len(lines) - 1
        with open(temp, 'w', encoding='utf-8', newline='', buffering=8192*128) as outf:
            outf.write(header)
            batch = []
            batch_size = 1000
            for line in lines[1:]:
                if line not in seen:
                    seen.add(line)
                    batch.append(line)
                    unique_rows += 1
                    if len(batch) >= batch_size:
                        outf.writelines(batch)
                        batch.clear()
            if batch:
                outf.writelines(batch)
        temp.replace(file_path)
        return file_path.name, total_rows, unique_rows
    except Exception as e:
        if temp.exists():
            temp.unlink()
        raise Exception(f"error {file_path.name}: {e}")

def polars(file_path: Path):
    import polars as pl
    df = pl.read_csv(str(file_path), encoding='utf-8', ignore_errors=True, infer_schema_length=10000, rechunk=True)
    total = len(df)
    df_unique = df.unique(maintain_order=True)
    unique = len(df_unique)
    if total != unique:
        df_unique.write_csv(str(file_path))
    return file_path.name, total, unique

def process(file_path: Path, engine: str):
    if engine == 'cudf':
        from gpu.cudf_funcs import remove_duplicates
        return remove_duplicates(file_path)
    if engine == 'polars':
        return polars(file_path)
    return stdlib(file_path)