from pathlib import Path

def remove_duplicates(file_path: Path):
    import hipdf
    df = hipdf.read_csv(str(file_path))
    total = len(df)
    df_unique = df.drop_duplicates()
    unique = len(df_unique)
    if total != unique:
        df_unique.to_csv(str(file_path), index=False)
    return file_path.name, total, unique

def split_file(file_path: Path, rows_per_chunk: int, output_dir: Path):
    import hipdf
    df = hipdf.read_csv(str(file_path))
    total = len(df)
    if total <= rows_per_chunk:
        return file_path.name, 0
    base_name = file_path.stem
    chunks = 0
    for i in range(0, total, rows_per_chunk):
        chunk = df.iloc[i:i + rows_per_chunk]
        chunk_num = chunks + 1
        out_file = output_dir / f"{base_name}_part_{chunk_num:04d}.csv"
        chunk.to_csv(str(out_file), index=False)
        chunks += 1
    return file_path.name, chunks