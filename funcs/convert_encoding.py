# funcs/convert_encoding.py
from pathlib import Path

def detect(file_path: Path) -> str:
    import chardet
    with open(file_path, 'rb') as f:
        detector = chardet.universal_detector.UniversalDetector()
        for _ in range(128):
            chunk = f.read(8192)
            if not chunk:
                break
            detector.feed(chunk)
            if detector.done:
                break
        detector.close()
    return detector.result.get('encoding') or 'utf-8'

def process(file_path: Path, source: str, target: str, output_dir: Path):
    try:
        src_enc = detect(file_path) if source == 'auto' else source
        if src_enc.lower() == target.lower():
            return file_path.name, 'skipped (same encoding)'
        out_file = output_dir / file_path.name
        with open(file_path, 'r', encoding=src_enc, errors='replace', newline='') as inf, \
             open(out_file, 'w', encoding=target, errors='replace', newline='') as outf:
            for line in inf:
                outf.write(line)
        return file_path.name, f'converted {src_enc} -> {target}'
    except Exception as e:
        raise Exception(f"error {file_path.name}: {e}")