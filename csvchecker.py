import os
from pathlib import Path
from typing import List

class CSVProcessor:
    banner = r"""
                                         /$$                           /$$                                
                                        | $$                          | $$                                
  /$$$$$$$  /$$$$$$$ /$$    /$$ /$$$$$$$| $$$$$$$   /$$$$$$   /$$$$$$$| $$   /$$  /$$$$$$   /$$$$$$       
 /$$_____/ /$$_____/|  $$  /$$//$$_____/| $$__  $$ /$$__  $$ /$$_____/| $$  /$$/ /$$__  $$ /$$__  $$      
| $$      |  $$$$$$  \  $$/$$/| $$      | $$  \ $$| $$$$$$$$| $$      | $$$$$$/ | $$$$$$$$| $$  \__/      
| $$       \____  $$  \  $$$/ | $$      | $$  | $$| $$_____/| $$      | $$_  $$ | $$_____/| $$            
|  $$$$$$$ /$$$$$$$/   \  $/  |  $$$$$$$| $$  | $$|  $$$$$$$|  $$$$$$$| $$ \  $$|  $$$$$$$| $$            
 \_______/|_______/     \_/    \_______/|__/  |__/ \_______/ \_______/|__/  \__/ \_______/|__/                                                                                                       
"""
   
    __slots__ = ('ops', 'max_workers', 'chunk_size', '_has_deps', '_tqdm', '_fore', '_polars_available', '_chardet_available', '_chardet')
   
    def __init__(self):
        self.ops = {
            "1": ("remove duplicates from csv", self._remove_dupes),
            "2": ("split csv files", self._split_csv),
            "3": ("convert csv encoding", self._convert_encoding),
            "4": ("my github", self._open_github)
        }
        self.max_workers = min(32, (os.cpu_count() or 1) * 2)
        self.chunk_size = 8192
        self._has_deps = None
        self._tqdm = None
        self._fore = None
        self._polars_available = None
        self._chardet_available = None
        self._chardet = None
   
    def _check_polars(self):
        if self._polars_available is not None:
            return self._polars_available
       
        try:
            import polars
            self._polars_available = True
        except ImportError:
            self._polars_available = False
       
        return self._polars_available
   
    def _check_chardet(self):
        if self._chardet_available is not None:
            return self._chardet_available
       
        try:
            import chardet
            self._chardet_available = True
            self._chardet = chardet
        except ImportError:
            self._chardet_available = False
            self._chardet = None
       
        return self._chardet_available
   
    def _init_deps(self):
        if self._has_deps is not None:
            return
       
        try:
            from tqdm import tqdm
            from colorama import Fore, init
            init(autoreset=True)
            self._has_deps = True
            self._tqdm = tqdm
            self._fore = Fore
        except ImportError:
            self._has_deps = False
   
    def _clear(self):
        os.system('cls' if os.name == 'nt' else 'clear')
   
    def _banner(self):
        self._init_deps()
        print(self._fore.BLUE + self.banner if self._has_deps else self.banner)
   
    def _get_csvs(self, directory: Path) -> List[Path]:
        return sorted(directory.glob('*.csv'))
   
    def _remove_duplicate_rows_polars(self, file_path: Path) -> tuple[str, int, int]:
        import polars as pl
       
        try:
            df = pl.read_csv(
                file_path,
                encoding='utf-8',
                ignore_errors=True,
                infer_schema_length=10000,
                rechunk=True
            )
           
            total_rows = len(df)
            df_unique = df.unique(maintain_order=True)
            unique_rows = len(df_unique)
           
            if total_rows != unique_rows:
                df_unique.write_csv(file_path)
           
            return (file_path.name, total_rows, unique_rows)
           
        except Exception as e:
            raise Exception(f"error processing {file_path.name}: {e}")
   
    def _remove_duplicate_rows_stdlib(self, file_path: Path) -> tuple[str, int, int]:
        temp = file_path.with_suffix('.tmp')
        seen = set()
        total_rows = 0
        unique_rows = 0
       
        try:
            with open(file_path, 'r', encoding='utf-8', newline='', errors='replace') as inf:
                lines = inf.readlines()
           
            if not lines:
                return (file_path.name, 0, 0)
           
            header = lines[0]
            total_rows = len(lines) - 1
           
            with open(temp, 'w', encoding='utf-8', newline='', buffering=self.chunk_size * 128) as outf:
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
            return (file_path.name, total_rows, unique_rows)
           
        except Exception as e:
            if temp.exists():
                temp.unlink()
            raise Exception(f"error processing {file_path.name}: {e}")
   
    def _split_csv_polars(self, file_path: Path, rows_per_chunk: int, output_dir: Path) -> tuple[str, int]:
        import polars as pl
       
        try:
            df = pl.read_csv(
                file_path,
                encoding='utf-8',
                ignore_errors=True,
                infer_schema_length=10000
            )
           
            total_rows = len(df)
            chunks_created = 0
           
            if total_rows <= rows_per_chunk:
                return (file_path.name, 0)
           
            base_name = file_path.stem
           
            for i in range(0, total_rows, rows_per_chunk):
                chunk = df.slice(i, rows_per_chunk)
                chunk_num = (i // rows_per_chunk) + 1
                output_file = output_dir / f"{base_name}_part_{chunk_num:04d}.csv"
                chunk.write_csv(output_file)
                chunks_created += 1
           
            return (file_path.name, chunks_created)
           
        except Exception as e:
            raise Exception(f"error splitting {file_path.name}: {e}")
   
    def _split_csv_stdlib(self, file_path: Path, rows_per_chunk: int, output_dir: Path) -> tuple[str, int]:
        try:
            with open(file_path, 'r', encoding='utf-8', newline='', errors='replace') as inf:
                header = inf.readline()
               
                if not header:
                    return (file_path.name, 0)
               
                base_name = file_path.stem
                chunk_num = 1
                chunks_created = 0
                current_chunk = []
               
                for line in inf:
                    current_chunk.append(line)
                   
                    if len(current_chunk) >= rows_per_chunk:
                        output_file = output_dir / f"{base_name}_part_{chunk_num:04d}.csv"
                        with open(output_file, 'w', encoding='utf-8', newline='', buffering=self.chunk_size * 128) as outf:
                            outf.write(header)
                            outf.writelines(current_chunk)
                       
                        chunks_created += 1
                        chunk_num += 1
                        current_chunk.clear()
               
                if current_chunk:
                    output_file = output_dir / f"{base_name}_part_{chunk_num:04d}.csv"
                    with open(output_file, 'w', encoding='utf-8', newline='', buffering=self.chunk_size * 128) as outf:
                        outf.write(header)
                        outf.writelines(current_chunk)
                    chunks_created += 1
               
                return (file_path.name, chunks_created)
               
        except Exception as e:
            raise Exception(f"error splitting {file_path.name}: {e}")
   
    def _split_csv(self):
        path_input = input("enter csv directory path: ").strip()
        if not path_input:
            print("empty path")
            return
       
        directory = Path(path_input).expanduser().resolve()
        if not directory.exists() or not directory.is_dir():
            print("invalid directory")
            return
       
        files = self._get_csvs(directory)
        if not files:
            print("no csv files found")
            return
       
        rows_input = input("enter rows per chunk (default 10000): ").strip()
        rows_per_chunk = int(rows_input) if rows_input.isdigit() else 10000
       
        if rows_per_chunk < 1:
            print("invalid chunk size")
            return
       
        output_dir = directory / "split_output"
        output_dir.mkdir(exist_ok=True)
       
        use_polars = self._check_polars()
        engine = "polars" if use_polars else "stdlib"
        print(f"found {len(files)} csv files (engine: {engine})")
        print(f"splitting into chunks of {rows_per_chunk} rows")
        print(f"output directory: {output_dir}")
       
        from concurrent.futures import ThreadPoolExecutor, as_completed
       
        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            func = self._split_csv_polars if use_polars else self._split_csv_stdlib
            futures = {executor.submit(func, f, rows_per_chunk, output_dir): f for f in files}
           
            if self._has_deps:
                for future in self._tqdm(as_completed(futures), total=len(files), desc="splitting", unit='file'):
                    try:
                        name, chunks = future.result()
                        if chunks > 0:
                            self._tqdm.write(f"{name}: created {chunks} chunks")
                        else:
                            self._tqdm.write(f"{name}: too small to split")
                    except Exception as e:
                        self._tqdm.write(f"error: {e}")
            else:
                done = 0
                for future in as_completed(futures):
                    done += 1
                    try:
                        name, chunks = future.result()
                        if chunks > 0:
                            print(f"[{done}/{len(files)}] {name}: {chunks} chunks")
                        else:
                            print(f"[{done}/{len(files)}] {name}: too small")
                    except Exception as e:
                        print(f"error: {e}")
       
        print("splitting completed")
   
    def _detect_encoding(self, file_path: Path) -> str:
        with open(file_path, 'rb') as f:
            detector = self._chardet.universal_detector.UniversalDetector()
            for _ in range(128):
                chunk = f.read(self.chunk_size)
                if not chunk:
                    break
                detector.feed(chunk)
                if detector.done:
                    break
            detector.close()
        enc = detector.result.get('encoding')
        return enc if enc else 'utf-8'
   
    def _convert_file(self, file_path: Path, source: str, target: str, output_dir: Path) -> tuple[str, str]:
        try:
            if source == 'auto':
                source_enc = self._detect_encoding(file_path)
            else:
                source_enc = source
           
            if source_enc.lower() == target.lower():
                return (file_path.name, 'skipped - same encoding')
           
            output_file = output_dir / file_path.name
           
            with open(file_path, 'r', encoding=source_enc, errors='replace', newline='') as inf, \
                 open(output_file, 'w', encoding=target, errors='replace', newline='') as outf:
                for line in inf:
                    outf.write(line)
           
            return (file_path.name, f'converted from {source_enc} to {target}')
        except Exception as e:
            raise Exception(f"error converting {file_path.name}: {e}")
   
    def _convert_encoding(self):
        path_input = input("enter csv directory path: ").strip()
        if not path_input:
            print("empty path")
            return
       
        directory = Path(path_input).expanduser().resolve()
        if not directory.exists() or not directory.is_dir():
            print("invalid directory")
            return
       
        files = self._get_csvs(directory)
        if not files:
            print("no csv files found")
            return
       
        encodings = ['auto', 'windows-1251', 'utf-16', 'utf-8', 'utf-16le', 'utf-16be', 'cp1252', 'iso-8859-1']
        target_encodings = [e for e in encodings if e != 'auto']
       
        print("\nselect source encoding:")
        for i, enc in enumerate(encodings, 1):
            print(f"[{i}] {enc}")
        choice = input().strip()
        if not choice.isdigit() or not 1 <= int(choice) <= len(encodings):
            print("invalid choice")
            return
        source = encodings[int(choice) - 1]
       
        if source == 'auto' and not self._check_chardet():
            print("chardet not available for auto detection. install with pip install chardet")
            return
       
        print("\nselect target encoding:")
        for i, enc in enumerate(target_encodings, 1):
            print(f"[{i}] {enc}")
        choice = input().strip()
        if not choice.isdigit() or not 1 <= int(choice) <= len(target_encodings):
            print("invalid choice")
            return
        target = target_encodings[int(choice) - 1]
       
        if source != 'auto' and source.lower() == target.lower():
            print("source and target are the same")
            return
       
        output_dir = directory / "converted_output"
        output_dir.mkdir(exist_ok=True)
       
        print(f"found {len(files)} csv files")
        print(f"converting from {source} to {target}")
        print(f"output directory: {output_dir}")
       
        from concurrent.futures import ThreadPoolExecutor, as_completed
       
        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            futures = {executor.submit(self._convert_file, f, source, target, output_dir): f for f in files}
           
            if self._has_deps:
                for future in self._tqdm(as_completed(futures), total=len(files), desc="converting", unit='file'):
                    try:
                        name, status = future.result()
                        self._tqdm.write(f"{name}: {status}")
                    except Exception as e:
                        self._tqdm.write(f"error: {e}")
            else:
                done = 0
                for future in as_completed(futures):
                    done += 1
                    try:
                        name, status = future.result()
                        print(f"[{done}/{len(files)}] {name}: {status}")
                    except Exception as e:
                        print(f"error: {e}")
       
        print("conversion completed")
   
    def _process_parallel(self, files: List[Path], func, operation: str):
        from concurrent.futures import ThreadPoolExecutor, as_completed
       
        if not files:
            print("no csv files found")
            return
       
        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            futures = {executor.submit(func, f): f for f in files}
           
            if self._has_deps:
                for future in self._tqdm(as_completed(futures), total=len(files), desc=operation, unit='file'):
                    try:
                        result = future.result()
                        if operation == "removing duplicates" and result:
                            name, total, unique = result
                            if total != unique:
                                self._tqdm.write(f"{name}: {total - unique} duplicates removed")
                    except Exception as e:
                        self._tqdm.write(f"error: {e}")
            else:
                done = 0
                for future in as_completed(futures):
                    done += 1
                    try:
                        result = future.result()
                        if operation == "removing duplicates" and result:
                            name, total, unique = result
                            status = f"[{done}/{len(files)}] {name}"
                            if total != unique:
                                status += f" ({total - unique} dupes)"
                            print(status)
                        else:
                            print(f"[{done}/{len(files)}] processed")
                    except Exception as e:
                        print(f"error: {e}")
   
    def _remove_dupes(self):
        path_input = input("enter csv directory path: ").strip()
        if not path_input:
            print("empty path")
            return
       
        directory = Path(path_input).expanduser().resolve()
        if not directory.exists() or not directory.is_dir():
            print("invalid directory")
            return
       
        files = self._get_csvs(directory)
        if not files:
            print("no csv files found")
            return
       
        use_polars = self._check_polars()
        engine = "polars" if use_polars else "stdlib"
        print(f"found {len(files)} csv files (engine: {engine})")
       
        func = self._remove_duplicate_rows_polars if use_polars else self._remove_duplicate_rows_stdlib
        self._process_parallel(files, func, "removing duplicates")
        print("duplicates removed")
   
    def _open_github(self):
        import webbrowser
       
        try:
            webbrowser.open("https://github.com/s1z1-balance")
            print("github opened in browser")
        except Exception:
            print("github: https://github.com/s1z1-balance")
   
    def run(self):
        try:
            self._clear()
            self._banner()
           
            while True:
                print('\n'.join(f"[{k}] {desc}" for k, (desc, _) in self.ops.items()))
                print("[0] exit")
               
                choice = input("\n┌──(csvchecker@root)\n└─$ ").strip()
               
                if choice == "0":
                    print("goodbye..")
                    break
               
                if choice in self.ops:
                    try:
                        self.ops[choice][1]()
                        input("\npress enter to continue..")
                        self._clear()
                        self._banner()
                    except Exception as e:
                        print(f"\nerror: {e}")
                        input("\npress enter to continue..")
                        self._clear()
                        self._banner()
                else:
                    print("invalid choice")
                    input("\npress enter to continue..")
                    self._clear()
                    self._banner()
       
        except KeyboardInterrupt:
            print("\n\ngoodbye..")
def main():
    processor = CSVProcessor()
    processor.run()
if __name__ == "__main__":
    main()
