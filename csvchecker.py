import os
from pathlib import Path
from typing import List
from concurrent.futures import ThreadPoolExecutor, as_completed

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

    __slots__ = ('ops', 'max_workers', 'chunk_size', '_has_deps', '_tqdm', '_fore', '_polars_available',
                 '_chardet_available', '_chardet', 'gpu_enabled', 'gpu_vendor', '_cudf_available', '_hipdf_available')

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
        self.gpu_enabled = False
        self.gpu_vendor = None
        self._cudf_available = None
        self._hipdf_available = None
        self._detect_gpu()

    def _detect_gpu(self):
        if self.gpu_vendor is not None:
            return
        
        has_cudf = self._check_cudf()
        has_hipdf = self._check_hipdf()
        
        if has_cudf:
            self.gpu_vendor = 'nvidia'
            return
        if has_hipdf:
            self.gpu_vendor = 'amd'
            return
        
        try:
            import torch
            if torch.cuda.is_available():
                name = torch.cuda.get_device_name(0).lower()
                if any(k in name for k in ['amd', 'radeon', 'rx', 'instinct']):
                    self.gpu_vendor = 'amd'
                else:
                    self.gpu_vendor = 'nvidia'
        except Exception:
            pass

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

    def _check_cudf(self):
        if self._cudf_available is not None:
            return self._cudf_available
        try:
            import cudf
            self._cudf_available = True
        except ImportError:
            self._cudf_available = False
        return self._cudf_available

    def _check_hipdf(self):
        if self._hipdf_available is not None:
            return self._hipdf_available
        try:
            import hipdf
            self._hipdf_available = True
        except ImportError:
            self._hipdf_available = False
        return self._hipdf_available

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

        status = "gpu acceleration: "
        if self.gpu_enabled:
            status += "enabled"
            if self.gpu_vendor == 'nvidia':
                status += " (cuda)"
                color = self._fore.GREEN
            elif self.gpu_vendor == 'amd':
                status += " (rocm)"
                color = self._fore.RED
            else:
                color = self._fore.YELLOW
        else:
            status += "disabled"
            color = self._fore.WHITE if self._has_deps else ""

        print(color + status if self._has_deps else status)

    def _get_csvs(self, directory: Path) -> List[Path]:
        return sorted(directory.glob('*.csv'))

    def _toggle_gpu(self):
        self._detect_gpu()
        
        if not self.gpu_vendor:
            print("no gpu detected")
            self.gpu_enabled = False
            return

        if self.gpu_vendor == 'amd':
            print("amd gpu detected. for full gpu acceleration you need rocm + hipdf")

        self.gpu_enabled = not self.gpu_enabled
        print(f"gpu acceleration now {'enabled' if self.gpu_enabled else 'disabled'}")

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

        use_hipdf = self.gpu_enabled and self.gpu_vendor == 'amd' and self._check_hipdf()
        use_cudf = self.gpu_enabled and self.gpu_vendor == 'nvidia' and self._check_cudf()
        use_gpu = use_hipdf or use_cudf
        use_polars = self._check_polars() if not use_gpu else False
        engine = 'hipdf' if use_hipdf else 'cudf' if use_cudf else 'polars' if use_polars else 'stdlib'

        accel = ""
        if use_gpu:
            accel = f" (gpu accelerated via {'rocm' if self.gpu_vendor == 'amd' else 'cuda'})"

        if self.gpu_enabled and self.gpu_vendor and not use_gpu:
            print("warning: no gpu library available (hipdf or cudf) - falling back to polars or stdlib")

        print(f"found {len(files)} csv files")
        print(f"engine: {engine}{accel}")

        from funcs.remove_dupes import process as proc_func

        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            futures = {executor.submit(proc_func, f, engine): f for f in files}
            if self._has_deps:
                for future in self._tqdm(as_completed(futures), total=len(files), desc="removing duplicates", unit="file"):
                    try:
                        name, total, unique = future.result()
                        removed = total - unique
                        if removed:
                            self._tqdm.write(f"{name}: {removed} duplicates removed")
                        else:
                            self._tqdm.write(f"{name}: no duplicates")
                    except Exception as e:
                        self._tqdm.write(f"error: {e}")
            else:
                done = 0
                for future in as_completed(futures):
                    done += 1
                    try:
                        name, total, unique = future.result()
                        removed = total - unique
                        print(f"[{done}/{len(files)}] {name}: {removed} dupes removed" if removed else f"[{done}/{len(files)}] {name}: clean")
                    except Exception as e:
                        print(f"error: {e}")
        print("duplicates removal completed")

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

        use_hipdf = self.gpu_enabled and self.gpu_vendor == 'amd' and self._check_hipdf()
        use_cudf = self.gpu_enabled and self.gpu_vendor == 'nvidia' and self._check_cudf()
        use_gpu = use_hipdf or use_cudf
        use_polars = self._check_polars() if not use_gpu else False
        engine = 'hipdf' if use_hipdf else 'cudf' if use_cudf else 'polars' if use_polars else 'stdlib'

        accel = ""
        if use_gpu:
            accel = f" (gpu accelerated via {'rocm' if self.gpu_vendor == 'amd' else 'cuda'})"

        if self.gpu_enabled and self.gpu_vendor and not use_gpu:
            print("warning: no gpu library available (hipdf or cudf) - falling back to polars or stdlib")

        print(f"found {len(files)} csv files")
        print(f"engine: {engine}{accel}")
        print(f"chunks of {rows_per_chunk} rows -> {output_dir}")

        from funcs.split_csv import process as proc_func

        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            futures = {executor.submit(proc_func, f, rows_per_chunk, output_dir, engine): f for f in files}
            if self._has_deps:
                for future in self._tqdm(as_completed(futures), total=len(files), desc="splitting", unit="file"):
                    try:
                        name, chunks = future.result()
                        if chunks:
                            self._tqdm.write(f"{name}: {chunks} chunks created")
                        else:
                            self._tqdm.write(f"{name}: too small")
                    except Exception as e:
                        self._tqdm.write(f"error: {e}")
            else:
                done = 0
                for future in as_completed(futures):
                    done += 1
                    try:
                        name, chunks = future.result()
                        print(f"[{done}/{len(files)}] {name}: {chunks} chunks" if chunks else f"[{done}/{len(files)}] {name}: skipped")
                    except Exception as e:
                        print(f"error: {e}")
        print("splitting completed")

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
        print("\nsource encoding:")
        for i, enc in enumerate(encodings, 1):
            print(f"[{i}] {enc}")
        choice = input().strip()
        if not choice.isdigit() or not 1 <= int(choice) <= len(encodings):
            print("invalid")
            return
        source = encodings[int(choice) - 1]
        if source == 'auto' and not self._check_chardet():
            print("chardet not installed")
            return

        print("\ntarget encoding:")
        for i, enc in enumerate(target_encodings, 1):
            print(f"[{i}] {enc}")
        choice = input().strip()
        if not choice.isdigit() or not 1 <= int(choice) <= len(target_encodings):
            print("invalid")
            return
        target = target_encodings[int(choice) - 1]

        output_dir = directory / "converted_output"
        output_dir.mkdir(exist_ok=True)
        print(f"found {len(files)} files")
        print(f"{source} -> {target}")
        print(f"output: {output_dir}")

        from funcs.convert_encoding import process as proc_func

        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            futures = {executor.submit(proc_func, f, source, target, output_dir): f for f in files}
            if self._has_deps:
                for future in self._tqdm(as_completed(futures), total=len(files), desc="converting", unit="file"):
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

    def _open_github(self):
        import webbrowser
        try:
            webbrowser.open("https://github.com/s1z1-balance")
            print("opened in browser")
        except Exception:
            print("https://github.com/s1z1-balance")

    def run(self):
        try:
            self._clear()
            self._banner()
            while True:
                print("\noperations:")
                for k in sorted(self.ops, key=int):
                    print(f"[{k}] {self.ops[k][0]}")
                print("\n[99] toggle gpu acceleration")
                print("[0] exit")
                choice = input("\n┌──(csvchecker@root)\n└─$ ").strip()
                if choice == "0":
                    print("goodbye")
                    break
                if choice == "99":
                    self._toggle_gpu()
                    input("\npress enter..")
                    self._clear()
                    self._banner()
                    continue
                if choice in self.ops:
                    try:
                        self.ops[choice][1]()
                        input("\npress enter..")
                    except Exception as e:
                        print(f"error: {e}")
                        input("\npress enter..")
                    self._clear()
                    self._banner()
                else:
                    print("invalid")
                    input("\npress enter..")
                    self._clear()
                    self._banner()
        except KeyboardInterrupt:
            print("\ngoodbye")

def main():
    processor = CSVProcessor()
    processor.run()

if __name__ == "__main__":
    main()