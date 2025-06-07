#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import csv
import os
from pathlib import Path
from typing import List
import time
import webbrowser
from concurrent.futures import ThreadPoolExecutor, as_completed

try:
    from tqdm import tqdm
    from colorama import Fore, init
    init(autoreset=True)
    HAS_DEPS = True
except ImportError:
    HAS_DEPS = False

class CSVProcessor:
    BANNER = r"""
 .o88b. .d8888. db    db  .o88b. db   dB d88888b  .o88b. db   dD d88888b d8888b.
d8P  Y8 88'  YP 88    88 d8P  Y8 88   88 88'     d8P  Y8 88 ,8P' 88'     88  `8D
8P      `8bo.   Y8    8P 8P      88ooo88 88ooooo 8P      88,8P   88ooooo 88oobY'
8b        `Y8b. `8b  d8' 8b      88~~~88 88~~~~~ 8b      88`8b   88~~~~~ 88`8b
Y8b  d8 db   8D  `8bd8'  Y8b  d8 88   88 88.     Y8b  d8 88 `88. 88.     88 `88.
 `Y88P' `8888Y'    YP     `Y88P' YP   YP Y88888P  `Y88P' YP   YD Y88888P 88   YD
"""
    
    def __init__(self):
        self.ops = {
            "1": ("remove duplicates from csv", self._remove_dupes),
            "2": ("clean non-utf8 chars", self._clean_chars),
            "3": ("my github", self._open_github)
        }
    
    def _clear(self):
        os.system('cls' if os.name == 'nt' else 'clear')
    
    def _banner(self):
        if HAS_DEPS:
            print(Fore.BLUE + self.BANNER)
        else:
            print(self.BANNER)
    
    def _get_csvs(self, directory: Path) -> List[Path]:
        return list(directory.glob('*.csv'))
    
    def _remove_duplicate_rows(self, file_path: Path):
        temp = file_path.with_suffix('.tmp')
        seen = set()
        
        try:
            with open(file_path, 'r', encoding='utf-8', newline='') as inf, \
                 open(temp, 'w', encoding='utf-8', newline='') as outf:
                
                reader = csv.reader(inf)
                writer = csv.writer(outf)
                
                header = next(reader, None)
                if header:
                    writer.writerow(header)
                
                for row in reader:
                    row_tuple = tuple(row)
                    if row_tuple not in seen:
                        seen.add(row_tuple)
                        writer.writerow(row)
            
            temp.replace(file_path)
            
        except Exception as e:
            if temp.exists():
                temp.unlink()
            print(f"error processing {file_path.name}: {e}")
    
    def _clean_file_chars(self, file_path: Path):
        temp = file_path.with_suffix('.tmp')
        
        try:
            with open(file_path, 'r', encoding='utf-8', errors='replace', newline='') as inf, \
                 open(temp, 'w', encoding='utf-8', newline='') as outf:
                
                reader = csv.reader(inf)
                writer = csv.writer(outf)
                
                for row in reader:
                    cleaned = [
                        cell.encode('utf-8', 'ignore').decode('utf-8') if isinstance(cell, str) else cell
                        for cell in row
                    ]
                    writer.writerow(cleaned)
            
            temp.replace(file_path)
            
        except Exception as e:
            if temp.exists():
                temp.unlink()
            print(f"error cleaning {file_path.name}: {e}")
    
    def _process_parallel(self, files: List[Path], func):
        if not files:
            print("no csv files found")
            return
        
        with ThreadPoolExecutor(max_workers=4) as executor:
            if HAS_DEPS:
                futures = {executor.submit(func, f): f for f in files}
                for future in tqdm(as_completed(futures), total=len(files), desc="processing"):
                    try:
                        future.result()
                    except Exception as e:
                        print(f"error: {e}")
            else:
                futures = {executor.submit(func, f): f for f in files}
                done = 0
                for future in as_completed(futures):
                    done += 1
                    try:
                        future.result()
                        print(f"[{done}/{len(files)}] processed")
                    except Exception as e:
                        print(f"error: {e}")
    
    def _remove_dupes(self):
        path = input("enter csv directory path: ").strip()
        if not path:
            print("empty path")
            return
        
        directory = Path(path)
        if not directory.exists() or not directory.is_dir():
            print("invalid directory")
            return
        
        files = self._get_csvs(directory)
        print(f"found {len(files)} csv files")
        self._process_parallel(files, self._remove_duplicate_rows)
        print("duplicates removed")
    
    def _clean_chars(self):
        path = input("enter csv directory path: ").strip()
        if not path:
            print("empty path")
            return
        
        directory = Path(path)
        if not directory.exists() or not directory.is_dir():
            print("invalid directory")
            return
        
        files = self._get_csvs(directory)
        print(f"found {len(files)} csv files")
        self._process_parallel(files, self._clean_file_chars)
        print("chars cleaned")
    
    def _open_github(self):
        try:
            webbrowser.open("https://github.com/s1z1-balance")
            print("github opened")
        except:
            print("github: https://github.com/s1z1-balance")
    
    def run(self):
        try:
            self._clear()
            self._banner()
            
            while True:
                for k, (desc, _) in self.ops.items():
                    print(f"[{k}] {desc}")
                print("[0] exit")
                
                choice = input("\n┌──(csvprocessor@root)\n└─$ ").strip()
                
                if choice == "0":
                    print("goodbye..")
                    break
                
                if choice in self.ops:
                    try:
                        self.ops[choice][1]()
                        input("press enter..")
                        self._clear()
                        self._banner()
                    except Exception as e:
                        print(f"error: {e}")
                        time.sleep(1)
                        self._clear()
                        self._banner()
                else:
                    print("wrong choice")
                    time.sleep(0.8)
                    self._clear()
                    self._banner()
        
        except KeyboardInterrupt:
            print("\ngoodbye..")

def main():
    processor = CSVProcessor()
    processor.run()

if __name__ == "__main__":
    main()