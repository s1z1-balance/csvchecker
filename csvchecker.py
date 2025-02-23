import csv
import os
from tqdm import tqdm
import time
import webbrowser
from colorama import Style, init, Fore

init(autoreset=True)

a = r"""
 .o88b. .d8888. db    db  .o88b. db   db d88888b  .o88b. db   dD d88888b d8888b.
d8P  Y8 88'  YP 88    88 d8P  Y8 88   88 88'     d8P  Y8 88 ,8P' 88'     88  `8D
8P      `8bo.   Y8    8P 8P      88ooo88 88ooooo 8P      88,8P   88ooooo 88oobY'
8b        `Y8b. `8b  d8' 8b      88   88 88      8b      88`8b   88      88`8b
Y8b  d8 db   8D  `8bd8'  Y8b  d8 88   88 88.     Y8b  d8 88 `88. 88.     88 `88.
 `Y88P' `8888Y'    YP     `Y88P' YP   YP Y88888P  `Y88P' YP   YD Y88888P 88   YD
"""

def main():
    try:
        os.system("cls" if os.name == "nt" else "clear")
        os.system("title CSVChecker-0.5 (made by @s1z1)" if os.name == "nt" else "")
        
        print(Fore.BLUE + a)

        options = {
             "1": ("remove duplicates from csv", process_csv_files, {"mode": "remove_duplicates"}),
             "2": ("delete not utf-8 csv", process_csv_files, {"mode": "clean_strings"}),
             "3": ("my github", open_github)
        }

        while True:
            for key, (desc, func, *args) in options.items():
                  print(f"[{key}] {desc}")

            choice = input("\n┌──(csvchecker@s1z1)-[home/root/linux]\n└─$").strip()
            if choice == "0":
                print("goodbye..")
                break
            elif choice in options:
                if choice in ["1", "2"]:
                    directory = input("enter the path to the directory with .csv: ").strip()
                    if os.path.isdir(directory):
                        options[choice][1](directory, **options[choice][2])
                    else:
                        print("directory not found!")
                else:
                    options[choice][1]()
            else:
                print("wrong choice!")
                time.sleep(0.8)
                os.system("cls" if os.name == "nt" else "clear")
                print(Fore.BLUE + a)
    except KeyboardInterrupt:
        print("\ngoodbye..")

def open_github():
    webbrowser.open("https://github.com/s1z1-balance")
    time.sleep(0.8)
    os.system("cls" if os.name == "nt" else "clear")
    print(Fore.BLUE + a)

def process_csv_files(directory, mode="remove_duplicates"):
    csv_files = [file for file in os.listdir(directory) if file.endswith(".csv")]
    if not csv_files:
        print("there are no .csv files in the specified directory.")
        time.sleep(0.8)
        os.system("cls" if os.name == "nt" else "clear")
        print(Fore.BLUE + a)
        return

    print(f"detected {len(csv_files)} .csv files. Beginning processing...")

    for csv_file in tqdm(csv_files, desc="file processing"):
        file_path = os.path.join(directory, csv_file)
        try:
            if mode == "remove_duplicates":
                remove_duplicates(file_path)
            elif mode == "clean_strings":
                clean_strings(file_path)
            print(f"File {csv_file} successfully processed.")
        except Exception as e:
            print(f"\nunknown error during file processing {csv_file}: {e}")

def remove_duplicates(file_path):
    temp_file = file_path + ".tmp"
    seen = set()
    with open(file_path, mode='r', encoding='utf-8', newline='') as file, \
         open(temp_file, mode='w', encoding='utf-8', newline='') as temp:
        reader = csv.reader(file)
        writer = csv.writer(temp)
        header = next(reader, None)
        if header:
            writer.writerow(header)
        for row in reader:
            row_tuple = tuple(row)
            if row_tuple not in seen:
                seen.add(row_tuple)
                writer.writerow(row)
    os.replace(temp_file, file_path)

def clean_strings(file_path):
    temp_file = file_path + ".tmp"
    with open(file_path, mode='r', encoding='utf-8', newline='') as file, \
         open(temp_file, mode='w', encoding='utf-8', newline='') as temp:
        reader = csv.reader(file)
        writer = csv.writer(temp)
        header = next(reader, None)
        if header:
            writer.writerow(header)
        for row in reader:
            cleaned_row = [
                col.encode("utf-8", "ignore").decode("utf-8") if isinstance(col, str) else col
                for col in row
            ]
            writer.writerow(cleaned_row)
    os.replace(temp_file, file_path)

if __name__ == "__main__":
    main()

