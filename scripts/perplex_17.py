#!/usr/bin/env python3
import os
import csv
import re
from tqdm import tqdm
from rich.console import Console
from rich.table import Table

# ==== Настройки ====
GROUP_CSV = "group_2.csv"
MAPPING_CSV = "mapping.csv"
OUTPUT_NEW_JOBS = "to_copy.csv"
OUTPUT_NEW_MAPPING = "to_add_mapping.csv"
OUTPUT_TAR_LIST = "tar_list.txt"

TARGET_MOUNTS = [
    "/mnt/disk_C1",
    "/mnt/disk_B7",
    "/mnt/disk_B8",
    "/mnt/disk_B9",
    "/mnt/disk_B10",
    "/mnt/disk_B2",
    "/mnt/disk_C4",
    "/mnt/disk_C2",
    "/mnt/disk_C3"
]
SOURCE_PREFIX = "/mnt/cifs/raidix/#OLD_FILMS/"
TAR_PATTERN = re.compile(r"\.(\d+)-(\d+)\.(\w+)\.tar$", re.IGNORECASE)

console = Console()

# === Загрузка заданий с полным сохранением строк ===
def load_jobs_full():
    jobs_full = set()
    jobs_paths = dict()  # path до ';' => полная строка
    with open(GROUP_CSV, encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            jobs_full.add(line)
            path = line.split(';', 1)[0]
            jobs_paths[path] = line
    return jobs_full, jobs_paths

# === Загрузка mapping ===
def load_mapping():
    mapping = {}
    with open(MAPPING_CSV, newline="", encoding="utf-8") as f:
        reader = csv.reader(f)
        for src, dst in reader:
            mapping[src] = dst
    return mapping

# === Сканирование дисков только #OLD_FILMS ===
def scan_disks():
    found_files = set()
    tar_files = set()
    for mount in TARGET_MOUNTS:
        old_prefix = os.path.join(mount, "raidix/#OLD_FILMS") + "/"
        if not os.path.exists(old_prefix):
            continue
        for root, dirs, files in tqdm(os.walk(old_prefix), desc=f"Скан {mount}", unit="dir"):
            for name in files:
                fpath = os.path.join(root, name)
                found_files.add(fpath)
                if TAR_PATTERN.search(name):
                    tar_files.add(fpath)
    return found_files, tar_files

def build_dest_paths(rel_path):
    return [m + "/raidix/#OLD_FILMS/" + rel_path for m in TARGET_MOUNTS]

# === Основная функция ===
def main():
    jobs_full, jobs_paths = load_jobs_full()
    mapping = load_mapping()
    found_files, tar_files = scan_disks()

    # Сохраняем список таров
    with open(OUTPUT_TAR_LIST, "w", encoding="utf-8") as f:
        for path in sorted(tar_files):
            f.write(path + "\n")
    console.print(f"[green]Список TAR сохранён в {OUTPUT_TAR_LIST}[/green] (всего {len(tar_files)})")

    jobs_with_file = 0
    jobs_without_file = 0
    jobs_with_file_and_mapping = 0
    extra_files = []
    new_jobs_for_copy = []
    new_mapping_rows = []

    job_paths_set = set(jobs_paths.keys())

    for full_line in tqdm(jobs_full, desc="Проверка заданий"):
        rel_path = full_line.split(';', 1)[0]

        actual_path = None
        for path in build_dest_paths(rel_path):
            if path in found_files:
                actual_path = path
                break

        if actual_path:
            jobs_with_file += 1
            in_mapping = False
            if full_line in mapping and mapping[full_line] == actual_path:
                in_mapping = True
            else:
                # Иногда в mapping может быть другой src, поэтому попробуем поиск по ключу rel_path с SOURCE_PREFIX
                src_key = SOURCE_PREFIX + rel_path
                if src_key in mapping and mapping[src_key] == actual_path:
                    in_mapping = True

            if in_mapping:
                jobs_with_file_and_mapping += 1
            else:
                new_mapping_rows.append((full_line, actual_path))
        else:
            jobs_without_file += 1
            new_jobs_for_copy.append(full_line)

    # Файлы без задания
    for f in found_files:
        rel_path = None
        for mount in TARGET_MOUNTS:
            prefix = mount + "/raidix/#OLD_FILMS/"
            if f.startswith(prefix):
                rel_path_candidate = f.replace(prefix, "")
                if rel_path_candidate not in job_paths_set:
                    extra_files.append(f)
                break

    total_jobs = len(jobs_full)
    total_files = len(found_files)
    total_tar = len(tar_files)
    total_mapping_lines = len(mapping)
    missing_in_mapping = jobs_with_file - jobs_with_file_and_mapping

    mapped_dsts = set(mapping.values())
    on_disk_in_mapping = len(found_files & mapped_dsts)
    on_disk_not_in_mapping = total_files - on_disk_in_mapping

    # Вывод отчёта
    table = Table(title="Сводка копирования", show_lines=True)
    table.add_column("Статус", style="bold cyan")
    table.add_column("Количество", style="bold yellow", justify="right")

    table.add_row("Всего строк заданий", str(total_jobs))
    table.add_row("Всего файлов на дисках", str(total_files))
    table.add_row("   ├─ из них TAR", str(total_tar))
    table.add_row("Строк заданий с файлом", str(jobs_with_file))
    table.add_row("Строк заданий без файла", str(jobs_without_file))
    table.add_row("Файлов без задания", str(len(extra_files)))
    table.add_row("Всего строк в mapping.csv", str(total_mapping_lines))
    table.add_row("Не хватает в mapping (по заданиям)", str(missing_in_mapping))
    table.add_row("Файлов на дисках в mapping", str(on_disk_in_mapping))
    table.add_row("Файлов на дисках вне mapping", str(on_disk_not_in_mapping))
    console.print(table)

    # Запись файлов для докопирования и дополнения mapping
    with open(OUTPUT_NEW_JOBS, "w", encoding="utf-8") as f:
        for line in sorted(new_jobs_for_copy):
            f.write(line + "\n")

    with open(OUTPUT_NEW_MAPPING, "w", encoding="utf-8") as f:
        for src_full, dst_full in sorted(new_mapping_rows):
            f.write(f"{src_full},{dst_full}\n")

    console.print(f"[green]Файл на докопирование:[/green] {OUTPUT_NEW_JOBS}")
    console.print(f"[green]Файл для дополнения mapping:[/green] {OUTPUT_NEW_MAPPING}")

if __name__ == "__main__":
    main()
