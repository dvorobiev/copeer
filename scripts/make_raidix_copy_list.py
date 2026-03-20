#!/usr/bin/env python3
"""
Генерация copeer-совместимого CSV для копирования файлов диска с RAIDIX.

Использование:
  python make_raidix_copy_list.py --disk A1 --disks-dir /path/to/disks_files --output output/copy_from_raidix_A1.csv
"""

import argparse
import csv
import os
import re
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

DEFAULT_RAIDIX_ROOT = "/mnt/cifs/raidix"
WORKERS = 32


def disk_path_to_raidix(full_path: str, raidix_root: str) -> str | None:
    """Канонизация пути с диска в путь RAIDIX."""
    m = re.match(r'^/mnt/disk_[^/]+/raidix/(.*)', full_path)
    if m:
        return os.path.join(raidix_root, m.group(1))
    return None


def load_disk_files(disk: str, disks_dir: Path) -> list[tuple[str, str, str, str, str]]:
    """Загружает только обычные файлы из CSV диска."""
    csv_path = disks_dir / f"{disk}_output_file.csv"
    if not csv_path.exists():
        print(f"Ошибка: файл {csv_path} не найден", file=sys.stderr)
        sys.exit(1)

    rows = []
    with open(csv_path, encoding="utf-8", newline="") as f:
        reader = csv.reader(f, delimiter=";", quotechar='"')
        for row in reader:
            if len(row) < 5:
                continue
            if row[1].strip() == "обычный файл":
                rows.append((row[0], row[1], row[2], row[3], row[4]))
    return rows


def _norm_time(ts: str) -> str:
    """Нормализует временну́ю метку до 'YYYY-MM-DD HH:MM:SS'."""
    ts = ts.strip()
    return ts[:19] if len(ts) >= 19 else ts


def check_exists(raidix_path: str) -> bool:
    return os.path.exists(raidix_path)


def main():
    parser = argparse.ArgumentParser(
        description="Генерация copeer-совместимого CSV для копирования с RAIDIX"
    )
    parser.add_argument("--disk", required=True, metavar="DISK",
                        help="Имя диска (например, A1)")
    parser.add_argument("--disks-dir", required=True, metavar="DIR",
                        help="Каталог с CSV-листингами дисков (*_output_file.csv)")
    parser.add_argument("--output", required=True, metavar="FILE",
                        help="Путь к выходному CSV файлу")
    parser.add_argument("--raidix-root", default=DEFAULT_RAIDIX_ROOT,
                        help=f"Корень RAIDIX (по умолчанию: {DEFAULT_RAIDIX_ROOT})")
    args = parser.parse_args()

    disk = args.disk
    raidix_root = args.raidix_root
    disks_dir = Path(args.disks_dir)
    output_path = Path(args.output)

    if not disks_dir.exists():
        print(f"Ошибка: каталог {disks_dir} не найден", file=sys.stderr)
        sys.exit(1)

    print(f"Загружаю список файлов диска {disk}...")
    rows = load_disk_files(disk, disks_dir)
    total = len(rows)
    print(f"Найдено файлов: {total:,}")

    pairs = []
    skipped_parse = 0
    for row in rows:
        rp = disk_path_to_raidix(row[0], raidix_root)
        if rp:
            pairs.append((row, rp))
        else:
            skipped_parse += 1

    print(f"Проверяю наличие файлов на RAIDIX ({WORKERS} потоков)...")

    found_rows = []
    missing_count = 0

    with ThreadPoolExecutor(max_workers=WORKERS) as pool:
        futures = {pool.submit(check_exists, rp): (row, rp) for row, rp in pairs}
        done = 0
        for future in as_completed(futures):
            row, rp = futures[future]
            done += 1
            if done % 1000 == 0:
                print(f"  Проверено: {done:,}/{len(pairs):,}", end="\r", flush=True)
            if future.result():
                found_rows.append((row, rp))
            else:
                missing_count += 1

    print()

    output_path.parent.mkdir(parents=True, exist_ok=True)
    with open(output_path, "w", encoding="utf-8", newline="") as f:
        writer = csv.writer(f, delimiter=";", quotechar='"', quoting=csv.QUOTE_ALL)
        for (orig_path, ftype, mtime, atime, size), rp in found_rows:
            writer.writerow([rp, "regular file", _norm_time(mtime), _norm_time(atime), size])

    found = len(found_rows)
    pct_found = (found / total * 100) if total else 0
    pct_miss = (missing_count / total * 100) if total else 0

    print(f"\nДиск {disk}: {total:,} файлов")
    print(f"  ✓ Найдено на RAIDIX: {found:>8,} ({pct_found:.1f}%) → записано в {output_path}")
    print(f"  ✗ Не найдено:        {missing_count:>8,} ({pct_miss:.1f}%) → пропущено")
    if skipped_parse:
        print(f"  ? Пропущено (путь):  {skipped_parse:>8,}")

    print(f"""
Запусти копирование:
  python /Users/dvorobiev/copeer/copeer.py -i {output_path}
  (в config.yaml указать нужный целевой mount point)
""")


if __name__ == "__main__":
    main()
