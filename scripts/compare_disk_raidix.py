#!/usr/bin/env python3
"""
Сравнение покрытия файлов диска на RAIDIX или примонтированном диске.

Использование:
  # Проверка против RAIDIX (по умолчанию)
  python compare_disk_raidix.py --disk A1 --disks-dir /path/to/disks_files
  python compare_disk_raidix.py --all --disks-dir /path/to/disks_files
  python compare_disk_raidix.py --disk A1 --missing

  # Обратная проверка: все ли файлы из CSV есть на физическом диске
  python compare_disk_raidix.py --disk B2 --disks-dir disk_state --dest-mount /mnt/slot3
  python compare_disk_raidix.py --disk B2 --disks-dir disk_state --dest-mount /mnt/slot3 --missing
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


def disk_path_to_mount(full_path: str, dest_mount: str) -> str | None:
    """Канонизация пути с диска в путь на примонтированном диске.

    Берёт всё после /mnt/disk_XX/ и подставляет под dest_mount.
    Пример: /mnt/disk_B2/raidix/foo/bar.ext -> /mnt/slot3/raidix/foo/bar.ext
    """
    m = re.match(r'^/mnt/disk_[^/]+/(.*)', full_path)
    if m:
        return os.path.join(dest_mount, m.group(1))
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


def check_exists(raidix_path: str) -> bool:
    return os.path.exists(raidix_path)


def process_disk(disk: str, disks_dir: Path, raidix_root: str,
                 save_missing: bool, output_dir: Path,
                 dest_mount: str | None = None) -> dict:
    rows = load_disk_files(disk, disks_dir)
    total = len(rows)

    pairs = []
    skipped = 0
    for orig_path, *_ in rows:
        if dest_mount:
            rp = disk_path_to_mount(orig_path, dest_mount)
        else:
            rp = disk_path_to_raidix(orig_path, raidix_root)
        if rp:
            pairs.append((orig_path, rp))
        else:
            skipped += 1

    found_paths = []
    missing_paths = []

    with ThreadPoolExecutor(max_workers=WORKERS) as pool:
        futures = {pool.submit(check_exists, rp): (orig, rp) for orig, rp in pairs}
        for future in as_completed(futures):
            orig, rp = futures[future]
            if future.result():
                found_paths.append(rp)
            else:
                missing_paths.append(orig)

    if save_missing:
        output_dir.mkdir(parents=True, exist_ok=True)
        label = dest_mount.replace("/", "_").strip("_") if dest_mount else f"raidix_{disk}"
        out_file = output_dir / f"missing_on_{label}_{disk}.csv"
        with open(out_file, "w", encoding="utf-8", newline="") as f:
            writer = csv.writer(f, delimiter=";", quotechar='"', quoting=csv.QUOTE_ALL)
            for p in sorted(missing_paths):
                writer.writerow([p])
        print(f"  → Список отсутствующих: {out_file}")

    return {
        "disk": disk,
        "total": total,
        "skipped": skipped,
        "found": len(found_paths),
        "missing": len(missing_paths),
    }


def print_result(r: dict, dest_label: str = "RAIDIX"):
    total = r["total"]
    found = r["found"]
    missing = r["missing"]
    pct_found = (found / total * 100) if total else 0
    pct_missing = (missing / total * 100) if total else 0
    print(f"\nДиск {r['disk']}: {total:,} файлов")
    print(f"  ✓ На {dest_label}:  {found:>8,} ({pct_found:.1f}%)")
    print(f"  ✗ Нет на {dest_label}:  {missing:>8,} ({pct_missing:.1f}%)")
    if r["skipped"]:
        print(f"  ? Пропущено:      {r['skipped']:>8,} (нераспознанный путь)")


def get_all_disks(disks_dir: Path) -> list[str]:
    return sorted(p.name.replace("_output_file.csv", "")
                  for p in disks_dir.glob("*_output_file.csv"))


def main():
    parser = argparse.ArgumentParser(description="Сравнение покрытия диска на RAIDIX")
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--disk", metavar="DISK", help="Имя диска (например, A1)")
    group.add_argument("--all", action="store_true", help="Обработать все диски")
    parser.add_argument("--disks-dir", required=True, metavar="DIR",
                        help="Каталог с CSV-листингами дисков (*_output_file.csv)")
    parser.add_argument("--output-dir", default="output", metavar="DIR",
                        help="Каталог для выходных файлов (по умолчанию: output)")
    parser.add_argument("--missing", action="store_true",
                        help="Сохранить список отсутствующих файлов в --output-dir/")
    parser.add_argument("--raidix-root", default=DEFAULT_RAIDIX_ROOT,
                        help=f"Корень RAIDIX (по умолчанию: {DEFAULT_RAIDIX_ROOT})")
    parser.add_argument("--dest-mount", metavar="PATH",
                        help="Обратная проверка: путь к примонтированному диску (например, /mnt/slot3). "
                             "Проверяет что все файлы из CSV физически есть на этом диске.")
    args = parser.parse_args()

    disks_dir = Path(args.disks_dir)
    output_dir = Path(args.output_dir)
    dest_mount = args.dest_mount

    if not disks_dir.exists():
        print(f"Ошибка: каталог {disks_dir} не найден", file=sys.stderr)
        sys.exit(1)

    if dest_mount:
        print(f"Режим: обратная проверка против {dest_mount}")
        dest_label = dest_mount
    else:
        dest_label = "RAIDIX"

    disks = get_all_disks(disks_dir) if args.all else [args.disk]

    results = []
    for disk in disks:
        print(f"Обрабатываю диск {disk}...", end=" ", flush=True)
        r = process_disk(disk, disks_dir, args.raidix_root, args.missing, output_dir, dest_mount)
        results.append(r)
        print("готово")

    for r in results:
        print_result(r, dest_label)

    if args.all and len(results) > 1:
        total_all = sum(r["total"] for r in results)
        found_all = sum(r["found"] for r in results)
        missing_all = sum(r["missing"] for r in results)
        pct = (found_all / total_all * 100) if total_all else 0
        pct_miss = (missing_all / total_all * 100) if total_all else 0
        print(f"\n{'─' * 45}")
        print(f"Итого ({len(results)} дисков): {total_all:,} файлов")
        print(f"  ✓ На {dest_label}:  {found_all:>8,} ({pct:.1f}%)")
        print(f"  ✗ Нет на {dest_label}:  {missing_all:>8,} ({pct_miss:.1f}%)")


if __name__ == "__main__":
    main()
