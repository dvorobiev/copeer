# copeer_rich_TUI.py
"""
Утилита для архивации и параллельного копирования больших объемов данных
с TUI-дашбордом на базе Rich и возможностью возобновления работы.
"""

# Стандартная библиотека
import argparse, csv, logging, os, re, subprocess, sys, tarfile, time, yaml, math
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor
from queue import Queue
from pathlib import Path
from threading import Lock

# Сторонние библиотеки
from rich.console import Console
from rich.filesize import decimal
from rich.layout import Layout
from rich.live import Live
from rich.logging import RichHandler
from rich.panel import Panel
from rich.progress import (BarColumn, Progress, TaskProgressColumn,
                           TextColumn, TimeRemainingColumn, TransferSpeedColumn)
from rich.prompt import Prompt
from rich.table import Table

# --- Глобальные переменные и константы ---
console = Console()
__version__ = "6.0.0" # НОВАЯ ВЕРСИЯ
CONFIG_FILE = "config.yaml"
status_queue = Queue()
DEFAULT_CONFIG = {
    'mount_points': ["/mnt/disk1", "/mnt/disk2"],
    'source_root': '/path/to/source/data',
    'destination_root': '/',
    'threshold': 98.0,
    'state_file': "copier_state.csv",
    'mapping_file': "mapping.csv",
    'error_log_file': "errors.log",
    'dry_run_mapping_file': "dry_run_mapping.csv",
    'threads': 8,
    'disk_strategy': "round_robin", #or fill
    'max_concurrent_disks': "2",
    'min_files_for_sequence': 50,
    'image_extensions': ['dpx', 'cri', 'tiff', 'tif', 'exr', 'png', 'jpg', 'jpeg', 'tga', 'j2c'],
}
SEQUENCE_RE = re.compile(r'^(.*?)[\._]*(\d+)\.([a-zA-Z0-9]+)$', re.IGNORECASE)

logging.basicConfig(level="INFO", format="%(message)s", datefmt="[%X]", handlers=[RichHandler(rich_tracebacks=True, show_path=False, console=console)])
log = logging.getLogger("rich")

file_lock = Lock()
worker_stats = {}

# --- Основные классы ---

def show_sequences_paginated(archive_jobs, page_size=10):
    """
    Показывает секвенции с постраничным выводом.
    Для каждой секвенции отображает: имя с полным путем, количество файлов, размер.
    """
    if not archive_jobs:
        console.print("[yellow]Секвенции для архивации не найдены.[/yellow]")
        return
    
    total_sequences = len(archive_jobs)
    total_pages = math.ceil(total_sequences / page_size)
    current_page = 1
    
    while True:
        console.clear()
        console.rule(f"[cyan]Список секвенций ({total_sequences} всего)[/cyan]")
        
        # Определяем диапазон для текущей страницы
        start_idx = (current_page - 1) * page_size
        end_idx = min(start_idx + page_size, total_sequences)
        
        # Создаем таблицу для секвенций
        table = Table(title=f"Страница {current_page} из {total_pages}", show_header=True, box=None, padding=(0, 1))
        table.add_column("№", style="dim", width=4, justify="right")
        table.add_column("Имя секвенции", style="cyan", no_wrap=False, ratio=3)
        table.add_column("Файлов", style="yellow", justify="right", width=8)
        table.add_column("Размер", style="green", justify="right", width=12)
        
        # Заполняем таблицу данными текущей страницы
        for i in range(start_idx, end_idx):
            job = archive_jobs[i]
            sequence_name = job.get('tar_filename', os.path.basename(job['key']))
            full_path = job['key']
            file_count = len(job.get('source_files', []))
            size = job.get('size', 0)
            
            # Отображаем полный путь, но красиво обрезаем если слишком длинный
            display_path = full_path
            if len(display_path) > 80:
                display_path = "..." + display_path[-77:]
            
            table.add_row(
                str(i + 1),
                f"[bold]{sequence_name}[/bold]\n[dim]{display_path}[/dim]",
                f"{file_count:,}",
                decimal(size)
            )
        
        console.print(table)
        
        # Показываем навигационное меню
        choices = []
        prompt_parts = []
        
        if current_page > 1:
            choices.append("p")
            prompt_parts.append("([cyan]P[/cyan])revious")
        
        if current_page < total_pages:
            choices.append("n")
            prompt_parts.append("([cyan]N[/cyan])ext")
        
        choices.append("q")
        prompt_parts.append("([red]Q[/red])uit")
        
        prompt_text = "\n[bold]" + " / ".join(prompt_parts) + "[/bold]"
        
        choice = Prompt.ask(prompt_text, choices=choices, default="q").lower()
        
        if choice == 'p' and current_page > 1:
            current_page -= 1
        elif choice == 'n' and current_page < total_pages:
            current_page += 1
        elif choice == 'q':
            console.clear()
            break

# Замените этот класс целиком
class DiskManager:
    """
    Управляет выбором диска для записи. Приоритетно использует пул дисков,
    но при необходимости ищет место в остальных, чтобы избежать сбоев.
    """
    def __init__(self, mount_points, threshold, strategy='fill', is_dry_run=False, max_concurrent_disks=999):
        self.mount_points = mount_points
        self.threshold = threshold
        self.strategy = strategy
        self.is_dry_run = is_dry_run
        self.max_concurrent_disks = int(max_concurrent_disks)
        self.active_disk = None
        self.next_disk_index = 0
        self.lock = Lock()

        log.info(f"Стратегия распределения по дискам: {self.strategy}")
        if self.strategy == 'round_robin':
            log.info(f"Максимальное кол-во одновременно используемых дисков (в приоритете): {self.max_concurrent_disks}")
        self._select_initial_disk()

    def _get_disk_usage(self, path):
        """Возвращает использование диска в процентах."""
        if self.is_dry_run: return 0.0
        if not os.path.exists(path): return 100.0
        try:
            st = os.statvfs(path)
            used = (st.f_blocks - st.f_bfree) * st.f_frsize
            total = st.f_blocks * st.f_frsize
            return round(used / total * 100, 2) if total > 0 else 0.0
        except FileNotFoundError: return 100.0

    def _get_disk_free_space(self, path):
        """Возвращает свободное место на диске в байтах."""
        if self.is_dry_run: return sys.maxsize
        if not os.path.exists(path): return 0
        try:
            st = os.statvfs(path)
            return st.f_bavail * st.f_frsize
        except FileNotFoundError:
            return 0

    def _is_disk_suitable(self, mount_path, required_space=0):
        """Проверяет, подходит ли диск по всем критериям (существует, не переполнен, есть место)."""
        if not self.is_dry_run:
            if not os.path.exists(mount_path):
                return False
            if self._get_disk_usage(mount_path) >= self.threshold:
                return False
            if self._get_disk_free_space(mount_path) <= required_space:
                return False
        return True

    def _select_initial_disk(self):
        for mount in self.mount_points:
            if self._is_disk_suitable(mount):
                self.active_disk = mount
                log.info(f"Найден как минимум один доступный диск для начала работы: {self.active_disk}")
                return
        log.error("🛑 Не найдено подходящих дисков для начала работы.")
        raise RuntimeError("Не найдено подходящих дисков")

    # ПОЛНОСТЬЮ ПЕРЕРАБОТАННЫЙ МЕТОД С ДВУХФАЗНЫМ ПОИСКОМ
    def get_current_destination(self, job_size=0):
        with self.lock:
            if self.strategy == 'round_robin':
                # --- ФАЗА 1: ПОПЫТКА НАЙТИ МЕСТО В ПРИОРИТЕТНОМ ПУЛЕ ---
                preferred_pool = self.mount_points[:self.max_concurrent_disks]

                if preferred_pool:
                    for i in range(len(preferred_pool)):
                        check_index = (self.next_disk_index + i) % len(preferred_pool)
                        disk_to_check = preferred_pool[check_index]

                        if self._is_disk_suitable(disk_to_check, job_size):
                            # Нашли подходящий диск в пуле, используем его
                            self.next_disk_index = (check_index + 1) % len(preferred_pool)
                            return disk_to_check

                # --- ФАЗА 2: АВАРИЙНЫЙ ПОИСК В ОСТАЛЬНЫХ ДИСКАХ ---
                log.warning(f"[yellow]В приоритетном пуле ({self.max_concurrent_disks} шт.) нет места для файла {decimal(job_size)}. Ищу в остальных дисках...[/yellow]")

                fallback_disks = self.mount_points[self.max_concurrent_disks:]
                for disk in fallback_disks:
                    if self._is_disk_suitable(disk, job_size):
                        log.info(f"Найден подходящий диск вне пула: [bold green]{disk}[/bold green]")
                        # Важно: не меняем self.next_disk_index, т.к. мы работаем вне пула
                        return disk

                # Если и здесь не нашли, значит места нет нигде
                raise RuntimeError(f"🛑 Во ВСЕХ дисках не найдено места для файла размером {decimal(job_size)}")

            else:  # Стратегия 'fill'
                suitable_disks = [m for m in self.mount_points if self._is_disk_suitable(m, job_size)]
                if not suitable_disks:
                    raise RuntimeError(f"🛑 Нет доступных дисков для файла размером {decimal(job_size)}.")

                if self.active_disk not in suitable_disks:
                    log.warning(f"Диск [bold]{self.active_disk}[/bold] не подходит для задания размером {decimal(job_size)}. Ищу следующий...")
                    self.active_disk = suitable_disks[0]
                    log.info(f"Переключился на диск: [bold green]{self.active_disk}[/bold green]")
                return self.active_disk

    def get_all_disks_status(self):
        return [(m, self._get_disk_usage(m)) for m in self.mount_points]
# --- Вспомогательные функции ---

def load_config():
    config = DEFAULT_CONFIG.copy()
    if os.path.exists(CONFIG_FILE):
        try:
            with open(CONFIG_FILE, 'r', encoding='utf-8') as f: config.update(yaml.safe_load(f) or {})
        except Exception as e: console.print(f"[bold red]Ошибка чтения {CONFIG_FILE}: {e}.[/bold red]")
    else:
        with open(CONFIG_FILE, 'w', encoding='utf-8') as f: yaml.dump(DEFAULT_CONFIG, f, sort_keys=False, allow_unicode=True)
        log.info(f"Создан файл конфигурации по умолчанию: [cyan]{CONFIG_FILE}[/cyan]")
    config['image_extensions'] = set(e.lower() for e in config.get('image_extensions', []))
    return config

def load_previous_state(state_file, processed_items_keys):
    if os.path.exists(state_file):
        try:
            with open(state_file, 'r', encoding='utf-8') as f:
                for row in csv.reader(f):
                    if row: processed_items_keys.add(row[0])
            log.info(f"Загружено {len(processed_items_keys)} записей из файла состояния.")
        except Exception as e: log.error(f"Не удалось прочитать файл состояния {state_file}: {e}")

def write_log(state_log_file, mapping_log_file, key, dest_path=None, is_dry_run=False):
    with file_lock:
        if not is_dry_run:
            with open(state_log_file, "a", newline='', encoding='utf-8') as f: csv.writer(f).writerow([key])
        if dest_path:
            # В dry-run режиме пишем в dry_run_mapping_file, иначе в обычный mapping_file
            target_mapping_file = mapping_log_file.replace('mapping.csv', 'dry_run_mapping.csv') if is_dry_run else mapping_log_file
            with open(target_mapping_file, "a", newline='', encoding='utf-8') as f: csv.writer(f).writerow([key, dest_path])

def find_sequences(dirs, config):
    all_sequences, sequence_files = [], set()
    for dir_path, files_with_sizes in dirs.items():
        sequences_in_dir = defaultdict(list)
        for filename, file_size in files_with_sizes:
            match = SEQUENCE_RE.match(filename)
            if match and match.group(3).lower() in config.get('image_extensions', set()):
                prefix, frame, ext = match.groups()
                sequences_in_dir[(prefix, ext.lower())].append((int(frame), os.path.join(dir_path, filename), file_size))
        for (prefix, ext), file_tuples in sequences_in_dir.items():
            if len(file_tuples) >= config.get('min_files_for_sequence', 50):
                file_tuples.sort()
                frames, full_paths, sizes = zip(*file_tuples)
                
                # Проверяем непрерывность последовательности
                min_frame, max_frame = min(frames), max(frames)
                expected_frames = max_frame - min_frame + 1
                actual_frames = len(frames)
                
                # Разрешаем небольшие пропуски (например, до 5% от общего количества кадров)
                max_allowed_gaps = max(1, int(expected_frames * 0.05))  # Максимум 5% пропусков
                missing_frames = expected_frames - actual_frames
                
                if missing_frames <= max_allowed_gaps:
                    # Это действительно секвенция с минимальными пропусками
                    safe_prefix = re.sub(r'[^\w\.\-]', '_', prefix.strip())
                    tar_filename = f"{safe_prefix}.{min_frame:04d}-{max_frame:04d}.{ext}.tar"
                    virtual_tar_path = os.path.join(dir_path, tar_filename)
                    all_sequences.append({
                        'type': 'sequence', 
                        'key': virtual_tar_path, 
                        'dir_path': dir_path, 
                        'tar_filename': tar_filename, 
                        'source_files': list(full_paths), 
                        'size': sum(sizes),
                        'frame_info': {
                            'min_frame': min_frame,
                            'max_frame': max_frame,
                            'expected_frames': expected_frames,
                            'actual_frames': actual_frames,
                            'missing_frames': missing_frames
                        }
                    })
                    sequence_files.update(full_paths)
                else:
                    # Слишком много пропусков - не считаем секвенцией
                    log.warning(f"Пропущена потенциальная секвенция {prefix} в {dir_path}: "
                              f"слишком много пропусков ({missing_frames} из {expected_frames} кадров, "
                              f"допустимо максимум {max_allowed_gaps})")
    return all_sequences, sequence_files

def archive_sequence_to_destination(job, dest_tar_path, progress_callback=None):
    try:
        os.makedirs(os.path.dirname(dest_tar_path), exist_ok=True)
        source_files = job.get('source_files', [])
        total_files = len(source_files)
        with tarfile.open(dest_tar_path, "w") as tar:
            for i, file_path in enumerate(source_files):
                # Нормализуем путь для проверки существования файла
                normalized_file_path = normalize_unicode_quotes(file_path)
                
                # Детальное логирование для диагностики
                log.debug(f"Проверяем файл в секвенции:")
                log.debug(f"  Оригинальный путь: {repr(file_path)}")
                log.debug(f"  Нормализованный путь: {repr(normalized_file_path)}")
                log.debug(f"  Существует (оригинальный): {os.path.exists(file_path)}")
                log.debug(f"  Существует (нормализованный): {os.path.exists(normalized_file_path)}")
                
                if os.path.exists(normalized_file_path):
                    tar.add(normalized_file_path, arcname=os.path.basename(normalized_file_path))
                elif os.path.exists(file_path):
                    # Попробуем оригинальный путь если нормализованный не работает
                    tar.add(file_path, arcname=os.path.basename(file_path))
                    log.debug(f"Использован оригинальный путь вместо нормализованного")
                else:
                    log.warning(f"В секвенции не найден файл: {file_path}")
                    log.debug(f"Также проверен нормализованный путь: {normalized_file_path}")
                if progress_callback:
                    progress_callback(i + 1, total_files)
        return True
    except (IOError, OSError, tarfile.TarError) as e:
        log.error(f"Не удалось создать или записать архив {dest_tar_path}: {e}")
        return False

def parse_scientific_notation(size_str: str) -> int:
    try:
        cleaned_str = size_str.replace(',', '.').strip()
        if 'E' in cleaned_str.upper(): return int(float(cleaned_str))
        return int(cleaned_str)
    except (ValueError, TypeError): return 0

def normalize_unicode_quotes(path: str) -> str:
    """
    Нормализует кавычки в путях файлов.
    Заменяет фигурные Unicode кавычки на обычные ASCII кавычки.
    """
    # Заменяем фигурные Unicode кавычки на обычные ASCII кавычки
    normalized = path.replace('"', '"').replace('"', '"')
    return normalized

# --- Логика анализа и выполнения ---

def scan_directory_and_plan_jobs(source_dir_path, config, processed_items_keys):
    console.rule("[yellow]Шаг 1: Сканирование каталога и планирование[/]")
    console.print(f"Сканирование каталога: [bold cyan]{source_dir_path}[/bold cyan]")
    all_file_paths = []
    with console.status("[bold green]Поиск файлов...[/bold green]"):
        for root, _, files in os.walk(source_dir_path):
            for name in files:
                all_file_paths.append(os.path.join(root, name))

    dirs, all_files_map = defaultdict(list), {}
    with Progress(console=console, transient=True) as progress:
        task = progress.add_task("[green]Анализ файлов...", total=len(all_file_paths))
        for path in all_file_paths:
            try:
                size = os.path.getsize(path)
                path_obj = Path(path)
                dirs[str(path_obj.parent)].append((path_obj.name, size))
                all_files_map[str(path_obj)] = size
            except FileNotFoundError:
                log.warning(f"Файл не найден во время анализа: {path}")
            progress.update(task, advance=1)

    sequences, sequence_files = find_sequences(dirs, config)
    standalone_files = set(all_files_map.keys()) - sequence_files
    archive_jobs = [job for job in sequences if job['key'] not in processed_items_keys]
    copy_jobs = [{'type': 'file', 'key': f, 'size': all_files_map[f]} for f in standalone_files if f not in processed_items_keys]
    archive_jobs.sort(key=lambda j: j.get('size', 0), reverse=True)
    copy_jobs.sort(key=lambda j: j.get('size', 0), reverse=True)
    stats = {"total_found": len(all_files_map), "mode": "dir"}
    return copy_jobs, archive_jobs, stats

# Замените эту функцию целиком в copeer.py
def analyze_and_plan_jobs(input_csv_path, config, processed_items_keys):
    console.rule("[yellow]Шаг 1: Анализ и планирование[/]")
    console.print(f"Анализ файла: [bold cyan]{input_csv_path}[/bold cyan]")
    dirs, all_files_from_csv = defaultdict(list), {}
    source_root = config.get('source_root')
    if source_root: console.print(f"Используется корень источника: [cyan]{source_root}[/cyan]")
    lines_total, lines_ignored_dirs, malformed_lines = 0, 0, []

    try:
        with open(input_csv_path, 'r', encoding='utf-8', errors='ignore') as f: total_lines_for_progress = sum(1 for _ in f)
        with Progress(console=console, transient=True) as progress:
            task = progress.add_task("[green]Анализ CSV...", total=total_lines_for_progress)
            with open(input_csv_path, 'r', encoding='utf-8', errors='ignore') as f:
                reader = csv.reader(f, delimiter=';')
                for i, row in enumerate(reader):
                    progress.update(task, advance=1); lines_total += 1
                    if not row or len(row) < 5: malformed_lines.append((i + 1, str(row), "Недостаточно колонок")); continue
                    rel_path, file_type, size_str = row[0], row[1], row[4]
                    if 'directory' in file_type: lines_ignored_dirs += 1; continue
                    if 'file' not in file_type: malformed_lines.append((i + 1, str(row), f"Неизвестный тип: {file_type}")); continue
                    size = parse_scientific_notation(size_str)
                    absolute_source_path = os.path.normpath(os.path.join(source_root, rel_path) if source_root else rel_path)
                    path_obj = Path(absolute_source_path)
                    dirs[str(path_obj.parent)].append((path_obj.name, size))
                    all_files_from_csv[absolute_source_path] = size
    except Exception as e: console.print(f"[bold red]Критическая ошибка при чтении CSV: {e}[/bold red]"); sys.exit(1)

    sequences, sequence_files = find_sequences(dirs, config)
    standalone_files = set(all_files_from_csv.keys()) - sequence_files

    archive_jobs_all = sequences
    copy_jobs_all = [{'type': 'file', 'key': f, 'size': all_files_from_csv[f]} for f in standalone_files]

    # --- ИСПРАВЛЕННАЯ ЛОГИКА ---
    # Проверяем не виртуальное имя архива, а наличие первого исходного файла секвенции в state-файле.
    archive_jobs_to_process = [job for job in archive_jobs_all if job.get('source_files') and job['source_files'][0] not in processed_items_keys]
    # ---------------------------

    copy_jobs_to_process = [job for job in copy_jobs_all if job['key'] not in processed_items_keys]

    archive_jobs_to_process.sort(key=lambda j: j.get('size', 0), reverse=True)
    copy_jobs_to_process.sort(key=lambda j: j.get('size', 0), reverse=True)

    stats = {
        "mode": "csv", "lines_total": lines_total, "lines_ignored_dirs": lines_ignored_dirs,
        "malformed_lines": malformed_lines, "total_found": len(all_files_from_csv)
    }
    return copy_jobs_to_process, archive_jobs_to_process, stats
def compute_dest_path(job, dest_mount_point, config):
    """Вычисляет путь назначения для задания."""
    source_root = config.get('source_root')
    destination_root = config.get('destination_root', '/')
    absolute_source_key = job['key']
    rel_path = (os.path.relpath(absolute_source_key, source_root)
                if source_root and absolute_source_key.startswith(source_root)
                else absolute_source_key.lstrip(os.path.sep))
    return Path(os.path.normpath(os.path.join(dest_mount_point, destination_root.lstrip(os.path.sep), rel_path)))


def preflight_skip_existing(copy_jobs, archive_jobs, single_dest, config):
    """
    Проверяет какие файлы/архивы уже есть на dest-диске.
    Показывает отчёт и возвращает только те задания, которые нужно выполнить.
    """
    console.rule("[cyan]Pre-flight: проверка существующих файлов[/cyan]")
    console.print(f"Режим: [bold]докопирование на[/bold] [green]{single_dest}[/green]")

    already_done_copy, to_copy = [], []
    already_done_archive, to_archive = [], []

    with Progress(console=console, transient=True) as progress:
        task = progress.add_task("[green]Проверка файлов...", total=len(copy_jobs) + len(archive_jobs))

        for job in copy_jobs:
            dest_path = compute_dest_path(job, single_dest, config)
            try:
                if dest_path.exists() and dest_path.stat().st_size == job['size']:
                    already_done_copy.append(job)
                else:
                    to_copy.append(job)
            except OSError:
                to_copy.append(job)
            progress.update(task, advance=1)

        for job in archive_jobs:
            dest_path = compute_dest_path(job, single_dest, config)
            try:
                if dest_path.exists():
                    already_done_archive.append(job)
                else:
                    to_archive.append(job)
            except OSError:
                to_archive.append(job)
            progress.update(task, advance=1)

    already_size = sum(j['size'] for j in already_done_copy) + sum(j['size'] for j in already_done_archive)
    to_do_size = sum(j['size'] for j in to_copy) + sum(j['size'] for j in to_archive)

    table = Table(show_header=False, box=None, padding=(0, 2))
    table.add_column("Параметр", style="cyan", no_wrap=True)
    table.add_column("Значение", style="white", justify="right")
    table.add_row("Всего заданий:", f"{len(copy_jobs) + len(archive_jobs):,}")
    table.add_row("Уже есть на диске (пропустить):", f"[dim]{len(already_done_copy) + len(already_done_archive):,}[/dim] ({decimal(already_size)})")
    table.add_row("Нужно скопировать:", f"[yellow]{len(to_copy) + len(to_archive):,}[/yellow] ({decimal(to_do_size)})")
    console.print(table)
    console.print()

    return to_copy, to_archive


# Замените эту функцию целиком
def show_summary_and_confirm(copy_jobs, archive_jobs, stats):
    while True:
        console.rule("[yellow]Отчет по анализу[/]")
        report_table = Table(title=None, show_header=False, box=None, padding=(0, 2))
        report_table.add_column("Параметр", style="cyan", no_wrap=True)
        report_table.add_column("Значение", style="white", justify="right")

        # --- Блок 1: Анализ исходного источника (CSV или директории) ---
        if stats.get("mode") == "csv":
            report_table.add_row("Всего строк в CSV:", f"{stats.get('lines_total', 0):,}")
            malformed_count = len(stats.get('malformed_lines', []))
            # Новая, более понятная формулировка
            report_table.add_row("  Пропущено (некорректный формат):", f"[{'red' if malformed_count > 0 else 'dim'}]{malformed_count:,}[/]")
            report_table.add_row("  Пропущено (записи с типом 'directory'):", f"[dim]{stats.get('lines_ignored_dirs', 0):,}[/dim]")
            report_table.add_section()

        # --- Блок 2: Сводка по найденным файлам и их разбивка по статусу ---
        total_valid_files = stats.get('total_found', 0)
        report_table.add_row("[bold]Всего найдено валидных файлов:", f"[bold green]{total_valid_files:,}[/bold green]")
        report_table.add_section()

        # Теперь расчеты и вывод стали логичными
        archive_size = sum(j.get('size', 0) for j in archive_jobs)
        copy_size = sum(j.get('size', 0) for j in copy_jobs)

        # Рассчитываем количество уже обработанных файлов
        processed_count = total_valid_files - (len(copy_jobs) + len(archive_jobs))

        report_table.add_row("Уже обработано (пропущено):", f"[dim]{processed_count:,}[/dim]")
        report_table.add_row("Новых заданий на архивацию:", f"[yellow]{len(archive_jobs):,}[/yellow] ({decimal(archive_size)})")
        report_table.add_row("Новых заданий на копирование:", f"[yellow]{len(copy_jobs):,}[/yellow] ({decimal(copy_size)})")

        console.print(report_table)

        choices = ["s", "q"]
        prompt_text = "\n[bold]Выберите действие: ([green]S[/green])тарт / ([red]Q[/red])uit"
        
        # Добавляем опцию просмотра секвенций, если они есть
        if archive_jobs:
            choices.append("v")
            prompt_text += " / ([cyan]V[/cyan])iew sequences"
        
        malformed_lines = stats.get('malformed_lines', [])
        if malformed_lines:
            choices.append("e")
            prompt_text += " / ([yellow]E[/yellow])rrors"
        choice = Prompt.ask(prompt_text, choices=choices, default="s").lower()
        if choice == 's': return True
        if choice == 'q': return False
        if choice == 'v' and archive_jobs:
            show_sequences_paginated(archive_jobs)
            console.clear()
        if choice == 'e' and malformed_lines:
            console.print("\n[bold yellow]----- Список некорректных строк (первые 50) -----[/bold yellow]")
            for num, line, reason in malformed_lines[:50]: console.print(f"[dim]Строка #{num} ({reason}):[/dim] {line}")
            console.input("\n[bold]Нажмите [green]Enter[/green] для возврата в меню...[/bold]")
            console.clear()
# ИСПРАВЛЕНО: Явно принимает is_dry_run
# Замените эту функцию целиком
# Замените эту функцию целиком
def process_job_worker(worker_id, job, config, disk_manager, is_dry_run, is_debug_mode, progress_callback=None, single_dest=None, skip_existing=False):
    """
    Обрабатывает задание, отправляет обновления в очередь и корректно логирует ошибки
    как для отдельных файлов, так и для секвенций.
    """
    short_name = job.get('tar_filename') or os.path.basename(job['key'])
    op_type_text = "[yellow]Архивация[/yellow]" if job['type'] == 'sequence' else "[cyan]Копирование[/cyan]"
    status_queue.put((worker_id, {"status": op_type_text, "job": job, "progress": 0, "disk_idx": None}))

    try:
        dest_mount_point = single_dest if single_dest else disk_manager.get_current_destination(job['size'])

        disk_idx = '?'
        try:
            disk_idx = config['mount_points'].index(dest_mount_point) + 1
        except (ValueError, KeyError):
            pass

        status_queue.put((worker_id, {"disk_idx": disk_idx}))

        source_root = config.get('source_root')
        destination_root = config.get('destination_root', '/')
        absolute_source_key = job['key']

        rel_path = os.path.relpath(absolute_source_key, source_root) if source_root and absolute_source_key.startswith(source_root) else absolute_source_key.lstrip(os.path.sep)
        dest_path = os.path.normpath(os.path.join(dest_mount_point, destination_root.lstrip(os.path.sep), rel_path))

        source_keys_to_log = []

        if job['type'] == 'sequence':
            # Для секвенций в лог состояния пойдут все исходные файлы
            source_keys_to_log = job.get('source_files', [])
            if not is_dry_run:
                if skip_existing and Path(dest_path).exists():
                    status_queue.put((worker_id, {"status": "[dim]Пропущен[/dim]", "progress": 100}))
                    return (job['type'], job['size'], source_keys_to_log, dest_path)
                if not archive_sequence_to_destination(job, dest_path, progress_callback):
                    raise RuntimeError(f"Не удалось создать архив {short_name}")
            else: # Dry-run симуляция
                total_files = len(job.get('source_files', []))
                for i in range(total_files):
                    time.sleep(0.005)
                    if progress_callback: progress_callback(i + 1, total_files)
        else:  # 'file'
            # Для обычного файла - только его ключ
            source_keys_to_log = [absolute_source_key]
            if not is_dry_run:
                # Нормализуем путь для проверки существования файла
                normalized_source_key = normalize_unicode_quotes(absolute_source_key)
                
                # Детальное логирование для диагностики
                log.debug(f"Проверяем отдельный файл:")
                log.debug(f"  Оригинальный путь: {repr(absolute_source_key)}")
                log.debug(f"  Нормализованный путь: {repr(normalized_source_key)}")
                log.debug(f"  Существует (оригинальный): {os.path.exists(absolute_source_key)}")
                log.debug(f"  Существует (нормализованный): {os.path.exists(normalized_source_key)}")
                
                # Проверяем существование файла (сначала нормализованный, потом оригинальный)
                if os.path.exists(normalized_source_key):
                    actual_source_path = normalized_source_key
                elif os.path.exists(absolute_source_key):
                    actual_source_path = absolute_source_key
                    log.debug(f"Использован оригинальный путь вместо нормализованного")
                else:
                    raise FileNotFoundError(f"Исходный файл не найден: {absolute_source_key}")
                
                # Используем найденный путь для копирования
                if skip_existing:
                    dest_p = Path(dest_path)
                    try:
                        if dest_p.exists() and dest_p.stat().st_size == job['size']:
                            status_queue.put((worker_id, {"status": "[dim]Пропущен[/dim]", "progress": 100}))
                            return (job['type'], job['size'], source_keys_to_log, dest_path)
                    except OSError:
                        pass

                os.makedirs(os.path.dirname(dest_path), exist_ok=True)

                # ... (блок rsync остается без изменений)
                rsync_cmd = ["rsync", "-a", "--no-i-r", "--progress", actual_source_path, dest_path]
                status_queue.put((worker_id, {"status": "[blue]rsync...[/blue]"}))
                process = subprocess.Popen(rsync_cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, encoding='utf-8', errors='ignore')
                progress_re = re.compile(r'\s+(\d+)%')
                line_buffer = ""
                for char in iter(lambda: process.stdout.read(1), ''):
                    if char in ['\r', '\n']:
                        if match := progress_re.search(line_buffer):
                            status_queue.put((worker_id, {"progress": int(match.group(1))}))
                        line_buffer = ""
                    else:
                        line_buffer += char
                process.stdout.close()
                return_code = process.wait()
                if return_code != 0 and "died with <Signals.SIGINT: 2>" not in (error_output := process.stderr.read()) and return_code != -2:
                    raise subprocess.CalledProcessError(return_code, rsync_cmd, stderr=error_output)

            else: # Dry-run симуляция
                steps = 5 if is_debug_mode else 3
                delay = 0.7 if is_debug_mode else 0.2
                for i in range(steps):
                    status_queue.put((worker_id, {"status": "[cyan]Симуляция[/cyan]", "progress": f"{i+1}/{steps}"}))
                    time.sleep(delay)

        final_status = "[bold green]Завершено[/bold green]"
        if is_dry_run: final_status = "[green]Готово (dr)[/green]"
        status_queue.put((worker_id, {"status": final_status, "progress": 100}))

        # Возвращаем тип, размер и КЛЮЧИ ИСХОДНЫХ ФАЙЛОВ для записи в лог состояния
        return (job['type'], job['size'], source_keys_to_log, dest_path)

    except Exception as e:
        if not isinstance(e, KeyboardInterrupt):
            status_queue.put((worker_id, {"status": "[bold red]Ошибка[/bold red]", "progress": 0}))

            # --- ТИХОЕ ЛОГИРОВАНИЕ ОШИБОК БЕЗ ВЫВОДА В КОНСОЛЬ ---
            if job['type'] == 'sequence':
                # Если упала секвенция, логируем все её ИСХОДНЫЕ файлы
                error_message = f"Sequence processing failed for '{job['key']}': {e}"
                with file_lock:
                    with open(config['error_log_file'], "a", encoding='utf-8') as f:
                        for source_file in job.get('source_files', []):
                             f.write(f"{time.asctime()};{source_file};{error_message}\n")
            else:
                # Если упал обычный файл, логируем его ключ, как и раньше
                with file_lock:
                    with open(config['error_log_file'], "a", encoding='utf-8') as f:
                        f.write(f"{time.asctime()};{job['key']};{e}\n")

        return (None, 0, None, None)

def make_layout() -> Layout:
    layout = Layout(name="root")
    layout.split_column(Layout(name="top", size=19), Layout(name="middle"), Layout(name="bottom", size=3))
    layout["top"].split_row(Layout(name="summary"), Layout(name="disks"))
    return layout

def generate_summary_panel(plan, completed) -> Panel:
    table = Table(box=None, expand=True)
    table.add_column("Тип задания", style="cyan", no_wrap=True)
    table.add_column("Выполнено", style="green", justify="right")
    table.add_column("Ошибки", style="red", justify="right")
    table.add_column("Размер", style="green", justify="right")
    s_plan, f_plan = plan.get('sequences', {}), plan.get('files', {})
    s_done, s_total = completed['sequence']['count'], s_plan.get('count', 0)
    s_errors = completed['sequence']['errors']
    s_size_done, s_size_total = completed['sequence']['size'], s_plan.get('size', 0)
    s_errors_text = f"[red]{s_errors}[/red]" if s_errors > 0 else "0"
    table.add_row("Архивация", f"{s_done} / {s_total}", s_errors_text, f"{decimal(s_size_done)} / {decimal(s_size_total)}")
    f_done, f_total = completed['files']['count'], f_plan.get('count', 0)
    f_errors = completed['files']['errors']
    f_size_done, f_size_total = completed['files']['size'], f_plan.get('size', 0)
    f_errors_text = f"[red]{f_errors}[/red]" if f_errors > 0 else "0"
    table.add_row("Копирование", f"{f_done} / {f_total}", f_errors_text, f"{decimal(f_size_done)} / {decimal(f_size_total)}")
    total_count_done, total_count_plan = s_done + f_done, s_total + f_total
    total_errors = s_errors + f_errors
    total_size_done, total_size_plan = s_size_done + f_size_done, s_size_total + f_size_total
    total_errors_text = f"[bold red]{total_errors}[/bold red]" if total_errors > 0 else "[bold]0[/bold]"
    table.add_row("[bold]Всего[/bold]", f"[bold]{total_count_done} / {total_count_plan}[/bold]", total_errors_text, f"[bold]{decimal(total_size_done)} / {decimal(total_size_plan)}[/bold]")
    return Panel(table, title="📊 План выполнения", border_style="yellow")

def generate_disks_panel(disk_manager: DiskManager, config) -> Panel:
    table = Table(box=None, expand=True)
    table.add_column("Диск", style="white", no_wrap=True)
    table.add_column("Размер", style="dim", justify="right")
    table.add_column("Заполнено", style="green", ratio=1)
    table.add_column("%", style="bold", justify="right")
    for mount, percent in disk_manager.get_all_disks_status():
        color = "green" if percent < config['threshold'] else "red"
        try:
            st = os.statvfs(mount); total = st.f_blocks * st.f_frsize; used = total - (st.f_bfree * st.f_frsize)
            size_str = f"{decimal(used)} / {decimal(total)}"
        except FileNotFoundError: size_str = "[red]Н/Д[/red]"
        bar = Progress(BarColumn(bar_width=None, style=color, complete_style=color))
        bar.add_task("d", total=100, completed=percent)
        is_active = " (*)" if mount == disk_manager.active_disk else ""
        table.add_row(f"[bold]{mount}{is_active}[/bold]", size_str, bar, f"{percent:.1f}%")
    return Panel(table, title="📦 Диски", border_style="blue")

# Замените эту функцию целиком
def generate_workers_panel(threads) -> Panel:
    table = Table(box=None, expand=True, show_header=True)
    table.add_column("Размер", justify="right", style="cyan", width=12)
    table.add_column("Имя файла", style="white", no_wrap=True, ratio=2)
    table.add_column("Статус", justify="left", style="white", width=20) # Немного увеличим ширину
    table.add_column("Прогресс", justify="left", ratio=2)

    for worker_id in range(1, threads + 1):
        stats = worker_stats.get(worker_id)
        if stats and (job := stats.get("job")):
            size_str = decimal(job['size'])
            short_name = job.get('tar_filename') or os.path.basename(job['key'])
            status_text = stats.get("status", "")
            progress_val = stats.get("progress", 0)

            # --- НОВЫЙ БЛОК: ФОРМИРУЕМ СТРОКУ СТАТУСА С ДИСКОМ ---
            disk_idx = stats.get("disk_idx")
            if disk_idx:
                status_with_disk = f"{status_text} [dim]➜ [[/dim][bold cyan]{disk_idx}[/bold cyan][dim]][/dim]"
            else:
                status_with_disk = status_text
            # -----------------------------------------------------

            progress_widget = ""
            if isinstance(progress_val, (int, float)) and progress_val > 0:
                progress_widget = Progress(BarColumn(bar_width=None), TaskProgressColumn(), expand=True)
                progress_widget.add_task("p", total=100, completed=progress_val)
            elif progress_val:
                progress_widget = str(progress_val)

            table.add_row(size_str, short_name, status_with_disk, progress_widget)
        elif stats:
            table.add_row("[dim]---[/dim]", f"[grey50]{stats.get('status', 'Ожидание...')}[/grey50]", "", "")

    return Panel(table, title=f"👷 Потоки ({threads})", border_style="green")
# --- Точка входа ---

def main(args):
    """Главная функция с унифицированным процессом анализа и выполнения."""
    config = load_config()
    if args.dry_run: config['dry_run'] = True

    # КЛЮЧЕВОЕ ИСПРАВЛЕНИЕ: Устанавливаем флаги один раз в начале
    is_dry_run = config['dry_run']
    is_debug_mode = os.getenv("COPEER_DEBUG") == "1"

    if is_debug_mode and is_dry_run:
        console.rule("[bold yellow]⚠️  АКТИВЕН РЕЖИМ ОТЛАДКИ DRY-RUN ⚠️[/bold yellow]")
        console.print("Симуляция работы будет очень медленной для наглядности.")

    console.rule(f"[bold]Copeer v{__version__}[/bold] | Двухфазное выполнение")

    if is_dry_run:
        dry_run_log_path = config['dry_run_mapping_file']
        try:
            with open(dry_run_log_path, 'w', newline='', encoding='utf-8') as f:
                csv.writer(f).writerow(["source_path", "destination_path"])
        except IOError as e: console.print(f"[bold red]Не удалось создать файл отчета: {e}[/bold red]")

    processed_items_keys = set()
    load_previous_state(config['state_file'], processed_items_keys)

    if args.input_file:
        copy_jobs, archive_jobs, stats = analyze_and_plan_jobs(args.input_file, config, processed_items_keys)
    elif args.source_dir:
        config['source_root'] = os.path.abspath(args.source_dir)
        copy_jobs, archive_jobs, stats = scan_directory_and_plan_jobs(args.source_dir, config, processed_items_keys)
    else:
        console.print("[bold red]Ошибка: Не указан источник данных (--input-file или --source-dir).[/bold red]"); sys.exit(1)

    if not copy_jobs and not archive_jobs:
        console.print("[green]Все задания уже выполнены. Завершение работы.[/green]"); return
    # --- НОВЫЙ БЛОК: ФИЛЬТРАЦИЯ ЗАДАНИЙ В ЗАВИСИМОСТИ ОТ РЕЖИМА ---
    if args.mode == 'copy':
        console.print("[bold cyan]Режим 'только копирование': задания на архивацию будут пропущены.[/bold cyan]")
        archive_jobs = []
    elif args.mode == 'archive':
        console.print("[bold yellow]Режим 'только архивация': задания на копирование будут пропущены.[/bold yellow]")
        copy_jobs = []
    # --- КОНЕЦ НОВОГО БЛОКА ---
    # КЛЮЧЕВОЕ ИСПРАВЛЕНИЕ: Отчет и подтверждение всегда вызываются
    if not show_summary_and_confirm(copy_jobs, archive_jobs, stats):
        console.print("[yellow]Выполнение отменено пользователем.[/yellow]"); sys.exit(0)

    # Режим докопирования: фильтруем уже существующие файлы
    if args.skip_existing and args.single_dest:
        copy_jobs, archive_jobs = preflight_skip_existing(copy_jobs, archive_jobs, args.single_dest, config)
        if not copy_jobs and not archive_jobs:
            console.print("[green]Все файлы уже есть на диске. Нечего копировать.[/green]")
            return
    elif args.skip_existing and not args.single_dest:
        console.print("[bold yellow]Предупреждение: --skip-existing без --single-dest работает в режиме проверки внутри воркера (без pre-flight отчёта).[/bold yellow]")

    # Используем single-dest как единственную точку монтирования, если задана
    mount_points_for_manager = [args.single_dest] if args.single_dest else config['mount_points']
    if args.single_dest:
        console.print(f"Режим [bold]single-dest[/bold]: все задания направляются на [green]{args.single_dest}[/green]")

# Создаем настоящий DiskManager всегда, но передаем ему флаг is_dry_run
    disk_manager = DiskManager(
        mount_points_for_manager,
        config['threshold'],
        config.get('disk_strategy', 'fill'),
        is_dry_run=is_dry_run,
        max_concurrent_disks=config.get('max_concurrent_disks', 999) # 999 - "бесконечность" по умолчанию
    )
    if not is_dry_run and not disk_manager.active_disk: return

    for i in range(1, config['threads'] + 1):
        worker_stats[i] = {"status": "[grey50]Ожидание...[/grey50]", "job": None, "progress": 0}

    layout = make_layout()
    completed_stats = {"sequence": {"count": 0, "size": 0, "errors": 0}, "files": {"count": 0, "size": 0, "errors": 0}}
    plan_summary = {
        "sequences": {"count": len(archive_jobs), "size": sum(j.get('size', 0) for j in archive_jobs)},
        "files": {"count": len(copy_jobs), "size": sum(j.get('size', 0) for j in copy_jobs)}
    }

    layout["summary"].update(generate_summary_panel(plan_summary, completed_stats))
    layout["disks"].update(generate_disks_panel(disk_manager, config))
    layout["middle"].update(generate_workers_panel(config['threads']))

    executor = None
    try:
        with Live(layout, screen=True, redirect_stderr=False, vertical_overflow="visible", refresh_per_second=10) as live:
            with ThreadPoolExecutor(max_workers=config['threads']) as executor:
                if copy_jobs:
                    progress_bar = Progress(TextColumn("[bold blue]Копирование:[/bold blue]"), BarColumn(), TaskProgressColumn(), "•", TransferSpeedColumn())
                    main_task = progress_bar.add_task("copying", total=sum(j.get('size', 0) for j in copy_jobs))
                    layout["bottom"].update(Panel(progress_bar, title="🚀 Фаза 1: Копирование", border_style="magenta", height=3))

                    worker_id_queue, id_lock = list(range(1, config['threads'] + 1)), Lock()
                    def get_worker_id():
                        with id_lock: return worker_id_queue.pop(0) if worker_id_queue else None
                    def release_worker_id(worker_id):
                        if worker_id is not None:
                            with id_lock:
                                worker_id_queue.append(worker_id)

                    def job_wrapper(job):
                        worker_id = None
                        while worker_id is None:
                            worker_id = get_worker_id()
                            if worker_id is None: time.sleep(0.1)
                        try:
                            return process_job_worker(worker_id, job, config, disk_manager, is_dry_run, is_debug_mode,
                                                      single_dest=args.single_dest, skip_existing=args.skip_existing)
                        finally: release_worker_id(worker_id)

                    active_futures = {executor.submit(job_wrapper, job) for job in copy_jobs}

                    while active_futures:
                        while not status_queue.empty():
                            worker_id, update_data = status_queue.get(); worker_stats[worker_id].update(update_data)

                        done_futures = {f for f in active_futures if f.done()}
                        for future in done_futures:
                            job_type, size, keys, path = future.result()
                            if job_type:
                                for key in keys: write_log(config['state_file'], config['mapping_file'], key, path, is_dry_run)
                                completed_stats['files']['count'] += 1; completed_stats['files']['size'] += size
                                progress_bar.update(main_task, advance=size)
                            else:
                                # Увеличиваем счетчик ошибок
                                completed_stats['files']['errors'] += 1
                        active_futures -= done_futures

                        layout["summary"].update(generate_summary_panel(plan_summary, completed_stats))
                        layout["disks"].update(generate_disks_panel(disk_manager, config))
                        layout["middle"].update(generate_workers_panel(config['threads']))
                        time.sleep(0.1)

                if archive_jobs:
                    for i in range(1, config['threads'] + 1):
                        worker_stats[i] = {"status": "[grey50]Ожидание...[/grey50]", "job": None, "progress": 0}

                    progress_bar = Progress(TextColumn("[bold yellow]Архивация:[/bold yellow]"), BarColumn(), TaskProgressColumn(), "•", TimeRemainingColumn())
                    main_task = progress_bar.add_task("archiving", total=len(archive_jobs))
                    layout["bottom"].update(Panel(progress_bar, title="📦 Фаза 2: Архивация (1 поток)", border_style="yellow", height=3))

                    for job in archive_jobs:
                        def progress_callback(current, total):
                            status_queue.put((1, {"progress": (current / total) * 100}))

                        future = executor.submit(process_job_worker, 1, job, config, disk_manager, is_dry_run, is_debug_mode, progress_callback, args.single_dest, args.skip_existing)

                        while not future.done():
                            while not status_queue.empty():
                                worker_id, update_data = status_queue.get(); worker_stats[worker_id].update(update_data)
                            layout["summary"].update(generate_summary_panel(plan_summary, completed_stats))
                            layout["disks"].update(generate_disks_panel(disk_manager, config))
                            layout["middle"].update(generate_workers_panel(config['threads']))
                            time.sleep(0.1)

                        job_type, size, keys, path = future.result()
                        if job_type:
                            for key in keys: write_log(config['state_file'], config['mapping_file'], key, path, is_dry_run)
                            completed_stats['sequence']['count'] += 1; completed_stats['sequence']['size'] += size
                        else:
                            # Увеличиваем счетчик ошибок
                            completed_stats['sequence']['errors'] += 1
                        progress_bar.update(main_task, advance=1)

    except KeyboardInterrupt:
        console.print("\n[bold yellow]Прерывание... Ожидание завершения потоков...[/bold yellow]")
    finally:
        if 'executor' in locals() and executor:
            executor.shutdown(wait=True, cancel_futures=True)
        console.print("\n[bold green]Выход.[/bold green]")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Анализирует источник, архивирует и копирует файлы с TUI-дашбордом.", prog="copeer")
    parser.add_argument("-v", "--version", action="version", version=f"%(prog)s v{__version__}")
    source_group = parser.add_mutually_exclusive_group(required=True)
    source_group.add_argument("-i", "--input-file", help="Путь к CSV файлу со списком исходных файлов.")
    source_group.add_argument("-s", "--source-dir", help="Путь к исходной директории для сканирования.")
    parser.add_argument("--dry-run", action="store_true", help="Выполнить анализ без реального копирования.")
    parser.add_argument("--mode", choices=['all', 'copy', 'archive'], default='all', help="Режим работы: 'all' - всё (по умолчанию), 'copy' - только копирование, 'archive' - только архивация.")
    parser.add_argument("--single-dest", metavar='PATH',
                        help="Копировать всё на один диск (режим докопирования). Обходит DiskManager.")
    parser.add_argument("--skip-existing", action='store_true',
                        help="Пропускать файлы, которые уже существуют на dest-диске и совпадают по размеру.")
    args = parser.parse_args()
    main(args)
