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
__version__ = "5.0.1" # ИСПРАВЛЕНО
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
    'disk_strategy: round_robin', #or fill
    'min_files_for_sequence': 50,
    'image_extensions': ['dpx', 'cri', 'tiff', 'tif', 'exr', 'png', 'jpg', 'jpeg', 'tga', 'j2c'],
}
SEQUENCE_RE = re.compile(r'^(.*?)[\._]*(\d+)\.([a-zA-Z0-9]+)$', re.IGNORECASE)

logging.basicConfig(level="INFO", format="%(message)s", datefmt="[%X]", handlers=[RichHandler(rich_tracebacks=True, show_path=False, console=console)])
log = logging.getLogger("rich")

file_lock = Lock()
worker_stats = {}

# --- Основные классы ---

class DiskManager:
    """
    Управляет выбором диска для записи, поддерживая две стратегии:
    - 'fill': Заполнять один диск, затем переходить к следующему.
    - 'round_robin': Распределять задания по дискам по кругу.
    """
    def __init__(self, mount_points, threshold, strategy='fill'):
        self.mount_points = mount_points
        self.threshold = threshold
        self.strategy = strategy
        self.active_disk = None  # Для стратегии 'fill'
        self.next_disk_index = 0 # Для стратегии 'round_robin'
        self.lock = Lock()

        log.info(f"Стратегия распределения по дискам: [bold cyan]{self.strategy}[/bold cyan]")
        self._select_initial_disk()

    def _get_disk_usage(self, path):
        if not os.path.exists(path): return 100.0 # Считаем недоступный диск полным
        try:
            st = os.statvfs(path)
            used = (st.f_blocks - st.f_bfree) * st.f_frsize
            total = st.f_blocks * st.f_frsize
            return round(used / total * 100, 2) if total > 0 else 0.0
        except FileNotFoundError:
            return 100.0

    def _select_initial_disk(self):
        # Эта функция нужна в основном для стратегии 'fill' и для первоначальной проверки
        for mount in self.mount_points:
            if not os.path.exists(mount):
                log.warning(f"Точка монтирования [bold yellow]{mount}[/bold yellow] не существует. Пропускаю.")
                continue
            if self._get_disk_usage(mount) < self.threshold:
                self.active_disk = mount
                log.info(f"Найден как минимум один доступный диск для начала работы: [bold green]{self.active_disk}[/bold green]")
                return
        log.error("🛑 Не найдено подходящих дисков для начала работы.")
        raise RuntimeError("Не найдено подходящих дисков")

    def get_current_destination(self):
        """Возвращает путь к целевому диску в зависимости от выбранной стратегии."""
        with self.lock: # Блокировка необходима, т.к. несколько потоков будут вызывать этот метод

            # 1. Получаем список всех дисков, на которые еще можно писать
            usable_disks = [m for m in self.mount_points if os.path.exists(m) and self._get_disk_usage(m) < self.threshold]

            if not usable_disks:
                raise RuntimeError("🛑 Нет доступных дисков: все переполнены или недоступны.")

            # 2. Выбираем диск в зависимости от стратегии
            if self.strategy == 'round_robin':
                # --- ЛОГИКА 'ROUND_ROBIN' ---
                if self.next_disk_index >= len(usable_disks):
                    self.next_disk_index = 0 # Сбрасываем индекс, если список дисков сократился

                selected_disk = usable_disks[self.next_disk_index]
                # Передвигаем указатель на следующий диск для следующего вызова
                self.next_disk_index = (self.next_disk_index + 1) % len(usable_disks)
                return selected_disk

            else: # --- ЛОГИКА 'FILL' (старое поведение) ---
                if self.active_disk not in usable_disks:
                    log.warning(f"Диск [bold]{self.active_disk}[/bold] заполнен или недоступен. Ищу следующий...")
                    self.active_disk = usable_disks[0] # Просто берем первый доступный
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
            log.info(f"Загружено [bold]{len(processed_items_keys)}[/bold] записей из файла состояния.")
        except Exception as e: log.error(f"Не удалось прочитать файл состояния {state_file}: {e}")

def write_log(state_log_file, mapping_log_file, key, dest_path=None, is_dry_run=False):
    with file_lock:
        if not is_dry_run:
            with open(state_log_file, "a", newline='', encoding='utf-8') as f: csv.writer(f).writerow([key])
        if dest_path:
            with open(mapping_log_file, "a", newline='', encoding='utf-8') as f: csv.writer(f).writerow([key, dest_path])

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
                min_frame, max_frame = min(frames), max(frames)
                safe_prefix = re.sub(r'[^\w\.\-]', '_', prefix.strip())
                tar_filename = f"{safe_prefix}.{min_frame:04d}-{max_frame:04d}.{ext}.tar"
                virtual_tar_path = os.path.join(dir_path, tar_filename)
                all_sequences.append({'type': 'sequence', 'key': virtual_tar_path, 'dir_path': dir_path, 'tar_filename': tar_filename, 'source_files': list(full_paths), 'size': sum(sizes)})
                sequence_files.update(full_paths)
    return all_sequences, sequence_files

def archive_sequence_to_destination(job, dest_tar_path, progress_callback=None):
    try:
        os.makedirs(os.path.dirname(dest_tar_path), exist_ok=True)
        source_files = job.get('source_files', [])
        total_files = len(source_files)
        with tarfile.open(dest_tar_path, "w") as tar:
            for i, file_path in enumerate(source_files):
                if os.path.exists(file_path):
                    tar.add(file_path, arcname=os.path.basename(file_path))
                else:
                    log.warning(f"В секвенции не найден файл: {file_path}")
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
    archive_jobs_to_process = [job for job in archive_jobs_all if job['key'] not in processed_items_keys]
    copy_jobs_to_process = [job for job in copy_jobs_all if job['key'] not in processed_items_keys]
    archive_jobs_to_process.sort(key=lambda j: j.get('size', 0), reverse=True)
    copy_jobs_to_process.sort(key=lambda j: j.get('size', 0), reverse=True)
    stats = {
        "mode": "csv", "lines_total": lines_total, "lines_ignored_dirs": lines_ignored_dirs,
        "malformed_lines": malformed_lines, "total_found": len(all_files_from_csv)
    }
    return copy_jobs_to_process, archive_jobs_to_process, stats

def show_summary_and_confirm(copy_jobs, archive_jobs, stats):
    while True:
        console.rule("[yellow]Отчет по анализу[/]")
        report_table = Table(title=None, show_header=False, box=None, padding=(0, 2))
        report_table.add_column("Параметр", style="cyan", no_wrap=True)
        report_table.add_column("Значение", style="white", justify="right")
        if stats.get("mode") == "csv":
            report_table.add_row("Всего строк в CSV файле:", f"{stats.get('lines_total', 0):,}")
            report_table.add_row("  Пропущено (директории):", f"[dim]{stats.get('lines_ignored_dirs', 0):,}[/dim]")
            malformed_count = len(stats.get('malformed_lines', []))
            report_table.add_row(f"  Пропущено (некорректный формат):", f"[{'red' if malformed_count > 0 else 'dim'}]{malformed_count:,}[/]")
        report_table.add_row("[bold]Найдено файлов для обработки:", f"[bold green]{stats.get('total_found', 0):,}[/bold green]")
        report_table.add_section()
        archive_size = sum(j.get('size', 0) for j in archive_jobs)
        copy_size = sum(j.get('size', 0) for j in copy_jobs)
        report_table.add_row("Заданий на архивацию:", f"[yellow]{len(archive_jobs):,}[/yellow] ({decimal(archive_size)})")
        report_table.add_row("Заданий на копирование:", f"[yellow]{len(copy_jobs):,}[/yellow] ({decimal(copy_size)})")
        total_processed_count = stats.get('total_found', 0) - (len(copy_jobs) + len(archive_jobs))
        report_table.add_row("Пропущено (уже выполнены):", f"[dim]{total_processed_count:,}[/dim]")
        console.print(report_table)

        choices = ["s", "q"]
        prompt_text = "\n[bold]Выберите действие: ([green]S[/green])тарт / ([red]Q[/red])uit"
        malformed_lines = stats.get('malformed_lines', [])
        if malformed_lines:
            choices.append("e")
            prompt_text += " / ([yellow]E[/yellow])rrors"
        choice = Prompt.ask(prompt_text, choices=choices, default="s").lower()
        if choice == 's': return True
        if choice == 'q': return False
        if choice == 'e' and malformed_lines:
            console.print("\n[bold yellow]----- Список некорректных строк (первые 50) -----[/bold yellow]")
            for num, line, reason in malformed_lines[:50]: console.print(f"[dim]Строка #{num} ({reason}):[/dim] {line}")
            console.input("\n[bold]Нажмите [green]Enter[/green] для возврата в меню...[/bold]")
            console.clear()

# ИСПРАВЛЕНО: Явно принимает is_dry_run
# Замените эту функцию целиком
def process_job_worker(worker_id, job, config, disk_manager, is_dry_run, is_debug_mode, progress_callback=None):
    """
    Обрабатывает задание, отправляет обновления в очередь и корректно парсит rsync, читая посимвольно.
    """
    short_name = job.get('tar_filename') or os.path.basename(job['key'])
    op_type_text = "[yellow]Архивация[/yellow]" if job['type'] == 'sequence' else "[cyan]Копирование[/cyan]"
    status_queue.put((worker_id, {"status": op_type_text, "job": job, "progress": 0}))

    try:
        dest_mount_point = disk_manager.get_current_destination()
        source_root = config.get('source_root')
        destination_root = config.get('destination_root', '/')
        absolute_source_key = job['key']
        if source_root and absolute_source_key.startswith(os.path.normpath(source_root) + os.sep):
            rel_path = os.path.relpath(absolute_source_key, source_root)
        else:
            rel_path = absolute_source_key.lstrip(os.path.sep)
        dest_path = os.path.normpath(os.path.join(dest_mount_point, destination_root.lstrip(os.path.sep), rel_path))
        source_keys_to_log = []

        if job['type'] == 'sequence':
            if not is_dry_run:
                if not archive_sequence_to_destination(job, dest_path, progress_callback):
                    raise RuntimeError(f"Не удалось создать архив {short_name}")
            else:
                total_files = len(job.get('source_files', []))
                for i in range(total_files):
                    time.sleep(0.005)
                    if progress_callback: progress_callback(i + 1, total_files)
            source_keys_to_log = job['source_files']
        else:  # 'file'
            source_keys_to_log = [absolute_source_key]
            if not is_dry_run:
                # --- ИСПРАВЛЕННЫЙ БЛОК: ЗАПУСК RSYNC С ПОСИМВОЛЬНЫМ ЧТЕНИЕМ ---
                if not os.path.exists(absolute_source_key): raise FileNotFoundError(f"Исходный файл не найден: {absolute_source_key}")
                os.makedirs(os.path.dirname(dest_path), exist_ok=True)

                rsync_cmd = ["rsync", "-a", "--no-i-r", "--progress", absolute_source_key, dest_path]
                status_queue.put((worker_id, {"status": "[blue]rsync...[/blue]", "progress": 0}))

                process = subprocess.Popen(rsync_cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, encoding='utf-8', errors='ignore')

                progress_re = re.compile(r'\s+(\d+)%')
                line_buffer = ""
                # Читаем по одному символу, чтобы ловить и '\r' и '\n'
                for char in iter(lambda: process.stdout.read(1), ''):
                    if char in ['\r', '\n']:
                        match = progress_re.search(line_buffer)
                        if match:
                            percentage = int(match.group(1))
                            status_queue.put((worker_id, {"progress": percentage}))
                        line_buffer = "" # Сбрасываем буфер строки
                    else:
                        line_buffer += char # Накапливаем строку

                process.stdout.close()
                return_code = process.wait()
                if return_code != 0:
                    error_output = process.stderr.read()
                    # Игнорируем ошибку от прерывания по Ctrl+C
                    if "died with <Signals.SIGINT: 2>" not in error_output and return_code != -2:
                         raise subprocess.CalledProcessError(return_code, rsync_cmd, stderr=error_output)
                # -----------------------------------------------------------------
            else: # Dry-run
                steps = 5 if is_debug_mode else 3
                delay = 0.7 if is_debug_mode else 0.2
                for i in range(steps):
                    status_queue.put((worker_id, {"status": "[cyan]Симуляция[/cyan]", "progress": f"{i+1}/{steps}"}))
                    time.sleep(delay)

        final_status = "[bold green]Завершено[/bold green]"
        if is_dry_run: final_status = "[green]Готово (dr)[/green]"
        status_queue.put((worker_id, {"status": final_status, "progress": 100}))
        return (job['type'], job['size'], source_keys_to_log, dest_path)
    except Exception as e:
        if not isinstance(e, KeyboardInterrupt):
            status_queue.put((worker_id, {"status": "[bold red]Ошибка[/bold red]", "progress": 0}))
            log.error(f"Ошибка при обработке {job['key']}: {e}")
            with file_lock:
                with open(config['error_log_file'], "a", encoding='utf-8') as f:
                    f.write(f"{time.asctime()};{job['key']};{e}\n")
        return (None, 0, None, None)
# --- Функции TUI ---

def make_layout() -> Layout:
    layout = Layout(name="root")
    layout.split_column(Layout(name="top", size=5), Layout(name="middle"), Layout(name="bottom", size=3))
    layout["top"].split_row(Layout(name="summary"), Layout(name="disks"))
    return layout

def generate_summary_panel(plan, completed) -> Panel:
    table = Table(box=None, expand=True)
    table.add_column("Тип задания", style="cyan", no_wrap=True)
    table.add_column("Выполнено", style="green", justify="right")
    table.add_column("Размер", style="green", justify="right")
    s_plan, f_plan = plan.get('sequences', {}), plan.get('files', {})
    s_done, s_total = completed['sequence']['count'], s_plan.get('count', 0)
    s_size_done, s_size_total = completed['sequence']['size'], s_plan.get('size', 0)
    table.add_row("Архивация", f"{s_done} / {s_total}", f"{decimal(s_size_done)} / {decimal(s_size_total)}")
    f_done, f_total = completed['files']['count'], f_plan.get('count', 0)
    f_size_done, f_size_total = completed['files']['size'], f_plan.get('size', 0)
    table.add_row("Копирование", f"{f_done} / {f_total}", f"{decimal(f_size_done)} / {decimal(f_size_total)}")
    total_count_done, total_count_plan = s_done + f_done, s_total + f_total
    total_size_done, total_size_plan = s_size_done + f_size_done, s_size_total + f_size_total
    table.add_row("[bold]Всего[/bold]", f"[bold]{total_count_done} / {total_count_plan}[/bold]", f"[bold]{decimal(total_size_done)} / {decimal(total_size_plan)}[/bold]")
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

def generate_workers_panel(threads) -> Panel:
    table = Table(box=None, expand=True, show_header=True)
    table.add_column("Размер", justify="right", style="cyan", width=12)
    table.add_column("Имя файла", style="white", no_wrap=True, ratio=2)
    table.add_column("Статус", justify="left", style="white", width=15)
    table.add_column("Прогресс", justify="left", ratio=2)
    for worker_id in range(1, threads + 1):
        stats = worker_stats.get(worker_id) # Используем get для безопасности
        if stats and (job := stats.get("job")):
            size_str = decimal(job['size'])
            short_name = job.get('tar_filename') or os.path.basename(job['key'])
            status_text, progress_val = stats.get("status", ""), stats.get("progress", 0)
            progress_widget = ""
            if isinstance(progress_val, (int, float)) and progress_val > 0:
                progress_widget = Progress(BarColumn(bar_width=None), TaskProgressColumn(), expand=True)
                progress_widget.add_task("p", total=100, completed=progress_val)
            else:
                progress_widget = str(progress_val) if progress_val else ""
            table.add_row(size_str, short_name, status_text, progress_widget)
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

    disk_manager = DiskManager(config['mount_points'], config['threshold'], config.get('disk_strategy', 'fill')) if not is_dry_run else type('FakeDisk', (), {'active_disk': config['mount_points'][0] if config['mount_points'] else "/dry/run/dest", 'get_current_destination': lambda self: self.active_disk, 'get_all_disks_status': lambda self: [(p, 0.0) for p in config['mount_points']]})()
    if not is_dry_run and not disk_manager.active_disk: return

    for i in range(1, config['threads'] + 1):
        worker_stats[i] = {"status": "[grey50]Ожидание...[/grey50]", "job": None, "progress": 0}

    layout = make_layout()
    completed_stats = {"sequence": {"count": 0, "size": 0}, "files": {"count": 0, "size": 0}}
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
                            # КЛЮЧЕВОЕ ИСПРАВЛЕНИЕ: Явная передача флагов
                            return process_job_worker(worker_id, job, config, disk_manager, is_dry_run, is_debug_mode)
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

                        future = executor.submit(process_job_worker, 1, job, config, disk_manager, is_dry_run, is_debug_mode, progress_callback)

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
    args = parser.parse_args()
    main(args)
