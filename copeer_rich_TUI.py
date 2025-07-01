# copeer.py
"""
Утилита для архивации и параллельного копирования больших объемов данных
с TUI-дашбордом на базе Rich и возможностью возобновления работы.
"""

# Стандартная библиотека
import argparse
import csv
import logging
import os
import re
import subprocess
import sys
import tarfile
import time
import yaml
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from threading import Lock, get_ident

# Импорты для Unix-систем (для живого прогресса rsync)
try:
    import fcntl
    import select
    UNIX_SYSTEM = True
except ImportError:
    UNIX_SYSTEM = False

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
__version__ = "3.1.0"  # Добавлена статистика по скорости
CONFIG_FILE = "config.yaml"
DEFAULT_CONFIG = {
    'mount_points': ["/mnt/disk1", "/mnt/disk2"],
    'source_root': '/path/to/source/data',
    'destination_root': '/',
    'threshold': 98.0,
    'state_file': "copier_state.csv",
    'mapping_file': "mapping.csv",
    'error_log_file': "errors.log",
    'dry_run_mapping_file': "dry_run_mapping.csv",
    'dry_run': False,
    'threads': 8,
    'min_files_for_sequence': 50,
    'image_extensions': ['dpx', 'cri', 'tiff', 'tif', 'exr', 'png', 'jpg', 'jpeg', 'tga', 'j2c'],
    'progress_mode': 'advanced' if UNIX_SYSTEM else 'simple'
}
SEQUENCE_RE = re.compile(r'^(.*?)[\._]*(\d+)\.([a-zA-Z0-9]+)$', re.IGNORECASE)

logging.basicConfig(level="INFO", format="%(message)s", datefmt="[%X]", handlers=[RichHandler(rich_tracebacks=True, show_path=False, console=console)])
log = logging.getLogger("rich")

file_lock = Lock()
worker_stats = defaultdict(lambda: {"status": "[grey50]Ожидание...[/grey50]", "speed": "", "progress": None})


# --- Основные классы ---

class DiskManager:
    """Управляет выбором диска для записи."""
    def __init__(self, mount_points, threshold):
        self.mount_points = mount_points
        self.threshold = threshold
        self.active_disk = None
        self.lock = Lock()
        self._select_initial_disk()

    def _get_disk_usage(self, path):
        if not os.path.exists(path): return 0.0
        try:
            st = os.statvfs(path)
            used = (st.f_blocks - st.f_bfree) * st.f_frsize
            total = st.f_blocks * st.f_frsize
            return round(used / total * 100, 2) if total > 0 else 0
        except FileNotFoundError: return 100

    def _select_initial_disk(self):
        for mount in self.mount_points:
            if not os.path.exists(mount):
                log.warning(f"Точка монтирования [bold yellow]{mount}[/bold yellow] не существует. Пропускаю.")
                continue
            if self._get_disk_usage(mount) < self.threshold:
                self.active_disk = mount
                log.info(f"Выбран начальный диск: [bold green]{self.active_disk}[/bold green]")
                return
        log.error("🛑 Не найдено подходящих дисков для начала работы.")
        raise RuntimeError("Не найдено подходящих дисков")

    def get_current_destination(self):
        with self.lock:
            if not self.active_disk: raise RuntimeError("🛑 Нет доступных дисков.")
            if self._get_disk_usage(self.active_disk) >= self.threshold:
                log.warning(f"Диск [bold]{self.active_disk}[/bold] заполнен. Ищу следующий...")
                available_disks = [m for m in self.mount_points if os.path.exists(m)]
                try:
                    current_index = available_disks.index(self.active_disk)
                    next_disks = available_disks[current_index + 1:] + available_disks[:current_index]
                except (ValueError, IndexError): next_disks = available_disks
                self.active_disk = next((m for m in next_disks if self._get_disk_usage(m) < self.threshold), None)
                if self.active_disk: log.info(f"Переключился на диск: [bold green]{self.active_disk}[/bold green]")
            if not self.active_disk: raise RuntimeError("🛑 Нет доступных дисков: все переполнены или недоступны.")
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

def archive_sequence_to_destination(job, dest_tar_path):
    os.makedirs(os.path.dirname(dest_tar_path), exist_ok=True)
    with tarfile.open(dest_tar_path, "w") as tar:
        for file_path in job['source_files']:
            if os.path.exists(file_path): tar.add(file_path, arcname=os.path.basename(file_path))
            else: log.warning(f"В секвенции не найден файл: {file_path}")

# --- Логика анализа и выполнения ---

def analyze_and_plan_jobs(input_csv_path, config, processed_items_keys):
    console.rule("[yellow]Шаг 1: Анализ и планирование[/]")
    console.print(f"Анализ файла: [bold cyan]{input_csv_path}[/bold cyan]")

    parser_primary = re.compile(r'^"([^"]+)","([^"]+)",.*')
    parser_fallback = re.compile(r'^"([^"]+\.\w{2,5})",.*', re.IGNORECASE)

    dirs, all_files_from_csv = defaultdict(list), {}
    source_root = config.get('source_root')
    if source_root:
        console.print(f"Используется корень источника: [cyan]{source_root}[/cyan]")

    lines_total, lines_ignored_dirs, malformed_lines = 0, 0, []

    try:
        with open(input_csv_path, 'r', encoding='utf-8', errors='ignore') as f: total_lines_for_progress = sum(1 for _ in f)

        with Progress(console=console) as progress:
            task = progress.add_task("[green]Анализ CSV...", total=total_lines_for_progress)
            with open(input_csv_path, 'r', encoding='utf-8', errors='ignore') as f:
                for line in f:
                    progress.update(task, advance=1)
                    lines_total += 1
                    cleaned_line = line.strip().replace('""', '"')
                    if not cleaned_line:
                        malformed_lines.append((lines_total, line, "Пустая строка")); continue

                    rel_path, file_type, size = None, "", 0
                    match = parser_primary.match(cleaned_line)
                    if match:
                        rel_path, file_type = match.groups()
                    else:
                        match = parser_fallback.match(cleaned_line)
                        if match: rel_path, file_type = match.group(1), "file"

                    if rel_path:
                        if 'directory' in file_type:
                            lines_ignored_dirs += 1; continue

                        size_match = re.search(r',"(\d+)"$', cleaned_line)
                        if size_match:
                            try: size = int(size_match.group(1))
                            except (ValueError, IndexError): size = 0

                        absolute_source_path = os.path.normpath(os.path.join(source_root, rel_path) if source_root else rel_path)
                        path_obj = Path(absolute_source_path)
                        dirs[str(path_obj.parent)].append((path_obj.name, size))
                        all_files_from_csv[absolute_source_path] = size
                    else:
                        malformed_lines.append((lines_total, line, "Неизвестный формат строки"))

    except (KeyboardInterrupt, SystemExit): console.print("\n[yellow]Анализ прерван пользователем.[/yellow]"); sys.exit(0)
    except FileNotFoundError: console.print(f"[bold red]Критическая ошибка: CSV-файл не найден: {input_csv_path}[/]"); sys.exit(1)
    except Exception as e: console.print(f"[bold red]Критическая ошибка при чтении CSV: {e}[/]"); sys.exit(1)

    if not all_files_from_csv:
        if malformed_lines:
            console.print("\n[bold red]Не найдено корректных файлов. Обнаружены проблемы:[/bold red]")
            for num, err_line, reason in malformed_lines[:50]: console.print(f"[dim]Строка #{num} ({reason}):[/dim] {err_line}")
        else: log.warning("В CSV не найдено ни одного файла для обработки.")
        sys.exit(0)

    sequences, sequence_files = find_sequences(dirs, config)
    standalone_files = set(all_files_from_csv.keys()) - sequence_files

    jobs = sequences + [{'type': 'file', 'key': f, 'size': all_files_from_csv[f]} for f in standalone_files]
    jobs_to_process = [job for job in jobs if job['key'] not in processed_items_keys]

    if not jobs_to_process:
        log.info("[bold green]✅ Все задания из входного файла уже выполнены. Завершение.[/bold green]")
        return [], None

    jobs_to_process.sort(key=lambda j: j.get('size', 0), reverse=True)
    seq_jobs = [j for j in jobs_to_process if j['type'] == 'sequence']
    file_jobs = [j for j in jobs_to_process if j['type'] == 'file']

    while True:
        console.rule("[yellow]Отчет по анализу[/]")
        report_table = Table(title=None, show_header=False, box=None, padding=(0, 2))
        report_table.add_column("Параметр", style="cyan", no_wrap=True)
        report_table.add_column("Значение", style="white", justify="right")

        report_table.add_row("Всего строк в CSV файле:", f"{lines_total:,}")
        report_table.add_row("  Пропущено (директории):", f"[dim]{lines_ignored_dirs:,}[/dim]")
        malformed_count = len(malformed_lines)
        report_table.add_row(f"  Пропущено (неопознанный формат):", f"[{'red' if malformed_count > 0 else 'dim'}]{malformed_count:,}[/]")
        report_table.add_row("[bold]Найдено файлов для обработки:", f"[bold green]{len(all_files_from_csv):,}[/bold green]")
        report_table.add_section()
        report_table.add_row("Из них сгруппировано в секвенции:", f"{len(sequence_files):,}")
        report_table.add_row("  Что соответствует заданиям на архивацию:", f"[yellow]{len(sequences):,}[/yellow]")
        report_table.add_row("Осталось отдельных файлов для копирования:", f"{len(standalone_files):,}")
        report_table.add_section()
        report_table.add_row("Всего заданий до возобновления:", f"{len(jobs):,}")
        report_table.add_row("  Пропущено (уже выполнены):", f"[dim]{len(jobs) - len(jobs_to_process):,}[/dim]")
        report_table.add_row("[bold]Всего заданий к выполнению:", f"[bold bright_magenta]{len(jobs_to_process):,}[/bold bright_magenta]")

        console.print(report_table)

        choices = ["s", "q"]
        prompt_text = "\n[bold]Выберите действие: ([green]S[/green])тарт / ([red]Q[/red])uit"
        if malformed_count > 0:
            choices.append("e")
            prompt_text += " / ([yellow]E[/yellow])rrors"

        choice = Prompt.ask(prompt_text, choices=choices, default="s").lower()

        if choice == 's': break
        elif choice == 'q': console.print("[yellow]Выполнение отменено пользователем.[/yellow]"); sys.exit(0)
        elif choice == 'e' and malformed_count > 0:
            console.print("\n[bold yellow]----- Список некорректных строк (первые 50) -----[/bold yellow]")
            for num, line, reason in malformed_lines[:50]: console.print(f"[dim]Строка #{num} ({reason}):[/dim] {line}")
            console.input("\n[bold]Нажмите [green]Enter[/green] для возврата в меню...[/bold]")
            console.clear()

    plan_summary = {
        "sequences": {"count": len(seq_jobs), "size": sum(j['size'] for j in seq_jobs)},
        "files": {"count": len(file_jobs), "size": sum(j['size'] for j in file_jobs)},
        "total": {"count": len(jobs_to_process), "size": sum(j['size'] for j in jobs_to_process)},
        "skipped": len(jobs) - len(jobs_to_process)
    }
    return jobs_to_process, plan_summary


def process_job_worker(job, config, disk_manager):
    thread_id = get_ident()
    progress_mode = config.get('progress_mode', 'simple')
    is_dry_run = config['dry_run']

    short_name = job.get('tar_filename') or os.path.basename(job['key'])
    status_text = f"[yellow]Архивирую:[/] {short_name}" if job['type'] == 'sequence' else f"[cyan]Копирую:[/] {short_name}"

    worker_stats[thread_id] = {"status": status_text, "speed": "", "progress": 0 if progress_mode == 'advanced' and not is_dry_run else None}

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

        if job['type'] == 'sequence':
            if not is_dry_run: archive_sequence_to_destination(job, dest_path)
            else: time.sleep(0.01)
            source_keys_to_log = job['source_files']
        else: # 'file'
            source_keys_to_log = [absolute_source_key]
            if not is_dry_run:
                if not os.path.exists(absolute_source_key): raise FileNotFoundError(f"Исходный файл не найден: {absolute_source_key}")
                os.makedirs(os.path.dirname(dest_path), exist_ok=True)

                if progress_mode == 'advanced' and UNIX_SYSTEM:
                    rsync_cmd = ["rsync", "-a", "--checksum", "--info=progress2", "--no-i-r", "--outbuf=L", absolute_source_key, dest_path]
                    process = subprocess.Popen(rsync_cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, bufsize=1, universal_newlines=True, encoding='utf-8', errors='replace')
                    fd = process.stdout.fileno()
                    flags = fcntl.fcntl(fd, fcntl.F_GETFL)
                    fcntl.fcntl(fd, fcntl.F_SETFL, flags | os.O_NONBLOCK)
                    progress_re = re.compile(r'\s*[\d,.]+[KMGT]?\s+(\d+)%\s+([\d.]+\w+/s)')
                    while process.poll() is None:
                        try:
                            chunk = process.stdout.read()
                            if chunk:
                                last_update = chunk.strip().split('\r')[-1]
                                match = progress_re.search(last_update)
                                if match: worker_stats[thread_id]['progress'], worker_stats[thread_id]['speed'] = int(match.group(1)), match.group(2)
                        except (IOError, TypeError): time.sleep(0.1)
                    if process.wait() != 0 and process.returncode != 20:
                        raise subprocess.CalledProcessError(process.returncode, rsync_cmd, stderr=process.stderr.read())
                else: # 'simple' mode
                    rsync_cmd = ["rsync", "-a", "--checksum", absolute_source_key, dest_path]
                    subprocess.run(rsync_cmd, check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
            else: # dry_run
                time.sleep(0.05)

        worker_stats[thread_id] = {"status": "[green]Свободен[/green]", "speed": "", "progress": None}
        return (job['type'], job['size'], source_keys_to_log, dest_path)

    except Exception as e:
        if not isinstance(e, KeyboardInterrupt):
            worker_stats[thread_id] = {"status": f"[red]Ошибка:[/] {short_name}", "speed": "ERROR", "progress": None}
            log.error(f"Ошибка при обработке {job['key']}: {e}")
            with file_lock:
                 with open(config['error_log_file'], "a", encoding='utf-8') as f:
                    f.write(f"{time.asctime()};{job['key']};{e}\n")
        return (None, 0, None, None)


# --- Функции для отрисовки TUI ---

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

    s_done, s_total, s_size_done, s_size_total = completed['sequence']['count'], plan['sequences']['count'], completed['sequence']['size'], plan['sequences']['size']
    table.add_row("Архивация", f"{s_done} / {s_total}", f"{decimal(s_size_done)} / {decimal(s_size_total)}")

    f_done, f_total, f_size_done, f_size_total = completed['files']['count'], plan['files']['count'], completed['files']['size'], plan['files']['size']
    table.add_row("Копирование", f"{f_done} / {f_total}", f"{decimal(f_size_done)} / {decimal(f_size_total)}")

    table.add_row("[bold]Всего[/bold]", f"[bold]{s_done + f_done} / {s_total + f_total}[/bold]", f"[bold]{decimal(s_size_done + f_size_done)} / {decimal(s_size_total + f_size_total)}[/bold]")
    return Panel(table, title="📊 План выполнения", border_style="yellow")

def generate_disks_panel(disk_manager: DiskManager, config) -> Panel:
    table = Table(box=None, expand=True)
    table.add_column("Диск", style="white", no_wrap=True)
    table.add_column("Заполнено", style="green", ratio=1)
    table.add_column("%", style="bold", justify="right")
    for mount, percent in disk_manager.get_all_disks_status():
        color = "green" if percent < config['threshold'] else "red"
        bar = Progress(BarColumn(bar_width=None), style=color, complete_style=color)
        bar.add_task("d", total=100, completed=percent)
        is_active = " (*)" if mount == disk_manager.active_disk else ""
        table.add_row(f"[bold]{mount}{is_active}[/bold]", bar, f"{percent:.1f}%")
    return Panel(table, title="📦 Диски", border_style="blue")

def generate_workers_panel(threads) -> Panel:
    table = Table.grid(expand=True, padding=(0, 1))
    table.add_column("Поток", justify="center", style="cyan", width=12)
    table.add_column("Статус", style="white", no_wrap=True)

    for tid in sorted(worker_stats.keys()):
        stats = worker_stats.get(tid, {})
        status = stats.get("status", "[grey50]Ожидание...[/grey50]")
        progress_val = stats.get("progress")

        if progress_val is not None and 0 <= progress_val <= 100:
            p_bar = Progress(BarColumn(bar_width=None), TextColumn("{task.percentage:>3.0f}%"))
            p_bar.add_task("p", total=100, completed=progress_val)
            status_grid = Table.grid(expand=True); status_grid.add_row(status); status_grid.add_row(p_bar)
            table.add_row(str(tid), status_grid)
        else:
            table.add_row(str(tid), status)

    return Panel(table, title=f"👷 Потоки ({threads})", border_style="green")


# --- Точка входа ---

# Замените эту функцию целиком
def main(args):
    """Главная функция скрипта."""
    config = load_config()
    if args.dry_run: config['dry_run'] = True

    # Поднимаем версию за этот критический фикс
    console.rule(f"[bold]Copeer v3.0.1[/bold] | Режим: {'Dry Run' if config['dry_run'] else 'Реальная работа'}")

    is_dry_run = config['dry_run']
    if is_dry_run:
        dry_run_log_path = config['dry_run_mapping_file']
        try:
            with open(dry_run_log_path, 'w', newline='', encoding='utf-8') as f:
                csv.writer(f).writerow(["source_path", "destination_path"])
            console.print(f"Отчет dry-run будет сохранен в: [cyan]{dry_run_log_path}[/cyan]")
        except IOError as e: console.print(f"[bold red]Не удалось создать файл отчета: {e}[/bold red]")

    processed_items_keys = set()
    load_previous_state(config['state_file'], processed_items_keys)

    jobs_to_process, plan_summary = analyze_and_plan_jobs(args.input_file, config, processed_items_keys)
    if not jobs_to_process: return

    disk_manager = DiskManager(config['mount_points'], config['threshold']) if not is_dry_run else type('FakeDisk', (), {'active_disk': config['mount_points'][0] if config['mount_points'] else "/dry/run/dest", 'get_current_destination': lambda self: self.active_disk, 'get_all_disks_status': lambda self: [(p, 0.0) for p in config['mount_points']]})()
    if not is_dry_run and not disk_manager.active_disk: return

    console.rule("[yellow]Шаг 2: Выполнение[/]")
    time.sleep(1)

    # --- ИЗМЕНЕНИЕ: Собираем ВЕСЬ интерфейс ДО запуска Live ---
    layout = make_layout()

    # Создаем все компоненты
    completed_stats = {"sequence": {"count": 0, "size": 0}, "files": {"count": 0, "size": 0}}

    job_counter_column = TextColumn(f"[cyan]0/{plan_summary['total']['count']} заданий[/cyan]")
    progress_bar = Progress(TextColumn("[bold blue]Общий прогресс:[/bold blue]"), BarColumn(), TaskProgressColumn(), TextColumn("•"),
                            job_counter_column, TextColumn("•"), TransferSpeedColumn(), TextColumn("•"), TimeRemainingColumn())
    main_task = progress_bar.add_task("выполнение", total=plan_summary['total']['count'])

    # Предварительно заполняем все слои layout'а
    layout["summary"].update(generate_summary_panel(plan_summary, completed_stats))
    layout["disks"].update(generate_disks_panel(disk_manager, config))
    layout["middle"].update(generate_workers_panel(config['threads']))
    layout["bottom"].update(Panel(progress_bar, title="🚀 Процесс выполнения", border_style="magenta", expand=False))

    jobs_completed_count, all_jobs_successful = 0, True

    try:
        # Теперь передаем в Live полностью готовый layout
        with Live(layout, screen=True, redirect_stderr=False, vertical_overflow="visible", refresh_per_second=4) as live:
            with ThreadPoolExecutor(max_workers=config['threads']) as executor:

                future_to_job = {executor.submit(process_job_worker, job, config, disk_manager): job for job in jobs_to_process}

                for future in as_completed(future_to_job):
                    job_type, size_processed, source_keys, dest_path = future.result()

                    if job_type:
                        for key in source_keys:
                            write_log(config['state_file'], config['mapping_file'], key, dest_path, is_dry_run)

                        if job_type == 'sequence':
                            completed_stats['sequence']['count'] += 1; completed_stats['sequence']['size'] += size_processed
                        else:
                            completed_stats['files']['count'] += 1; completed_stats['files']['size'] += size_processed
                    else:
                        all_jobs_successful = False

                    jobs_completed_count += 1
                    progress_bar.update(main_task, advance=1)
                    job_counter_column.text_format = f"[cyan]{jobs_completed_count}/{plan_summary['total']['count']} заданий[/cyan]"

                    # В цикле мы только ОБНОВЛЯЕМ панели, а не создаем их
                    layout["summary"].update(generate_summary_panel(plan_summary, completed_stats))
                    if not is_dry_run:
                        layout["disks"].update(generate_disks_panel(disk_manager, config))
                    layout["middle"].update(generate_workers_panel(config['threads']))

    except (KeyboardInterrupt, SystemExit):
        console.print("\n[bold red]Процесс прерван.[/bold red]")
        sys.exit(1)

    total_duration = time.monotonic() - (live.start_time if 'live' in locals() else time.monotonic())
    total_bytes_processed = completed_stats['sequence']['size'] + completed_stats['files']['size']
    final_avg_speed = total_bytes_processed / total_duration if total_duration > 0 else 0

    console.rule(f"[bold {'green' if all_jobs_successful else 'yellow'}]Выполнение завершено[/bold {'green' if all_jobs_successful else 'yellow'}]")
    console.print(f"  Общее время выполнения: {time.strftime('%H:%M:%S', time.gmtime(total_duration))}")
    console.print(f"  Всего обработано данных: {decimal(total_bytes_processed)}")
    console.print(f"  Средняя скорость: [bold magenta]{decimal(final_avg_speed)}/s[/bold magenta]")
    if not all_jobs_successful:
        console.print("[yellow]В процессе работы были ошибки. Проверьте лог.[/yellow]")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Анализирует CSV, архивирует и копирует файлы.", prog="copeer")
    parser.add_argument("-v", "--version", action="version", version=f"%(prog)s v{__version__}")
    parser.add_argument("input_file", help="Путь к CSV файлу со списком исходных файлов.")
    parser.add_argument("--dry-run", action="store_true", help="Выполнить анализ без реального копирования.")
    args = parser.parse_args()
    main(args)
