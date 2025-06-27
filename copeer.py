# archiver_copier_v2.py
# Для установки зависимостей используйте менеджер пакетов uv:
# 1. uv venv
# 2. source .venv/bin/activate (или .\.venv\Scripts\activate в Windows)
# 3. uv pip install -r requirements.txt

import os
import sys
import csv
import subprocess
import logging
import time
import yaml
import re
import tarfile
import argparse
from pathlib import Path
from rich.console import Console
from rich.progress import Progress, BarColumn, TextColumn, TaskProgressColumn, TransferSpeedColumn, TimeRemainingColumn
from rich.panel import Panel
from rich.layout import Layout
from rich.live import Live
from rich.table import Table
from rich.logging import RichHandler
from rich.filesize import decimal
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock, get_ident
from collections import defaultdict

console = Console()

# --- ИЗМЕНЕНИЕ: ВЕРСИЯ ---
__version__ = "2.0.0"
# --- Конфигурация и глобальные переменные ---
CONFIG_FILE = "config.yaml"
DEFAULT_CONFIG = {
    'mount_points': ["/mnt/disk1", "/mnt/disk2"],
    'source_base_path': None,
    'threshold': 98.0,
    'state_file': "copier_state.csv",
    'mapping_file': "mapping.csv",
    'error_log_file': "errors.log",
    'dry_run': False,
    'threads': 8,
    'min_files_for_sequence': 100,
    'image_extensions': ['dpx', 'tiff', 'tif', 'exr', 'png', 'jpg', 'jpeg', 'tga']
}
SEQUENCE_RE = re.compile(r'^(.*?)[\._]*(\d+)\.([a-zA-Z0-9]+)$', re.IGNORECASE)

logging.basicConfig(level="INFO", format="%(message)s", datefmt="[%X]", handlers=[RichHandler(rich_tracebacks=True, show_path=False, console=console)])
log = logging.getLogger("rich")

processed_items_keys = set()
file_lock = Lock()
worker_stats = defaultdict(lambda: {"status": "[grey50]Ожидание...[/grey50]", "speed": 0})

# --- Классы и основные функции (без изменений) ---

class DiskManager:
    """Управляет выбором диска для записи, чтобы не превышать порог заполнения."""
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

def load_config():
    config = DEFAULT_CONFIG.copy()
    if os.path.exists(CONFIG_FILE):
        try:
            with open(CONFIG_FILE, 'r', encoding='utf-8') as f: config.update(yaml.safe_load(f) or {})
        except Exception as e: console.print(f"[bold red]Ошибка чтения {CONFIG_FILE}: {e}. Использованы значения по умолчанию.[/bold red]")
    else:
        with open(CONFIG_FILE, 'w', encoding='utf-8') as f: yaml.dump(DEFAULT_CONFIG, f, sort_keys=False, allow_unicode=True)
        log.info(f"Создан файл конфигурации по умолчанию: [cyan]{CONFIG_FILE}[/cyan]")
    config['image_extensions'] = set(e.lower() for e in config.get('image_extensions', []))
    return config

def load_previous_state(state_file):
    if os.path.exists(state_file):
        try:
            with open(state_file, 'r', encoding='utf-8') as f: processed_items_keys.update(row[0] for row in csv.reader(f) if row)
            log.info(f"Загружено [bold]{len(processed_items_keys)}[/bold] записей из файла состояния.")
        except Exception as e: log.error(f"Не удалось прочитать файл состояния {state_file}: {e}")

def write_log(file_path, key, dest_path=None):
    with file_lock:
        with open(DEFAULT_CONFIG['state_file'], "a", newline='', encoding='utf-8') as f: csv.writer(f).writerow([key])
        if dest_path:
            with open(DEFAULT_CONFIG['mapping_file'], "a", newline='', encoding='utf-8') as f: csv.writer(f).writerow([key, dest_path])

def find_sequences(dirs, config):
    all_sequences, sequence_files = [], set()
    for dir_path, files_with_sizes in dirs.items():
        sequences_in_dir = defaultdict(list)
        for filename, file_size in files_with_sizes:
            match = SEQUENCE_RE.match(filename)
            if match and match.group(3).lower() in config['image_extensions']:
                prefix, frame, ext = match.groups()
                full_path = os.path.join(dir_path, filename)
                sequences_in_dir[(prefix, ext.lower())].append((int(frame), full_path, file_size))

        for (prefix, ext), file_tuples in sequences_in_dir.items():
            if len(file_tuples) >= config['min_files_for_sequence']:
                file_tuples.sort()
                frames, full_paths, sizes = zip(*file_tuples)
                min_frame, max_frame = min(frames), max(frames)

                # --- ИСПРАВЛЕНИЕ ЗДЕСЬ ---
                # Разбиваем присваивание на две строки, чтобы избежать UnboundLocalError
                safe_prefix = re.sub(r'[^\w\.\-]', '_', prefix.strip())
                tar_filename = f"{safe_prefix}.{min_frame:04d}-{max_frame:04d}.{ext}.tar"

                virtual_tar_path = os.path.join(dir_path, tar_filename)
                all_sequences.append({
                    'type': 'sequence',
                    'key': virtual_tar_path,
                    'dir_path': dir_path,
                    'tar_filename': tar_filename,
                    'source_files': list(full_paths),
                    'size': sum(sizes)
                })
                sequence_files.update(full_paths)
    return all_sequences, sequence_files

# --- ИЗМЕНЕНО: Функция теперь возвращает детальную сводку для TUI ---
def analyze_and_plan_jobs(input_csv_path, config):
    """Анализирует CSV, формирует план работ и возвращает детальную сводку."""
    console.rule("[yellow]Шаг 1: Анализ и планирование[/yellow]")
    log.info(f"Анализ файла: [bold cyan]{input_csv_path}[/cyan]")

    line_parser = re.compile(r'^"(.*?)","(.*?)",.*,"(\d+)"$')
    dirs, all_files_from_csv = defaultdict(list), {}
    base_path = config.get('source_base_path')
    if base_path: log.info(f"Используется базовый путь источника: [cyan]{base_path}[/cyan]")
    try:
        with open(input_csv_path, 'r', encoding='utf-8', errors='ignore') as f:
            for i, line in enumerate(f):
                line = line.strip()
                if not line: continue
                match = line_parser.match(line)
                if not match: continue
                rel_path, file_type, size_str = match.groups()
                if 'regular file' not in file_type: continue
                try: size = int(size_str)
                except ValueError: continue
                full_path = os.path.normpath(os.path.join(base_path, rel_path) if base_path else rel_path)
                path_obj = Path(full_path)
                dirs[str(path_obj.parent)].append((path_obj.name, size))
                all_files_from_csv[full_path] = size
    except FileNotFoundError: console.print(f"[bold red]Критическая ошибка: CSV-файл не найден: {input_csv_path}[/]"); sys.exit(1)
    except Exception as e: console.print(f"[bold red]Критическая ошибка при чтении CSV: {e}[/]"); sys.exit(1)

    if not all_files_from_csv:
        log.warning("В CSV не найдено ни одного файла для обработки. Завершение.")
        sys.exit(0)

    sequences, sequence_files = find_sequences(dirs, config)
    standalone_files = set(all_files_from_csv.keys()) - sequence_files
    jobs = sequences + [{'type': 'file', 'key': f, 'size': all_files_from_csv[f]} for f in standalone_files]
    jobs_to_process = [job for job in jobs if job['key'] not in processed_items_keys]

    # Сортируем задания по размеру, от самого большого к самому маленькому.
    # Это смешает секвенции и файлы для более эффективной работы потоков.
    jobs_to_process.sort(key=lambda j: j['size'], reverse=True)

    seq_jobs = [j for j in jobs_to_process if j['type'] == 'sequence']
    file_jobs = [j for j in jobs_to_process if j['type'] == 'file']

    # --- ИЗМЕНЕНО: Формируем словарь со сводкой для TUI ---
    plan_summary = {
        "sequences": {"count": len(seq_jobs), "size": sum(j['size'] for j in seq_jobs)},
        "files": {"count": len(file_jobs), "size": sum(j['size'] for j in file_jobs)},
        "total": {"count": len(jobs_to_process), "size": sum(j['size'] for j in jobs_to_process)},
        "skipped": len(jobs) - len(jobs_to_process)
    }

    source_root = os.path.commonpath(list(all_files_from_csv.keys())) if all_files_from_csv else (base_path or os.getcwd())
    return jobs_to_process, plan_summary, source_root

def archive_sequence_to_destination(job, dest_tar_path):
    try:
        os.makedirs(os.path.dirname(dest_tar_path), exist_ok=True)
        with tarfile.open(dest_tar_path, "w") as tar:
            for file_path in job['source_files']:
                if os.path.exists(file_path): tar.add(file_path, arcname=os.path.basename(file_path))
                else: log.warning(f"В секвенции не найден файл: {file_path}")
        return True
    except Exception as e:
        log.error(f"✖ Ошибка при создании архива {dest_tar_path}: {e}")
        if os.path.exists(dest_tar_path): os.remove(dest_tar_path)
        return False

def process_job_worker(job, source_root, config, disk_manager):
    thread_id, start_time = get_ident(), time.monotonic()
    try:
        dest_root = disk_manager.get_current_destination()
        try: rel_path = os.path.relpath(os.path.dirname(job['key']), start=source_root)
        except ValueError: rel_path = os.path.dirname(job['key']).lstrip(os.path.sep)

        if job['type'] == 'sequence':
            short_name = job['tar_filename']
            worker_stats[thread_id]['status'] = f"[yellow]Архивирую:[/] {short_name}"
            dest_path = os.path.join(dest_root, rel_path, short_name)
            if not config['dry_run']:
                if not archive_sequence_to_destination(job, dest_path): raise RuntimeError(f"Не удалось создать архив {short_name}")
            else: time.sleep(0.1)
        else:
            short_name = os.path.basename(job['key'])
            worker_stats[thread_id]['status'] = f"[cyan]Копирую:[/] {short_name}"
            dest_path = os.path.join(dest_root, rel_path, short_name)
            if not config['dry_run']:
                if not os.path.exists(job['key']): raise FileNotFoundError(f"Исходный файл не найден: {job['key']}")
                os.makedirs(os.path.dirname(dest_path), exist_ok=True)
                subprocess.run(["rsync", "-a", "--checksum", job['key'], dest_path], check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            else: time.sleep(0.01)

        if not config['dry_run']: write_log(config['state_file'], job['key'], dest_path)
        elapsed = time.monotonic() - start_time
        speed = job['size'] / elapsed if elapsed > 0 else 0
        worker_stats[thread_id]['status'] = "[green]Свободен[/green]"
        worker_stats[thread_id]['speed'] = speed
        return job['key'], job['size'], job['type']
    except Exception as e:
        log.error(f"Ошибка при обработке {job['key']}: {e}")
        short_name = os.path.basename(job['key'])
        worker_stats[thread_id]['status'] = f"[red]Ошибка:[/] {short_name}"
        worker_stats[thread_id]['speed'] = -1
        with open(config['error_log_file'], "a", encoding='utf-8') as f: f.write(f"{time.asctime()};{job['key']};{e}\n")
        return None, 0, job['type']

# --- ИЗМЕНЕНО: Функции для TUI теперь создают более сложный макет ---
def make_layout() -> Layout:
    """Определяет структуру TUI."""
    layout = Layout(name="root")
    layout.split_column(
        Layout(name="top", ratio=1),
        Layout(name="middle", ratio=2),
        Layout(name="bottom", size=3) # Фиксированный размер для прогресс-бара
    )
    layout["top"].split_row(Layout(name="summary"), Layout(name="disks"))
    return layout

# --- НОВАЯ ФУНКЦИЯ: Генерирует панель сводки с динамическим прогрессом ---
def generate_summary_panel(plan, completed) -> Panel:
    table = Table(box=None, expand=True)
    table.add_column("Тип задания", style="cyan", no_wrap=True)
    table.add_column("Выполнено (шт)", style="green", justify="right")
    table.add_column("Размер", style="green", justify="right")

    # Секвенции
    s_done = completed['sequence']['count']
    s_total = plan['sequences']['count']
    s_size_done = completed['sequence']['size']
    s_size_total = plan['sequences']['size']
    table.add_row(
        "Архивация секвенций",
        f"{s_done} / {s_total}",
        f"{decimal(s_size_done)} / {decimal(s_size_total)}"
    )
    # Файлы
    f_done = completed['files']['count']
    f_total = plan['files']['count']
    f_size_done = completed['files']['size']
    f_size_total = plan['files']['size']
    table.add_row(
        "Копирование файлов",
        f"{f_done} / {f_total}",
        f"{decimal(f_size_done)} / {decimal(f_size_total)}"
    )
    # Всего
    t_done = s_done + f_done
    t_total = s_total + f_total
    t_size_done = s_size_done + f_size_done
    t_size_total = s_size_total + f_size_total
    table.add_row(
        "[bold]Всего[/bold]",
        f"[bold]{t_done} / {t_total}[/bold]",
        f"[bold]{decimal(t_size_done)} / {decimal(t_size_total)}[/bold]"
    )
    return Panel(table, title="📊 План выполнения", border_style="yellow")

def generate_disks_panel(disk_manager: DiskManager, config) -> Panel:
    table = Table(box=None, expand=True)
    table.add_column("Диск", style="white", no_wrap=True)
    table.add_column("Заполнено", style="green", ratio=1)
    table.add_column("%", style="bold", justify="right")
    for mount, percent in disk_manager.get_all_disks_status():
        color = "green" if percent < config['threshold'] else "red"
        bar = Progress(BarColumn(bar_width=None, style=color, complete_style=color), expand=True)
        task_id = bar.add_task("disk_usage", total=100); bar.update(task_id, completed=percent)
        is_active = " (*)" if mount == disk_manager.active_disk else ""
        table.add_row(f"[bold]{mount}{is_active}[/bold]", bar, f"{percent:.1f}%")
    return Panel(table, title="📦 Диски", border_style="blue")

def generate_workers_panel(threads) -> Panel:
    table = Table(expand=True)
    table.add_column("Поток", justify="center", style="cyan", width=12)
    table.add_column("Статус", style="white", no_wrap=True, ratio=2)
    table.add_column("Скорость", justify="right", style="magenta", width=15)
    sorted_workers = sorted(worker_stats.keys())
    for tid in sorted_workers:
        stats, speed_str = worker_stats[tid], ""
        if stats['speed'] > 0: speed_str = f"{decimal(stats['speed'])}/s"
        elif stats['speed'] == -1: speed_str = "[red]ERROR[/red]"
        else: speed_str = "[dim]---[/dim]"
        table.add_row(str(tid), stats['status'], speed_str)
    return Panel(table, title=f"👷 Потоки ({threads})", border_style="green")

# --- ИЗМЕНЕНО: Главная функция теперь управляет обновленным TUI ---
def main(args):
    config = load_config()
    if args.dry_run: config['dry_run'] = True
    if args.source_base: config['source_base_path'] = args.source_base

    console.rule(f"[bold]Archiver & Copier V{__version__}[/bold] | Режим: {'Dry Run' if config['dry_run'] else 'Реальная работа'}")

    load_previous_state(config['state_file'])

    jobs_to_process, plan_summary, source_root = analyze_and_plan_jobs(args.input_file, config)

    if not jobs_to_process:
        log.info("[bold green]✅ Все задания из входного файла уже выполнены. Завершение.[/bold green]")
        return
    if plan_summary['skipped'] > 0:
        log.info(f"[dim]Пропущено {plan_summary['skipped']} уже выполненных заданий.[/dim]")
    log.info(f"Определен общий корень источника для структуры назначения: [dim]{source_root}[/dim]")

    if not config['dry_run']:
        disk_manager = DiskManager(config['mount_points'], config['threshold'])
        if not disk_manager.active_disk: return
    else:
        class FakeDiskManager:
            active_disk = "/dry/run/dest"
            def get_current_destination(self): return self.active_disk
            def get_all_disks_status(self): return [(p, 0.0) for p in config['mount_points']]
        disk_manager = FakeDiskManager()
        log.info("Dry-run: симуляция записи на диск.")

    console.rule("[yellow]Шаг 2: Выполнение[/]")
    time.sleep(1)

    # --- ИЗМЕНЕНО: Настройка прогресс-бара и счетчиков для TUI ---
    job_counter_column = TextColumn(f"[cyan]0/{plan_summary['total']['count']} заданий[/cyan]")
    progress = Progress(
        TextColumn("[bold blue]Общий прогресс:[/bold blue]"), BarColumn(), TaskProgressColumn(), TextColumn("•"),
        job_counter_column, TextColumn("•"), TransferSpeedColumn(), TextColumn("•"), TimeRemainingColumn()
    )
    main_task = progress.add_task("выполнение", total=plan_summary['total']['size'])
    layout = make_layout()
    layout["bottom"].update(Panel(progress, title="🚀 Процесс выполнения", border_style="magenta", expand=False))

    completed_stats = {
        "sequence": {"count": 0, "size": 0},
        "files": {"count": 0, "size": 0}
    }
    jobs_completed_count = 0
    all_jobs_successful = True

    try:
        with Live(layout, screen=True, redirect_stderr=False, vertical_overflow="visible") as live:
            with ThreadPoolExecutor(max_workers=config['threads']) as executor:
                # Первоначальное отображение всех панелей
                layout["summary"].update(generate_summary_panel(plan_summary, completed_stats))
                layout["disks"].update(generate_disks_panel(disk_manager, config))
                layout["middle"].update(generate_workers_panel(config['threads']))

                future_to_job = {executor.submit(process_job_worker, job, source_root, config, disk_manager): job for job in jobs_to_process}

                for future in as_completed(future_to_job):
                    key, size_processed, job_type = future.result()

                    if key is not None:
                        # Обновляем счетчики для панели сводки
                        if job_type == 'sequence':
                            completed_stats['sequence']['count'] += 1
                            completed_stats['sequence']['size'] += size_processed
                        else:
                            completed_stats['files']['count'] += 1
                            completed_stats['files']['size'] += size_processed
                    else:
                        all_jobs_successful = False

                    # Обновляем общий прогресс
                    jobs_completed_count += 1
                    progress.update(main_task, advance=size_processed)
                    job_counter_column.text_format = f"[cyan]{jobs_completed_count}/{plan_summary['total']['count']} заданий[/cyan]"

                    # Обновление всех панелей TUI в реальном времени
                    layout["summary"].update(generate_summary_panel(plan_summary, completed_stats))
                    if not config['dry_run']:
                        layout["disks"].update(generate_disks_panel(disk_manager, config))
                    layout["middle"].update(generate_workers_panel(config['threads']))

    except (Exception, KeyboardInterrupt):
        console.print_exception(show_locals=False)
        console.print("\n[bold red]Процесс прерван. Состояние сохранено.[/bold red]")
        sys.exit(1)

    if all_jobs_successful and progress.finished:
        console.rule("[bold green]✅ Все задания успешно выполнены[/bold green]")
    else:
        console.rule("[bold yellow]Выполнение завершено, но были ошибки. Проверьте лог.[/bold yellow]")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Анализирует CSV, архивирует и копирует файлы в хранилище.")
    parser.add_argument(
        "-v", "--version",
        action="version",
        version=f"%(prog)s v{__version__}"
    )
    parser.add_argument("input_file", help="Путь к CSV файлу со списком исходных файлов.")
    parser.add_argument("--dry-run", action="store_true", help="Выполнить анализ без реального копирования.")
    parser.add_argument("--source-base", help="Базовый путь для относительных путей из CSV.")
    args = parser.parse_args()
    main(args)
