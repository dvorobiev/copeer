# copeer_rich_TUI.py
"""
Утилита для архивации и параллельного копирования больших объемов данных
с TUI-дашбордом на базе Rich и возможностью возобновления работы.
"""

# Стандартная библиотека
import argparse, csv, logging, os, re, subprocess, sys, tarfile, time, yaml, math
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from threading import Lock, get_ident

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
__version__ = "4.2.0"
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
}
SEQUENCE_RE = re.compile(r'^(.*?)[\._]*(\d+)\.([a-zA-Z0-9]+)$', re.IGNORECASE)

logging.basicConfig(level="INFO", format="%(message)s", datefmt="[%X]", handlers=[RichHandler(rich_tracebacks=True, show_path=False, console=console)])
log = logging.getLogger("rich")

file_lock = Lock()
worker_stats = defaultdict(lambda: {"status": "[grey50]Ожидание...[/grey50]"})


# --- Основные классы ---

class DiskManager:
    """Управляет выбором диска для записи."""
    def __init__(self, mount_points, threshold):
        self.mount_points, self.threshold, self.active_disk, self.lock = mount_points, threshold, None, Lock()
        self._select_initial_disk()

    def _get_disk_usage(self, path):
        if not os.path.exists(path): return 0.0
        try:
            st = os.statvfs(path); used = (st.f_blocks - st.f_bfree) * st.f_frsize; total = st.f_blocks * st.f_frsize
            return round(used / total * 100, 2) if total > 0 else 0
        except FileNotFoundError: return 100

    def _select_initial_disk(self):
        for mount in self.mount_points:
            if not os.path.exists(mount):
                log.warning(f"Точка монтирования [bold yellow]{mount}[/bold yellow] не существует. Пропускаю."); continue
            if self._get_disk_usage(mount) < self.threshold:
                self.active_disk = mount; log.info(f"Выбран начальный диск: [bold green]{self.active_disk}[/bold green]"); return
        log.error("🛑 Не найдено подходящих дисков для начала работы."); raise RuntimeError("Не найдено подходящих дисков")

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

def parse_scientific_notation(size_str: str) -> int:
    try:
        cleaned_str = size_str.replace(',', '.').strip()
        if 'E' in cleaned_str.upper(): return int(float(cleaned_str))
        return int(cleaned_str)
    except (ValueError, TypeError): return 0


# --- Логика анализа и выполнения ---

def analyze_and_plan_jobs(input_csv_path, config, processed_items_keys):
    console.rule("[yellow]Шаг 1: Анализ и планирование[/]")
    console.print(f"Анализ файла: [bold cyan]{input_csv_path}[/bold cyan]")
    dirs, all_files_from_csv = defaultdict(list), {}
    source_root = config.get('source_root')
    if source_root: console.print(f"Используется корень источника: [cyan]{source_root}[/cyan]")
    lines_total, lines_ignored_dirs, malformed_lines = 0, 0, []

    try:
        with open(input_csv_path, 'r', encoding='utf-8', errors='ignore') as f: total_lines_for_progress = sum(1 for _ in f)
        with Progress(console=console) as progress:
            task = progress.add_task("[green]Анализ CSV...", total=total_lines_for_progress)
            with open(input_csv_path, 'r', encoding='utf-8', errors='ignore') as f:
                reader = csv.reader(f, delimiter=';')
                for i, row in enumerate(reader):
                    progress.update(task, advance=1)
                    lines_total += 1
                    if not row or len(row) < 5: malformed_lines.append((i + 1, str(row), "Недостаточно колонок")); continue
                    rel_path, file_type, size_str = row[0], row[1], row[4]
                    if 'directory' in file_type: lines_ignored_dirs += 1; continue
                    if 'file' not in file_type: malformed_lines.append((i + 1, str(row), f"Неизвестный тип: {file_type}")); continue
                    size = parse_scientific_notation(size_str)
                    if size == 0 and size_str.strip() not in ('0', ''): malformed_lines.append((i + 1, str(row), f"Некорректный размер: {size_str}"))
                    absolute_source_path = os.path.normpath(os.path.join(source_root, rel_path) if source_root else rel_path)
                    path_obj = Path(absolute_source_path)
                    dirs[str(path_obj.parent)].append((path_obj.name, size))
                    all_files_from_csv[absolute_source_path] = size
    except (KeyboardInterrupt, SystemExit): console.print("\n[yellow]Анализ прерван пользователем.[/yellow]"); sys.exit(0)
    except Exception as e: console.print(f"[bold red]Критическая ошибка при чтении CSV: {e}[/bold red]"); sys.exit(1)

    if not all_files_from_csv: sys.exit(0)

    sequences, sequence_files = find_sequences(dirs, config)
    standalone_files = set(all_files_from_csv.keys()) - sequence_files
    archive_jobs_all = sequences
    copy_jobs_all = [{'type': 'file', 'key': f, 'size': all_files_from_csv[f]} for f in standalone_files]
    archive_jobs_to_process = [job for job in archive_jobs_all if job['key'] not in processed_items_keys]
    copy_jobs_to_process = [job for job in copy_jobs_all if job['key'] not in processed_items_keys]
    archive_jobs_to_process.sort(key=lambda j: j.get('size', 0), reverse=True)
    copy_jobs_to_process.sort(key=lambda j: j.get('size', 0), reverse=True)

    while True:
        console.rule("[yellow]Отчет по анализу[/]")
        report_table = Table(title=None, show_header=False, box=None, padding=(0, 2))
        report_table.add_column("Параметр", style="cyan", no_wrap=True)
        report_table.add_column("Значение", style="white", justify="right")
        report_table.add_row("Всего строк в CSV файле:", f"{lines_total:,}")
        report_table.add_row("  Пропущено (директории):", f"[dim]{lines_ignored_dirs:,}[/dim]")
        malformed_count = len(malformed_lines)
        report_table.add_row(f"  Пропущено (некорректный формат):", f"[{'red' if malformed_count > 0 else 'dim'}]{malformed_count:,}[/]")
        report_table.add_row("[bold]Найдено файлов для обработки:", f"[bold green]{len(all_files_from_csv):,}[/bold green]")
        report_table.add_section()
        archive_size = sum(j.get('size', 0) for j in archive_jobs_to_process)
        copy_size = sum(j.get('size', 0) for j in copy_jobs_to_process)
        report_table.add_row("Заданий на архивацию:", f"[yellow]{len(archive_jobs_to_process):,}[/yellow] ({decimal(archive_size)})")
        report_table.add_row("Заданий на копирование:", f"[yellow]{len(copy_jobs_to_process):,}[/yellow] ({decimal(copy_size)})")
        report_table.add_row("Пропущено (уже выполнены):", f"[dim]{(len(archive_jobs_all) + len(copy_jobs_all)) - (len(archive_jobs_to_process) + len(copy_jobs_to_process)):,}[/dim]")

        console.print(report_table)

        choices = ["s", "q"]
        prompt_text = "\n[bold]Выберите действие: ([green]S[/green])тарт / ([red]Q[/red])uit"
        if malformed_lines:
            choices.append("e")
            prompt_text += " / ([yellow]E[/yellow])rrors"

        choice = Prompt.ask(prompt_text, choices=choices, default="s").lower()

        if choice == 's': break
        elif choice == 'q': console.print("[yellow]Выполнение отменено пользователем.[/yellow]"); sys.exit(0)
        elif choice == 'e' and malformed_lines:
            console.print("\n[bold yellow]----- Список некорректных строк (первые 50) -----[/bold yellow]")
            for num, line, reason in malformed_lines[:50]: console.print(f"[dim]Строка #{num} ({reason}):[/dim] {line}")
            console.input("\n[bold]Нажмите [green]Enter[/green] для возврата в меню...[/bold]")
            console.clear()

    return copy_jobs_to_process, archive_jobs_to_process


# Замените эту функцию целиком
def process_job_worker(worker_id, job, config, disk_manager):
    """
    Обрабатывает одно задание, используя переданный ID для обновления статуса.
    """
    is_dry_run = config['dry_run']
    short_name = job.get('tar_filename') or os.path.basename(job['key'])
    op_type_text = "[yellow]Архивирую[/yellow]:" if job['type'] == 'sequence' else "[cyan]Копирую[/cyan]:"

    # Используем переданный worker_id
    worker_stats[worker_id] = {
        "status": f"{op_type_text} {short_name}",
        "job_info": job
    }

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
                if not archive_sequence_to_destination(job, dest_path):
                    raise RuntimeError(f"Не удалось создать архив {short_name}")
            else:
                time.sleep(0.05)
            source_keys_to_log = job['source_files']
        else: # 'file'
            source_keys_to_log = [absolute_source_key]
            if not is_dry_run:
                if not os.path.exists(absolute_source_key):
                    raise FileNotFoundError(f"Исходный файл не найден: {absolute_source_key}")
                os.makedirs(os.path.dirname(dest_path), exist_ok=True)
                rsync_cmd = ["rsync", "-a", absolute_source_key, dest_path]
                subprocess.run(rsync_cmd, check=True, stdout=subprocess.DEVNULL, stderr=subprocess.PIPE)
            else:
                time.sleep(0.05)

        worker_stats[worker_id]['status'] = "[green]Свободен[/green]"
        return (job['type'], job['size'], source_keys_to_log, dest_path)

    except Exception as e:
        if not isinstance(e, KeyboardInterrupt):
            worker_stats[worker_id]['status'] = f"[red]Ошибка:[/] {short_name}"
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

    s_plan = plan.get('sequences', {})
    f_plan = plan.get('files', {})
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
            st = os.statvfs(mount)
            total = st.f_blocks * st.f_frsize
            used = total - (st.f_bfree * st.f_frsize)
            size_str = f"{decimal(used)} / {decimal(total)}"
        except FileNotFoundError: size_str = "[red]Н/Д[/red]"
        bar = Progress(BarColumn(bar_width=None, style=color, complete_style=color))
        bar.add_task("d", total=100, completed=percent)
        is_active = " (*)" if mount == disk_manager.active_disk else ""
        table.add_row(f"[bold]{mount}{is_active}[/bold]", size_str, bar, f"{percent:.1f}%")
    return Panel(table, title="📦 Диски", border_style="blue")

# Замените эту функцию целиком
def generate_workers_panel(threads) -> Panel:
    """Генерирует панель потоков с жестким выравниванием."""
    # Используем Table, а не Table.grid
    table = Table(box=None, expand=True, show_header=False)
    table.add_column("Размер", justify="right", style="cyan", width=12, no_wrap=True)
    table.add_column("Статус", justify="left", style="white", no_wrap=True) # justify="left"

    # Создаем полный список слотов, даже если они еще не работают
    all_worker_slots = list(range(1, threads + 1))

    for worker_id in all_worker_slots:
        stats = worker_stats.get(worker_id)

        if stats and stats.get("status") != "[green]Свободен[/green]":
            status_text = stats.get("status")
            job_info = stats.get("job_info")
            size_str = decimal(job_info['size']) if job_info and 'size' in job_info else "[dim]---[/dim]"
            table.add_row(size_str, status_text)
        else:
            # Если слот свободен или еще не запущен
            table.add_row("[dim]---[/dim]", "[green]Свободен[/green]")

    return Panel(table, title=f"👷 Потоки ({threads})", border_style="green")


# --- Точка входа ---

# Замените эту функцию целиком
# Замените эту функцию целиком
def main(args):
    """Главная функция скрипта с двухфазным выполнением и надежным TUI."""
    config = load_config()
    if args.dry_run: config['dry_run'] = True

    console.rule(f"[bold]Copeer v4.3.0[/bold] | Двухфазное выполнение")

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

    copy_jobs, archive_jobs = analyze_and_plan_jobs(args.input_file, config, processed_items_keys)

    if not copy_jobs and not archive_jobs:
        return

    disk_manager = DiskManager(config['mount_points'], config['threshold']) if not is_dry_run else type('FakeDisk', (), {'active_disk': config['mount_points'][0] if config['mount_points'] else "/dry/run/dest", 'get_current_destination': lambda self: self.active_disk, 'get_all_disks_status': lambda self: [(p, 0.0) for p in config['mount_points']]})()
    if not is_dry_run and not disk_manager.active_disk: return

    total_time_start = time.monotonic()
    all_jobs_successful = True
    total_bytes_processed = 0

    # --- ФАЗА 1: КОПИРОВАНИЕ ФАЙЛОВ (МНОГОПОТОЧНОЕ) ---
    if copy_jobs:
        console.rule(f"[yellow]Фаза 1: Копирование {len(copy_jobs)} файлов[/yellow]")
        time.sleep(1)

        layout = make_layout()
        # Статистика будет обновляться для обеих фаз
        completed_stats = {"sequence": {"count": 0, "size": 0}, "files": {"count": 0, "size": 0}}

        # Полный план для отображения
        plan_summary = {
            "sequences": {"count": len(archive_jobs), "size": sum(j.get('size', 0) for j in archive_jobs)},
            "files": {"count": len(copy_jobs), "size": sum(j.get('size', 0) for j in copy_jobs)}
        }

        job_counter_column = TextColumn(f"[cyan]0/{len(copy_jobs)}[/cyan]")
        progress_bar = Progress(TextColumn("[bold blue]Копирование:[/bold blue]"), BarColumn(), TaskProgressColumn(), "•", job_counter_column, "•", TransferSpeedColumn())
        main_task = progress_bar.add_task("copying", total=sum(j.get('size', 0) for j in copy_jobs))

        layout["summary"].update(generate_summary_panel(plan_summary, completed_stats))
        layout["disks"].update(generate_disks_panel(disk_manager, config))
        layout["middle"].update(generate_workers_panel(config['threads']))
        layout["bottom"].update(Panel(progress_bar, title="🚀 Фаза 1: Копирование", border_style="magenta"))

        try:
            with Live(layout, screen=True, redirect_stderr=False, vertical_overflow="visible", refresh_per_second=2) as live:
                with ThreadPoolExecutor(max_workers=config['threads']) as executor:

                    # Система очередей для ID воркеров
                    worker_id_queue = list(range(1, config['threads'] + 1))
                    id_lock = Lock()

                    def get_worker_id():
                        with id_lock: return worker_id_queue.pop(0) if worker_id_queue else None

                    def release_worker_id(worker_id):
                        if worker_id is not None:
                            with id_lock: worker_id_queue.append(worker_id)

                    def job_wrapper(job):
                        worker_id = get_worker_id()
                        try:
                            return process_job_worker(worker_id, job, config, disk_manager)
                        finally:
                            release_worker_id(worker_id)

                    future_to_job = {executor.submit(job_wrapper, job): job for job in copy_jobs}

                    for future in as_completed(future_to_job):
                        job_type, size_processed, source_keys, dest_path = future.result()

                        if job_type:
                            for key in source_keys: write_log(config['state_file'], config['mapping_file'], key, dest_path, is_dry_run)
                            completed_stats['files']['count'] += 1
                            completed_stats['files']['size'] += size_processed
                            total_bytes_processed += size_processed
                        else: all_jobs_successful = False

                        progress_bar.update(main_task, advance=size_processed)
                        job_counter_column.text_format = f"[cyan]{completed_stats['files']['count']}/{len(copy_jobs)}[/cyan]"

                        layout["summary"].update(generate_summary_panel(plan_summary, completed_stats))
                        if not is_dry_run: layout["disks"].update(generate_disks_panel(disk_manager, config))
                        layout["middle"].update(generate_workers_panel(config['threads']))
        except (KeyboardInterrupt, SystemExit):
            console.print("\n[bold red]Процесс прерван.[/bold red]"); sys.exit(1)

    # --- ФАЗА 2: АРХИВАЦИЯ СЕКВЕНЦИЙ (ОДНОПОТОЧНАЯ) ---
    if archive_jobs:
        console.rule(f"[yellow]Фаза 2: Архивация {len(archive_jobs)} секвенций (в 1 поток)[/yellow]")

        with Progress(console=console, transient=True) as progress:
            task = progress.add_task("[green]Архивация...", total=len(archive_jobs))
            for i, job in enumerate(archive_jobs):
                progress.update(task, description=f"[green]Архивация [/][cyan]({i+1}/{len(archive_jobs)})[/]: [yellow]{job['tar_filename']}[/yellow]")

                # Используем worker_id = 1 для однопоточной фазы
                job_type, size_processed, source_keys, dest_path = process_job_worker(1, job, config, disk_manager)

                if job_type:
                    for key in source_keys: write_log(config['state_file'], config['mapping_file'], key, dest_path, is_dry_run)
                    total_bytes_processed += size_processed
                else: all_jobs_successful = False

                progress.update(task, advance=1)

    # --- Итоговая статистика ---
    total_duration = time.monotonic() - total_time_start
    final_avg_speed = total_bytes_processed / total_duration if total_duration > 0 else 0
    console.rule(f"[bold {'green' if all_jobs_successful else 'yellow'}]Выполнение завершено[/bold {'green' if all_jobs_successful else 'yellow'}]")
    console.print(f"  Общее время выполнения: {time.strftime('%H:%M:%S', time.gmtime(total_duration))}")
    console.print(f"  Всего обработано данных: {decimal(total_bytes_processed)}")
    console.print(f"  Средняя скорость: [bold magenta]{decimal(final_avg_speed)}/s[/bold magenta]")
    if not all_jobs_successful: console.print("[yellow]В процессе работы были ошибки. Проверьте лог.[/yellow]")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Анализирует CSV, архивирует и копирует файлы.", prog="copeer")
    parser.add_argument("-v", "--version", action="version", version=f"%(prog)s v{__version__}")
    parser.add_argument("input_file", help="Путь к CSV файлу со списком исходных файлов.")
    parser.add_argument("--dry-run", action="store_true", help="Выполнить анализ без реального копирования.")
    args = parser.parse_args()
    main(args)
