# copier_final_perfected.py
import os
import sys
import csv
import subprocess
import logging
import time
import yaml
import json
from pathlib import Path
from rich.console import Console
from rich.progress import Progress, BarColumn, TextColumn, TaskProgressColumn, TransferSpeedColumn, TimeRemainingColumn
from rich.panel import Panel
from rich.layout import Layout
from rich.live import Live
from rich.table import Table
from rich.logging import RichHandler
from rich.prompt import Confirm
from rich.filesize import decimal
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock, get_ident
from collections import defaultdict

console = Console()

# --- Конфигурация и глобальные переменные ---
CONFIG_FILE = "config.yaml"
SESSION_FILE = "session.json"
DEFAULT_CONFIG = {
    'mount_points': ["/mnt/disk1", "/mnt/disk2"],
    'threshold': 98.0,
    'state_file': "copier_state.csv",
    'mapping_file': "mapping.csv",
    'error_log_file': "errors.log",
    'dry_run': False,
    'threads': 8
}

logging.basicConfig(level="INFO", format="%(message)s", datefmt="[%X]", handlers=[RichHandler(rich_tracebacks=True, show_path=False)])
log = logging.getLogger("rich")

# Этот set будет заполняться функцией load_previous_state
copied_files = set()
file_lock = Lock()
worker_stats = defaultdict(lambda: {"status": "[grey50]Ожидание...[/grey50]", "speed": 0})

# --- Классы и функции ---
class DiskManager:
    def __init__(self, mount_points, threshold):
        self.mount_points=mount_points;self.threshold=threshold;self.active_disk=None;self.lock=Lock();self._select_initial_disk()
    def _get_disk_usage(self,path):
        try:st=os.statvfs(path);used=(st.f_blocks-st.f_bfree)*st.f_frsize;total=st.f_blocks*st.f_frsize;return round(used/total*100,2)if total>0 else 0,used,total
        except FileNotFoundError:return 100,0,0
    def _select_initial_disk(self):
        for mount in self.mount_points:
            percent,_,_=self._get_disk_usage(mount)
            if percent<self.threshold:self.active_disk=mount;return
        self.active_disk=None
    def get_current_destination(self):
        with self.lock:
            if not self.active_disk:raise RuntimeError("🛑 Нет доступных дисков: все переполнены или недоступны.")
            percent,_,_=self._get_disk_usage(self.active_disk)
            if percent>=self.threshold:
                log.warning(f"Диск [bold]{self.active_disk}[/bold] заполнен. Ищу следующий...")
                try:current_index=self.mount_points.index(self.active_disk);next_disks=self.mount_points[current_index+1:]
                except ValueError:next_disks=self.mount_points
                self.active_disk=None
                for mount in next_disks:
                    p,_,_=self._get_disk_usage(mount)
                    if p<self.threshold:self.active_disk=mount;log.info(f"Переключился на диск: [bold green]{self.active_disk}[/bold green]");break
            if not self.active_disk:raise RuntimeError("🛑 Нет доступных дисков: все переполнены или недоступны.")
            return self.active_disk
    def get_all_disks_status(self):
        statuses=[];
        for m in self.mount_points:percent,_,_=self._get_disk_usage(m);statuses.append((m,percent))
        return statuses

def load_config():
    config=DEFAULT_CONFIG.copy()
    if os.path.exists(CONFIG_FILE):
        try:
            with open(CONFIG_FILE) as f:user_config=yaml.safe_load(f) or {};config.update(user_config)
        except Exception as e:console.print(f"[bold red]Ошибка чтения {CONFIG_FILE}: {e}. Будут использованы значения по умолчанию.[/bold red]")
    else:
        with open(CONFIG_FILE,'w') as f:yaml.dump(config,f,sort_keys=False,allow_unicode=True)
    return config

def load_previous_state(state_file):
    if os.path.exists(state_file):
        try:
            with open(state_file, newline='', encoding='utf-8') as f:
                reader = csv.reader(f)
                # Убедимся, что строка не пустая, прежде чем добавлять
                copied_files.update(str(Path(row[0])) for row in reader if row)
        except Exception as e:
            log.error(f"Не удалось прочитать файл состояния {state_file}: {e}")


def write_mapping_and_state(config,src,dest):
    with file_lock:
        with open(config['mapping_file'],"a",newline='',encoding='utf-8') as f_map:csv.writer(f_map).writerow([src,dest])
        with open(config['state_file'],"a",newline='',encoding='utf-8') as f_state:csv.writer(f_state).writerow([src])

def save_session(files,source_root):
    session_data={'files_to_copy':files,'source_root':str(source_root)};
    with open(SESSION_FILE,'w',encoding='utf-8') as f:json.dump(session_data,f,indent=2)

def load_session():
    if os.path.exists(SESSION_FILE):
        with open(SESSION_FILE,'r',encoding='utf-8') as f:return json.load(f)
    return None

def clear_session():
    if os.path.exists(SESSION_FILE):os.remove(SESSION_FILE)

def get_files_to_copy():
    session = load_session()
    if session:
        console.print("[bold yellow]Обнаружена прерванная сессия.[/bold yellow]")
        if Confirm.ask("Продолжить копирование с предыдущими настройками?", default=True):
            console.print("[green]Продолжаем прерванную сессию...[/green]")
            return session['files_to_copy'], session['source_root']
        else:
            # ИЗМЕНЕНИЕ: Спрашиваем, нужно ли все почистить
            if Confirm.ask("[bold red]Очистить все предыдущие результаты (state, mapping, logs) и начать новую сессию с нуля?[/bold red]", default=False):
                config = load_config()
                for f_key in ['state_file', 'mapping_file', 'error_log_file']:
                    f_path = config.get(f_key)
                    if f_path and os.path.exists(f_path):
                        os.remove(f_path)
                        log.info(f"Файл [yellow]{f_path}[/yellow] удален.")
                console.print("[green]Все старые данные очищены.[/green]")
            clear_session()

    console.print("\n[bold]Выберите источник файлов:[/bold]");console.print(" [cyan]1[/cyan]) Из файла со списком путей");console.print(" [cyan]2[/cyan]) Из каталога (рекурсивно)")
    choice=console.input("[yellow]Ваш выбор (1-2):[/yellow] ");files_to_copy,source_root=[],None
    if choice=='1':
        file_list_path=console.input("Введите путь к файлу со списком: ").strip()
        with open(file_list_path,encoding='utf-8') as f:files_to_copy=[line.strip()for line in f if line.strip()]
        source_root=os.path.commonpath(files_to_copy)if files_to_copy else None
    elif choice=='2':
        source_root_str = console.input("Введите путь к каталогу для копирования: ").strip()
        source_root = Path(source_root_str).expanduser().resolve() # Обрабатываем ~ и получаем абсолютный путь
        files_to_copy=[str(p)for p in source_root.rglob("*")if p.is_file()and not p.is_symlink()]
    else:console.print("[red]Неверный выбор.[/red]");sys.exit(1)

    if not source_root:console.print("[red]Не удалось определить корневой каталог источника. Прерывание.[/red]");sys.exit(1)
    
    save_session(files_to_copy,source_root)
    return files_to_copy,str(source_root)

def copy_file(src, source_root, config, disk_manager):
    thread_id = get_ident()
    short_src = Path(src).name
    worker_stats[thread_id]['status'] = f"[yellow]Копирую:[/] {short_src}"

    try:
        start_time = time.monotonic()
        file_size = os.path.getsize(src)
        dest_root = disk_manager.get_current_destination()
        rel_path = os.path.relpath(src, start=source_root)
        dest_path = os.path.join(dest_root, rel_path)
        os.makedirs(os.path.dirname(dest_path), exist_ok=True)

        if config['dry_run']:
            time.sleep(0.01) # Имитация работы
        else:
            # Используем -a (архивный режим) и -c (проверка по чек-сумме)
            subprocess.run(["rsync", "-ac", src, dest_path], check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            write_mapping_and_state(config, src, dest_path)

        elapsed_time = time.monotonic() - start_time
        speed = file_size / elapsed_time if elapsed_time > 0 else 0

        worker_stats[thread_id]['status'] = "[green]Свободен[/green]"
        worker_stats[thread_id]['speed'] = speed
        return src, file_size

    except Exception as e:
        log.error(f"Ошибка копирования {src}: {e}")
        worker_stats[thread_id]['status'] = f"[red]Ошибка:[/] {short_src}"
        worker_stats[thread_id]['speed'] = -1
        with open(config['error_log_file'], "a", encoding='utf-8') as f_err:
            f_err.write(f"{time.asctime()};{src};{e}\n")
        return None, 0

def make_layout() -> Layout:
    layout=Layout(name="root");layout.split_column(Layout(name="top"),Layout(name="middle"),Layout(name="bottom"));return layout

def generate_disks_panel(disk_manager: DiskManager, config) -> Panel:
    table=Table(box=None,expand=True);table.add_column("Диск",style="white",no_wrap=True);table.add_column("Заполнено",style="green",ratio=1);table.add_column("%",style="bold",justify="right")
    for mount,percent in disk_manager.get_all_disks_status():
        color="green" if percent<config['threshold'] else "red";bar=Progress(BarColumn(bar_width=None,style=color,complete_style=color),expand=True)
        task_id=bar.add_task("disk_usage",total=100);bar.update(task_id,completed=percent)
        is_active=" (*)" if mount==disk_manager.active_disk else "";table.add_row(f"[bold]{mount}{is_active}[/bold]",bar,f"{percent:.1f}%")
    return Panel(table,title="📦 Диски",border_style="blue")

def generate_workers_panel() -> Panel:
    table=Table(expand=True);table.add_column("Поток",justify="center",style="cyan",width=12);table.add_column("Статус",style="white",no_wrap=True,ratio=2);table.add_column("Скорость",justify="right",style="magenta",width=15)
    for tid in sorted(worker_stats.keys()):
        stats=worker_stats[tid]
        speed_str=""
        if stats['speed']>0:speed_str=f"{decimal(stats['speed'])}/s"
        elif stats['speed']==-1:speed_str="[red]ERROR[/red]"
        else:speed_str="[dim]---[/dim]"
        table.add_row(str(tid),stats['status'],speed_str)
    return Panel(table,title="👷 Потоки",border_style="green")

def main():
    config = load_config()
    console.rule(f"[bold]Data Copier[/bold] | Режим: {'Dry Run' if config['dry_run'] else 'Реальное копирование'}")

    # 1. СНАЧАЛА получаем задачу на эту сессию (новую или продолжение старой)
    files_to_copy, source_root = get_files_to_copy()

    # 2. ТЕПЕРЬ загружаем состояние, чтобы понять, что из ЭТОЙ задачи уже сделано
    load_previous_state(config['state_file'])

    # 3. Фильтруем список файлов, нормализуя пути для корректного сравнения
    files_to_process = [f for f in files_to_copy if str(Path(f).resolve()) not in copied_files]

    if not files_to_process:
        console.print("[bold green]✅ Все файлы из выбранной сессии уже скопированы. Завершение.[/bold green]")
        clear_session()
        return

    # Подсчитываем количество уже скопированных файлов из текущего задания
    copied_in_this_session = len(files_to_copy) - len(files_to_process)
    total_files_to_process = len(files_to_process)
    console.print(f"Всего файлов в задании: [bold]{len(files_to_copy)}[/bold]. Уже скопировано: [bold]{copied_in_this_session}[/bold]. Осталось: [bold green]{total_files_to_process}[/bold green]")

    disk_manager = DiskManager(config['mount_points'], config['threshold'])

    file_counter_column = TextColumn(f"[cyan]0/{total_files_to_process} файлов[/cyan]")

    try:
        # Считаем размер только тех файлов, что реально нужно обработать
        total_size = sum(os.path.getsize(f) for f in files_to_process)
    except FileNotFoundError:
        log.error("Один из файлов для копирования не найден. Проверьте пути в источнике.")
        sys.exit(1)

    progress = Progress(
        TextColumn("[bold blue]Общий прогресс:[/bold blue]"),
        BarColumn(),
        TaskProgressColumn(),
        TextColumn("•"),
        file_counter_column,
        TextColumn("•"),
        TransferSpeedColumn(),
        TextColumn("•"),
        TimeRemainingColumn(),
    )

    main_task = progress.add_task("копирование", total=total_size)

    layout = make_layout()
    layout["top"].update(generate_disks_panel(disk_manager, config))
    layout["middle"].update(generate_workers_panel())
    layout["bottom"].update(Panel(progress, title="🚀 Процесс копирования", border_style="magenta"))

    copy_successful = True
    files_completed_count = 0
    try:
        with Live(layout, screen=True, redirect_stderr=False, vertical_overflow="visible") as live:
            with ThreadPoolExecutor(max_workers=config['threads']) as executor:
                # Передаем source_root в submit
                future_to_file = {executor.submit(copy_file, src, source_root, config, disk_manager): src for src in files_to_process}

                for future in as_completed(future_to_file):
                    src, size_copied = future.result()
                    files_completed_count += 1

                    if src is None:
                        copy_successful = False

                    progress.update(main_task, advance=size_copied)
                    file_counter_column.text_format = f"[cyan]{files_completed_count}/{total_files_to_process} файлов[/cyan]"

                    layout["top"].update(generate_disks_panel(disk_manager, config))
                    layout["middle"].update(generate_workers_panel())
    except (Exception, KeyboardInterrupt):
        console.print("\n[bold red]Копирование прервано пользователем или из-за критической ошибки.[/bold red]")
        console.print("[yellow]Сессия сохранена. При следующем запуске вы сможете продолжить.[/yellow]")
        sys.exit(1)

    if copy_successful and progress.finished:
        console.rule("[bold green]✅ Копирование успешно завершено[/bold green]")
        clear_session()
    else:
        console.rule("[bold yellow]Копирование завершено, но были ошибки. Проверьте лог.[/bold yellow]")
        console.print("[yellow]Сессия сохранена. Вы можете перезапустить скрипт, чтобы попробовать скопировать проблемные файлы снова.[/yellow]")

if __name__ == "__main__":
    main()
