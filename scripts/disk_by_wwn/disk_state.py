import os
import subprocess
import time
import pandas as pd
from rich.console import Console
from rich.table import Table
from rich.prompt import Prompt
from rich.text import Text
import shutil
import logging
import math

# --- Конфигурация ---
CSV_FILE = 'DISK_WWN.csv'
SERIAL_PORT = '/dev/ttyUSB0'
MAX_ACTIVE_DISKS = 10
DISK_APPEAR_TIMEOUT_SEC = 60
DISK_CHECK_INTERVAL_SEC = 1 # Уменьшаем интервал для более плавного счетчика
LOG_FILE = 'disk_manager.log'
MOUNT_BASE_PATH = "/mnt/"
SCRIPT_DIR = os.path.dirname(os.path.realpath(__file__))
# Пороговые значения для определения типа диска (в Терабайтах)
DISK_SIZE_18TB_THRESHOLD = 17.0
DISK_SIZE_20TB_THRESHOLD = 19.0
# Порог заполнения для оранжевого цвета (95%)
DISK_FULL_THRESHOLD = 0.95


class DiskManager:
    def __init__(self, csv_path, serial_port):
        self.console = Console()
        self.serial_port = serial_port
        self.setup_logging()
        
        try:
            self.df = pd.read_csv(csv_path, sep=';')
            self.df['mount_point'] = MOUNT_BASE_PATH + self.df['mount_point'].astype(str)
            self.df['status'] = 'ВЫКЛ'
            self.df['disk_id'] = self.df.apply(
                lambda row: f"{chr(ord('A') + row['module'] - 1)}{row['position']}",
                axis=1
            )
            self.df.set_index('disk_id', inplace=True)
        except FileNotFoundError:
            self.console.print(f"[bold red]Ошибка: Файл {csv_path} не найден.[/bold red]")
            exit()
        self.check_initial_state()

    def _send_serial_command(self, device_command):
        # ... (без изменений)
        self.logger.info(f"Отправка прямой команды в {self.serial_port}: {device_command.strip()}")
        try:
            with open(self.serial_port, 'wb') as f:
                f.write(device_command.encode('utf-8'))
                f.flush()
            return True
        except Exception as e:
            self.console.print(f"[bold red]Критическая ошибка записи в порт {self.serial_port}: {e}[/bold red]")
            return False

    def _run_bash_command(self, command, capture=False):
        # ... (без изменений)
        self.logger.info(f"Выполнение bash-команды: {command}")
        try:
            result = subprocess.run(
                ['/bin/bash', '-c', command], check=True, capture_output=capture, text=True, stderr=subprocess.PIPE
            )
            if result.stdout: self.logger.info(f"STDOUT: {result.stdout.strip()}")
            return result
        except subprocess.CalledProcessError as e:
            self.console.print(f"[bold red]Ошибка выполнения команды:[/bold red] `{command}`")
            self.console.print(f"[red]Stderr: {e.stderr.strip()}[/red]")
            return None
    
    def power_on_and_mount(self, disk_id):
        disk_id = disk_id.upper()
        if disk_id not in self.df.index: return
        disk_info = self.df.loc[disk_id]
        if disk_info['status'] == 'СМОНТИРОВАН': return
        if len(self.df[self.df['status'] == 'СМОНТИРОВАН']) >= MAX_ACTIVE_DISKS: return

        m, n = disk_info['module'], disk_info['position']
        wwn = disk_info['wwn']
        mount_point = disk_info['mount_point']
        
        self.console.print(f"Включение диска {disk_id}...")
        power_on_command = f'#hdd_m{m} n{n} on\r\n'
        if not self._send_serial_command(power_on_command): return
        
        partition_path = f"/dev/disk/by-id/{wwn}-part1"
        self.logger.info(f"Целевой путь к разделу: {partition_path}")
        
        self.console.print(f"Ожидание появления раздела (до {DISK_APPEAR_TIMEOUT_SEC} секунд)...")
        disk_found = False
        with self.console.status("[bold yellow]Поиск устройства...") as status:
            for i in range(1, DISK_APPEAR_TIMEOUT_SEC + 1):
                status.update(f"[bold yellow]Поиск устройства... {i}/{DISK_APPEAR_TIMEOUT_SEC} сек.")
                if os.path.exists(partition_path):
                    disk_found = True
                    self.console.print(f"[green]✔ Раздел обнаружен! (прошло {i} сек.)[/green]")
                    break
                time.sleep(DISK_CHECK_INTERVAL_SEC)
        
        if not disk_found:
            self.console.print(f"[bold red]✖ Ошибка: Раздел диска не определился системой за {DISK_APPEAR_TIMEOUT_SEC} секунд.[/bold red]")
            self.power_off_and_unmount(disk_id, is_retry=True)
            return

        # <<< ИЗМЕНЕНИЕ: Вызываем partprobe для обновления информации о разделах
        self.console.print("[yellow]Обновление информации о разделах в системе...[/yellow]")
        self._run_bash_command("partprobe")
        time.sleep(2) # Небольшая пауза после partprobe
            
        os.makedirs(mount_point, exist_ok=True)
        
        self.console.print(f"Монтирование {partition_path} в {mount_point}...")
        mount_script_path = os.path.join(SCRIPT_DIR, 'scripts', 'mount_disk.sh')
        mount_cmd = f'"{mount_script_path}" "{partition_path}" "{mount_point}"'
        if self._run_bash_command(mount_cmd) is None:
            self.console.print("[bold red]Не удалось выполнить скрипт монтирования.[/bold red]")
            self.power_off_and_unmount(disk_id, is_retry=True)
            return

        if os.path.ismount(mount_point):
            self.df.loc[disk_id, 'status'] = 'СМОНТИРОВАН'
            self.console.print(f"[bold green]✔ Диск {disk_id} успешно включен и смонтирован![/bold green]")
        else:
            self.console.print(f"[bold red]✖ Критическая ошибка: скрипт монтирования отработал, но точка не смонтирована.[/bold red]")

    def power_off_and_unmount(self, disk_id, is_retry=False):
        # ... (без изменений)
        disk_id = disk_id.upper()
        if disk_id not in self.df.index: return
        disk_info = self.df.loc[disk_id]
        if disk_info['status'] == 'ВЫКЛ' and not is_retry: return
        m, n = disk_info['module'], disk_info['position']
        mount_point = disk_info['mount_point']
        if os.path.ismount(mount_point):
            self.console.print(f"Отмонтирование {mount_point}...")
            self._run_bash_command(f"umount {mount_point}")
            time.sleep(1)
        self.console.print(f"Выключение диска {disk_id}...")
        self._send_serial_command(f'#hdd_m{m} n{n} off\r\n')
        self.df.loc[disk_id, 'status'] = 'ВЫКЛ'
        self.console.print(f"[bold green]✔ Диск {disk_id} выключен.[/bold green]")
    
    def _get_disk_usage_info(self, mount_point):
        try:
            total, used, free = shutil.disk_usage(mount_point)
            total_tb = total / (1024**4)
            free_tb = free / (1024**4)
            
            # Определяем иконку по общему размеру
            if total_tb > DISK_SIZE_20TB_THRESHOLD:
                icon = "●" # Круг для 20+ ТБ
            elif total_tb > DISK_SIZE_18TB_THRESHOLD:
                icon = "■" # Квадрат для 18+ ТБ
            else:
                icon = "◆" # Ромб для остальных
                
            # Определяем цвет по заполнению
            if (used / total) > DISK_FULL_THRESHOLD:
                color = "orange3" # Оранжевый
            else:
                color = "green" # Зеленый
                
            return {
                "icon": f"[{color}]{icon}[/{color}]",
                "free_space_str": f"[cyan]{free_tb:.2f}ТБ[/cyan]"
            }
        except FileNotFoundError:
            return {"icon": "[red]✖[/red]", "free_space_str": ""}
            
    def display_grid_chess_style(self):
        self.console.clear()
        table = Table(title="Состояние дисковой полки", title_style="bold magenta", show_header=True)
        max_module = self.df['module'].max()
        max_position = self.df['position'].max()
        
        # Заголовки
        headers = [" "] + [chr(ord('A') + i - 1) for i in range(1, max_module + 1)]
        for header in headers:
            table.add_column(header, justify="center", min_width=7)

        # Подготовка данных
        grid_data = {}
        for disk_id, row in self.df.iterrows():
            pos, mod = row['position'], row['module']
            if pos not in grid_data:
                grid_data[pos] = {}
            if row['status'] == 'СМОНТИРОВАН':
                grid_data[pos][mod] = self._get_disk_usage_info(row['mount_point'])
            else:
                grid_data[pos][mod] = {"icon": "[dim]○[/dim]", "free_space_str": ""}

        # Заполнение таблицы
        for pos_num in range(max_position, 0, -1):
            if pos_num not in grid_data: continue
            
            row_cells = [f"[cyan]{pos_num}[/cyan]"]
            for mod_num in range(1, max_module + 1):
                cell_info = grid_data.get(pos_num, {}).get(mod_num)
                if cell_info:
                    cell_content = f"{cell_info['icon']}\n{cell_info['free_space_str']}"
                else:
                    cell_content = ""
                row_cells.append(cell_content)
            table.add_row(*row_cells)
            
        self.console.print(table)
        self.console.print("Легенда: [green]●[/green]/[green]■[/green] - Вкл (20/18ТБ)  [orange3]●[/orange3] - Заполнен (>95%)  [dim]○[/dim] - Выкл")
        active_count = len(self.df[self.df['status'] == 'СМОНТИРОВАН'])
        self.console.print(f"\nАктивных дисков: [bold green]{active_count}[/bold green] из {MAX_ACTIVE_DISKS} возможных.")

    # ... (остальные функции без изменений)
    def setup_logging(self):
        self.logger = logging.getLogger(__name__)
        self.logger.setLevel(logging.INFO)
        file_handler = logging.FileHandler(LOG_FILE)
        formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
        file_handler.setFormatter(formatter)
        if not self.logger.handlers: self.logger.addHandler(file_handler)
        self.logger.info("--- Утилита запущена ---")
    
    def check_initial_state(self):
        self.logger.info("Проверка начального состояния дисков...")
        for disk_id, row in self.df.iterrows():
            if os.path.ismount(row['mount_point']): self.df.loc[disk_id, 'status'] = 'СМОНТИРОВАН'
        active_disks = len(self.df[self.df['status'] == 'СМОНТИРОВАН'])
        self.logger.info(f"Обнаружено {active_disks} активных дисков.")

    def main_loop(self):
        while True:
            self.display_grid_chess_style()
            choice = Prompt.ask("\n[bold]Действие[/bold] ([cyan]1[/cyan]-Вкл, [cyan]2[/cyan]-Выкл, [cyan]3[/cyan]-Обновить, [cyan]q[/cyan]-Выход)", choices=['1', '2', '3', 'q'], default='3')
            if choice == '1':
                disk_id = Prompt.ask("ID диска для включения")
                self.power_on_and_mount(disk_id)
            elif choice == '2':
                disk_id = Prompt.ask("ID диска для выключения")
                self.power_off_and_unmount(disk_id)
            elif choice == 'q': break
            if choice in ['1', '2']: input("\nНажмите Enter...")
        self.logger.info("--- Утилита завершила работу ---")
        self.console.print("[bold]Завершение работы.[/bold]")


if __name__ == "__main__":
    if os.geteuid() != 0:
        print("Этот скрипт требует прав суперпользователя (root).")
        exit()
    manager = DiskManager(CSV_FILE, SERIAL_PORT)
    manager.main_loop()
