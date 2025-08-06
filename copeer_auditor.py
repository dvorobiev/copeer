# copeer_auditor.py (v3.3 - Dynamic Path Normalization)
"""
Интерактивная утилита-аудитор для анализа, слияния и верификации
результатов работы copeer.py.

v3.3: Заменена жесткая привязка к именам каталогов ('#NEW_FILMS') на
      динамическое определение структуры пути.
"""
import csv
import os
import sys
from pathlib import Path
from collections import defaultdict

# Сторонние библиотеки (убедитесь, что rich установлен: pip install rich)
from rich.console import Console
from rich.table import Table
from rich.progress import Progress
from rich.prompt import Prompt, Confirm

console = Console()

# --- Вспомогательные функции ---

def parse_scientific_notation(size_str: str) -> int:
    """Парсит размер файла из научной нотации или обычного числа."""
    try:
        cleaned_str = str(size_str).replace(',', '.').strip()
        if 'E' in cleaned_str.upper(): return int(float(cleaned_str))
        return int(cleaned_str)
    except (ValueError, TypeError): return 0

def normalize_directory_path(path_str: str) -> str:
    """
    Приводит путь к общему виду для сравнения, убирая префиксы дисков
    и оставляя только значимую часть структуры.
    Работает динамически, находя '/mnt/' префикс.
    """
    p = Path(path_str)
    parts = p.parts

    # Динамический поиск: если путь начинается с /mnt/<something>/...
    # Это наиболее надежный способ отсечь специфичную для машины часть пути.
    if len(parts) > 3 and parts[0] == '/' and parts[1] == 'mnt':
        # Значимая часть начинается после /mnt/<disk_name>
        # Берем 3 уровня вложенности для сравнения
        relevant_parts = parts[3:3+3]
        return str(Path(*relevant_parts))

    # Запасной вариант для путей, не соответствующих шаблону (например, относительных)
    # или если структура иная. Берем последние 3 компонента.
    fallback_parts = parts[-3:]
    return str(Path(*fallback_parts))

# --- Функции команд ---

def handle_stats():
    """Отображает детальную и компактную статистику по mapping-файлу."""
    console.rule("[bold magenta]4. Статистика по mapping-файлу[/bold magenta]")
    map_file_path = Prompt.ask("[bold]Укажите путь к mapping.csv файлу[/bold]")

    if not os.path.exists(map_file_path):
        console.print(f"[bold red]Ошибка: Файл '{map_file_path}' не найден.[/bold red]")
        return

    try:
        with open(map_file_path, 'r', encoding='utf-8', errors='ignore') as f:
            rows = [row for row in csv.reader(f) if len(row) >= 2]
            if not rows or len(rows) < 2:
                console.print("[yellow]Файл маппинга пуст или содержит только заголовок.[/yellow]")
                return
            header = rows.pop(0)
    except Exception as e:
        console.print(f"[bold red]Не удалось прочитать файл: {e}[/bold red]")
        return

    # 1. Сбор данных
    source_dirs_raw = {os.path.dirname(row[0]) for row in rows}
    dest_dirs_raw = {os.path.dirname(row[1]) for row in rows}
    dest_mount_stats = defaultdict(int)
    for _, dest_path in rows:
        parts = Path(dest_path).parts
        if len(parts) > 2 and parts[1] == 'mnt':
            mount_point = f"{parts[0]}{parts[1]}/{parts[2]}/"
            dest_mount_stats[mount_point] += 1

    # 2. Нормализация и сравнение каталогов
    source_dirs_norm = {normalize_directory_path(p) for p in source_dirs_raw}
    dest_dirs_norm = {normalize_directory_path(p) for p in dest_dirs_raw}
    all_unique_dirs = sorted(list(source_dirs_norm.union(dest_dirs_norm)))

    # 3. Вывод
    console.clear()
    console.rule(f"[bold]Статистика для [cyan]{os.path.basename(map_file_path)}[/cyan][/bold]")

    # Таблица сравнения каталогов
    dir_table = Table(title="Сводка по структуре каталогов", padding=(0, 1))
    dir_table.add_column("Общий каталог", style="magenta", no_wrap=True)
    dir_table.add_column("Источник", justify="center")
    dir_table.add_column("Назначение", justify="center")

    for d in all_unique_dirs:
        in_source = "[green]✅[/green]" if d in source_dirs_norm else "[red]❌[/red]"
        in_dest = "[green]✅[/green]" if d in dest_dirs_norm else "[red]❌[/red]"
        dir_table.add_row(d, in_source, in_dest)

    # Таблица статистики по дискам
    disk_table = Table(title="Распределение файлов по дискам", padding=(0, 1))
    disk_table.add_column("Диск", style="green")
    disk_table.add_column("Кол-во файлов", style="green bold", justify="right")

    total_files = 0
    for mp, count in sorted(dest_mount_stats.items(), key=lambda item: item[1], reverse=True):
        disk_table.add_row(mp, f"{count:,}")
        total_files += count
    disk_table.add_section()
    disk_table.add_row("[bold]Всего[/bold]", f"[bold]{total_files:,}[/bold]")

    # Размещаем таблицы рядом для компактности
    layout_table = Table.grid(expand=True)
    layout_table.add_column(ratio=2)
    layout_table.add_column(ratio=1)
    layout_table.add_row(dir_table, disk_table)

    console.print(layout_table)


def handle_merge():
    """Склеивает mapping-файлы с предварительной аналитикой и подтверждением."""
    console.rule("[bold cyan]1. Слияние mapping-файлов[/bold cyan]")
    maps_dir_path = Prompt.ask("[bold]Укажите путь к директории с mapping-файлами[/bold]")
    maps_dir = Path(maps_dir_path)

    if not maps_dir.is_dir():
        console.print(f"[bold red]Ошибка: Директория '{maps_dir_path}' не найдена.[/bold red]")
        return

    pattern = Prompt.ask("[bold]Укажите шаблон для поиска файлов[/bold]", default="mapping*.csv")
    map_files = sorted(list(maps_dir.glob(pattern)))

    if not map_files:
        console.print(f"[bold red]Файлы по шаблону '{pattern}' в директории '{maps_dir_path}' не найдены.[/bold red]")
        return

    console.print(f"\nНайдено {len(map_files)} файлов для анализа слияния...")

    all_unique_mappings, file_stats = set(), []
    for file_path in map_files:
        try:
            with open(file_path, 'r', encoding='utf-8', errors='ignore') as f:
                reader = csv.reader(f)
                try:
                    next(reader) # Пропускаем заголовок
                    rows = [tuple(row) for row in reader if len(row) >= 2]
                    file_stats.append((file_path.name, len(rows)))
                    all_unique_mappings.update(rows)
                except StopIteration:
                    file_stats.append((file_path.name, 0))
        except Exception as e:
            console.print(f"[yellow]Предупреждение: Не удалось прочитать {file_path}: {e}[/yellow]")

    summary_table = Table(title="Аналитика по mapping-файлам")
    summary_table.add_column("Имя файла", style="green", no_wrap=True)
    summary_table.add_column("Количество записей", justify="right")
    for name, count in file_stats:
        summary_table.add_row(name, f"{count:,}")
    summary_table.add_section()
    summary_table.add_row("[bold]Всего уникальных записей[/bold]", f"[bold cyan]{len(all_unique_mappings):,}[/bold cyan]")
    console.print(summary_table)

    output_filename = "mapping_master.csv"
    output_filepath = maps_dir / output_filename

    if not Confirm.ask(f"\n[bold]Сохранить {len(all_unique_mappings):,} записей в файл '{output_filepath}'?[/bold]"):
        console.print("[yellow]Слияние отменено пользователем.[/yellow]")
        return

    sorted_mappings = sorted(list(all_unique_mappings))
    with open(output_filepath, 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow(['source_path', 'destination_path'])
        writer.writerows(sorted_mappings)
    console.print(f"✅ Успешно сохранено в: [bold cyan]{output_filepath}[/bold cyan]")


def handle_analyze():
    """Сравнивает исходный список и state-файл для поиска необработанных файлов."""
    console.rule("[bold yellow]2. Анализ полноты копирования[/bold yellow]")
    source_list_path = Prompt.ask("[bold]Укажите путь к ИСХОДНОМУ CSV со списком ВСЕХ файлов[/bold]")
    if not os.path.exists(source_list_path):
        console.print(f"[bold red]Ошибка: Файл '{source_list_path}' не найден.[/bold red]")
        return

    state_file_path = Prompt.ask("[bold]Укажите путь к файлу состояния (copier_state.csv)[/bold]")
    if not os.path.exists(state_file_path):
        console.print(f"[bold red]Ошибка: Файл '{state_file_path}' не найден.[/bold red]")
        return

    try:
        with open(state_file_path, 'r', encoding='utf-8') as f:
            processed_files = {row[0] for row in csv.reader(f) if row}
        console.print(f"Загружено [bold]{len(processed_files):,}[/bold] записей из файла состояния.")
    except Exception as e:
        console.print(f"[bold red]Не удалось прочитать state-файл: {e}[/bold red]")
        return

    console.print("Анализ исходного списка и автоматическое определение `source_root`...")
    all_source_paths_from_list, source_data_map = [], {}
    try:
        with open(source_list_path, 'r', encoding='utf-8', errors='ignore') as f:
            reader = csv.reader(f, delimiter=';')
            for row in reader:
                if not row or len(row) < 5 or 'directory' in row[1]: continue
                path_str = row[0]
                all_source_paths_from_list.append(path_str)
                source_data_map[path_str] = [path_str, 'file', parse_scientific_notation(row[4]), '', row[4]]
    except Exception as e:
        console.print(f"[bold red]Не удалось прочитать исходный список: {e}[/bold red]")
        return

    source_root = os.path.commonpath(all_source_paths_from_list) if all_source_paths_from_list else None
    if source_root:
        console.print(f"Автоматически определен `source_root`: [cyan]{source_root}[/cyan]")
    else:
        console.print("[yellow]Не удалось определить общий `source_root`. Пути будут считаться абсолютными.[/yellow]")
        source_root = ""

    intended_files = {os.path.normpath(os.path.join(source_root, p) if not os.path.isabs(p) else p) for p in all_source_paths_from_list}

    missing_files = sorted(list(intended_files - processed_files))

    table = Table(title="Отчет по анализу")
    table.add_column("Параметр", style="cyan")
    table.add_column("Количество", justify="right", style="white")
    table.add_row("Всего файлов в исходном списке", f"{len(intended_files):,}")
    table.add_row("[green]Успешно обработано (есть в state-файле)[/green]", f"{len(intended_files) - len(missing_files):,}")
    table.add_row("[red]Не обработано (отсутствуют в state-файле)[/red]", f"[red]{len(missing_files):,}")
    console.print(table)

    if missing_files:
        output_file = "missing_for_copy.csv"
        console.print(f"\nСохранение списка из {len(missing_files):,} необработанных файлов в [bold cyan]{output_file}[/bold cyan]...")
        with open(output_file, 'w', newline='', encoding='utf-8') as f:
            writer = csv.writer(f, delimiter=';')
            for abs_path in missing_files:
                rel_path = os.path.relpath(abs_path, source_root)
                if rel_path in source_data_map:
                    writer.writerow(source_data_map[rel_path])
        console.print(f"✅ Готово. Используйте [bold]'{output_file}'[/bold] как --input-file для copeer.py.")
    else:
        console.print("\n[bold green]✅ Отлично! Все файлы из исходного списка были обработаны.[/bold green]")


def handle_verify():
    """Проверяет физическое наличие КОНЕЧНЫХ файлов на дисках по mapping-файлу."""
    console.rule("[bold blue]3. Верификация файлов на дисках[/bold blue]")
    map_file_path = Prompt.ask("[bold]Укажите путь к mapping-файлу (например, mapping_master.csv)[/bold]")
    if not os.path.exists(map_file_path):
        console.print(f"[bold red]Ошибка: Файл '{map_file_path}' не найден.[/bold red]")
        return

    try:
        with open(map_file_path, 'r', encoding='utf-8', errors='ignore') as f:
            reader = csv.reader(f)
            next(reader)
            unique_dest_paths = {row[1] for row in reader if len(row) >= 2}
    except Exception as e:
        console.print(f"[bold red]Не удалось прочитать файл: {e}[/bold red]")
        return

    found_count, missing_count, missing_paths = 0, 0, []
    with Progress(console=console) as progress:
        task = progress.add_task("[green]Проверка файлов...", total=len(unique_dest_paths))
        for dest_path in unique_dest_paths:
            if os.path.exists(dest_path):
                found_count += 1
            else:
                missing_count += 1
                missing_paths.append(dest_path)
            progress.update(task, advance=1)

    table = Table(title=f"Отчет по верификации")
    table.add_column("Статус", style="cyan")
    table.add_column("Количество", justify="right", style="white")
    table.add_row("Всего уникальных конечных файлов", f"{len(unique_dest_paths):,}")
    table.add_row("[green]Найдено на диске[/green]", f"{found_count:,}")
    table.add_row("[red]Отсутствует на диске[/red]", f"{missing_count:,}")
    console.print(table)

    if missing_paths:
        if Confirm.ask("\n[bold]Сохранить список отсутствующих файлов?[/bold]"):
            output_file = "physically_missing.csv"
            with open(output_file, 'w', newline='', encoding='utf-8') as f:
                writer = csv.writer(f)
                writer.writerow(['missing_destination_path'])
                for path in sorted(missing_paths):
                    writer.writerow([path])
            console.print(f"✅ Список сохранен в [bold cyan]{output_file}[/bold cyan].")

def show_menu():
    """Отображает главное меню."""
    console.rule("[bold]Меню Copeer Auditor[/bold]")
    table = Table(box=None, show_header=False)
    table.add_column(style="cyan")
    table.add_column()
    table.add_row("[1]", "Склеить `mapping` файлы")
    table.add_row("[2]", "Найти недокопированные файлы (по state-файлу)")
    table.add_row("[3]", "Проверить наличие файлов на дисках (Верификация)")
    table.add_row("[4]", "Показать статистику по `mapping` файлу")
    table.add_row("[q]", "Выход")
    console.print(table)

def main():
    """Главная функция с интерактивным меню."""
    while True:
        show_menu()
        choice = Prompt.ask("\n[bold]Выберите действие[/bold]", choices=['1', '2', '3', '4', 'q'], default='q')

        if choice == '1':
            handle_merge()
        elif choice == '2':
            handle_analyze()
        elif choice == '3':
            handle_verify()
        elif choice == '4':
            handle_stats()
        elif choice == 'q':
            console.print("[bold green]Выход.[/bold green]")
            break

        Prompt.ask("\n[dim]Нажмите Enter для возврата в меню...[/dim]", default="")
        console.clear()

if __name__ == "__main__":
    try:
        main()
    except (KeyboardInterrupt, EOFError):
        console.print("\n[bold yellow]Выход по запросу пользователя.[/bold yellow]")
