# copeer_auditor.py (v4.1 - Adjusted Directory Depth)
"""
Интерактивная утилита-аудитор для анализа, слияния и верификации
результатов работы copeer.py.

v4.1: Увеличена глубина отображения каталогов в статистике для
      большей детализации.
"""
import csv
import os
import sys
from pathlib import Path
from collections import defaultdict

# Сторонние библиотеки
from rich.console import Console
from rich.table import Table
from rich.progress import Progress
from rich.panel import Panel

# НОВАЯ БИБЛИОТЕКА ДЛЯ ИНТЕРАКТИВНОСТИ
import questionary
from prompt_toolkit.completion import PathCompleter

console = Console()
# Создаем один экземпляр автодополнителя для многократного использования
path_completer = PathCompleter(expanduser=True, only_directories=False)
dir_completer = PathCompleter(expanduser=True, only_directories=True)


# --- Вспомогательные функции ---

def parse_scientific_notation(size_str: str) -> int:
    try:
        cleaned_str = str(size_str).replace(',', '.').strip()
        if 'E' in cleaned_str.upper(): return int(float(cleaned_str))
        return int(cleaned_str)
    except (ValueError, TypeError): return 0

def normalize_directory_path(path_str: str) -> str:
    """
    Приводит путь к общему виду для сравнения, убирая префиксы дисков
    и оставляя только значимую часть структуры.
    """
    p = Path(path_str)
    parts = p.parts
    if len(parts) > 3 and parts[0] == '/' and parts[1] == 'mnt':
        # ИЗМЕНЕНИЕ: Увеличиваем глубину с 3 до 4 уровней
        relevant_parts = parts[3:3+4]
        return str(Path(*relevant_parts))
    # Запасной вариант для путей, не соответствующих шаблону
    fallback_parts = parts[-4:]
    return str(Path(*fallback_parts))

def find_source_root(state_file_paths, source_list_paths):
    if not state_file_paths or not source_list_paths:
        return None
    source_map = {os.path.basename(p): p for p in source_list_paths}
    for abs_path_str in state_file_paths:
        basename = os.path.basename(abs_path_str)
        if basename in source_map:
            rel_path_str = source_map[basename]
            rel_path_clean = rel_path_str.lstrip('./')
            if abs_path_str.endswith(rel_path_clean):
                end_index = abs_path_str.rfind(rel_path_clean)
                source_root = abs_path_str[:end_index]
                return source_root.rstrip('/')
    return None


# --- Функции команд ---

def handle_stats():
    console.rule("[bold magenta]4. Статистика по mapping-файлу[/bold magenta]")
    map_file_path = questionary.path(
        "Укажите путь к mapping.csv файлу:",
        completer=path_completer,
        validate=lambda p: os.path.exists(p) or "Файл не найден"
    ).ask()
    if not map_file_path: return

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

    source_dirs_raw = {os.path.dirname(row[0]) for row in rows}
    unique_dest_paths = {row[1] for row in rows}
    dest_dirs_raw = {os.path.dirname(p) for p in unique_dest_paths}

    physical_file_stats = defaultdict(int)
    for dest_path in unique_dest_paths:
        parts = Path(dest_path).parts
        if len(parts) > 2 and parts[1] == 'mnt':
            mount_point = f"/{parts[1]}/{parts[2]}/"
            physical_file_stats[mount_point] += 1

    source_dirs_norm = {normalize_directory_path(p) for p in source_dirs_raw}
    dest_dirs_norm = {normalize_directory_path(p) for p in dest_dirs_raw}
    all_unique_dirs = sorted(list(source_dirs_norm.union(dest_dirs_norm)))

    console.clear()
    console.rule(f"[bold]Статистика для [cyan]{os.path.basename(map_file_path)}[/cyan][/bold]")

    summary_text = (
        f"Обработано записей (исходных файлов): [cyan]{len(rows):,}[/cyan]\n"
        f"Создано физических файлов/архивов: [green bold]{len(unique_dest_paths):,}[/green bold]"
    )
    console.print(Panel(summary_text, title="Общая сводка", border_style="dim"))

    dir_table = Table(title="Сводка по структуре каталогов", padding=(0, 1))
    dir_table.add_column("Общий каталог", style="magenta", no_wrap=True)
    dir_table.add_column("Источник", justify="center")
    dir_table.add_column("Назначение", justify="center")

    for d in all_unique_dirs:
        in_source = "[green]✅[/green]" if d in source_dirs_norm else "[red]❌[/red]"
        in_dest = "[green]✅[/green]" if d in dest_dirs_norm else "[red]❌[/red]"
        dir_table.add_row(d, in_source, in_dest)

    disk_table = Table(title="Распределение физических файлов по дискам", padding=(0, 1))
    disk_table.add_column("Диск", style="green")
    disk_table.add_column("Кол-во файлов", style="green bold", justify="right")

    total_phys_files = 0
    for mp, count in sorted(physical_file_stats.items(), key=lambda item: item[1], reverse=True):
        disk_table.add_row(mp, f"{count:,}")
        total_phys_files += count
    disk_table.add_section()
    disk_table.add_row("[bold]Всего[/bold]", f"[bold]{total_phys_files:,}[/bold]")

    layout_table = Table.grid(expand=True, padding=(1,3))
    layout_table.add_column(ratio=2)
    layout_table.add_column(ratio=1)
    layout_table.add_row(dir_table, disk_table)

    console.print(layout_table)


def handle_merge():
    console.rule("[bold cyan]1. Слияние mapping-файлов[/bold cyan]")
    maps_dir_path = questionary.path(
        "Укажите путь к директории с mapping-файлами:",
        completer=dir_completer,
        validate=lambda p: os.path.isdir(p) or "Директория не найдена"
    ).ask()
    if not maps_dir_path: return

    maps_dir = Path(maps_dir_path)

    pattern = questionary.text("Укажите шаблон для поиска файлов:", default="mapping*.csv").ask()
    if not pattern: return

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
                    next(reader)
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

    do_save = questionary.confirm(f"Сохранить {len(all_unique_mappings):,} записей в файл '{output_filepath}'?").ask()
    if not do_save:
        console.print("[yellow]Слияние отменено пользователем.[/yellow]")
        return

    sorted_mappings = sorted(list(all_unique_mappings))
    with open(output_filepath, 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow(['source_path', 'destination_path'])
        writer.writerows(sorted_mappings)
    console.print(f"✅ Успешно сохранено в: [bold cyan]{output_filepath}[/bold cyan]")


def handle_analyze():
    console.rule("[bold yellow]2. Анализ полноты копирования[/bold yellow]")
    source_list_path = questionary.path(
        "Укажите путь к ИСХОДНОМУ CSV со списком ВСЕХ файлов:",
        completer=path_completer,
        validate=lambda p: os.path.exists(p) or "Файл не найден"
    ).ask()
    if not source_list_path: return

    state_file_path = questionary.path(
        "Укажите путь к файлу состояния (copier_state.csv):",
        completer=path_completer,
        validate=lambda p: os.path.exists(p) or "Файл не найден"
    ).ask()
    if not state_file_path: return

    try:
        with open(state_file_path, 'r', encoding='utf-8') as f:
            processed_files_abs = {row[0] for row in csv.reader(f) if row}
        console.print(f"Загружено [bold]{len(processed_files_abs):,}[/bold] записей из файла состояния.")
    except Exception as e:
        console.print(f"[bold red]Не удалось прочитать state-файл: {e}[/bold red]")
        return

    console.print("Анализ исходного списка...")
    source_list_paths_rel, source_data_map = [], {}
    try:
        with open(source_list_path, 'r', encoding='utf-8', errors='ignore') as f:
            reader = csv.reader(f, delimiter=';')
            for row in reader:
                if not row or len(row) < 5 or 'directory' in row[1]: continue
                rel_path_str = row[0]
                source_list_paths_rel.append(rel_path_str)
                source_data_map[rel_path_str] = [rel_path_str, 'file', parse_scientific_notation(row[4]), '', row[4]]
    except Exception as e:
        console.print(f"[bold red]Не удалось прочитать исходный список: {e}[/bold red]")
        return

    console.print("Интеллектуальное определение `source_root`...")
    source_root = find_source_root(processed_files_abs, source_list_paths_rel)

    if source_root:
        console.print(f"✅ Автоматически определен `source_root`: [cyan]{source_root}[/cyan]")
    else:
        console.print("[bold yellow]Не удалось определить `source_root` автоматически.[/bold yellow]")
        source_root = ""

    intended_files_abs = {os.path.normpath(os.path.join(source_root, p.lstrip('./'))) for p in source_list_paths_rel}

    missing_files_abs = sorted(list(intended_files_abs - processed_files_abs))

    table = Table(title="Отчет по анализу")
    table.add_column("Параметр", style="cyan")
    table.add_column("Количество", justify="right", style="white")
    table.add_row("Всего файлов в исходном списке", f"{len(intended_files_abs):,}")
    table.add_row("[green]Успешно обработано (есть в state-файле)[/green]", f"{len(intended_files_abs) - len(missing_files_abs):,}")
    table.add_row("[red]Не обработано (отсутствуют в state-файле)[/red]", f"[red]{len(missing_files_abs):,}")
    console.print(table)

    if missing_files_abs:
        output_file = "missing_for_copy.csv"
        console.print(f"\nСохранение списка из {len(missing_files_abs):,} необработанных файлов в [bold cyan]{output_file}[/bold cyan]...")
        with open(output_file, 'w', newline='', encoding='utf-8') as f:
            writer = csv.writer(f, delimiter=';')
            for abs_path in missing_files_abs:
                rel_path = os.path.relpath(abs_path, source_root) if source_root else abs_path
                rel_path_unix = rel_path.replace(os.path.sep, '/')

                original_rel_path = None
                if rel_path_unix in source_data_map:
                    original_rel_path = rel_path_unix
                elif f"./{rel_path_unix}" in source_data_map:
                    original_rel_path = f"./{rel_path_unix}"

                if original_rel_path:
                    writer.writerow(source_data_map[original_rel_path])
        console.print(f"✅ Готово. Используйте [bold]'{output_file}'[/bold] как --input-file для copeer.py.")
    else:
        console.print("\n[bold green]✅ Отлично! Все файлы из исходного списка были обработаны.[/bold green]")


def handle_verify():
    console.rule("[bold blue]3. Верификация файлов на дисках[/bold blue]")
    map_file_path = questionary.path(
        "Укажите путь к mapping-файлу:",
        default="./mapping_master.csv",
        completer=path_completer,
        validate=lambda p: os.path.exists(p) or "Файл не найден"
    ).ask()
    if not map_file_path: return

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
        do_save = questionary.confirm("Сохранить список отсутствующих файлов?").ask()
        if do_save:
            output_file = "physically_missing.csv"
            with open(output_file, 'w', newline='', encoding='utf-8') as f:
                writer = csv.writer(f)
                writer.writerow(['missing_destination_path'])
                for path in sorted(missing_paths):
                    writer.writerow([path])
            console.print(f"✅ Список сохранен в [bold cyan]{output_file}[/bold cyan].")

def main():
    while True:
        console.rule("[bold]Меню Copeer Auditor[/bold]")
        choice = questionary.select(
            "Выберите действие:",
            choices=[
                "1. Склеить `mapping` файлы",
                "2. Найти недокопированные файлы (по state-файлу)",
                "3. Проверить наличие файлов на дисках (Верификация)",
                "4. Показать статистику по `mapping` файлу",
                questionary.Separator(),
                "Выход"
            ],
            use_indicator=True
        ).ask()

        if choice is None or choice == "Выход":
            console.print("[bold green]Выход.[/bold green]")
            break

        console.clear()

        if "1." in choice:
            handle_merge()
        elif "2." in choice:
            handle_analyze()
        elif "3." in choice:
            handle_verify()
        elif "4." in choice:
            handle_stats()

        questionary.press_any_key_to_continue("Нажмите любую клавишу для возврата в меню...").ask()
        console.clear()

if __name__ == "__main__":
    try:
        main()
    except (KeyboardInterrupt, EOFError):
        console.print("\n[bold yellow]Выход по запросу пользователя.[/bold yellow]")
