# copeer_auditor.py (v5.0 - Dynamic path normalization and map filtering)
"""
Интерактивная утилита-аудитор для анализа, слияния и верификации
результатов работы copeer.py.

v5.0:
- Добавлена функция для фильтрации mapping-файла по файлу-заданию.
- Улучшена функция сравнения плана и маппинга: теперь она автоматически
  определяет относительный путь по маркеру 'raidix', а не по жестко
  заданной строке.
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
from rich.text import Text

# Библиотека для интерактивности
import questionary
from prompt_toolkit.completion import PathCompleter

console = Console()
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
    p = Path(path_str)
    parts = p.parts
    if len(parts) > 3 and parts[0] == '/' and parts[1] == 'mnt':
        relevant_parts = parts[3:3+4]
        return str(Path(*relevant_parts))
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

def normalize_source_path_for_comparison(source_path_str: str) -> str:
    """
    Нормализует абсолютный путь из mapping-файла для сравнения с относительным путем из файла-задания.
    Ищет 'raidix' и берет все, что идет после следующего компонента.
    Пример: /mnt/cifs/raidix/#OLD_FILMS/path/to/file -> path/to/file
    """
    try:
        p = Path(source_path_str)
        parts = p.parts
        # Ищем 'raidix' в компонентах пути
        if 'raidix' in parts:
            raidix_index = parts.index('raidix')
            # Убеждаемся, что после raidix есть хотя бы 1 компонент (корень проекта)
            if len(parts) > raidix_index + 1:
                # Берем все части ПОСЛЕ корневой папки проекта (которая идет сразу за raidix)
                relative_parts = parts[raidix_index + 2:]
                return str(Path(*relative_parts))
    except (ValueError, IndexError):
        # В случае ошибки, возвращаем исходный путь, сравнение скорее всего не сработает
        pass
    # Если 'raidix' не найден или структура не та, возвращаем исходную строку
    return source_path_str


# --- Основные функции команд ---

def _run_verification(stats_data):
    """Вспомогательная функция для запуска и отображения верификации."""
    console.rule("[bold blue]Верификация файлов[/bold blue]")

    all_dest_paths = []
    for data in stats_data.values():
        for disk_paths in data.get("destinations", {}).values():
            all_dest_paths.extend(disk_paths)

    if not all_dest_paths:
        console.print("[yellow]Нет файлов для верификации.[/yellow]")
        return

    missing_paths = set()
    with Progress(console=console) as progress:
        task = progress.add_task("[green]Проверка файлов...", total=len(all_dest_paths))
        for path in all_dest_paths:
            if not os.path.exists(path):
                missing_paths.add(path)
            progress.update(task, advance=1)

    console.rule("[bold]Отчет по верификации[/bold]")
    verification_table = Table(title="Детализация верификации по каталогам", padding=(0, 1))
    verification_table.add_column("Общий каталог", style="magenta", no_wrap=True)
    verification_table.add_column("Источник", justify="center")
    verification_table.add_column("Назначение", justify="left")

    for norm_dir, data in stats_data.items():
        in_source = "[green]✅[/green]" if data["in_source"] else "[red]❌[/red]"

        dest_text = Text()
        if not data["destinations"]:
            dest_text.append("❌", style="red")
        else:
            sorted_disks = sorted(data["destinations"].items())
            for i, (disk, paths) in enumerate(sorted_disks):
                is_any_missing = any(p in missing_paths for p in paths)
                if is_any_missing:
                    dest_text.append("❌ ", style="bold red")
                else:
                    dest_text.append("✅ ", style="bold green")
                disk_name = Path(disk).name
                dest_text.append(f"{disk_name}: ", style="green")
                dest_text.append(f"{len(paths):,}", style="cyan")
                if i < len(sorted_disks) - 1:
                    dest_text.append("\n")

        verification_table.add_row(norm_dir, in_source, dest_text)

    console.print(verification_table)

    summary_table = Table(title="Итоговая сводка верификации", show_header=False)
    summary_table.add_column(style="cyan"); summary_table.add_column(justify="right", style="bold")
    summary_table.add_row("Всего проверено файлов/архивов", f"{len(all_dest_paths):,}")
    summary_table.add_row("Найдено на диске", f"[green]{len(all_dest_paths) - len(missing_paths):,}[/green]")
    summary_table.add_row("Отсутствует на диске", f"[red]{len(missing_paths):,}[/red]")
    console.print(summary_table)

    if missing_paths:
        do_save = questionary.confirm("Сохранить список отсутствующих файлов?").ask()
        if do_save:
            output_file = "physically_missing.csv"
            with open(output_file, 'w', newline='', encoding='utf-8') as f:
                writer = csv.writer(f); writer.writerow(['missing_destination_path'])
                for path in sorted(missing_paths): writer.writerow([path])
            console.print(f"✅ Список сохранен в [bold cyan]{output_file}[/bold cyan].")


def handle_stats_and_verify():
    console.rule("[bold magenta]3. Аудит и верификация по mapping-файлу[/bold magenta]")
    map_file_path = questionary.path("Укажите путь к mapping.csv файлу:", completer=path_completer, validate=lambda p: os.path.exists(p) or "Файл не найден").ask()
    if not map_file_path: return

    try:
        with open(map_file_path, 'r', encoding='utf-8', errors='ignore') as f:
            rows = [row for row in csv.reader(f) if len(row) >= 2]
            if not rows or len(rows) < 2:
                console.print("[yellow]Файл маппинга пуст или содержит только заголовок.[/yellow]"); return
            header = rows.pop(0)
    except Exception as e:
        console.print(f"[bold red]Не удалось прочитать файл: {e}[/bold red]"); return

    source_paths = {row[0] for row in rows}; dest_paths = {row[1] for row in rows}
    norm_source_dirs = {normalize_directory_path(os.path.dirname(p)) for p in source_paths}
    all_unique_norm_dirs = sorted(list(norm_source_dirs))
    stats_data = defaultdict(lambda: {"in_source": False, "destinations": defaultdict(list)})
    for p in source_paths: stats_data[normalize_directory_path(os.path.dirname(p))]["in_source"] = True
    for p in dest_paths:
        norm_dir = normalize_directory_path(os.path.dirname(p))
        parts = Path(p).parts; disk = f"/{parts[1]}/{parts[2]}" if len(parts) > 2 and parts[1] == 'mnt' else "unknown"
        stats_data[norm_dir]["destinations"][disk].append(p)

    console.clear(); console.rule(f"[bold]Статистика для [cyan]{os.path.basename(map_file_path)}[/cyan][/bold]")
    summary_text = (f"Обработано записей (исходных файлов): [cyan]{len(rows):,}[/cyan]\n" f"Создано физических файлов/архивов: [green bold]{len(dest_paths):,}[/green bold]")
    console.print(Panel(summary_text, title="Общая сводка", border_style="dim"))

    stats_table = Table(title="Детализация по каталогам и дискам", padding=(0, 1))
    stats_table.add_column("Общий каталог", style="magenta", no_wrap=True); stats_table.add_column("Источник", justify="center"); stats_table.add_column("Назначение", justify="left")
    for norm_dir in all_unique_norm_dirs:
        data = stats_data[norm_dir]; in_source = "[green]✅[/green]" if data["in_source"] else "[red]❌[/red]"
        dest_text = Text()
        if not data["destinations"]: dest_text.append("❌", style="red")
        else:
            sorted_disks = sorted(data["destinations"].items())
            for i, (disk, paths) in enumerate(sorted_disks):
                disk_name = Path(disk).name; dest_text.append(f"{disk_name}: ", style="green"); dest_text.append(f"{len(paths):,}", style="cyan")
                if i < len(sorted_disks) - 1: dest_text.append("\n")
        stats_table.add_row(norm_dir, in_source, dest_text)
    console.print(stats_table)

    if questionary.confirm("Хотите верифицировать эти файлы?", default=False).ask(): _run_verification(stats_data)


def handle_merge():
    console.rule("[bold cyan]1. Слияние mapping-файлов[/bold cyan]")
    maps_dir_path = questionary.path("Укажите путь к директории с mapping-файлами:", completer=dir_completer, validate=lambda p: os.path.isdir(p) or "Директория не найдена").ask()
    if not maps_dir_path: return

    pattern = questionary.text("Укажите шаблон для поиска файлов:", default="mapping*.csv").ask()
    if not pattern: return

    map_files = sorted(list(Path(maps_dir_path).glob(pattern)))
    if not map_files: console.print(f"[bold red]Файлы по шаблону '{pattern}' не найдены.[/bold red]"); return

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
                except StopIteration: file_stats.append((file_path.name, 0))
        except Exception as e: console.print(f"[yellow]Предупреждение: Не удалось прочитать {file_path}: {e}[/yellow]")

    summary_table = Table(title="Аналитика по mapping-файлам"); summary_table.add_column("Имя файла", style="green", no_wrap=True); summary_table.add_column("Количество записей", justify="right")
    for name, count in file_stats: summary_table.add_row(name, f"{count:,}")
    summary_table.add_section(); summary_table.add_row("[bold]Всего уникальных записей[/bold]", f"[bold cyan]{len(all_unique_mappings):,}[/bold cyan]")
    console.print(summary_table)

    output_filepath = Path(maps_dir_path) / "mapping_master.csv"
    if questionary.confirm(f"Сохранить {len(all_unique_mappings):,} записей в файл '{output_filepath}'?").ask():
        sorted_mappings = sorted(list(all_unique_mappings))
        with open(output_filepath, 'w', newline='', encoding='utf-8') as f:
            writer = csv.writer(f); writer.writerow(['source_path', 'destination_path']); writer.writerows(sorted_mappings)
        console.print(f"✅ Успешно сохранено в: [bold cyan]{output_filepath}[/bold cyan]")
    else: console.print("[yellow]Слияние отменено пользователем.[/yellow]")


def handle_analyze():
    console.rule("[bold yellow]2. Найти недокопированные файлы[/bold yellow]")
    source_list_path = questionary.path("Укажите путь к ИСХОДНОМУ CSV со списком ВСЕХ файлов:", completer=path_completer, validate=lambda p: os.path.exists(p) or "Файл не найден").ask()
    if not source_list_path: return
    state_file_path = questionary.path("Укажите путь к файлу состояния (copier_state.csv):", completer=path_completer, validate=lambda p: os.path.exists(p) or "Файл не найден").ask()
    if not state_file_path: return

    try:
        with open(state_file_path, 'r', encoding='utf-8') as f: processed_files_abs = {row[0] for row in csv.reader(f) if row}
        console.print(f"Загружено [bold]{len(processed_files_abs):,}[/bold] записей из файла состояния.")
    except Exception as e: console.print(f"[bold red]Не удалось прочитать state-файл: {e}[/bold red]"); return

    console.print("Анализ исходного списка...")
    source_list_paths_rel, source_data_map = [], {}
    try:
        with open(source_list_path, 'r', encoding='utf-8', errors='ignore') as f:
            reader = csv.reader(f, delimiter=';')
            for row in reader:
                if not row or len(row) < 5 or 'directory' in row[1]: continue
                rel_path_str = row[0]
                source_list_paths_rel.append(rel_path_str)
                source_data_map[rel_path_str] = row
    except Exception as e: console.print(f"[bold red]Не удалось прочитать исходный список: {e}[/bold red]"); return

    console.print("Интеллектуальное определение `source_root`..."); source_root = find_source_root(processed_files_abs, source_list_paths_rel)
    if source_root: console.print(f"✅ Автоматически определен `source_root`: [cyan]{source_root}[/cyan]")
    else: console.print("[bold yellow]Не удалось определить `source_root` автоматически.[/bold yellow]"); source_root = ""

    intended_files_abs = {os.path.normpath(os.path.join(source_root, p.lstrip('./'))) for p in source_list_paths_rel}
    missing_files_abs = sorted(list(intended_files_abs - processed_files_abs))

    table = Table(title="Отчет по анализу"); table.add_column("Параметр", style="cyan"); table.add_column("Количество", justify="right", style="white")
    table.add_row("Всего файлов в исходном списке", f"{len(intended_files_abs):,}")
    table.add_row("[green]Успешно обработано (есть в state-файле)[/green]", f"{len(intended_files_abs) - len(missing_files_abs):,}")
    table.add_row("[red]Не обработано (отсутствуют в state-файле)[/red]", f"{len(missing_files_abs):,}")
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
                if rel_path_unix in source_data_map: original_rel_path = rel_path_unix
                elif f"./{rel_path_unix}" in source_data_map: original_rel_path = f"./{rel_path_unix}"
                if original_rel_path: writer.writerow(source_data_map[original_rel_path])
        console.print(f"✅ Готово. Используйте [bold]'{output_file}'[/bold] как --input-file для copeer.py.")
    else: console.print("\n[bold green]✅ Отлично! Все файлы из исходного списка были обработаны.[/bold green]")


def handle_plan_vs_map():
    """Сравнивает файл-задание и mapping-файл, анализируя исходные пути."""
    console.rule("[bold green]4. Сравнение плана и `mapping` (по исходным путям)[/bold green]")

    plan_file_path = questionary.path(
        "Укажите путь к файлу ЗАДАНИЯ (например, group_2.csv):",
        completer=path_completer,
        validate=lambda p: os.path.exists(p) or "Файл не найден"
    ).ask()
    if not plan_file_path: return

    map_file_path = questionary.path(
        "Укажите путь к MAPPING-файлу (например, mapping_master.csv):",
        completer=path_completer,
        validate=lambda p: os.path.exists(p) or "Файл не найден"
    ).ask()
    if not map_file_path: return

    # --- 1. Загрузка данных ---
    plan_data = {} # {относительный_путь: полная_строка}
    try:
        console.print(f"Загрузка файла задания: [cyan]{os.path.basename(plan_file_path)}[/cyan]...")
        with open(plan_file_path, 'r', encoding='utf-8', errors='ignore') as f:
            for line in f:
                if line.strip():
                    plan_data[line.strip().split(';')[0]] = line.strip()
    except Exception as e:
        console.print(f"[bold red]Не удалось прочитать файл задания: {e}[/bold red]"); return

    mapped_source_paths = set()
    try:
        console.print(f"Загрузка mapping-файла: [cyan]{os.path.basename(map_file_path)}[/cyan]...")
        with open(map_file_path, 'r', encoding='utf-8', errors='ignore') as f:
            reader = csv.reader(f)
            next(reader, None) # Пропускаем заголовок
            for row in reader:
                if len(row) >= 1:
                    mapped_source_paths.add(row[0])
    except Exception as e:
        console.print(f"[bold red]Не удалось прочитать mapping-файл: {e}[/bold red]"); return

    # --- 2. УМНОЕ И НАДЕЖНОЕ СРАВНЕНИЕ ---
    console.print("Сравнение...")

    # Нормализуем пути из маппинга с помощью нашей новой универсальной функции
    normalized_mapped_sources = {normalize_source_path_for_comparison(p) for p in mapped_source_paths}

    # Вычитаем из множества путей плана множество нормализованных путей из маппинга
    missing_from_map_paths = set(plan_data.keys()) - normalized_mapped_sources

    # --- 3. Вывод результатов ---
    table = Table(title="Отчет о сравнении")
    table.add_column("Параметр", style="cyan")
    table.add_column("Количество", justify="right", style="white")
    table.add_row("Всего файлов в задании", f"{len(plan_data):,}")
    table.add_row("[green]Найдено в mapping-файле[/green]", f"{len(plan_data) - len(missing_from_map_paths):,}")
    table.add_row("[red]Отсутствует в mapping-файле[/red]", f"{len(missing_from_map_paths):,}")
    console.print(table)

    if missing_from_map_paths:
        do_save = questionary.confirm("Сохранить список отсутствующих в маппинге файлов?").ask()
        if do_save:
            output_format = questionary.select(
                "В каком формате сохранить?",
                choices=["Простой список (.txt)", "Готовый файл-задание (.csv) для copeer.py"]
            ).ask()

            if output_format == "Простой список (.txt)":
                output_file = "plan_missing_in_map.txt"
                with open(output_file, 'w', encoding='utf-8') as f:
                    for path in sorted(list(missing_from_map_paths)):
                        f.write(f"{path}\n")
                console.print(f"✅ Список сохранен в [bold cyan]{output_file}[/bold cyan].")
            else:
                output_file = "remaining_for_copy.csv"
                with open(output_file, 'w', encoding='utf-8') as f:
                    for path in sorted(list(missing_from_map_paths)):
                        if path in plan_data:
                            f.write(f"{plan_data[path]}\n")
                console.print(f"✅ Готовый файл-задание сохранен в [bold cyan]{output_file}[/bold cyan].")


def handle_filter_map_by_plan():
    """Фильтрует mapping-файл, оставляя только записи, присутствующие в файле-задании."""
    console.rule("[bold blue]5. Фильтровать `mapping` по файлу-заданию[/bold blue]")

    plan_file_path = questionary.path(
        "Укажите путь к файлу ЗАДАНИЯ (по которому будем фильтровать):",
        completer=path_completer,
        validate=lambda p: os.path.exists(p) or "Файл не найден"
    ).ask()
    if not plan_file_path: return

    map_file_path = questionary.path(
        "Укажите путь к MAPPING-файлу (который будем фильтровать):",
        completer=path_completer,
        validate=lambda p: os.path.exists(p) or "Файл не найден"
    ).ask()
    if not map_file_path: return

    # --- 1. Загрузка файла-задания (плана) ---
    plan_relative_paths = set()
    try:
        console.print(f"Загрузка файла задания: [cyan]{os.path.basename(plan_file_path)}[/cyan]...")
        with open(plan_file_path, 'r', encoding='utf-8', errors='ignore') as f:
            for line in f:
                if line.strip():
                    plan_relative_paths.add(line.strip().split(';')[0])
    except Exception as e:
        console.print(f"[bold red]Не удалось прочитать файл задания: {e}[/bold red]"); return

    # --- 2. Загрузка и фильтрация mapping-файла ---
    kept_rows = []
    original_map_count = 0
    try:
        console.print(f"Фильтрация mapping-файла: [cyan]{os.path.basename(map_file_path)}[/cyan]...")
        with open(map_file_path, 'r', encoding='utf-8', errors='ignore') as f:
            reader = csv.reader(f)
            header = next(reader, None)
            if not header:
                 console.print(f"[bold red]Mapping-файл пуст или не содержит заголовка.[/bold red]"); return

            for row in reader:
                original_map_count += 1
                if len(row) < 1: continue

                source_path = row[0]
                # Используем нашу новую универсальную функцию для нормализации
                normalized_path = normalize_source_path_for_comparison(source_path)

                # Если нормализованный путь есть в нашем задании, сохраняем всю строку
                if normalized_path in plan_relative_paths:
                    kept_rows.append(row)

    except Exception as e:
        console.print(f"[bold red]Не удалось прочитать mapping-файл: {e}[/bold red]"); return

    # --- 3. Вывод результатов и сохранение ---
    table = Table(title="Отчет о фильтрации")
    table.add_column("Параметр", style="cyan")
    table.add_column("Количество", justify="right", style="white")
    table.add_row("Всего файлов в задании для фильтрации", f"{len(plan_relative_paths):,}")
    table.add_row("Всего записей в исходном mapping-файле", f"{original_map_count:,}")
    table.add_row("[green]Останется записей после фильтрации[/green]", f"{len(kept_rows):,}")
    table.add_row("[yellow]Будет удалено записей[/yellow]", f"{original_map_count - len(kept_rows):,}")
    console.print(table)

    if kept_rows:
        output_filename = f"{Path(map_file_path).stem}_filtered.csv"
        do_save = questionary.confirm(f"Сохранить отфильтрованные записи в новый файл '{output_filename}'?").ask()
        if do_save:
            try:
                with open(output_filename, 'w', newline='', encoding='utf-8') as f:
                    writer = csv.writer(f)
                    writer.writerow(header)
                    writer.writerows(kept_rows)
                console.print(f"✅ Отфильтрованный mapping-файл сохранен в [bold cyan]{output_filename}[/bold cyan].")
            except Exception as e:
                console.print(f"[bold red]Ошибка при сохранении файла: {e}[/bold red]")
    else:
        console.print("[yellow]После фильтрации не осталось ни одной записи. Файл не будет создан.[/yellow]")


def main():
    while True:
        console.rule("[bold]Меню Copeer Auditor[/bold]")
        choice = questionary.select(
            "Выберите действие:",
            choices=[
                "1. Склеить `mapping` файлы",
                "2. Найти недокопированные файлы (по state-файлу)",
                "3. Аудит и верификация (по mapping-файлу)",
                "4. Сравнить план и `mapping` (найти что не в логе)",
                "5. Фильтровать `mapping` по файлу-заданию",
                questionary.Separator(),
                "Выход"
            ],
            use_indicator=True
        ).ask()

        if choice is None or choice == "Выход":
            console.print("[bold green]Выход.[/bold green]"); break

        console.clear()

        if "1." in choice: handle_merge()
        elif "2." in choice: handle_analyze()
        elif "3." in choice: handle_stats_and_verify()
        elif "4." in choice: handle_plan_vs_map()
        elif "5." in choice: handle_filter_map_by_plan()

        questionary.press_any_key_to_continue("Нажмите любую клавишу для возврата в меню...").ask()
        console.clear()

if __name__ == "__main__":
    try:
        main()
    except (KeyboardInterrupt, EOFError):
        console.print("\n[bold yellow]Выход по запросу пользователя.[/bold yellow]")
