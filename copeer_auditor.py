# copeer_auditor.py (v2 - No Sequence Guessing)
"""
Утилита для анализа, слияния и верификации результатов работы copeer.py.
Эта версия НЕ пытается заново определять секвенции, а работает напрямую
с файлами состояния и маппинга, что является более корректным подходом.
"""
import argparse
import csv
import os
import sys
from pathlib import Path

# Сторонние библиотеки (убедитесь, что rich установлен: pip install rich)
from rich.console import Console
from rich.table import Table
from rich.progress import Progress
from rich.prompt import Confirm

console = Console()

# --- Вспомогательные функции ---

def get_abs_path(path, root):
    """Преобразует путь в абсолютный, если он еще не такой."""
    if os.path.isabs(path):
        return os.path.normpath(path)
    if root is None:
        console.print("[bold red]Ошибка: source_root не определен. Невозможно преобразовать относительные пути в абсолютные.[/bold red]")
        console.print("       [yellow]Укажите 'source_root' в config.yaml или используйте аргумент --source-root.[/yellow]")
        sys.exit(1)
    return os.path.normpath(os.path.join(root, path))

def parse_scientific_notation(size_str: str) -> int:
    """Парсит размер файла из научной нотации или обычного числа."""
    try:
        cleaned_str = str(size_str).replace(',', '.').strip()
        if 'E' in cleaned_str.upper(): return int(float(cleaned_str))
        return int(cleaned_str)
    except (ValueError, TypeError): return 0

# --- Функции команд ---

def handle_merge(args):
    """
    Шаг 1: Склеивает несколько mapping-файлов в один, удаляя дубликаты,
    с предварительным анализом и подтверждением.
    """
    console.rule("[bold cyan]Шаг 1: Слияние mapping-файлов[/bold cyan]")
    maps_dir = Path(args.maps_dir)
    if not maps_dir.is_dir():
        console.print(f"[bold red]Ошибка: Директория '{maps_dir}' не найдена.[/bold red]")
        sys.exit(1)

    map_files = sorted(list(maps_dir.glob(args.pattern)))
    if not map_files:
        console.print(f"[bold red]Файлы по шаблону '{args.pattern}' в директории '{maps_dir}' не найдены.[/bold red]")
        sys.exit(1)

    console.print(f"Найдено {len(map_files)} файлов для анализа слияния...")

    all_unique_mappings = set()
    file_stats = []

    for file_path in map_files:
        try:
            with open(file_path, 'r', encoding='utf-8', errors='ignore') as f:
                reader = csv.reader(f)
                try:
                    # Пропускаем заголовок
                    next(reader)
                    rows = [tuple(row) for row in reader if len(row) >= 2]
                    file_stats.append((file_path.name, len(rows)))
                    all_unique_mappings.update(rows)
                except StopIteration:
                    file_stats.append((file_path.name, 0)) # Пустой файл
        except Exception as e:
            console.print(f"[yellow]Предупреждение: Не удалось прочитать {file_path}: {e}[/yellow]")

    # Отображаем аналитику перед слиянием
    summary_table = Table(title="Аналитика по mapping-файлам")
    summary_table.add_column("Имя файла", style="green", no_wrap=True)
    summary_table.add_column("Количество записей", justify="right")

    for name, count in file_stats:
        summary_table.add_row(name, f"{count:,}")

    summary_table.add_section()
    summary_table.add_row("[bold]Всего уникальных записей[/bold]", f"[bold cyan]{len(all_unique_mappings):,}[/bold cyan]")
    console.print(summary_table)

    # Запрашиваем подтверждение
    if not Confirm.ask(f"\n[bold]Сохранить {len(all_unique_mappings):,} уникальных записей в файл '{args.output_file}'?[/bold]"):
        console.print("[yellow]Слияние отменено пользователем.[/yellow]")
        return

    # Сортируем для консистентного вывода
    sorted_mappings = sorted(list(all_unique_mappings))

    with open(args.output_file, 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow(['source_path', 'destination_path'])
        writer.writerows(sorted_mappings)

    console.print(f"✅ Успешно сохранено в: [bold cyan]{args.output_file}[/bold cyan]")


def handle_analyze(args):
    """
    Шаг 2: Сравнивает исходный список и state-файл для поиска необработанных файлов.
    Больше не угадывает секвенции.
    """
    console.rule("[bold yellow]Шаг 2: Анализ полноты копирования[/bold yellow]")

    # 1. Загружаем state-файл
    try:
        with open(args.state_file, 'r', encoding='utf-8') as f:
            # copier_state.csv содержит абсолютные пути к успешно обработанным ИСХОДНЫМ файлам
            processed_files = {row[0] for row in csv.reader(f) if row}
        console.print(f"Загружено [bold]{len(processed_files):,}[/bold] записей из файла состояния ([cyan]{args.state_file}[/cyan])")
    except FileNotFoundError:
        console.print(f"[bold red]Ошибка: Файл состояния '{args.state_file}' не найден.[/bold red]")
        sys.exit(1)

    # 2. Загружаем исходный список файлов
    console.print(f"Анализ исходного списка файлов: [cyan]{args.source_list}[/cyan]")
    intended_files = set()
    source_data_map = {} # {abs_path: [rel_path, 'file', size_bytes, '', size_scientific]}
    try:
        with open(args.source_list, 'r', encoding='utf-8', errors='ignore') as f:
            reader = csv.reader(f, delimiter=';')
            for row in reader:
                if not row or len(row) < 5 or 'directory' in row[1]:
                    continue
                rel_path, _, _, _, size_str = row
                # Путь в исходном файле может быть относительным. Нужен source_root для преобразования.
                abs_path = get_abs_path(rel_path, args.source_root)
                intended_files.add(abs_path)
                source_data_map[abs_path] = [rel_path, 'file', parse_scientific_notation(size_str), '', size_str]
    except FileNotFoundError:
        console.print(f"[bold red]Ошибка: Исходный список '{args.source_list}' не найден.[/bold red]")
        sys.exit(1)

    total_files_in_source_list = len(intended_files)
    console.print(f"Найдено [bold]{total_files_in_source_list:,}[/bold] валидных файловых записей в исходном списке.")

    # 3. Находим разницу - это и есть недостающие файлы. Просто и надежно.
    missing_files = sorted(list(intended_files - processed_files))

    # 4. Формируем отчет
    table = Table(title="Отчет по анализу")
    table.add_column("Параметр", style="cyan")
    table.add_column("Количество", justify="right", style="white")

    processed_count = total_files_in_source_list - len(missing_files)
    table.add_row("Всего файлов в исходном списке", f"{total_files_in_source_list:,}")
    table.add_row("[green]Успешно обработано (есть в state-файле)[/green]", f"[green]{processed_count:,}[/green]")
    table.add_row("[red]Не обработано (отсутствуют в state-файле)[/red]", f"[red]{len(missing_files):,}[/red]")
    console.print(table)

    if missing_files:
        console.print(f"\nСохранение списка из {len(missing_files):,} необработанных файлов в [bold cyan]{args.output_missing}[/bold cyan]...")
        with open(args.output_missing, 'w', newline='', encoding='utf-8') as f:
            writer = csv.writer(f, delimiter=';')
            for abs_path in missing_files:
                if abs_path in source_data_map:
                    writer.writerow(source_data_map[abs_path])
        console.print("✅ Готово. Этот файл можно использовать как новый --input-file для copeer.py.")
    else:
        console.print("\n[bold green]✅ Отлично! Все файлы из исходного списка были обработаны.[/bold green]")


def handle_verify(args):
    """
    Шаг 3: Проверяет физическое наличие КОНЕЧНЫХ файлов на дисках по mapping-файлу.
    """
    console.rule("[bold magenta]Шаг 3: Верификация файлов на дисках[/bold magenta]")
    mapping_file = Path(args.mapping_file)
    if not mapping_file.is_file():
        console.print(f"[bold red]Ошибка: Mapping-файл '{mapping_file}' не найден.[/bold red]")
        sys.exit(1)

    try:
        with open(mapping_file, 'r', encoding='utf-8', errors='ignore') as f:
            reader = csv.reader(f)
            next(reader) # Пропускаем заголовок
            # Получаем уникальные пути назначения, чтобы не проверять один и тот же .tar файл 1000 раз
            unique_dest_paths = {row[1] for row in reader if len(row) >= 2}
    except Exception as e:
        console.print(f"[bold red]Не удалось прочитать файл: {e}[/bold red]")
        sys.exit(1)

    found_count = 0
    missing_count = 0
    missing_dest_paths = []

    with Progress(console=console) as progress:
        task = progress.add_task("[green]Проверка файлов...", total=len(unique_dest_paths))
        for dest_path in unique_dest_paths:
            if os.path.exists(dest_path):
                found_count += 1
            else:
                missing_count += 1
                missing_dest_paths.append(dest_path)
            progress.update(task, advance=1)

    table = Table(title=f"Отчет по верификации для '{mapping_file.name}'")
    table.add_column("Статус", style="cyan")
    table.add_column("Количество", justify="right", style="white")
    table.add_row("Всего уникальных конечных файлов", f"{len(unique_dest_paths):,}")
    table.add_row("[green]Найдено на диске[/green]", f"[green]{found_count:,}[/green]")
    table.add_row("[red]Отсутствует на диске[/red]", f"[red]{missing_count:,}[/red]")
    console.print(table)

    if missing_dest_paths and args.output_missing:
        console.print(f"\nСохранение списка из {missing_count:,} физически отсутствующих файлов в [bold cyan]{args.output_missing}[/bold cyan]...")
        with open(args.output_missing, 'w', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            writer.writerow(['missing_destination_path'])
            for path in sorted(missing_dest_paths):
                writer.writerow([path])
        console.print("✅ Готово.")


def main():
    parser = argparse.ArgumentParser(
        description="Утилита-аудитор для результатов работы copeer.py.",
        formatter_class=argparse.RawTextHelpFormatter
    )
    subparsers = parser.add_subparsers(dest="command", required=True, help="Доступные команды")

    # Команда 1: Слияние
    parser_merge = subparsers.add_parser("merge", help="Склеить mapping-файлы в один с предварительной аналитикой и подтверждением.")
    parser_merge.add_argument("maps_dir", help="Директория, содержащая ваши mapping-файлы.")
    parser_merge.add_argument("--pattern", default="mapping*.csv", help="Шаблон для поиска файлов (по умолчанию: 'mapping*.csv').")
    parser_merge.add_argument("--output-file", default="mapping_master.csv", help="Имя итогового объединенного файла (по умолчанию: 'mapping_master.csv').")
    parser_merge.set_defaults(func=handle_merge)

    # Команда 2: Анализ
    parser_analyze = subparsers.add_parser("analyze", help="Сравнить исходный список и state-файл, чтобы найти, что не скопировалось.")
    parser_analyze.add_argument("source_list", help="Путь к ИСХОДНОМУ CSV-файлу со списком ВСЕХ файлов.")
    parser_analyze.add_argument("state_file", help="Путь к файлу состояния copier_state.csv.")
    parser_analyze.add_argument("--source-root", help="Абсолютный путь к корню исходных файлов. Необходим, если в source_list пути относительные.")
    parser_analyze.add_argument("--output-missing", default="missing_for_copy.csv", help="Файл, куда будет сохранен список недостающих файлов для повторного запуска copeer.py.")
    parser_analyze.set_defaults(func=handle_analyze)

    # Команда 3: Верификация
    parser_verify = subparsers.add_parser("verify", help="Проверить физическое наличие КОНЕЧНЫХ файлов на дисках по mapping-файлу.")
    parser_verify.add_argument("mapping_file", help="Путь к объединенному master-mapping файлу (созданному командой 'merge').")
    parser_verify.add_argument("--output-missing", help="(Опционально) Сохранить список физически отсутствующих файлов.")
    parser_verify.set_defaults(func=handle_verify)

    # Если запустить без команды, показать помощь
    if len(sys.argv) == 1:
        parser.print_help(sys.stderr)
        sys.exit(1)

    args = parser.parse_args()

    # Для команды analyze нужен source_root. Пытаемся взять его из аргумента или из config.yaml
    if args.command == 'analyze' and not args.source_root:
        config_path = "config.yaml"
        if os.path.exists(config_path):
            import yaml
            try:
                with open(config_path, 'r') as f:
                    config = yaml.safe_load(f)
                    if 'source_root' in config:
                        args.source_root = config['source_root']
            except Exception as e:
                console.print(f"[yellow]Предупреждение: не удалось прочитать 'source_root' из {config_path}: {e}[/yellow]")

    args.func(args)

if __name__ == "__main__":
    main()
