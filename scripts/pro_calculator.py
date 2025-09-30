# -*- coding: utf-8 -*-

import os
import glob
import readline  # Включает автодополнение и историю для input()

from rich.console import Console
from rich.panel import Panel
from rich.table import Table
from rich.progress import (
    Progress,
    BarColumn,
    TextColumn,
    MofNCompleteColumn,
)
from rich.filesize import decimal as decimal_filesize

# --- УЛУЧШЕННАЯ Логика для автодополнения пути к файлу ---

class PathCompleter:
    """
    Улучшенный класс для автодополнения путей, который позволяет
    перемещаться по каталогам.
    """
    def complete(self, text, state):
        # Если это первый вызов для данного текста, генерируем список совпадений
        if state == 0:
            # Расширяем тильду (~) до домашней директории
            expanded_text = os.path.expanduser(text)

            # Определяем, в какой директории искать и какой префикс использовать
            if os.path.isdir(expanded_text) and not text.endswith(os.sep):
                # Если пользователь ввел имя существующей директории без слэша в конце,
                # мы добавим слэш и будем искать внутри нее.
                dir_path = expanded_text
                prefix = ''
            else:
                dir_path = os.path.dirname(expanded_text)
                prefix = os.path.basename(expanded_text)

            # Если путь к директории пуст, значит ищем в текущей директории
            if not dir_path:
                search_dir = '.'
            else:
                search_dir = dir_path

            self.matches = []
            try:
                # Получаем список всех файлов и каталогов в директории для поиска
                for item in os.listdir(search_dir):
                    if item.startswith(prefix):
                        # Собираем полный путь к найденному элементу
                        full_path = os.path.join(dir_path, item)
                        
                        # Если это директория, добавляем слэш в конце
                        if os.path.isdir(os.path.expanduser(full_path)):
                             self.matches.append(full_path + os.sep)
                        else:
                             self.matches.append(full_path)
            except (OSError, FileNotFoundError):
                # Если директория не существует, совпадений не будет
                pass

        # Возвращаем совпадение для текущего состояния (state)
        try:
            return self.matches[state]
        except IndexError:
            return None

def setup_path_completion():
    """Настраивает readline для автодополнения путей."""
    # Устанавливаем наш кастомный комплитер
    readline.set_completer(PathCompleter().complete)
    
    # Символы, которые разделяют "слово" для автодополнения
    readline.set_completer_delims(' \t\n;')

    # Привязываем Tab к функции автодополнения
    if 'libedit' in readline.__doc__:
        # Для macOS
        readline.parse_and_bind("bind ^I rl_complete")
    else:
        # Для Linux
        readline.parse_and_bind("tab: complete")

# --- Основная логика анализа файла (без изменений) ---

def analyze_file(file_path, console):
    """
    Анализирует файл, суммирует размеры и выводит результат с помощью Rich.
    """
    total_size_bytes = 0
    line_count = 0
    error_count = 0

    try:
        file_total_bytes = os.path.getsize(file_path)
        with Progress(
            TextColumn("[bold blue]{task.description}", justify="right"),
            BarColumn(bar_width=None),
            "[progress.percentage]{task.percentage:>3.1f}%",
            "•",
            MofNCompleteColumn(),
            console=console,
        ) as progress:
            task = progress.add_task("[green]Анализ файла...", total=file_total_bytes)
            with open(file_path, 'r', encoding='utf-8', errors='ignore') as f:
                for line in f:
                    line_count += 1
                    cleaned_line = line.strip()
                    if cleaned_line:
                        try:
                            parts = cleaned_line.split(';')
                            size_str = parts[-1]
                            total_size_bytes += int(size_str)
                        except (ValueError, IndexError):
                            error_count += 1
                    progress.update(task, advance=len(line.encode('utf-8', 'ignore')))
            progress.update(task, completed=file_total_bytes)

    except FileNotFoundError:
        console.print(f"[bold red]Ошибка: Файл не найден по пути '{file_path}'[/bold red]")
        return
    except IsADirectoryError:
        console.print(f"[bold red]Ошибка: Указанный путь '{file_path}' является каталогом, а не файлом.[/bold red]")
        return
    except Exception as e:
        console.print(f"[bold red]Произошла непредвиденная ошибка: {e}[/bold red]")
        return
    
    display_results(total_size_bytes, line_count, error_count, console)

def display_results(total_bytes, lines, errors, console):
    """Формирует и выводит красивую таблицу с результатами."""
    table = Table(show_header=True, header_style="bold magenta")
    table.add_column("Метрика", style="dim", width=25)
    table.add_column("Значение", justify="right")

    BYTES_IN_TB = 1024**4

    table.add_row("Обработано строк", f"{lines:,}")
    if errors > 0:
        table.add_row("[yellow]Пропущено строк (ошибка формата)[/yellow]", f"[yellow]{errors:,}[/yellow]")
    
    table.add_row("Общий размер (Байты)", f"{total_bytes:,}")
    table.add_row("Общий размер (Human-readable)", f"[cyan]{decimal_filesize(total_bytes)}[/cyan]")
    table.add_row(
        "[bold]Итоговая сумма (ТБ)[/bold]", 
        f"[bold green]{total_bytes / BYTES_IN_TB:.4f} ТБ[/bold green]"
    )

    console.print(Panel.fit(table, title="[bold]Результаты анализа[/bold]", border_style="green"))


if __name__ == "__main__":
    console = Console()
    
    setup_path_completion()

    console.print("[bold cyan]Калькулятор дискового пространства[/bold cyan]")
    console.print("Начните вводить путь к файлу и используйте [bold yellow]TAB[/bold yellow] для автодополнения.")
    
    try:
        input_file_path = input("Введите путь к файлу: ")
        expanded_path = os.path.expanduser(input_file_path.strip())
        
        analyze_file(expanded_path, console)
        
    except (KeyboardInterrupt, EOFError):
        console.print("\n[yellow]Выход из программы.[/yellow]")
