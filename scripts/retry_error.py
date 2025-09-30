import argparse
import csv
import os
import time

def create_retry_csv_auto(error_log_file, output_csv_file):
    """
    Анализирует лог ошибок, АВТОМАТИЧЕСКИ определяет source_root,
    проверяет наличие файлов и создает CSV для повторной попытки.
    """
    print(f"Анализ файла ошибок: '{error_log_file}'")

    try:
        with open(error_log_file, 'r', encoding='utf-8') as f:
            error_lines = f.readlines()
    except FileNotFoundError:
        print(f"[ОШИБКА] Файл ошибок не найден: '{error_log_file}'")
        return

    # --- Фаза 1: Сбор всех путей и авто-определение source_root ---
    all_absolute_paths = []
    for line in error_lines:
        if not line.strip() or ';' not in line:
            continue
        try:
            # Формат: дата;путь;сообщение
            path = line.split(';', 2)[1].strip()
            all_absolute_paths.append(path)
        except IndexError:
            continue # Пропускаем некорректные строки

    if not all_absolute_paths:
        print("[ИНФО] В файле ошибок не найдено путей для обработки.")
        return

    # Используем os.path.commonpath для поиска общей базовой директории
    auto_detected_source_root = os.path.commonpath(all_absolute_paths)
    
    # Если общая часть - это файл, берем его родительскую директорию
    if not os.path.isdir(auto_detected_source_root):
        auto_detected_source_root = os.path.dirname(auto_detected_source_root)
        
    print(f"[ИНФО] Автоматически определен source_root: '{auto_detected_source_root}'")

    # --- Фаза 2: Генерация CSV для повторной попытки ---
    files_to_retry = []
    processed_paths = set()
    skipped_count = 0

    for absolute_path in all_absolute_paths:
        # Пропускаем дубликаты
        if absolute_path in processed_paths:
            continue
        processed_paths.add(absolute_path)
        
        # Проверяем, существует ли файл до сих пор
        if not os.path.exists(absolute_path):
            print(f"[ПРОПУСК] Исходный файл по-прежнему не найден: {absolute_path}")
            skipped_count += 1
            continue

        # Собираем метаданные для формата copeer.py
        try:
            stats = os.stat(absolute_path)
            # Вычисляем относительный путь, используя наш авто-определенный root
            relative_path = os.path.relpath(absolute_path, auto_detected_source_root)
            
            # Формат: rel_path; file_type; mtime; mtime; size
            file_type = "regular file"
            mtime = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(stats.st_mtime))
            size = stats.st_size
            
            row = [relative_path, file_type, mtime, mtime, str(size)]
            files_to_retry.append(row)

        except Exception as e:
            print(f"[ОШИБКА] Не удалось получить метаданные для файла '{absolute_path}': {e}")
            skipped_count += 1


    # --- Фаза 3: Запись результата в новый CSV ---
    if not files_to_retry:
        print("\nНе найдено существующих файлов для повторной попытки.")
        return

    try:
        with open(output_csv_file, 'w', newline='', encoding='utf-8') as f:
            writer = csv.writer(f, delimiter=';')
            writer.writerows(files_to_retry)
    except IOError as e:
        print(f"\n[ОШИБКА] Не удалось записать выходной файл '{output_csv_file}': {e}")
        return

    print("\n" + "="*20 + " ОТЧЕТ " + "="*20)
    print(f"Всего уникальных ошибок в логе: {len(processed_paths)}")
    print(f"Пропущено (файлы не существуют): {skipped_count}")
    print(f"✅ Успешно создан файл для повторной попытки: '{output_csv_file}'")
    print(f"   - В него добавлено {len(files_to_retry)} файлов.")
    print("="*48)
    print("\nТеперь вы можете запустить основной скрипт с этим файлом:")
    print(f"python3 copeer.py --input-file {output_csv_file}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Создает из лога ошибок copeer.py новый CSV-файл для повторной попытки.",
        formatter_class=argparse.RawTextHelpFormatter
    )
    parser.add_argument(
        '-e', '--error-log',
        required=True,
        help="Путь к файлу с логом ошибок (например, 'errors.log')."
    )
    parser.add_argument(
        '-o', '--output-file',
        default='retry_list.csv',
        help="Имя выходного файла для повторной попытки (по умолчанию: 'retry_list.csv')."
    )

    args = parser.parse_args()
    create_retry_csv_auto(args.error_log, args.output_file)
