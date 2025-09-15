#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Скрипт для исправления путей в CSV файлах путем поиска реальных путей в файловой системе.
Не придумывает кавычки, а ищет существующие файлы/директории.
"""

import os
import sys
import csv
import glob
from pathlib import Path

def find_real_path(broken_path: str) -> str:
    """
    Ищет реальный путь в файловой системе для сломанного пути.
    Использует glob для поиска вариантов с разными кавычками.
    """
    # Убираем все кавычки из пути
    clean_path = broken_path.replace('"', '').replace('"', '').replace('"', '')
    
    # Разбиваем путь на части
    parts = clean_path.split('/')
    
    # Начинаем с корня и идем по частям
    current_path = '/'
    
    for i, part in enumerate(parts):
        if not part:  # пропускаем пустые части
            continue
            
        # Строим путь до текущей части
        test_path = os.path.join(current_path, part)
        
        if os.path.exists(test_path):
            current_path = test_path
        else:
            # Ищем варианты с кавычками
            parent_dir = current_path
            if os.path.exists(parent_dir):
                try:
                    # Получаем список всех элементов в директории
                    items = os.listdir(parent_dir)
                    
                    # Ищем совпадение по базовому имени (без кавычек)
                    part_clean = part.replace('"', '').replace('"', '').replace('"', '')
                    
                    found = None
                    for item in items:
                        item_clean = item.replace('"', '').replace('"', '').replace('"', '')
                        if item_clean == part_clean:
                            found = item
                            break
                    
                    if found:
                        current_path = os.path.join(parent_dir, found)
                    else:
                        # Если не нашли, возвращаем что есть
                        return broken_path
                except (PermissionError, OSError):
                    # Если нет доступа к директории, возвращаем что есть
                    return broken_path
            else:
                return broken_path
    
    return current_path

def fix_csv_file(input_file: str, output_file: str):
    """
    Исправляет пути в CSV файле, находя реальные пути в файловой системе.
    """
    fixed_count = 0
    total_count = 0
    found_count = 0
    
    print(f"Обрабатываем файл: {input_file}")
    print(f"Результат будет сохранен в: {output_file}")
    print("-" * 60)
    
    with open(input_file, 'r', encoding='utf-8', errors='ignore') as infile, \
         open(output_file, 'w', encoding='utf-8', newline='') as outfile:
        
        reader = csv.reader(infile, delimiter=';')
        writer = csv.writer(outfile, delimiter=';', quoting=csv.QUOTE_MINIMAL)
        
        for row_num, row in enumerate(reader, 1):
            if not row or len(row) < 1:
                writer.writerow(row)
                continue
            
            original_path = row[0]
            total_count += 1
            
            # Ищем правильный путь в файловой системе
            real_path = find_real_path(original_path)
            
            # Проверяем, изменился ли путь
            if original_path != real_path:
                fixed_count += 1
                if row_num <= 5:  # Показываем первые 5 исправлений
                    print(f"Строка {row_num}:")
                    print(f"  Было:  {repr(original_path)}")
                    print(f"  Стало: {repr(real_path)}")
                    print()
            
            # Проверяем, существует ли файл
            if os.path.exists(real_path):
                found_count += 1
                if found_count <= 3:  # Показываем первые 3 найденных
                    print(f"✅ Файл найден: {real_path}")
            
            # Записываем исправленную строку
            row[0] = real_path
            writer.writerow(row)
            
            if row_num % 1000 == 0:
                print(f"Обработано строк: {row_num}")
    
    print("-" * 60)
    print(f"📊 Статистика:")
    print(f"   Всего строк обработано: {total_count}")
    print(f"   Путей исправлено: {fixed_count}")
    print(f"   Файлов найдено: {found_count}")
    print(f"   Процент найденных файлов: {(found_count/total_count*100):.1f}%")

def main():
    if len(sys.argv) != 2:
        print("Использование: python fix_csv_quotes.py <input_csv_file>")
        print("Пример: python fix_csv_quotes.py remaining_for_copy.csv")
        sys.exit(1)
    
    input_file = sys.argv[1]
    
    if not os.path.exists(input_file):
        print(f"❌ Файл не найден: {input_file}")
        sys.exit(1)
    
    # Создаем имя выходного файла
    input_path = Path(input_file)
    output_file = str(input_path.parent / f"{input_path.stem}_fixed{input_path.suffix}")
    
    try:
        fix_csv_file(input_file, output_file)
        print(f"✅ Готово! Исправленный файл сохранен как: {output_file}")
    except Exception as e:
        print(f"❌ Ошибка при обработке файла: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()