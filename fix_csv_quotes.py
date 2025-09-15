#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Скрипт для исправления кавычек в CSV файлах с путями файлов.
Исправляет пути вида 'название"' на '"название"' проверяя существование файлов в файловой системе.
"""

import os
import sys
import csv
from pathlib import Path

def fix_quotes_in_path(path: str) -> str:
    """
    Исправляет проблемные кавычки в путях.
    Конкретно для случаев вида: 'Шарика"/' -> '"Шарика"/'
    """
    # Разбиваем путь на части
    parts = path.split('/')
    fixed_parts = []
    
    for part in parts:
        # Исправляем части вида 'Слово"' (только закрывающая кавычка)
        if '"' in part and not part.startswith('"'):
            # Поиск последнего пробела перед кавычкой
            if ' ' in part:
                # Находим последний пробел перед словом в кавычках
                quote_pos = part.rfind('"')
                if quote_pos > 0:
                    # Находим последний пробел перед закрывающей кавычкой
                    space_pos = part.rfind(' ', 0, quote_pos)
                    if space_pos != -1:
                        # Вставляем открывающую кавычку после пробела
                        fixed_part = part[:space_pos+1] + '"' + part[space_pos+1:]
                        # Заменяем ASCII кавычки на Unicode
                        fixed_part = fixed_part.replace('"', '"')
                        fixed_parts.append(fixed_part)
                    else:
                        fixed_parts.append(part.replace('"', '"'))
                else:
                    fixed_parts.append(part.replace('"', '"'))
            else:
                fixed_parts.append(part.replace('"', '"'))
        else:
            # Просто заменяем ASCII кавычки на Unicode
            fixed_parts.append(part.replace('"', '"'))
    
    return '/'.join(fixed_parts)

def find_existing_path(original_path: str) -> str:
    """
    Ищет существующий путь к файлу, пробуя разные варианты.
    """
    # Пробуем исходный путь
    if os.path.exists(original_path):
        return original_path
    
    # Пробуем исправленный путь
    fixed_path = fix_quotes_in_path(original_path)
    if os.path.exists(fixed_path):
        return fixed_path
    
    # Если ничего не найдено, возвращаем исправленный путь
    return fixed_path

def fix_csv_file(input_file: str, output_file: str):
    """
    Исправляет пути в CSV файле, проверяя существование файлов в файловой системе.
    """
    fixed_count = 0
    total_count = 0
    not_found_count = 0
    
    print(f"Обрабатываем файл: {input_file}")
    print(f"Результат будет сохранен в: {output_file}")
    print("-" * 60)
    
    with open(input_file, 'r', encoding='utf-8', errors='ignore') as infile, \
         open(output_file, 'w', encoding='utf-8', newline='') as outfile:
        
        reader = csv.reader(infile, delimiter=';')
        writer = csv.writer(outfile, delimiter=';')
        
        for row_num, row in enumerate(reader, 1):
            if not row or len(row) < 1:
                writer.writerow(row)
                continue
            
            original_path = row[0]
            total_count += 1
            
            # Ищем правильный путь
            correct_path = find_existing_path(original_path)
            
            # Проверяем, изменился ли путь
            if original_path != correct_path:
                fixed_count += 1
                if row_num <= 5:  # Показываем первые 5 исправлений
                    print(f"Строка {row_num}:")
                    print(f"  Было:  {repr(original_path)}")
                    print(f"  Стало: {repr(correct_path)}")
                    print()
            
            # Проверяем, существует ли файл
            if not os.path.exists(correct_path):
                not_found_count += 1
                if not_found_count <= 3:  # Показываем первые 3 не найденных
                    print(f"⚠️  Файл не найден: {correct_path}")
            
            # Записываем исправленную строку
            row[0] = correct_path
            writer.writerow(row)
            
            if row_num % 1000 == 0:
                print(f"Обработано строк: {row_num}")
    
    print("-" * 60)
    print(f"📊 Статистика:")
    print(f"   Всего строк обработано: {total_count}")
    print(f"   Путей исправлено: {fixed_count}")
    print(f"   Файлов не найдено: {not_found_count}")
    print(f"   Процент исправлений: {(fixed_count/total_count*100):.1f}%")

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