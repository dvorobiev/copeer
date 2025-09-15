#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Скрипт для исправления кавычек в CSV файлах.
Очищает проблемные кавычки, которые вызывают проблемы с CSV парсингом.
"""

import os
import sys
from pathlib import Path

def clean_quotes_in_path(path: str) -> str:
    """
    Очищает кавычки в пути от проблем с CSV экранированием.
    Убирает внешние кавычки и заменяет двойные кавычки на одинарные.
    """
    # Убираем кавычки из начала и конца, если они есть
    if path.startswith('"') and path.endswith('"'):
        path = path[1:-1]
    
    # Заменяем двойные кавычки на правильные
    path = path.replace('""', '"')
    
    return path

def fix_csv_file(input_file: str, output_file: str):
    """
    Исправляет кавычки в CSV файле, очищая их от проблем с экранированием.
    """
    fixed_count = 0
    total_count = 0
    
    print(f"Обрабатываем файл: {input_file}")
    print(f"Результат будет сохранен в: {output_file}")
    print("-" * 60)
    
    try:
        with open(input_file, 'r', encoding='utf-8', errors='ignore') as infile, \
             open(output_file, 'w', encoding='utf-8') as outfile:
            
            for line_num, line in enumerate(infile, 1):
                line = line.rstrip('\n\r')
                if not line:
                    outfile.write('\n')
                    continue
                
                total_count += 1
                
                # Разбиваем строку по первому точке с запятой
                parts = line.split(';', 1)
                if len(parts) < 2:
                    outfile.write(line + '\n')
                    continue
                
                original_path = parts[0]
                rest_part = parts[1]
                
                # Очищаем кавычки в пути
                cleaned_path = clean_quotes_in_path(original_path)
                
                # Проверяем, изменился ли путь
                if original_path != cleaned_path:
                    fixed_count += 1
                    if fixed_count <= 5:  # Показываем первые 5 исправлений
                        print(f"Строка {line_num}:")
                        print(f"  Было:  {repr(original_path)}")
                        print(f"  Стало: {repr(cleaned_path)}")
                        print()
                
                # Записываем исправленную строку
                cleaned_line = cleaned_path + ';' + rest_part
                outfile.write(cleaned_line + '\n')
                
                if line_num % 1000 == 0:
                    print(f"Обработано строк: {line_num}")
        
        print("-" * 60)
        print(f"📊 Статистика:")
        print(f"   Всего строк обработано: {total_count}")
        print(f"   Путей исправлено: {fixed_count}")
        print(f"   Процент исправленных: {(fixed_count/total_count*100):.1f}%")
        
    except Exception as e:
        print(f"❌ Ошибка при обработке: {e}")
        raise

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