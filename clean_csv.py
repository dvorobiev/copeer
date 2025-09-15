#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Простой скрипт для очистки кавычек в CSV файлах.
Убирает все лишние кавычки и оставляет только правильные пути.
"""

import sys

def clean_quotes_in_line(line):
    """
    Очищает строку от лишних кавычек в первом поле (пути к файлу).
    """
    # Разбиваем строку по первому точке с запятой
    parts = line.split(';', 1)
    if len(parts) < 2:
        return line
    
    path_part = parts[0]
    rest_part = parts[1]
    
    # Убираем кавычки из начала и конца, если они есть
    if path_part.startswith('"') and path_part.endswith('"'):
        path_part = path_part[1:-1]
    
    # Заменяем двойные кавычки на правильные
    path_part = path_part.replace('""', '"')
    
    # Собираем строку обратно
    return path_part + ';' + rest_part

def main():
    if len(sys.argv) != 2:
        print("Использование: python clean_csv.py <input_csv_file>")
        print("Пример: python clean_csv.py remaining_for_copy_fixed.csv")
        sys.exit(1)
    
    input_file = sys.argv[1]
    output_file = input_file.replace('.csv', '_clean.csv')
    
    try:
        with open(input_file, 'r', encoding='utf-8') as infile, \
             open(output_file, 'w', encoding='utf-8') as outfile:
            
            line_count = 0
            for line in infile:
                line = line.rstrip('\n\r')
                if line:
                    cleaned_line = clean_quotes_in_line(line)
                    outfile.write(cleaned_line + '\n')
                    line_count += 1
                    
                    # Показываем примеры первых 3 исправлений
                    if line_count <= 3 and line != cleaned_line:
                        print(f"Строка {line_count}:")
                        print(f"  Было:  {repr(line)}")
                        print(f"  Стало: {repr(cleaned_line)}")
                        print()
        
        print(f"✅ Готово! Обработано {line_count} строк.")
        print(f"Результат сохранен в: {output_file}")
        
    except Exception as e:
        print(f"❌ Ошибка: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()