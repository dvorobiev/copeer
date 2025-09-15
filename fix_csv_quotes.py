#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Скрипт для исправления путей в CSV файлах.
Ищет реальные пути файлов на диске и заменяет проблемные пути.
"""

import os
import sys
import glob
from pathlib import Path

def find_real_path(problematic_path: str) -> str:
    """
    Ищет реальный путь файла на диске, исходя из проблемного пути.
    """
    # Убираем все кавычки для поиска
    clean_path = problematic_path.strip('"').replace('""', '"').rstrip('"')
    
    # Проверяем, существует ли путь как есть
    if os.path.exists(clean_path):
        return clean_path
    
    # Если прямой путь не найден, пробуем найти похожий
    try:
        # Разбиваем путь на части
        path_parts = clean_path.split('/')
        
        # Ищем от корня, проверяя каждую часть
        current_search = '/'
        
        for i, part in enumerate(path_parts[1:], 1):  # пропускаем первую пустую часть
            if not part:  # пропускаем пустые части
                continue
                
            # Строим текущий путь
            test_path = os.path.join(current_search, part)
            
            if os.path.exists(test_path):
                current_search = test_path
                continue
            
            # Если точного совпадения нет, ищем похожие папки/файлы
            parent_dir = current_search
            if os.path.exists(parent_dir) and os.path.isdir(parent_dir):
                # Ищем в родительской папке
                search_pattern = os.path.join(parent_dir, '*')
                candidates = glob.glob(search_pattern)
                
                best_match = None
                for candidate in candidates:
                    candidate_name = os.path.basename(candidate)
                    # Проверяем похожесть (убираем проблемные кавычки для сравнения)
                    clean_candidate = candidate_name.replace('"', '')
                    clean_part = part.replace('"', '')
                    
                    if clean_candidate == clean_part or clean_part in clean_candidate:
                        best_match = candidate
                        break
                
                if best_match:
                    current_search = best_match
                else:
                    # Если не нашли, останавливаемся
                    break
            else:
                break
        
        # Проверяем финальный путь
        if os.path.exists(current_search) and current_search != '/':
            return current_search
            
    except Exception as e:
        print(f"Ошибка при поиске пути {clean_path}: {e}")
    
    # Если ничего не нашли, возвращаем очищенный путь
    return clean_path

def fix_csv_file(input_file: str, output_file: str):
    """
    Исправляет пути в CSV файле, ища реальные пути на диске.
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
                
                # Ищем реальный путь на диске
                real_path = find_real_path(original_path)
                
                # Проверяем, изменился ли путь
                if original_path != real_path:
                    fixed_count += 1
                    if fixed_count <= 5:  # Показываем первые 5 исправлений
                        print(f"Строка {line_num}:")
                        print(f"  Было:  {repr(original_path)}")
                        print(f"  Стало: {repr(real_path)}")
                        
                        # Проверяем, существует ли найденный путь
                        if os.path.exists(real_path):
                            print(f"  ✅ Файл найден на диске")
                        else:
                            print(f"  ⚠️  Файл не найден (очищен путь)")
                        print()
                
                # Записываем строку с найденным путем
                fixed_line = real_path + ';' + rest_part
                outfile.write(fixed_line + '\n')
                
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