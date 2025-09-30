#!/bin/bash

# Файл для анализа
MAPPING_FILE="mapping.csv"

# --- Проверки ---
if [[ ! -f "$MAPPING_FILE" ]]; then
    echo "Ошибка: Файл '$MAPPING_FILE' не найден!"
    exit 1
fi

if [[ ! -s "$MAPPING_FILE" ]]; then
    echo "Файл '$MAPPING_FILE' пуст. Нет данных для анализа."
    exit 0
fi

# --- Основная логика ---
declare -A disk_counts

while IFS= read -r line; do
    # ИСПРАВЛЕНО: Используем запятую как разделитель
    destination_path=$(echo "$line" | cut -d',' -f2)

    # Извлекаем имя диска (третий компонент пути)
    disk_name=$(echo "$destination_path" | cut -d'/' -f3)

    if [[ -n "$disk_name" ]]; then
        ((disk_counts["$disk_name"]++))
    fi
done < "$MAPPING_FILE"


# --- Вывод отчета ---
echo "-------------------------------------"
echo "📊 Статистика копирования по дискам:"
echo "-------------------------------------"

if [ ${#disk_counts[@]} -eq 0 ]; then
    echo "Не удалось извлечь данные о дисках. Проверьте формат файла."
else
    # Сортируем диски по имени для красивого вывода
    for disk in $(echo "${!disk_counts[@]}" | tr ' ' '\n' | sort); do
        count=${disk_counts[$disk]}
        printf "%-20s: %d файлов\n" "$disk" "$count"
    done
fi

echo "-------------------------------------"
total_files=$(wc -l < "$MAPPING_FILE")
echo "Всего скопировано файлов: $total_files"
echo "-------------------------------------"
