#!/bin/bash

TARGET_DIR=${1:-.}

if [ ! -d "$TARGET_DIR" ]; then
    echo "Ошибка: Директория '$TARGET_DIR' не найдена."
    exit 1
fi

echo -e "📊 Статистика для директории: \e[1;36m$(realpath "$TARGET_DIR")\e[0m\n"

# --- Этап 1: Интерактивный подсчет файлов ---
echo -e "\e[1;33mЭтап 1: Подсчет файлов (нажмите Ctrl+C для отмены)...\e[0m"

TOTAL_FILES=$(stdbuf -oL find "$TARGET_DIR" -type f -print0 | awk '
    BEGIN { RS="\0"; count=0 }
    {
        count++;
        if (count == 1 || count % 1000 == 0) {
            printf "\rНайдено: %d файлов...", count > "/dev/stderr"
        }
    }
    END {
        printf "\rНайдено: %d файлов.      \n", count > "/dev/stderr"
        print count
    }
')

echo ""

# --- Этап 2: Анализ общего размера ---
echo -e "\e[1;33mЭтап 2: Анализ общего размера...\e[0m"
TOTAL_SIZE=$(find "$TARGET_DIR" -type f -print0 | xargs -0 du -ch --apparent-size 2>/dev/null | tail -1 | awk '{print $1}')

echo ""

# --- Этап 3: Вывод результатов ---
echo -e "\e[1;33mРезультаты:\e[0m"
echo -e "  \e[1mОбщий размер:\e[0m \e[1;32m${TOTAL_SIZE}\e[0m"
echo -e "  \e[1mВсего файлов:\e[0m \e[1;32m${TOTAL_FILES}\e[0m"
echo ""

echo -e "\e[1;33mТоп 10 самых больших подкаталогов:\e[0m"
du -h --max-depth=1 "$TARGET_DIR" 2>/dev/null | sort -rh | head -n 11 | tail -n +2 | awk '{printf "  \e[32m%-10s\e[0m %s\n", $1, $2}'
