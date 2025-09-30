#!/usr/bin/env bash

# Цвета через tput
COLOR_GREEN=$(tput setaf 2)
COLOR_YELLOW=$(tput setaf 3)
COLOR_CYAN=$(tput setaf 6)
COLOR_MAGENTA=$(tput setaf 5)
COLOR_RESET=$(tput sgr0)

# Проверка аргумента
if [[ -z "$1" ]]; then
  printf "%sUsage:%s %s <mapping.csv>\n" "$COLOR_YELLOW" "$COLOR_RESET" "$0"
  exit 1
fi

INPUT="$1"
DIR="$(dirname "$INPUT")"
COPIER_STATE="$DIR/copier_state.csv"

# Общее число файлов
TOTAL=$(wc -l < "$INPUT")
# Число строк в copier_state.csv
if [[ -f "$COPIER_STATE" ]]; then
  COPIER_LINES=$(wc -l < "$COPIER_STATE")
else
  COPIER_LINES=0
fi

printf "\n%sОбщее число файлов в %s%s%s: %s%d%s\n" \
  "$COLOR_CYAN" "$COLOR_RESET" "$INPUT" "$COLOR_CYAN" "$COLOR_GREEN" "$TOTAL" "$COLOR_RESET"
printf "%sЧисло строк в %s%s%s: %s%d%s\n\n" \
  "$COLOR_CYAN" "$COLOR_RESET" "copier_state.csv" "$COLOR_CYAN" "$COLOR_GREEN" "$COPIER_LINES" "$COLOR_RESET"

# Статистика по mount point
printf "%sСтатистика по файлам на mount point:%s\n" "$COLOR_YELLOW" "$COLOR_RESET"
cut -d',' -f2 "$INPUT" \
  | tr -d '"' \
  | grep '^/mnt/disk_' \
  | cut -d'/' -f1-3 \
  | sed 's|$|/|' \
  | sort \
  | uniq -c \
  | sort -nr \
  | while read -r count mp; do
      printf "  %s%s%s %s%d%s\n" \
        "$COLOR_GREEN" "$mp" "$COLOR_RESET" "$COLOR_GREEN" "$count" "$COLOR_RESET"
    done

# Три уровня для /mnt/cifs/raidix/
printf "\n%sУникальные каталоги (3 уровня) внутри /mnt/cifs/raidix/: %s\n" "$COLOR_YELLOW" "$COLOR_RESET"
cut -d',' -f1 "$INPUT" \
  | tr -d '"' \
  | grep '^/mnt/cifs/raidix/' \
  | sed 's|^/mnt/cifs/raidix/||' \
  | awk -F'/' '{ print $1"/"$2"/"$3 }' \
  | sort -u \
  | while read -r dir; do
      printf "  %s%s%s\n" "$COLOR_MAGENTA" "$dir" "$COLOR_RESET"
    done

# Четыре уровня для /mnt/disk_*
printf "\n%sУникальные каталоги (4 уровня) внутри /mnt/disk_*: %s\n" "$COLOR_YELLOW" "$COLOR_RESET"
cut -d',' -f2 "$INPUT" \
  | tr -d '"' \
  | grep '^/mnt/disk_' \
  | sed -E 's|^(/mnt/disk_[^/]+/)||' \
  | awk -F'/' '{ print $1"/"$2"/"$3"/"$4 }' \
  | sort -u \
  | while read -r dir; do
      printf "  %s%s%s\n" "$COLOR_MAGENTA" "$dir" "$COLOR_RESET"
    done
