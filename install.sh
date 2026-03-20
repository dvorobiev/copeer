#!/usr/bin/env bash
# Установка copeer на сервер.
# Использование: bash install.sh
#
# Что делает:
#   1. Проверяет Python 3.10+
#   2. Устанавливает зависимости (pip)
#   3. Делает скрипты исполняемыми
#   4. Показывает команды для запуска

set -euo pipefail

COPEER_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

echo "=== Установка copeer ==="
echo "Каталог: $COPEER_DIR"
echo

# 1. Проверка Python
PYTHON=$(command -v python3 || command -v python || true)
if [[ -z "$PYTHON" ]]; then
    echo "Ошибка: python3 не найден" >&2
    exit 1
fi

PY_VERSION=$("$PYTHON" -c 'import sys; print(f"{sys.version_info.major}.{sys.version_info.minor}")')
PY_MAJOR=$("$PYTHON" -c 'import sys; print(sys.version_info.major)')
PY_MINOR=$("$PYTHON" -c 'import sys; print(sys.version_info.minor)')

if [[ "$PY_MAJOR" -lt 3 || ("$PY_MAJOR" -eq 3 && "$PY_MINOR" -lt 10) ]]; then
    echo "Ошибка: нужен Python 3.10+, найден $PY_VERSION" >&2
    exit 1
fi

echo "  ✓ Python $PY_VERSION ($PYTHON)"

# 2. Установка зависимостей
echo "  Устанавливаю зависимости..."
install_deps() {
    if "$PYTHON" -m pip install --quiet -r "$COPEER_DIR/requirements.txt" 2>/dev/null; then
        echo "  ✓ Зависимости установлены (pip)"
    elif "$PYTHON" -m pip install --quiet --break-system-packages \
         -r "$COPEER_DIR/requirements.txt" 2>/dev/null; then
        echo "  ✓ Зависимости установлены (pip --break-system-packages)"
    else
        echo "  pip не найден, устанавливаю через apt..."
        apt-get install -y python3-rich python3-yaml 2>/dev/null || \
        sudo apt-get install -y python3-rich python3-yaml
        echo "  ✓ Зависимости установлены (apt)"
    fi
}
install_deps

# 3. Права на запуск
chmod +x "$COPEER_DIR/copeer.py"
chmod +x "$COPEER_DIR/scripts/compare_disk_raidix.py"
chmod +x "$COPEER_DIR/scripts/make_raidix_copy_list.py"
echo "  ✓ Права на скрипты установлены"

echo
echo "=== Установка завершена ==="
echo

echo "Дальнейшие шаги:"
echo
echo "  # Шаг 1: проверить покрытие диска A1 на RAIDIX"
echo "  python $COPEER_DIR/scripts/compare_disk_raidix.py \\"
echo "    --disk A1 \\"
echo "    --disks-dir /path/to/disks_files"
echo
echo "  # Шаг 2: сгенерировать список файлов для копирования"
echo "  python $COPEER_DIR/scripts/make_raidix_copy_list.py \\"
echo "    --disk A1 \\"
echo "    --disks-dir /path/to/disks_files \\"
echo "    --output $COPEER_DIR/output/copy_from_raidix_A1.csv"
echo
echo "  # Шаг 3: запустить копирование"
echo "  python $COPEER_DIR/copeer.py -i $COPEER_DIR/output/copy_from_raidix_A1.csv"
echo
echo "  # Режим докопирования (если на диске уже часть данных):"
echo "  python $COPEER_DIR/copeer.py -i $COPEER_DIR/output/copy_from_raidix_A1.csv \\"
echo "    --single-dest /mnt/disk_A1 \\"
echo "    --skip-existing"
echo
echo "  Замени /path/to/disks_files на каталог с CSV-листингами дисков (*_output_file.csv)"
