#!/bin/bash

export LC_ALL=C.UTF-8

PROJECT_DIR="/home/wasserman/copeer/scripts/disk_by_wwn"
VENV_PYTHON="$PROJECT_DIR/venv/bin/python3"
MAIN_SCRIPT="$PROJECT_DIR/disk_state.py"

echo "=== ДИАГНОСТИКА ЗАПУСКА ==="
echo "Пользователь: $(whoami)"
echo "Группы: $(groups)"
echo "Права на ttyUSB0: $(ls -l /dev/ttyUSB0 2>/dev/null || echo 'НЕ НАЙДЕН')"
echo "=========================="

# Проверяем что устройство существует
if [ ! -e "/dev/ttyUSB0" ]; then
    echo "ОШИБКА: /dev/ttyUSB0 не существует!"
    echo "Подключенные USB устройства:"
    ls -l /dev/ttyUSB* 2>/dev/null || echo "Нет USB serial устройств"
    exit 1
fi

# Запускаем Python скрипт БЕЗ sudo -i, но с правами root
cd "$PROJECT_DIR" && sudo "$VENV_PYTHON" "$MAIN_SCRIPT"
