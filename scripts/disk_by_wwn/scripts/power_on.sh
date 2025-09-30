#!/bin/bash -x

# Проверяем, что передано два аргумента
if [ "$#" -ne 2 ]; then
    echo "Ошибка: Требуется 2 аргумента: номер модуля и номер позиции" >&2
    exit 1
fi

MODULE=$1
POSITION=$2
SERIAL_PORT="/dev/ttyUSB0"

# Формируем и выполняем команду
echo "Включение диска: Модуль=${MODULE}, Позиция=${POSITION}"
echo -ne "#hdd_m${MODULE} n${POSITION} on\n\r" > ${SERIAL_PORT}
