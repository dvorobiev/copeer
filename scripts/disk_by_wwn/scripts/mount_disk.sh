#!/bin/bash

# Проверяем, что передано два аргумента
if [ "$#" -ne 2 ]; then
    echo "Ошибка: Требуется 2 аргумента: путь к устройству и точка монтирования" >&2
    exit 1
fi

DEVICE_PATH=$1
MOUNT_POINT=$2

# echo "Монтирование ${DEVICE_PATH} в ${MOUNT_POINT}..."

# Ваша команда монтирования для XFS
# Явно указываем тип файловой системы (-t xfs) для надежности
mount -t xfs -o noatime,nodiratime,logbufs=8,logbsize=256k,largeio,inode64,swalloc,allocsize=131072k "${DEVICE_PATH}" "${MOUNT_POINT}"

# Проверяем код возврата последней команды
if [ $? -ne 0 ]; then
    echo "Ошибка монтирования!" >&2
    # Выводим последние сообщения ядра, это может быть полезно
    dmesg | tail -n 10
    exit 1
fi

# echo "Монтирование успешно завершено."
exit 0
