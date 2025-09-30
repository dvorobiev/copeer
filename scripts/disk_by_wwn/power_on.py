import os
import time

SERIAL_PORT = "/dev/ttyUSB0"  # укажи нужный порт
MODULE = 1
POSITION = 2

command_str = f"#hdd_m{MODULE} n{POSITION} on\r\n"

print(f"Попытка отправить команду включения: {command_str.strip()} -> {SERIAL_PORT}")

if os.geteuid() != 0:
    print("Этот скрипт требует запуска от root. Попробуй: sudo python3 test_serial_power_on.py")
    exit(1)

try:
    with open(SERIAL_PORT, 'wb') as serial_port:
        serial_port.write(command_str.encode('utf-8'))
        serial_port.flush()
    print("Команда отправлена успешно.")
except Exception as e:
    print(f"Ошибка при отправке команды: {e}")
