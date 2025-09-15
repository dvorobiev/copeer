# copeer_lite.py
# Версия без интерактивного TUI. Простой, надежный и последовательный вывод.

import argparse
import csv
import logging
import os
import re
import subprocess
import sys
import tarfile
import time
import yaml
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from threading import Lock, get_ident

# --- Настройка логирования ---
# Простой формат для вывода в консоль
logging.basicConfig(level="INFO", format="[%(asctime)s] %(message)s", datefmt="%H:%M:%S")
log = logging.getLogger(__name__)


# --- Глобальные переменные и константы ---
__version__ = "1.0.0-lite"
CONFIG_FILE = "config.yaml"
DEFAULT_CONFIG = {
    'mount_points': ["/mnt/disk1", "/mnt/disk2"],
    'source_root': '/path/to/source/data',
    'destination_root': '/',
    'threshold': 98.0,
    'state_file': "copier_state.csv",
    'mapping_file': "mapping.csv",
    'error_log_file': "errors.log",
    'dry_run_mapping_file': "dry_run_mapping.csv",
    'dry_run': False,
    'threads': 8,
    'min_files_for_sequence': 50,
    'image_extensions': ['dpx', 'cri', 'tiff', 'tif', 'exr', 'png', 'jpg', 'jpeg', 'tga', 'j2c'],
}
SEQUENCE_RE = re.compile(r'^(.*?)[\._]*(\d+)\.([a-zA-Z0-9]+)$', re.IGNORECASE)
file_lock = Lock()


# --- Основные классы и функции ---

class DiskManager:
    """Управляет выбором диска для записи, чтобы не превышать порог заполнения."""
    def __init__(self, mount_points, threshold):
        self.mount_points = mount_points
        self.threshold = threshold
        self.active_disk = None
        self.lock = Lock()
        self._select_initial_disk()

    def _get_disk_usage(self, path):
        if not os.path.exists(path): return 0.0
        try:
            st = os.statvfs(path)
            used = (st.f_blocks - st.f_bfree) * st.f_frsize
            total = st.f_blocks * st.f_frsize
            return round(used / total * 100, 2) if total > 0 else 0
        except FileNotFoundError: return 100

    def _select_initial_disk(self):
        for mount in self.mount_points:
            if not os.path.exists(mount):
                log.warning(f"Точка монтирования {mount} не существует. Пропускаю.")
                continue
            if self._get_disk_usage(mount) < self.threshold:
                self.active_disk = mount
                log.info(f"Выбран начальный диск: {self.active_disk}")
                return
        log.error("🛑 Не найдено подходящих дисков для начала работы.")
        raise RuntimeError("Не найдено подходящих дисков")

    def get_current_destination(self):
        with self.lock:
            if not self.active_disk: raise RuntimeError("🛑 Нет доступных дисков.")
            if self._get_disk_usage(self.active_disk) >= self.threshold:
                log.warning(f"Диск {self.active_disk} заполнен. Ищу следующий...")
                available_disks = [m for m in self.mount_points if os.path.exists(m)]
                try:
                    current_index = available_disks.index(self.active_disk)
                    next_disks = available_disks[current_index + 1:] + available_disks[:current_index]
                except (ValueError, IndexError):
                    next_disks = available_disks
                self.active_disk = next((m for m in next_disks if self._get_disk_usage(m) < self.threshold), None)
                if self.active_disk:
                    log.info(f"Переключился на диск: {self.active_disk}")
            if not self.active_disk:
                raise RuntimeError("🛑 Нет доступных дисков: все переполнены или недоступны.")
            return self.active_disk

def load_config():
    """Загружает конфигурацию из YAML файла или создает его по умолчанию."""
    config = DEFAULT_CONFIG.copy()
    if os.path.exists(CONFIG_FILE):
        try:
            with open(CONFIG_FILE, 'r', encoding='utf-8') as f:
                config.update(yaml.safe_load(f) or {})
        except Exception as e:
            log.error(f"Ошибка чтения {CONFIG_FILE}: {e}. Использованы значения по умолчанию.")
    else:
        with open(CONFIG_FILE, 'w', encoding='utf-8') as f:
            yaml.dump(DEFAULT_CONFIG, f, sort_keys=False, allow_unicode=True)
        log.info(f"Создан файл конфигурации по умолчанию: {CONFIG_FILE}")
    config['image_extensions'] = set(e.lower() for e in config.get('image_extensions', []))
    return config

def load_previous_state(state_file):
    """Загружает ключи уже обработанных элементов для возобновления работы."""
    processed = set()
    if os.path.exists(state_file):
        try:
            with open(state_file, 'r', encoding='utf-8') as f:
                processed.update(row[0] for row in csv.reader(f) if row)
            log.info(f"Загружено {len(processed)} записей из файла состояния.")
        except Exception as e:
            log.error(f"Не удалось прочитать файл состояния {state_file}: {e}")
    return processed

def write_log(state_file, mapping_file, key, dest_path, is_dry_run):
    """Потокобезопасная запись в лог-файлы."""
    with file_lock:
        if not is_dry_run:
            with open(state_file, "a", newline='', encoding='utf-8') as f:
                csv.writer(f).writerow([key])
        if dest_path:
            with open(mapping_file, "a", newline='', encoding='utf-8') as f:
                csv.writer(f).writerow([key, dest_path])

def find_sequences(dirs, config):
    """Находит все последовательности в сгруппированных по каталогам файлах."""
    all_sequences, sequence_files = [], set()
    for dir_path, files_with_sizes in dirs.items():
        sequences_in_dir = defaultdict(list)
        for filename, file_size in files_with_sizes:
            match = SEQUENCE_RE.match(filename)
            if match and match.group(3).lower() in config.get('image_extensions', set()):
                prefix, frame, ext = match.groups()
                full_path = os.path.join(dir_path, filename)
                sequences_in_dir[(prefix, ext.lower())].append((int(frame), full_path, file_size))
        for (prefix, ext), file_tuples in sequences_in_dir.items():
            if len(file_tuples) >= config.get('min_files_for_sequence', 50):
                file_tuples.sort()
                frames, full_paths, sizes = zip(*file_tuples)
                
                # Проверяем непрерывность последовательности
                min_frame, max_frame = min(frames), max(frames)
                expected_frames = max_frame - min_frame + 1
                actual_frames = len(frames)
                
                # Разрешаем небольшие пропуски (например, до 5% от общего количества кадров)
                max_allowed_gaps = max(1, int(expected_frames * 0.05))  # Максимум 5% пропусков
                missing_frames = expected_frames - actual_frames
                
                if missing_frames <= max_allowed_gaps:
                    # Это действительно секвенция с минимальными пропусками
                    safe_prefix = re.sub(r'[^\w\.\-]', '_', prefix.strip())
                    tar_filename = f"{safe_prefix}.{min_frame:04d}-{max_frame:04d}.{ext}.tar"
                    virtual_tar_path = os.path.join(dir_path, tar_filename)
                    all_sequences.append({
                        'type': 'sequence', 
                        'key': virtual_tar_path, 
                        'dir_path': dir_path, 
                        'tar_filename': tar_filename, 
                        'source_files': list(full_paths), 
                        'size': sum(sizes),
                        'frame_info': {
                            'min_frame': min_frame,
                            'max_frame': max_frame,
                            'expected_frames': expected_frames,
                            'actual_frames': actual_frames,
                            'missing_frames': missing_frames
                        }
                    })
                    sequence_files.update(full_paths)
                else:
                    # Слишком много пропусков - не считаем секвенцией
                    log.warning(f"Пропущена потенциальная секвенция {prefix} в {dir_path}: "
                              f"слишком много пропусков ({missing_frames} из {expected_frames} кадров, "
                              f"допустимо максимум {max_allowed_gaps})")
    return all_sequences, sequence_files

def archive_sequence_to_destination(job, dest_tar_path):
    """Создает tar-архив из файлов секвенции."""
    os.makedirs(os.path.dirname(dest_tar_path), exist_ok=True)
    with tarfile.open(dest_tar_path, "w") as tar:
        for file_path in job['source_files']:
            if os.path.exists(file_path):
                tar.add(file_path, arcname=os.path.basename(file_path))
            else:
                log.warning(f"В секвенции не найден файл: {file_path}")

# --- Логика анализа и выполнения ---

def analyze_and_plan_jobs(input_csv_path, config, processed_items_keys):
    """Анализирует CSV и формирует план работ."""
    log.info("--- Шаг 1: Анализ и планирование ---")
    log.info(f"Анализ файла: {input_csv_path}")

    parser_primary = re.compile(r'^"([^"]+)","([^"]+)",.*')
    parser_fallback = re.compile(r'^"([^"]+\.\w{2,5})",.*', re.IGNORECASE)

    dirs, all_files_from_csv = defaultdict(list), {}
    source_root = config.get('source_root')
    if source_root:
        log.info(f"Используется корень источника: {source_root}")

    lines_total, lines_ignored_dirs, malformed_lines = 0, 0, []

    with open(input_csv_path, 'r', encoding='utf-8', errors='ignore') as f:
        for line in f:
            lines_total += 1
            cleaned_line = line.strip().replace('""', '"')
            if not cleaned_line: continue

            rel_path, file_type, size = None, "", 0
            match = parser_primary.match(cleaned_line)
            if match:
                rel_path, file_type = match.groups()
            else:
                match = parser_fallback.match(cleaned_line)
                if match: rel_path, file_type = match.group(1), "file"

            if rel_path:
                if 'directory' in file_type:
                    lines_ignored_dirs += 1
                    continue

                size_match = re.search(r',"(\d+)"$', cleaned_line)
                if size_match:
                    try: size = int(size_match.group(1))
                    except (ValueError, IndexError): size = 0

                absolute_source_path = os.path.normpath(os.path.join(source_root, rel_path) if source_root else rel_path)
                path_obj = Path(absolute_source_path)
                dirs[str(path_obj.parent)].append((path_obj.name, size))
                all_files_from_csv[absolute_source_path] = size
            else:
                malformed_lines.append((lines_total, line))

    sequences, sequence_files = find_sequences(dirs, config)
    standalone_files = set(all_files_from_csv.keys()) - sequence_files

    jobs = sequences + [{'type': 'file', 'key': f, 'size': all_files_from_csv[f]} for f in standalone_files]
    jobs_to_process = [job for job in jobs if job['key'] not in processed_items_keys]
    jobs_to_process.sort(key=lambda j: j.get('size', 0), reverse=True)

    # --- Вывод текстового отчета ---
    print("\n--- Отчет по анализу ---")
    print(f"Всего строк в CSV файле:             {lines_total:,}")
    print(f"  Пропущено (директории):          {lines_ignored_dirs:,}")
    print(f"  Пропущено (неопознанный формат): {len(malformed_lines):,}")
    print(f"Найдено файлов для обработки:        {len(all_files_from_csv):,}")
    print("-" * 20)
    print(f"Заданий на архивацию:                {len(sequences):,}")
    print(f"Заданий на копирование:              {len(standalone_files):,}")
    print("-" * 20)
    print(f"Всего заданий к выполнению:          {len(jobs_to_process):,}")
    print(f"  Пропущено (уже выполнены):       {len(jobs) - len(jobs_to_process):,}")
    print("-" * 20)
    if malformed_lines:
        print("\n[ВНИМАНИЕ] Найдены некорректные строки (первые 10):")
        for num, line in malformed_lines[:10]:
            print(f"  Строка #{num}: {line}")

    return jobs_to_process

def process_job_worker(job, config, disk_manager):
    """Обрабатывает одно задание, логируя начало и конец."""
    thread_id = get_ident()
    is_dry_run = config['dry_run']
    short_name = job.get('tar_filename') or os.path.basename(job['key'])
    op_type = "Архивация" if job['type'] == 'sequence' else "Копирование"

    log.info(f"[Поток {thread_id}] Начало: {op_type} -> {short_name}")

    try:
        dest_mount_point = disk_manager.get_current_destination()
        source_root = config.get('source_root')
        destination_root = config.get('destination_root', '/')
        absolute_source_key = job['key']

        if source_root and absolute_source_key.startswith(os.path.normpath(source_root) + os.sep):
            rel_path = os.path.relpath(absolute_source_key, source_root)
        else:
            rel_path = absolute_source_key.lstrip(os.path.sep)

        dest_path = os.path.normpath(os.path.join(dest_mount_point, destination_root.lstrip(os.path.sep), rel_path))

        source_keys_to_log = []
        if job['type'] == 'sequence':
            if not is_dry_run:
                archive_sequence_to_destination(job, dest_path)
            else:
                time.sleep(0.05)
            source_keys_to_log = job['source_files']
        else: # 'file'
            source_keys_to_log = [absolute_source_key]
            if not is_dry_run:
                os.makedirs(os.path.dirname(dest_path), exist_ok=True)
                rsync_cmd = ["rsync", "-a", "--checksum", absolute_source_key, dest_path]
                subprocess.run(rsync_cmd, check=True, stdout=subprocess.DEVNULL, stderr=subprocess.PIPE)
            else:
                time.sleep(0.05)

        log.info(f"[Поток {thread_id}] Успех: {short_name}")
        return (job['type'], job['size'], source_keys_to_log, dest_path)

    except Exception as e:
        log.error(f"[Поток {thread_id}] ОШИБКА при обработке {short_name}: {e}")
        with file_lock:
             with open(config['error_log_file'], "a", encoding='utf-8') as f:
                f.write(f"{time.asctime()};{job['key']};{e}\n")
        return (None, 0, None, None)

# --- Точка входа ---

def main(args):
    """Главная функция скрипта."""
    config = load_config()
    if args.dry_run:
        config['dry_run'] = True

    log.info(f"--- Copeer v{__version__} ---")
    log.info(f"Режим: {'Dry Run' if config['dry_run'] else 'Реальная работа'}")

    is_dry_run = config['dry_run']
    if is_dry_run:
        dry_run_log_path = config['dry_run_mapping_file']
        try:
            with open(dry_run_log_path, 'w', newline='', encoding='utf-8') as f:
                csv.writer(f).writerow(["source_path", "destination_path"])
            log.info(f"Отчет dry-run будет сохранен в: {dry_run_log_path}")
        except IOError as e:
            log.error(f"Не удалось создать файл отчета для dry-run: {e}")

    processed_items_keys = load_previous_state(config['state_file'])
    jobs_to_process = analyze_and_plan_jobs(args.input_file, config, processed_items_keys)
    if not jobs_to_process:
        log.info("Все задания уже выполнены. Завершение.")
        return

    try:
        input("\nНажмите Enter для начала выполнения или Ctrl+C для отмены...")
    except KeyboardInterrupt:
        log.warning("\nВыполнение отменено пользователем.")
        return

    disk_manager = DiskManager(config['mount_points'], config['threshold'])

    log.info(f"--- Шаг 2: Выполнение {len(jobs_to_process)} заданий ---")

    jobs_completed = 0
    total_jobs = len(jobs_to_process)

    with ThreadPoolExecutor(max_workers=config['threads']) as executor:
        future_to_job = {executor.submit(process_job_worker, job, config, disk_manager): job for job in jobs_to_process}

        for future in as_completed(future_to_job):
            job_type, _, source_keys, dest_path = future.result()

            jobs_completed += 1
            log.info(f"Прогресс: {jobs_completed} / {total_jobs} заданий выполнено.")

            if job_type:
                # Запись в лог-файлы
                for key in source_keys:
                    write_log(
                        config['state_file'],
                        config['dry_run_mapping_file'] if is_dry_run else config['mapping_file'],
                        key, dest_path, is_dry_run
                    )

    log.info("--- Все задания обработаны ---")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Анализирует CSV, архивирует и копирует файлы в хранилище. Lite-версия без TUI.",
        prog="copeer_lite"
    )
    parser.add_argument(
        "-v", "--version",
        action="version",
        version=f"%(prog)s v{__version__}"
    )
    parser.add_argument("input_file", help="Путь к CSV файлу со списком исходных файлов.")
    parser.add_argument("--dry-run", action="store_true", help="Выполнить анализ без реального копирования.")
    args = parser.parse_args()
    main(args)
