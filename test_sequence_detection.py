#!/usr/bin/env python3
import re
from collections import defaultdict

# Тестовые данные как в реальной проблеме
test_files_bad = [
    ('5M5A4205.jpg', 1000),
    ('5M5A4207.jpg', 1000), 
    ('5M5A4208.jpg', 1000),
    ('5M5A4209.jpg', 1000),
    ('5M5A4210.jpg', 1000),
    ('5M5A4213.jpg', 1000),
    ('5M5A4215.jpg', 1000),
    ('5M5A4216.jpg', 1000),
    ('5M5A4218.jpg', 1000),
    ('5M5A4222.jpg', 1000),
]

# Хорошая секвенция (непрерывная)
test_files_good = [
    ('frame0001.dpx', 2000),
    ('frame0002.dpx', 2000),
    ('frame0003.dpx', 2000),
    ('frame0004.dpx', 2000),
    ('frame0005.dpx', 2000),
    ('frame0006.dpx', 2000),
    ('frame0007.dpx', 2000),
    ('frame0008.dpx', 2000),
    ('frame0009.dpx', 2000),
    ('frame0010.dpx', 2000),
]

print("=== ТЕСТ 1: ПЛОХАЯ СЕКВЕНЦИЯ (много пропусков) ===")
test_files = test_files_bad

SEQUENCE_RE = re.compile(r'^(.*?)[\._]*(\d+)\.([a-zA-Z0-9]+)$', re.IGNORECASE)

sequences_in_dir = defaultdict(list)
for filename, file_size in test_files:
    match = SEQUENCE_RE.match(filename)
    if match:
        prefix, frame, ext = match.groups()
        sequences_in_dir[(prefix, ext.lower())].append((int(frame), filename, file_size))

for (prefix, ext), file_tuples in sequences_in_dir.items():
    print(f'Анализируем последовательность: {prefix}.{ext}')
    file_tuples.sort()
    frames, full_paths, sizes = zip(*file_tuples)
    
    min_frame, max_frame = min(frames), max(frames)
    expected_frames = max_frame - min_frame + 1
    actual_frames = len(frames)
    
    max_allowed_gaps = max(1, int(expected_frames * 0.05))
    missing_frames = expected_frames - actual_frames
    
    print(f'  Кадры: {min_frame}-{max_frame}')
    print(f'  Ожидается кадров: {expected_frames}')
    print(f'  Фактических кадров: {actual_frames}')
    print(f'  Пропущено кадров: {missing_frames}')
    print(f'  Максимум допустимых пропусков: {max_allowed_gaps}')
    print(f'  Является секвенцией: {missing_frames <= max_allowed_gaps}')
    print()

print("=== ТЕСТ 2: ХОРОШАЯ СЕКВЕНЦИЯ (непрерывная) ===")
test_files = test_files_good

sequences_in_dir = defaultdict(list)
for filename, file_size in test_files:
    match = SEQUENCE_RE.match(filename)
    if match:
        prefix, frame, ext = match.groups()
        sequences_in_dir[(prefix, ext.lower())].append((int(frame), filename, file_size))

for (prefix, ext), file_tuples in sequences_in_dir.items():
    print(f'Анализируем последовательность: {prefix}.{ext}')
    file_tuples.sort()
    frames, full_paths, sizes = zip(*file_tuples)
    
    min_frame, max_frame = min(frames), max(frames)
    expected_frames = max_frame - min_frame + 1
    actual_frames = len(frames)
    
    max_allowed_gaps = max(1, int(expected_frames * 0.05))
    missing_frames = expected_frames - actual_frames
    
    print(f'  Кадры: {min_frame}-{max_frame}')
    print(f'  Ожидается кадров: {expected_frames}')
    print(f'  Фактических кадров: {actual_frames}')
    print(f'  Пропущено кадров: {missing_frames}')
    print(f'  Максимум допустимых пропусков: {max_allowed_gaps}')
    print(f'  Является секвенцией: {missing_frames <= max_allowed_gaps}')
    print()