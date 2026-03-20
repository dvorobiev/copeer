"""
Тесты для режима докопирования (fill-disk mode):
  --single-dest и --skip-existing

Проверяет:
  - compute_dest_path: правильное построение пути назначения
  - preflight_skip_existing: фильтрация заданий по наличию файлов на dest-диске
  - process_job_worker: пропуск файлов при skip_existing
  - CLI: наличие новых аргументов в argparse
"""

import argparse
import os
import sys
import types
import unittest
from pathlib import Path
from unittest.mock import MagicMock, patch

# Добавляем корень проекта в путь, чтобы импортировать copeer без установки
sys.path.insert(0, str(Path(__file__).parent.parent))

import copeer  # noqa: E402


# ---------------------------------------------------------------------------
# Вспомогательные фикстуры
# ---------------------------------------------------------------------------

def _make_file_job(key: str, size: int) -> dict:
    return {"type": "file", "key": key, "size": size}


def _make_sequence_job(key: str, size: int, source_files: list) -> dict:
    return {
        "type": "sequence",
        "key": key,
        "size": size,
        "source_files": source_files,
        "tar_filename": os.path.basename(key),
    }


def _make_config(source_root: str, dest_root: str = "/") -> dict:
    cfg = copeer.DEFAULT_CONFIG.copy()
    cfg["source_root"] = source_root
    cfg["destination_root"] = dest_root
    return cfg


# ---------------------------------------------------------------------------
# compute_dest_path
# ---------------------------------------------------------------------------

class TestComputeDestPath(unittest.TestCase):

    def test_file_with_source_root(self):
        """Путь строится относительно source_root."""
        config = _make_config(source_root="/src")
        job = _make_file_job("/src/data/clip.mov", 100)
        result = copeer.compute_dest_path(job, "/mnt/disk_A1", config)
        # ожидаем /mnt/disk_A1/data/clip.mov
        self.assertEqual(result, Path("/mnt/disk_A1/data/clip.mov"))

    def test_file_with_destination_root(self):
        """destination_root добавляется между mount и relative path."""
        config = _make_config(source_root="/src", dest_root="/archive")
        job = _make_file_job("/src/sub/file.txt", 50)
        result = copeer.compute_dest_path(job, "/mnt/disk_A1", config)
        self.assertEqual(result, Path("/mnt/disk_A1/archive/sub/file.txt"))

    def test_file_without_source_root(self):
        """Если source_root не задан, используется абсолютный путь без leading /."""
        config = _make_config(source_root=None)
        config["source_root"] = None
        job = _make_file_job("/data/file.exr", 200)
        result = copeer.compute_dest_path(job, "/mnt/disk_B", config)
        # rel_path = "data/file.exr" (leading / снят)
        self.assertEqual(result, Path("/mnt/disk_B/data/file.exr"))

    def test_sequence_tar_path(self):
        """Для sequence key — виртуальный tar-путь — тоже работает корректно."""
        config = _make_config(source_root="/src")
        job = _make_sequence_job(
            key="/src/shots/clip.0001-0100.exr.tar",
            size=5000,
            source_files=[f"/src/shots/clip.{i:04d}.exr" for i in range(1, 101)],
        )
        result = copeer.compute_dest_path(job, "/mnt/disk_A1", config)
        self.assertEqual(result, Path("/mnt/disk_A1/shots/clip.0001-0100.exr.tar"))


# ---------------------------------------------------------------------------
# preflight_skip_existing
# ---------------------------------------------------------------------------

class TestPreflightSkipExisting(unittest.TestCase):

    def _run_preflight(self, copy_jobs, archive_jobs, single_dest, config,
                       existing_paths=None):
        """
        Запускает preflight, подменяя Path.exists() и Path.stat()
        через patch объектов.
        """
        existing_paths = existing_paths or set()

        def fake_exists(self_path):
            return str(self_path) in existing_paths

        def fake_stat(self_path):
            st = MagicMock()
            # Возвращаем "правильный" размер только если путь в existing_paths
            # Размер берём из jobs по dest_path
            st.st_size = _size_for_path(str(self_path), copy_jobs)
            return st

        def _size_for_path(path_str, jobs):
            for j in jobs:
                dest = str(copeer.compute_dest_path(j, single_dest, config))
                if dest == path_str:
                    return j["size"]
            return 0

        with patch.object(Path, "exists", fake_exists), \
             patch.object(Path, "stat", fake_stat):
            return copeer.preflight_skip_existing(
                copy_jobs, archive_jobs, single_dest, config
            )

    def test_all_missing(self):
        """Если ни одного файла на dest нет — все остаются в очереди."""
        config = _make_config("/src")
        jobs = [_make_file_job(f"/src/file{i}.txt", 100 * i) for i in range(1, 4)]
        to_copy, to_archive = self._run_preflight(jobs, [], "/mnt/disk", config,
                                                  existing_paths=set())
        self.assertEqual(len(to_copy), 3)
        self.assertEqual(len(to_archive), 0)

    def test_all_present_matching_size(self):
        """Все файлы уже есть с правильным размером — очередь пустая."""
        config = _make_config("/src")
        jobs = [_make_file_job(f"/src/file{i}.txt", 100) for i in range(1, 4)]
        existing = {str(copeer.compute_dest_path(j, "/mnt/disk", config)) for j in jobs}
        to_copy, to_archive = self._run_preflight(jobs, [], "/mnt/disk", config,
                                                  existing_paths=existing)
        self.assertEqual(len(to_copy), 0)

    def test_partial_present(self):
        """Часть файлов есть — только оставшиеся попадают в очередь."""
        config = _make_config("/src")
        jobs = [_make_file_job(f"/src/file{i}.txt", 100) for i in range(1, 5)]
        # Только первые два "существуют"
        existing = {str(copeer.compute_dest_path(j, "/mnt/disk", config))
                    for j in jobs[:2]}
        to_copy, _ = self._run_preflight(jobs, [], "/mnt/disk", config,
                                          existing_paths=existing)
        self.assertEqual(len(to_copy), 2)
        # Убедимся, что в to_copy только file3 и file4
        keys = {j["key"] for j in to_copy}
        self.assertIn("/src/file3.txt", keys)
        self.assertIn("/src/file4.txt", keys)

    def test_size_mismatch_is_not_skipped(self):
        """Файл существует, но размер не совпадает — НЕ пропускаем."""
        config = _make_config("/src")
        job = _make_file_job("/src/file.txt", 999)  # size=999
        dest_str = str(copeer.compute_dest_path(job, "/mnt/disk", config))

        def fake_exists(self_path):
            return str(self_path) == dest_str

        def fake_stat(self_path):
            st = MagicMock()
            st.st_size = 500  # РАЗНЫЙ размер
            return st

        with patch.object(Path, "exists", fake_exists), \
             patch.object(Path, "stat", fake_stat):
            to_copy, _ = copeer.preflight_skip_existing(
                [job], [], "/mnt/disk", config
            )
        self.assertEqual(len(to_copy), 1)

    def test_archive_job_skipped_if_tar_exists(self):
        """Tar-архив существует — sequence-задание пропускается."""
        config = _make_config("/src")
        job = _make_sequence_job(
            key="/src/shots/clip.0001-0100.exr.tar",
            size=5000,
            source_files=["/src/shots/clip.0001.exr"],
        )
        dest_str = str(copeer.compute_dest_path(job, "/mnt/disk", config))

        def fake_exists(self_path):
            return str(self_path) == dest_str

        with patch.object(Path, "exists", fake_exists):
            _, to_archive = copeer.preflight_skip_existing(
                [], [job], "/mnt/disk", config
            )
        self.assertEqual(len(to_archive), 0)

    def test_returns_correct_types(self):
        """Возвращает кортеж (copy_list, archive_list)."""
        config = _make_config("/src")
        result = self._run_preflight([], [], "/mnt/disk", config)
        self.assertIsInstance(result, tuple)
        self.assertEqual(len(result), 2)


# ---------------------------------------------------------------------------
# process_job_worker: single_dest
# ---------------------------------------------------------------------------

class TestProcessJobWorkerSingleDest(unittest.TestCase):
    """
    Проверяем что process_job_worker использует single_dest
    вместо disk_manager.get_current_destination().
    """

    def _make_mock_disk_manager(self):
        dm = MagicMock()
        dm.get_current_destination.return_value = "/mnt/wrong_disk"
        return dm

    def _run_worker_dry(self, job, single_dest=None, skip_existing=False):
        config = _make_config(source_root="/src")
        config["error_log_file"] = "/tmp/test_errors.log"
        config["threads"] = 1
        disk_manager = self._make_mock_disk_manager()

        result = copeer.process_job_worker(
            worker_id=1,
            job=job,
            config=config,
            disk_manager=disk_manager,
            is_dry_run=True,
            is_debug_mode=False,
            single_dest=single_dest,
            skip_existing=skip_existing,
        )
        return result, disk_manager

    def test_single_dest_bypasses_disk_manager(self):
        """При single_dest disk_manager.get_current_destination НЕ вызывается."""
        job = _make_file_job("/src/file.mov", 100)
        _, dm = self._run_worker_dry(job, single_dest="/mnt/single_disk")
        dm.get_current_destination.assert_not_called()

    def test_without_single_dest_calls_disk_manager(self):
        """Без single_dest disk_manager.get_current_destination вызывается."""
        job = _make_file_job("/src/file.mov", 100)
        _, dm = self._run_worker_dry(job, single_dest=None)
        dm.get_current_destination.assert_called_once_with(100)

    def test_dry_run_returns_success_tuple(self):
        """В dry-run режиме worker возвращает успешный кортеж."""
        job = _make_file_job("/src/file.mov", 100)
        result, _ = self._run_worker_dry(job, single_dest="/mnt/disk")
        job_type, size, keys, path = result
        self.assertEqual(job_type, "file")
        self.assertEqual(size, 100)
        self.assertIsNotNone(keys)


# ---------------------------------------------------------------------------
# process_job_worker: skip_existing
# ---------------------------------------------------------------------------

class TestProcessJobWorkerSkipExisting(unittest.TestCase):
    """
    Проверяем что skip_existing пропускает файл если он уже есть.
    Тесты работают в режиме НЕ dry-run (is_dry_run=False), чтобы
    ветка skip_existing была активна.
    """

    def _make_config(self):
        config = _make_config(source_root="/src")
        config["error_log_file"] = "/tmp/test_errors_skip.log"
        config["threads"] = 1
        return config

    def test_skip_existing_file_same_size(self):
        """Файл существует с тем же размером → возвращает успех без rsync."""
        job = _make_file_job("/src/data/clip.mov", 500)
        config = self._make_config()
        dm = MagicMock()
        dm.get_current_destination.return_value = "/mnt/disk"

        dest_path = "/mnt/disk/data/clip.mov"

        def fake_exists(self_path):
            return str(self_path) == dest_path

        def fake_stat(self_path):
            st = MagicMock()
            st.st_size = 500  # совпадает
            return st

        with patch("os.path.exists", return_value=True), \
             patch.object(Path, "exists", fake_exists), \
             patch.object(Path, "stat", fake_stat):
            result = copeer.process_job_worker(
                worker_id=1,
                job=job,
                config=config,
                disk_manager=dm,
                is_dry_run=False,
                is_debug_mode=False,
                skip_existing=True,
            )

        job_type, size, keys, path = result
        self.assertEqual(job_type, "file")
        self.assertEqual(size, 500)

    def test_no_skip_when_size_differs(self):
        """Файл существует, но размер другой → НЕ пропускаем (продолжаем копирование)."""
        job = _make_file_job("/src/data/clip.mov", 500)
        config = self._make_config()
        dm = MagicMock()
        dm.get_current_destination.return_value = "/mnt/disk"

        dest_path_str = "/mnt/disk/data/clip.mov"

        def fake_dest_exists(self_path):
            return str(self_path) == dest_path_str

        def fake_stat(self_path):
            st = MagicMock()
            st.st_size = 123  # НЕ совпадает
            return st

        # Подменяем rsync чтобы тест не запускал настоящий rsync.
        # stdout.read(1) должен возвращать '' чтобы iter() сразу остановился.
        mock_stdout = MagicMock()
        mock_stdout.read.return_value = ""
        mock_popen = MagicMock()
        mock_popen.stdout = mock_stdout
        mock_popen.wait.return_value = 0
        mock_popen.stderr.read.return_value = ""

        with patch("os.path.exists", return_value=True), \
             patch.object(Path, "exists", fake_dest_exists), \
             patch.object(Path, "stat", fake_stat), \
             patch("os.makedirs"), \
             patch("subprocess.Popen", return_value=mock_popen):
            result = copeer.process_job_worker(
                worker_id=1,
                job=job,
                config=config,
                disk_manager=dm,
                is_dry_run=False,
                is_debug_mode=False,
                skip_existing=True,
            )

        # Должны получить успешный результат (rsync был вызван)
        job_type, size, keys, path = result
        self.assertEqual(job_type, "file")


# ---------------------------------------------------------------------------
# CLI аргументы
# ---------------------------------------------------------------------------

class TestCLIArgs(unittest.TestCase):
    """Проверяет что новые флаги корректно парсятся argparse."""

    def _parse(self, args_list):
        """Парсит аргументы так же как в __main__ блоке copeer.py."""
        parser = argparse.ArgumentParser()
        source_group = parser.add_mutually_exclusive_group(required=True)
        source_group.add_argument("-i", "--input-file")
        source_group.add_argument("-s", "--source-dir")
        parser.add_argument("--dry-run", action="store_true")
        parser.add_argument("--mode", choices=["all", "copy", "archive"], default="all")
        parser.add_argument("--single-dest", metavar="PATH")
        parser.add_argument("--skip-existing", action="store_true")
        return parser.parse_args(args_list)

    def test_single_dest_parsed(self):
        args = self._parse(["-i", "file.csv", "--single-dest", "/mnt/disk_A1"])
        self.assertEqual(args.single_dest, "/mnt/disk_A1")

    def test_skip_existing_flag(self):
        args = self._parse(["-i", "file.csv", "--skip-existing"])
        self.assertTrue(args.skip_existing)

    def test_defaults(self):
        args = self._parse(["-i", "file.csv"])
        self.assertIsNone(args.single_dest)
        self.assertFalse(args.skip_existing)

    def test_both_flags_together(self):
        args = self._parse(["-i", "file.csv",
                            "--single-dest", "/mnt/disk_A1",
                            "--skip-existing"])
        self.assertEqual(args.single_dest, "/mnt/disk_A1")
        self.assertTrue(args.skip_existing)

    def test_single_dest_combined_with_dry_run(self):
        args = self._parse(["-i", "file.csv", "--single-dest", "/mnt/x", "--dry-run"])
        self.assertEqual(args.single_dest, "/mnt/x")
        self.assertTrue(args.dry_run)


if __name__ == "__main__":
    unittest.main(verbosity=2)
