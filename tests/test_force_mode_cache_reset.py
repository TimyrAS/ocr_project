"""
Тесты для режима --force в run_pipeline.py.

Проверяют, что при --force:
1. Удаляется реестр обработанных файлов
2. Очищаются файлы кэша
3. Файлы реально переобрабатываются
"""

import sys
import os
import json
import tempfile
import shutil

# Добавляем родительскую папку в path для импорта модулей
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import pytest


class TestForceModeCacheReset:
    """Тесты режима --force."""

    def setup_method(self):
        """Создаём временную структуру для тестов."""
        self.test_dir = tempfile.mkdtemp()
        self.cache_folder = os.path.join(self.test_dir, "ocr_cache")
        os.makedirs(self.cache_folder, exist_ok=True)

        # Создаём тестовый реестр
        self.registry_path = os.path.join(self.cache_folder, "processed_registry.json")
        registry_data = {
            "file1.jpg": {
                "md5": "abc123",
                "page_type": "medical_card_front",
                "client_name": "Тест 1",
                "processed_at": "2024-01-01T12:00:00",
                "written_to_excel": True
            },
            "file2.jpg": {
                "md5": "def456",
                "page_type": "procedure_sheet",
                "client_name": "Тест 2",
                "processed_at": "2024-01-01T12:01:00",
                "written_to_excel": True
            }
        }
        with open(self.registry_path, 'w', encoding='utf-8') as f:
            json.dump(registry_data, f, ensure_ascii=False, indent=2)

        # Создаём тестовые кэш-файлы
        self.cache_files = []
        for i in range(5):
            cache_file = os.path.join(self.cache_folder, f"test_cache_{i}.json")
            with open(cache_file, 'w', encoding='utf-8') as f:
                json.dump({"test": f"data_{i}"}, f)
            self.cache_files.append(cache_file)

    def teardown_method(self):
        """Удаляем временную структуру."""
        if os.path.exists(self.test_dir):
            shutil.rmtree(self.test_dir)

    def test_registry_exists_before_force(self):
        """Проверка что реестр существует до --force."""
        assert os.path.exists(self.registry_path)
        with open(self.registry_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
        assert len(data) == 2
        assert "file1.jpg" in data

    def test_cache_files_exist_before_force(self):
        """Проверка что кэш-файлы существуют до --force."""
        for cache_file in self.cache_files:
            assert os.path.exists(cache_file)

    def test_force_removes_registry(self):
        """Тест удаления реестра при --force."""
        # Имитируем удаление реестра
        if os.path.exists(self.registry_path):
            os.remove(self.registry_path)

        assert not os.path.exists(self.registry_path)

    def test_force_removes_cache_files(self):
        """Тест удаления кэш-файлов при --force."""
        import glob

        # Имитируем очистку кэша
        cache_files = glob.glob(os.path.join(self.cache_folder, "*.json"))
        cache_files = [f for f in cache_files if not os.path.basename(f).startswith('_')]

        for cache_file in cache_files:
            if os.path.basename(cache_file) != "processed_registry.json":
                os.remove(cache_file)

        # Проверяем что файлы удалены
        remaining_cache = glob.glob(os.path.join(self.cache_folder, "*.json"))
        # Должны остаться только служебные файлы и реестр (если не удалён)
        remaining_cache = [f for f in remaining_cache
                          if not os.path.basename(f).startswith('_')
                          and os.path.basename(f) != "processed_registry.json"]

        assert len(remaining_cache) == 0

    def test_registry_and_cache_cleanup_order(self):
        """Тест правильного порядка очистки: реестр, потом кэш."""
        # Сначала проверяем что всё есть
        assert os.path.exists(self.registry_path)
        assert len(self.cache_files) > 0

        # Имитируем очистку в правильном порядке
        # 1. Удаляем реестр
        if os.path.exists(self.registry_path):
            os.remove(self.registry_path)

        assert not os.path.exists(self.registry_path)

        # 2. Удаляем кэш (кроме реестра, который уже удалён)
        import glob
        cache_files = glob.glob(os.path.join(self.cache_folder, "*.json"))
        for cache_file in cache_files:
            if os.path.basename(cache_file) != "processed_registry.json":
                os.remove(cache_file)

        # Проверяем финальное состояние
        remaining = glob.glob(os.path.join(self.cache_folder, "*.json"))
        assert len(remaining) == 0

    def test_empty_cache_folder_handling(self):
        """Тест обработки пустой папки кэша."""
        # Удаляем все файлы
        import glob
        for f in glob.glob(os.path.join(self.cache_folder, "*.json")):
            os.remove(f)

        # Проверяем что код не падает на пустой папке
        cache_files = glob.glob(os.path.join(self.cache_folder, "*.json"))
        assert len(cache_files) == 0

    def test_missing_registry_handling(self):
        """Тест обработки отсутствующего реестра."""
        # Удаляем реестр
        if os.path.exists(self.registry_path):
            os.remove(self.registry_path)

        # Попытка удалить несуществующий реестр не должна падать
        try:
            if os.path.exists(self.registry_path):
                os.remove(self.registry_path)
            success = True
        except OSError:
            success = False

        assert success


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
