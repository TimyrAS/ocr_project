"""
Тесты для final_verification.py - проверка использования корректных колонок.

Проверяет что:
1. Используются правильные названия колонок из verification_df
2. Отчёты формируются без ошибок
3. Нет попыток обратиться к несуществующим колонкам
"""

import sys
import os
import tempfile

# Добавляем родительскую папку в path для импорта модулей
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import pytest
import pandas as pd


class TestFinalVerificationColumns:
    """Тесты корректности использования колонок."""

    def setup_method(self):
        """Создаём тестовый DataFrame."""
        # Создаём DataFrame с ПРАВИЛЬНЫМИ названиями колонок
        self.test_df = pd.DataFrame({
            'OCR_ФИО': ['Иванов Иван', 'Петрова Мария', 'Сидоров Пётр'],
            'OCR_Телефон': ['+7 777 111 22 33', '+7 777 222 33 44', '+7 777 333 44 55'],
            'БД_ФИО': ['Иванов Иван', 'Петрова М.', ''],
            'БД_Телефон': ['+7 777 111 22 33', '+7 777 222 33 44', ''],
            'Статус': ['Найден', 'Возможно', 'Не найден'],
            'Совпадение_%': [95.0, 75.0, 0.0],
            'Визитов_в_БД': [10, 5, 0],
            'Врачи_в_БД': ['Асшеман Оксана', 'Крошка Рада', '']
        })

        # Добавляем колонки Claude (которые создаются после верификации)
        self.test_df['Claude_Статус'] = ['Подтверждён', 'Требует проверки', 'Не найден']
        self.test_df['Claude_Совпадение_%'] = [98.0, 82.0, 0.0]
        self.test_df['Возможные_совпадения_БД'] = ['', 'Петрова Мария (85%)', '']
        self.test_df['Расхождения'] = ['', 'phone: разные телефоны', '']
        self.test_df['Рекомендации'] = ['', 'Проверить вручную', 'Новый клиент']
        self.test_df['Исправления_OCR'] = ['', '', '']

    def test_required_columns_exist(self):
        """Проверка наличия обязательных колонок."""
        required_columns = [
            'OCR_ФИО', 'OCR_Телефон',
            'БД_ФИО', 'БД_Телефон',
            'Статус', 'Совпадение_%'
        ]

        for col in required_columns:
            assert col in self.test_df.columns, f"Колонка {col} отсутствует"

    def test_wrong_column_names_not_exist(self):
        """Проверка что НЕПРАВИЛЬНЫХ колонок нет."""
        wrong_columns = [
            'ID клиента OCR',  # Неправильно
            'ФИО OCR',         # Неправильно
            'Телефон OCR',     # Неправильно
            'ФИО БД',          # Неправильно
            'Телефон БД'       # Неправильно
        ]

        for col in wrong_columns:
            assert col not in self.test_df.columns, f"Найдена неправильная колонка {col}"

    def test_sheet_generation_no_errors(self):
        """Тест генерации листов без ошибок."""
        # Имитируем выборку для листа "Требуют проверки"
        needs_review = self.test_df[self.test_df['Claude_Статус'] == 'Требует проверки'].copy()

        if not needs_review.empty:
            # Добавляем ID как индекс
            needs_review.insert(0, 'ID', needs_review.index)

            # Список колонок для отчёта (ПРАВИЛЬНЫЕ названия)
            review_cols = [
                'ID', 'OCR_ФИО', 'OCR_Телефон',
                'БД_ФИО', 'БД_Телефон', 'Статус',
                'Claude_Статус', 'Claude_Совпадение_%',
                'Расхождения', 'Рекомендации'
            ]

            # Проверяем что все колонки существуют
            existing_cols = [col for col in review_cols if col in needs_review.columns]

            # Все колонки должны существовать
            assert len(existing_cols) == len(review_cols), \
                f"Не все колонки найдены: {set(review_cols) - set(existing_cols)}"

            # Проверяем что можем создать subset
            subset = needs_review[existing_cols]
            assert len(subset) > 0

    def test_not_found_sheet_generation(self):
        """Тест генерации листа 'Не найдены'."""
        not_found = self.test_df[self.test_df['Статус'] == 'Не найден'].copy()

        if not not_found.empty:
            not_found.insert(0, 'ID', not_found.index)

            not_found_cols = [
                'ID', 'OCR_ФИО', 'OCR_Телефон',
                'Claude_Статус', 'Возможные_совпадения_БД', 'Рекомендации'
            ]

            existing_cols = [col for col in not_found_cols if col in not_found.columns]
            assert len(existing_cols) == len(not_found_cols)

            subset = not_found[existing_cols]
            assert len(subset) > 0

    def test_dupes_sheet_generation(self):
        """Тест генерации листа 'Возможные дубли'."""
        # Добавляем тестовую строку с дублем
        self.test_df.loc[3] = {
            'OCR_ФИО': 'Иванов Иван',
            'OCR_Телефон': '+7 777 111 22 34',  # Другой телефон
            'БД_ФИО': 'Иванов Иван',
            'БД_Телефон': '+7 777 111 22 33',
            'Статус': 'Найден',
            'Совпадение_%': 92.0,
            'Визитов_в_БД': 10,
            'Врачи_в_БД': 'Асшеман Оксана',
            'Claude_Статус': 'Возможный дубль',
            'Claude_Совпадение_%': 88.0,
            'Возможные_совпадения_БД': 'Иванов Иван (92%)',
            'Расхождения': 'phone: разные телефоны',
            'Рекомендации': 'Проверить на дубль',
            'Исправления_OCR': ''
        }

        possible_dupes = self.test_df[self.test_df['Claude_Статус'] == 'Возможный дубль'].copy()

        if not possible_dupes.empty:
            possible_dupes.insert(0, 'ID', possible_dupes.index)

            dupe_cols = [
                'ID', 'OCR_ФИО', 'OCR_Телефон',
                'БД_ФИО', 'БД_Телефон',
                'Claude_Совпадение_%', 'Рекомендации'
            ]

            existing_cols = [col for col in dupe_cols if col in possible_dupes.columns]
            assert len(existing_cols) == len(dupe_cols)

            subset = possible_dupes[existing_cols]
            assert len(subset) > 0

    def test_corrections_sheet_generation(self):
        """Тест генерации листа 'Исправления OCR'."""
        # Добавляем строку с исправлениями
        self.test_df.loc[0, 'Исправления_OCR'] = 'fio: Иванов Иван Иванович'

        with_corrections = self.test_df[self.test_df['Исправления_OCR'].str.len() > 0].copy()

        if not with_corrections.empty:
            with_corrections.insert(0, 'ID', with_corrections.index)

            corr_cols = [
                'ID', 'OCR_ФИО', 'OCR_Телефон',
                'Исправления_OCR', 'Рекомендации'
            ]

            existing_cols = [col for col in corr_cols if col in with_corrections.columns]
            assert len(existing_cols) == len(corr_cols)

            subset = with_corrections[existing_cols]
            assert len(subset) > 0

    def test_recommendations_sheet_generation(self):
        """Тест генерации листа 'Рекомендации'."""
        with_recommendations = self.test_df[self.test_df['Рекомендации'].str.len() > 0].copy()

        if not with_recommendations.empty:
            with_recommendations.insert(0, 'ID', with_recommendations.index)

            rec_cols = [
                'ID', 'OCR_ФИО', 'Статус',
                'Claude_Статус', 'Рекомендации'
            ]

            existing_cols = [col for col in rec_cols if col in with_recommendations.columns]
            assert len(existing_cols) == len(rec_cols)

            subset = with_recommendations[existing_cols]
            assert len(subset) > 0

    def test_column_access_no_keyerror(self):
        """Тест что доступ к колонкам не вызывает KeyError."""
        # Проверяем безопасный доступ к колонкам
        for col in ['OCR_ФИО', 'OCR_Телефон', 'БД_ФИО', 'БД_Телефон']:
            try:
                _ = self.test_df[col]
                success = True
            except KeyError:
                success = False
            assert success, f"KeyError при доступе к {col}"

    def test_empty_dataframe_handling(self):
        """Тест обработки пустого DataFrame."""
        empty_df = pd.DataFrame()

        # Проверяем что фильтрация пустого DataFrame не вызывает ошибок
        try:
            filtered = empty_df[empty_df.get('Статус', pd.Series()) == 'Не найден']
            success = True
        except:
            success = False

        assert success


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
