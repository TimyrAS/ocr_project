"""
Интеграционные тесты для OCR пайплайна.

Проверяют:
1. Merge fallback-результатов по не-секвенциальным индексам
2. Парсинг check_results/summary от Claude
3. Сохранение raw_payload/parse_mode в результатах
"""

import sys
import os

# Добавляем родительскую папку в path для импорта модулей
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import pytest
import pandas as pd
from unittest.mock import Mock
import json


class TestFallbackMergeIntegration:
    """Тесты merge fallback-результатов."""

    def test_merge_non_sequential_indices(self):
        """Тест merge по не-секвенциальным индексам (1, 3, 7)."""
        from final_verification import enhance_verification_df

        # Создаём DataFrame с не-секвенциальными индексами
        verification_df = pd.DataFrame({
            'OCR_ФИО': ['Клиент 0', 'Клиент 1', 'Клиент 2', 'Клиент 3', 'Клиент 4'],
            'Статус': ['Найден', 'Не найден', 'Найден', 'Возможно', 'Найден']
        }, index=[0, 1, 2, 3, 4])

        # Выбираем только fallback-строки (индексы 1 и 3)
        fallback_mask = verification_df["Статус"].isin(["Не найден", "Возможно"])
        fallback_df = verification_df[fallback_mask].copy()

        # Проверяем что индексы сохранены
        assert list(fallback_df.index) == [1, 3]

        # Имитируем результаты Claude (client_id = оригинальный индекс!)
        claude_results = [
            {
                'client_id': '1',
                'final_status': 'Подтверждён',
                'confidence_score': 95,
                'possible_matches': [],
                'discrepancies': [],
                'ocr_corrections': {},
                'recommendations': ['OK']
            },
            {
                'client_id': '3',
                'final_status': 'Требует проверки',
                'confidence_score': 75,
                'possible_matches': [],
                'discrepancies': [],
                'ocr_corrections': {},
                'recommendations': ['Проверить']
            }
        ]

        # Обогащаем fallback_df
        enhanced_fallback = enhance_verification_df(fallback_df, claude_results)

        # Проверяем что индексы НЕ изменились
        assert list(enhanced_fallback.index) == [1, 3]

        # Проверяем что данные правильно замержились по индексам
        assert enhanced_fallback.at[1, 'Claude_Статус'] == 'Подтверждён'
        assert enhanced_fallback.at[1, 'Claude_Совпадение_%'] == 95

        assert enhanced_fallback.at[3, 'Claude_Статус'] == 'Требует проверки'
        assert enhanced_fallback.at[3, 'Claude_Совпадение_%'] == 75

        # Теперь мержим обратно в полный verification_df
        for idx in enhanced_fallback.index:
            if idx in verification_df.index:
                for col in enhanced_fallback.columns:
                    if col.startswith('Claude_') or col in ['Возможные_совпадения_БД', 'Расхождения', 'Рекомендации', 'Исправления_OCR']:
                        verification_df.at[idx, col] = enhanced_fallback.at[idx, col]

        # Проверяем финальное состояние
        assert verification_df.at[1, 'Claude_Статус'] == 'Подтверждён'
        assert verification_df.at[3, 'Claude_Статус'] == 'Требует проверки'

        # Индексы 0, 2, 4 не должны иметь Claude данных
        assert pd.isna(verification_df.at[0, 'Claude_Статус']) or verification_df.at[0, 'Claude_Статус'] == ''

    def test_merge_with_gaps(self):
        """Тест merge с большими пропусками в индексах (10, 50, 99)."""
        from final_verification import enhance_verification_df

        # DataFrame с большими пропусками
        verification_df = pd.DataFrame({
            'OCR_ФИО': ['A', 'B', 'C'],
            'Статус': ['Не найден', 'Не найден', 'Не найден']
        }, index=[10, 50, 99])

        claude_results = [
            {'client_id': '10', 'final_status': 'OK1', 'confidence_score': 90,
             'possible_matches': [], 'discrepancies': [], 'ocr_corrections': {}, 'recommendations': []},
            {'client_id': '50', 'final_status': 'OK2', 'confidence_score': 85,
             'possible_matches': [], 'discrepancies': [], 'ocr_corrections': {}, 'recommendations': []},
            {'client_id': '99', 'final_status': 'OK3', 'confidence_score': 80,
             'possible_matches': [], 'discrepancies': [], 'ocr_corrections': {}, 'recommendations': []},
        ]

        enhanced = enhance_verification_df(verification_df, claude_results)

        assert list(enhanced.index) == [10, 50, 99]
        assert enhanced.at[10, 'Claude_Статус'] == 'OK1'
        assert enhanced.at[50, 'Claude_Статус'] == 'OK2'
        assert enhanced.at[99, 'Claude_Статус'] == 'OK3'


class TestClaudeResponseParsing:
    """Тесты парсинга ответов Claude."""

    def test_parse_check_results_format(self):
        """Тест парсинга формата с check_results."""
        from final_verification import parse_claude_batch_response

        # Создаём мок ответа Claude
        mock_response = Mock()
        mock_response.content = [Mock()]
        mock_response.content[0].text = json.dumps({
            "check_results": [
                {
                    "client_id": "0",
                    "final_status": "Подтверждён",
                    "confidence_score": 95,
                    "possible_matches": [],
                    "discrepancies": [],
                    "ocr_corrections": {},
                    "recommendations": ["OK"]
                }
            ],
            "summary": "Проверка завершена"
        }, ensure_ascii=False)

        mock_log = Mock()
        results = parse_claude_batch_response(mock_response, mock_log)

        assert len(results) == 1
        assert results[0]['client_id'] == '0'
        assert results[0]['final_status'] == 'Подтверждён'
        assert results[0]['confidence_score'] == 95

    def test_parse_check_results_with_summary(self):
        """Тест что summary не ломает парсинг."""
        from final_verification import parse_claude_batch_response

        mock_response = Mock()
        mock_response.content = [Mock()]
        mock_response.content[0].text = json.dumps({
            "summary": "Обработано 2 клиента",
            "check_results": [
                {"client_id": "1", "status": "OK", "confidence": 90,
                 "possible_matches": [], "discrepancies": [], "ocr_corrections": {}, "recommendations": []},
                {"client_id": "2", "final_status": "Нет", "confidence_score": 50,
                 "possible_matches": [], "discrepancies": [], "ocr_corrections": {}, "recommendations": []},
            ]
        }, ensure_ascii=False)

        mock_log = Mock()
        results = parse_claude_batch_response(mock_response, mock_log)

        assert len(results) == 2
        assert results[0]['client_id'] == '1'
        assert results[0]['final_status'] == 'OK'  # Нормализовано из status


class TestRawPayloadPreservation:
    """Тесты сохранения raw_payload и parse_mode."""

    def test_raw_payload_in_result(self):
        """Тест что raw_payload сохраняется в result."""
        from client_card_ocr import normalize_claude_response

        payload = {
            "page_type": "medical_card_front",
            "data": {"fio": "Тест"}
        }

        result = normalize_claude_response(payload, "", "test.jpg")

        assert "raw_payload" in result
        assert result["raw_payload"] == payload
        assert "parse_mode" in result

    def test_parse_mode_values(self):
        """Тест разных значений parse_mode."""
        from client_card_ocr import normalize_claude_response

        # strict
        result1 = normalize_claude_response(
            {"page_type": "medical_card_front", "data": {}},
            "", "test.jpg"
        )
        assert result1["parse_mode"] == "strict"

        # recovered (document_type)
        result2 = normalize_claude_response(
            {"document_type": "medical_card_front"},
            "", "test.jpg"
        )
        assert result2["parse_mode"] == "recovered"

        # fallback (unknown)
        result3 = normalize_claude_response(
            {"unknown_field": "value"},
            "", "test.jpg"
        )
        assert result3["parse_mode"] == "fallback"

    def test_raw_payload_for_all_formats(self):
        """Тест что raw_payload есть для всех форматов."""
        from client_card_ocr import normalize_claude_response

        formats = [
            {"page_type": "medical_card_front", "data": {}},
            {"document_type": "procedure_sheet"},
            {"медицинская_карта": {"fio": "X"}},
            {"unknown": "data"}
        ]

        for payload in formats:
            result = normalize_claude_response(payload, "", "test.jpg")
            assert "raw_payload" in result
            assert "parse_mode" in result


class TestNewStatusPipelineIntegration:
    """Интеграционные тесты для новой системы статусов в пайплайне."""

    def test_fallback_verification_with_new_statuses(self):
        """Тест: fallback-верификация запускается для новых статусов."""
        import pandas as pd
        from config import STATUS_DB_FOUND, STATUS_DB_MAYBE, STATUS_DB_NOT_FOUND

        # Имитируем verification_df с новыми статусами
        verification_df = pd.DataFrame({
            'OCR_ФИО': ['Клиент 1', 'Клиент 2', 'Клиент 3', 'Клиент 4'],
            'OCR_Телефон': ['', '', '', ''],
            'Статус_картотеки': ['Найден в OCR'] * 4,
            'Статус_БД': [
                STATUS_DB_FOUND,
                STATUS_DB_MAYBE,
                STATUS_DB_NOT_FOUND,
                STATUS_DB_FOUND
            ],
            'Статус': [
                STATUS_DB_FOUND,
                STATUS_DB_MAYBE,
                STATUS_DB_NOT_FOUND,
                STATUS_DB_FOUND
            ]
        })

        # Логика из run_pipeline.py (fallback-only режим)
        status_column = "Статус_БД" if "Статус_БД" in verification_df.columns else "Статус"

        if status_column == "Статус_БД":
            fallback_mask = verification_df[status_column].isin([
                STATUS_DB_MAYBE,
                STATUS_DB_NOT_FOUND
            ])
        else:
            fallback_mask = verification_df[status_column].isin(["Не найден", "Возможно"])

        fallback_df = verification_df[fallback_mask].copy()

        # Проверяем что fallback НЕ пустой (должен содержать 2 клиента)
        assert len(fallback_df) > 0, "Fallback-верификация не должна быть пропущена"
        assert len(fallback_df) == 2
        assert list(fallback_df.index) == [1, 2]

    def test_save_not_found_creates_file(self):
        """Тест: clients_not_found.xlsx создаётся для Статус_БД = 'Нет в БД'."""
        import pandas as pd
        import tempfile
        import os
        from verify_with_db import save_not_found_clients
        from config import STATUS_DB_FOUND, STATUS_DB_MAYBE, STATUS_DB_NOT_FOUND

        verification_df = pd.DataFrame({
            'OCR_ФИО': ['Клиент 1', 'Клиент 2', 'Клиент 3'],
            'OCR_Телефон': ['', '', ''],
            'Статус_БД': [
                STATUS_DB_FOUND,
                STATUS_DB_MAYBE,
                STATUS_DB_NOT_FOUND  # Только этот должен попасть в файл
            ]
        })

        # Создаём mock ocr_sheets
        ocr_sheets = {
            'Клиенты': pd.DataFrame({
                'ФИО': ['Клиент 1', 'Клиент 2', 'Клиент 3'],
                'Телефон': ['', '', '']
            })
        }

        # Создаём временный файл
        with tempfile.NamedTemporaryFile(suffix=".xlsx", delete=False) as tmp:
            tmp_path = tmp.name

        try:
            result_path = save_not_found_clients(verification_df, ocr_sheets, tmp_path)

            # Проверяем что файл создан
            assert result_path is not None, "clients_not_found.xlsx должен быть создан"
            assert os.path.exists(tmp_path)

            # Читаем файл и проверяем содержимое
            not_found_df = pd.read_excel(tmp_path, sheet_name="Не_найдены")
            assert len(not_found_df) == 1, "Должен быть только 1 ненайденный клиент"
            assert not_found_df.iloc[0]['Причина'] == STATUS_DB_NOT_FOUND

        finally:
            # Удаляем временный файл
            if os.path.exists(tmp_path):
                os.remove(tmp_path)

    def test_empty_not_found_no_file(self):
        """Тест: clients_not_found.xlsx НЕ создаётся если все клиенты найдены."""
        import pandas as pd
        import tempfile
        import os
        from verify_with_db import save_not_found_clients
        from config import STATUS_DB_FOUND, STATUS_DB_MAYBE

        verification_df = pd.DataFrame({
            'OCR_ФИО': ['Клиент 1', 'Клиент 2'],
            'OCR_Телефон': ['', ''],
            'Статус_БД': [
                STATUS_DB_FOUND,
                STATUS_DB_MAYBE
            ]
        })

        ocr_sheets = {
            'Клиенты': pd.DataFrame({
                'ФИО': ['Клиент 1', 'Клиент 2'],
                'Телефон': ['', '']
            })
        }

        with tempfile.NamedTemporaryFile(suffix=".xlsx", delete=False) as tmp:
            tmp_path = tmp.name

        try:
            result_path = save_not_found_clients(verification_df, ocr_sheets, tmp_path)

            # Проверяем что файл НЕ создан (функция возвращает None)
            assert result_path is None, "clients_not_found.xlsx не должен создаваться если все найдены"

        finally:
            # Удаляем временный файл если он был создан
            if os.path.exists(tmp_path):
                os.remove(tmp_path)


class TestFuzzyMatchNotFoundIntegration:
    """Интеграционные тесты fuzzy-матчинга в clients_not_found.xlsx."""

    def test_end_to_end_fuzzy_match_ocr_errors(self):
        """Интеграционный тест: OCR-ошибки не препятствуют подтягиванию полной строки."""
        import pandas as pd
        import tempfile
        import os
        from verify_with_db import save_not_found_clients
        from config import STATUS_DB_NOT_FOUND

        # Эмулируем реальный сценарий:
        # 1. OCR распознал ФИО с ошибками (лишние пробелы, опечатки)
        # 2. Клиент не найден в БД
        # 3. Нужно подтянуть полную строку из OCR для детального отчёта

        verification_df = pd.DataFrame({
            'OCR_ФИО': [
                'Чаплено Карина',      # OCR-ошибка (пропущена 'к')
                'Семёнов  Пётр',       # Лишние пробелы + ё
                'иванова мария'        # Нижний регистр
            ],
            'OCR_Телефон': ['', '', ''],
            'Статус_БД': [STATUS_DB_NOT_FOUND] * 3
        })

        ocr_sheets = {
            'Клиенты': pd.DataFrame({
                'ФИО': [
                    'Чапленко Карина',     # Правильное написание
                    'Семенов Петр',        # Без ё, один пробел
                    'Иванова Мария'        # Заглавные буквы
                ],
                'Телефон': ['', '', ''],
                'Email': ['chaplen@mail.ru', 'semenov@mail.ru', 'ivanova@mail.ru'],
                'Адрес': ['Адрес 1', 'Адрес 2', 'Адрес 3'],
                'ИИН': ['111', '222', '333']
            })
        }

        with tempfile.NamedTemporaryFile(suffix=".xlsx", delete=False) as tmp:
            tmp_path = tmp.name

        try:
            result_path = save_not_found_clients(verification_df, ocr_sheets, tmp_path)
            assert result_path is not None

            # Читаем результат
            not_found_df = pd.read_excel(tmp_path, sheet_name="Не_найдены")

            # Проверяем что все 3 клиента в файле
            assert len(not_found_df) == 3

            # Проверяем что для всех подтянулись полные данные
            assert 'Email' in not_found_df.columns
            assert 'Адрес' in not_found_df.columns
            assert 'ИИН' in not_found_df.columns

            # Проверяем конкретные значения (fuzzy-match сработал)
            emails = not_found_df['Email'].tolist()
            assert 'chaplen@mail.ru' in emails, "Email для 'Чаплено' не подтянулся"
            assert 'semenov@mail.ru' in emails, "Email для 'Семёнов' не подтянулся"
            assert 'ivanova@mail.ru' in emails, "Email для 'иванова' не подтянулся"

            # Проверяем что ИИНы подтянулись (могут быть как строки, так и числа)
            iins = [str(x) for x in not_found_df['ИИН'].tolist()]
            assert '111' in iins
            assert '222' in iins
            assert '333' in iins

        finally:
            if os.path.exists(tmp_path):
                os.remove(tmp_path)

    def test_fuzzy_match_prefers_best_score(self):
        """Тест: при нескольких похожих ФИО выбирается лучшее совпадение."""
        import pandas as pd
        import tempfile
        import os
        from verify_with_db import save_not_found_clients
        from config import STATUS_DB_NOT_FOUND

        verification_df = pd.DataFrame({
            'OCR_ФИО': ['Иванов'],  # Короткое ФИО
            'OCR_Телефон': [''],
            'Статус_БД': [STATUS_DB_NOT_FOUND]
        })

        # В OCR есть несколько похожих клиентов
        ocr_sheets = {
            'Клиенты': pd.DataFrame({
                'ФИО': [
                    'Иванов',              # Точное совпадение (score = 1.0)
                    'Иванов Иван',         # Частичное (score < 1.0)
                    'Иванов Пётр'          # Частичное (score < 1.0)
                ],
                'Телефон': ['', '', ''],
                'Адрес': ['Адрес 1', 'Адрес 2', 'Адрес 3']
            })
        }

        with tempfile.NamedTemporaryFile(suffix=".xlsx", delete=False) as tmp:
            tmp_path = tmp.name

        try:
            result_path = save_not_found_clients(verification_df, ocr_sheets, tmp_path)
            assert result_path is not None

            not_found_df = pd.read_excel(tmp_path, sheet_name="Не_найдены")
            assert len(not_found_df) == 1

            # Должен выбраться лучший матч (точное совпадение)
            assert not_found_df.iloc[0]['Адрес'] == 'Адрес 1', "Должен выбраться точный матч 'Иванов'"

        finally:
            if os.path.exists(tmp_path):
                os.remove(tmp_path)


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
