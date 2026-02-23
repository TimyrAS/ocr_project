"""
Тесты для новых статусов картотеки и БД.

Проверяют:
1. Маппинг новых статусов (нет "Не найден" для картотеки)
2. Короткое ФИО не дает "Найден в БД" без телефона
3. Наличие новых OCR-текстовых колонок
4. Ужесточенные правила матчинга
"""

import sys
import os

# Добавляем родительскую папку в path для импорта модулей
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import pytest
import pandas as pd
from unittest.mock import Mock


class TestNewStatusMapping:
    """Тесты маппинга новых статусов."""

    def test_kartoteka_always_found(self):
        """Тест что Статус_картотеки всегда 'Найден в OCR'."""
        from verify_with_db import verify_clients

        # Мокаем данные
        ocr_sheets = {
            "Клиенты": pd.DataFrame({
                "ФИО": ["Иванов Иван", "Петрова", "Сидоров Пётр Павлович"],
                "Телефон": ["", "", ""]
            })
        }

        db_index = {}  # Пустая БД

        threshold = 0.70
        result_df = verify_clients(ocr_sheets, db_index, threshold)

        # Проверяем что все клиенты имеют Статус_картотеки = "Найден в OCR"
        assert "Статус_картотеки" in result_df.columns
        assert all(result_df["Статус_картотеки"] == "Найден в OCR")

    def test_no_not_found_status_for_kartoteka(self):
        """Тест что статус 'Не найден' больше не появляется."""
        from verify_with_db import verify_clients

        ocr_sheets = {
            "Клиенты": pd.DataFrame({
                "ФИО": ["Новый Клиент"],
                "Телефон": [""]
            })
        }

        db_index = {}

        result_df = verify_clients(ocr_sheets, db_index, 0.70)

        # Проверяем что нет статуса "Не найден"
        assert "Не найден" not in result_df["Статус_БД"].values
        assert "Не найден" not in result_df["Статус"].values

        # Вместо этого должен быть "Нет в БД (новый для картотеки)"
        assert all(result_df["Статус_БД"] == "Нет в БД (новый для картотеки)")

    def test_backward_compatibility_alias(self):
        """Тест что Статус = alias для Статус_БД."""
        from verify_with_db import verify_clients

        ocr_sheets = {
            "Клиенты": pd.DataFrame({
                "ФИО": ["Тест Тестов"],
                "Телефон": [""]
            })
        }

        db_index = {}
        result_df = verify_clients(ocr_sheets, db_index, 0.70)

        # Проверяем что Статус == Статус_БД
        assert "Статус" in result_df.columns
        assert "Статус_БД" in result_df.columns
        assert all(result_df["Статус"] == result_df["Статус_БД"])


class TestShortFIOMatching:
    """Тесты матчинга коротких ФИО."""

    def test_short_fio_without_phone_not_found_in_db(self):
        """Тест: короткое ФИО (1 слово) без телефона → максимум 'Возможное совпадение'."""
        from verify_with_db import verify_clients

        ocr_sheets = {
            "Клиенты": pd.DataFrame({
                "ФИО": ["Чапленко"],  # Только фамилия
                "Телефон": [""]  # Нет телефона
            })
        }

        # БД с похожим клиентом
        db_index = {
            "чапленко рома": {
                "name_orig": "Чапленко Рома",
                "phone": "77771234567",
                "visits": [],
                "doctors": [],
                "total_visits": 5
            }
        }

        result_df = verify_clients(ocr_sheets, db_index, 0.70)

        # Проверяем что статус НЕ "Найден в БД"
        assert result_df.iloc[0]["Статус_БД"] != "Найден в БД"

        # Должен быть либо "Возможное совпадение" либо "Нет в БД"
        assert result_df.iloc[0]["Статус_БД"] in [
            "Возможное совпадение в БД",
            "Нет в БД (новый для картотеки)"
        ]

    def test_full_fio_without_phone_can_be_found(self):
        """Тест: полное ФИО (>=2 слова) + высокий score → может быть 'Найден в БД'."""
        from verify_with_db import verify_clients

        ocr_sheets = {
            "Клиенты": pd.DataFrame({
                "ФИО": ["Чапленко Рома"],  # Полное ФИО
                "Телефон": [""]
            })
        }

        db_index = {
            "чапленко рома": {
                "name_orig": "Чапленко Рома",
                "phone": "77771234567",
                "visits": [],
                "doctors": [],
                "total_visits": 5
            }
        }

        result_df = verify_clients(ocr_sheets, db_index, 0.70)

        # С полным ФИО и высоким score должен быть "Найден в БД"
        assert result_df.iloc[0]["Статус_БД"] == "Найден в БД"

    def test_phone_match_overrides_short_fio(self):
        """Тест: совпадение телефона → "Найден в БД" даже с коротким ФИО."""
        from verify_with_db import verify_clients

        ocr_sheets = {
            "Клиенты": pd.DataFrame({
                "ФИО": ["Иванов"],  # Короткое ФИО
                "Телефон": ["77771234567"]  # Есть телефон
            })
        }

        db_index = {
            "иванов иван": {
                "name_orig": "Иванов Иван",
                "phone": "77771234567",  # Совпадает!
                "visits": [],
                "doctors": [],
                "total_visits": 3
            }
        }

        result_df = verify_clients(ocr_sheets, db_index, 0.70)

        # Совпадение телефона → "Найден в БД"
        assert result_df.iloc[0]["Статус_БД"] == "Найден в БД"


class TestFallbackStatusFilter:
    """Тесты фильтра для fallback-верификации."""

    def test_fallback_filter_new_statuses(self):
        """Тест: fallback-фильтр корректно выбирает строки по Статус_БД."""
        import pandas as pd
        from config import STATUS_DB_FOUND, STATUS_DB_MAYBE, STATUS_DB_NOT_FOUND

        # Создаём DataFrame с новыми статусами
        verification_df = pd.DataFrame({
            'OCR_ФИО': ['Клиент 1', 'Клиент 2', 'Клиент 3', 'Клиент 4'],
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

        # Применяем фильтр как в run_pipeline.py
        status_column = "Статус_БД" if "Статус_БД" in verification_df.columns else "Статус"
        fallback_mask = verification_df[status_column].isin([
            STATUS_DB_MAYBE,
            STATUS_DB_NOT_FOUND
        ])
        fallback_df = verification_df[fallback_mask].copy()

        # Проверяем что выбрано 2 клиента (индексы 1 и 2)
        assert len(fallback_df) == 2
        assert list(fallback_df.index) == [1, 2]
        assert all(fallback_df['Статус_БД'].isin([STATUS_DB_MAYBE, STATUS_DB_NOT_FOUND]))

    def test_fallback_filter_backward_compatibility(self):
        """Тест: fallback-фильтр работает со старыми статусами."""
        import pandas as pd

        # DataFrame со старыми статусами (без Статус_БД)
        verification_df = pd.DataFrame({
            'OCR_ФИО': ['A', 'B', 'C', 'D'],
            'Статус': ['Найден', 'Не найден', 'Возможно', 'Найден']
        })

        # Применяем старый фильтр
        status_column = "Статус_БД" if "Статус_БД" in verification_df.columns else "Статус"
        fallback_mask = verification_df[status_column].isin(["Не найден", "Возможно"])
        fallback_df = verification_df[fallback_mask].copy()

        # Проверяем что выбрано 2 клиента (индексы 1 и 2)
        assert len(fallback_df) == 2
        assert list(fallback_df.index) == [1, 2]


class TestSaveNotFoundClientsLogic:
    """Тесты функции save_not_found_clients."""

    def test_not_found_filter_new_status(self):
        """Тест: save_not_found_clients корректно фильтрует по Статус_БД."""
        import pandas as pd
        from config import STATUS_DB_FOUND, STATUS_DB_MAYBE, STATUS_DB_NOT_FOUND

        verification_df = pd.DataFrame({
            'OCR_ФИО': ['Клиент 1', 'Клиент 2', 'Клиент 3'],
            'OCR_Телефон': ['', '', ''],
            'Статус_БД': [
                STATUS_DB_FOUND,
                STATUS_DB_MAYBE,
                STATUS_DB_NOT_FOUND
            ]
        })

        # Применяем фильтр как в save_not_found_clients()
        status_column = "Статус_БД" if "Статус_БД" in verification_df.columns else "Статус"
        not_found = verification_df[verification_df[status_column] == STATUS_DB_NOT_FOUND].copy()

        # Проверяем что выбран только 1 клиент (индекс 2)
        assert len(not_found) == 1
        assert not_found.iloc[0]['OCR_ФИО'] == 'Клиент 3'
        assert not_found.iloc[0]['Статус_БД'] == STATUS_DB_NOT_FOUND

    def test_not_found_filter_backward_compatibility(self):
        """Тест: save_not_found_clients работает со старым форматом."""
        import pandas as pd

        verification_df = pd.DataFrame({
            'OCR_ФИО': ['A', 'B', 'C'],
            'OCR_Телефон': ['', '', ''],
            'Статус': ['Найден', 'Возможно', 'Не найден']
        })

        # Применяем старый фильтр
        status_column = "Статус_БД" if "Статус_БД" in verification_df.columns else "Статус"
        not_found = verification_df[verification_df[status_column] == "Не найден"].copy()

        # Проверяем что выбран только 1 клиент
        assert len(not_found) == 1
        assert not_found.iloc[0]['OCR_ФИО'] == 'C'


class TestFuzzyMatchInNotFound:
    """Тесты fuzzy-матчинга ФИО при формировании clients_not_found.xlsx."""

    def test_fuzzy_match_with_extra_spaces(self):
        """Тест: лишние пробелы не мешают матчингу."""
        import pandas as pd
        import tempfile
        import os
        from verify_with_db import save_not_found_clients
        from config import STATUS_DB_NOT_FOUND

        # verification_df с нормализованным ФИО
        verification_df = pd.DataFrame({
            'OCR_ФИО': ['Чапленко Карина'],  # Нормализованное
            'OCR_Телефон': [''],
            'Статус_БД': [STATUS_DB_NOT_FOUND]
        })

        # ocr_sheets с лишними пробелами
        ocr_sheets = {
            'Клиенты': pd.DataFrame({
                'ФИО': ['Чапленко  Карина'],  # Два пробела
                'Телефон': [''],
                'Адрес': ['Test Address']
            })
        }

        with tempfile.NamedTemporaryFile(suffix=".xlsx", delete=False) as tmp:
            tmp_path = tmp.name

        try:
            result_path = save_not_found_clients(verification_df, ocr_sheets, tmp_path)
            assert result_path is not None

            # Читаем файл и проверяем что полная строка подтянулась
            not_found_df = pd.read_excel(tmp_path, sheet_name="Не_найдены")
            assert len(not_found_df) == 1
            # Проверяем что есть дополнительные поля из полной строки
            assert 'Адрес' in not_found_df.columns
            assert not_found_df.iloc[0]['Адрес'] == 'Test Address'

        finally:
            if os.path.exists(tmp_path):
                os.remove(tmp_path)

    def test_fuzzy_match_with_ocr_typo(self):
        """Тест: OCR-ошибка (пропущена буква) не ломает матчинг."""
        import pandas as pd
        import tempfile
        import os
        from verify_with_db import save_not_found_clients
        from config import STATUS_DB_NOT_FOUND

        # verification_df с OCR-ошибкой
        verification_df = pd.DataFrame({
            'OCR_ФИО': ['Чаплено Карина'],  # Пропущена 'к'
            'OCR_Телефон': [''],
            'Статус_БД': [STATUS_DB_NOT_FOUND]
        })

        # ocr_sheets с правильным написанием
        ocr_sheets = {
            'Клиенты': pd.DataFrame({
                'ФИО': ['Чапленко Карина'],  # Правильное
                'Телефон': [''],
                'Email': ['test@example.com']
            })
        }

        with tempfile.NamedTemporaryFile(suffix=".xlsx", delete=False) as tmp:
            tmp_path = tmp.name

        try:
            result_path = save_not_found_clients(verification_df, ocr_sheets, tmp_path)
            assert result_path is not None

            # Читаем файл и проверяем что полная строка подтянулась несмотря на опечатку
            not_found_df = pd.read_excel(tmp_path, sheet_name="Не_найдены")
            assert len(not_found_df) == 1
            assert 'Email' in not_found_df.columns
            assert not_found_df.iloc[0]['Email'] == 'test@example.com'

        finally:
            if os.path.exists(tmp_path):
                os.remove(tmp_path)

    def test_fuzzy_match_yo_e_equivalence(self):
        """Тест: ё и е считаются одинаковыми после нормализации."""
        import pandas as pd
        import tempfile
        import os
        from verify_with_db import save_not_found_clients
        from config import STATUS_DB_NOT_FOUND

        verification_df = pd.DataFrame({
            'OCR_ФИО': ['Семёнов Пётр'],  # С ё
            'OCR_Телефон': [''],
            'Статус_БД': [STATUS_DB_NOT_FOUND]
        })

        ocr_sheets = {
            'Клиенты': pd.DataFrame({
                'ФИО': ['Семенов Петр'],  # Без ё
                'Телефон': [''],
                'ИИН': ['123456789012']
            })
        }

        with tempfile.NamedTemporaryFile(suffix=".xlsx", delete=False) as tmp:
            tmp_path = tmp.name

        try:
            result_path = save_not_found_clients(verification_df, ocr_sheets, tmp_path)
            assert result_path is not None

            not_found_df = pd.read_excel(tmp_path, sheet_name="Не_найдены")
            assert len(not_found_df) == 1
            assert 'ИИН' in not_found_df.columns
            # Excel может сохранить числовую строку как int
            assert str(not_found_df.iloc[0]['ИИН']) == '123456789012'

        finally:
            if os.path.exists(tmp_path):
                os.remove(tmp_path)

    def test_fuzzy_match_case_insensitive(self):
        """Тест: регистр не важен для матчинга."""
        import pandas as pd
        import tempfile
        import os
        from verify_with_db import save_not_found_clients
        from config import STATUS_DB_NOT_FOUND

        verification_df = pd.DataFrame({
            'OCR_ФИО': ['иванов иван'],  # Нижний регистр
            'OCR_Телефон': [''],
            'Статус_БД': [STATUS_DB_NOT_FOUND]
        })

        ocr_sheets = {
            'Клиенты': pd.DataFrame({
                'ФИО': ['ИВАНОВ ИВАН'],  # Верхний регистр
                'Телефон': [''],
                'Дата рождения': ['01.01.1990']
            })
        }

        with tempfile.NamedTemporaryFile(suffix=".xlsx", delete=False) as tmp:
            tmp_path = tmp.name

        try:
            result_path = save_not_found_clients(verification_df, ocr_sheets, tmp_path)
            assert result_path is not None

            not_found_df = pd.read_excel(tmp_path, sheet_name="Не_найдены")
            assert len(not_found_df) == 1
            assert 'Дата рождения' in not_found_df.columns
            assert not_found_df.iloc[0]['Дата рождения'] == '01.01.1990'

        finally:
            if os.path.exists(tmp_path):
                os.remove(tmp_path)

    def test_fuzzy_match_no_false_positives(self):
        """Тест: слишком разные ФИО не совпадают (избегаем ложных срабатываний)."""
        import pandas as pd
        import tempfile
        import os
        from verify_with_db import save_not_found_clients
        from config import STATUS_DB_NOT_FOUND

        verification_df = pd.DataFrame({
            'OCR_ФИО': ['Иванов Иван'],  # Совсем другое ФИО
            'OCR_Телефон': [''],
            'Статус_БД': [STATUS_DB_NOT_FOUND]
        })

        ocr_sheets = {
            'Клиенты': pd.DataFrame({
                'ФИО': ['Петров Пётр'],  # Не должно совпасть
                'Телефон': [''],
                'Email': ['should_not_match@example.com']
            })
        }

        with tempfile.NamedTemporaryFile(suffix=".xlsx", delete=False) as tmp:
            tmp_path = tmp.name

        try:
            result_path = save_not_found_clients(verification_df, ocr_sheets, tmp_path)
            assert result_path is not None

            not_found_df = pd.read_excel(tmp_path, sheet_name="Не_найдены")
            assert len(not_found_df) == 1

            # Проверяем что НЕ подтянулись поля из чужой строки
            # Если Email есть, значит произошло ложное срабатывание
            if 'Email' in not_found_df.columns:
                # Email должен быть пустым или NaN (не подтянулся)
                assert pd.isna(not_found_df.iloc[0]['Email']) or not_found_df.iloc[0]['Email'] == ''

        finally:
            if os.path.exists(tmp_path):
                os.remove(tmp_path)


class TestSubstringWordBoundary:
    """Тест защиты правила подстроки от ложноположительных."""

    def test_short_substring_ivan_vs_ivanova(self):
        """Тест: 'Иван' НЕ совпадает с 'Иванова' по подстроке (разные люди)."""
        from verify_with_db import match_names

        # "Иван" — подстрока "Иванова", но это разные люди.
        # С word-boundary правилом: {"иван"} ⊄ {"иванова"} → бонус НЕ даётся.
        # Fuzzy score ≈ 0.73 — ниже порога 0.85.
        score = match_names("Иван", "Иванова")
        assert score < 0.85, (
            f"'Иван' vs 'Иванова' дал score={score:.2f}, "
            f"ожидается < 0.85 (защита от ложноположительных)"
        )

        # Обратный порядок
        score_rev = match_names("Иванова", "Иван")
        assert score_rev < 0.85

    def test_word_boundary_correct_match(self):
        """Тест: 'Иванов' совпадает с 'Иванов Иван' по подстроке (одно лицо)."""
        from verify_with_db import match_names

        # "Иванов" — полное слово внутри "Иванов Иван"
        score = match_names("Иванов", "Иванов Иван")
        assert score >= 0.95, (
            f"'Иванов' vs 'Иванов Иван' дал score={score:.2f}, "
            f"ожидается >= 0.95 (word-boundary совпадение)"
        )


class TestOCRTextColumns:
    """Тесты наличия OCR-текстовых колонок."""

    def test_ocr_text_columns_exist(self):
        """Тест наличия всех OCR-текстовых колонок в headers."""
        from client_card_ocr import write_to_excel
        import tempfile
        import openpyxl

        # Создаем минимальные тестовые данные
        grouped_clients = {
            "client_1": {
                "name": "Тест Тестов",
                "phone": "",
                "iin": "",
                "pages": [
                    {
                        "filename": "test.jpg",
                        "page_type": "medical_card_front",
                        "data": {"fio": "Тест Тестов"},
                        "ocr_text": "Тестовый OCR текст"
                    }
                ]
            }
        }

        all_results = [
            {
                "filename": "test.jpg",
                "page_type": "medical_card_front",
                "data": {"fio": "Тест Тестов"},
                "ocr_text": "Тестовый OCR текст"
            }
        ]

        # Создаем временный файл
        with tempfile.NamedTemporaryFile(suffix=".xlsx", delete=False) as tmp:
            tmp_path = tmp.name

        try:
            # Временно меняем OUTPUT_FILE
            import config
            old_output = config.OUTPUT_FILE
            config.OUTPUT_FILE = tmp_path

            write_to_excel(grouped_clients, all_results)

            # Читаем файл
            wb = openpyxl.load_workbook(tmp_path)
            ws = wb.active
            headers = [cell.value for cell in ws[1]]

            # Проверяем наличие всех OCR-текстовых колонок
            required_columns = [
                "OCR_Текст_Лицевая",
                "OCR_Текст_Внутренняя",
                "OCR_Текст_Процедуры",
                "OCR_Текст_Покупки",
                "OCR_Текст_Комплексы",
                "OCR_Текст_Ботокс",
                "OCR_Текст_Полный"
            ]

            for col in required_columns:
                assert col in headers, f"Колонка {col} отсутствует"

            config.OUTPUT_FILE = old_output

        finally:
            # Удаляем временный файл
            if os.path.exists(tmp_path):
                os.remove(tmp_path)

    def test_ocr_text_truncation(self):
        """Тест ограничения длины OCR-текста до 32000 символов."""
        from client_card_ocr import truncate_text

        short_text = "Короткий текст"
        assert truncate_text(short_text) == short_text

        long_text = "A" * 40000
        truncated = truncate_text(long_text)

        assert len(truncated) <= 32000 + 20  # +20 для "... [ОБРЕЗАНО]"
        assert truncated.endswith("... [ОБРЕЗАНО]")

    def test_collect_ocr_texts_by_type(self):
        """Тест сбора OCR-текстов по типам страниц."""
        from client_card_ocr import collect_ocr_texts

        pages = [
            {
                "page_type": "medical_card_front",
                "ocr_text": "Текст лицевой"
            },
            {
                "page_type": "procedure_sheet",
                "ocr_text": "Текст процедур 1"
            },
            {
                "page_type": "procedure_sheet",
                "ocr_text": "Текст процедур 2"
            },
            {
                "page_type": "products_list",
                "ocr_text": "Текст покупок"
            }
        ]

        result = collect_ocr_texts(pages)

        assert "front" in result
        assert "procedures" in result
        assert "products" in result
        assert "full" in result

        assert "Текст лицевой" in result["front"]
        assert "Текст процедур 1" in result["procedures"]
        assert "Текст процедур 2" in result["procedures"]
        assert "Текст покупок" in result["products"]

        # Полный текст должен содержать все
        assert "Текст лицевой" in result["full"]
        assert "Текст процедур 1" in result["full"]
        assert "Текст покупок" in result["full"]


class TestAliasesAndFallback:
    def test_extract_identifiers_aliases(self):
        import client_card_ocr as cco

        payload = {
            "пациент": {"фио": "Семенов Петр", "телефон": "+7 700 123 45 67", "иин": "999"},
            "patient_info": {"name": "Семенов Петр", "phone": "", "iin": "888"},
            "client_name": "Семенов Петр",
        }
        normalized = cco.collect_name_phone_iin(payload)
        assert normalized.get("fio") == "Семенов Петр"
        assert normalized.get("phone") == "+7 700 123 45 67"
        assert normalized.get("iin") == "999"

    def test_group_by_client_without_fio_but_phone(self):
        import client_card_ocr as cco

        results = [
            {"page_type": "procedure_sheet", "data": {"phone": "123"}, "ocr_text": "t"},
        ]
        grouped = cco.group_by_client(results)
        assert "_unmatched" not in grouped
        assert len(grouped) == 1
        c = list(grouped.values())[0]
        assert c["name"] == "(без ФИО)"


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
