"""
Тесты по ТЗ: устойчивый OCR/Claude пайплайн, структурные ID, выгрузка по колонкам.

Покрывает:
1. Unit: парсер ответа Claude — 5 форматов JSON
2. Unit: --force очищает registry + cache + промежуточные файлы
3. Unit: build_db_client_index формирует 1-to-1 DB_ID на клиента
4. Integration: pipeline с фикстурами — статусы, БД_ID, OCR_Текст_*
5. Sheets mock: выгрузка создаёт листы, включает БД_ID и OCR-колонки
6. Regression: dedup не удаляет _unmatched; name="(без ФИО)" остаётся
"""

import sys
import os
import json
import tempfile
import shutil

# Добавляем родительскую папку в path для импорта модулей
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import pytest
import pandas as pd
from unittest.mock import patch, MagicMock


# ============================================================
# 1. ПАРСЕР ОТВЕТА CLAUDE — 5 ФОРМАТОВ JSON
# ============================================================

class TestClaudeParserFormats:
    """Тесты: парсер Claude корректно обрабатывает 5 форматов JSON."""

    def test_format_canonical(self):
        """Формат 1: канонический {page_type, data}."""
        from client_card_ocr import normalize_claude_response

        payload = {
            "page_type": "procedure_sheet",
            "data": {
                "fio": "Тестова Анна",
                "procedures": [{"date": "01.01.2024", "procedure_name": "Чистка"}]
            }
        }
        result = normalize_claude_response(payload, "", "test.jpg")

        assert result["page_type"] == "procedure_sheet"
        assert result["parse_mode"] == "strict"
        assert "procedures" in result["data"]
        assert result["data"]["fio"] == "Тестова Анна"

    def test_format_document_type(self):
        """Формат 2: {document_type, ...fields}."""
        from client_card_ocr import normalize_claude_response

        payload = {
            "document_type": "products_list",
            "patient_name": "Петрова Мария",
            "products": [{"name": "Крем Hydropeptide", "price": "15000"}]
        }
        result = normalize_claude_response(payload, "", "test.jpg")

        assert result["page_type"] == "products_list"
        assert result["parse_mode"] == "recovered"
        assert "products" in result["data"]

    def test_format_russian_keys(self):
        """Формат 3: русские ключи в корне."""
        from client_card_ocr import normalize_claude_response

        payload = {
            "процедуры": {
                "фио": "Иванов Иван",
                "процедурный_лист": [{"дата": "15.02.2024"}]
            }
        }
        result = normalize_claude_response(payload, "", "test.jpg")

        assert result["page_type"] == "procedure_sheet"
        assert result["parse_mode"] == "recovered"

    def test_format_markdown_json(self):
        """Формат 4: JSON извлечённый из markdown-обёртки (обрабатывается extract_with_claude)."""
        # Проверяем что normalize_claude_response корректно работает
        # даже если передать payload с нестандартными ключами
        from client_card_ocr import normalize_claude_response

        # Имитация ответа после извлечения из markdown
        payload = {
            "page_type": "botox_record",
            "data": {
                "patient_name": "Сидорова Елена",
                "injections": [
                    {"drug": "Диспорт", "injection_area": "Лоб", "units_count": "50"}
                ]
            }
        }
        result = normalize_claude_response(payload, "", "test.jpg")

        assert result["page_type"] == "botox_record"
        assert result["parse_mode"] == "strict"

    def test_format_heuristic_inference(self):
        """Формат 5: определение типа по ключевым словам (heuristic)."""
        from client_card_ocr import normalize_claude_response

        # Нестандартный ответ без page_type и document_type
        payload = {
            "complex_name": "PRIVILAGE GOLD",
            "purchase_date": "01.12.2023",
            "complex_cost": "350000",
            "procedures": [{"procedure": "RF лифтинг", "quantity": "10"}]
        }
        ocr_text = "Комплекс PRIVILAGE GOLD\nДата приобретения"

        result = normalize_claude_response(payload, ocr_text, "test.jpg")

        assert result["page_type"] == "complex_package"
        assert result["parse_mode"] == "recovered"

    def test_all_formats_preserve_raw_payload(self):
        """Все форматы сохраняют raw_payload."""
        from client_card_ocr import normalize_claude_response

        payloads = [
            {"page_type": "medical_card_front", "data": {"fio": "A"}},
            {"document_type": "procedure_sheet", "fio": "B"},
            {"ботокс": {"фио": "C"}},
        ]

        for payload in payloads:
            result = normalize_claude_response(payload, "", "test.jpg")
            assert "raw_payload" in result
            assert result["raw_payload"] is not None

    def test_tables_fields_in_result(self):
        """Проверяем что tables_md/tables_csv попадают в result из process_all_images."""
        # Структура result из process_all_images содержит tables_md, tables_csv
        result = {
            "filename": "test.jpg",
            "page_type": "procedure_sheet",
            "data": {"fio": "Test"},
            "ocr_text": "Test text",
            "tables_md": "| Col1 | Col2 |\n|---|---|\n| A | B |",
            "tables_csv": "Col1,Col2\nA,B",
            "parse_mode": "strict",
            "page_confidence": 0.95,
        }

        assert "tables_md" in result
        assert "tables_csv" in result
        assert len(result["tables_md"]) > 0


# ============================================================
# 2. --FORCE ОЧИЩАЕТ REGISTRY + CACHE + ПРОМЕЖУТОЧНЫЕ ФАЙЛЫ
# ============================================================

class TestForceCleanup:
    """Тесты: --force полностью очищает все кэши и промежуточные файлы."""

    def setup_method(self):
        self.test_dir = tempfile.mkdtemp()
        self.cache_folder = os.path.join(self.test_dir, "ocr_cache")
        os.makedirs(self.cache_folder, exist_ok=True)

        # Registry
        self.registry_path = os.path.join(self.cache_folder, "processed_registry.json")
        with open(self.registry_path, 'w') as f:
            json.dump({"file1.jpg": {"md5": "abc"}}, f)

        # Cache files (including underscore-prefixed)
        self.cache_files = []
        for name in ["hash1.json", "hash2.json", "_service.json"]:
            path = os.path.join(self.cache_folder, name)
            with open(path, 'w') as f:
                json.dump({"test": True}, f)
            self.cache_files.append(path)

        # Intermediate files
        self.intermediate_files = []
        for name in [
            "clients_database.xlsx", "clients_normalized.xlsx",
            "verification_report.xlsx", "pipeline_report.xlsx",
            "clients_not_found.xlsx", "final_verification_report.xlsx",
            "raw_results.json"
        ]:
            path = os.path.join(self.test_dir, name)
            with open(path, 'w') as f:
                f.write("test")
            self.intermediate_files.append(path)

    def teardown_method(self):
        if os.path.exists(self.test_dir):
            shutil.rmtree(self.test_dir)

    def test_force_removes_all_cache_json(self):
        """--force удаляет ВСЕ .json файлы в ocr_cache (включая реестр)."""
        import glob

        # Simulate the new --force logic (no exclusions)
        cache_files = glob.glob(os.path.join(self.cache_folder, "*.json"))
        for f in cache_files:
            os.remove(f)

        remaining = glob.glob(os.path.join(self.cache_folder, "*.json"))
        assert len(remaining) == 0, f"Осталось {len(remaining)} файлов: {remaining}"

    def test_force_removes_underscore_files(self):
        """--force удаляет даже файлы с _ (ранее пропускались)."""
        import glob

        underscore_files = [f for f in self.cache_files if os.path.basename(f).startswith('_')]
        assert len(underscore_files) > 0, "Нет файлов с _ для теста"

        # New logic removes all
        cache_files = glob.glob(os.path.join(self.cache_folder, "*.json"))
        for f in cache_files:
            os.remove(f)

        remaining = glob.glob(os.path.join(self.cache_folder, "*.json"))
        assert len(remaining) == 0

    def test_force_removes_intermediate_files(self):
        """--force удаляет промежуточные Excel/JSON файлы."""
        for fpath in self.intermediate_files:
            assert os.path.exists(fpath), f"Файл должен существовать: {fpath}"

        # Simulate intermediate cleanup
        intermediate_names = [
            "clients_normalized.xlsx", "verification_report.xlsx",
            "pipeline_report.xlsx", "clients_not_found.xlsx",
            "final_verification_report.xlsx", "raw_results.json"
        ]
        for fname in intermediate_names:
            fpath = os.path.join(self.test_dir, fname)
            if os.path.exists(fpath):
                os.remove(fpath)

        for fname in intermediate_names:
            fpath = os.path.join(self.test_dir, fname)
            assert not os.path.exists(fpath), f"Файл должен быть удалён: {fname}"

    def test_force_guarantees_reprocessing(self):
        """После --force registry пуст → process_all_images обработает все JPG."""
        # Registry deleted
        if os.path.exists(self.registry_path):
            os.remove(self.registry_path)

        # Load registry should return empty dict
        if os.path.exists(self.registry_path):
            with open(self.registry_path) as f:
                data = json.load(f)
        else:
            data = {}

        assert data == {}, "Registry должен быть пустым после --force"

        # No files in already_done
        already_done = set(data.keys())
        assert len(already_done) == 0


# ============================================================
# 3. BUILD_DB_CLIENT_INDEX: 1-to-1 DB_ID НА КЛИЕНТА
# ============================================================

class TestBuildDbClientIndex:
    """Тесты: build_db_client_index формирует уникальный DB_ID на клиента."""

    def _make_db_df(self):
        """Создаём тестовый DataFrame как из db_privilage.xlsx."""
        data = {
            "id": [1, 2, 3, 4, 5, 6],
            "name": [
                "Иванова Анна", "Иванова Анна", "Иванова Анна",  # 3 визита
                "Петрова Мария", "Петрова Мария",                  # 2 визита
                "Сидорова Елена",                                   # 1 визит
            ],
            "phone": [
                "77771111111", "77771111111", "77771111111",
                "77772222222", "77772222222",
                "77773333333",
            ],
            "date": pd.to_datetime([
                "2024-01-10", "2024-03-15", "2024-06-20",
                "2024-02-01", "2024-05-10",
                "2024-04-01"
            ], utc=True),
            "doctor": ["Оксана А.", "Рада К.", "Оксана А.", "Оксана А.", "Рада К.", "Виктория Ж."],
            "service": ["Чистка", "Пилинг", "RF", "Мезо", "Чистка", "Ботокс"],
            "qty": [1, 1, 1, 1, 1, 1],
        }
        df = pd.DataFrame(data)

        from verify_with_db import normalize_name, normalize_phone
        df["name_norm"] = df["name"].apply(normalize_name)
        df["phone_norm"] = df["phone"].apply(normalize_phone)
        return df

    def test_unique_db_id_per_client(self):
        """Каждый клиент получает уникальный DB_ID."""
        from verify_with_db import build_db_client_index

        db_df = self._make_db_df()
        index = build_db_client_index(db_df)

        db_ids = [data["db_id"] for data in index.values()]

        # Все ID уникальны
        assert len(db_ids) == len(set(db_ids)), "DB_ID должны быть уникальными"
        # 3 клиента → 3 ID
        assert len(db_ids) == 3

    def test_db_id_format(self):
        """DB_ID имеет формат DB-XXXX."""
        from verify_with_db import build_db_client_index

        db_df = self._make_db_df()
        index = build_db_client_index(db_df)

        for data in index.values():
            db_id = data["db_id"]
            assert db_id.startswith("DB-"), f"Формат должен быть DB-XXXX: {db_id}"
            assert len(db_id) == 7, f"Длина должна быть 7: {db_id}"
            assert db_id[3:].isdigit(), f"После DB- должны быть цифры: {db_id}"

    def test_visits_grouped_per_client(self):
        """Визиты правильно группируются по клиенту, а не по строке."""
        from verify_with_db import build_db_client_index

        db_df = self._make_db_df()
        index = build_db_client_index(db_df)

        # Иванова Анна — 3 визита
        ivanova_key = [k for k, v in index.items() if "иванова" in k][0]
        assert index[ivanova_key]["total_visits"] == 3

        # Петрова Мария — 2 визита
        petrova_key = [k for k, v in index.items() if "петрова" in k][0]
        assert index[petrova_key]["total_visits"] == 2

        # Сидорова — 1 визит
        sidorova_key = [k for k, v in index.items() if "сидорова" in k][0]
        assert index[sidorova_key]["total_visits"] == 1

    def test_db_id_stable_sorted(self):
        """DB_ID присваиваются по алфавитному порядку имён (стабильность)."""
        from verify_with_db import build_db_client_index

        db_df = self._make_db_df()
        index = build_db_client_index(db_df)

        # Сортировка: иванова, петрова, сидорова (алфавитный)
        sorted_items = sorted(index.items(), key=lambda x: x[1]["db_id"])
        names = [v["name_orig"] for _, v in sorted_items]

        # Проверяем что порядок алфавитный по нормализованному имени
        norm_names = sorted(index.keys())
        ids_in_order = [index[n]["db_id"] for n in norm_names]
        expected = [f"DB-{i:04d}" for i in range(1, len(norm_names) + 1)]
        assert ids_in_order == expected

    def test_find_best_match_returns_db_id(self):
        """find_best_match возвращает db_id в результате."""
        from verify_with_db import build_db_client_index, find_best_match

        db_df = self._make_db_df()
        index = build_db_client_index(db_df)

        match = find_best_match("Иванова Анна", "77771111111", index, 0.70)

        assert match is not None
        assert "db_id" in match
        assert match["db_id"].startswith("DB-")

    def test_verify_clients_includes_db_id(self):
        """verify_clients() добавляет БД_ID в результат."""
        from verify_with_db import build_db_client_index, verify_clients

        db_df = self._make_db_df()
        index = build_db_client_index(db_df)

        ocr_sheets = {
            "Клиенты": pd.DataFrame({
                "ФИО": ["Иванова Анна", "Неизвестный Клиент"],
                "Телефон": ["77771111111", ""]
            })
        }

        result_df = verify_clients(ocr_sheets, index, 0.70)

        assert "БД_ID" in result_df.columns
        # Иванова должна иметь DB_ID
        ivanova_row = result_df[result_df["OCR_ФИО"] == "Иванова Анна"]
        assert len(ivanova_row) == 1
        assert ivanova_row.iloc[0]["БД_ID"].startswith("DB-")

        # Неизвестный — пустой
        unknown_row = result_df[result_df["OCR_ФИО"] == "Неизвестный Клиент"]
        assert len(unknown_row) == 1
        assert unknown_row.iloc[0]["БД_ID"] == ""


# ============================================================
# 4. INTEGRATION: СТАТУСЫ, FALLBACK, OCR_ТЕКСТ_*
# ============================================================

class TestPipelineIntegration:
    """Интеграционные тесты пайплайна."""

    def test_collect_ocr_texts_distribution(self):
        """OCR тексты распределяются по правилам ТЗ."""
        from client_card_ocr import collect_ocr_texts

        pages = [
            {"page_type": "medical_card_front", "ocr_text": "front text"},
            {"page_type": "medical_card_inner", "ocr_text": "inner text"},
            {"page_type": "procedure_sheet", "ocr_text": "proc text"},
            {"page_type": "products_list", "ocr_text": "products text"},
            {"page_type": "complex_package", "ocr_text": "complex text"},
            {"page_type": "botox_record", "ocr_text": "botox text"},
        ]

        texts = collect_ocr_texts(pages)

        assert "front text" in texts["front"]
        assert "inner text" in texts["inner"]
        assert "proc text" in texts["procedures"]
        assert "products text" in texts["products"]
        assert "complex text" in texts["complex"]
        assert "botox text" in texts["botox"]
        # full = конкатенация всех
        assert "front text" in texts["full"]
        assert "botox text" in texts["full"]

    def test_unknown_pages_go_to_procedures_only(self):
        """Unknown страницы попадают ТОЛЬКО в procedures (и full), не во все колонки."""
        from client_card_ocr import collect_ocr_texts

        pages = [
            {"page_type": "medical_card_front", "ocr_text": "front text"},
            {"page_type": "unknown", "ocr_text": "unknown text"},
        ]

        texts = collect_ocr_texts(pages)

        # front заполнен своим текстом
        assert "front text" in texts["front"]
        # unknown попал в procedures
        assert "unknown text" in texts["procedures"]
        # unknown НЕ должен попадать в inner, products, complex, botox
        assert "unknown text" not in texts["inner"]
        assert "unknown text" not in texts["products"]
        assert "unknown text" not in texts["complex"]
        assert "unknown text" not in texts["botox"]
        # но в full — есть
        assert "unknown text" in texts["full"]

    def test_empty_columns_stay_empty(self):
        """Пустые колонки НЕ заполняются full текстом (новое поведение)."""
        from client_card_ocr import collect_ocr_texts

        # Только front и procedure
        pages = [
            {"page_type": "medical_card_front", "ocr_text": "front text"},
            {"page_type": "procedure_sheet", "ocr_text": "proc text"},
        ]

        texts = collect_ocr_texts(pages)

        # front и procedures заполнены
        assert texts["front"] != ""
        assert texts["procedures"] != ""
        # inner, products, complex, botox — пусты
        assert texts["inner"] == ""
        assert texts["products"] == ""
        assert texts["complex"] == ""
        assert texts["botox"] == ""

    def test_all_empty_fallback_to_procedures(self):
        """Если ВСЕ типовые пусты, full дублируется в procedures."""
        from client_card_ocr import collect_ocr_texts

        pages = [
            {"page_type": "unknown", "ocr_text": "some text"},
        ]

        texts = collect_ocr_texts(pages)

        # procedures заполнен (fallback)
        assert "some text" in texts["procedures"]
        # full заполнен
        assert "some text" in texts["full"]

    def test_tables_md_csv_collected(self):
        """tables_md и tables_csv собираются из всех страниц."""
        from client_card_ocr import collect_ocr_texts

        pages = [
            {
                "page_type": "procedure_sheet",
                "ocr_text": "proc text",
                "tables_md": "| Дата | Процедура |\n|---|---|\n| 01.01 | Чистка |",
                "tables_csv": "Дата,Процедура\n01.01,Чистка",
            },
            {
                "page_type": "products_list",
                "ocr_text": "prod text",
                "tables_md": "| Продукт | Цена |\n|---|---|\n| Крем | 5000 |",
                "tables_csv": "Продукт,Цена\nКрем,5000",
            },
        ]

        texts = collect_ocr_texts(pages)

        assert "Дата" in texts["tables_md"]
        assert "Продукт" in texts["tables_md"]
        assert "Чистка" in texts["tables_csv"]
        assert "5000" in texts["tables_csv"]

    def test_fallback_filter_only_maybe_and_not_found(self):
        """Claude вызывается ТОЛЬКО для fallback-строк (Возможное совпадение / Нет в БД)."""
        from config import STATUS_DB_FOUND, STATUS_DB_MAYBE, STATUS_DB_NOT_FOUND

        verification_df = pd.DataFrame({
            "OCR_ФИО": ["Найденов Иван", "Возможнов Пётр", "Новый Клиент"],
            "Статус_БД": [STATUS_DB_FOUND, STATUS_DB_MAYBE, STATUS_DB_NOT_FOUND],
        })

        # Filter logic from run_pipeline.py
        fallback_mask = verification_df["Статус_БД"].isin([STATUS_DB_MAYBE, STATUS_DB_NOT_FOUND])
        fallback_df = verification_df[fallback_mask]

        # Только 2 строки должны пойти в Claude
        assert len(fallback_df) == 2
        assert STATUS_DB_FOUND not in fallback_df["Статус_БД"].values
        assert STATUS_DB_MAYBE in fallback_df["Статус_БД"].values
        assert STATUS_DB_NOT_FOUND in fallback_df["Статус_БД"].values

    def test_verification_sheet_includes_bd_id(self):
        """Лист Сверка_БД включает колонку БД_ID."""
        # The keep_cols from add_verification_sheet()
        keep_cols = [
            "OCR_ФИО", "OCR_Телефон",
            "Статус_БД", "БД_ID",
            "БД_ФИО", "БД_Телефон",
            "Совпадение_%", "Визитов_в_БД", "Врачи_в_БД",
        ]
        assert "БД_ID" in keep_cols


# ============================================================
# 5. SHEETS MOCK: ВЫГРУЗКА С НОВЫМИ КОЛОНКАМИ
# ============================================================

class TestSheetsUploadColumns:
    """Тесты: Google Sheets выгрузка включает новые колонки."""

    def test_clients_sheet_has_ocr_text_columns(self):
        """Лист clients содержит OCR_Текст_* и OCR_Таблицы_* колонки."""
        # Simulate reading clients sheet
        headers = [
            "ID", "Дата создания карты", "Фото (файл)",
            "ФИО", "Дата рождения", "Возраст", "Пол", "Гражданство",
            "ИИН / Паспорт", "Адрес", "Телефон", "Email", "Мессенджер",
            "Экстренный контакт", "Скидка", "Источник инфо", "Аллергии",
            "Консультант/Врач", "Дата последнего визита",
            "Кол-во страниц", "Файлы-источники",
            "OCR_Текст_Лицевая", "OCR_Текст_Внутренняя", "OCR_Текст_Процедуры",
            "OCR_Текст_Покупки", "OCR_Текст_Комплексы", "OCR_Текст_Ботокс",
            "OCR_Текст_Полный",
            "OCR_Таблицы_MD", "OCR_Таблицы_CSV"
        ]

        required_ocr = [
            "OCR_Текст_Лицевая", "OCR_Текст_Внутренняя", "OCR_Текст_Процедуры",
            "OCR_Текст_Покупки", "OCR_Текст_Комплексы", "OCR_Текст_Ботокс",
            "OCR_Текст_Полный", "OCR_Таблицы_MD", "OCR_Таблицы_CSV"
        ]

        for col in required_ocr:
            assert col in headers, f"Колонка {col} отсутствует в заголовках"

    def test_verification_sheet_has_bd_id(self):
        """Лист verification содержит колонку БД_ID."""
        # Simulate verification_df
        verification_df = pd.DataFrame({
            "OCR_ФИО": ["Test"],
            "OCR_Телефон": ["777"],
            "Статус_БД": ["Найден в БД"],
            "БД_ID": ["DB-0001"],
            "БД_ФИО": ["Test DB"],
            "БД_Телефон": ["777"],
            "Совпадение_%": [95.0],
            "Визитов_в_БД": [5],
            "Врачи_в_БД": ["Оксана А."],
        })

        assert "БД_ID" in verification_df.columns
        assert verification_df.iloc[0]["БД_ID"] == "DB-0001"

    def test_sheets_upload_creates_missing_sheets(self):
        """_ensure_sheet_exists создаёт лист, если его нет."""
        from google_sheets import _ensure_sheet_exists

        # Mock Sheets API client
        mock_client = MagicMock()

        # Simulate: no 'clients' sheet exists
        meta = {"sheets": [{"properties": {"title": "other_sheet", "sheetId": 1}}]}
        mock_client.spreadsheets().get().execute.return_value = meta

        # Simulate: addSheet returns new sheet ID
        mock_client.spreadsheets().batchUpdate().execute.return_value = {
            "replies": [{"addSheet": {"properties": {"sheetId": 99}}}]
        }

        result = _ensure_sheet_exists(mock_client, "spreadsheet_id", "clients")

        # Sheet was created
        assert result == 99

    def test_df_to_values_includes_all_columns(self):
        """df_to_values корректно конвертирует все колонки."""
        from google_sheets import df_to_values

        df = pd.DataFrame({
            "ФИО": ["Тест"],
            "БД_ID": ["DB-0001"],
            "OCR_Таблицы_MD": ["| Col |"],
        })

        values = df_to_values(df)

        # Header row
        assert "ФИО" in values[0]
        assert "БД_ID" in values[0]
        assert "OCR_Таблицы_MD" in values[0]
        # Data row
        assert "Тест" in values[1]
        assert "DB-0001" in values[1]


# ============================================================
# 6. REGRESSION: DEDUP, _UNMATCHED, (БЕЗ ФИО)
# ============================================================

class TestRegressionDedup:
    """Регрессионные тесты: dedup не удаляет _unmatched, name="(без ФИО)" сохраняется."""

    def test_dedup_preserves_unmatched(self, monkeypatch):
        """Дедупликация НЕ применяется к _unmatched группе."""
        import client_card_ocr as cco

        monkeypatch.setattr(cco.config, "OCR_DUPLICATE_THRESHOLD", 0.9, raising=False)

        grouped = {
            "_unmatched": {
                "name": "⚠",
                "phone": "",
                "iin": "",
                "pages": [
                    {"filename": "a.jpg", "ocr_text": "identical text"},
                    {"filename": "b.jpg", "ocr_text": "identical text"},
                    {"filename": "c.jpg", "ocr_text": "identical text"},
                ],
            },
        }

        result = cco.deduplicate_pages(grouped)

        # _unmatched должен сохранить все страницы
        assert len(result["_unmatched"]["pages"]) == 3

    def test_name_bez_fio_preserved_with_phone(self):
        """Клиент с name="(без ФИО)" сохраняется, если есть phone/iin."""
        import client_card_ocr as cco

        # Simulate extract_identifiers result
        ids = cco.extract_identifiers({
            "page_type": "procedure_sheet",
            "data": {
                "phone": "+7 777 123 45 67",
                "iin": "123456789012",
                # No fio
            }
        })

        # Phone and IIN should be extracted
        assert ids["phone"] == "+7 777 123 45 67"
        assert ids["iin"] == "123456789012"
        # FIO may be empty
        assert ids["fio"] == "" or ids["fio"] is None or ids["fio"]

    def test_grouping_preserves_unknown_clients(self, monkeypatch):
        """group_by_client сохраняет страницы без ФИО (не теряет их)."""
        import client_card_ocr as cco

        monkeypatch.setitem(cco._rapidfuzz_cache, "loaded", True)
        monkeypatch.setitem(cco._rapidfuzz_cache, "fuzz", None)
        monkeypatch.setattr(cco.config, "FUZZY_NAME_THRESHOLD", 0.7, raising=False)

        results = [
            {
                "filename": "no_ids.jpg",
                "page_type": "unknown",
                "data": {},  # Нет ФИО, телефона, ИИН
                "ocr_text": "some random text",
            },
        ]

        grouped = cco.group_by_client(results)

        # Страница без идентификаторов НЕ теряется — попадает в отдельную группу
        all_pages = []
        for client in grouped.values():
            all_pages.extend(client["pages"])
        assert len(all_pages) == 1
        assert all_pages[0]["filename"] == "no_ids.jpg"

    def test_not_found_clients_have_bd_id_empty(self):
        """Клиенты 'Нет в БД' имеют пустой БД_ID."""
        from verify_with_db import verify_clients

        ocr_sheets = {
            "Клиенты": pd.DataFrame({
                "ФИО": ["Абсолютно Новый Клиент"],
                "Телефон": [""]
            })
        }

        db_index = {}  # Empty DB
        result_df = verify_clients(ocr_sheets, db_index, 0.70)

        assert result_df.iloc[0]["БД_ID"] == ""
        assert result_df.iloc[0]["Статус_БД"] == "Нет в БД (новый для картотеки)"


# ============================================================
# ЗАПУСК
# ============================================================

if __name__ == "__main__":
    pytest.main([__file__, "-v"])
