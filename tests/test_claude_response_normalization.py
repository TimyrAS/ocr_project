"""
Тесты для нормализации ответов Claude API.

Проверяют устойчивость парсера к различным форматам ответов.
"""

import sys
import os

# Добавляем родительскую папку в path для импорта модулей
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import pytest
from client_card_ocr import normalize_claude_response, infer_page_type_from_content


class TestClaudeResponseNormalization:
    """Тесты нормализации ответов Claude."""

    def test_canonical_format(self):
        """Тест канонического формата {page_type, data}."""
        payload = {
            "page_type": "medical_card_front",
            "data": {
                "fio": "Иванова Анна Петровна",
                "phone": "+7 777 123 45 67",
                "birth_date": "15.03.1985"
            }
        }

        result = normalize_claude_response(payload, "", "test.jpg")

        assert result["page_type"] == "medical_card_front"
        assert result["data"]["fio"] == "Иванова Анна Петровна"
        assert result["parse_mode"] == "strict"
        assert "raw_payload" in result

    def test_document_type_format(self):
        """Тест формата с document_type."""
        payload = {
            "document_type": "medical_card_front",
            "fio": "Петров Иван Сергеевич",
            "phone": "+7 777 999 88 77",
            "iin": "123456789012"
        }

        result = normalize_claude_response(payload, "", "test.jpg")

        assert result["page_type"] == "medical_card_front"
        assert result["data"]["fio"] == "Петров Иван Сергеевич"
        assert result["data"]["phone"] == "+7 777 999 88 77"
        assert result["parse_mode"] == "recovered"

    def test_russian_keys_format(self):
        """Тест формата с русскими ключами."""
        payload = {
            "медицинская_карта": {
                "фио": "Сидорова Мария",
                "телефон": "87771234567",
                "дата_рождения": "20.05.1990"
            }
        }

        result = normalize_claude_response(payload, "", "test.jpg")

        assert result["page_type"] == "medical_card_front"
        assert result["data"]["фио"] == "Сидорова Мария"
        assert result["parse_mode"] == "recovered"

    def test_invalid_page_type(self):
        """Тест некорректного page_type."""
        payload = {
            "page_type": "invalid_type_123",
            "data": {"fio": "Test"}
        }

        result = normalize_claude_response(payload, "", "test.jpg")

        assert result["page_type"] == "unknown"
        assert result["parse_mode"] == "strict"

    def test_procedure_sheet_inference(self):
        """Тест определения типа по содержимому - процедурный лист."""
        payload = {
            "procedures": [
                {
                    "date": "10.01.2024",
                    "procedure_name": "Чистка лица",
                    "cost": "15000"
                }
            ]
        }

        ocr_text = "Процедурный лист\nЧистка лица 15000 тг"

        result = normalize_claude_response(payload, ocr_text, "test.jpg")

        assert result["page_type"] == "procedure_sheet"
        assert result["parse_mode"] in ["recovered", "fallback"]

    def test_botox_inference(self):
        """Тест определения типа ботокс по ключевым словам."""
        payload = {
            "injections": [
                {
                    "drug": "Диспорт",
                    "injection_area": "Лоб",
                    "units_count": "50"
                }
            ]
        }

        ocr_text = "Ботулинический токсин\nДиспорт Лоб 50 единиц"

        result = normalize_claude_response(payload, ocr_text, "test.jpg")

        assert result["page_type"] == "botox_record"

    def test_complex_package_inference(self):
        """Тест определения типа комплекс."""
        payload = {
            "complex_name": "PRIVILAGE",
            "purchase_date": "01.12.2023",
            "complex_cost": "350000"
        }

        ocr_text = "Комплекс PRIVILAGE\nДата покупки: 01.12.2023"

        result = normalize_claude_response(payload, ocr_text, "test.jpg")

        assert result["page_type"] == "complex_package"

    def test_unknown_format(self):
        """Тест полностью нераспознанного формата."""
        payload = {
            "random_field": "random_value",
            "another_field": 123
        }

        result = normalize_claude_response(payload, "", "test.jpg")

        assert result["page_type"] == "unknown"
        assert result["parse_mode"] == "fallback"
        assert result["data"] == payload

    def test_empty_payload(self):
        """Тест пустого payload."""
        result = normalize_claude_response({}, "", "test.jpg")

        assert result["page_type"] == "unknown"
        assert result["parse_mode"] == "fallback"
        assert result["data"] == {}

    def test_none_payload(self):
        """Тест None payload."""
        result = normalize_claude_response(None, "", "test.jpg")

        assert result["page_type"] == "unknown"
        assert result["parse_mode"] == "fallback"

    def test_long_russian_document_type_products(self):
        """Тест длинной русской фразы для products_list."""
        payload = {
            "document_type": "список приобретенных средств для домашнего ухода",
            "products": [
                {"name": "Крем", "price": "5000"}
            ]
        }

        result = normalize_claude_response(payload, "", "test.jpg")

        assert result["page_type"] == "products_list"
        assert result["parse_mode"] == "recovered"

    def test_long_russian_document_type_botox(self):
        """Тест длинной русской фразы для botox_record."""
        payload = {
            "document_type": "ботулинический токсин",
            "injections": [
                {"drug": "Диспорт", "area": "Лоб"}
            ]
        }

        result = normalize_claude_response(payload, "", "test.jpg")

        assert result["page_type"] == "botox_record"
        assert result["parse_mode"] == "recovered"

    def test_raw_payload_and_parse_mode_preserved(self):
        """Тест что raw_payload и parse_mode сохраняются."""
        payload = {"page_type": "medical_card_front", "data": {"fio": "Test"}}

        result = normalize_claude_response(payload, "", "test.jpg")

        assert "raw_payload" in result
        assert result["raw_payload"] == payload
        assert "parse_mode" in result
        assert result["parse_mode"] == "strict"


class TestInferPageType:
    """Тесты функции определения типа страницы."""

    def test_medical_card_front_inference(self):
        """Тест определения лицевой стороны медкарты."""
        payload = {
            "fio": "Тестов Тест",
            "birth_date": "01.01.2000",
            "iin": "000101000000",
            "citizenship": "РК",
            "allergies": "Нет"
        }

        ocr_text = "ФИО Дата рождения ИИН Гражданство Аллергии"

        result = infer_page_type_from_content(payload, ocr_text, "test.jpg")

        assert result == "medical_card_front"

    def test_medical_card_inner_inference(self):
        """Тест определения внутренней стороны медкарты."""
        payload = {
            "complaints": "Жалобы на...",
            "objective_status": "Объективный статус",
            "diagnosis": "Предварительный диагноз",
            "blood_pressure": "120/80"
        }

        ocr_text = "Жалобы Объективный статус Диагноз Давление"

        result = infer_page_type_from_content(payload, ocr_text, "test.jpg")

        assert result == "medical_card_inner"

    def test_insufficient_indicators(self):
        """Тест недостаточного количества индикаторов."""
        payload = {"name": "Test"}
        ocr_text = "Some random text"

        result = infer_page_type_from_content(payload, ocr_text, "test.jpg")

        assert result == "unknown"


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
