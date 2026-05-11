"""
Тесты пользовательских ошибок runtime: невосстановимые ошибки Claude и
заблокированный Excel-файл.
"""

from types import SimpleNamespace

import pytest


class TestClaudeNonRetriableErrors:
    def test_low_credit_error_is_non_retriable(self):
        from client_card_ocr import _is_non_retriable_claude_error

        exc = Exception(
            "Error code: 400 - Your credit balance is too low to access "
            "the Anthropic API."
        )

        assert _is_non_retriable_claude_error(exc) is True

    def test_transient_error_is_retriable(self):
        from client_card_ocr import _is_non_retriable_claude_error

        assert _is_non_retriable_claude_error(Exception("timeout")) is False

    def test_process_all_images_stops_after_first_low_credit_error(
        self, tmp_path, monkeypatch
    ):
        import config
        import client_card_ocr

        photo_dir = tmp_path / "Photo"
        photo_dir.mkdir()
        image_path = photo_dir / "card.jpg"
        image_path.write_bytes(b"fake-image")

        monkeypatch.setattr(config, "INPUT_FOLDER", str(photo_dir))
        monkeypatch.setattr(config, "CACHE_FOLDER", str(tmp_path / "cache"))
        monkeypatch.setattr(
            config,
            "PROCESSED_REGISTRY",
            str(tmp_path / "processed_registry.json"),
            raising=False,
        )
        monkeypatch.setattr(config, "MAX_RETRIES", 3)
        monkeypatch.setattr(config, "API_DELAY", 0)

        calls = {"ocr": 0, "claude": 0}

        def fake_ocr(_vision_client, _img_path):
            calls["ocr"] += 1
            return SimpleNamespace(
                full_text="ocr text",
                enhanced_text="ocr text",
                tables_md="",
                tables_csv="",
                page_confidence=0.9,
            )

        def fake_extract(_claude_client, _img_path, _ocr_text):
            calls["claude"] += 1
            raise Exception(
                "Error code: 400 - Your credit balance is too low to access "
                "the Anthropic API."
            )

        monkeypatch.setattr(client_card_ocr, "ocr_image_structured", fake_ocr)
        monkeypatch.setattr(client_card_ocr, "extract_with_claude", fake_extract)

        results = client_card_ocr.process_all_images(None, None)

        assert calls == {"ocr": 1, "claude": 1}
        assert len(results) == 1
        assert results[0]["page_type"] == "error"
        assert results[0]["data"]["non_retriable"] is True


class TestExcelPermissionHandling:
    def test_write_to_excel_raises_clear_permission_error(self, tmp_path, monkeypatch):
        import config
        import client_card_ocr
        from openpyxl.workbook.workbook import Workbook

        monkeypatch.setattr(config, "OUTPUT_FILE", str(tmp_path / "clients.xlsx"))

        def locked_save(self, filename):
            raise PermissionError("file is locked")

        monkeypatch.setattr(Workbook, "save", locked_save)

        grouped_clients = {
            "client": {
                "name": "Тестовый Клиент",
                "iin": "",
                "phone": "",
                "pages": [
                    {
                        "page_type": "medical_card_front",
                        "filename": "card.jpg",
                        "data": {"fio": "Тестовый Клиент"},
                    }
                ],
            }
        }

        with pytest.raises(PermissionError, match="Закройте"):
            client_card_ocr.write_to_excel(grouped_clients, [], price_index=None)
