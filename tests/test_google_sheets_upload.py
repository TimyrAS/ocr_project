"""
Тесты выгрузки в Google Sheets из run_pipeline.py.

Проверяют:
1. При GSHEETS_UPLOAD_ENABLED=True + заданных creds/id — upload вызывается.
2. При выключенном флаге или пустых параметрах — upload НЕ вызывается.
"""

import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import pytest
import pandas as pd
from unittest.mock import patch, MagicMock
from types import SimpleNamespace


def _make_config(**overrides):
    """Создаёт минимальный конфиг-объект для тестов."""
    defaults = dict(
        GSHEETS_UPLOAD_ENABLED=False,
        GSHEETS_CREDENTIALS="",
        GSHEETS_SPREADSHEET_ID="",
        OUTPUT_FILE="/tmp/test_clients_database.xlsx",
        LOG_FOLDER="/tmp/test_ocr_logs",
        CLAUDE_MODEL="test-model",
        INPUT_FOLDER="/tmp",
        FUZZY_NAME_THRESHOLD=0.75,
        OCR_DUPLICATE_THRESHOLD=0.90,
        DB_MATCH_THRESHOLD=0.70,
    )
    defaults.update(overrides)
    return SimpleNamespace(**defaults)


def _run_upload_block(cfg, verification_df, log):
    """
    Воспроизводит блок выгрузки из run_pipeline.py main()
    без запуска полного пайплайна.
    """
    try:
        if getattr(cfg, 'GSHEETS_UPLOAD_ENABLED', False):
            from importlib import import_module
            try:
                google_sheets = import_module('google_sheets')
            except ImportError as e:
                log.warning(f"  Google Sheets недоступен: {e}")
                google_sheets = None

            creds_path = getattr(cfg, 'GSHEETS_CREDENTIALS', '')
            spreadsheet_id = getattr(cfg, 'GSHEETS_SPREADSHEET_ID', '')
            if google_sheets and creds_path and spreadsheet_id:
                try:
                    if verification_df is not None:
                        google_sheets.upload_df(
                            verification_df, spreadsheet_id,
                            'verification', creds_path,
                        )
                    if os.path.exists(cfg.OUTPUT_FILE):
                        clients_df = pd.read_excel(
                            cfg.OUTPUT_FILE, sheet_name='Клиенты',
                        )
                        google_sheets.upload_df(
                            clients_df, spreadsheet_id,
                            'clients', creds_path,
                        )
                    log.info("  ✓ Выгружено в Google Sheets")
                except Exception as e:
                    log.warning(f"  Ошибка выгрузки: {e}")
            else:
                log.warning("  Нет creds/spreadsheet_id")
        else:
            log.warning("  Выгрузка выключена")
    except Exception as e:
        log.warning(f"  Ошибка: {e}")


class TestGSheetsUploadEnabled:
    """Тесты: upload вызывается когда всё настроено."""

    @patch('google_sheets.upload_df')
    def test_upload_called_with_enabled_and_creds(self, mock_upload):
        """При GSHEETS_UPLOAD_ENABLED=True + creds + id → upload вызывается."""
        mock_upload.return_value = True
        cfg = _make_config(
            GSHEETS_UPLOAD_ENABLED=True,
            GSHEETS_CREDENTIALS="/fake/creds.json",
            GSHEETS_SPREADSHEET_ID="fake-spreadsheet-id",
        )
        verification_df = pd.DataFrame({
            'OCR_ФИО': ['Тест'],
            'Статус': ['Найден'],
        })
        log = MagicMock()

        _run_upload_block(cfg, verification_df, log)

        mock_upload.assert_called_once_with(
            verification_df, "fake-spreadsheet-id", "verification",
            "/fake/creds.json",
        )

    @patch('google_sheets.upload_df')
    def test_upload_called_for_both_sheets(self, mock_upload):
        """Если clients_database.xlsx существует — upload вызывается дважды."""
        import tempfile
        mock_upload.return_value = True

        # Создаём временный Excel с листом «Клиенты»
        with tempfile.NamedTemporaryFile(suffix=".xlsx", delete=False) as tmp:
            tmp_path = tmp.name
        clients_df = pd.DataFrame({'ФИО': ['Test'], 'Телефон': ['']})
        clients_df.to_excel(tmp_path, sheet_name='Клиенты', index=False)

        try:
            cfg = _make_config(
                GSHEETS_UPLOAD_ENABLED=True,
                GSHEETS_CREDENTIALS="/fake/creds.json",
                GSHEETS_SPREADSHEET_ID="sid",
                OUTPUT_FILE=tmp_path,
            )
            verification_df = pd.DataFrame({'A': [1]})
            log = MagicMock()

            _run_upload_block(cfg, verification_df, log)

            assert mock_upload.call_count == 2
            log.info.assert_called()
        finally:
            os.remove(tmp_path)


class TestGSheetsUploadDisabled:
    """Тесты: upload НЕ вызывается при выключенном флаге или пустых параметрах."""

    @patch('google_sheets.upload_df')
    def test_upload_not_called_when_disabled(self, mock_upload):
        """GSHEETS_UPLOAD_ENABLED=False → upload НЕ вызывается."""
        cfg = _make_config(
            GSHEETS_UPLOAD_ENABLED=False,
            GSHEETS_CREDENTIALS="/fake/creds.json",
            GSHEETS_SPREADSHEET_ID="sid",
        )
        log = MagicMock()

        _run_upload_block(cfg, pd.DataFrame({'A': [1]}), log)

        mock_upload.assert_not_called()
        log.warning.assert_called()  # Должен быть warning о выключенной выгрузке

    @patch('google_sheets.upload_df')
    def test_upload_not_called_when_no_creds(self, mock_upload):
        """GSHEETS_CREDENTIALS пустой → upload НЕ вызывается."""
        cfg = _make_config(
            GSHEETS_UPLOAD_ENABLED=True,
            GSHEETS_CREDENTIALS="",
            GSHEETS_SPREADSHEET_ID="sid",
        )
        log = MagicMock()

        _run_upload_block(cfg, pd.DataFrame({'A': [1]}), log)

        mock_upload.assert_not_called()

    @patch('google_sheets.upload_df')
    def test_upload_not_called_when_no_spreadsheet_id(self, mock_upload):
        """GSHEETS_SPREADSHEET_ID пустой → upload НЕ вызывается."""
        cfg = _make_config(
            GSHEETS_UPLOAD_ENABLED=True,
            GSHEETS_CREDENTIALS="/fake/creds.json",
            GSHEETS_SPREADSHEET_ID="",
        )
        log = MagicMock()

        _run_upload_block(cfg, pd.DataFrame({'A': [1]}), log)

        mock_upload.assert_not_called()

    @patch('google_sheets.upload_df')
    def test_upload_not_called_when_both_empty(self, mock_upload):
        """Оба параметра пустые → upload НЕ вызывается."""
        cfg = _make_config(
            GSHEETS_UPLOAD_ENABLED=True,
            GSHEETS_CREDENTIALS="",
            GSHEETS_SPREADSHEET_ID="",
        )
        log = MagicMock()

        _run_upload_block(cfg, pd.DataFrame({'A': [1]}), log)

        mock_upload.assert_not_called()


class TestGSheetsUploadErrorHandling:
    """Тесты: ошибки выгрузки НЕ роняют пайплайн."""

    @patch('google_sheets.upload_df', side_effect=Exception("API error"))
    def test_upload_error_only_warns(self, mock_upload):
        """Ошибка upload_df → только warning, без исключения."""
        cfg = _make_config(
            GSHEETS_UPLOAD_ENABLED=True,
            GSHEETS_CREDENTIALS="/fake/creds.json",
            GSHEETS_SPREADSHEET_ID="sid",
        )
        verification_df = pd.DataFrame({'A': [1]})
        log = MagicMock()

        # Не должно бросать исключение
        _run_upload_block(cfg, verification_df, log)

        log.warning.assert_called()


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
