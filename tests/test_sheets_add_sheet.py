"""
Тесты устойчивости google_sheets.upload_df к отсутствующим листам.

Проверяют:
1. Если лист существует — upload работает без addSheet.
2. Если листа нет — создаёт через batchUpdate addSheet, затем clear+update.
3. Ошибка API не роняет вызывающий код.
"""

import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import pytest
import pandas as pd
from unittest.mock import patch, MagicMock, call


def _mock_client_with_sheets(existing_sheets):
    """
    Создаёт mock Google Sheets client с заданными существующими листами.
    existing_sheets: list of {"title": str, "sheetId": int}
    """
    client = MagicMock()

    # spreadsheets().get() — возвращает metadata
    sheets_meta = [
        {"properties": {"title": s["title"], "sheetId": s["sheetId"]}}
        for s in existing_sheets
    ]
    client.spreadsheets().get.return_value.execute.return_value = {
        "sheets": sheets_meta
    }

    # spreadsheets().batchUpdate() — для addSheet
    client.spreadsheets().batchUpdate.return_value.execute.return_value = {
        "replies": [{"addSheet": {"properties": {"sheetId": 999, "title": "new_sheet"}}}]
    }

    # spreadsheets().values().clear() / update()
    client.spreadsheets().values().clear.return_value.execute.return_value = {}
    client.spreadsheets().values().update.return_value.execute.return_value = {}

    return client


class TestSheetExists:
    """Тесты: лист уже существует."""

    @patch('google_sheets.load_client')
    def test_upload_existing_sheet_no_add(self, mock_load):
        """Лист 'verification' существует → addSheet НЕ вызывается."""
        from google_sheets import upload_df

        client = _mock_client_with_sheets([{"title": "verification", "sheetId": 1}])
        mock_load.return_value = client

        df = pd.DataFrame({"A": [1, 2], "B": ["x", "y"]})
        result = upload_df(df, "spreadsheet-id", "verification", "/fake/creds.json")

        assert result is True
        # batchUpdate (addSheet) не должен вызываться
        client.spreadsheets().batchUpdate.assert_not_called()
        # clear и update должны быть вызваны
        client.spreadsheets().values().clear.assert_called_once()
        client.spreadsheets().values().update.assert_called_once()


class TestSheetMissing:
    """Тесты: листа нет — создаётся через addSheet."""

    @patch('google_sheets.load_client')
    def test_missing_sheet_creates_via_add_sheet(self, mock_load):
        """Листа 'verification' нет → addSheet вызывается, затем clear+update."""
        from google_sheets import upload_df

        client = _mock_client_with_sheets([])  # Нет листов
        mock_load.return_value = client

        df = pd.DataFrame({"A": [1]})
        result = upload_df(df, "sid", "verification", "/fake/creds.json")

        assert result is True
        # batchUpdate (addSheet) должен вызываться
        client.spreadsheets().batchUpdate.assert_called_once()
        call_body = client.spreadsheets().batchUpdate.call_args
        add_req = call_body[1]["body"]["requests"][0]["addSheet"]
        assert add_req["properties"]["title"] == "verification"

        # После создания — clear и update
        client.spreadsheets().values().clear.assert_called_once()
        client.spreadsheets().values().update.assert_called_once()

    @patch('google_sheets.load_client')
    def test_missing_sheet_among_others(self, mock_load):
        """Есть другие листы, но нужного нет → addSheet вызывается."""
        from google_sheets import upload_df

        client = _mock_client_with_sheets([
            {"title": "clients", "sheetId": 1},
            {"title": "procedures", "sheetId": 2},
        ])
        mock_load.return_value = client

        df = pd.DataFrame({"X": [10]})
        result = upload_df(df, "sid", "verification", "/fake/creds.json")

        assert result is True
        client.spreadsheets().batchUpdate.assert_called_once()


class TestSheetNoClear:
    """Тесты: clear=False пропускает очистку."""

    @patch('google_sheets.load_client')
    def test_no_clear_skips_clear_call(self, mock_load):
        """clear=False → values().clear() НЕ вызывается."""
        from google_sheets import upload_df

        client = _mock_client_with_sheets([{"title": "test", "sheetId": 1}])
        mock_load.return_value = client

        df = pd.DataFrame({"A": [1]})
        result = upload_df(df, "sid", "test", "/fake/creds.json", clear=False)

        assert result is True
        client.spreadsheets().values().clear.assert_not_called()
        client.spreadsheets().values().update.assert_called_once()


class TestEnsureSheetExists:
    """Тесты _ensure_sheet_exists напрямую."""

    @patch('google_sheets.load_client')
    def test_ensure_returns_existing_id(self, mock_load):
        """Существующий лист → возвращает его sheetId."""
        from google_sheets import _ensure_sheet_exists

        client = _mock_client_with_sheets([{"title": "my_sheet", "sheetId": 42}])

        sheet_id = _ensure_sheet_exists(client, "sid", "my_sheet")
        assert sheet_id == 42
        client.spreadsheets().batchUpdate.assert_not_called()

    @patch('google_sheets.load_client')
    def test_ensure_creates_missing(self, mock_load):
        """Отсутствующий лист → создаётся, возвращает новый sheetId."""
        from google_sheets import _ensure_sheet_exists

        client = _mock_client_with_sheets([])

        sheet_id = _ensure_sheet_exists(client, "sid", "new_sheet")
        assert sheet_id == 999
        client.spreadsheets().batchUpdate.assert_called_once()


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
