"""Helpers for uploading DataFrames to Google Sheets via service account."""

import json
import logging
from pathlib import Path
from typing import Optional

import pandas as pd

try:
    from google.oauth2 import service_account
    from googleapiclient.discovery import build
    from googleapiclient.errors import HttpError
except ImportError:  # pragma: no cover
    service_account = None
    build = None
    HttpError = None


SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]

log = logging.getLogger(__name__)


def load_client(creds_path: str):
    if service_account is None:
        raise ImportError("google-api-python-client not installed")
    creds = service_account.Credentials.from_service_account_file(creds_path, scopes=SCOPES)
    return build("sheets", "v4", credentials=creds)


def df_to_values(df: pd.DataFrame):
    # Convert DataFrame to list-of-lists with header
    return [list(df.columns)] + df.fillna("").astype(str).values.tolist()


def _ensure_sheet_exists(client, spreadsheet_id: str, sheet_name: str):
    """
    Проверяет, существует ли лист. Если нет — создаёт через batchUpdate addSheet.
    Возвращает sheetId (int).
    """
    # Получаем список существующих листов
    meta = client.spreadsheets().get(spreadsheetId=spreadsheet_id, fields="sheets.properties").execute()
    for s in meta.get("sheets", []):
        props = s.get("properties", {})
        if props.get("title") == sheet_name:
            log.info(f"  Лист '{sheet_name}' найден (id={props.get('sheetId')})")
            return props.get("sheetId")

    # Лист не найден — создаём
    log.info(f"  Лист '{sheet_name}' не найден, создаём...")
    body = {
        "requests": [{
            "addSheet": {
                "properties": {"title": sheet_name}
            }
        }]
    }
    resp = client.spreadsheets().batchUpdate(spreadsheetId=spreadsheet_id, body=body).execute()
    new_id = resp["replies"][0]["addSheet"]["properties"]["sheetId"]
    log.info(f"  ✓ Лист '{sheet_name}' создан (id={new_id})")
    return new_id


def upload_df(df: pd.DataFrame, spreadsheet_id: str, sheet_name: str, creds_path: str, clear: bool = True):
    client = load_client(creds_path)

    # Убеждаемся что лист существует (создаём при необходимости)
    _ensure_sheet_exists(client, spreadsheet_id, sheet_name)

    values = df_to_values(df)
    body = {"values": values}
    range_name = f"{sheet_name}!A1"
    if clear:
        client.spreadsheets().values().clear(spreadsheetId=spreadsheet_id, range=sheet_name).execute()
    client.spreadsheets().values().update(
        spreadsheetId=spreadsheet_id,
        range=range_name,
        valueInputOption="RAW",
        body=body,
    ).execute()
    return True
