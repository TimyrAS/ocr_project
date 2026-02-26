"""
Тесты детерминированного smoke-прогона.

1. Helper-функции: _is_smoke_mode, _gsheets_disabled — логика флагов.
2. BadZipFile guard: add_verification_sheet / enrich_clients_with_db_match
   с повреждённым файлом завершаются warning, не исключением.
3. Интеграция: SMOKE_MODE=true → Google Sheets не вызывается, pipeline exit 0.
4. quality_baseline.py задаёт все 3 env var в smoke-команде.
"""

import logging
import os
import subprocess
import sys
import types
from pathlib import Path

import pandas as pd
import pytest

PROJECT_DIR = Path(__file__).resolve().parent.parent


# ============================================================
# 1. HELPER-ФУНКЦИИ: _is_smoke_mode, _gsheets_disabled
# ============================================================


class TestSmokeHelpers:
    """Unit-тесты для _is_smoke_mode() и _gsheets_disabled()."""

    @pytest.fixture(autouse=True)
    def clean_env(self, monkeypatch):
        """Убираем SMOKE_MODE и GSHEETS_UPLOAD_ENABLED из env перед каждым тестом."""
        monkeypatch.delenv("SMOKE_MODE", raising=False)
        monkeypatch.delenv("GSHEETS_UPLOAD_ENABLED", raising=False)

    def _reload(self):
        import importlib
        import run_pipeline
        importlib.reload(run_pipeline)
        return run_pipeline

    # ── _is_smoke_mode ──────────────────────────────────────

    def test_smoke_mode_false_by_default(self):
        rp = self._reload()
        assert rp._is_smoke_mode() is False

    @pytest.mark.parametrize("val", ["true", "True", "TRUE", "1", "yes", "on"])
    def test_smoke_mode_truthy_values(self, monkeypatch, val):
        monkeypatch.setenv("SMOKE_MODE", val)
        rp = self._reload()
        assert rp._is_smoke_mode() is True

    @pytest.mark.parametrize("val", ["false", "0", "no", "off", ""])
    def test_smoke_mode_falsy_values(self, monkeypatch, val):
        monkeypatch.setenv("SMOKE_MODE", val)
        rp = self._reload()
        assert rp._is_smoke_mode() is False

    # ── _gsheets_disabled ───────────────────────────────────

    def test_gsheets_disabled_by_smoke_mode(self, monkeypatch):
        """SMOKE_MODE=true → _gsheets_disabled=True независимо от config."""
        monkeypatch.setenv("SMOKE_MODE", "true")
        rp = self._reload()
        cfg_on = types.SimpleNamespace(GSHEETS_UPLOAD_ENABLED=True)
        assert rp._gsheets_disabled(cfg_on) is True

    def test_gsheets_disabled_by_env_false(self, monkeypatch):
        """GSHEETS_UPLOAD_ENABLED=false env → _gsheets_disabled=True."""
        monkeypatch.setenv("GSHEETS_UPLOAD_ENABLED", "false")
        rp = self._reload()
        cfg_on = types.SimpleNamespace(GSHEETS_UPLOAD_ENABLED=True)
        assert rp._gsheets_disabled(cfg_on) is True

    @pytest.mark.parametrize("val", ["false", "0", "no", "off"])
    def test_gsheets_disabled_env_all_falsy(self, monkeypatch, val):
        """Все falsy-значения GSHEETS_UPLOAD_ENABLED отключают GSheets."""
        monkeypatch.setenv("GSHEETS_UPLOAD_ENABLED", val)
        rp = self._reload()
        cfg = types.SimpleNamespace(GSHEETS_UPLOAD_ENABLED=True)
        assert rp._gsheets_disabled(cfg) is True

    def test_gsheets_enabled_when_config_true_no_env(self):
        """config=True, ENV не задана → _gsheets_disabled=False."""
        rp = self._reload()
        cfg = types.SimpleNamespace(GSHEETS_UPLOAD_ENABLED=True)
        assert rp._gsheets_disabled(cfg) is False

    def test_gsheets_disabled_when_config_false_no_env(self):
        """config=False, ENV не задана → _gsheets_disabled=True."""
        rp = self._reload()
        cfg = types.SimpleNamespace(GSHEETS_UPLOAD_ENABLED=False)
        assert rp._gsheets_disabled(cfg) is True

    def test_smoke_mode_overrides_config_enabled(self, monkeypatch):
        """SMOKE_MODE=true переопределяет config.GSHEETS_UPLOAD_ENABLED=True."""
        monkeypatch.setenv("SMOKE_MODE", "true")
        rp = self._reload()
        cfg = types.SimpleNamespace(GSHEETS_UPLOAD_ENABLED=True)
        assert rp._gsheets_disabled(cfg) is True

    def test_env_gsheets_overrides_config_enabled(self, monkeypatch):
        """ENV GSHEETS_UPLOAD_ENABLED=false переопределяет config=True."""
        monkeypatch.setenv("GSHEETS_UPLOAD_ENABLED", "false")
        rp = self._reload()
        cfg = types.SimpleNamespace(GSHEETS_UPLOAD_ENABLED=True)
        assert rp._gsheets_disabled(cfg) is True


# ============================================================
# 2. BADZIP GUARD: add_verification_sheet / enrich_clients_with_db_match
# ============================================================


class TestBadZipGuard:
    """
    Проверяет, что функции записи в Excel корректно обрабатывают
    повреждённый файл: логируют warning, не пробрасывают исключение.
    """

    @pytest.fixture
    def corrupt_xlsx(self, tmp_path):
        f = tmp_path / "corrupt.xlsx"
        f.write_bytes(b"this is not a zip file at all")
        return str(f)

    @pytest.fixture
    def minimal_verification_df(self):
        return pd.DataFrame({
            "OCR_ФИО": ["Test Client"],
            "OCR_Телефон": ["+7 777 000 00 00"],
            "Статус_БД": ["Найден в БД"],
            "БД_ID": ["DB-0001"],
            "БД_ФИО": ["Test Client"],
            "БД_Телефон": ["+7 777 000 00 00"],
            "Совпадение_%": [95.0],
            "Визитов_в_БД": [3],
            "Врачи_в_БД": ["Оксана А."],
        })

    @pytest.fixture
    def capturing_log(self):
        """Logger, который собирает WARNING-сообщения."""
        log = logging.getLogger("test_badzip")
        log.setLevel(logging.DEBUG)
        messages = []

        class _Handler(logging.Handler):
            def emit(self, record):
                messages.append(record.getMessage())

        h = _Handler()
        log.addHandler(h)
        yield log, messages
        log.removeHandler(h)

    def test_add_verification_sheet_corrupt_no_exception(
        self, corrupt_xlsx, minimal_verification_df, capturing_log
    ):
        """add_verification_sheet с corrupt xlsx → no exception, warning logged."""
        import run_pipeline
        log, messages = capturing_log

        # Must not raise
        run_pipeline.add_verification_sheet(corrupt_xlsx, minimal_verification_df, log)

        # Warning was emitted
        assert any("BadZipFile" in m or "повреждён" in m or "открыть" in m
                   for m in messages), f"Нет warning о BadZipFile: {messages}"

    def test_enrich_clients_corrupt_no_exception(
        self, corrupt_xlsx, minimal_verification_df, capturing_log
    ):
        """enrich_clients_with_db_match с corrupt xlsx → no exception, warning logged."""
        import run_pipeline
        log, messages = capturing_log

        run_pipeline.enrich_clients_with_db_match(
            corrupt_xlsx, minimal_verification_df, log
        )

        assert any("BadZipFile" in m or "повреждён" in m or "открыть" in m
                   for m in messages), f"Нет warning о BadZipFile: {messages}"

    def test_add_verification_sheet_nonexistent_file(self, capturing_log):
        """add_verification_sheet с несуществующим файлом → no exception."""
        import run_pipeline
        log, messages = capturing_log
        df = pd.DataFrame({"OCR_ФИО": ["T"]})

        run_pipeline.add_verification_sheet("/nonexistent/path/x.xlsx", df, log)
        # Function returns early with a warning
        assert any("не найден" in m for m in messages)

    def test_enrich_clients_nonexistent_file(self, capturing_log):
        """enrich_clients_with_db_match с несуществующим файлом → no exception."""
        import run_pipeline
        log, messages = capturing_log
        df = pd.DataFrame({"OCR_ФИО": ["T"]})

        run_pipeline.enrich_clients_with_db_match(
            "/nonexistent/path/x.xlsx", df, log
        )
        assert any("не найден" in m for m in messages)

    def test_add_verification_sheet_empty_df(self, corrupt_xlsx, capturing_log):
        """add_verification_sheet с пустым DataFrame → no exception, early return."""
        import run_pipeline
        log, messages = capturing_log
        run_pipeline.add_verification_sheet(corrupt_xlsx, pd.DataFrame(), log)
        # Should warn about empty df, not about BadZipFile
        assert any("пустой" in m for m in messages)


# ============================================================
# 3. ИНТЕГРАЦИЯ: SMOKE_MODE=true → pipeline exit 0, нет GSheets
# ============================================================


class TestSmokeModeIntegration:
    """
    Subprocess-тесты: SMOKE_MODE=true гарантирует детерминированный прогон.
    """

    def _run(self, extra_env: dict) -> subprocess.CompletedProcess:
        env = {k: v for k, v in os.environ.items()
               if k not in {"SMOKE_MODE", "GSHEETS_UPLOAD_ENABLED",
                            "ENABLE_FINAL_VERIFICATION"}}
        env.update(extra_env)
        return subprocess.run(
            [sys.executable, "run_pipeline.py", "--skip-ocr"],
            capture_output=True, text=True,
            cwd=str(PROJECT_DIR), env=env, timeout=120,
        )

    def test_smoke_mode_exits_zero(self):
        """SMOKE_MODE=true + ENABLE_FINAL_VERIFICATION=false → exit 0."""
        result = self._run({
            "SMOKE_MODE": "true",
            "GSHEETS_UPLOAD_ENABLED": "false",
            "ENABLE_FINAL_VERIFICATION": "false",
        })
        combined = result.stdout + result.stderr
        assert result.returncode == 0, (
            f"Ожидался exit 0, получен {result.returncode}.\n"
            f"stdout: {result.stdout[-600:]}"
        )

    def test_smoke_mode_no_gsheets_upload(self):
        """SMOKE_MODE=true → Google Sheets не выгружается (нет '✓ Выгружено')."""
        result = self._run({
            "SMOKE_MODE": "true",
            "GSHEETS_UPLOAD_ENABLED": "false",
            "ENABLE_FINAL_VERIFICATION": "false",
        })
        combined = result.stdout + result.stderr
        assert "✓ Выгружено в Google Sheets" not in combined

    def test_smoke_mode_no_gsheets_warning_noise(self):
        """SMOKE_MODE=true → нет предупреждения 'выключена' (тихий режим)."""
        result = self._run({
            "SMOKE_MODE": "true",
            "GSHEETS_UPLOAD_ENABLED": "false",
            "ENABLE_FINAL_VERIFICATION": "false",
        })
        combined = result.stdout + result.stderr
        # В smoke-режиме Google Sheets пропускается тихо
        assert "Google Sheets выключена" not in combined

    def test_smoke_mode_no_claude_block(self):
        """SMOKE_MODE=true + ENABLE_FINAL_VERIFICATION=false → Claude-блок не входит."""
        result = self._run({
            "SMOKE_MODE": "true",
            "GSHEETS_UPLOAD_ENABLED": "false",
            "ENABLE_FINAL_VERIFICATION": "false",
        })
        combined = result.stdout + result.stderr
        assert "Финальная верификация Claude" not in combined

    def test_smoke_mode_pipeline_success_message(self):
        """SMOKE_MODE=true → pipeline завершается с 'ПАЙПЛАЙН ЗАВЕРШЁН УСПЕШНО'."""
        result = self._run({
            "SMOKE_MODE": "true",
            "GSHEETS_UPLOAD_ENABLED": "false",
            "ENABLE_FINAL_VERIFICATION": "false",
        })
        combined = result.stdout + result.stderr
        assert "ПАЙПЛАЙН ЗАВЕРШЁН УСПЕШНО" in combined


# ============================================================
# 4. quality_baseline.py ЗАДАЁТ ВСЕ 3 ENV VAR В SMOKE
# ============================================================


class TestBaselineEnvVars:
    """Проверяет, что quality_baseline.py корректно задаёт smoke env vars."""

    def test_run_smoke_sets_enable_final_verification(self):
        """run_smoke() устанавливает ENABLE_FINAL_VERIFICATION=false."""
        import inspect
        from quality_baseline import run_smoke
        src = inspect.getsource(run_smoke)
        assert "ENABLE_FINAL_VERIFICATION" in src
        assert "false" in src.lower()

    def test_run_smoke_sets_gsheets_disabled(self):
        """run_smoke() устанавливает GSHEETS_UPLOAD_ENABLED=false."""
        import inspect
        from quality_baseline import run_smoke
        src = inspect.getsource(run_smoke)
        assert "GSHEETS_UPLOAD_ENABLED" in src

    def test_run_smoke_sets_smoke_mode(self):
        """run_smoke() устанавливает SMOKE_MODE=true."""
        import inspect
        from quality_baseline import run_smoke
        src = inspect.getsource(run_smoke)
        assert "SMOKE_MODE" in src
        assert "true" in src.lower()

    def test_quality_baseline_md_documents_smoke_envs(self):
        """Сгенерированный baseline.md содержит команду с 3 env vars."""
        from quality_baseline import _generate_md

        # Минимальный baseline для генерации md
        bl = {
            "generated_at": "2026-02-24T12:00:00Z",
            "python_version": "3.11.0",
            "pytest": {
                "command": "python3 -m pytest tests/",
                "total": 10, "passed": 10, "failed": 0,
                "duration_sec": 2.0,
                "runs": [{"passed": 10, "failed": 0, "total": 10,
                           "duration_sec": 2.0, "returncode": 0}],
                "flaky_candidates": [],
            },
            "smoke": {
                "command": "python3 run_pipeline.py --skip-ocr",
                "exit_code": 0, "duration_sec": 3.0, "status": "passed",
            },
        }
        md = _generate_md(bl)
        assert "ENABLE_FINAL_VERIFICATION" in md
        assert "GSHEETS_UPLOAD_ENABLED" in md
        assert "SMOKE_MODE" in md
