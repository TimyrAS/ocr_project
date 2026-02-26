"""
Unit-тесты baseline качества OCR-пайплайна.

1. Структура и обязательные ключи baseline.json.
2. Типы полей и логическая согласованность данных.
3. Guard финальной верификации (ENV + config):
   - блок 6.5 не выполняется,
   - Claude-клиент не инициализируется,
   - пайплайн завершается с кодом 0.
4. Парсер pytest: корректный подсчёт failed + error.
5. Валидация --repeat: значения <= 0 отклоняются.
"""

import json
import os
import subprocess
import sys
from pathlib import Path

import pytest

PROJECT_DIR = Path(__file__).resolve().parent.parent

# ── Обязательные ключи согласно контракту ────────────────────────────────────
REQUIRED_TOP_KEYS = {"generated_at", "python_version", "pytest", "smoke"}

REQUIRED_PYTEST_KEYS = {
    "command",
    "total",
    "passed",
    "failed",
    "duration_sec",
    "runs",
    "flaky_candidates",
}

REQUIRED_SMOKE_KEYS = {"command", "exit_code", "duration_sec", "status"}


# ── Fixture helper ────────────────────────────────────────────────────────────

def _make_valid_baseline() -> dict:
    """Минимально-корректный baseline dict для тестирования схемы."""
    return {
        "generated_at": "2026-02-24T12:00:00Z",
        "python_version": "3.11.0",
        "pytest": {
            "command": "python3 -m pytest tests/ -v --tb=short",
            "total": 10,
            "passed": 10,
            "failed": 0,
            "duration_sec": 2.5,
            "runs": [
                {
                    "passed": 10,
                    "failed": 0,
                    "total": 10,
                    "duration_sec": 2.5,
                    "returncode": 0,
                }
            ],
            "flaky_candidates": [],
        },
        "smoke": {
            "command": "python3 run_pipeline.py --skip-ocr",
            "exit_code": 0,
            "duration_sec": 3.1,
            "status": "passed",
        },
    }


# ============================================================
# 1. СХЕМА: ОБЯЗАТЕЛЬНЫЕ КЛЮЧИ
# ============================================================


class TestBaselineSchemaRequiredKeys:
    """Проверяет наличие всех обязательных ключей в baseline dict."""

    def test_top_level_keys_present(self):
        """Все ключи верхнего уровня присутствуют."""
        baseline = _make_valid_baseline()
        missing = REQUIRED_TOP_KEYS - set(baseline)
        assert not missing, f"Отсутствуют ключи верхнего уровня: {missing}"

    def test_pytest_section_keys(self):
        """Секция pytest содержит все обязательные ключи."""
        baseline = _make_valid_baseline()
        missing = REQUIRED_PYTEST_KEYS - set(baseline["pytest"])
        assert not missing, f"Отсутствуют ключи в pytest: {missing}"

    def test_smoke_section_keys(self):
        """Секция smoke содержит все обязательные ключи."""
        baseline = _make_valid_baseline()
        missing = REQUIRED_SMOKE_KEYS - set(baseline["smoke"])
        assert not missing, f"Отсутствуют ключи в smoke: {missing}"

    @pytest.mark.parametrize("key", sorted(REQUIRED_TOP_KEYS))
    def test_each_top_key(self, key):
        """Каждый ключ верхнего уровня присутствует (параметрический)."""
        assert key in _make_valid_baseline()

    @pytest.mark.parametrize("key", sorted(REQUIRED_PYTEST_KEYS))
    def test_each_pytest_key(self, key):
        """Каждый ключ секции pytest присутствует (параметрический)."""
        assert key in _make_valid_baseline()["pytest"]

    @pytest.mark.parametrize("key", sorted(REQUIRED_SMOKE_KEYS))
    def test_each_smoke_key(self, key):
        """Каждый ключ секции smoke присутствует (параметрический)."""
        assert key in _make_valid_baseline()["smoke"]


# ============================================================
# 2. СХЕМА: ТИПЫ ПОЛЕЙ
# ============================================================


class TestBaselineSchemaTypes:
    """Проверяет типы обязательных полей baseline dict."""

    def test_generated_at_is_nonempty_string(self):
        bl = _make_valid_baseline()
        assert isinstance(bl["generated_at"], str)
        assert len(bl["generated_at"]) > 0

    def test_python_version_is_dotted_string(self):
        bl = _make_valid_baseline()
        assert isinstance(bl["python_version"], str)
        assert "." in bl["python_version"]

    def test_pytest_command_is_string(self):
        bl = _make_valid_baseline()
        assert isinstance(bl["pytest"]["command"], str)

    def test_pytest_total_is_int(self):
        bl = _make_valid_baseline()
        assert isinstance(bl["pytest"]["total"], int)

    def test_pytest_passed_is_int(self):
        bl = _make_valid_baseline()
        assert isinstance(bl["pytest"]["passed"], int)

    def test_pytest_failed_is_int(self):
        bl = _make_valid_baseline()
        assert isinstance(bl["pytest"]["failed"], int)

    def test_pytest_duration_sec_is_numeric(self):
        bl = _make_valid_baseline()
        assert isinstance(bl["pytest"]["duration_sec"], (int, float))

    def test_pytest_runs_is_nonempty_list(self):
        bl = _make_valid_baseline()
        assert isinstance(bl["pytest"]["runs"], list)
        assert len(bl["pytest"]["runs"]) >= 1

    def test_pytest_flaky_candidates_is_list(self):
        bl = _make_valid_baseline()
        assert isinstance(bl["pytest"]["flaky_candidates"], list)

    def test_smoke_command_is_string(self):
        bl = _make_valid_baseline()
        assert isinstance(bl["smoke"]["command"], str)

    def test_smoke_exit_code_is_int(self):
        bl = _make_valid_baseline()
        assert isinstance(bl["smoke"]["exit_code"], int)

    def test_smoke_duration_sec_is_numeric(self):
        bl = _make_valid_baseline()
        assert isinstance(bl["smoke"]["duration_sec"], (int, float))

    def test_smoke_status_is_valid_enum(self):
        """smoke.status принимает только 'passed' или 'failed'."""
        bl = _make_valid_baseline()
        assert bl["smoke"]["status"] in ("passed", "failed")


# ============================================================
# 3. СХЕМА: ЛОГИЧЕСКАЯ СОГЛАСОВАННОСТЬ
# ============================================================


class TestBaselineSchemaLogic:
    """Проверяет логическую согласованность полей baseline dict."""

    def test_passed_plus_failed_equals_total(self):
        """pytest.passed + pytest.failed == pytest.total."""
        bl = _make_valid_baseline()
        ps = bl["pytest"]
        assert ps["passed"] + ps["failed"] == ps["total"]

    def test_smoke_status_passed_implies_exit_zero(self):
        """smoke.status='passed' ↔ exit_code=0."""
        bl = _make_valid_baseline()
        ss = bl["smoke"]
        if ss["status"] == "passed":
            assert ss["exit_code"] == 0
        else:
            assert ss["exit_code"] != 0

    def test_failed_status_has_nonzero_exit(self):
        """Если smoke.status='failed', exit_code != 0."""
        bl = _make_valid_baseline()
        bl["smoke"]["status"] = "failed"
        bl["smoke"]["exit_code"] = 1
        ss = bl["smoke"]
        assert ss["exit_code"] != 0

    def test_runs_count_matches_repeat(self):
        """Количество прогонов в runs соответствует заданному repeat."""
        bl = _make_valid_baseline()
        # По умолчанию 1 прогон в fixture; добавляем второй
        bl["pytest"]["runs"].append(bl["pytest"]["runs"][0].copy())
        assert len(bl["pytest"]["runs"]) == 2

    def test_json_round_trip(self):
        """baseline.json сериализуется/десериализуется без потерь."""
        bl = _make_valid_baseline()
        serialized = json.dumps(bl, ensure_ascii=False)
        recovered = json.loads(serialized)
        assert recovered == bl

    def test_pytest_duration_nonnegative(self):
        bl = _make_valid_baseline()
        assert bl["pytest"]["duration_sec"] >= 0

    def test_smoke_duration_nonnegative(self):
        bl = _make_valid_baseline()
        assert bl["smoke"]["duration_sec"] >= 0

    def test_pytest_counts_nonnegative(self):
        bl = _make_valid_baseline()
        ps = bl["pytest"]
        assert ps["total"] >= 0
        assert ps["passed"] >= 0
        assert ps["failed"] >= 0


# ============================================================
# 4. АРТЕФАКТ: baseline.json (пропускается, если ещё не создан)
# ============================================================


class TestBaselineJsonArtifact:
    """
    Проверяет реальный baseline.json в artifacts/quality/.
    Тесты пропускаются, если файл ещё не создан.
    Запустите quality_baseline.py, чтобы создать артефакт.
    """

    @pytest.fixture
    def baseline_path(self):
        return PROJECT_DIR / "artifacts" / "quality" / "baseline.json"

    @pytest.fixture
    def baseline_data(self, baseline_path):
        if not baseline_path.exists():
            pytest.skip(
                "baseline.json не найден — запустите: python3 quality_baseline.py"
            )
        with open(baseline_path, encoding="utf-8") as f:
            return json.load(f)

    def test_artifact_has_required_top_keys(self, baseline_data):
        missing = REQUIRED_TOP_KEYS - set(baseline_data)
        assert not missing, f"Отсутствуют ключи: {missing}"

    def test_artifact_pytest_section(self, baseline_data):
        missing = REQUIRED_PYTEST_KEYS - set(baseline_data["pytest"])
        assert not missing, f"Отсутствуют ключи pytest: {missing}"

    def test_artifact_smoke_section(self, baseline_data):
        missing = REQUIRED_SMOKE_KEYS - set(baseline_data["smoke"])
        assert not missing, f"Отсутствуют ключи smoke: {missing}"

    def test_artifact_smoke_status_passed(self, baseline_data):
        """Smoke должен завершиться успешно."""
        assert baseline_data["smoke"]["status"] == "passed", (
            f"smoke.status = {baseline_data['smoke']['status']!r}, ожидается 'passed'"
        )

    def test_artifact_smoke_exit_zero(self, baseline_data):
        assert baseline_data["smoke"]["exit_code"] == 0

    def test_artifact_pytest_counts_consistent(self, baseline_data):
        ps = baseline_data["pytest"]
        assert ps["passed"] + ps["failed"] == ps["total"]


# ============================================================
# 5. GUARD: ENABLE_FINAL_VERIFICATION=false
# ============================================================


class TestFinalVerificationGuard:
    """
    Проверяет guard финальной верификации в run_pipeline.py:
    - ENV ENABLE_FINAL_VERIFICATION (false/0/no/off) → блок 6.5 пропускается
    - config.ENABLE_FINAL_VERIFICATION=False (без ENV) → блок 6.5 пропускается
    - Claude-клиент не инициализируется в обоих случаях
    - Пайплайн завершается с кодом 0
    """

    def _run_skip_ocr(self, extra_env: dict) -> subprocess.CompletedProcess:
        env = {k: v for k, v in os.environ.items()
               if k != "ENABLE_FINAL_VERIFICATION"}
        env.update(extra_env)
        return subprocess.run(
            [sys.executable, "run_pipeline.py", "--skip-ocr"],
            capture_output=True,
            text=True,
            cwd=str(PROJECT_DIR),
            env=env,
            timeout=120,
        )

    def test_env_false_exits_zero(self):
        """run_pipeline.py --skip-ocr с ENABLE_FINAL_VERIFICATION=false → exit 0."""
        result = self._run_skip_ocr({"ENABLE_FINAL_VERIFICATION": "false"})
        combined = result.stdout + result.stderr
        assert result.returncode == 0, (
            f"Ожидался exit 0, получен {result.returncode}.\n"
            f"stdout: {result.stdout[-800:]}\nstderr: {result.stderr[-400:]}"
        )

    def test_env_false_claude_block_not_entered(self):
        """При ENABLE_FINAL_VERIFICATION=false блок Claude верификации не выполняется."""
        result = self._run_skip_ocr({"ENABLE_FINAL_VERIFICATION": "false"})
        combined = result.stdout + result.stderr
        # Блок не должен стартовать
        assert "Финальная верификация Claude" not in combined, (
            "Блок Claude верификации был запущен несмотря на "
            "ENABLE_FINAL_VERIFICATION=false"
        )

    def test_env_false_guard_message_or_no_claude(self):
        """
        При ENABLE_FINAL_VERIFICATION=false вывод содержит сообщение о пропуске
        (если данных для верификации достаточно) ИЛИ не содержит Claude-ошибок.
        Оба варианта корректны — зависит от наличия OCR-данных.
        """
        result = self._run_skip_ocr({"ENABLE_FINAL_VERIFICATION": "false"})
        combined = result.stdout + result.stderr
        # Нет признаков запуска Claude API
        assert "Claude API — OK" not in combined
        assert "Ошибка финальной верификации" not in combined

    def test_env_false_does_not_call_init_claude_client(self):
        """
        При ENABLE_FINAL_VERIFICATION=false init_claude_client не вызывается.
        Проверяется через патч на уровне subprocess.
        """
        code = (
            "import os, sys\n"
            f"sys.path.insert(0, {str(PROJECT_DIR)!r})\n"
            "os.environ['ENABLE_FINAL_VERIFICATION'] = 'false'\n"
            "import client_card_ocr as _cco\n"
            "_calls = []\n"
            "_orig = _cco.init_claude_client\n"
            "def _tracked(*a, **kw):\n"
            "    _calls.append(True)\n"
            "    return _orig(*a, **kw)\n"
            "_cco.init_claude_client = _tracked\n"
            "sys.argv = ['run_pipeline.py', '--skip-ocr']\n"
            "import run_pipeline\n"
            "try:\n"
            "    run_pipeline.main()\n"
            "except SystemExit:\n"
            "    pass\n"
            "if _calls:\n"
            "    print('__CLAUDE_INIT_CALLED__')\n"
            "else:\n"
            "    print('__CLAUDE_INIT_NOT_CALLED__')\n"
        )
        result = subprocess.run(
            [sys.executable, "-c", code],
            capture_output=True,
            text=True,
            timeout=120,
        )
        combined = result.stdout + result.stderr
        assert "__CLAUDE_INIT_CALLED__" not in combined, (
            "init_claude_client был вызван несмотря на ENABLE_FINAL_VERIFICATION=false.\n"
            f"stdout: {result.stdout[-600:]}"
        )

    @pytest.mark.parametrize("falsy_value", ["false", "0", "no", "off", "False", "OFF"])
    def test_env_falsy_values_skip_claude(self, falsy_value):
        """ENV значения false/0/no/off (любой регистр) отключают Claude-верификацию."""
        result = self._run_skip_ocr({"ENABLE_FINAL_VERIFICATION": falsy_value})
        combined = result.stdout + result.stderr
        assert result.returncode == 0, (
            f"ENABLE_FINAL_VERIFICATION={falsy_value!r} → exit {result.returncode}.\n"
            f"stdout: {result.stdout[-600:]}"
        )
        assert "Финальная верификация Claude" not in combined

    def test_config_false_skips_claude_without_env(self):
        """
        config.ENABLE_FINAL_VERIFICATION=False (без ENV) → init_claude_client не вызывается.
        """
        code = (
            "import os, sys\n"
            f"sys.path.insert(0, {str(PROJECT_DIR)!r})\n"
            "# Убираем ENV-переменную, чтобы приоритет перешёл к config\n"
            "os.environ.pop('ENABLE_FINAL_VERIFICATION', None)\n"
            "import config\n"
            "config.ENABLE_FINAL_VERIFICATION = False\n"
            "import client_card_ocr as _cco\n"
            "_calls = []\n"
            "def _tracked(*a, **kw): _calls.append(True)\n"
            "_cco.init_claude_client = _tracked\n"
            "sys.argv = ['run_pipeline.py', '--skip-ocr']\n"
            "import run_pipeline\n"
            "try:\n"
            "    run_pipeline.main()\n"
            "except SystemExit:\n"
            "    pass\n"
            "print('CALLED' if _calls else 'NOT_CALLED')\n"
        )
        env = {k: v for k, v in os.environ.items()
               if k != "ENABLE_FINAL_VERIFICATION"}
        result = subprocess.run(
            [sys.executable, "-c", code],
            capture_output=True, text=True, timeout=120, env=env,
        )
        combined = result.stdout + result.stderr
        assert "CALLED" not in result.stdout.splitlines()[-1:], (
            "init_claude_client вызван несмотря на config.ENABLE_FINAL_VERIFICATION=False.\n"
            f"stdout: {result.stdout[-600:]}"
        )

    def test_config_false_helper_returns_true(self):
        """_final_verification_disabled() = True при config.ENABLE_FINAL_VERIFICATION=False."""
        import importlib
        import types

        # Временный объект-config
        fake_cfg = types.SimpleNamespace(ENABLE_FINAL_VERIFICATION=False)

        env_backup = os.environ.pop("ENABLE_FINAL_VERIFICATION", None)
        try:
            import run_pipeline
            importlib.reload(run_pipeline)
            assert run_pipeline._final_verification_disabled(fake_cfg) is True
        finally:
            if env_backup is not None:
                os.environ["ENABLE_FINAL_VERIFICATION"] = env_backup

    def test_config_true_helper_returns_false(self):
        """_final_verification_disabled() = False при config.ENABLE_FINAL_VERIFICATION=True."""
        import importlib
        import types

        fake_cfg = types.SimpleNamespace(ENABLE_FINAL_VERIFICATION=True)

        env_backup = os.environ.pop("ENABLE_FINAL_VERIFICATION", None)
        try:
            import run_pipeline
            importlib.reload(run_pipeline)
            assert run_pipeline._final_verification_disabled(fake_cfg) is False
        finally:
            if env_backup is not None:
                os.environ["ENABLE_FINAL_VERIFICATION"] = env_backup

    def test_env_overrides_config_true(self):
        """ENV=false переопределяет config.ENABLE_FINAL_VERIFICATION=True."""
        import importlib
        import types

        fake_cfg = types.SimpleNamespace(ENABLE_FINAL_VERIFICATION=True)
        os.environ["ENABLE_FINAL_VERIFICATION"] = "false"
        try:
            import run_pipeline
            importlib.reload(run_pipeline)
            assert run_pipeline._final_verification_disabled(fake_cfg) is True
        finally:
            os.environ.pop("ENABLE_FINAL_VERIFICATION", None)

    def test_guard_present_in_source(self):
        """run_pipeline.py содержит _final_verification_disabled и _FALSY_VERIF."""
        source = (PROJECT_DIR / "run_pipeline.py").read_text(encoding="utf-8")
        assert "_final_verification_disabled" in source
        assert "_FALSY_VERIF" in source
        assert "ENABLE_FINAL_VERIFICATION" in source


# ============================================================
# 6. ПАРСЕР PYTEST: КОРРЕКТНЫЙ ПОДСЧЁТ failed + error
# ============================================================


class TestParserFailedPlusError:
    """
    Проверяет, что _parse_pytest_output корректно суммирует
    'N failed' и 'N error' из одной итоговой строки pytest.
    """

    def _parse(self, text: str):
        from quality_baseline import _parse_pytest_output
        return _parse_pytest_output(text)

    def test_only_passed(self):
        """5 passed → passed=5, failed=0."""
        _, passed, failed = self._parse("= 5 passed in 1.23s =")
        assert passed == 5
        assert failed == 0

    def test_failed_only(self):
        """1 failed, 4 passed → passed=4, failed=1."""
        _, passed, failed = self._parse("= 1 failed, 4 passed in 2.34s =")
        assert passed == 4
        assert failed == 1

    def test_error_only(self):
        """1 error, 3 passed → passed=3, failed=1 (error считается как провал)."""
        _, passed, failed = self._parse("= 1 error, 3 passed in 1.00s =")
        assert passed == 3
        assert failed == 1

    def test_failed_plus_error(self):
        """1 failed, 1 error, 1 passed → passed=1, failed=2."""
        _, passed, failed = self._parse("= 1 failed, 1 error, 1 passed in 2.34s =")
        assert passed == 1
        assert failed == 2

    def test_multiple_failures_and_errors(self):
        """3 failed, 2 error, 5 passed → passed=5, failed=5."""
        _, passed, failed = self._parse("= 3 failed, 2 error, 5 passed in 5.00s =")
        assert passed == 5
        assert failed == 5

    def test_no_summary_line(self):
        """Строка без итога → passed=0, failed=0."""
        _, passed, failed = self._parse("collecting ... collected 3 items")
        assert passed == 0
        assert failed == 0

    def test_individual_test_lines_parsed(self):
        """Отдельные строки PASSED/FAILED/ERROR попадают в test_results."""
        output = (
            "tests/test_foo.py::test_ok PASSED\n"
            "tests/test_foo.py::test_bad FAILED\n"
            "tests/test_foo.py::test_err ERROR\n"
            "= 1 passed, 2 failed in 1.00s =\n"
        )
        results, passed, failed = self._parse(output)
        assert results.get("tests/test_foo.py::test_ok") == "passed"
        assert results.get("tests/test_foo.py::test_bad") == "failed"
        assert results.get("tests/test_foo.py::test_err") == "failed"
        assert passed == 1
        assert failed == 2

    def test_summary_assignment_not_accumulation(self):
        """
        failed задаётся присваиванием на каждой итоговой строке,
        а не накапливается через +=.
        Если две строки с итогом — побеждает последняя.
        """
        # Второй прогон должен перезаписать счётчики первого
        output = (
            "= 1 failed in 1.0s =\n"     # первый прогон (не итоговая)
            "= 5 passed in 2.0s =\n"     # второй прогон (итоговая)
        )
        _, passed, failed = self._parse(output)
        assert passed == 5
        assert failed == 0


# ============================================================
# 7. ВАЛИДАЦИЯ --repeat
# ============================================================


class TestRepeatValidation:
    """
    Проверяет, что --repeat <= 0 вызывает контролируемую ошибку,
    а не IndexError или неопределённое поведение.
    """

    def _run_baseline(self, *extra_args) -> subprocess.CompletedProcess:
        return subprocess.run(
            [sys.executable, "quality_baseline.py"] + list(extra_args),
            capture_output=True,
            text=True,
            cwd=str(PROJECT_DIR),
            timeout=30,
        )

    def test_repeat_zero_rejected(self):
        """--repeat 0 → exit != 0 с понятным сообщением об ошибке."""
        result = self._run_baseline("--repeat", "0",
                                    "--pytest-cmd", "echo skip",
                                    "--smoke-cmd", "echo skip")
        assert result.returncode != 0, (
            "--repeat 0 должен завершиться с ошибкой, а не успешно"
        )
        combined = result.stdout + result.stderr
        # argparse выводит ошибку в stderr, проверяем что есть разумный текст
        assert any(
            kw in combined.lower()
            for kw in ("repeat", "error", "invalid", "должно", "1", ">=")
        ), f"Нет понятного сообщения об ошибке: {combined[:400]}"

    def test_repeat_negative_rejected(self):
        """--repeat -1 → exit != 0."""
        result = self._run_baseline("--repeat", "-1",
                                    "--pytest-cmd", "echo skip",
                                    "--smoke-cmd", "echo skip")
        assert result.returncode != 0

    def test_repeat_one_accepted(self):
        """--repeat 1 принимается argparse (не вызывает ошибку типа)."""
        from quality_baseline import _positive_int
        assert _positive_int("1") == 1

    def test_repeat_large_accepted(self):
        """--repeat 10 принимается."""
        from quality_baseline import _positive_int
        assert _positive_int("10") == 10

    def test_repeat_zero_raises_argument_type_error(self):
        """_positive_int('0') бросает ArgumentTypeError."""
        import argparse
        from quality_baseline import _positive_int
        with pytest.raises(argparse.ArgumentTypeError, match=r">=\s*1|должно"):
            _positive_int("0")

    def test_repeat_negative_raises_argument_type_error(self):
        """_positive_int('-5') бросает ArgumentTypeError."""
        import argparse
        from quality_baseline import _positive_int
        with pytest.raises(argparse.ArgumentTypeError):
            _positive_int("-5")

    def test_repeat_non_integer_raises_argument_type_error(self):
        """_positive_int('abc') бросает ArgumentTypeError."""
        import argparse
        from quality_baseline import _positive_int
        with pytest.raises(argparse.ArgumentTypeError, match=r"целое"):
            _positive_int("abc")
