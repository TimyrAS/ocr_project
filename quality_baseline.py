#!/usr/bin/env python3
"""
Воспроизводимый baseline качества OCR-пайплайна.

Запускает pytest N раз, затем smoke-прогон CLI, записывает результаты
в baseline.json и baseline.md в указанный каталог.

CLI:
    python3 quality_baseline.py
    python3 quality_baseline.py --repeat 3 --output-dir artifacts/quality
    python3 quality_baseline.py \\
        --pytest-cmd "python3 -m pytest tests/ -v" \\
        --smoke-cmd "python3 run_pipeline.py --skip-ocr"
"""

import argparse
import json
import os
import re
import shlex
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

PROJECT_DIR = Path(__file__).resolve().parent


# ============================================================
# CLI
# ============================================================

def _positive_int(value: str) -> int:
    """argparse type: целое число >= 1, иначе — понятная ошибка."""
    try:
        ivalue = int(value)
    except ValueError:
        raise argparse.ArgumentTypeError(
            f"--repeat ожидает целое число, получено: {value!r}"
        )
    if ivalue <= 0:
        raise argparse.ArgumentTypeError(
            f"--repeat должно быть >= 1, получено: {value}"
        )
    return ivalue


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Генерирует воспроизводимый baseline качества OCR-пайплайна.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Примеры:
  python3 quality_baseline.py
  python3 quality_baseline.py --repeat 3 --output-dir artifacts/quality
  python3 quality_baseline.py \\
      --pytest-cmd "python3 -m pytest tests/ -v --tb=short" \\
      --smoke-cmd "python3 run_pipeline.py --skip-ocr"
        """,
    )
    parser.add_argument(
        "--pytest-cmd",
        default="python3 -m pytest tests/ -v --tb=short",
        help='Команда запуска pytest (default: "python3 -m pytest tests/ -v --tb=short")',
    )
    parser.add_argument(
        "--smoke-cmd",
        default="python3 run_pipeline.py --skip-ocr",
        help='Команда smoke-прогона (default: "python3 run_pipeline.py --skip-ocr")',
    )
    parser.add_argument(
        "--repeat",
        type=_positive_int,
        default=2,
        help="Количество прогонов pytest >= 1 (default: 2)",
    )
    parser.add_argument(
        "--output-dir",
        default="artifacts/quality",
        help="Каталог для сохранения артефактов (default: artifacts/quality)",
    )
    return parser.parse_args()


# ============================================================
# PYTEST RUNNER
# ============================================================

def _parse_pytest_output(stdout: str):
    """
    Разбирает вывод pytest -v.

    Возвращает:
        test_results : dict  {test_id: 'passed' | 'failed'}
        passed       : int
        failed       : int   (сумма всех 'N failed' + 'N error' из итоговой строки)
    """
    test_results = {}
    passed = 0
    failed = 0

    for line in stdout.splitlines():
        # Строки вида: "tests/foo.py::bar PASSED" или "FAILED" / "ERROR"
        m = re.match(r"^(tests/\S+::\S+)\s+(PASSED|FAILED|ERROR)", line)
        if m:
            test_id = m.group(1)
            outcome = m.group(2)
            test_results[test_id] = "passed" if outcome == "PASSED" else "failed"

        # Итоговая строка pytest, например:
        #   "5 passed in 1.2s"
        #   "4 passed, 1 failed in 2.3s"
        #   "1 failed, 1 error, 3 passed in 2.3s"
        # Признак итоговой строки — наличие хотя бы одного счётчика
        if re.search(r"\d+ (?:passed|failed|error)", line):
            m_pass = re.search(r"(\d+) passed", line)
            if m_pass:
                passed = int(m_pass.group(1))
            # findall ловит ВСЕ вхождения "N failed" и "N error" в одной строке,
            # затем суммирует их → корректный общий счётчик провалов.
            # Присваивание (не +=): каждая итоговая строка перезаписывает счётчик
            # (если строка без провалов → 0).
            all_fail_counts = re.findall(r"(\d+) (?:failed|error)", line)
            failed = sum(int(x) for x in all_fail_counts)

    return test_results, passed, failed


def _run_pytest_once(cmd: str, cwd: str) -> dict:
    """Запускает pytest один раз, возвращает статистику прогона."""
    t0 = time.time()
    result = subprocess.run(
        shlex.split(cmd),
        capture_output=True,
        text=True,
        cwd=cwd,
    )
    duration = round(time.time() - t0, 2)

    test_results, passed, failed = _parse_pytest_output(result.stdout)

    # Fallback: если не смогли распарсить, используем returncode
    if passed == 0 and failed == 0 and result.returncode != 0:
        failed = 1  # хотя бы один провал

    total = passed + failed
    return {
        "returncode": result.returncode,
        "passed": passed,
        "failed": failed,
        "total": total,
        "duration_sec": duration,
        "test_results": test_results,
    }


def collect_pytest_stats(cmd: str, cwd: str, repeat: int) -> dict:
    """
    Запускает pytest ``repeat`` раз, собирает агрегированную статистику.
    Определяет flaky-кандидаты: тесты, исход которых менялся между прогонами.
    """
    print(f"  [pytest] Команда: {cmd}")
    runs_raw = []

    for i in range(repeat):
        print(f"  [pytest] Прогон {i + 1}/{repeat}...", end="", flush=True)
        run = _run_pytest_once(cmd, cwd)
        runs_raw.append(run)
        status = "OK" if run["returncode"] == 0 else "FAIL"
        print(
            f" {status} ({run['passed']}/{run['total']} passed,"
            f" {run['duration_sec']:.1f}s)"
        )

    # Канонический результат — последний прогон
    canonical = runs_raw[-1]

    # Flaky-кандидаты: тест, у которого исходы различаются между прогонами
    all_test_ids: set = set()
    for r in runs_raw:
        all_test_ids.update(r["test_results"].keys())

    flaky_candidates = []
    for tid in sorted(all_test_ids):
        outcomes = {r["test_results"].get(tid, "missing") for r in runs_raw}
        if "passed" in outcomes and ("failed" in outcomes or "missing" in outcomes):
            flaky_candidates.append(tid)

    # runs для артефакта (без внутреннего словаря test_results)
    runs_summary = [
        {
            "passed": r["passed"],
            "failed": r["failed"],
            "total": r["total"],
            "duration_sec": r["duration_sec"],
            "returncode": r["returncode"],
        }
        for r in runs_raw
    ]

    return {
        "command": cmd,
        "total": canonical["total"],
        "passed": canonical["passed"],
        "failed": canonical["failed"],
        "duration_sec": canonical["duration_sec"],
        "runs": runs_summary,
        "flaky_candidates": flaky_candidates,
    }


# ============================================================
# SMOKE RUNNER
# ============================================================

def run_smoke(cmd: str, cwd: str) -> dict:
    """
    Запускает smoke-команду с ENABLE_FINAL_VERIFICATION=false,
    чтобы пайплайн не вызывал внешнее Claude API.
    """
    env = os.environ.copy()
    # Детерминированный smoke: отключаем все внешние интеграции
    env["ENABLE_FINAL_VERIFICATION"] = "false"
    env["GSHEETS_UPLOAD_ENABLED"] = "false"
    env["SMOKE_MODE"] = "true"

    print(f"  [smoke] Команда: {cmd}")
    t0 = time.time()
    result = subprocess.run(
        shlex.split(cmd),
        capture_output=True,
        text=True,
        cwd=cwd,
        env=env,
    )
    duration = round(time.time() - t0, 2)
    status = "passed" if result.returncode == 0 else "failed"

    print(f"  [smoke] Статус: {status} (exit {result.returncode}, {duration:.1f}s)")
    if result.returncode != 0:
        # Показываем хвост stderr для диагностики
        stderr_tail = result.stderr[-500:] if result.stderr else ""
        stdout_tail = result.stdout[-500:] if result.stdout else ""
        if stderr_tail:
            print(f"  [smoke] stderr: {stderr_tail}")
        if stdout_tail:
            print(f"  [smoke] stdout: {stdout_tail}")

    return {
        "command": cmd,
        "exit_code": result.returncode,
        "duration_sec": duration,
        "status": status,
    }


# ============================================================
# MARKDOWN REPORT
# ============================================================

def _generate_md(baseline: dict) -> str:
    """Генерирует человекочитаемый Markdown-отчёт из baseline dict."""
    ps = baseline["pytest"]
    ss = baseline["smoke"]
    generated = baseline["generated_at"]
    python_ver = baseline["python_version"]

    pytest_ok = ps["failed"] == 0
    smoke_ok = ss["status"] == "passed"

    pytest_icon = "✓" if pytest_ok else "✗"
    smoke_icon = "✓" if smoke_ok else "✗"
    pytest_status = "PASS" if pytest_ok else "FAIL"

    lines = [
        "# Quality Baseline",
        "",
        f"**Дата:** {generated}  ",
        f"**Python:** {python_ver}  ",
        "",
        "---",
        "",
        "## pytest",
        "",
        "| Параметр | Значение |",
        "|---|---|",
        f"| Команда | `{ps['command']}` |",
        f"| Итого тестов | {ps['total']} |",
        f"| Прошло | {ps['passed']} |",
        f"| Упало | {ps['failed']} |",
        f"| Время (посл. прогон) | {ps['duration_sec']:.2f}s |",
        f"| Прогонов | {len(ps['runs'])} |",
        f"| Статус | {pytest_icon} **{pytest_status}** |",
    ]

    if ps["flaky_candidates"]:
        lines += ["", "### Flaky-кандидаты", ""]
        for tid in ps["flaky_candidates"]:
            lines.append(f"- `{tid}`")
    else:
        lines += ["", "_Flaky-тестов не обнаружено._"]

    lines += [
        "",
        "## Smoke",
        "",
        "| Параметр | Значение |",
        "|---|---|",
        f"| Команда | `{ss['command']}` |",
        f"| Код выхода | {ss['exit_code']} |",
        f"| Время | {ss['duration_sec']:.2f}s |",
        f"| Статус | {smoke_icon} **{ss['status']}** |",
        "",
        "---",
        "",
        "## Воспроизведение",
        "",
        "```bash",
        "# Сгенерировать baseline",
        "python3 quality_baseline.py",
        "",
        "# С 3 прогонами для flaky-детекции",
        "python3 quality_baseline.py --repeat 3",
        "",
        "# Ручной smoke-прогон (все 3 env var для детерминизма)",
        f"ENABLE_FINAL_VERIFICATION=false GSHEETS_UPLOAD_ENABLED=false SMOKE_MODE=true {ss['command']}",
        "",
        "# Запуск тестов схемы baseline",
        "python3 -m pytest tests/test_quality_baseline_schema.py -v",
        "```",
    ]

    return "\n".join(lines) + "\n"


# ============================================================
# MAIN
# ============================================================

def generate_baseline(args: argparse.Namespace) -> dict:
    """Генерирует baseline.json и baseline.md, возвращает baseline dict."""
    output_dir = PROJECT_DIR / args.output_dir
    output_dir.mkdir(parents=True, exist_ok=True)

    generated_at = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    python_version = (
        f"{sys.version_info.major}.{sys.version_info.minor}.{sys.version_info.micro}"
    )

    print("\n=== pytest ===")
    pytest_stats = collect_pytest_stats(args.pytest_cmd, str(PROJECT_DIR), args.repeat)

    print("\n=== smoke ===")
    smoke_result = run_smoke(args.smoke_cmd, str(PROJECT_DIR))

    baseline = {
        "generated_at": generated_at,
        "python_version": python_version,
        "pytest": pytest_stats,
        "smoke": smoke_result,
    }

    json_path = output_dir / "baseline.json"
    with open(json_path, "w", encoding="utf-8") as f:
        json.dump(baseline, f, indent=2, ensure_ascii=False)

    md_path = output_dir / "baseline.md"
    with open(md_path, "w", encoding="utf-8") as f:
        f.write(_generate_md(baseline))

    print("\n=== Артефакты ===")
    print(f"  baseline.json : {json_path}")
    print(f"  baseline.md   : {md_path}")

    return baseline


def main() -> None:
    args = parse_args()
    t_total = time.time()
    baseline = generate_baseline(args)
    elapsed = time.time() - t_total

    ps = baseline["pytest"]
    ss = baseline["smoke"]

    print(f"\n=== Итог ({elapsed:.1f}s) ===")
    print(f"  pytest : {ps['passed']}/{ps['total']} passed, {ps['failed']} failed")
    print(f"  smoke  : {ss['status']} (exit {ss['exit_code']})")

    if ps["failed"] > 0 or ss["status"] != "passed":
        print("\n[BASELINE FAIL] Есть упавшие тесты или smoke не прошёл.")
        sys.exit(1)

    print("\n[BASELINE PASS]")


if __name__ == "__main__":
    main()
