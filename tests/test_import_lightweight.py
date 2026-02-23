"""
Тесты лёгкости импорта client_card_ocr.

Проверяют, что `import client_card_ocr` НЕ загружает тяжёлые SDK-модули
(google-cloud-vision, anthropic, openpyxl, PIL) и выполняется быстро.
"""

import subprocess
import sys
import time

import pytest


HEAVY_MODULES = [
    "google.cloud.vision",
    "google.oauth2.service_account",
    "anthropic",
    "openpyxl",
    "PIL",
    "tqdm",
    "rapidfuzz",
]


def test_import_client_card_ocr_fast_enough():
    """import client_card_ocr завершается < 2 секунд."""
    code = (
        "import time; t0 = time.time(); "
        "import client_card_ocr; "
        "print(f'{time.time() - t0:.3f}')"
    )
    result = subprocess.run(
        [sys.executable, "-c", code],
        capture_output=True, text=True, timeout=10,
        cwd=str(__import__("pathlib").Path(__file__).resolve().parent.parent),
    )
    assert result.returncode == 0, f"Import failed: {result.stderr}"
    # Берём последнюю строку — setup_logging() может печатать в stdout
    elapsed = float(result.stdout.strip().split("\n")[-1])
    assert elapsed < 2.0, f"Import took {elapsed:.3f}s (limit 2s)"


def test_no_heavy_modules_loaded_on_plain_import():
    """После import client_card_ocr тяжёлые модули НЕ в sys.modules."""
    code = (
        "import sys; import client_card_ocr; "
        "heavy = %r; "
        "loaded = [m for m in heavy if m in sys.modules]; "
        "print(','.join(loaded) if loaded else 'NONE')"
    ) % HEAVY_MODULES
    result = subprocess.run(
        [sys.executable, "-c", code],
        capture_output=True, text=True, timeout=10,
        cwd=str(__import__("pathlib").Path(__file__).resolve().parent.parent),
    )
    assert result.returncode == 0, f"Import failed: {result.stderr}"
    # Берём последнюю строку — setup_logging() может печатать в stdout
    output = result.stdout.strip().split("\n")[-1]
    assert output == "NONE", f"Heavy modules loaded on import: {output}"
