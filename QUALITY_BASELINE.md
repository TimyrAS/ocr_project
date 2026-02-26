# Quality Baseline — OCR Pipeline

Воспроизводимый baseline качества OCR-пайплайна: зафиксированный прогон тестов,
smoke-проверка CLI и единый JSON-артефакт метрик на дату запуска.

---

## Команды воспроизведения

```bash
# Базовый baseline (2 прогона pytest + smoke)
python3 quality_baseline.py

# С 3 прогонами для обнаружения flaky-тестов
python3 quality_baseline.py --repeat 3

# Свои команды
python3 quality_baseline.py \
    --pytest-cmd "python3 -m pytest tests/ -v --tb=short" \
    --smoke-cmd "python3 run_pipeline.py --skip-ocr" \
    --repeat 2 \
    --output-dir artifacts/quality

# Ручной smoke (все 3 флага для полного детерминизма)
ENABLE_FINAL_VERIFICATION=false GSHEETS_UPLOAD_ENABLED=false SMOKE_MODE=true \
    python3 run_pipeline.py --skip-ocr

# Тесты напрямую
python3 -m pytest tests/ -v

# Тесты схемы baseline
python3 -m pytest tests/test_quality_baseline_schema.py -v
```

---

## Параметры CLI

| Параметр | По умолчанию | Описание |
|---|---|---|
| `--pytest-cmd` | `python3 -m pytest tests/ -v --tb=short` | Команда запуска pytest |
| `--smoke-cmd` | `python3 run_pipeline.py --skip-ocr` | Команда smoke-прогона |
| `--repeat` | `2` | Количество прогонов pytest (для flaky-детекции) |
| `--output-dir` | `artifacts/quality` | Каталог для сохранения артефактов |

---

## Артефакты

| Файл | Описание |
|---|---|
| `artifacts/quality/baseline.json` | Машиночитаемые метрики (см. схему ниже) |
| `artifacts/quality/baseline.md` | Человекочитаемый отчёт |

---

## Схема baseline.json

```json
{
  "generated_at": "2026-02-24T12:00:00Z",
  "python_version": "3.11.0",
  "pytest": {
    "command": "python3 -m pytest tests/ -v --tb=short",
    "total": 146,
    "passed": 146,
    "failed": 0,
    "duration_sec": 2.0,
    "runs": [
      { "passed": 146, "failed": 0, "total": 146, "duration_sec": 2.0, "returncode": 0 },
      { "passed": 146, "failed": 0, "total": 146, "duration_sec": 1.9, "returncode": 0 }
    ],
    "flaky_candidates": []
  },
  "smoke": {
    "command": "python3 run_pipeline.py --skip-ocr",
    "exit_code": 0,
    "duration_sec": 5.0,
    "status": "passed"
  }
}
```

### Обязательные поля

| Поле | Тип | Описание |
|---|---|---|
| `generated_at` | string (ISO-8601 UTC) | Дата и время генерации |
| `python_version` | string | Версия Python |
| `pytest.command` | string | Выполненная команда pytest |
| `pytest.total` | int | Всего тестов |
| `pytest.passed` | int | Прошло |
| `pytest.failed` | int | Упало |
| `pytest.duration_sec` | float | Время последнего прогона |
| `pytest.runs` | list | Детали каждого прогона |
| `pytest.flaky_candidates` | list[string] | Тесты с нестабильным исходом |
| `smoke.command` | string | Выполненная команда smoke |
| `smoke.exit_code` | int | Код выхода процесса |
| `smoke.duration_sec` | float | Время прогона |
| `smoke.status` | `"passed"` \| `"failed"` | Итоговый статус smoke |

---

## Критерии прохождения baseline

- `smoke.status = "passed"` — smoke завершается с кодом 0
- `pytest.failed = 0` — все тесты прошли
- `pytest.flaky_candidates = []` — нет нестабильных тестов

---

## Переменные окружения

| Переменная | Значение | Эффект |
|---|---|---|
| `ENABLE_FINAL_VERIFICATION` | `false` | Пропускает Claude-верификацию (шаг 6.5) |
| `GSHEETS_UPLOAD_ENABLED` | `false` | Пропускает выгрузку в Google Sheets |
| `SMOKE_MODE` | `true` | Тихий пропуск всех внешних интеграций; устойчивость к повреждённым Excel-файлам |

Все три переменные устанавливаются автоматически `quality_baseline.py`
при запуске smoke-прогона, гарантируя детерминизм без внешних API.
