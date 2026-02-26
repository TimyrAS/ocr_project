# Contributing

## Требования

- Python 3.12+
- Зависимости: `pip install -r requirements.txt pytest`

---

## Тесты

```bash
# Весь тест-сьют (257 тестов)
python3 -m pytest tests/ -q

# Только схема baseline
python3 -m pytest tests/test_quality_baseline_schema.py -q

# Только smoke и tz-хелперы
python3 -m pytest tests/test_smoke_deterministic.py -q
```

---

## Smoke-прогон

Детерминированный прогон без внешних API (Google Vision, Claude, Google Sheets):

```bash
ENABLE_FINAL_VERIFICATION=false GSHEETS_UPLOAD_ENABLED=false SMOKE_MODE=true \
    python3 run_pipeline.py --skip-ocr
```

| Переменная | Значение | Эффект |
|---|---|---|
| `ENABLE_FINAL_VERIFICATION` | `false` | Пропускает Claude-верификацию (шаг 6.5) |
| `GSHEETS_UPLOAD_ENABLED` | `false` | Пропускает выгрузку в Google Sheets |
| `SMOKE_MODE` | `true` | Тихий пропуск внешних интеграций; устойчивость к повреждённым Excel |

---

## Quality Baseline

Генерирует `artifacts/quality/baseline.json` и `baseline.md`.
Все три env-флага выставляются автоматически внутри `quality_baseline.py`.

```bash
# Стандартный запуск (2 прогона pytest для flaky-детекции)
python3 quality_baseline.py

# CI-профиль: 1 прогон, быстрый вывод
python3 quality_baseline.py --repeat 1 --pytest-cmd "python3 -m pytest tests/ -q"
```

Каталог `artifacts/` исключён из git (`.gitignore`) — артефакты не коммитятся.

Подробности: [QUALITY_BASELINE.md](QUALITY_BASELINE.md).

---

## Структура коммитов

Следуйте правилу атомарности: один коммит — одно изменение.

```
feat: <новая возможность>
fix:  <исправление бага>
refactor: <рефакторинг без изменения поведения>
test: <только тесты>
docs: <только документация>
ci:   <изменения CI/CD>
```
