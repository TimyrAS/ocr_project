# OCR Project — CLAUDE.md

## Обзор

Пайплайн оцифровки бумажных клиентских карточек косметологической клиники:
Google Vision OCR → Claude API (парсинг) → Excel → сверка с БД «Привилегия».

## Структура директорий

```
ocr_project/
├── input/              # Входные фото карточек (JPG/PNG/HEIC)
├── data/               # Справочники и БД
│   ├── db_privilage.xlsx    # Выгрузка CRM «Привилегия»
│   ├── БД_Клиенты.xlsx     # Дополнительный справочник клиентов
│   └── price.xlsx           # Прайс-лист для сверки цен
├── ocr_cache/          # Кэш OCR-результатов + реестр обработанных
├── scripts/            # Вспомогательные скрипты
├── tests/              # Тесты (pytest)
├── ocr_logs/           # Логи пайплайна
├── ocr_debug/          # Debug bbox/confidence (если включено)
├── config.py           # Все настройки
├── run_pipeline.py     # Единый пайплайн
├── client_card_ocr.py  # OCR ядро (Vision + Claude)
├── verify_with_db.py   # Сверка с БД
├── normalize_ocr.py    # Нормализация OCR → формат БД
├── final_verification.py # Финальная верификация Claude
├── price_loader.py     # Индекс прайс-листа
└── quality_baseline.py # Baseline: pytest N раз + smoke
```

## Листы Excel (clients_database.xlsx)

| Лист | Содержимое |
|------|-----------|
| Клиенты | ФИО, телефон, email, дата рождения, ИИН, врач, скидка... |
| Процедуры | ID, ФИО, дата, процедура, описание, стоимость |
| Покупки | ID, ФИО, дата, консультант, наименование, цена |
| Комплексы | ID, пациент, контакты, врач, комплекс, процедуры |
| Ботокс | ID, ФИО, препарат, зона, дозы, даты |
| Сверка_БД | Результаты матчинга OCR ↔ БД |

## ID-схема

- OCR-клиенты: формат `[NNNN]` (числовой, из группировки)
- БД-клиенты: формат `DB-NNNN` (стабильный, из `build_db_client_index`)

## Статусы сверки

- `Найден в OCR` — клиент распознан из карточки
- `Найден в БД` — совпадение с БД (телефон или ФИО ≥2 слова + score ≥0.85)
- `Возможное совпадение в БД` — fuzzy match ниже порога уверенности
- `Нет в БД (новый для картотеки)` — не найден в БД

## Ключевые пороги (config.py)

- `FUZZY_NAME_THRESHOLD = 0.60` — группировка клиентов по ФИО
- `DB_MATCH_THRESHOLD = 0.70` — матчинг OCR ↔ БД
- `OCR_DUPLICATE_THRESHOLD = 0.90` — дедупликация страниц
- `NOT_FOUND_FUZZY_THRESHOLD = 0.85` — подтягивание полной строки

## Известные правила

- `python3` (не `python`) для запуска
- Smoke-режим: `SMOKE_MODE=true`, `ENABLE_FINAL_VERIFICATION=false`, `GSHEETS_UPLOAD_ENABLED=false`
- ENV vars имеют приоритет над config.py для флагов верификации и GSheets
- tz-aware/tz-naive: `strip tzinfo` перед сортировкой дат в `build_db_client_index`
- `run_ocr_pipeline()` возвращает `(path, price_stats)` tuple

## Тесты

```bash
python3 -m pytest tests/ -q
```

~276 тестов. Не ломай при изменении путей.

## Правила для сессий

1. Перед изменением — прочитай файл
2. Не добавляй фичи сверх ТЗ
3. После изменений — прогони `pytest -q`
4. Не коммить автоматически
