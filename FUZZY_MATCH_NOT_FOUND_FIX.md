# Отчёт: Устойчивый fuzzy-матчинг ФИО в clients_not_found.xlsx

## Дата: 2026-02-13
## Статус: ✅ Реализовано и протестировано (62/62 тестов)

---

## Цель

Сделать устойчивый матчинг ФИО при формировании `clients_not_found.xlsx`, чтобы полная OCR-строка клиента подтягивалась даже при наличии OCR-ошибок, лишних пробелов, различий в регистре и ё/е.

## Проблема

В `verify_with_db.py` функция `save_not_found_clients()` использовала строгое сравнение `val == ocr_name` для поиска полной строки клиента в OCR-данных. Это приводило к тому, что при OCR-ошибках полная информация клиента не подтягивалась в отчёт, хотя клиент корректно попадал в список ненайденных.

**Пример:**
- `verification_df` содержит: `OCR_ФИО = "Чаплено Карина"` (OCR-ошибка)
- `ocr_sheets` содержит: `ФИО = "Чапленко Карина"` + полная информация (Email, Адрес, ИИН)
- Строгое сравнение: `"Чаплено Карина" == "Чапленко Карина"` → **False**
- Результат: полная информация **не подтягивается** в `clients_not_found.xlsx`

---

## Решение

### 1. Заменено строгое сравнение на fuzzy-match

**Файл:** `verify_with_db.py` (функция `save_not_found_clients()`, строки 406-446)

**Было:**
```python
for _, nf_row in not_found.iterrows():
    ocr_name = nf_row["OCR_ФИО"]

    # Ищем полную запись в OCR
    for idx, ocr_row in clients_sheet.iterrows():
        found_match = False
        for col in clients_sheet.columns:
            col_lower = str(col).lower().strip()
            if any(alias in col_lower for alias in FIO_ALIASES):
                val = str(ocr_row[col]) if pd.notna(ocr_row[col]) else ""
                if val == ocr_name:  # ❌ Строгое сравнение
                    full_record = ocr_row.to_dict()
                    full_record["OCR_Телефон"] = nf_row["OCR_Телефон"]
                    full_record["Причина"] = STATUS_DB_NOT_FOUND
                    not_found_full.append(full_record)
                    found_match = True
                    break
        if found_match:
            break
```

**Стало:**
```python
# Порог для fuzzy-match при поиске полной строки клиента
# Достаточно высокий (0.85), чтобы избежать ложных совпадений,
# но позволяет учесть OCR-ошибки, лишние пробелы, ё/е и т.д.
FUZZY_MATCH_THRESHOLD = 0.85

for _, nf_row in not_found.iterrows():
    ocr_name = nf_row["OCR_ФИО"]

    # Ищем полную запись в OCR с fuzzy-match
    best_match_score = 0.0
    best_match_row = None

    for idx, ocr_row in clients_sheet.iterrows():
        # Ищем ФИО в OCR-данных
        for col in clients_sheet.columns:
            col_lower = str(col).lower().strip()
            if any(alias in col_lower for alias in FIO_ALIASES):
                val = str(ocr_row[col]) if pd.notna(ocr_row[col]) else ""
                if val and val != "nan":
                    # ✅ Используем match_names() для устойчивого сравнения
                    score = match_names(ocr_name, val, threshold=FUZZY_MATCH_THRESHOLD)
                    if score >= FUZZY_MATCH_THRESHOLD and score > best_match_score:
                        best_match_score = score
                        best_match_row = ocr_row
                        break  # Нашли ФИО в этой строке, переходим к следующей

    # Если нашли подходящее совпадение, добавляем полную строку
    if best_match_row is not None:
        full_record = best_match_row.to_dict()
        full_record["OCR_Телефон"] = nf_row["OCR_Телефон"]
        full_record["Причина"] = STATUS_DB_NOT_FOUND
        not_found_full.append(full_record)
```

**Ключевые изменения:**
- ✅ Используется `match_names()` - уже существующая функция с нечётким сравнением
- ✅ Порог 0.85 - баланс между точностью и устойчивостью к ошибкам
- ✅ Выбирается **лучшее совпадение** (best score) при наличии нескольких похожих
- ✅ Сохранены причина и статусы (контракт не нарушен)

---

### 2. Улучшена нормализация ФИО

**Файл:** `verify_with_db.py` (функция `normalize_name()`, строки 33-44)

**Было:**
```python
def normalize_name(name):
    """Нормализация ФИО для сравнения."""
    if not name or not isinstance(name, str):
        return ""
    name = name.strip().lower()
    # Убираем ID в квадратных скобках
    if name.startswith("[") and "]" in name:
        name = name.split("]", 1)[1].strip()
    # Убираем лишние пробелы
    name = " ".join(name.split())
    return name
```

**Стало:**
```python
def normalize_name(name):
    """Нормализация ФИО для сравнения."""
    if not name or not isinstance(name, str):
        return ""
    name = name.strip().lower()
    # Убираем ID в квадратных скобках
    if name.startswith("[") and "]" in name:
        name = name.split("]", 1)[1].strip()
    # Убираем лишние пробелы
    name = " ".join(name.split())
    # ✅ Нормализация ё → е для устойчивости к OCR-ошибкам
    name = name.replace('ё', 'е')
    return name
```

**Ключевое изменение:**
- ✅ Добавлена замена `ё → е` для устойчивости к OCR-вариациям написания

---

## Тестовые кейсы

### Добавлено 7 новых тестов:

#### В `test_new_statuses.py` (класс `TestFuzzyMatchInNotFound` - 5 тестов):

1. ✅ `test_fuzzy_match_with_extra_spaces`
   - Кейс: `"Чапленко  Карина"` (2 пробела) ↔ `"Чапленко Карина"` (1 пробел)
   - Проверка: полная строка подтягивается

2. ✅ `test_fuzzy_match_with_ocr_typo`
   - Кейс: `"Чаплено Карина"` (пропущена 'к') ↔ `"Чапленко Карина"`
   - Проверка: полная строка подтягивается несмотря на опечатку

3. ✅ `test_fuzzy_match_yo_e_equivalence`
   - Кейс: `"Семёнов Пётр"` (с ё) ↔ `"Семенов Петр"` (без ё)
   - Проверка: ё и е считаются эквивалентными

4. ✅ `test_fuzzy_match_case_insensitive`
   - Кейс: `"иванов иван"` (нижний регистр) ↔ `"ИВАНОВ ИВАН"` (верхний)
   - Проверка: регистр не влияет на матчинг

5. ✅ `test_fuzzy_match_no_false_positives`
   - Кейс: `"Иванов Иван"` ↔ `"Петров Пётр"` (совсем разные ФИО)
   - Проверка: слишком разные ФИО **не совпадают** (порог 0.85)

#### В `test_integration.py` (класс `TestFuzzyMatchNotFoundIntegration` - 2 теста):

6. ✅ `test_end_to_end_fuzzy_match_ocr_errors`
   - End-to-end тест с 3 клиентами:
     - `"Чаплено Карина"` → находит `"Чапленко Карина"`
     - `"Семёнов  Пётр"` → находит `"Семенов Петр"`
     - `"иванова мария"` → находит `"Иванова Мария"`
   - Проверка: для всех подтянулись Email, Адрес, ИИН

7. ✅ `test_fuzzy_match_prefers_best_score`
   - Кейс: `"Иванов"` при наличии: `"Иванов"`, `"Иванов Иван"`, `"Иванов Пётр"`
   - Проверка: выбирается **лучшее совпадение** (точное: `"Иванов"`)

---

## Результаты тестирования

### Синтаксис Python

```bash
$ ./.venv/bin/python -m py_compile verify_with_db.py tests/test_new_statuses.py tests/test_integration.py
# ✅ Без ошибок
```

### Все тесты (62 теста)

```bash
$ ./.venv/bin/python -m pytest tests/ -v

============================== test session starts ==============================
collected 62 items

tests/test_claude_response_normalization.py ................        [ 25%]
tests/test_final_verification_columns.py .........                  [ 40%]
tests/test_force_mode_cache_reset.py .......                        [ 51%]
tests/test_integration.py ............                              [ 70%]
tests/test_new_statuses.py ..................                       [100%]

✅ 62 passed in 1.85s
```

**Было:** 55 тестов
**Добавлено:** 7 новых тестов
**Итого:** 62 теста

---

## Верификация нормализации

```python
$ python -c "from verify_with_db import normalize_name, match_names

test_cases = [
    ('Семёнов Пётр', 'Семенов Петр'),
    ('Чаплено  Карина', 'Чапленко Карина'),
    ('иванов иван', 'ИВАНОВ ИВАН'),
]

for name1, name2 in test_cases:
    score = match_names(name1, name2, threshold=0.85)
    print(f'{name1} ↔ {name2}: Score={score:.2f}, Match={score >= 0.85}')
"
```

**Результат:**
```
Семёнов Пётр ↔ Семенов Петр: Score=1.00, Match=True
Чаплено  Карина ↔ Чапленко Карина: Score=0.97, Match=True
иванов иван ↔ ИВАНОВ ИВАН: Score=1.00, Match=True
```

---

## Критерии приёмки

| Критерий | Статус |
|----------|--------|
| Синтаксис Python без ошибок | ✅ Пройдено |
| Все текущие тесты зелёные | ✅ 62/62 |
| Новые тесты добавлены (7+) | ✅ 7 тестов |
| Лишние пробелы обрабатываются | ✅ Тест пройден |
| ё/е эквивалентны | ✅ Тест пройден |
| OCR-опечатки обрабатываются | ✅ Тест "Чаплено" → "Чапленко" |
| Регистр не важен | ✅ Тест пройден |
| Нет ложных срабатываний | ✅ Порог 0.85 |
| Полная OCR-строка подтягивается | ✅ End-to-end тест |
| Причина и статусы не изменены | ✅ Контракт сохранён |

---

## Примеры использования

### До изменений (строгое сравнение):

```
verification_df:
  OCR_ФИО: "Чаплено Карина"  (OCR-ошибка)
  OCR_Телефон: ""
  Статус_БД: "Нет в БД (новый для картотеки)"

ocr_sheets['Клиенты']:
  ФИО: "Чапленко Карина"
  Email: "chaplen@mail.ru"
  Адрес: "ул. Ленина, 10"
  ИИН: "123456789012"

clients_not_found.xlsx:
  OCR_ФИО: "Чаплено Карина"
  OCR_Телефон: ""
  Причина: "Нет в БД (новый для картотеки)"
  ❌ Email, Адрес, ИИН: отсутствуют (строгое сравнение не сработало)
```

### После изменений (fuzzy-match):

```
verification_df:
  OCR_ФИО: "Чаплено Карина"  (OCR-ошибка)
  OCR_Телефон: ""
  Статус_БД: "Нет в БД (новый для картотеки)"

ocr_sheets['Клиенты']:
  ФИО: "Чапленко Карина"
  Email: "chaplen@mail.ru"
  Адрес: "ул. Ленина, 10"
  ИИН: "123456789012"

clients_not_found.xlsx:
  ФИО: "Чапленко Карина"  (из OCR-строки)
  OCR_Телефон: ""
  ✅ Email: "chaplen@mail.ru"
  ✅ Адрес: "ул. Ленина, 10"
  ✅ ИИН: "123456789012"
  Причина: "Нет в БД (новый для картотеки)"
```

---

## Изменённые файлы

### 1. `verify_with_db.py`

**Функция `normalize_name()` (строки 33-44):**
- Добавлена нормализация `ё → е`

**Функция `save_not_found_clients()` (строки 406-446):**
- Заменено строгое сравнение `val == ocr_name` на fuzzy-match
- Добавлен порог `FUZZY_MATCH_THRESHOLD = 0.85`
- Реализован выбор лучшего совпадения (best score)
- Сохранены причина и статусы (контракт не нарушен)

### 2. `tests/test_new_statuses.py`

**Класс `TestFuzzyMatchInNotFound` (5 новых тестов):**
- `test_fuzzy_match_with_extra_spaces`
- `test_fuzzy_match_with_ocr_typo`
- `test_fuzzy_match_yo_e_equivalence`
- `test_fuzzy_match_case_insensitive`
- `test_fuzzy_match_no_false_positives`

### 3. `tests/test_integration.py`

**Класс `TestFuzzyMatchNotFoundIntegration` (2 новых теста):**
- `test_end_to_end_fuzzy_match_ocr_errors`
- `test_fuzzy_match_prefers_best_score`

---

## Summary Diff

### Добавлено:
- Fuzzy-match с порогом 0.85 для поиска полной строки клиента
- Выбор лучшего совпадения при наличии нескольких похожих
- Нормализация `ё → е` в `normalize_name()`
- 7 новых unit/integration тестов
- Комментарии и docstrings для новой логики

### Изменено:
- Функция `save_not_found_clients()`: строгое сравнение → fuzzy-match
- Функция `normalize_name()`: добавлена замена `ё → е`

### Сохранено:
- Причина (`STATUS_DB_NOT_FOUND`) не изменена
- Статусы не изменены
- Структура `clients_not_found.xlsx` не изменена
- Backward compatibility полностью сохранена
- Все существующие 55 тестов продолжают проходить

---

## Заключение

✅ **Все задачи выполнены**
✅ **62/62 тестов прошли**
✅ **Синтаксис корректен**
✅ **Контракт сохранён**
✅ **Готово к продакшену**

Изменения:
1. Fuzzy-match устойчив к OCR-ошибкам, лишним пробелам, ё/е, регистру
2. Полная OCR-строка корректно подтягивается в `clients_not_found.xlsx`
3. Порог 0.85 предотвращает ложные срабатывания
4. Нормализация `ё → е` улучшает матчинг везде, где используется `normalize_name()`
5. 7 новых тестов покрывают все edge cases

---

**Автор:** Claude Sonnet 4.5
**Дата:** 2026-02-13
