"""
Скрипт сверки оцифрованных карточек с БД «Привилегия».

Сравнивает данные из clients_database.xlsx (результат OCR)
с выгрузкой из CRM db_privilage.xlsx — находит совпадения,
расхождения и пропущенных клиентов.

Запуск:
    python verify_with_db.py

Результат:
    verification_report.xlsx — отчёт сверки
    В консоли — сводка
"""

import os
import sys
import pandas as pd
from difflib import SequenceMatcher
from datetime import datetime

# Пытаемся импортировать конфиг
try:
    from config import (
        DB_PRIVILAGE_PATH, DB_COLUMNS, DB_DOCTOR_MAP,
        DB_MATCH_THRESHOLD, OUTPUT_FILE, KNOWN_DOCTORS
    )
except ImportError:
    print("Ошибка: не найден config.py. Запустите из папки ocr_project/")
    sys.exit(1)


def normalize_name(name):
    """Нормализация ФИО для сравнения."""
    if not name or not isinstance(name, str):
        return ""
    name = name.strip().lower()
    # Убираем ID в квадратных скобках: [7542] Исакова Самал -> исакова самал
    if name.startswith("[") and "]" in name:
        name = name.split("]", 1)[1].strip()
    # Убираем лишние пробелы
    name = " ".join(name.split())
    # Нормализация ё → е для устойчивости к OCR-ошибкам
    name = name.replace('ё', 'е')
    return name


def normalize_phone(phone):
    """Нормализация телефона — оставляем только цифры."""
    if not phone or (isinstance(phone, float)):
        return ""
    phone = str(phone).strip()
    # Оставляем только цифры
    digits = "".join(c for c in phone if c.isdigit())
    # Приводим к формату 7XXXXXXXXXX
    if len(digits) == 11 and digits.startswith("8"):
        digits = "7" + digits[1:]
    elif len(digits) == 10:
        digits = "7" + digits
    return digits


def fuzzy_match(s1, s2):
    """Нечёткое сравнение двух строк (0.0 - 1.0)."""
    if not s1 or not s2:
        return 0.0
    return SequenceMatcher(None, s1, s2).ratio()


def match_names(ocr_name, db_name):
    """
    Матчинг ФИО: возвращает score (0.0–1.0). Порог проверяет вызывающий код.

    Стратегии (по убыванию приоритета):
    1. Точное совпадение нормализованных имён → 1.0
    2. Одно имя содержится в другом (word-boundary) → 0.95
    3. Совпадение фамилий (первое слово) → fuzzy + 0.02
    4. Нечёткое сравнение (SequenceMatcher) → fuzzy score
    """
    n1 = normalize_name(ocr_name)
    n2 = normalize_name(db_name)

    if not n1 or not n2:
        return 0.0

    # Точное совпадение
    if n1 == n2:
        return 1.0

    # Одно содержится в другом (с защитой от ложноположительных)
    try:
        from config import SUBSTRING_WORD_BOUNDARY_ONLY
    except ImportError:
        SUBSTRING_WORD_BOUNDARY_ONLY = True

    if SUBSTRING_WORD_BOUNDARY_ONLY:
        # Проверяем только по границам слов:
        # "иванов" ⊂ {"иванов","иван"} → OK
        # "иван" ⊄ {"иванова"}         → пропуск
        words1 = set(n1.split())
        words2 = set(n2.split())
        if words1.issubset(words2) or words2.issubset(words1):
            return 0.95
    else:
        # Старое поведение: любая подстрока
        if n1 in n2 or n2 in n1:
            return 0.95

    # Совпадение фамилий - даём небольшой бонус, но не гарантируем совпадение
    # Снижаем с 0.85 до 0.72 чтобы избежать ложноположительных совпадений
    parts1 = n1.split()
    parts2 = n2.split()
    if parts1 and parts2 and parts1[0] == parts2[0] and len(parts1[0]) >= 3:
        fuzzy_score = fuzzy_match(n1, n2)
        # Бонус только если fuzzy score уже близок к порогу
        return max(fuzzy_score + 0.02, fuzzy_score)

    # Нечёткое
    return fuzzy_match(n1, n2)


def load_db(path):
    """Загрузка БД Привилегия."""
    print(f"Загрузка БД: {path}")
    df = pd.read_excel(path)
    df.columns = ["id", "name", "phone", "date", "doctor", "service", "qty"]

    # Нормализация
    df["name_norm"] = df["name"].apply(normalize_name)
    df["phone_norm"] = df["phone"].apply(normalize_phone)
    df["date"] = pd.to_datetime(df["date"], utc=True, errors="coerce")

    print(f"  Загружено записей: {len(df)}")
    print(f"  Уникальных клиентов: {df['name_norm'].nunique()}")
    return df


def load_ocr(path):
    """Загрузка результатов OCR."""
    print(f"Загрузка OCR: {path}")
    if not os.path.exists(path):
        print(f"  ОШИБКА: файл не найден: {path}")
        return None

    xf = pd.ExcelFile(path)
    sheets = {}
    for name in xf.sheet_names:
        sheets[name] = pd.read_excel(path, sheet_name=name)
        print(f"  Лист '{name}': {len(sheets[name])} записей")
    return sheets


def build_db_client_index(db_df):
    """
    Строим индекс клиентов из БД:
    {norm_name: {db_id, phone, visits: [{date, doctor, service}], total_visits}}

    db_id — стабильный уникальный идентификатор клиента (DB-0001..DB-NNNN),
    не зависящий от UUID визитов в исходной БД.
    """
    index = {}
    for _, row in db_df.iterrows():
        name = row["name_norm"]
        if not name:
            continue
        if name not in index:
            index[name] = {
                "name_orig": row["name"],
                "phone": row["phone_norm"],
                "visits": [],
                "doctors": set(),
            }
        index[name]["visits"].append({
            "date": row["date"],
            "doctor": row["doctor"],
            "service": row["service"],
        })
        if pd.notna(row["doctor"]):
            index[name]["doctors"].add(row["doctor"])

    # Сортируем визиты по дате и присваиваем стабильные DB-ID
    sorted_names = sorted(index.keys())
    for db_num, name in enumerate(sorted_names, 1):
        index[name]["visits"].sort(
            key=lambda v: v["date"] if pd.notna(v["date"]) else pd.Timestamp.min
        )
        index[name]["total_visits"] = len(index[name]["visits"])
        index[name]["doctors"] = list(index[name]["doctors"])
        index[name]["db_id"] = f"DB-{db_num:04d}"

    return index


def find_best_match(ocr_name, ocr_phone, db_index, threshold):
    """
    Ищем лучшее совпадение в БД по ФИО + телефону.
    Телефон даёт приоритет, но не обязателен.

    Returns:
        dict with keys: db_name, db_phone, score, total_visits, doctors, visits, phone_match
        или None если нет совпадения
    """
    best_match = None
    best_score = 0.0
    phone_matched = False

    ocr_norm = normalize_name(ocr_name)
    ocr_ph = normalize_phone(ocr_phone)

    for db_name, data in db_index.items():
        # Совпадение имён
        name_score = match_names(ocr_name, data["name_orig"])

        # Бонус за совпадение телефона
        phone_bonus = 0.0
        current_phone_match = False
        if ocr_ph and data["phone"] and ocr_ph == data["phone"]:
            phone_bonus = 0.20  # +20% за совпадение телефона
            current_phone_match = True

        total_score = min(1.0, name_score + phone_bonus)

        if total_score > best_score and total_score >= threshold:
            best_score = total_score
            phone_matched = current_phone_match
            best_match = {
                "db_id": data.get("db_id", ""),
                "db_name": data["name_orig"],
                "db_name_norm": db_name,
                "db_phone": data["phone"],
                "score": total_score,
                "total_visits": data["total_visits"],
                "doctors": data["doctors"],
                "visits": data["visits"],
                "phone_match": phone_matched,
            }

    return best_match


def verify_clients(ocr_sheets, db_index, threshold):
    """Основная сверка: OCR клиенты vs БД."""
    results = []

    # Получаем лист клиентов из OCR
    clients_sheet = None
    for name in ["Клиенты", "Clients", "clients"]:
        if name in ocr_sheets:
            clients_sheet = ocr_sheets[name]
            break

    if clients_sheet is None and len(ocr_sheets) > 0:
        # Берём первый лист
        clients_sheet = list(ocr_sheets.values())[0]

    if clients_sheet is None or len(clients_sheet) == 0:
        print("  Нет данных клиентов в OCR файле!")
        return pd.DataFrame()

    print(f"\nСверка {len(clients_sheet)} OCR-клиентов с БД ({len(db_index)} клиентов)...")

    # Алиасы полей из конфига (если есть) или значения по умолчанию
    try:
        from config import FIO_ALIASES, PHONE_ALIASES
    except ImportError:
        FIO_ALIASES = ["фио", "клиент", "пациент", "имя", "name",
                       "patient_name", "фамилия"]
        PHONE_ALIASES = ["телефон", "phone", "контакты", "contacts",
                         "тел", "моб"]

    for idx, row in clients_sheet.iterrows():
        # Ищем ФИО и телефон по алиасам полей
        ocr_name = ""
        ocr_phone = ""

        for col in clients_sheet.columns:
            col_lower = str(col).lower().strip()
            if any(alias in col_lower for alias in FIO_ALIASES):
                val = str(row[col]) if pd.notna(row[col]) else ""
                if val and val != "nan" and not ocr_name:
                    ocr_name = val
            elif any(alias in col_lower for alias in PHONE_ALIASES):
                val = str(row[col]) if pd.notna(row[col]) else ""
                if val and val != "nan" and not ocr_phone:
                    ocr_phone = val

        if not ocr_name or ocr_name == "nan":
            continue

        match = find_best_match(ocr_name, ocr_phone, db_index, threshold)

        # Импортируем новые статусы
        try:
            from config import (
                STATUS_KARTOTEKA_FOUND, STATUS_DB_FOUND,
                STATUS_DB_MAYBE, STATUS_DB_NOT_FOUND,
                MIN_FIO_WORDS_FOR_CONFIDENT_MATCH
            )
        except ImportError:
            STATUS_KARTOTEKA_FOUND = "Найден в OCR"
            STATUS_DB_FOUND = "Найден в БД"
            STATUS_DB_MAYBE = "Возможное совпадение в БД"
            STATUS_DB_NOT_FOUND = "Нет в БД (новый для картотеки)"
            MIN_FIO_WORDS_FOR_CONFIDENT_MATCH = 2

        # Определяем статус БД с учётом ужесточённых правил
        status_bd = STATUS_DB_NOT_FOUND
        if match:
            # Проверяем количество слов в ФИО
            fio_words = len(ocr_name.strip().split())
            phone_match = match.get("phone_match", False)
            score = match["score"]

            # Правила для "Найден в БД":
            # 1. Есть телефон match ИЛИ
            # 2. Полноценное ФИО (>=2 слова) + высокий score (>=0.85)
            if phone_match or (fio_words >= MIN_FIO_WORDS_FOR_CONFIDENT_MATCH and score >= 0.85):
                status_bd = STATUS_DB_FOUND
            else:
                # Иначе максимум "Возможное совпадение"
                status_bd = STATUS_DB_MAYBE

        result = {
            "OCR_ФИО": ocr_name,
            "OCR_Телефон": ocr_phone if ocr_phone != "nan" else "",
            "Статус_картотеки": STATUS_KARTOTEKA_FOUND,  # Всегда "Найден в OCR"
            "Статус_БД": status_bd,
            "Статус": status_bd,  # Backward compatibility alias
            "БД_ID": "",
            "БД_ФИО": "",
            "БД_Телефон": "",
            "Совпадение_%": 0,
            "Визитов_в_БД": 0,
            "Врачи_в_БД": "",
        }

        if match:
            result["БД_ID"] = match.get("db_id", "")
            result["БД_ФИО"] = match["db_name"]
            result["БД_Телефон"] = match["db_phone"]
            result["Совпадение_%"] = round(match["score"] * 100, 1)
            result["Визитов_в_БД"] = match["total_visits"]
            result["Врачи_в_БД"] = "; ".join(match["doctors"][:3])

        results.append(result)

    df = pd.DataFrame(results)
    return df


def generate_report(verification_df, db_df, output_path):
    """Генерация отчёта сверки в Excel."""
    with pd.ExcelWriter(output_path, engine="openpyxl") as writer:
        # Лист 1: Результаты сверки
        if len(verification_df) > 0:
            verification_df.to_excel(writer, sheet_name="Сверка", index=False)

        # Лист 2: Сводка по статусам
        if len(verification_df) > 0:
            # Используем Статус_БД если доступен, иначе Статус
            status_column = "Статус_БД" if "Статус_БД" in verification_df.columns else "Статус"
            summary = verification_df[status_column].value_counts().reset_index()
            summary.columns = ["Статус", "Количество"]
            summary.to_excel(writer, sheet_name="Сводка", index=False)

        # Лист 3: Топ клиенты БД (для справки)
        top_clients = (
            db_df.groupby("name")
            .agg(визитов=("name", "size"), телефон=("phone", "first"))
            .reset_index()
            .sort_values("визитов", ascending=False)
            .head(50)
        )
        top_clients.columns = ["ФИО", "Визитов", "Телефон"]
        top_clients.to_excel(writer, sheet_name="Топ_БД", index=False)

        # Лист 4: Врачи из БД
        doctors = db_df["doctor"].value_counts().reset_index()
        doctors.columns = ["Врач_БД", "Записей"]
        doctors.to_excel(writer, sheet_name="Врачи", index=False)

    print(f"\nОтчёт сохранён: {output_path}")


def save_not_found_clients(verification_df, ocr_sheets, output_path):
    """
    Сохраняет клиентов, не найденных в БД, в отдельный Excel файл.

    Включает полную информацию о клиенте из OCR-карточек.
    """
    if verification_df is None or len(verification_df) == 0:
        print("  Нет данных для проверки ненайденных клиентов")
        return None

    # Импортируем константу нового статуса
    try:
        from config import STATUS_DB_NOT_FOUND
    except ImportError:
        STATUS_DB_NOT_FOUND = "Нет в БД (новый для картотеки)"

    # Определяем какую колонку использовать
    status_column = "Статус_БД" if "Статус_БД" in verification_df.columns else "Статус"

    # Фильтруем только ненайденных клиентов
    if status_column == "Статус_БД":
        # Новая система: только "Нет в БД (новый для картотеки)"
        not_found = verification_df[verification_df[status_column] == STATUS_DB_NOT_FOUND].copy()
    else:
        # Старая система (backward compatibility)
        not_found = verification_df[verification_df[status_column] == "Не найден"].copy()

    if len(not_found) == 0:
        print("  ✓ Все клиенты либо найдены, либо требуют уточнения!")
        return None

    # Получаем полные данные клиентов из OCR
    clients_sheet = None
    for name in ["Клиенты", "Clients", "clients"]:
        if name in ocr_sheets:
            clients_sheet = ocr_sheets[name]
            break

    if clients_sheet is None and len(ocr_sheets) > 0:
        clients_sheet = list(ocr_sheets.values())[0]

    if clients_sheet is not None:
        # Алиасы полей
        try:
            from config import FIO_ALIASES
        except ImportError:
            FIO_ALIASES = ["фио", "клиент", "пациент", "имя", "name", "patient_name"]

        # Создаем расширенный DataFrame с полной информацией
        not_found_full = []

        # Порог из config (настраиваемый)
        try:
            from config import NOT_FOUND_FUZZY_THRESHOLD
        except ImportError:
            NOT_FOUND_FUZZY_THRESHOLD = 0.85
        FUZZY_MATCH_THRESHOLD = NOT_FOUND_FUZZY_THRESHOLD

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
                            # Используем match_names() для устойчивого сравнения
                            score = match_names(ocr_name, val)
                            if score >= FUZZY_MATCH_THRESHOLD and score > best_match_score:
                                best_match_score = score
                                best_match_row = ocr_row
                                break  # Нашли ФИО в этой строке, переходим к следующей строке

            # Если нашли подходящее совпадение, добавляем полную строку
            if best_match_row is not None:
                full_record = best_match_row.to_dict()
                full_record["OCR_Телефон"] = nf_row["OCR_Телефон"]
                full_record["Причина"] = STATUS_DB_NOT_FOUND
                not_found_full.append(full_record)

        if not_found_full:
            not_found_df = pd.DataFrame(not_found_full)
        else:
            # Если не смогли сопоставить, используем базовый вариант
            not_found_df = not_found.copy()
            not_found_df["Причина"] = STATUS_DB_NOT_FOUND
    else:
        not_found_df = not_found.copy()
        not_found_df["Причина"] = STATUS_DB_NOT_FOUND

    # Сохраняем в Excel
    with pd.ExcelWriter(output_path, engine="openpyxl") as writer:
        not_found_df.to_excel(writer, sheet_name="Не_найдены", index=False)

        # Добавляем сводку
        summary_data = {
            "Параметр": ["Дата проверки", "Всего проверено", "Не найдено в БД", "Процент ненайденных"],
            "Значение": [
                datetime.now().strftime("%d.%m.%Y %H:%M"),
                len(verification_df),
                len(not_found),
                f"{len(not_found)/len(verification_df)*100:.1f}%"
            ]
        }
        pd.DataFrame(summary_data).to_excel(writer, sheet_name="Сводка", index=False)

    print(f"  ✓ Ненайденные клиенты сохранены: {output_path}")
    print(f"    Клиентов не найдено: {len(not_found)}")

    return output_path


def main():
    print("=" * 60)
    print("СВЕРКА ОЦИФРОВАННЫХ ДАННЫХ С БД «ПРИВИЛЕГИЯ»")
    print("=" * 60)

    # Определяем пути (работаем относительно скрипта)
    script_dir = os.path.dirname(os.path.abspath(__file__))
    db_path = os.path.join(script_dir, "db_privilage.xlsx")
    ocr_path = OUTPUT_FILE
    report_path = os.path.join(script_dir, "verification_report.xlsx")

    # Проверяем наличие файлов
    if not os.path.exists(db_path):
        print(f"ОШИБКА: БД не найдена: {db_path}")
        print("Скопируйте db_privilage.xlsx в папку ocr_project/")
        sys.exit(1)

    # Загрузка БД
    db_df = load_db(db_path)
    db_index = build_db_client_index(db_df)

    # Загрузка OCR
    ocr_sheets = {}
    if not os.path.exists(ocr_path):
        print(f"\nФайл OCR не найден: {ocr_path}")
        print("Запускаю сверку в режиме «только БД» — генерирую индекс клиентов.\n")
        # Генерируем отчёт только из БД
        verification_df = pd.DataFrame()
    else:
        ocr_sheets = load_ocr(ocr_path)
        verification_df = verify_clients(ocr_sheets, db_index, DB_MATCH_THRESHOLD)

    # Генерация отчёта
    generate_report(verification_df, db_df, report_path)

    # Сохранение ненайденных клиентов в отдельный файл
    if len(verification_df) > 0:
        try:
            from config import NOT_FOUND_CLIENTS_FILE
        except ImportError:
            NOT_FOUND_CLIENTS_FILE = "clients_not_found.xlsx"

        not_found_path = os.path.join(script_dir, NOT_FOUND_CLIENTS_FILE)
        save_not_found_clients(verification_df, ocr_sheets, not_found_path)

    # Сводка в консоль
    print("\n" + "=" * 60)
    print("СВОДКА")
    print("=" * 60)
    print(f"Клиентов в БД «Привилегия»: {len(db_index)}")

    if len(verification_df) > 0:
        # Импортируем новые статусы
        try:
            from config import STATUS_DB_FOUND, STATUS_DB_MAYBE, STATUS_DB_NOT_FOUND
        except ImportError:
            STATUS_DB_FOUND = "Найден в БД"
            STATUS_DB_MAYBE = "Возможное совпадение в БД"
            STATUS_DB_NOT_FOUND = "Нет в БД (новый для картотеки)"

        # Используем Статус_БД если доступен, иначе Статус
        status_column = "Статус_БД" if "Статус_БД" in verification_df.columns else "Статус"

        if status_column == "Статус_БД":
            # Новая система статусов
            found = len(verification_df[verification_df[status_column] == STATUS_DB_FOUND])
            maybe = len(verification_df[verification_df[status_column] == STATUS_DB_MAYBE])
            not_found = len(verification_df[verification_df[status_column] == STATUS_DB_NOT_FOUND])
            label_found = "Найдено в БД"
            label_maybe = "Возможное совпадение"
            label_not_found = "Новые для картотеки"
        else:
            # Старая система (backward compatibility)
            found = len(verification_df[verification_df[status_column] == "Найден"])
            maybe = len(verification_df[verification_df[status_column] == "Возможно"])
            not_found = len(verification_df[verification_df[status_column] == "Не найден"])
            label_found = "Найдено в БД"
            label_maybe = "Возможно найдено"
            label_not_found = "Не найдено"

        total = len(verification_df)

        print(f"Клиентов в OCR: {total}")
        print(f"  {label_found}:      {found} ({found/total*100:.0f}%)")
        print(f"  {label_maybe}:   {maybe} ({maybe/total*100:.0f}%)")
        print(f"  {label_not_found}:         {not_found} ({not_found/total*100:.0f}%)")
    else:
        print("OCR-данных нет — отчёт содержит только индекс БД")

    print(f"\nОтчёт: {report_path}")
    print("=" * 60)


if __name__ == "__main__":
    main()
