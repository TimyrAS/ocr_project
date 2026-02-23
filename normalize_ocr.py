#!/usr/bin/env python3
"""
Нормализация OCR-данных под формат БД «Привилегия».

Берёт clients_database.xlsx (результат OCR) и создаёт
clients_normalized.xlsx с полями, приведёнными к формату БД.

Это позволяет:
- Быстро сверять OCR с БД (одинаковые названия столбцов)
- Импортировать в CRM без ручного переименования
- Объединять данные из карточек и БД в единую таблицу

Запуск:
    python normalize_ocr.py                          # по умолчанию
    python normalize_ocr.py --input my_file.xlsx     # свой файл
    python normalize_ocr.py --output result.xlsx     # свой выход

Маппинг полей настраивается в config.py (OCR_*_FIELD_MAP).
"""

import os
import sys
import pandas as pd
from datetime import datetime

try:
    import config
except ImportError:
    print("ОШИБКА: не найден config.py. Запустите из папки ocr_project/")
    sys.exit(1)


# ============================================================
# МАППИНГ ЛИСТОВ → КОНФИГ
# ============================================================

SHEET_FIELD_MAPS = {
    "Клиенты":    getattr(config, 'OCR_CLIENT_FIELD_MAP', {}),
    "Мед_данные": {},   # Мед. данные не маппятся на БД (нет аналога)
    "Процедуры":  getattr(config, 'OCR_PROCEDURE_FIELD_MAP', {}),
    "Покупки":    getattr(config, 'OCR_PURCHASE_FIELD_MAP', {}),
    "Комплексы":  getattr(config, 'OCR_COMPLEX_FIELD_MAP', {}),
    "Ботокс":     getattr(config, 'OCR_BOTOX_FIELD_MAP', {}),
}


# ============================================================
# НОРМАЛИЗАЦИЯ ДАННЫХ
# ============================================================

def normalize_phone(phone):
    """Приводит телефон к формату 7XXXXXXXXXX."""
    if not phone or (isinstance(phone, float)):
        return ""
    phone = str(phone).strip()
    digits = "".join(c for c in phone if c.isdigit())
    if len(digits) == 11 and digits.startswith("8"):
        digits = "7" + digits[1:]
    elif len(digits) == 10:
        digits = "7" + digits
    return digits


def normalize_date(date_val):
    """Приводит дату к формату ДД.ММ.ГГГГ."""
    if not date_val or str(date_val).strip() in ("", "nan", "NaT", "None"):
        return ""
    s = str(date_val).strip()

    # Уже в формате ДД.ММ.ГГГГ
    if len(s) >= 8 and "." in s and s.count(".") == 2:
        return s

    # Пробуем стандартные форматы
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d", "%d/%m/%Y", "%d-%m-%Y"):
        try:
            dt = datetime.strptime(s[:19], fmt)
            return dt.strftime("%d.%m.%Y")
        except (ValueError, IndexError):
            continue

    return s  # Возвращаем как есть


def normalize_doctor(doctor_name):
    """Приводит имя врача из OCR к формату БД (через DB_DOCTOR_MAP)."""
    if not doctor_name or str(doctor_name).strip() in ("", "nan"):
        return ""
    name = str(doctor_name).strip()

    # Обратный маппинг: полное имя → короткое (как в БД)
    reverse_map = getattr(config, 'DB_DOCTOR_MAP', {})
    # DB_DOCTOR_MAP: {"Оксана А. - врач": "Асшеман Оксана"}
    # Нам нужен обратный: {"Асшеман Оксана": "Оксана А. - врач"}
    for db_short, ocr_full in reverse_map.items():
        if name.lower() == ocr_full.lower():
            return db_short

    return name


# ============================================================
# ОСНОВНАЯ ЛОГИКА
# ============================================================

def normalize_sheet(df, field_map, sheet_name):
    """
    Переименовывает столбцы по маппингу и нормализует данные.
    Возвращает новый DataFrame.
    """
    if df is None or len(df) == 0:
        return pd.DataFrame()

    # Переименование столбцов
    rename_dict = {}
    for old_col in df.columns:
        if old_col in field_map:
            rename_dict[old_col] = field_map[old_col]

    new_df = df.rename(columns=rename_dict).copy()

    # Нормализация телефонов
    for col in new_df.columns:
        if col.lower() in ("телефон", "phone", "контакты"):
            new_df[col] = new_df[col].apply(normalize_phone)

    # Нормализация дат
    for col in new_df.columns:
        col_l = col.lower()
        if any(w in col_l for w in ("дата", "date", "визит")):
            new_df[col] = new_df[col].apply(normalize_date)

    # Нормализация врачей (оставляем OCR-формат, не конвертируем в БД-формат)
    # Причина: при сверке verify_with_db сам маппит через DB_DOCTOR_MAP
    # Но если лист — "Процедуры" или "Покупки", добавляем столбец с БД-форматом
    for col in new_df.columns:
        if col.lower() in ("доктор", "врач", "консультант"):
            new_df[f"{col}_БД"] = new_df[col].apply(normalize_doctor)

    return new_df


def normalize_ocr_file(input_path, output_path):
    """
    Полная нормализация OCR Excel → нормализованный Excel.

    Создаёт файл с:
    - Переименованными столбцами (как в БД)
    - Нормализованными телефонами (7XXXXXXXXXX)
    - Нормализованными датами (ДД.ММ.ГГГГ)
    - Сводным листом «Все_визиты» (процедуры + покупки + комплексы + ботокс
      объединённые в один формат, как в БД)
    """
    print(f"Нормализация: {input_path}")

    if not os.path.exists(input_path):
        print(f"  ОШИБКА: файл не найден: {input_path}")
        return None

    xf = pd.ExcelFile(input_path)
    normalized_sheets = {}

    for sheet_name in xf.sheet_names:
        df = pd.read_excel(input_path, sheet_name=sheet_name)
        field_map = SHEET_FIELD_MAPS.get(sheet_name, {})

        if field_map:
            new_df = normalize_sheet(df, field_map, sheet_name)
            # Имя листа тоже нормализуем
            normalized_sheets[sheet_name] = new_df
            print(f"  ✓ {sheet_name}: {len(new_df)} строк, "
                  f"{len(field_map)} полей переименовано")
        else:
            # Лист без маппинга — копируем как есть
            normalized_sheets[sheet_name] = df
            print(f"  ~ {sheet_name}: {len(df)} строк (без маппинга)")

    # ── Создаём сводный лист «Все_визиты» ──
    # Объединяем процедуры, покупки, комплексы, ботокс
    # в формат БД: Клиент | Телефон | Дата визита | Доктор | Процедура | Количество
    all_visits = []

    # Процедуры
    if "Процедуры" in normalized_sheets:
        proc = normalized_sheets["Процедуры"]
        for _, row in proc.iterrows():
            all_visits.append({
                "ID": row.get("ID", ""),
                "Клиент": row.get("Клиент", ""),
                "Дата визита": row.get("Дата визита", ""),
                "Доктор": row.get("Доктор", row.get("Доктор_БД", "")),
                "Процедура": row.get("Процедура", ""),
                "Количество": 1,
                "Стоимость": row.get("Стоимость", ""),
                "Источник": "Процедурный лист",
            })

    # Покупки
    if "Покупки" in normalized_sheets:
        purch = normalized_sheets["Покупки"]
        for _, row in purch.iterrows():
            all_visits.append({
                "ID": row.get("ID", ""),
                "Клиент": row.get("Клиент", ""),
                "Дата визита": row.get("Дата визита", ""),
                "Доктор": row.get("Доктор", row.get("Доктор_БД", "")),
                "Процедура": row.get("Процедура", ""),
                "Количество": 1,
                "Стоимость": row.get("Стоимость", ""),
                "Источник": "Покупки",
            })

    # Комплексы
    if "Комплексы" in normalized_sheets:
        comp = normalized_sheets["Комплексы"]
        for _, row in comp.iterrows():
            proc_name = row.get("Процедура", "")
            if not proc_name or str(proc_name) == "nan":
                proc_name = row.get("Комплекс", "")
            all_visits.append({
                "ID": row.get("ID", ""),
                "Клиент": row.get("Клиент", ""),
                "Дата визита": row.get("Дата визита",
                                       row.get("Дата процедуры", "")),
                "Доктор": row.get("Доктор", row.get("Доктор_БД", "")),
                "Процедура": proc_name,
                "Количество": row.get("Количество", 1),
                "Стоимость": row.get("Стоимость", ""),
                "Источник": "Комплекс",
            })

    # Ботокс
    if "Ботокс" in normalized_sheets:
        botox = normalized_sheets["Ботокс"]
        for _, row in botox.iterrows():
            zone = row.get("Зона", "")
            drug = row.get("Препарат", "")
            proc_name = f"Ботулинотерапия: {drug}" if drug else "Ботулинотерапия"
            if zone and str(zone) != "nan":
                proc_name += f" ({zone})"
            all_visits.append({
                "ID": row.get("ID", ""),
                "Клиент": row.get("Клиент", ""),
                "Дата визита": row.get("Дата визита", ""),
                "Доктор": "",
                "Процедура": proc_name,
                "Количество": row.get("Количество", 1),
                "Стоимость": "",
                "Источник": "Ботокс",
            })

    if all_visits:
        visits_df = pd.DataFrame(all_visits)
        # Убираем пустые строки
        visits_df = visits_df[
            visits_df["Клиент"].notna() &
            (visits_df["Клиент"] != "") &
            (visits_df["Клиент"] != "nan")
        ]
        normalized_sheets["Все_визиты"] = visits_df
        print(f"  ★ Все_визиты: {len(visits_df)} записей "
              f"(формат БД: Клиент/Дата/Доктор/Процедура/Кол-во)")

    # ── Сохраняем ──
    with pd.ExcelWriter(output_path, engine="openpyxl") as writer:
        # Сначала сводный лист
        if "Все_визиты" in normalized_sheets:
            normalized_sheets["Все_визиты"].to_excel(
                writer, sheet_name="Все_визиты", index=False
            )

        # Потом остальные
        for name, df in normalized_sheets.items():
            if name == "Все_визиты":
                continue
            if len(df) > 0:
                df.to_excel(writer, sheet_name=name, index=False)

    print(f"\n  ✓ Нормализованный файл: {output_path}")
    print(f"    Листов: {len(normalized_sheets)}")
    return output_path


# ============================================================
# CLI
# ============================================================

def main():
    import argparse

    parser = argparse.ArgumentParser(
        description="Нормализация OCR-данных под формат БД «Привилегия»"
    )
    parser.add_argument(
        '--input', '-i',
        default=getattr(config, 'OUTPUT_FILE', 'clients_database.xlsx'),
        help='Входной файл OCR Excel (по умолчанию: clients_database.xlsx)'
    )
    parser.add_argument(
        '--output', '-o',
        default=None,
        help='Выходной нормализованный файл (по умолчанию: clients_normalized.xlsx)'
    )
    args = parser.parse_args()

    output = args.output
    if not output:
        script_dir = os.path.dirname(os.path.abspath(__file__))
        output = os.path.join(
            script_dir,
            getattr(config, 'NORMALIZED_FILE', 'clients_normalized.xlsx')
        )

    print("=" * 60)
    print("НОРМАЛИЗАЦИЯ OCR → ФОРМАТ БД")
    print("=" * 60)

    normalize_ocr_file(args.input, output)

    print("\n" + "=" * 60)


if __name__ == "__main__":
    main()
