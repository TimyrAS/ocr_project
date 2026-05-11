#!/usr/bin/env python3
"""
ЕДИНЫЙ ПАЙПЛАЙН: OCR → Excel → Сверка с БД → Итоговый отчёт.

Объединяет client_card_ocr.py и verify_with_db.py
в один автоматический процесс.

Порядок работы:
  ШАГ 1 — OCR + парсинг (Google Vision + Claude API)
  ШАГ 2 — Группировка клиентов (fuzzy matching ФИО/ИИН/телефон)
  ШАГ 3 — Дедупликация страниц (перцептивный хэш + OCR-текст)
  ШАГ 4 — Запись клиентской базы в Excel (6 листов)
  ШАГ 5 — Сверка с БД «Привилегия» (матчинг ФИО + телефон)
  ШАГ 6 — Итоговый отчёт (Excel + консоль)

Запуск:
    python run_pipeline.py              # Полный пайплайн
    python run_pipeline.py --skip-ocr   # Только сверка (OCR уже сделан)
    python run_pipeline.py --only-ocr   # Только OCR (без сверки)

Результаты:
    clients_database.xlsx      — клиентская база (6 листов)
    verification_report.xlsx   — отчёт сверки с БД
    pipeline_report.xlsx       — итоговый комбинированный отчёт
    raw_results.json           — сырые данные OCR
    ocr_logs/ocr_YYYY-MM-DD.log — детальный лог
"""

import os
import sys

# Фикс SSL для gRPC на macOS: указываем путь к корневым сертификатам
if not os.environ.get("GRPC_DEFAULT_SSL_ROOTS_FILE_PATH"):
    try:
        import certifi
        os.environ["GRPC_DEFAULT_SSL_ROOTS_FILE_PATH"] = certifi.where()
    except ImportError:
        pass

import time
import json
import argparse
import logging
from datetime import datetime

import pandas as pd

# ============================================================
# Проверка окружения перед импортом тяжёлых модулей
# ============================================================

def check_config():
    """Проверяет наличие config.py и критических параметров."""
    try:
        import config
    except ImportError:
        print("ОШИБКА: не найден config.py.")
        print("Запустите скрипт из папки ocr_project/")
        sys.exit(1)
    return config


def check_dependencies():
    """Проверяет все зависимости."""
    missing = []
    for module in ['google.cloud.vision', 'anthropic', 'openpyxl', 'PIL', 'tqdm']:
        try:
            __import__(module)
        except ImportError:
            missing.append(module)
    if missing:
        print(f"ОШИБКА: не установлены пакеты: {', '.join(missing)}")
        print("Установите: pip install google-cloud-vision anthropic openpyxl Pillow tqdm")
        sys.exit(1)


# ============================================================
# ЛОГИРОВАНИЕ ДЛЯ ПАЙПЛАЙНА
# ============================================================

def setup_pipeline_logging(config):
    """Настройка логирования для пайплайна."""
    log_folder = getattr(config, 'LOG_FOLDER', './ocr_logs')
    # Fallback: если путь недоступен, пишем логи рядом со скриптом
    try:
        os.makedirs(log_folder, exist_ok=True)
    except (PermissionError, OSError):
        log_folder = os.path.join(
            os.path.dirname(os.path.abspath(__file__)), "ocr_logs"
        )
        os.makedirs(log_folder, exist_ok=True)

    today = datetime.now().strftime('%Y-%m-%d')
    log_file = os.path.join(log_folder, f"pipeline_{today}.log")

    logger = logging.getLogger('pipeline')
    logger.setLevel(logging.DEBUG)
    logger.handlers.clear()

    # Файл — полный лог
    fh = logging.FileHandler(log_file, encoding='utf-8')
    fh.setLevel(logging.DEBUG)
    fh.setFormatter(logging.Formatter(
        '%(asctime)s | %(levelname)-7s | %(message)s',
        datefmt='%H:%M:%S'
    ))
    logger.addHandler(fh)

    # Консоль — основной вывод
    ch = logging.StreamHandler(sys.stdout)
    ch.setLevel(logging.INFO)
    ch.setFormatter(logging.Formatter('%(message)s'))
    logger.addHandler(ch)

    logger.info(f"Лог пайплайна: {log_file}")
    return logger


# ============================================================
# ШАГ 1-4: OCR ПАЙПЛАЙН
# (импорт из client_card_ocr.py)
# ============================================================

def run_ocr_pipeline(log, config):
    """
    Запускает полный OCR-пайплайн:
    Vision API → Claude → группировка → дедупликация → Excel.

    Возвращает путь к созданному Excel-файлу.
    """
    log.info("")
    log.info("╔══════════════════════════════════════════════════════╗")
    log.info("║  ШАГ 1-4: ОЦИФРОВКА КАРТОЧЕК (OCR)                 ║")
    log.info("╚══════════════════════════════════════════════════════╝")

    # Импортируем функции из client_card_ocr
    try:
        from client_card_ocr import (
            init_vision_client, init_claude_client,
            process_all_images, group_by_client,
            deduplicate_pages, write_to_excel
        )
    except ImportError as e:
        log.error(f"Не удалось импортировать client_card_ocr: {e}")
        log.error("Убедитесь, что client_card_ocr.py находится в той же папке.")
        return None

    # --- Проверки ---
    if not os.path.exists(config.GOOGLE_VISION_CREDENTIALS):
        log.error(f"Файл Google Vision не найден: {config.GOOGLE_VISION_CREDENTIALS}")
        return None

    if not os.path.exists(config.INPUT_FOLDER):
        log.error(f"Папка с фото не найдена: {config.INPUT_FOLDER}")
        return None

    # Считаем фото
    from pathlib import Path
    image_files = [
        f for f in os.listdir(config.INPUT_FOLDER)
        if Path(f).suffix.lower() in config.IMAGE_EXTENSIONS
    ]

    if not image_files:
        log.error(f"Фотографии не найдены в: {config.INPUT_FOLDER}")
        return None

    log.info(f"\n  Папка фото: {config.INPUT_FOLDER}")
    log.info(f"  Найдено фото: {len(image_files)}")
    log.info(f"  Модель Claude: {config.CLAUDE_MODEL}")

    base_url = getattr(config, 'ANTHROPIC_BASE_URL', None)
    if base_url:
        log.info(f"  Proxy: {base_url}")

    # --- ШАГ 1: Инициализация API ---
    log.info("\n── ШАГ 1: Инициализация API ──")
    try:
        vision_client = init_vision_client()
        log.info("  Google Vision — OK ✓")
    except Exception as e:
        log.error(f"  Google Vision — ОШИБКА: {e}")
        return None

    try:
        claude_client = init_claude_client()
        log.info("  Claude API — OK ✓")
    except Exception as e:
        log.error(f"  Claude API — ОШИБКА: {e}")
        return None

    # --- ШАГ 2: OCR + парсинг ---
    log.info("\n── ШАГ 2: OCR + интеллектуальный парсинг ──")
    t_ocr = time.time()
    results = process_all_images(vision_client, claude_client)
    ocr_time = time.time() - t_ocr

    # Сохраняем сырые данные
    raw_path = os.path.join(
        os.path.dirname(config.OUTPUT_FILE) or '.', "raw_results.json"
    )
    with open(raw_path, 'w', encoding='utf-8') as f:
        json.dump(results, f, ensure_ascii=False, indent=2)
    log.info(f"  Сырые данные: {raw_path}")

    # Статистика OCR
    page_types = {}
    errors = 0
    for r in results:
        pt = r.get("page_type", "unknown")
        page_types[pt] = page_types.get(pt, 0) + 1
        if pt == "error":
            errors += 1

    log.info(f"\n  Результаты OCR ({ocr_time:.0f}с):")
    for pt, cnt in sorted(page_types.items()):
        log.info(f"    {pt}: {cnt}")
    if errors:
        log.warning(f"    ⚠ Ошибок: {errors}")

    successful_results = [r for r in results if r.get("page_type") != "error"]
    if not successful_results:
        log.error("\n  ✗ Нет успешно распарсенных страниц. Excel не обновлён.")
        log.error("  Исправьте ошибку OCR/Claude и повторите запуск.")
        return None

    # --- ШАГ 3: Группировка ---
    log.info("\n── ШАГ 3: Группировка по клиентам ──")
    grouped = group_by_client(results)
    n_clients = len([k for k in grouped if k != '_unmatched'])
    log.info(f"  Уникальных клиентов: {n_clients}")

    # --- ШАГ 4: Дедупликация + Excel ---
    log.info("\n── ШАГ 4: Дедупликация + запись Excel ──")
    grouped = deduplicate_pages(grouped)
    try:
        from price_loader import PriceIndex
        price_index = PriceIndex()
        price_index.load(getattr(config, "PRICE_LIST_PATH", ""))
    except ImportError:
        price_index = None
    try:
        price_stats = write_to_excel(grouped, results, price_index=price_index)
    except PermissionError as e:
        log.error(f"\n  ✗ Не удалось записать Excel: {e}")
        log.error("  Закройте clients_database.xlsx в Excel/предпросмотре и повторите запуск.")
        return None

    log.info(f"\n  ✓ Excel сохранён: {config.OUTPUT_FILE}")

    return config.OUTPUT_FILE, price_stats


# ============================================================
# ШАГ 4.5: НОРМАЛИЗАЦИЯ OCR → ФОРМАТ БД
# (импорт из normalize_ocr.py)
# ============================================================

def run_normalization(log, config, ocr_excel_path):
    """
    Нормализует OCR Excel — переименовывает поля под формат БД,
    создаёт сводный лист «Все_визиты».

    Возвращает путь к нормализованному файлу или None.
    """
    log.info("")
    log.info("╔══════════════════════════════════════════════════════╗")
    log.info("║  ШАГ 4.5: НОРМАЛИЗАЦИЯ OCR → ФОРМАТ БД             ║")
    log.info("╚══════════════════════════════════════════════════════╝")

    try:
        from normalize_ocr import normalize_ocr_file
    except ImportError as e:
        log.error(f"Не удалось импортировать normalize_ocr: {e}")
        log.warning("Нормализация пропущена.")
        return None

    if not ocr_excel_path or not os.path.exists(ocr_excel_path):
        log.warning(f"  OCR-файл не найден: {ocr_excel_path}")
        log.warning("  Нормализация пропущена.")
        return None

    script_dir = os.path.dirname(os.path.abspath(__file__))
    normalized_name = getattr(config, 'NORMALIZED_FILE', 'clients_normalized.xlsx')
    normalized_path = os.path.join(script_dir, normalized_name)

    t_norm = time.time()
    try:
        result = normalize_ocr_file(ocr_excel_path, normalized_path)
    except Exception as e:
        log.warning(f"  Нормализация не удалась (файл повреждён или нечитаем): {e}")
        return None
    norm_time = time.time() - t_norm

    if result:
        log.info(f"  Нормализация за {norm_time:.1f}с")
        return normalized_path
    else:
        log.warning("  Нормализация не удалась.")
        return None


# ============================================================
# ФЛАГ ФИНАЛЬНОЙ ВЕРИФИКАЦИИ
# ============================================================

# Значения env var / config, которые отключают финальную верификацию
_FALSY_VERIF = {"false", "0", "no", "off"}
_TRUTHY_FLAGS = {"true", "1", "yes", "on"}


def _is_smoke_mode() -> bool:
    """True если SMOKE_MODE задан как truthy (true/1/yes/on)."""
    return os.environ.get("SMOKE_MODE", "").lower().strip() in _TRUTHY_FLAGS


def _gsheets_disabled(cfg) -> bool:
    """
    Возвращает True, если выгрузка в Google Sheets отключена.

    Приоритет:
    1. SMOKE_MODE=true → всегда отключено (тихий пропуск)
    2. ENV GSHEETS_UPLOAD_ENABLED=falsy → отключено
    3. config.GSHEETS_UPLOAD_ENABLED=False → отключено
    """
    if _is_smoke_mode():
        return True
    env_val = os.environ.get("GSHEETS_UPLOAD_ENABLED", "").lower().strip()
    if env_val:
        return env_val in _FALSY_VERIF
    return not bool(getattr(cfg, "GSHEETS_UPLOAD_ENABLED", False))


def _final_verification_disabled(cfg) -> bool:
    """
    Возвращает True, если финальная верификация Claude отключена.

    Приоритет:
    1. Переменная окружения ENABLE_FINAL_VERIFICATION (если задана):
       значения false / 0 / no / off трактуются как «выключено».
    2. config.ENABLE_FINAL_VERIFICATION (если ENV не задана).
    """
    env_val = os.environ.get("ENABLE_FINAL_VERIFICATION", "").lower().strip()
    if env_val:                             # ENV задана → она приоритетна
        return env_val in _FALSY_VERIF
    # ENV не задана → берём из config
    return not bool(getattr(cfg, "ENABLE_FINAL_VERIFICATION", True))


# ============================================================
# ШАГ 5-6: СВЕРКА С БД
# (импорт из verify_with_db.py)
# ============================================================

def run_verification(log, config, ocr_excel_path):
    """
    Запускает сверку оцифрованных данных с БД «Привилегия».

    Возвращает (путь_к_отчёту, DataFrame_сверки) или (None, None).
    """
    log.info("")
    log.info("╔══════════════════════════════════════════════════════╗")
    log.info("║  ШАГ 5-6: СВЕРКА С БД «ПРИВИЛЕГИЯ»                 ║")
    log.info("╚══════════════════════════════════════════════════════╝")

    # Импортируем функции из verify_with_db
    try:
        from verify_with_db import (
            load_db, load_ocr, build_db_client_index,
            verify_clients, generate_report as generate_verification_report,
            save_not_found_clients
        )
    except ImportError as e:
        log.error(f"Не удалось импортировать verify_with_db: {e}")
        log.error("Убедитесь, что verify_with_db.py находится в той же папке.")
        return None, None

    # Определяем пути
    script_dir = os.path.dirname(os.path.abspath(__file__))
    db_path = getattr(config, 'DB_PRIVILAGE_PATH',
                      os.path.join(script_dir, "db_privilage.xlsx"))
    report_path = os.path.join(script_dir, "verification_report.xlsx")

    # Проверяем наличие БД
    if not os.path.exists(db_path):
        log.warning(f"  БД не найдена: {db_path}")
        log.warning("  Скопируйте db_privilage.xlsx в папку data/")
        log.warning("  Сверка пропущена.")
        return None, None

    # --- Загрузка БД ---
    log.info("\n── ШАГ 5: Загрузка и индексация БД ──")
    t_db = time.time()
    db_df = load_db(db_path)
    db_index = build_db_client_index(db_df)
    db_time = time.time() - t_db
    log.info(f"  Клиентов в БД: {len(db_index)}")
    log.info(f"  Загрузка за {db_time:.1f}с")

    # --- Сверка ---
    log.info("\n── ШАГ 6: Матчинг OCR ↔ БД ──")

    threshold = getattr(config, 'DB_MATCH_THRESHOLD', 0.70)
    log.info(f"  Порог совпадения: {threshold*100:.0f}%")

    verification_df = pd.DataFrame()
    ocr_sheets = None

    if ocr_excel_path and os.path.exists(ocr_excel_path):
        t_match = time.time()
        try:
            ocr_sheets = load_ocr(ocr_excel_path)
        except Exception as e:
            log.warning(f"  Не удалось загрузить OCR-файл (повреждён или нечитаем): {e}")
            ocr_sheets = None
        if ocr_sheets:
            verification_df = verify_clients(ocr_sheets, db_index, threshold)
        match_time = time.time() - t_match
        log.info(f"  Сверка за {match_time:.1f}с")
    else:
        log.warning(f"  OCR-файл не найден: {ocr_excel_path}")
        log.warning("  Генерация отчёта только из БД.")

    # ========== ШАГ 6.5: ФИНАЛЬНАЯ ВЕРИФИКАЦИЯ CLAUDE ==========
    # Guard: ENV ENABLE_FINAL_VERIFICATION (приоритет) или config.ENABLE_FINAL_VERIFICATION.
    # Значения false/0/no/off отключают Claude. quality_baseline.py ставит ENV=false.
    if _final_verification_disabled(config) and len(verification_df) > 0 and ocr_sheets:
        log.info("\n── ШАГ 6.5: Финальная верификация пропущена (отключено) ──")
    elif len(verification_df) > 0 and ocr_sheets:
        log.info("\n── ШАГ 6.5: Финальная верификация Claude ──")

        try:
            from final_verification import run_final_claude_verification
            from client_card_ocr import init_claude_client

            # Проверяем режим fallback-only
            fallback_only = getattr(config, 'FINAL_VERIFICATION_FALLBACK_ONLY', True)

            if fallback_only:
                # Импортируем константы статусов
                try:
                    from config import STATUS_DB_MAYBE, STATUS_DB_NOT_FOUND
                except ImportError:
                    STATUS_DB_MAYBE = "Возможное совпадение в БД"
                    STATUS_DB_NOT_FOUND = "Нет в БД (новый для картотеки)"

                # Используем Статус_БД если доступен, иначе Статус (backward compatibility)
                status_column = "Статус_БД" if "Статус_БД" in verification_df.columns else "Статус"

                # Фильтруем строки для fallback-верификации
                if status_column == "Статус_БД":
                    # Новая система: берём "Возможное совпадение" и "Нет в БД"
                    fallback_mask = verification_df[status_column].isin([
                        STATUS_DB_MAYBE,
                        STATUS_DB_NOT_FOUND
                    ])
                    status_names = f"{STATUS_DB_MAYBE} / {STATUS_DB_NOT_FOUND}"
                else:
                    # Старая система (backward compatibility)
                    fallback_mask = verification_df[status_column].isin(["Не найден", "Возможно"])
                    status_names = "Не найден / Возможно"

                fallback_df = verification_df[fallback_mask].copy()

                if len(fallback_df) == 0:
                    log.info(f"  Fallback-режим: нет клиентов для верификации (колонка: {status_column})")
                    log.info("  Финальная верификация пропущена.")
                else:
                    log.info(f"  Fallback-режим: верификация {len(fallback_df)} клиентов")
                    log.info(f"  Статусы: {status_names}")

                    # Инициализация Claude клиента
                    claude_client = init_claude_client()

                    t_verify = time.time()
                    enhanced_fallback_df, final_report_path = run_final_claude_verification(
                        log=log,
                        config=config,
                        claude_client=claude_client,
                        verification_df=fallback_df,
                        ocr_sheets=ocr_sheets,
                        db_index=db_index
                    )
                    verify_time = time.time() - t_verify

                    log.info(f"  Claude верификация за {verify_time:.1f}с")
                    if final_report_path:
                        log.info(f"  ✓ Отчёт верификации: {final_report_path}")

                    # Мержим результат обратно в полный verification_df
                    # Обновляем только те строки, которые были обработаны
                    for idx in enhanced_fallback_df.index:
                        if idx in verification_df.index:
                            for col in enhanced_fallback_df.columns:
                                if col.startswith('Claude_') or col in ['Возможные_совпадения_БД', 'Расхождения', 'Рекомендации', 'Исправления_OCR']:
                                    verification_df.at[idx, col] = enhanced_fallback_df.at[idx, col]
                        else:
                            # Если индекс не совпадает, добавляем новую строку (на всякий случай)
                            verification_df = pd.concat([verification_df, enhanced_fallback_df.loc[[idx]]], ignore_index=False)

                    log.info(f"  Обновлено {len(fallback_df)} записей в verification_df")

            else:
                # Режим "все клиенты"
                log.info(f"  Полная верификация всех {len(verification_df)} клиентов")

                # Инициализация Claude клиента
                claude_client = init_claude_client()

                t_verify = time.time()
                enhanced_verification_df, final_report_path = run_final_claude_verification(
                    log=log,
                    config=config,
                    claude_client=claude_client,
                    verification_df=verification_df,
                    ocr_sheets=ocr_sheets,
                    db_index=db_index
                )
                verify_time = time.time() - t_verify

                log.info(f"  Claude верификация за {verify_time:.1f}с")
                if final_report_path:
                    log.info(f"  ✓ Отчёт верификации: {final_report_path}")

                # Используем обогащённый DataFrame для дальнейшей работы
                verification_df = enhanced_verification_df

        except ImportError as e:
            log.warning(f"  Модуль final_verification не найден: {e}")
            log.warning("  Финальная верификация пропущена.")
        except Exception as e:
            log.error(f"  Ошибка финальной верификации: {e}")
            log.warning("  Продолжаем без финальной верификации.")
    # ============================================================

    # --- Генерация отчёта ---
    generate_verification_report(verification_df, db_df, report_path)
    log.info(f"  ✓ Отчёт сверки: {report_path}")

    # --- Сохранение ненайденных клиентов ---
    if len(verification_df) > 0 and ocr_sheets:
        try:
            not_found_file = getattr(config, 'NOT_FOUND_CLIENTS_FILE', 'clients_not_found.xlsx')
        except:
            not_found_file = 'clients_not_found.xlsx'

        not_found_path = os.path.join(script_dir, not_found_file)
        save_not_found_clients(verification_df, ocr_sheets, not_found_path)

    return report_path, verification_df


# ============================================================
# ИТОГОВЫЙ КОМБИНИРОВАННЫЙ ОТЧЁТ
# ============================================================

def generate_pipeline_report(log, config, verification_df, ocr_excel_path,
                             price_stats=None):
    """
    Генерирует итоговый комбинированный отчёт pipeline_report.xlsx
    со всеми ключевыми данными в одном файле.
    """
    script_dir = os.path.dirname(os.path.abspath(__file__))
    report_path = os.path.join(script_dir, "pipeline_report.xlsx")

    log.info("\n── Генерация итогового отчёта ──")

    try:
        with pd.ExcelWriter(report_path, engine="openpyxl") as writer:

            # Лист 1: Сводка пайплайна
            summary_data = {
                "Параметр": [
                    "Дата запуска",
                    "Модель Claude",
                    "Папка фото",
                    "Порог группировки ФИО",
                    "Порог дедупликации OCR",
                    "Порог сверки с БД",
                ],
                "Значение": [
                    datetime.now().strftime("%d.%m.%Y %H:%M"),
                    config.CLAUDE_MODEL,
                    config.INPUT_FOLDER,
                    f"{getattr(config, 'FUZZY_NAME_THRESHOLD', 0.75)*100:.0f}%",
                    f"{getattr(config, 'OCR_DUPLICATE_THRESHOLD', 0.90)*100:.0f}%",
                    f"{getattr(config, 'DB_MATCH_THRESHOLD', 0.70)*100:.0f}%",
                ]
            }
            pd.DataFrame(summary_data).to_excel(
                writer, sheet_name="Сводка", index=False
            )

            # Лист 2: Результаты сверки (если есть)
            if verification_df is not None and len(verification_df) > 0:
                verification_df.to_excel(
                    writer, sheet_name="Сверка_OCR_vs_БД", index=False
                )

                # Лист 3: Статистика сверки
                status_col = "Статус_БД" if "Статус_БД" in verification_df.columns else "Статус"
                stats = verification_df[status_col].value_counts().reset_index()
                stats.columns = ["Статус", "Количество"]
                total = len(verification_df)
                stats["Доля_%"] = (stats["Количество"] / total * 100).round(1)
                stats.to_excel(
                    writer, sheet_name="Статистика_сверки", index=False
                )

            # Лист: Прайс-сверка
            if price_stats is not None and price_stats.get("loaded"):
                price_rows = []
                for sheet_name in ("Процедуры", "Покупки", "Комплексы"):
                    s = price_stats.get(sheet_name, {})
                    price_rows.append({
                        "Лист": sheet_name,
                        "Матчей": s.get("found", 0),
                        "Подставлено": s.get("filled", 0),
                        "Расхождений": s.get("mismatched", 0),
                        "Неоднозначных": s.get("ambiguous", 0),
                    })
                pd.DataFrame(price_rows).to_excel(
                    writer, sheet_name="Прайс-сверка", index=False
                )
            else:
                pd.DataFrame([{"Лист": "price list not loaded"}]).to_excel(
                    writer, sheet_name="Прайс-сверка", index=False
                )

            # Лист 4: Клиенты из OCR (если Excel существует)
            if ocr_excel_path and os.path.exists(ocr_excel_path):
                try:
                    ocr_clients = pd.read_excel(ocr_excel_path, sheet_name="Клиенты")
                    ocr_clients.to_excel(
                        writer, sheet_name="Клиенты_OCR", index=False
                    )
                except Exception:
                    pass

            # Лист 5: БД Привилегия — топ клиенты
            db_path = getattr(config, 'DB_PRIVILAGE_PATH',
                              os.path.join(script_dir, "db_privilage.xlsx"))
            if os.path.exists(db_path):
                try:
                    db_df = pd.read_excel(db_path)
                    db_df.columns = ["id", "name", "phone", "date",
                                     "doctor", "service", "qty"]

                    # Топ клиенты
                    top = (
                        db_df.groupby("name")
                        .agg(визитов=("name", "size"),
                             телефон=("phone", "first"))
                        .reset_index()
                        .sort_values("визитов", ascending=False)
                        .head(100)
                    )
                    top.columns = ["ФИО", "Визитов", "Телефон"]
                    top.to_excel(
                        writer, sheet_name="Топ_клиенты_БД", index=False
                    )

                    # Врачи
                    doctors = db_df["doctor"].value_counts().reset_index()
                    doctors.columns = ["Врач", "Записей"]
                    doctors.to_excel(
                        writer, sheet_name="Врачи_БД", index=False
                    )

                    # Услуги
                    services = db_df["service"].value_counts().reset_index().head(50)
                    services.columns = ["Услуга", "Кол-во"]
                    services.to_excel(
                        writer, sheet_name="Топ_услуги_БД", index=False
                    )
                except Exception as e:
                    log.warning(f"  Не удалось прочитать БД: {e}")

        log.info(f"  ✓ Итоговый отчёт: {report_path}")
        return report_path

    except Exception as e:
        log.error(f"  Ошибка генерации отчёта: {e}")
        return None


# ============================================================
# ПЕЧАТЬ ФИНАЛЬНОЙ СВОДКИ
# ============================================================

def print_summary(log, verification_df, total_time, ocr_excel_path, config):
    """Красивая сводка в консоль."""
    log.info("")
    log.info("╔══════════════════════════════════════════════════════╗")
    log.info("║              ИТОГОВАЯ СВОДКА ПАЙПЛАЙНА              ║")
    log.info("╚══════════════════════════════════════════════════════╝")

    # OCR статистика
    if ocr_excel_path and os.path.exists(ocr_excel_path):
        try:
            ocr_df = pd.read_excel(ocr_excel_path, sheet_name="Клиенты")
            log.info(f"\n  📋 ОЦИФРОВКА:")
            log.info(f"     Клиентов распознано: {len(ocr_df)}")
        except Exception:
            pass

    # Сверка
    if verification_df is not None and len(verification_df) > 0:
        total = len(verification_df)

        # Используем новые статусы (Статус_БД) если доступны, иначе старые
        status_column = "Статус_БД" if "Статус_БД" in verification_df.columns else "Статус"

        # Импортируем новые статусы
        try:
            from config import STATUS_DB_FOUND, STATUS_DB_MAYBE, STATUS_DB_NOT_FOUND
        except ImportError:
            STATUS_DB_FOUND = "Найден в БД"
            STATUS_DB_MAYBE = "Возможное совпадение в БД"
            STATUS_DB_NOT_FOUND = "Нет в БД (новый для картотеки)"

        # Подсчитываем с учетом новых и старых статусов
        if status_column == "Статус_БД":
            found = len(verification_df[verification_df[status_column] == STATUS_DB_FOUND])
            maybe = len(verification_df[verification_df[status_column] == STATUS_DB_MAYBE])
            not_found = len(verification_df[verification_df[status_column] == STATUS_DB_NOT_FOUND])
        else:
            # Backward compatibility
            found = len(verification_df[verification_df[status_column] == "Найден"])
            maybe = len(verification_df[verification_df[status_column] == "Возможно"])
            not_found = len(verification_df[verification_df[status_column] == "Не найден"])

        log.info(f"\n  🔍 СВЕРКА С БД:")
        log.info(f"     Всего оцифровано:         {total}")
        log.info(f"     ✓ Найдено в БД:           {found} ({found/total*100:.0f}%)")
        log.info(f"     ~ Возможное совпадение:   {maybe} ({maybe/total*100:.0f}%)")
        log.info(f"     ⊕ Новые для картотеки:    {not_found} ({not_found/total*100:.0f}%)")

    # Файлы
    script_dir = os.path.dirname(os.path.abspath(__file__))
    log.info(f"\n  📁 ФАЙЛЫ:")
    if ocr_excel_path:
        log.info(f"     Клиентская база:     {ocr_excel_path}")

    norm_name = getattr(config, 'NORMALIZED_FILE', 'clients_normalized.xlsx')
    norm_path = os.path.join(script_dir, norm_name)
    if os.path.exists(norm_path):
        log.info(f"     Нормализованный:     {norm_path}")

    report_path = os.path.join(script_dir, "verification_report.xlsx")
    if os.path.exists(report_path):
        log.info(f"     Отчёт сверки:        {report_path}")

    pipeline_path = os.path.join(script_dir, "pipeline_report.xlsx")
    if os.path.exists(pipeline_path):
        log.info(f"     Итоговый отчёт:      {pipeline_path}")

    not_found_file = getattr(config, 'NOT_FOUND_CLIENTS_FILE', 'clients_not_found.xlsx')
    not_found_path = os.path.join(script_dir, not_found_file)
    if os.path.exists(not_found_path):
        log.info(f"     Не найдены в БД:     {not_found_path}")

    # Время
    log.info(f"\n  ⏱  Общее время: {total_time:.0f}с ({total_time/60:.1f} мин)")
    log.info("")
    log.info("═" * 56)


# ============================================================
# MAIN
# ============================================================

def parse_args():
    parser = argparse.ArgumentParser(
        description="Единый пайплайн: OCR → Excel → Сверка с БД → Отчёт",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Примеры:
  python run_pipeline.py              # Полный пайплайн (новые карточки)
  python run_pipeline.py --skip-ocr   # Только сверка (OCR уже готов)
  python run_pipeline.py --only-ocr   # Только OCR (без сверки)
  python run_pipeline.py --force      # Обработать ВСЕ заново (сброс реестра)
        """
    )
    parser.add_argument(
        '--skip-ocr', action='store_true',
        help='Пропустить OCR (использовать существующий clients_database.xlsx)'
    )
    parser.add_argument(
        '--only-ocr', action='store_true',
        help='Только OCR, без сверки с БД'
    )
    parser.add_argument(
        '--force', action='store_true',
        help='Сбросить реестр и обработать ВСЕ карточки заново'
    )
    return parser.parse_args()


def add_verification_sheet(clients_path: str, verification_df: pd.DataFrame, log: logging.Logger):
    """
    Добавляет лист «Сверка_БД» в clients_database.xlsx
    с результатами быстрой сверки OCR-клиентов с БД «Привилегия».
    """
    if not os.path.exists(clients_path):
        log.warning(f"  add_verification_sheet: файл не найден: {clients_path}")
        return
    if verification_df is None or len(verification_df) == 0:
        log.warning("  add_verification_sheet: verification_df пустой")
        return

    from openpyxl import load_workbook
    from zipfile import BadZipFile

    # Колонки для листа сверки (только ключевые, без OCR-текстов)
    keep_cols = [
        "OCR_ФИО", "OCR_Телефон",
        "Статус_БД", "БД_ID",
        "БД_ФИО", "БД_Телефон",
        "Совпадение_%", "Визитов_в_БД", "Врачи_в_БД",
    ]
    # Оставляем только существующие колонки
    cols = [c for c in keep_cols if c in verification_df.columns]
    vdf = verification_df[cols].copy()

    try:
        wb = load_workbook(clients_path)
    except BadZipFile:
        log.warning(f"  ⚠ add_verification_sheet: файл повреждён (BadZipFile): {clients_path}")
        return
    except Exception as e:
        log.warning(f"  ⚠ add_verification_sheet: не удалось открыть файл: {e}")
        return

    # Удаляем старый лист, если есть
    sheet_name = "Сверка_БД"
    if sheet_name in wb.sheetnames:
        del wb[sheet_name]

    ws = wb.create_sheet(sheet_name)

    # Заголовки
    for col_idx, col_name in enumerate(vdf.columns, 1):
        ws.cell(row=1, column=col_idx, value=col_name)

    # Данные
    for row_idx, (_, row) in enumerate(vdf.iterrows(), 2):
        for col_idx, col_name in enumerate(vdf.columns, 1):
            val = row[col_name]
            if pd.isna(val):
                val = ""
            ws.cell(row=row_idx, column=col_idx, value=val)

    # Автофильтр
    if ws.max_row > 1:
        ws.auto_filter.ref = ws.dimensions

    wb.save(clients_path)
    wb.close()
    log.info(f"  ✓ Лист «{sheet_name}» добавлен в {clients_path} ({len(vdf)} записей)")


def enrich_clients_with_db_match(clients_path: str, verification_df: pd.DataFrame, log: logging.Logger):
    """
    Дополняет clients_database.xlsx колонками с найденным ФИО из БД и статусом совпадения.
    Колонки добавляются только в лист 'Клиенты': БД_ФИО_совпадение, Статус_совпадения, Совпадение_%.

    Использует fuzzy-матчинг через match_names() вместо точного совпадения.
    Перезаписывает только лист 'Клиенты', остальные листы сохраняются через openpyxl.
    """
    if not os.path.exists(clients_path):
        log.warning(f"  ⚠ enrich_clients: файл не найден: {clients_path}")
        return
    if verification_df is None or len(verification_df) == 0:
        log.warning("  ⚠ enrich_clients: verification_df пустой")
        return

    try:
        from verify_with_db import match_names, normalize_name, normalize_phone
        from config import DB_MATCH_THRESHOLD
    except ImportError:
        log.warning("  ⚠ enrich_clients: не удалось импортировать verify_with_db/config")
        return

    # Готовим список записей verification_df для перебора
    vdf = verification_df.copy()
    vdf_records = []
    for _, vrow in vdf.iterrows():
        ocr_fio = str(vrow.get("OCR_ФИО", ""))
        ocr_phone = normalize_phone(str(vrow.get("OCR_Телефон", "")))
        vdf_records.append({
            "ocr_fio": ocr_fio,
            "ocr_phone": ocr_phone,
            "bd_id": vrow.get("БД_ID", ""),
            "bd_fio": vrow.get("БД_ФИО", ""),
            "status_bd": vrow.get("Статус_БД", ""),
            "score_pct": vrow.get("Совпадение_%", 0),
        })

    # Читаем только лист 'Клиенты' — остальные не трогаем
    from openpyxl import load_workbook
    from zipfile import BadZipFile

    try:
        wb = load_workbook(clients_path)
    except BadZipFile:
        log.warning(f"  ⚠ enrich_clients: файл повреждён (BadZipFile): {clients_path}")
        return
    except Exception as e:
        log.warning(f"  ⚠ enrich_clients: не удалось открыть файл: {e}")
        return

    if "Клиенты" not in wb.sheetnames:
        log.warning("  ⚠ enrich_clients: лист 'Клиенты' не найден")
        wb.close()
        return

    try:
        cdf = pd.read_excel(clients_path, sheet_name="Клиенты")
    except Exception as e:
        log.warning(f"  ⚠ enrich_clients: не удалось прочитать лист Клиенты: {e}")
        wb.close()
        return

    matched_bd_id = []
    matched_bd_fio = []
    matched_status = []
    matched_score = []

    for _, row in cdf.iterrows():
        client_fio = str(row.get("ФИО", ""))
        client_phone = normalize_phone(str(row.get("Телефон", "")))

        best_bd_id = ""
        best_bd_fio = ""
        best_status = ""
        best_score = 0.0

        for vrec in vdf_records:
            # Телефон даёт точное совпадение — максимальный приоритет
            phone_hit = (client_phone and vrec["ocr_phone"]
                         and client_phone == vrec["ocr_phone"])

            # Fuzzy-матчинг ФИО
            name_score = match_names(client_fio, vrec["ocr_fio"])

            if phone_hit:
                name_score = max(name_score, 0.95)

            if name_score > best_score and name_score >= DB_MATCH_THRESHOLD:
                best_score = name_score
                best_bd_id = vrec["bd_id"]
                best_bd_fio = vrec["bd_fio"]
                best_status = vrec["status_bd"]

        matched_bd_id.append(best_bd_id)
        matched_bd_fio.append(best_bd_fio)
        matched_status.append(best_status)
        matched_score.append(round(best_score * 100, 1) if best_score > 0 else "")

    cdf["БД_ID"] = matched_bd_id
    cdf["БД_ФИО_совпадение"] = matched_bd_fio
    cdf["Статус_совпадения"] = matched_status
    cdf["Совпадение_%"] = matched_score

    # Перезаписываем только лист 'Клиенты', сохраняя остальные
    # Удаляем старый лист и создаём новый с данными
    del wb["Клиенты"]
    ws = wb.create_sheet("Клиенты", 0)

    # Заголовки
    for col_idx, col_name in enumerate(cdf.columns, 1):
        ws.cell(row=1, column=col_idx, value=col_name)

    # Данные
    for row_idx, (_, data_row) in enumerate(cdf.iterrows(), 2):
        for col_idx, col_name in enumerate(cdf.columns, 1):
            val = data_row[col_name]
            if pd.isna(val):
                val = ""
            ws.cell(row=row_idx, column=col_idx, value=val)

    wb.save(clients_path)
    wb.close()
    log.info(f"  ✓ Клиенты дополнены ФИО из БД (fuzzy): {clients_path}")


def main():
    args = parse_args()
    cfg = check_config()

    # Логирование
    log = setup_pipeline_logging(cfg)

    log.info("")
    log.info("╔══════════════════════════════════════════════════════╗")
    log.info("║     ЕДИНЫЙ ПАЙПЛАЙН ОЦИФРОВКИ КЛИЕНТСКИХ КАРТОЧЕК  ║")
    log.info("║     Google Vision + Claude API + БД Привилегия      ║")
    log.info("╚══════════════════════════════════════════════════════╝")
    log.info(f"  Время запуска: {datetime.now().strftime('%d.%m.%Y %H:%M:%S')}")

    # ── Логирование путей ──
    log.info(f"  Input:       {cfg.INPUT_FOLDER}")
    log.info(f"  Output:      {cfg.OUTPUT_FILE}")
    log.info(f"  Data dir:    {getattr(cfg, 'DATA_DIR', 'N/A')}")
    log.info(f"  Cache:       {cfg.CACHE_FOLDER}")
    log.info(f"  DB Privilage:{getattr(cfg, 'DB_PRIVILAGE_PATH', 'N/A')}")
    db_clients = getattr(cfg, 'DB_CLIENTS_FILE', None)
    if db_clients:
        log.info(f"  DB Clients:  {db_clients}")
        if not os.path.exists(db_clients):
            log.warning(f"  БД_Клиенты.xlsx не найден: {db_clients} (пайплайн продолжит без него)")

    if args.force:
        log.info("  Режим: --force (полная переобработка)")
    elif args.skip_ocr:
        log.info("  Режим: --skip-ocr (только сверка)")
    elif args.only_ocr:
        log.info("  Режим: --only-ocr (только OCR)")
    else:
        log.info("  Режим: инкрементальный (только новые карточки)")

    t_start = time.time()
    ocr_excel_path = cfg.OUTPUT_FILE
    normalized_path = None
    verification_df = None
    price_stats = None

    # ── Сброс реестра и кэша при --force ──
    if args.force:
        # Сброс реестра
        reg_path = getattr(cfg, 'PROCESSED_REGISTRY', None)
        if not reg_path:
            reg_path = os.path.join(
                getattr(cfg, 'CACHE_FOLDER', './ocr_cache'),
                "processed_registry.json"
            )
        if os.path.exists(reg_path):
            try:
                os.remove(reg_path)
                log.info(f"  Реестр сброшен: {reg_path}")
            except OSError:
                log.warning(f"  Не удалось удалить реестр: {reg_path}")
        else:
            log.info("  Реестр не найден (и так пусто)")

        # Очистка кэша OCR (все .json файлы без исключений)
        cache_folder = getattr(cfg, 'CACHE_FOLDER', './ocr_cache')
        if os.path.exists(cache_folder):
            import glob
            cache_files = glob.glob(os.path.join(cache_folder, "*.json"))
            removed_count = 0
            for cache_file in cache_files:
                try:
                    os.remove(cache_file)
                    removed_count += 1
                except OSError:
                    pass
            if removed_count > 0:
                log.info(f"  Очищено файлов кэша: {removed_count}")
            else:
                log.info("  Кэш уже пустой")

        # Удаляем Excel для полной пересборки (иначе дозапись сохранит старые данные)
        if os.path.exists(ocr_excel_path):
            try:
                os.remove(ocr_excel_path)
                log.info(f"  Excel сброшен: {ocr_excel_path}")
            except OSError:
                log.warning(f"  Не удалось удалить Excel: {ocr_excel_path}")

        # Удаляем промежуточные отчёты для полной пересборки
        script_dir = os.path.dirname(os.path.abspath(__file__))
        intermediate_files = [
            getattr(cfg, 'NORMALIZED_FILE', 'clients_normalized.xlsx'),
            "verification_report.xlsx",
            "pipeline_report.xlsx",
            getattr(cfg, 'NOT_FOUND_CLIENTS_FILE', 'clients_not_found.xlsx'),
            getattr(cfg, 'FINAL_VERIFICATION_REPORT', 'final_verification_report.xlsx'),
            "raw_results.json",
        ]
        for fname in intermediate_files:
            fpath = os.path.join(script_dir, fname)
            if os.path.exists(fpath):
                try:
                    os.remove(fpath)
                    log.info(f"  Удалён: {fname}")
                except OSError:
                    log.warning(f"  Не удалось удалить: {fname}")

    # ── ШАГ 1-4: OCR ──
    if not args.skip_ocr:
        check_dependencies()
        ocr_result = run_ocr_pipeline(log, cfg)
        if ocr_result:
            ocr_excel_path, price_stats = ocr_result
        else:
            log.error("\n  ✗ OCR пайплайн завершился с ошибкой!")
            if not args.only_ocr:
                log.info("  Пробую продолжить сверку с существующим файлом...")
    else:
        if os.path.exists(ocr_excel_path):
            log.info(f"\n  Используется существующий файл: {ocr_excel_path}")
        else:
            log.warning(f"\n  ⚠ Файл не найден: {ocr_excel_path}")
            log.warning("  Сверка будет только по данным БД.")

    # ── ШАГ 4.5: Нормализация OCR → формат БД ──
    if not args.only_ocr:
        normalized_path = run_normalization(log, cfg, ocr_excel_path)

    # ── ШАГ 5-6: Сверка ──
    # Используем нормализованный файл для сверки (если есть), иначе оригинал
    if not args.only_ocr:
        verify_path = normalized_path if normalized_path else ocr_excel_path
        report_path, verification_df = run_verification(
            log, cfg, verify_path
        )

        # Итоговый комбинированный отчёт
        generate_pipeline_report(log, cfg, verification_df, ocr_excel_path,
                                 price_stats=price_stats)

    # ── Выгрузка в Google Sheets (если включено) ──
    try:
        if _gsheets_disabled(cfg):
            # В smoke-режиме: тихий пропуск (нет лишнего шума в логе)
            if not _is_smoke_mode():
                log.warning("  ⚠ Выгрузка в Google Sheets выключена (GSHEETS_UPLOAD_ENABLED=False)")
        else:
            from importlib import import_module
            try:
                google_sheets = import_module('google_sheets')
            except ImportError as e:
                log.warning(f"  ⚠ Google Sheets недоступен (нет зависимостей): {e}")
                google_sheets = None

            creds_path = getattr(cfg, 'GSHEETS_CREDENTIALS', '')
            spreadsheet_id = getattr(cfg, 'GSHEETS_SPREADSHEET_ID', '')
            if google_sheets and creds_path and spreadsheet_id:
                try:
                    if verification_df is not None:
                        google_sheets.upload_df(verification_df, spreadsheet_id, 'verification', creds_path)
                    if os.path.exists(cfg.OUTPUT_FILE):
                        clients_df = pd.read_excel(cfg.OUTPUT_FILE, sheet_name='Клиенты')
                        google_sheets.upload_df(clients_df, spreadsheet_id, 'clients', creds_path)
                    log.info("  ✓ Выгружено в Google Sheets")
                except Exception as e:
                    log.warning(f"  ⚠ Ошибка выгрузки в Google Sheets: {e}")
            else:
                log.warning("  ⚠ Google Sheets: не заданы GSHEETS_CREDENTIALS или GSHEETS_SPREADSHEET_ID")
    except Exception as e:
        log.warning(f"  ⚠ Ошибка в блоке выгрузки Google Sheets: {e}")

    # ── Добавляем лист «Сверка_БД» в clients_database.xlsx ──
    try:
        if verification_df is not None:
            add_verification_sheet(cfg.OUTPUT_FILE, verification_df, log)
    except Exception as e:
        log.warning(f"  ⚠ Не удалось добавить лист Сверка_БД: {e}")

    # ── Обогащаем clients_database.xlsx данными БД (ФИО из сверки) ──
    try:
        if verification_df is not None:
            enrich_clients_with_db_match(cfg.OUTPUT_FILE, verification_df, log)
    except Exception as e:
        log.warning(f"  ⚠ Не удалось дополнить clients_database.xlsx ФИО из БД: {e}")

    # ── Финальная сводка ──
    total_time = time.time() - t_start
    print_summary(log, verification_df, total_time, ocr_excel_path, cfg)

    log.info("  ✓ ПАЙПЛАЙН ЗАВЕРШЁН УСПЕШНО")
    log.info("")


if __name__ == "__main__":
    main()
