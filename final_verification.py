"""
Финальная верификация данных клиентов через Claude API.

Анализирует результаты сверки OCR ↔ БД и:
- Проверяет корректность автоматического матчинга
- Находит возможные ошибки OCR, используя контекст БД
- Выявляет расхождения между OCR и БД
- Для ненайденных клиентов ищет возможные совпадения
- Генерирует рекомендации по каждому клиенту

Запуск:
    Вызывается автоматически из run_pipeline.py после verify_clients()

Результат:
    - enhanced_verification_df — обогащённый DataFrame с новыми полями
    - final_verification_report.xlsx — отчёт с рекомендациями
"""

import logging
import pandas as pd
import json
from typing import Dict, List, Tuple, Optional
from datetime import datetime
from pathlib import Path

import anthropic

log = logging.getLogger('pipeline')

# ============================================================
# СИСТЕМНЫЙ ПРОМПТ ДЛЯ CLAUDE
# ============================================================

CLAUDE_VERIFICATION_PROMPT = """Ты — система финальной верификации данных клиентов косметологической клиники.

Тебе даны:
1. OCR-данные из бумажных карточек (ФИО, телефон)
2. Данные из БД «Привилегия» (история визитов, врачи, услуги)
3. Результат автоматической сверки (fuzzy matching, score)

ТВОЯ ЗАДАЧА:
1. Проверить корректность сверки — действительно ли это один и тот же клиент?
2. Найти возможные ошибки OCR, используя контекст БД
3. Выявить расхождения между OCR и БД (разные телефоны, даты)
4. Для НЕНАЙДЕННЫХ клиентов — поискать возможные совпадения в БД по:
   - Похожим фамилиям (опечатки, транслитерация)
   - Частичному совпадению телефона (последние 7 цифр)
   - Истории визитов (те же врачи, процедуры)
5. Дать рекомендации по каждому клиенту

АНАЛИЗИРУЙ:
- Если ФИО в OCR: "Иванова Анна", а в БД: "Анна Иванова" → это ОДНО ЛИЦО
- Если OCR распознал "Асшеман" как "Ассиман" → исправь по списку врачей
- Если телефоны разные, но ФИО совпадают → РАСХОЖДЕНИЕ (возможно, сменила номер)

КРИТИЧЕСКИ ВАЖНО:
- Твой ответ ДОЛЖЕН быть ТОЛЬКО валидным JSON и НИЧЕМ ДРУГИМ
- НЕ добавляй никаких объяснений, комментариев или markdown форматирования
- НЕ используй ``` или другие обёртки
- Начинай ответ сразу с {

ФОРМАТ ОТВЕТА (строго JSON):
{
  "clients": [
    {
      "client_id": "ID клиента из OCR",
      "final_status": "Подтверждён" | "Требует проверки" | "Возможный дубль" | "Не найден",
      "confidence_score": 0-100,
      "possible_matches": [
        {
          "db_name": "ФИО из БД",
          "db_phone": "телефон",
          "match_reason": "почему это возможное совпадение",
          "score": 0-100
        }
      ],
      "discrepancies": [
        {
          "field": "phone" | "fio" | "date" | "doctor",
          "ocr_value": "значение из OCR",
          "db_value": "значение из БД",
          "explanation": "объяснение расхождения"
        }
      ],
      "ocr_corrections": {
        "fio": "исправленное ФИО (если была ошибка OCR)",
        "doctor": "исправленное имя врача",
        "phone": "исправленный телефон"
      },
      "recommendations": [
        "Обновить телефон в БД",
        "Проверить дату рождения вручную",
        "Возможный дубль — объединить записи"
      ]
    }
  ]
}"""


# ============================================================
# ОСНОВНАЯ ФУНКЦИЯ
# ============================================================

def run_final_claude_verification(
    log,
    config,
    claude_client: anthropic.Anthropic,
    verification_df: pd.DataFrame,
    ocr_sheets: dict,
    db_index: dict
) -> Tuple[pd.DataFrame, str]:
    """
    Финальная верификация данных через Claude API.

    Args:
        log: Logger для вывода
        config: Конфигурация проекта
        claude_client: Инициализированный клиент Claude
        verification_df: Результаты сверки OCR ↔ БД
        ocr_sheets: OCR данные из load_ocr()
        db_index: Индекс БД клиентов из build_db_client_index()

    Returns:
        (enhanced_verification_df, report_path)
    """
    log.info(f"  Финальная верификация для {len(verification_df)} клиентов...")

    # Проверяем, включена ли финальная верификация
    if not getattr(config, 'ENABLE_FINAL_VERIFICATION', True):
        log.info("  Финальная верификация отключена в config.")
        return verification_df, None

    # Получаем параметры из config
    batch_size = getattr(config, 'VERIFICATION_BATCH_SIZE', 10)
    confidence_threshold = getattr(config, 'CLAUDE_CONFIDENCE_THRESHOLD', 90)
    max_possible_matches = getattr(config, 'MAX_POSSIBLE_MATCHES', 3)

    # Батчинг верификации
    results = batch_verify_clients(
        log=log,
        config=config,
        claude_client=claude_client,
        verification_df=verification_df,
        ocr_sheets=ocr_sheets,
        db_index=db_index,
        batch_size=batch_size,
        max_possible_matches=max_possible_matches
    )

    # Обогащаем verification_df новыми полями
    enhanced_df = enhance_verification_df(verification_df, results)

    # Генерируем отчёт
    report_path = getattr(config, 'FINAL_VERIFICATION_REPORT', 'final_verification_report.xlsx')
    report_path = str((Path(getattr(config, "BASE_DIR", Path.cwd())) / report_path) if not Path(report_path).is_absolute() else Path(report_path))


    generate_final_verification_report(enhanced_df, report_path, log)

    # Статистика
    confirmed = len(enhanced_df[enhanced_df['Claude_Статус'] == 'Подтверждён'])
    needs_review = len(enhanced_df[enhanced_df['Claude_Статус'] == 'Требует проверки'])
    possible_dupes = len(enhanced_df[enhanced_df['Claude_Статус'] == 'Возможный дубль'])
    not_found = len(enhanced_df[enhanced_df['Claude_Статус'] == 'Не найден'])

    log.info(f"  Подтверждено: {confirmed}, Требует проверки: {needs_review}, "
             f"Возможные дубли: {possible_dupes}, Не найдено: {not_found}")

    return enhanced_df, report_path


# ============================================================
# ПОДГОТОВКА КОНТЕКСТА
# ============================================================

def prepare_client_context(
    row: pd.Series,
    row_index: int,
    ocr_sheets: dict,
    db_index: dict
) -> dict:
    """
    Подготовка контекста клиента для отправки в Claude.

    Args:
        row: Строка из verification_df
        row_index: Индекс строки (используется как ID)
        ocr_sheets: Данные OCR
        db_index: Индекс БД

    Returns:
        Словарь с контекстом клиента
    """
    # Безопасное получение значений
    def safe_str(val):
        if pd.isna(val):
            return ""
        return str(val)

    def safe_int(val):
        if pd.isna(val):
            return 0
        try:
            return int(val)
        except:
            return 0

    def safe_float(val):
        if pd.isna(val):
            return 0.0
        try:
            return float(val)
        except:
            return 0.0

    context = {
        'client_id': str(row_index),
        'ocr_data': {
            'fio': safe_str(row.get('OCR_ФИО', '')),
            'phone': safe_str(row.get('OCR_Телефон', '')),
        },
        'db_data': {
            'fio': safe_str(row.get('БД_ФИО', '')),
            'phone': safe_str(row.get('БД_Телефон', '')),
            'visits_count': safe_int(row.get('Визитов_в_БД', 0)),
            'doctors': safe_str(row.get('Врачи_в_БД', '')),
        },
        'match_info': {
            'status': safe_str(row.get('Статус', '')),
            'score': safe_float(row.get('Совпадение_%', 0))
        }
    }

    return context


def prepare_batch_data(
    batch_df: pd.DataFrame,
    ocr_sheets: dict,
    db_index: dict,
    max_possible_matches: int = 3
) -> str:
    """
    Подготовка данных батча для отправки в Claude.

    Args:
        batch_df: DataFrame с батчем клиентов
        ocr_sheets: Данные OCR
        db_index: Индекс БД
        max_possible_matches: Максимум альтернативных совпадений

    Returns:
        Строка JSON с данными батча
    """
    batch_contexts = []

    for idx, row in batch_df.iterrows():
        context = prepare_client_context(row, idx, ocr_sheets, db_index)
        batch_contexts.append(context)

    # Добавляем список всех клиентов из БД (для поиска альтернативных совпадений)
    db_clients_list = []
    for db_name, client_data in db_index.items():
        db_clients_list.append({
            'fio': client_data.get('name_orig', ''),
            'phone': client_data.get('phone', ''),
            'visits': client_data.get('total_visits', 0),
            'doctors': ', '.join(client_data.get('doctors', [])[:3])
        })

    # Ограничиваем список БД (чтобы не превысить лимит токенов)
    db_clients_list = db_clients_list[:200]  # Топ-200 клиентов

    batch_data = {
        'clients': batch_contexts,
        'db_clients_sample': db_clients_list,
        'max_possible_matches': max_possible_matches
    }

    return json.dumps(batch_data, ensure_ascii=False, indent=2)


# ============================================================
# БАТЧИНГ ВЕРИФИКАЦИИ
# ============================================================

def batch_verify_clients(
    log,
    config,
    claude_client: anthropic.Anthropic,
    verification_df: pd.DataFrame,
    ocr_sheets: dict,
    db_index: dict,
    batch_size: int = 10,
    max_possible_matches: int = 3
) -> List[dict]:
    """
    Батчинг верификации клиентов через Claude API.

    Args:
        log: Logger
        config: Конфигурация
        claude_client: Клиент Claude
        verification_df: DataFrame с результатами сверки
        ocr_sheets: Данные OCR
        db_index: Индекс БД
        batch_size: Размер батча
        max_possible_matches: Максимум альтернативных совпадений

    Returns:
        Список результатов верификации
    """
    all_results = []
    total_batches = (len(verification_df) + batch_size - 1) // batch_size

    log.info(f"  Обработка {len(verification_df)} клиентов батчами по {batch_size}...")

    for i in range(0, len(verification_df), batch_size):
        batch_df = verification_df.iloc[i:i+batch_size]
        batch_num = (i // batch_size) + 1

        log.info(f"  Батч {batch_num}/{total_batches} ({len(batch_df)} клиентов)...")

        # Подготовка данных батча
        batch_data_str = prepare_batch_data(batch_df, ocr_sheets, db_index, max_possible_matches)

        # Отправка в Claude
        try:
            user_message = f"""Проверь этих {len(batch_df)} клиентов:

{batch_data_str}

ВАЖНО: Ответь ТОЛЬКО валидным JSON в формате из системного промпта. Не добавляй никаких пояснений, markdown, или других символов. Начни ответ сразу с {{"""

            response = claude_client.messages.create(
                model=config.CLAUDE_MODEL,
                max_tokens=8192,
                system=CLAUDE_VERIFICATION_PROMPT,
                messages=[{
                    "role": "user",
                    "content": user_message
                }]
            )

            # Парсинг ответа
            batch_results = parse_claude_batch_response(response, log)
            all_results.extend(batch_results)

        except Exception as e:
            log.error(f"  Ошибка при обработке батча {batch_num}: {e}")
            # Добавляем пустые результаты для клиентов из батча
            for idx, row in batch_df.iterrows():
                all_results.append({
                    'client_id': str(idx),
                    'final_status': 'Ошибка верификации',
                    'confidence_score': 0,
                    'possible_matches': [],
                    'discrepancies': [],
                    'ocr_corrections': {},
                    'recommendations': ['Верификация не выполнена из-за ошибки API']
                })

    return all_results


def parse_claude_batch_response(
    response: anthropic.types.Message,
    log
) -> List[dict]:
    """
    Парсинг ответа Claude для батча клиентов.

    Args:
        response: Ответ от Claude API
        log: Logger

    Returns:
        Список результатов верификации
    """
    try:
        # Извлекаем текст ответа
        response_text = response.content[0].text.strip()

        # Убираем markdown обёртки если есть
        if response_text.startswith('```'):
            # Ищем JSON между ``` блоками
            lines = response_text.split('\n')
            json_lines = []
            in_json = False
            for line in lines:
                if line.strip().startswith('```'):
                    if not in_json:
                        in_json = True
                        continue
                    else:
                        break
                if in_json:
                    json_lines.append(line)
            response_text = '\n'.join(json_lines)

        # Пытаемся найти JSON объект в тексте
        # Ищем первый { и последний }
        start_idx = response_text.find('{')
        end_idx = response_text.rfind('}')

        if start_idx == -1 or end_idx == -1:
            log.error(f"  Не найден JSON объект в ответе Claude")
            log.error(f"  Ответ: {response_text[:500]}")
            return []

        json_text = response_text[start_idx:end_idx+1]

        # Пытаемся распарсить JSON
        parsed = json.loads(json_text)

        # Поддерживаем разные варианты структуры ответа
        if 'clients' in parsed:
            results = parsed['clients']
        elif 'results' in parsed:
            results = parsed['results']
        elif 'check_results' in parsed:
            results = parsed['check_results']
        else:
            log.warning(f"  Ответ Claude не содержит поле 'clients', 'results' или 'check_results'")
            log.warning(f"  Ключи в ответе: {list(parsed.keys())}")
            return []

        # Нормализуем результаты к ожидаемому формату
        normalized_results = []
        for result in results:
            normalized = {
                'client_id': result.get('client_id', ''),
                'final_status': result.get('final_status') or result.get('status', 'Не найден'),
                'confidence_score': result.get('confidence_score') or result.get('confidence', 0),
                'possible_matches': result.get('possible_matches', []),
                'discrepancies': result.get('discrepancies', []),
                'ocr_corrections': result.get('ocr_corrections', {}),
                'recommendations': result.get('recommendations') or result.get('recommended_actions', [])
            }
            normalized_results.append(normalized)

        return normalized_results

    except json.JSONDecodeError as e:
        log.error(f"  Ошибка парсинга JSON от Claude: {e}")
        log.error(f"  Попытка распарсить: {json_text[:500] if 'json_text' in locals() else response_text[:500]}")
        return []
    except Exception as e:
        log.error(f"  Ошибка обработки ответа Claude: {e}")
        return []


# ============================================================
# ОБОГАЩЕНИЕ VERIFICATION_DF
# ============================================================

def enhance_verification_df(
    verification_df: pd.DataFrame,
    claude_results: List[dict]
) -> pd.DataFrame:
    """
    Добавляет результаты Claude в verification_df.

    Args:
        verification_df: Исходный DataFrame (с оригинальными индексами)
        claude_results: Результаты от Claude (client_id = оригинальный индекс)

    Returns:
        Обогащённый DataFrame (с сохранёнными оригинальными индексами)
    """
    # Создаём копию БЕЗ reset_index - сохраняем оригинальные индексы!
    enhanced_df = verification_df.copy()

    # Создаём маппинг client_id (оригинальный индекс) -> результат
    results_map = {}
    for result in claude_results:
        client_id_str = result.get('client_id', '')
        if client_id_str and client_id_str.isdigit():
            results_map[int(client_id_str)] = result

    # Добавляем новые колонки
    enhanced_df['Claude_Статус'] = ''
    enhanced_df['Claude_Совпадение_%'] = 0.0
    enhanced_df['Возможные_совпадения_БД'] = ''
    enhanced_df['Расхождения'] = ''
    enhanced_df['Рекомендации'] = ''
    enhanced_df['Исправления_OCR'] = ''

    # Заполняем данные по ОРИГИНАЛЬНЫМ индексам
    for idx in enhanced_df.index:
        if idx in results_map:
            result = results_map[idx]

            # Статус
            enhanced_df.at[idx, 'Claude_Статус'] = result.get('final_status', '')

            # Оценка совпадения
            enhanced_df.at[idx, 'Claude_Совпадение_%'] = result.get('confidence_score', 0)

            # Возможные совпадения
            possible_matches = result.get('possible_matches', [])
            if possible_matches and isinstance(possible_matches, list):
                matches_text = '; '.join([
                    f"{m.get('db_name', '')} ({m.get('score', 0)}%) - {m.get('match_reason', '')}"
                    for m in possible_matches
                    if isinstance(m, dict)
                ])
                enhanced_df.at[idx, 'Возможные_совпадения_БД'] = matches_text

            # Расхождения
            discrepancies = result.get('discrepancies', [])
            if discrepancies and isinstance(discrepancies, list):
                disc_text = '; '.join([
                    f"{d.get('field', '')}: OCR={d.get('ocr_value', '')} vs БД={d.get('db_value', '')} ({d.get('explanation', '')})"
                    for d in discrepancies
                    if isinstance(d, dict)
                ])
                enhanced_df.at[idx, 'Расхождения'] = disc_text

            # Рекомендации
            recommendations = result.get('recommendations', [])
            if recommendations and isinstance(recommendations, list):
                enhanced_df.at[idx, 'Рекомендации'] = '; '.join([
                    str(r) for r in recommendations
                ])

            # Исправления OCR
            corrections = result.get('ocr_corrections', {})
            if corrections and isinstance(corrections, dict):
                corr_text = '; '.join([
                    f"{field}: {value}"
                    for field, value in corrections.items()
                    if value
                ])
                enhanced_df.at[idx, 'Исправления_OCR'] = corr_text

    return enhanced_df


# ============================================================
# ГЕНЕРАЦИЯ ОТЧЁТА
# ============================================================

def generate_final_verification_report(
    enhanced_df: pd.DataFrame,
    output_path: str,
    log
):
    """
    Генерация отчёта финальной верификации.

    Args:
        enhanced_df: Обогащённый DataFrame
        output_path: Путь к файлу отчёта
        log: Logger
    """
    try:
        with pd.ExcelWriter(output_path, engine='openpyxl') as writer:

            # ====== ЛИСТ 1: СВОДКА ======
            summary_data = {
                'Метрика': [
                    'Всего клиентов',
                    'Подтверждены Claude',
                    'Требуют проверки',
                    'Возможные дубли',
                    'Не найдены',
                    'Найдено возможных совпадений',
                    'Исправлено ошибок OCR',
                    'Выявлено расхождений'
                ],
                'Значение': [
                    len(enhanced_df),
                    len(enhanced_df[enhanced_df['Claude_Статус'] == 'Подтверждён']),
                    len(enhanced_df[enhanced_df['Claude_Статус'] == 'Требует проверки']),
                    len(enhanced_df[enhanced_df['Claude_Статус'] == 'Возможный дубль']),
                    len(enhanced_df[enhanced_df['Claude_Статус'] == 'Не найден']),
                    len(enhanced_df[enhanced_df['Возможные_совпадения_БД'].str.len() > 0]),
                    len(enhanced_df[enhanced_df['Исправления_OCR'].str.len() > 0]),
                    len(enhanced_df[enhanced_df['Расхождения'].str.len() > 0])
                ]
            }
            summary_df = pd.DataFrame(summary_data)
            summary_df.to_excel(writer, sheet_name='Сводка', index=False)

            # ====== ЛИСТ 2: ТРЕБУЮТ ПРОВЕРКИ ======
            needs_review = enhanced_df[enhanced_df['Claude_Статус'] == 'Требует проверки'].copy()
            if not needs_review.empty:
                # Добавляем ID как индекс
                needs_review.insert(0, 'ID', needs_review.index)
                # Выбираем ключевые колонки
                review_cols = [
                    'ID', 'OCR_ФИО', 'OCR_Телефон',
                    'БД_ID', 'БД_ФИО', 'БД_Телефон', 'Статус_БД',
                    'Claude_Статус', 'Claude_Совпадение_%',
                    'Расхождения', 'Рекомендации'
                ]
                review_cols = [col for col in review_cols if col in needs_review.columns]
                needs_review[review_cols].to_excel(writer, sheet_name='Требуют_проверки', index=False)
            else:
                pd.DataFrame({'Сообщение': ['Нет клиентов, требующих проверки']}).to_excel(
                    writer, sheet_name='Требуют_проверки', index=False
                )

            # ====== ЛИСТ 3: ВОЗМОЖНЫЕ ДУБЛИ ======
            possible_dupes = enhanced_df[enhanced_df['Claude_Статус'] == 'Возможный дубль'].copy()
            if not possible_dupes.empty:
                # Добавляем ID как индекс
                possible_dupes.insert(0, 'ID', possible_dupes.index)
                dupe_cols = [
                    'ID', 'OCR_ФИО', 'OCR_Телефон',
                    'БД_ID', 'БД_ФИО', 'БД_Телефон',
                    'Claude_Совпадение_%', 'Рекомендации'
                ]
                dupe_cols = [col for col in dupe_cols if col in possible_dupes.columns]
                possible_dupes[dupe_cols].to_excel(writer, sheet_name='Возможные_дубли', index=False)
            else:
                pd.DataFrame({'Сообщение': ['Дубли не найдены']}).to_excel(
                    writer, sheet_name='Возможные_дубли', index=False
                )

            # ====== ЛИСТ 4: НЕ НАЙДЕНЫ (РАСШИРЕННЫЙ) ======
            status_col = 'Статус_БД' if 'Статус_БД' in enhanced_df.columns else 'Статус'
            try:
                from config import STATUS_DB_NOT_FOUND
            except ImportError:
                STATUS_DB_NOT_FOUND = "Нет в БД (новый для картотеки)"
            if status_col == 'Статус_БД':
                not_found = enhanced_df[enhanced_df[status_col] == STATUS_DB_NOT_FOUND].copy()
            else:
                not_found = enhanced_df[enhanced_df[status_col] == 'Не найден'].copy()
            if not not_found.empty:
                # Добавляем ID как индекс
                not_found.insert(0, 'ID', not_found.index)
                not_found_cols = [
                    'ID', 'OCR_ФИО', 'OCR_Телефон',
                    'БД_ID', 'Claude_Статус', 'Возможные_совпадения_БД', 'Рекомендации'
                ]
                not_found_cols = [col for col in not_found_cols if col in not_found.columns]
                not_found[not_found_cols].to_excel(writer, sheet_name='Не_найдены_расширенный', index=False)
            else:
                pd.DataFrame({'Сообщение': ['Все клиенты найдены']}).to_excel(
                    writer, sheet_name='Не_найдены_расширенный', index=False
                )

            # ====== ЛИСТ 5: ИСПРАВЛЕНИЯ OCR ======
            with_corrections = enhanced_df[enhanced_df['Исправления_OCR'].str.len() > 0].copy()
            if not with_corrections.empty:
                # Добавляем ID как индекс
                with_corrections.insert(0, 'ID', with_corrections.index)
                corr_cols = [
                    'ID', 'OCR_ФИО', 'OCR_Телефон', 'БД_ID',
                    'Исправления_OCR', 'Рекомендации'
                ]
                corr_cols = [col for col in corr_cols if col in with_corrections.columns]
                with_corrections[corr_cols].to_excel(writer, sheet_name='Исправления_OCR', index=False)
            else:
                pd.DataFrame({'Сообщение': ['Исправлений не требуется']}).to_excel(
                    writer, sheet_name='Исправления_OCR', index=False
                )

            # ====== ЛИСТ 6: ВСЕ РЕКОМЕНДАЦИИ ======
            with_recommendations = enhanced_df[enhanced_df['Рекомендации'].str.len() > 0].copy()
            if not with_recommendations.empty:
                # Добавляем ID как индекс
                with_recommendations.insert(0, 'ID', with_recommendations.index)
                rec_cols = [
                    'ID', 'OCR_ФИО', 'БД_ID', 'Статус_БД',
                    'Claude_Статус', 'Рекомендации'
                ]
                rec_cols = [col for col in rec_cols if col in with_recommendations.columns]
                with_recommendations[rec_cols].to_excel(writer, sheet_name='Рекомендации', index=False)
            else:
                pd.DataFrame({'Сообщение': ['Рекомендаций нет']}).to_excel(
                    writer, sheet_name='Рекомендации', index=False
                )

        log.info(f"  ✓ Отчёт сохранён: {output_path}")

    except Exception as e:
        log.error(f"  Ошибка генерации отчёта: {e}")
        raise
