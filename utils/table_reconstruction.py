"""
Реконструкция таблиц из структурных данных Google Vision API.

Google Vision document_text_detection() возвращает иерархию:
  pages[] → blocks[] → paragraphs[] → words[] → symbols[]
Каждый элемент содержит bounding_box и confidence.
Блоки с block_type == TABLE (2) — это таблицы.

Модуль:
- Извлекает структурные блоки из ответа Vision API
- Реконструирует таблицы в Markdown и CSV по bounding box позициям
- Формирует enhanced_text (таблицы + OCR-текст) для подачи в Claude
"""

import csv
import io
import json
import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import List, Tuple, Optional


# block_type значения из google.cloud.vision
BLOCK_TYPE_TEXT = 1
BLOCK_TYPE_TABLE = 2
BLOCK_TYPE_PICTURE = 3
BLOCK_TYPE_RULER = 4
BLOCK_TYPE_BARCODE = 5

BLOCK_TYPE_NAMES = {
    0: "UNKNOWN", 1: "TEXT", 2: "TABLE",
    3: "PICTURE", 4: "RULER", 5: "BARCODE",
}


@dataclass
class VisionWord:
    """Одно слово из Vision API с позицией и confidence."""
    text: str
    x_min: int
    y_min: int
    x_max: int
    y_max: int
    confidence: float

    @property
    def y_center(self) -> int:
        return (self.y_min + self.y_max) // 2

    @property
    def x_center(self) -> int:
        return (self.x_min + self.x_max) // 2

    @property
    def width(self) -> int:
        return max(self.x_max - self.x_min, 1)


@dataclass
class VisionBlock:
    """Один блок из Vision API."""
    block_type: int
    bounding_box: list  # [{x, y}, ...]
    confidence: float
    words: List[VisionWord] = field(default_factory=list)
    text: str = ""

    @property
    def block_type_name(self) -> str:
        return BLOCK_TYPE_NAMES.get(self.block_type, "UNKNOWN")


@dataclass
class OcrStructuredResult:
    """Полный структурированный результат OCR."""
    full_text: str
    blocks: List[VisionBlock] = field(default_factory=list)
    table_blocks: List[VisionBlock] = field(default_factory=list)
    tables_md: str = ""
    tables_csv: str = ""
    enhanced_text: str = ""
    page_confidence: float = 0.0


def _extract_bbox(bounding_box) -> dict:
    """Извлекает координаты из bounding_box Vision API."""
    vertices = getattr(bounding_box, 'vertices', None)
    if not vertices:
        return {"x_min": 0, "y_min": 0, "x_max": 0, "y_max": 0}

    xs = [v.x for v in vertices if hasattr(v, 'x')]
    ys = [v.y for v in vertices if hasattr(v, 'y')]

    if not xs or not ys:
        return {"x_min": 0, "y_min": 0, "x_max": 0, "y_max": 0}

    return {
        "x_min": min(xs),
        "y_min": min(ys),
        "x_max": max(xs),
        "y_max": max(ys),
    }


def _extract_bbox_list(bounding_box) -> list:
    """Извлекает вершины bbox как список {x, y} словарей."""
    vertices = getattr(bounding_box, 'vertices', None)
    if not vertices:
        return []
    return [{"x": v.x, "y": v.y} for v in vertices if hasattr(v, 'x')]


def _word_text_from_symbols(word) -> str:
    """Собирает текст слова из символов, учитывая detected_break."""
    text = ""
    for symbol in getattr(word, 'symbols', []):
        text += getattr(symbol, 'text', '')
    return text


def extract_structured_blocks(response) -> List[VisionBlock]:
    """
    Извлекает структурные блоки из ответа Vision API.

    Args:
        response: ответ от document_text_detection()

    Returns:
        список VisionBlock с типом, bbox, confidence и словами
    """
    annotation = getattr(response, 'full_text_annotation', None)
    if not annotation:
        return []

    blocks_out = []

    for page in getattr(annotation, 'pages', []):
        for block in getattr(page, 'blocks', []):
            block_type = int(getattr(block, 'block_type', 0))
            confidence = float(getattr(block, 'confidence', 0.0))
            bbox_list = _extract_bbox_list(getattr(block, 'bounding_box', None))

            words = []
            text_parts = []

            for paragraph in getattr(block, 'paragraphs', []):
                para_words = []
                for word in getattr(paragraph, 'words', []):
                    word_text = _word_text_from_symbols(word)
                    if not word_text:
                        continue
                    word_bbox = _extract_bbox(getattr(word, 'bounding_box', None))
                    word_conf = float(getattr(word, 'confidence', 0.0))

                    vw = VisionWord(
                        text=word_text,
                        x_min=word_bbox["x_min"],
                        y_min=word_bbox["y_min"],
                        x_max=word_bbox["x_max"],
                        y_max=word_bbox["y_max"],
                        confidence=word_conf,
                    )
                    words.append(vw)
                    para_words.append(word_text)

                if para_words:
                    text_parts.append(" ".join(para_words))

            vb = VisionBlock(
                block_type=block_type,
                bounding_box=bbox_list,
                confidence=confidence,
                words=words,
                text="\n".join(text_parts),
            )
            blocks_out.append(vb)

    return blocks_out


def _group_words_into_rows(words: List[VisionWord], row_tolerance: int = 15) -> List[List[VisionWord]]:
    """
    Группирует слова в строки по Y-координатам.

    Слова с |y_center_1 - y_center_2| <= row_tolerance попадают в одну строку.
    """
    if not words:
        return []

    sorted_words = sorted(words, key=lambda w: (w.y_center, w.x_center))

    rows = []
    current_row = [sorted_words[0]]
    current_y = sorted_words[0].y_center

    for word in sorted_words[1:]:
        if abs(word.y_center - current_y) <= row_tolerance:
            current_row.append(word)
        else:
            # Сортируем строку по X
            current_row.sort(key=lambda w: w.x_center)
            rows.append(current_row)
            current_row = [word]
            current_y = word.y_center

    if current_row:
        current_row.sort(key=lambda w: w.x_center)
        rows.append(current_row)

    return rows


def _detect_columns(rows: List[List[VisionWord]]) -> List[Tuple[int, int]]:
    """
    Определяет границы колонок по X-позициям слов.

    Алгоритм: собирает все x_min, находит кластеры с зазорами > медианной ширины слова.

    Returns:
        список (x_start, x_end) для каждой колонки, отсортированных по x_start
    """
    if not rows:
        return []

    # Собираем все слова
    all_words = [w for row in rows for w in row]
    if not all_words:
        return []

    # Медианная ширина слова для определения зазора
    widths = sorted(w.width for w in all_words)
    median_width = widths[len(widths) // 2] if widths else 20

    # Зазор = 1.5× медианной ширины (эмпирика)
    gap_threshold = max(median_width * 1.5, 30)

    # Собираем интервалы [x_min, x_max] по каждому слову, сортируем по x_min
    intervals = sorted([(w.x_min, w.x_max) for w in all_words], key=lambda i: i[0])

    # Кластеризуем по зазорам
    columns = []
    col_start = intervals[0][0]
    col_end = intervals[0][1]

    for x_min, x_max in intervals[1:]:
        if x_min - col_end > gap_threshold:
            columns.append((col_start, col_end))
            col_start = x_min
            col_end = x_max
        else:
            col_end = max(col_end, x_max)

    columns.append((col_start, col_end))

    return columns


def _assign_word_to_column(word: VisionWord, columns: List[Tuple[int, int]]) -> int:
    """Определяет индекс колонки для слова по x_center."""
    xc = word.x_center
    best_col = 0
    best_dist = float('inf')

    for i, (col_start, col_end) in enumerate(columns):
        col_center = (col_start + col_end) // 2
        dist = abs(xc - col_center)
        if dist < best_dist:
            best_dist = dist
            best_col = i

    return best_col


def reconstruct_table(block: VisionBlock, row_tolerance: int = 15) -> Tuple[str, str]:
    """
    Реконструирует таблицу из TABLE-блока Vision API.

    Args:
        block: VisionBlock с block_type == TABLE
        row_tolerance: допуск Y-группировки слов в строку (px)

    Returns:
        (markdown_str, csv_str)
    """
    if not block.words:
        return "", ""

    rows = _group_words_into_rows(block.words, row_tolerance)
    if not rows:
        return "", ""

    columns = _detect_columns(rows)

    # Если < 2 колонок — это не таблица, возвращаем как текст
    if len(columns) < 2:
        lines = [" ".join(w.text for w in row) for row in rows]
        text = "\n".join(lines)
        return text, text

    num_cols = len(columns)

    # Заполняем матрицу ячеек
    grid = []
    for row in rows:
        cells = [""] * num_cols
        for word in row:
            col_idx = _assign_word_to_column(word, columns)
            if cells[col_idx]:
                cells[col_idx] += " " + word.text
            else:
                cells[col_idx] = word.text
        grid.append(cells)

    # Markdown
    md_lines = []
    for i, cells in enumerate(grid):
        md_lines.append("| " + " | ".join(cells) + " |")
        if i == 0:
            md_lines.append("| " + " | ".join(["---"] * num_cols) + " |")
    md_str = "\n".join(md_lines)

    # CSV
    csv_buffer = io.StringIO()
    writer = csv.writer(csv_buffer)
    for cells in grid:
        writer.writerow(cells)
    csv_str = csv_buffer.getvalue().strip()

    return md_str, csv_str


def reconstruct_all_tables(
    blocks: List[VisionBlock],
    row_tolerance: int = 15,
) -> Tuple[str, str]:
    """
    Реконструирует все TABLE-блоки.

    Returns:
        (all_tables_md, all_tables_csv) — конкатенация через двойной перенос
    """
    md_parts = []
    csv_parts = []

    for block in blocks:
        if block.block_type != BLOCK_TYPE_TABLE:
            continue
        if not block.words:
            continue

        md, csv_text = reconstruct_table(block, row_tolerance)
        if md:
            md_parts.append(md)
        if csv_text:
            csv_parts.append(csv_text)

    all_md = "\n\n".join(md_parts)
    all_csv = "\n\n".join(csv_parts)

    return all_md, all_csv


def build_enhanced_text(full_text: str, tables_md: str) -> str:
    """
    Формирует обогащённый OCR-текст для Claude.

    Если таблицы найдены — prepend перед оригинальным текстом с маркерами.
    """
    if not tables_md:
        return full_text

    return f"[ТАБЛИЦЫ (реконструкция)]\n{tables_md}\n\n[OCR ТЕКСТ]\n{full_text}"


def save_bbox_debug(
    image_path: str,
    blocks: List[VisionBlock],
    page_confidence: float,
    debug_folder: str = "./ocr_debug",
):
    """
    Сохраняет per-image bbox/confidence debug JSON.

    Вынесено в utils, чтобы тестировать без импорта тяжёлого client_card_ocr.
    """
    os.makedirs(debug_folder, exist_ok=True)

    filename = os.path.basename(image_path)
    stem = Path(filename).stem
    debug_path = os.path.join(debug_folder, f"{stem}.json")

    debug_data = {
        "filename": filename,
        "page_confidence": round(page_confidence, 4),
        "blocks_count": len(blocks),
        "blocks": [],
    }

    for block in blocks:
        block_dict = {
            "block_type": block.block_type,
            "block_type_name": block.block_type_name,
            "confidence": round(block.confidence, 4),
            "bounding_box": block.bounding_box,
            "words_count": len(block.words),
            "words": [
                {
                    "text": w.text,
                    "confidence": round(w.confidence, 4),
                    "bbox": {
                        "x_min": w.x_min, "y_min": w.y_min,
                        "x_max": w.x_max, "y_max": w.y_max,
                    },
                }
                for w in block.words
            ],
        }
        debug_data["blocks"].append(block_dict)

    with open(debug_path, "w", encoding="utf-8") as f:
        json.dump(debug_data, f, ensure_ascii=False, indent=2)

    return debug_path
