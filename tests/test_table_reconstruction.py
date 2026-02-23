"""
Тесты реконструкции таблиц из структурных данных Vision API.

Проверяют:
1. Извлечение блоков из mock Vision response
2. Группировку слов в строки по Y-координатам
3. Определение колонок по X-позициям
4. Реконструкцию таблиц в Markdown и CSV
5. build_enhanced_text — prepend таблиц перед OCR-текстом
6. Bbox debug output
"""

import sys
import os
import json
import tempfile

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import pytest
from utils.table_reconstruction import (
    VisionWord, VisionBlock, OcrStructuredResult,
    reconstruct_table, reconstruct_all_tables,
    build_enhanced_text, _group_words_into_rows,
    _detect_columns, extract_structured_blocks,
    BLOCK_TYPE_TEXT, BLOCK_TYPE_TABLE,
)


def _w(text, x_min, y_min, x_max, y_max, confidence=0.95):
    """Хелпер для создания VisionWord."""
    return VisionWord(
        text=text, x_min=x_min, y_min=y_min,
        x_max=x_max, y_max=y_max, confidence=confidence,
    )


def _table_block(words):
    """Хелпер для создания TABLE VisionBlock."""
    return VisionBlock(
        block_type=BLOCK_TYPE_TABLE,
        bounding_box=[],
        confidence=0.9,
        words=words,
        text=" ".join(w.text for w in words),
    )


def _text_block(words):
    """Хелпер для создания TEXT VisionBlock."""
    return VisionBlock(
        block_type=BLOCK_TYPE_TEXT,
        bounding_box=[],
        confidence=0.9,
        words=words,
        text=" ".join(w.text for w in words),
    )


# ─── Группировка слов в строки ───

class TestRowGrouping:

    def test_two_rows(self):
        """Слова на разных Y → 2 строки."""
        words = [
            _w("A", 10, 10, 30, 30),   # row 1
            _w("B", 100, 10, 130, 30),  # row 1
            _w("C", 10, 100, 30, 120),  # row 2
            _w("D", 100, 100, 130, 120),  # row 2
        ]
        rows = _group_words_into_rows(words, row_tolerance=15)
        assert len(rows) == 2
        assert [w.text for w in rows[0]] == ["A", "B"]
        assert [w.text for w in rows[1]] == ["C", "D"]

    def test_tolerance_groups_close_words(self):
        """Слова с |y_center| <= tolerance → одна строка."""
        words = [
            _w("X", 10, 50, 30, 70),   # y_center = 60
            _w("Y", 100, 55, 130, 75),  # y_center = 65, diff = 5 < 15
        ]
        rows = _group_words_into_rows(words, row_tolerance=15)
        assert len(rows) == 1

    def test_sorted_by_x_within_row(self):
        """Слова внутри строки отсортированы по X."""
        words = [
            _w("Second", 200, 10, 260, 30),
            _w("First", 10, 10, 60, 30),
        ]
        rows = _group_words_into_rows(words, row_tolerance=15)
        assert rows[0][0].text == "First"
        assert rows[0][1].text == "Second"

    def test_empty_words(self):
        """Пустой список → пустой результат."""
        assert _group_words_into_rows([], row_tolerance=15) == []


# ─── Определение колонок ───

class TestColumnDetection:

    def test_two_columns(self):
        """Слова в двух кластерах по X → 2 колонки."""
        rows = [
            [_w("A", 10, 10, 50, 30), _w("B", 200, 10, 250, 30)],
            [_w("C", 15, 50, 55, 70), _w("D", 195, 50, 245, 70)],
        ]
        cols = _detect_columns(rows)
        assert len(cols) == 2
        # Первая колонка ~10-55, вторая ~195-250
        assert cols[0][0] < cols[1][0]

    def test_empty_rows(self):
        """Пустой список → пустой результат."""
        assert _detect_columns([]) == []


# ─── Реконструкция таблиц ───

class TestTableReconstruction:

    def test_2x2_table_markdown(self):
        """2×2 таблица → Markdown с | и ---."""
        words = [
            _w("Дата", 10, 10, 50, 30),
            _w("Процедура", 300, 10, 400, 30),
            _w("01.01.24", 10, 100, 80, 120),
            _w("Чистка", 300, 100, 380, 120),
        ]
        block = _table_block(words)
        md, csv_text = reconstruct_table(block, row_tolerance=15)

        assert "|" in md
        assert "---" in md
        assert "Дата" in md
        assert "Процедура" in md
        assert "01.01.24" in md
        assert "Чистка" in md

    def test_2x2_table_csv(self):
        """2×2 таблица → CSV."""
        words = [
            _w("Col1", 10, 10, 60, 30),
            _w("Col2", 200, 10, 300, 30),
            _w("A", 10, 100, 30, 120),
            _w("B", 200, 100, 220, 120),
        ]
        block = _table_block(words)
        _, csv_text = reconstruct_table(block, row_tolerance=15)

        assert "Col1" in csv_text
        assert "Col2" in csv_text
        lines = csv_text.strip().split("\n")
        assert len(lines) == 2  # header + 1 data row

    def test_3x3_table(self):
        """3 строки, 3 колонки → 3 строки в Markdown."""
        words = [
            # Row 1 (header)
            _w("A", 10, 10, 30, 30),
            _w("B", 200, 10, 220, 30),
            _w("C", 400, 10, 420, 30),
            # Row 2
            _w("1", 10, 100, 30, 120),
            _w("2", 200, 100, 220, 120),
            _w("3", 400, 100, 420, 120),
            # Row 3
            _w("4", 10, 200, 30, 220),
            _w("5", 200, 200, 220, 220),
            _w("6", 400, 200, 420, 220),
        ]
        block = _table_block(words)
        md, _ = reconstruct_table(block, row_tolerance=15)

        lines = md.strip().split("\n")
        # header + separator + 2 data rows = 4 lines
        assert len(lines) == 4
        assert "---" in lines[1]

    def test_empty_block(self):
        """Блок без слов → пустые строки."""
        block = _table_block([])
        md, csv_text = reconstruct_table(block)
        assert md == ""
        assert csv_text == ""

    def test_single_column_fallback(self):
        """Все слова в одной колонке → space-delimited текст (не таблица)."""
        words = [
            _w("Hello", 10, 10, 60, 30),
            _w("World", 10, 100, 60, 120),
        ]
        block = _table_block(words)
        md, _ = reconstruct_table(block, row_tolerance=15)

        # Не должно быть --- (нет колонок)
        assert "---" not in md
        assert "Hello" in md
        assert "World" in md


# ─── reconstruct_all_tables ───

class TestReconstructAllTables:

    def test_filters_only_table_blocks(self):
        """Из смеси TEXT + TABLE реконструируются только TABLE."""
        text_words = [_w("Текст", 10, 10, 60, 30)]
        table_words = [
            _w("A", 10, 10, 30, 30), _w("B", 200, 10, 220, 30),
            _w("1", 10, 100, 30, 120), _w("2", 200, 100, 220, 120),
        ]
        blocks = [
            _text_block(text_words),
            _table_block(table_words),
        ]
        md, csv_text = reconstruct_all_tables(blocks)
        assert "|" in md
        assert "Текст" not in md  # TEXT блок не должен быть в таблицах

    def test_no_table_blocks(self):
        """Нет TABLE блоков → пустой результат."""
        blocks = [_text_block([_w("Hello", 10, 10, 60, 30)])]
        md, csv_text = reconstruct_all_tables(blocks)
        assert md == ""
        assert csv_text == ""

    def test_empty_blocks_list(self):
        """Пустой список блоков → пустой результат."""
        md, csv_text = reconstruct_all_tables([])
        assert md == ""
        assert csv_text == ""


# ─── build_enhanced_text ───

class TestBuildEnhancedText:

    def test_no_tables_returns_original(self):
        """Без таблиц → оригинальный текст."""
        assert build_enhanced_text("OCR text", "") == "OCR text"

    def test_tables_prepended(self):
        """С таблицами → маркеры + таблица + оригинал."""
        result = build_enhanced_text("OCR text", "| A | B |\n| --- | --- |")
        assert "[ТАБЛИЦЫ (реконструкция)]" in result
        assert "[OCR ТЕКСТ]" in result
        assert "| A | B |" in result
        assert result.endswith("OCR text")

    def test_table_before_text(self):
        """Таблица идёт ПЕРЕД основным текстом."""
        result = build_enhanced_text("ORIGINAL", "TABLE_DATA")
        table_pos = result.index("TABLE_DATA")
        text_pos = result.index("ORIGINAL")
        assert table_pos < text_pos


# ─── extract_structured_blocks с mock ───

class TestExtractStructuredBlocks:

    def test_empty_response(self):
        """Пустой response → пустой список."""
        class MockResponse:
            full_text_annotation = None
        assert extract_structured_blocks(MockResponse()) == []

    def test_response_without_pages(self):
        """Annotation без pages → пустой список."""
        class MockAnnotation:
            pages = []
        class MockResponse:
            full_text_annotation = MockAnnotation()
        assert extract_structured_blocks(MockResponse()) == []

    def test_block_type_extraction(self):
        """Block type извлекается корректно."""
        class MockVertex:
            def __init__(self, x, y):
                self.x = x
                self.y = y
        class MockBBox:
            def __init__(self):
                self.vertices = [MockVertex(0, 0), MockVertex(100, 0),
                                 MockVertex(100, 50), MockVertex(0, 50)]
        class MockSymbol:
            def __init__(self, text):
                self.text = text
        class MockWord:
            def __init__(self, text):
                self.symbols = [MockSymbol(c) for c in text]
                self.bounding_box = MockBBox()
                self.confidence = 0.95
        class MockParagraph:
            def __init__(self, words):
                self.words = [MockWord(w) for w in words]
        class MockBlock:
            def __init__(self, block_type, paragraphs):
                self.block_type = block_type
                self.confidence = 0.9
                self.bounding_box = MockBBox()
                self.paragraphs = paragraphs
        class MockPage:
            def __init__(self, blocks):
                self.blocks = blocks
        class MockAnnotation:
            def __init__(self, pages):
                self.pages = pages
        class MockResponse:
            def __init__(self, annotation):
                self.full_text_annotation = annotation

        blocks_input = [
            MockBlock(1, [MockParagraph(["Hello"])]),  # TEXT
            MockBlock(2, [MockParagraph(["Data"])]),    # TABLE
        ]
        page = MockPage(blocks_input)
        annotation = MockAnnotation([page])
        response = MockResponse(annotation)

        blocks = extract_structured_blocks(response)
        assert len(blocks) == 2
        assert blocks[0].block_type == 1  # TEXT
        assert blocks[1].block_type == 2  # TABLE
        assert blocks[0].words[0].text == "Hello"
        assert blocks[1].words[0].text == "Data"


# ─── OcrStructuredResult ───

class TestOcrStructuredResult:

    def test_default_values(self):
        """Дефолтные значения OcrStructuredResult."""
        result = OcrStructuredResult(full_text="test")
        assert result.full_text == "test"
        assert result.blocks == []
        assert result.table_blocks == []
        assert result.tables_md == ""
        assert result.tables_csv == ""
        assert result.enhanced_text == ""
        assert result.page_confidence == 0.0


# ─── Bbox debug (unit) ───

class TestBboxDebug:

    def test_save_bbox_debug_creates_file(self):
        """save_bbox_debug создаёт JSON-файл с правильной структурой."""
        from utils.table_reconstruction import save_bbox_debug

        with tempfile.TemporaryDirectory() as tmpdir:
            blocks = [
                VisionBlock(
                    block_type=1, bounding_box=[], confidence=0.92,
                    words=[_w("Test", 10, 10, 50, 30)],
                    text="Test",
                )
            ]
            result_path = save_bbox_debug(
                "/fake/path/image001.jpg", blocks, 0.92, debug_folder=tmpdir,
            )

            assert os.path.exists(result_path)

            with open(result_path, 'r', encoding='utf-8') as f:
                data = json.load(f)

            assert data["filename"] == "image001.jpg"
            assert data["page_confidence"] == 0.92
            assert len(data["blocks"]) == 1
            assert data["blocks"][0]["block_type"] == 1
            assert data["blocks"][0]["words"][0]["text"] == "Test"
            assert "bbox" in data["blocks"][0]["words"][0]


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
