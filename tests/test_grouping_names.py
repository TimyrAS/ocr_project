import json
from pathlib import Path

import pandas as pd


def build_result(filename, page_type, data, ocr_text="TEXT"):
    return {
        "filename": filename,
        "page_type": page_type,
        "data": data,
        "ocr_text": ocr_text,
    }


def test_grouping_aliases_and_unknown_texts(monkeypatch):
    import client_card_ocr as cco

    # Отключаем rapidfuzz, чтобы использовался SequenceMatcher
    monkeypatch.setitem(cco._rapidfuzz_cache, "loaded", True)
    monkeypatch.setitem(cco._rapidfuzz_cache, "fuzz", None)
    monkeypatch.setattr(cco.config, "FUZZY_NAME_THRESHOLD", 0.7, raising=False)

    results = [
        # Фронт с fio в русских ключах
        build_result(
            "front.jpg",
            "medical_card_front",
            {"пациент": {"фио": "Карина Капленко", "телефон": "+7 700 000 00 00", "иин": "123"}},
            ocr_text="front text",
        ),
        # Процедурный лист с patient_info.name
        build_result(
            "proc1.jpg",
            "procedure_sheet",
            {"patient_info": {"name": "Карина Капленко"}},
            ocr_text="proc text",
        ),
        # products_list с client_name
        build_result(
            "products.jpg",
            "products_list",
            {"client_name": "Карина Капленко"},
            ocr_text="products text",
        ),
        # complex_package с patient
        build_result(
            "complex.jpg",
            "complex_package",
            {"patient": "Карина Капленко"},
            ocr_text="complex text",
        ),
        # unknown страница – текст должен уйти в full и procedures
        build_result(
            "unknown.jpg",
            "unknown",
            {"some": "data"},
            ocr_text="unknown text",
        ),
    ]

    grouped = cco.group_by_client(results)
    # Должен быть один клиент, без _unmatched
    assert "_unmatched" not in grouped
    assert len([k for k in grouped if k != "_unmatched"]) == 1

    client = grouped[list(k for k in grouped if k != "_unmatched") [0]]
    assert client["name"] == "карина капленко" or client["name"].lower() == "карина капленко"
    assert client["phone"]
    assert client["iin"] == "123"

    # Проверяем сбор текстов
    texts = cco.collect_ocr_texts(client["pages"])
    assert "front text" in texts["front"]
    assert "proc text" in texts["procedures"]
    assert "products text" in texts["products"]
    assert "complex text" in texts["complex"]
    # unknown добавляется как минимум в procedures и full
    assert "unknown text" in texts["procedures"]
    assert "unknown text" in texts["full"]


def test_dedup_not_applied_to_unmatched(monkeypatch):
    import client_card_ocr as cco

    grouped = {
        "_unmatched": {
            "name": "⚠",
            "phone": "",
            "iin": "",
            "pages": [
                {"filename": "a.jpg", "ocr_text": "text a"},
                {"filename": "b.jpg", "ocr_text": "text b"},
            ],
        },
        "client_1": {
            "name": "Test",
            "phone": "",
            "iin": "",
            "pages": [
                {"filename": "c.jpg", "ocr_text": "same"},
                {"filename": "d.jpg", "ocr_text": "same"},
            ],
        },
    }

    monkeypatch.setattr(cco.config, "OCR_DUPLICATE_THRESHOLD", 0.9, raising=False)
    res = cco.deduplicate_pages(grouped)

    # _unmatched не трогаем
    assert len(res["_unmatched"]["pages"]) == 2
    # client_1 должен быть дедуплицирован до 1
    assert len(res["client_1"]["pages"]) == 1
