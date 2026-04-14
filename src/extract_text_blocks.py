#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from __future__ import annotations

import re
from typing import Dict, List

from bs4 import BeautifulSoup

from auto_pipeline_utils import (
    EXTRACTED_DIR,
    WIKIPEDIA_RAW_DIR,
    ensure_dirs,
    load_seeds,
    read_json,
    slugify,
    unique_rows,
    write_csv,
)


TEXT_BLOCK_FIELDS = ["seed_entity_id", "block_id", "block_type", "text", "source"]


def clean_text(text: str) -> str:
    value = re.sub(r"\[\s*\d+\s*\]", "", text)
    value = re.sub(r"\s+", " ", value).strip()
    return value


def extract_lead_paragraphs(html: str, max_paragraphs: int = 4) -> List[str]:
    soup = BeautifulSoup(html, "html.parser")
    content = soup.find("div", class_=lambda value: value and "mw-parser-output" in value)
    if not content:
        return []

    paragraphs: List[str] = []
    for child in content.children:
        tag_name = getattr(child, "name", None)
        if tag_name in {"h2", "h3"}:
            break
        if tag_name != "p":
            continue
        text = clean_text(child.get_text(" ", strip=True))
        if len(text) < 40:
            continue
        paragraphs.append(text)
        if len(paragraphs) >= max_paragraphs:
            break
    return paragraphs


def main() -> None:
    ensure_dirs()
    rows: List[Dict[str, str]] = []

    for seed in load_seeds():
        seed_id = seed["entity_id"]
        slug = slugify(seed_id)
        summary_path = WIKIPEDIA_RAW_DIR / f"{slug}.summary.json"
        html_path = WIKIPEDIA_RAW_DIR / f"{slug}.page.html"
        if not summary_path.exists() or not html_path.exists():
            continue

        summary = read_json(summary_path)
        summary_text = clean_text(summary.get("extract", ""))
        if summary_text:
            rows.append(
                {
                    "seed_entity_id": seed_id,
                    "block_id": f"{seed_id}_summary",
                    "block_type": "summary_extract",
                    "text": summary_text,
                    "source": "wikipedia_summary",
                }
            )

        lead_paragraphs = extract_lead_paragraphs(html_path.read_text(encoding="utf-8"))
        for index, paragraph in enumerate(lead_paragraphs, start=1):
            rows.append(
                {
                    "seed_entity_id": seed_id,
                    "block_id": f"{seed_id}_lead_{index}",
                    "block_type": "lead_paragraph",
                    "text": paragraph,
                    "source": "wikipedia_html",
                }
            )

    write_csv(
        EXTRACTED_DIR / "text_blocks.csv",
        unique_rows(rows, ["seed_entity_id", "block_id"]),
        TEXT_BLOCK_FIELDS,
    )
    print("Text block extraction completed.")


if __name__ == "__main__":
    main()
