#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from __future__ import annotations

from typing import Dict, List

from auto_pipeline_utils import EXTRACTED_DIR, ensure_dirs, read_csv, unique_rows, write_csv


ENTITY_FIELDS = ["entity_id", "label", "class_id_hint", "description", "source", "source_key"]


def main() -> None:
    ensure_dirs()
    structured = read_csv(EXTRACTED_DIR / "entities_candidates_structured.csv")
    linked_path = EXTRACTED_DIR / "entities_candidates_ner_linked.csv"
    ner_rows = read_csv(linked_path) if linked_path.exists() else read_csv(EXTRACTED_DIR / "entities_candidates_ner.csv")
    merged: List[Dict[str, str]] = structured + ner_rows
    write_csv(
        EXTRACTED_DIR / "entities_candidates.csv",
        unique_rows(merged, ["entity_id", "label", "class_id_hint", "source"]),
        ENTITY_FIELDS,
    )
    print("Entity candidate merge completed.")


if __name__ == "__main__":
    main()
