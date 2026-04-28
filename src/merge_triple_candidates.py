#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from __future__ import annotations

from typing import Dict, List

from auto_pipeline_utils import EXTRACTED_DIR, ensure_dirs, read_csv, unique_rows, write_csv


TRIPLE_FIELDS = [
    "subject",
    "predicate",
    "object",
    "object_label",
    "object_type",
    "object_datatype",
    "object_class_hint",
    "source",
    "confidence",
]


def main() -> None:
    ensure_dirs()
    structured = read_csv(EXTRACTED_DIR / "triples_candidates_structured.csv")
    text_rows = read_csv(EXTRACTED_DIR / "triples_candidates_text.csv")
    model_rows = read_csv(EXTRACTED_DIR / "triples_candidates_text_model.csv")
    merged: List[Dict[str, str]] = structured + text_rows + model_rows
    write_csv(
        EXTRACTED_DIR / "triples_candidates.csv",
        unique_rows(merged, ["subject", "predicate", "object", "source"]),
        TRIPLE_FIELDS,
    )
    print("Triple candidate merge completed.")


if __name__ == "__main__":
    main()
