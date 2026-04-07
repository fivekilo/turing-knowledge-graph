#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from __future__ import annotations

from collections import Counter, defaultdict
from typing import Dict, List

from auto_pipeline_utils import (
    DEFAULT_CLASS_HIERARCHY,
    EXTRACTED_DIR,
    ensure_dirs,
    load_seeds,
    read_csv,
    write_csv,
)


ENTITY_FIELDS = ["entity_id", "label", "class_id", "description", "source"]
RELATION_FIELDS = ["relation_id", "count"]
CLASS_FIELDS = ["class_id", "count"]


def choose_best_label(rows: List[Dict[str, str]]) -> str:
    counts = Counter(row["label"] for row in rows if row.get("label"))
    return max(counts.items(), key=lambda item: (item[1], len(item[0])))[0] if counts else ""


def choose_best_class(rows: List[Dict[str, str]]) -> str:
    counts = Counter(row["class_id_hint"] for row in rows if row.get("class_id_hint"))
    return max(counts.items(), key=lambda item: (item[1], item[0] != "Entity"))[0] if counts else "Entity"


def choose_best_description(rows: List[Dict[str, str]]) -> str:
    descriptions = [row["description"] for row in rows if row.get("description")]
    return max(descriptions, key=len) if descriptions else ""


def aggregate_sources(rows: List[Dict[str, str]]) -> str:
    return "|".join(sorted({row["source"] for row in rows if row.get("source")})) or "auto-extracted"


def main() -> None:
    ensure_dirs()
    entity_rows = read_csv(EXTRACTED_DIR / "entities_candidates.csv")
    triple_rows = read_csv(EXTRACTED_DIR / "triples_candidates.csv")
    seed_map = {item["entity_id"]: item for item in load_seeds()}

    grouped: Dict[str, List[Dict[str, str]]] = defaultdict(list)
    for row in entity_rows:
        grouped[row["entity_id"]].append(row)

    entities_normalized = []
    for entity_id, rows in sorted(grouped.items()):
        entities_normalized.append(
            {
                "entity_id": entity_id,
                "label": seed_map.get(entity_id, {}).get("label", choose_best_label(rows)),
                "class_id": seed_map.get(entity_id, {}).get("class_id", choose_best_class(rows)),
                "description": choose_best_description(rows),
                "source": aggregate_sources(rows),
            }
        )

    write_csv(EXTRACTED_DIR / "entities_normalized.csv", entities_normalized, ENTITY_FIELDS)

    relation_counts = Counter(row["predicate"] for row in triple_rows if row.get("predicate"))
    write_csv(
        EXTRACTED_DIR / "relation_candidates.csv",
        [{"relation_id": key, "count": str(value)} for key, value in relation_counts.most_common()],
        RELATION_FIELDS,
    )

    hierarchy_ids = {item["class_id"] for item in DEFAULT_CLASS_HIERARCHY}
    class_counts = Counter(row["class_id"] for row in entities_normalized if row.get("class_id") in hierarchy_ids)
    write_csv(
        EXTRACTED_DIR / "class_candidates.csv",
        [{"class_id": key, "count": str(value)} for key, value in class_counts.most_common()],
        CLASS_FIELDS,
    )
    print("Normalization completed.")


if __name__ == "__main__":
    main()
