#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from __future__ import annotations

from typing import Dict, List, Set, Tuple

from auto_pipeline_utils import (
    DEFAULT_CLASS_HIERARCHY,
    DEFAULT_RELATION_SCHEMA,
    EXTRACTED_DIR,
    FUSED_DIR,
    INSTANCE_DIR,
    SCHEMA_DIR,
    backup_active_csvs,
    ensure_dirs,
    parent_chain,
    read_csv,
    write_csv,
    write_json,
)


CLASS_FIELDS = ["class_id", "label", "parent_class", "description"]
RELATION_FIELDS = ["relation_id", "label", "domain", "range", "property_type", "property_characteristics", "description"]
ENTITY_FIELDS = ["entity_id", "label", "class_id", "description", "source"]
TRIPLE_FIELDS = ["subject", "predicate", "object", "object_type", "object_datatype", "source", "confidence"]


def fuse_classes(entity_rows: List[Dict[str, str]], triple_rows: List[Dict[str, str]]) -> List[Dict[str, str]]:
    observed: Set[str] = {"Entity"}
    for row in entity_rows:
        for class_id in parent_chain(row.get("class_id", "Entity") or "Entity"):
            observed.add(class_id)
    for row in triple_rows:
        relation = DEFAULT_RELATION_SCHEMA.get(row.get("predicate", ""))
        if not relation:
            continue
        for class_id in [relation["domain"], relation["range"]]:
            if class_id == "Literal":
                continue
            for item in parent_chain(class_id):
                observed.add(item)
    return [item for item in DEFAULT_CLASS_HIERARCHY if item["class_id"] in observed]


def fuse_relations(triple_rows: List[Dict[str, str]]) -> List[Dict[str, str]]:
    predicates = {row["predicate"] for row in triple_rows if row.get("predicate") in DEFAULT_RELATION_SCHEMA}
    return [{"relation_id": relation_id, **DEFAULT_RELATION_SCHEMA[relation_id]} for relation_id in sorted(predicates)]


def fuse_triples(entity_rows: List[Dict[str, str]], triple_rows: List[Dict[str, str]]) -> List[Dict[str, str]]:
    entity_ids = {row["entity_id"] for row in entity_rows}
    entity_class_map = {row["entity_id"]: row["class_id"] for row in entity_rows}
    aggregated: Dict[Tuple[str, str, str, str, str], Dict[str, object]] = {}

    for row in triple_rows:
        predicate = row.get("predicate", "")
        subject = row.get("subject", "")
        object_value = row.get("object", "")
        object_type = row.get("object_type", "")
        object_datatype = row.get("object_datatype", "")
        confidence = float(row.get("confidence") or 0.0)

        relation = DEFAULT_RELATION_SCHEMA.get(predicate)
        if not relation or subject not in entity_ids:
            continue
        if relation["domain"] not in parent_chain(entity_class_map[subject]):
            continue
        if object_type == "entity" and object_value not in entity_ids:
            continue
        if object_type == "entity":
            if relation["range"] == "Literal":
                continue
            if relation["range"] not in parent_chain(entity_class_map[object_value]):
                continue
        elif relation["range"] != "Literal":
            continue

        key = (subject, predicate, object_value, object_type, object_datatype)
        if key not in aggregated:
            aggregated[key] = {
                "subject": subject,
                "predicate": predicate,
                "object": object_value,
                "object_type": object_type,
                "object_datatype": object_datatype,
                "sources": set(),
                "confidence": confidence,
            }
        aggregated[key]["sources"].add(row.get("source", "auto-extracted"))  # type: ignore[index]
        aggregated[key]["confidence"] = max(float(aggregated[key]["confidence"]), confidence)  # type: ignore[index]

    fused_rows = []
    for item in aggregated.values():
        fused_rows.append(
            {
                "subject": str(item["subject"]),
                "predicate": str(item["predicate"]),
                "object": str(item["object"]),
                "object_type": str(item["object_type"]),
                "object_datatype": str(item["object_datatype"]),
                "source": "|".join(sorted(item["sources"])) or "auto-extracted",  # type: ignore[arg-type]
                "confidence": f"{float(item['confidence']):.2f}",
            }
        )
    return sorted(fused_rows, key=lambda row: (row["subject"], row["predicate"], row["object"]))


def prune_entities(entity_rows: List[Dict[str, str]], triple_rows: List[Dict[str, str]]) -> List[Dict[str, str]]:
    referenced: Set[str] = set()
    for row in triple_rows:
        referenced.add(row["subject"])
        if row.get("object_type") == "entity":
            referenced.add(row["object"])

    pruned: List[Dict[str, str]] = []
    for row in entity_rows:
        sources = set((row.get("source") or "").split("|"))
        ner_only = sources == {"wikipedia_ner"}
        if ner_only and row["entity_id"] not in referenced:
            continue
        pruned.append(row)
    return pruned


def main() -> None:
    ensure_dirs()
    backup_active_csvs()

    entities = sorted(read_csv(EXTRACTED_DIR / "entities_normalized.csv"), key=lambda row: (row["class_id"], row["label"]))
    triples_candidates = read_csv(EXTRACTED_DIR / "triples_candidates.csv")
    triples = fuse_triples(entities, triples_candidates)
    entities = prune_entities(entities, triples)
    triples = fuse_triples(entities, triples_candidates)
    classes = fuse_classes(entities, triples)
    relations = fuse_relations(triples)

    write_csv(FUSED_DIR / "classes_fused.csv", classes, CLASS_FIELDS)
    write_csv(FUSED_DIR / "relations_fused.csv", relations, RELATION_FIELDS)
    write_csv(FUSED_DIR / "entities_fused.csv", entities, ENTITY_FIELDS)
    write_csv(FUSED_DIR / "triples_fused.csv", triples, TRIPLE_FIELDS)

    write_csv(SCHEMA_DIR / "classes.csv", classes, CLASS_FIELDS)
    write_csv(SCHEMA_DIR / "relations.csv", relations, RELATION_FIELDS)
    write_csv(INSTANCE_DIR / "entities.csv", entities, ENTITY_FIELDS)
    write_csv(INSTANCE_DIR / "triples.csv", triples, TRIPLE_FIELDS)

    write_json(
        FUSED_DIR / "fusion_summary.json",
        {
            "class_count": len(classes),
            "relation_count": len(relations),
            "entity_count": len(entities),
            "triple_count": len(triples),
        },
    )
    print("Fusion completed.")


if __name__ == "__main__":
    main()
