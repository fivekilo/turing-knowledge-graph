#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from __future__ import annotations

import re
from typing import Dict, List, Tuple

import spacy

from auto_pipeline_utils import EXTRACTED_DIR, ensure_dirs, load_seeds, read_csv, slugify, unique_rows, write_csv


MENTION_FIELDS = [
    "seed_entity_id",
    "block_id",
    "mention",
    "entity_id",
    "entity_type_hint",
    "model_label",
    "start",
    "end",
    "context",
    "source",
    "confidence",
]
ENTITY_FIELDS = ["entity_id", "label", "class_id_hint", "description", "source", "source_key"]

LABEL_MAP = {
    "PERSON": "Person",
    "ORG": "Organization",
    "GPE": "Place",
    "LOC": "Place",
    "FAC": "Place",
    "WORK_OF_ART": "Work",
    "EVENT": "Event",
}


def canonicalize_mention(text: str) -> str:
    stripped = re.sub(r"\s+", " ", text).strip(" ,.;:'\"")
    stripped = re.sub(r"^the\s+", "", stripped, flags=re.IGNORECASE)
    return stripped


def build_existing_entity_lookup() -> Dict[str, Tuple[str, str, str]]:
    rows = read_csv(EXTRACTED_DIR / "entities_candidates_structured.csv")
    lookup: Dict[str, Tuple[str, str, str]] = {}
    for row in rows:
        key = row["label"].strip().lower()
        if key and key not in lookup:
            lookup[key] = (row["entity_id"], row["label"], row["class_id_hint"])
    return lookup


def overlapping_structured_label(mention: str, context: str, existing_lookup: Dict[str, Tuple[str, str, str]]) -> bool:
    mention_lower = mention.lower()
    context_lower = context.lower()
    for label in existing_lookup:
        if label == mention_lower:
            continue
        if mention_lower in label and label in context_lower:
            return True
    return False


def valid_mention(
    text: str,
    class_hint: str,
    existing_lookup: Dict[str, Tuple[str, str, str]],
    seed: Dict[str, str],
    context: str,
    start: int,
    end: int,
    model_label: str,
) -> bool:
    stripped = canonicalize_mention(text)
    lowered = stripped.lower()
    if len(stripped) < 2:
        return False
    if lowered in {"he", "she", "it", "they", "his", "her", "their"}:
        return False
    token_count = len(stripped.split())
    if class_hint in {"Person", "Organization"} and token_count < 2 and lowered not in existing_lookup:
        return False
    if stripped.isupper() and len(stripped) <= 4 and lowered not in existing_lookup:
        return False
    if model_label == "GPE" and lowered.endswith("problem"):
        return False
    if class_hint == "Place" and f"order of the {lowered}" in context.lower():
        return False
    if class_hint == "Work" and token_count < 3 and lowered not in existing_lookup:
        return False
    if class_hint == "Person" and seed["class_id"] == "Person" and lowered not in existing_lookup:
        seed_tokens = set(seed["label"].lower().replace(",", " ").split())
        mention_tokens = set(lowered.replace(",", " ").split())
        if mention_tokens & seed_tokens:
            return False
        if start < 40:
            return False
    if class_hint == "Person" and end < len(context) and context[end:end + 1] == "(":
        return False
    if class_hint == "Person" and ", " + stripped in context and "(" in context[end: end + 20]:
        return False
    if class_hint == "Person" and lowered not in existing_lookup:
        context_lower = context.lower()
        if re.search(r"\b(like|such as)\s+" + re.escape(lowered) + r"\b", context_lower):
            return False
    if lowered not in existing_lookup and overlapping_structured_label(stripped, context, existing_lookup):
        return False
    return True


def main() -> None:
    ensure_dirs()
    try:
        nlp = spacy.load("en_core_web_sm")
    except OSError as exc:
        raise RuntimeError("spaCy model 'en_core_web_sm' is not installed.") from exc

    blocks = read_csv(EXTRACTED_DIR / "text_blocks.csv")
    existing_lookup = build_existing_entity_lookup()
    seed_map = {item["entity_id"]: item for item in load_seeds()}
    mention_rows: List[Dict[str, str]] = []
    entity_rows: List[Dict[str, str]] = []

    for block in blocks:
        seed = seed_map.get(block["seed_entity_id"])
        if not seed:
            continue
        doc = nlp(block["text"])
        for ent in doc.ents:
            class_hint = LABEL_MAP.get(ent.label_)
            if not class_hint:
                continue
            mention = canonicalize_mention(ent.text)
            if not valid_mention(
                mention,
                class_hint,
                existing_lookup,
                seed,
                ent.sent.text,
                ent.start_char - ent.sent.start_char,
                ent.end_char - ent.sent.start_char,
                ent.label_,
            ):
                continue

            existing = existing_lookup.get(mention.lower())
            if existing:
                entity_id, mention_label, class_hint = existing
            else:
                entity_id, mention_label = slugify(mention), mention
            mention_rows.append(
                {
                    "seed_entity_id": block["seed_entity_id"],
                    "block_id": block["block_id"],
                    "mention": mention_label,
                    "entity_id": entity_id,
                    "entity_type_hint": class_hint,
                    "model_label": ent.label_,
                    "start": str(ent.start_char),
                    "end": str(ent.end_char),
                    "context": ent.sent.text.strip(),
                    "source": "wikipedia_ner",
                    "confidence": "0.75",
                }
            )
            entity_rows.append(
                {
                    "entity_id": entity_id,
                    "label": mention_label,
                    "class_id_hint": class_hint,
                    "description": "",
                    "source": "wikipedia_ner",
                    "source_key": block["seed_entity_id"],
                }
            )

    write_csv(
        EXTRACTED_DIR / "ner_mentions.csv",
        unique_rows(mention_rows, ["seed_entity_id", "block_id", "mention", "entity_type_hint"]),
        MENTION_FIELDS,
    )
    write_csv(
        EXTRACTED_DIR / "entities_candidates_ner.csv",
        unique_rows(entity_rows, ["entity_id", "label", "class_id_hint", "source"]),
        ENTITY_FIELDS,
    )
    print("NER candidate extraction completed.")


if __name__ == "__main__":
    main()
