#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from __future__ import annotations

import re
from collections import defaultdict
from typing import Dict, List, Tuple

import spacy

from auto_pipeline_utils import EXTRACTED_DIR, ensure_dirs, load_seeds, read_csv, slugify, unique_rows, write_csv


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

ENTITY_SOURCE_PRIORITY = {
    "wikidata": 4,
    "wikipedia": 3,
    "wikipedia_summary": 3,
    "wikipedia_html": 3,
    "wikipedia_ner": 1,
}

NER_LABEL_MAP = {
    "PERSON": "Person",
    "ORG": "Organization",
    "GPE": "Place",
    "LOC": "Place",
    "FAC": "Organization",
    "EVENT": "Event",
    "WORK_OF_ART": "Work",
}


def choose_best_entity_rows(rows: List[Dict[str, str]]) -> Dict[str, Dict[str, str]]:
    grouped: Dict[str, List[Dict[str, str]]] = defaultdict(list)
    for row in rows:
        grouped[row["label"].strip().lower()].append(row)

    best: Dict[str, Dict[str, str]] = {}
    for label_key, candidates in grouped.items():
        best[label_key] = max(
            candidates,
            key=lambda row: (
                ENTITY_SOURCE_PRIORITY.get(row.get("source", ""), 0),
                row.get("source", "") != "wikipedia_ner",
                len(row.get("label", "")),
            ),
        )
    return best


def find_field_mentions(sentence_lower: str, entity_lookup: Dict[str, Dict[str, str]]) -> List[Tuple[str, str]]:
    matches: List[Tuple[str, str]] = []
    for row in entity_lookup.values():
        if row.get("class_id_hint") != "Field":
            continue
        label = row["label"].strip()
        if not label:
            continue
        pattern = r"\b" + re.escape(label.lower()) + r"\b"
        if re.search(pattern, sentence_lower):
            matches.append((row["entity_id"], row["label"]))
    return matches


def sentence_entities(doc_sentence, entity_lookup: Dict[str, Dict[str, str]]) -> List[Dict[str, str]]:
    entities: List[Dict[str, str]] = []
    seen = set()
    for ent in doc_sentence.ents:
        canonical = re.sub(r"\s+", " ", ent.text).strip(" ,.;:'\"")
        canonical = re.sub(r"^the\s+", "", canonical, flags=re.IGNORECASE)
        key = canonical.lower()
        row = entity_lookup.get(key)
        class_hint = row["class_id_hint"] if row else NER_LABEL_MAP.get(ent.label_)
        if not class_hint:
            continue
        entity_id = row["entity_id"] if row else slugify(canonical)
        label = row["label"] if row else canonical
        signature = (entity_id, class_hint)
        if signature in seen:
            continue
        seen.add(signature)
        entities.append({"entity_id": entity_id, "label": label, "class_id_hint": class_hint})
    return entities


def emit_entity_triple(subject: str, predicate: str, target: Dict[str, str], confidence: float) -> Dict[str, str]:
    return {
        "subject": subject,
        "predicate": predicate,
        "object": target["entity_id"],
        "object_label": target["label"],
        "object_type": "entity",
        "object_datatype": "",
        "object_class_hint": target["class_id_hint"],
        "source": "wikipedia_text_rule",
        "confidence": f"{confidence:.2f}",
    }


def dedupe_entities(rows: List[Dict[str, str]]) -> List[Dict[str, str]]:
    seen = set()
    result: List[Dict[str, str]] = []
    for row in rows:
        entity_id = row["entity_id"]
        if entity_id in seen:
            continue
        seen.add(entity_id)
        result.append(row)
    return result


def extract_from_sentence(seed: Dict[str, str], sentence_text: str, sent_entities: List[Dict[str, str]], field_mentions: List[Tuple[str, str]]) -> List[Dict[str, str]]:
    rows: List[Dict[str, str]] = []
    lower = sentence_text.lower()

    orgs = dedupe_entities([item for item in sent_entities if item["class_id_hint"] == "Organization"])
    places = dedupe_entities([item for item in sent_entities if item["class_id_hint"] == "Place"])
    events = [item for item in sent_entities if item["class_id_hint"] == "Event"]
    persons = [item for item in sent_entities if item["class_id_hint"] == "Person"]
    works = [item for item in sent_entities if item["class_id_hint"] == "Work"]

    if any(phrase in lower for phrase in ["graduated from", "earned a doctorate degree from", "studied at", "educated at"]):
        for org in orgs:
            rows.append(emit_entity_triple(seed["entity_id"], "studied_at", org, 0.72))

    if any(phrase in lower for phrase in ["worked for", "worked at", "joined", "worked in"]):
        for org in orgs:
            rows.append(emit_entity_triple(seed["entity_id"], "worked_at", org, 0.70))

    seed_label_lower = seed["label"].lower()
    if seed["class_id"] == "Organization" and lower.startswith(seed_label_lower) and " is " in lower and " in " in lower:
        for place in places[:2]:
            rows.append(emit_entity_triple(seed["entity_id"], "located_in", place, 0.68))

    if seed["class_id"] == "Person" and any(phrase in lower for phrase in ["during world war", "during the second world war", "in the second world war"]):
        for event in events:
            rows.append(emit_entity_triple(seed["entity_id"], "contributed_to", event, 0.60))

    if seed["class_id"] == "Person":
        for entity_id, label in field_mentions:
            rows.append(
                {
                    "subject": seed["entity_id"],
                    "predicate": "related_to_field",
                    "object": entity_id,
                    "object_label": label,
                    "object_type": "entity",
                    "object_datatype": "",
                    "object_class_hint": "Field",
                    "source": "wikipedia_text_rule",
                    "confidence": "0.62",
                }
            )

    if seed["class_id"] == "Work" and ("proof by" in lower or "paper by" in lower or "published by" in lower):
        for person in persons:
            rows.append(emit_entity_triple(person["entity_id"], "authored", {"entity_id": seed["entity_id"], "label": seed["label"], "class_id_hint": "Work"}, 0.64))

    if seed["class_id"] in {"Concept", "Machine"} and any(phrase in lower for phrase in ["invented by", "introduced by", "called it"]):
        predicate = "proposed" if seed["class_id"] == "Concept" else "designed"
        for person in persons:
            rows.append(emit_entity_triple(person["entity_id"], predicate, {"entity_id": seed["entity_id"], "label": seed["label"], "class_id_hint": seed["class_id"]}, 0.66))

    return rows


def main() -> None:
    ensure_dirs()
    try:
        nlp = spacy.load("en_core_web_sm")
    except OSError as exc:
        raise RuntimeError("spaCy model 'en_core_web_sm' is not installed.") from exc

    blocks = read_csv(EXTRACTED_DIR / "text_blocks.csv")
    entity_rows = read_csv(EXTRACTED_DIR / "entities_candidates.csv")
    entity_lookup = choose_best_entity_rows(entity_rows)
    seed_map = {item["entity_id"]: item for item in load_seeds()}

    triples: List[Dict[str, str]] = []
    for block in blocks:
        seed = seed_map.get(block["seed_entity_id"])
        if not seed:
            continue
        doc = nlp(block["text"])
        for sent in doc.sents:
            sentence_text = sent.text.strip()
            if len(sentence_text) < 20:
                continue
            sent_entities = sentence_entities(sent, entity_lookup)
            field_mentions = find_field_mentions(sentence_text.lower(), entity_lookup)
            triples.extend(extract_from_sentence(seed, sentence_text, sent_entities, field_mentions))

    write_csv(
        EXTRACTED_DIR / "triples_candidates_text.csv",
        unique_rows(triples, ["subject", "predicate", "object", "source"]),
        TRIPLE_FIELDS,
    )
    print("Text relation extraction completed.")


if __name__ == "__main__":
    main()
