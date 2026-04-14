#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from __future__ import annotations

import re
from typing import Dict, List, Tuple

import requests
from bs4 import BeautifulSoup

from auto_pipeline_utils import (
    EXTRACTED_DIR,
    WIKIDATA_RAW_DIR,
    WIKIPEDIA_RAW_DIR,
    ensure_dirs,
    load_seeds,
    read_json,
    slugify,
    unique_rows,
    write_csv,
)


USER_AGENT = "turing-knowledge-graph-bot/1.0"

WIKIDATA_PROPERTY_MAP = {
    "P569": ("has_birth_date", "literal", "date", "", 0.98),
    "P570": ("has_death_date", "literal", "date", "", 0.98),
    "P19": ("born_in", "entity", "", "Place", 0.98),
    "P20": ("died_in", "entity", "", "Place", 0.98),
    "P69": ("studied_at", "entity", "", "Organization", 0.95),
    "P108": ("worked_at", "entity", "", "Organization", 0.95),
    "P101": ("related_to_field", "entity", "", "Field", 0.90),
    "P27": ("has_nationality", "literal_label", "string", "", 0.92),
    "P50": ("authored", "inverse_entity", "", "Person", 0.95),
    "P1433": ("published_in", "entity", "", "PublicationVenue", 0.90),
    "P571": ("has_year", "literal", "gYear", "", 0.90),
    "P61": ("designed", "inverse_entity", "", "Person", 0.90),
    "P170": ("designed", "inverse_entity", "", "Person", 0.88),
}


def extract_year(text: str) -> str:
    match = re.search(r"(1[0-9]{3}|20[0-9]{2})", text)
    return match.group(1) if match else ""


def extract_iso_date(text: str) -> str:
    match = re.search(r"(\d{4}-\d{2}-\d{2})", text)
    return match.group(1) if match else ""


def clean_links(values: List[str]) -> List[str]:
    cleaned = []
    for value in values:
        label = value.strip()
        if not label or re.fullmatch(r"\[\s*\d+\s*\]", label):
            continue
        cleaned.append(label)
    return cleaned


def wbgetentities_labels(ids: List[str]) -> Dict[str, str]:
    if not ids:
        return {}
    response = requests.get(
        "https://www.wikidata.org/w/api.php",
        params={
            "action": "wbgetentities",
            "format": "json",
            "ids": "|".join(ids),
            "languages": "en",
            "props": "labels",
        },
        timeout=30,
        headers={"User-Agent": USER_AGENT},
    )
    response.raise_for_status()
    data = response.json()
    return {
        entity_id: payload["labels"]["en"]["value"]
        for entity_id, payload in data.get("entities", {}).items()
        if payload.get("labels", {}).get("en", {}).get("value")
    }


def candidate_entity(entity_id: str, label: str, class_id_hint: str, source: str, description: str = "") -> Dict[str, str]:
    return {
        "entity_id": entity_id,
        "label": label,
        "class_id_hint": class_id_hint or "Entity",
        "description": description,
        "source": source,
        "source_key": label,
    }


def infer_predicate_for_seed(seed: Dict[str, str], predicate: str) -> str:
    if predicate == "designed" and seed["class_id"] == "Concept":
        return "proposed"
    return predicate


def entity_triple(subject: str, predicate: str, object_id: str, object_label: str, object_class: str, source: str, confidence: float) -> Dict[str, str]:
    return {
        "subject": subject,
        "predicate": predicate,
        "object": object_id,
        "object_label": object_label,
        "object_type": "entity",
        "object_datatype": "",
        "object_class_hint": object_class,
        "source": source,
        "confidence": f"{confidence:.2f}",
    }


def literal_triple(subject: str, predicate: str, value: str, datatype: str, source: str, confidence: float) -> Dict[str, str]:
    return {
        "subject": subject,
        "predicate": predicate,
        "object": value,
        "object_label": value,
        "object_type": "literal",
        "object_datatype": datatype,
        "object_class_hint": "",
        "source": source,
        "confidence": f"{confidence:.2f}",
    }


def parse_wikipedia(seed: Dict[str, str], entities: List[Dict[str, str]], triples: List[Dict[str, str]]) -> None:
    slug = slugify(seed["entity_id"])
    summary_path = WIKIPEDIA_RAW_DIR / f"{slug}.summary.json"
    html_path = WIKIPEDIA_RAW_DIR / f"{slug}.page.html"
    if not summary_path.exists() or not html_path.exists():
        return

    summary = read_json(summary_path)
    html = html_path.read_text(encoding="utf-8")
    entities.append(candidate_entity(seed["entity_id"], seed["label"], seed["class_id"], "wikipedia", summary.get("extract", "")))

    soup = BeautifulSoup(html, "html.parser")
    infobox = soup.find("table", class_=lambda value: value and "infobox" in value)
    if not infobox:
        return

    for row in infobox.find_all("tr"):
        header = row.find("th")
        cell = row.find("td")
        if not header or not cell:
            continue
        field_name = header.get_text(" ", strip=True).lower()
        cell_text = cell.get_text(" ", strip=True)
        links = clean_links([a.get_text(" ", strip=True) for a in cell.find_all("a", href=True)])

        if field_name == "born":
            date_value = extract_iso_date(cell_text)
            if date_value:
                triples.append(literal_triple(seed["entity_id"], "has_birth_date", date_value, "date", "wikipedia", 0.90))
            if links:
                place_label = links[0]
                place_id = slugify(place_label)
                entities.append(candidate_entity(place_id, place_label, "Place", "wikipedia"))
                triples.append(entity_triple(seed["entity_id"], "born_in", place_id, place_label, "Place", "wikipedia", 0.88))

        elif field_name == "died":
            date_value = extract_iso_date(cell_text)
            if date_value:
                triples.append(literal_triple(seed["entity_id"], "has_death_date", date_value, "date", "wikipedia", 0.90))
            if links:
                place_label = links[0]
                place_id = slugify(place_label)
                entities.append(candidate_entity(place_id, place_label, "Place", "wikipedia"))
                triples.append(entity_triple(seed["entity_id"], "died_in", place_id, place_label, "Place", "wikipedia", 0.88))

        elif field_name in {"alma mater", "education"}:
            for label in links:
                target_id = slugify(label)
                entities.append(candidate_entity(target_id, label, "Organization", "wikipedia"))
                triples.append(entity_triple(seed["entity_id"], "studied_at", target_id, label, "Organization", "wikipedia", 0.87))

        elif field_name in {"institutions", "employer", "employers"}:
            for label in links:
                target_id = slugify(label)
                entities.append(candidate_entity(target_id, label, "Organization", "wikipedia"))
                triples.append(entity_triple(seed["entity_id"], "worked_at", target_id, label, "Organization", "wikipedia", 0.85))

        elif field_name == "fields":
            for label in links:
                target_id = slugify(label)
                entities.append(candidate_entity(target_id, label, "Field", "wikipedia"))
                triples.append(entity_triple(seed["entity_id"], "related_to_field", target_id, label, "Field", "wikipedia", 0.82))

        elif field_name in {"author", "authors"} and seed["class_id"] == "Work":
            for label in links:
                person_id = slugify(label)
                entities.append(candidate_entity(person_id, label, "Person", "wikipedia"))
                triples.append(entity_triple(person_id, "authored", seed["entity_id"], seed["label"], "Work", "wikipedia", 0.87))

        elif field_name in {"journal", "published in"} and seed["class_id"] == "Work":
            if links:
                venue_label = links[0]
                venue_id = slugify(venue_label)
                entities.append(candidate_entity(venue_id, venue_label, "PublicationVenue", "wikipedia"))
                triples.append(entity_triple(seed["entity_id"], "published_in", venue_id, venue_label, "PublicationVenue", "wikipedia", 0.84))

        elif field_name in {"published", "publication date"} and seed["class_id"] == "Work":
            year = extract_year(cell_text)
            if year:
                triples.append(literal_triple(seed["entity_id"], "has_year", year, "gYear", "wikipedia", 0.82))

        elif field_name in {"inventor", "invented by", "designed by"} and seed["class_id"] in {"Concept", "Machine"}:
            predicate = "proposed" if seed["class_id"] == "Concept" else "designed"
            for label in links:
                person_id = slugify(label)
                entities.append(candidate_entity(person_id, label, "Person", "wikipedia"))
                triples.append(entity_triple(person_id, predicate, seed["entity_id"], seed["label"], seed["class_id"], "wikipedia", 0.85))


def parse_wikidata(seed: Dict[str, str], entities: List[Dict[str, str]], triples: List[Dict[str, str]]) -> None:
    summary_path = WIKIPEDIA_RAW_DIR / f"{slugify(seed['entity_id'])}.summary.json"
    if not summary_path.exists():
        return
    summary = read_json(summary_path)
    qid = summary.get("wikibase_item")
    if not qid:
        return
    path = WIKIDATA_RAW_DIR / f"{qid}.json"
    if not path.exists():
        return

    payload = read_json(path)
    entity = payload["entities"][qid]
    for alias in entity.get("aliases", {}).get("en", [])[:5]:
        triples.append(literal_triple(seed["entity_id"], "has_alias", alias["value"], "string", "wikidata", 0.95))

    pending: List[Tuple[str, Tuple[str, str, str, str, float]]] = []
    ids: List[str] = []
    for property_id, config in WIKIDATA_PROPERTY_MAP.items():
        predicate, mode, datatype, target_class, confidence = config
        for claim in entity.get("claims", {}).get(property_id, []):
            value = claim.get("mainsnak", {}).get("datavalue", {}).get("value")
            if not value:
                continue
            if mode == "literal":
                if property_id in {"P569", "P570"}:
                    triples.append(literal_triple(seed["entity_id"], predicate, str(value.get("time", "")).strip("+").split("T")[0], datatype, "wikidata", confidence))
                elif property_id == "P571":
                    year = str(value.get("time", "")).strip("+")[:4]
                    if year:
                        triples.append(literal_triple(seed["entity_id"], predicate, year, datatype, "wikidata", confidence))
            elif mode in {"entity", "inverse_entity", "literal_label"} and isinstance(value, dict) and value.get("id"):
                pending.append((value["id"], config))
                ids.append(value["id"])

    labels = wbgetentities_labels(sorted(set(ids)))
    for target_qid, config in pending:
        predicate, mode, datatype, target_class, confidence = config
        predicate = infer_predicate_for_seed(seed, predicate)
        label = labels.get(target_qid)
        if not label:
            continue
        if mode == "literal_label":
            triples.append(literal_triple(seed["entity_id"], predicate, label, "string", "wikidata", confidence))
            continue
        target_id = slugify(label)
        entities.append(candidate_entity(target_id, label, target_class or "Entity", "wikidata"))
        if mode == "entity":
            triples.append(entity_triple(seed["entity_id"], predicate, target_id, label, target_class or "Entity", "wikidata", confidence))
        else:
            triples.append(entity_triple(target_id, predicate, seed["entity_id"], seed["label"], seed["class_id"], "wikidata", confidence))


def main() -> None:
    ensure_dirs()
    entities: List[Dict[str, str]] = []
    triples: List[Dict[str, str]] = []
    for seed in load_seeds():
        parse_wikipedia(seed, entities, triples)
        parse_wikidata(seed, entities, triples)

    entity_rows = unique_rows(entities, ["entity_id", "label", "source"])
    write_csv(
        EXTRACTED_DIR / "entities_candidates_structured.csv",
        entity_rows,
        ["entity_id", "label", "class_id_hint", "description", "source", "source_key"],
    )
    write_csv(
        EXTRACTED_DIR / "entities_candidates.csv",
        entity_rows,
        ["entity_id", "label", "class_id_hint", "description", "source", "source_key"],
    )
    triple_rows = unique_rows(triples, ["subject", "predicate", "object", "source"])
    write_csv(
        EXTRACTED_DIR / "triples_candidates_structured.csv",
        triple_rows,
        ["subject", "predicate", "object", "object_label", "object_type", "object_datatype", "object_class_hint", "source", "confidence"],
    )
    write_csv(
        EXTRACTED_DIR / "triples_candidates.csv",
        triple_rows,
        ["subject", "predicate", "object", "object_label", "object_type", "object_datatype", "object_class_hint", "source", "confidence"],
    )
    print("Extraction completed.")


if __name__ == "__main__":
    main()
