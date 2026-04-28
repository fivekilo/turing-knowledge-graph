#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from __future__ import annotations

import math
import re
from collections import Counter, defaultdict
from typing import Dict, Iterable, List, Set, Tuple

from auto_pipeline_utils import EXTRACTED_DIR, ensure_dirs, read_csv, unique_rows, write_csv


ENTITY_FIELDS = ["entity_id", "label", "class_id_hint", "description", "source", "source_key"]
DECISION_FIELDS = [
    "seed_entity_id",
    "block_id",
    "mention",
    "original_entity_id",
    "resolved_entity_id",
    "resolved_label",
    "class_id_hint",
    "status",
    "candidate_count",
    "best_score",
    "margin",
    "reason",
]
CANONICAL_FIELDS = ["entity_id", "canonical_entity_id", "canonical_label", "class_id", "reason"]

STOPWORDS = {
    "a",
    "an",
    "and",
    "as",
    "at",
    "by",
    "for",
    "from",
    "in",
    "into",
    "is",
    "of",
    "on",
    "or",
    "the",
    "to",
    "was",
    "with",
}

ROMAN_MAP = {
    "i": "1",
    "ii": "2",
    "iii": "3",
    "iv": "4",
    "v": "5",
    "vi": "6",
    "vii": "7",
    "viii": "8",
    "ix": "9",
    "x": "10",
}

ORDINAL_MAP = {
    "first": "1",
    "second": "2",
    "third": "3",
    "fourth": "4",
    "fifth": "5",
    "sixth": "6",
    "seventh": "7",
    "eighth": "8",
    "ninth": "9",
    "tenth": "10",
}

SOURCE_PRIORITY = {
    "wikidata": 5,
    "wikipedia": 4,
    "wikipedia_summary": 4,
    "wikipedia_html": 4,
    "wikipedia_ner": 1,
}


def normalize_surface(text: str) -> str:
    value = text.lower()
    value = value.replace("’", "'").replace("–", "-").replace("—", "-")
    value = re.sub(r"\b(" + "|".join(ROMAN_MAP) + r")\b", lambda m: ROMAN_MAP[m.group(1)], value)
    value = re.sub(r"\b(" + "|".join(ORDINAL_MAP) + r")\b", lambda m: ORDINAL_MAP[m.group(1)], value)
    value = re.sub(r"[^a-z0-9]+", " ", value)
    value = re.sub(r"\s+", " ", value).strip()
    return value


def surface_tokens(text: str) -> Set[str]:
    return {token for token in normalize_surface(text).split() if token and token not in STOPWORDS}


def surface_signature(text: str) -> str:
    return " ".join(sorted(surface_tokens(text)))


def compatible_types(left: str, right: str) -> bool:
    return left == right


def source_score(source: str) -> int:
    return max(SOURCE_PRIORITY.get(item, 0) for item in source.split("|") if item) if source else 0


class UnionFind:
    def __init__(self, items: Iterable[str]) -> None:
        self.parent = {item: item for item in items}

    def find(self, item: str) -> str:
        if self.parent[item] != item:
            self.parent[item] = self.find(self.parent[item])
        return self.parent[item]

    def union(self, left: str, right: str) -> None:
        left_root = self.find(left)
        right_root = self.find(right)
        if left_root != right_root:
            self.parent[right_root] = left_root


def choose_best_row(rows: List[Dict[str, str]]) -> Dict[str, str]:
    return max(
        rows,
        key=lambda row: (
            source_score(row.get("source", "")),
            "wikidata" in row.get("source", ""),
            len(row.get("description", "")),
            -len(row.get("label", "")),
            row.get("entity_id", ""),
        ),
    )


def build_alias_index(
    structured_rows: List[Dict[str, str]],
    structured_triples: List[Dict[str, str]],
) -> Tuple[Dict[str, List[Dict[str, str]]], Dict[str, Dict[str, str]]]:
    rows_by_entity: Dict[str, List[Dict[str, str]]] = defaultdict(list)
    for row in structured_rows:
        rows_by_entity[row["entity_id"]].append(row)

    kb_rows = {entity_id: choose_best_row(rows) for entity_id, rows in rows_by_entity.items()}
    alias_index: Dict[str, List[Dict[str, str]]] = defaultdict(list)

    for row in kb_rows.values():
        alias_index[normalize_surface(row["label"])].append(row)

    for triple in structured_triples:
        if triple.get("predicate") != "has_alias" or triple.get("object_type") != "literal":
            continue
        subject = triple.get("subject", "")
        alias = triple.get("object", "").strip()
        if not alias or subject not in kb_rows:
            continue
        alias_index[normalize_surface(alias)].append(kb_rows[subject])

    for key, values in alias_index.items():
        deduped = {row["entity_id"]: row for row in values}
        alias_index[key] = list(deduped.values())
    return alias_index, kb_rows


def build_profiles(
    kb_rows: Dict[str, Dict[str, str]],
    structured_triples: List[Dict[str, str]],
) -> Dict[str, Dict[str, object]]:
    aliases_by_entity: Dict[str, Set[str]] = defaultdict(set)
    relation_labels_by_entity: Dict[str, Set[str]] = defaultdict(set)
    relation_counts: Counter[str] = Counter()

    for triple in structured_triples:
        subject = triple.get("subject", "")
        if subject in kb_rows:
            relation_counts[subject] += 1
            relation_labels_by_entity[subject].add(triple.get("object_label", "") or triple.get("object", ""))
        if triple.get("object_type") == "entity":
            obj = triple.get("object", "")
            if obj in kb_rows:
                relation_counts[obj] += 1
                relation_labels_by_entity[obj].add(kb_rows.get(subject, {}).get("label", subject))
        if triple.get("predicate") == "has_alias" and triple.get("object_type") == "literal" and subject in kb_rows:
            aliases_by_entity[subject].add(triple.get("object", ""))

    profiles: Dict[str, Dict[str, object]] = {}
    for entity_id, row in kb_rows.items():
        aliases = aliases_by_entity.get(entity_id, set())
        texts = [row["label"], row.get("description", "")] + list(aliases) + list(relation_labels_by_entity.get(entity_id, set()))
        token_set: Set[str] = set()
        for text in texts:
            token_set.update(surface_tokens(text))
        profiles[entity_id] = {
            "row": row,
            "aliases": aliases,
            "tokens": token_set,
            "popularity": min(1.0, math.log1p(relation_counts.get(entity_id, 0) + len(aliases)) / 3.0),
        }
    return profiles


def candidate_surface_score(mention: str, candidate_row: Dict[str, str], aliases: Set[str]) -> Tuple[float, str]:
    mention_norm = normalize_surface(mention)
    label_norm = normalize_surface(candidate_row["label"])
    alias_norms = {normalize_surface(alias) for alias in aliases}
    if mention_norm == label_norm:
        return 1.0, "exact_label"
    if mention_norm in alias_norms:
        return 0.96, "exact_alias"

    mention_tokens = surface_tokens(mention)
    label_tokens = surface_tokens(candidate_row["label"])
    if mention_tokens and label_tokens:
        if mention_tokens == label_tokens:
            return 0.92, "token_equivalent"
        if mention_tokens.issubset(label_tokens):
            return 0.78, "token_containment"
        overlap = len(mention_tokens & label_tokens) / max(1, len(mention_tokens | label_tokens))
        if overlap >= 0.75 and len(mention_tokens) <= len(label_tokens):
            return 0.62 + 0.2 * overlap, "token_overlap"
    return 0.0, "no_match"


def context_similarity(context: str, profile_tokens: Set[str]) -> float:
    context_tokens = surface_tokens(context)
    if not context_tokens or not profile_tokens:
        return 0.0
    overlap = len(context_tokens & profile_tokens)
    return overlap / max(1, min(len(context_tokens), 8))


def rank_candidates(
    mention_row: Dict[str, str],
    alias_index: Dict[str, List[Dict[str, str]]],
    profiles: Dict[str, Dict[str, object]],
) -> List[Tuple[float, Dict[str, str], str]]:
    mention = mention_row["mention"]
    class_hint = mention_row["entity_type_hint"]
    mention_norm = normalize_surface(mention)
    direct = alias_index.get(mention_norm, [])
    candidates: Dict[str, Tuple[Dict[str, str], str, float]] = {}

    def maybe_add(row: Dict[str, str], reason: str, surface_score: float) -> None:
        if not compatible_types(class_hint, row["class_id_hint"]):
            return
        current = candidates.get(row["entity_id"])
        if current is None or surface_score > current[2]:
            candidates[row["entity_id"]] = (row, reason, surface_score)

    for row in direct:
        aliases = profiles[row["entity_id"]]["aliases"]  # type: ignore[index]
        surface_score, reason = candidate_surface_score(mention, row, aliases)  # type: ignore[arg-type]
        maybe_add(row, reason, surface_score)

    if not candidates:
        for entity_id, profile in profiles.items():
            row = profile["row"]  # type: ignore[assignment]
            aliases = profile["aliases"]  # type: ignore[assignment]
            surface_score, reason = candidate_surface_score(mention, row, aliases)
            if surface_score >= 0.75:
                maybe_add(row, reason, surface_score)

    ranked: List[Tuple[float, Dict[str, str], str]] = []
    for entity_id, (row, reason, surface_score) in candidates.items():
        profile = profiles[entity_id]
        ctx = context_similarity(mention_row["context"], profile["tokens"])  # type: ignore[arg-type]
        popularity = float(profile["popularity"])  # type: ignore[arg-type]
        score = 0.55 * surface_score + 0.30 * ctx + 0.10 * 1.0 + 0.05 * popularity
        ranked.append((score, row, reason))
    return sorted(ranked, key=lambda item: item[0], reverse=True)


def choose_link(
    mention_row: Dict[str, str],
    alias_index: Dict[str, List[Dict[str, str]]],
    profiles: Dict[str, Dict[str, object]],
) -> Dict[str, str]:
    ranked = rank_candidates(mention_row, alias_index, profiles)
    original_entity_id = mention_row["entity_id"]
    class_hint = mention_row["entity_type_hint"]
    mention = mention_row["mention"]
    if not ranked:
        return {
            "seed_entity_id": mention_row["seed_entity_id"],
            "block_id": mention_row["block_id"],
            "mention": mention,
            "original_entity_id": original_entity_id,
            "resolved_entity_id": original_entity_id,
            "resolved_label": mention,
            "class_id_hint": class_hint,
            "status": "nil",
            "candidate_count": "0",
            "best_score": "0.00",
            "margin": "0.00",
            "reason": "no_candidate",
        }

    best_score, best_row, reason = ranked[0]
    second_score = ranked[1][0] if len(ranked) > 1 else 0.0
    margin = best_score - second_score
    status = "linked" if best_score >= 0.60 and (margin >= 0.05 or best_score >= 0.72) else "nil"
    resolved_id = best_row["entity_id"] if status == "linked" else original_entity_id
    resolved_label = best_row["label"] if status == "linked" else mention
    return {
        "seed_entity_id": mention_row["seed_entity_id"],
        "block_id": mention_row["block_id"],
        "mention": mention,
        "original_entity_id": original_entity_id,
        "resolved_entity_id": resolved_id,
        "resolved_label": resolved_label,
        "class_id_hint": class_hint,
        "status": status,
        "candidate_count": str(len(ranked)),
        "best_score": f"{best_score:.2f}",
        "margin": f"{margin:.2f}",
        "reason": reason if status == "linked" else f"nil_after_{reason}",
    }


def build_linked_ner_rows(
    decisions: List[Dict[str, str]],
    kb_rows: Dict[str, Dict[str, str]],
) -> List[Dict[str, str]]:
    rows: List[Dict[str, str]] = []
    for item in decisions:
        resolved_id = item["resolved_entity_id"]
        if item["status"] == "linked" and resolved_id in kb_rows:
            kb_row = kb_rows[resolved_id]
            rows.append(
                {
                    "entity_id": kb_row["entity_id"],
                    "label": kb_row["label"],
                    "class_id_hint": kb_row["class_id_hint"],
                    "description": "",
                    "source": "wikipedia_ner",
                    "source_key": item["seed_entity_id"],
                }
            )
        else:
            rows.append(
                {
                    "entity_id": resolved_id,
                    "label": item["resolved_label"],
                    "class_id_hint": item["class_id_hint"],
                    "description": "",
                    "source": "wikipedia_ner",
                    "source_key": item["seed_entity_id"],
                }
            )
    return unique_rows(rows, ["entity_id", "label", "class_id_hint", "source", "source_key"])


def build_canonical_map(
    structured_rows: List[Dict[str, str]],
    linked_ner_rows: List[Dict[str, str]],
    alias_index: Dict[str, List[Dict[str, str]]],
) -> List[Dict[str, str]]:
    all_rows = structured_rows + linked_ner_rows
    best_by_id: Dict[str, Dict[str, str]] = {}
    grouped_by_id: Dict[str, List[Dict[str, str]]] = defaultdict(list)
    for row in all_rows:
        grouped_by_id[row["entity_id"]].append(row)
    for entity_id, rows in grouped_by_id.items():
        best_by_id[entity_id] = choose_best_row(rows)

    uf = UnionFind(best_by_id.keys())

    for entity_id, row in best_by_id.items():
        norm = normalize_surface(row["label"])
        for target in alias_index.get(norm, []):
            if target["entity_id"] != entity_id and compatible_types(row["class_id_hint"], target["class_id_hint"]):
                uf.union(entity_id, target["entity_id"])

    signature_groups: Dict[Tuple[str, str], List[str]] = defaultdict(list)
    for entity_id, row in best_by_id.items():
        signature_groups[(row["class_id_hint"], surface_signature(row["label"]))].append(entity_id)
    for ids in signature_groups.values():
        if len(ids) > 1:
            pivot = ids[0]
            for entity_id in ids[1:]:
                uf.union(pivot, entity_id)

    place_ids = [entity_id for entity_id, row in best_by_id.items() if row["class_id_hint"] == "Place"]
    place_tokens = {entity_id: surface_tokens(best_by_id[entity_id]["label"]) for entity_id in place_ids}
    for left in place_ids:
        for right in place_ids:
            if left == right:
                continue
            left_tokens = place_tokens[left]
            right_tokens = place_tokens[right]
            if right_tokens and right_tokens.issubset(left_tokens) and len(left_tokens) - len(right_tokens) <= 1:
                uf.union(left, right)

    groups: Dict[str, List[str]] = defaultdict(list)
    for entity_id in best_by_id:
        groups[uf.find(entity_id)].append(entity_id)

    canonical_choice: Dict[str, str] = {}
    for ids in groups.values():
        canonical = max(
            ids,
            key=lambda entity_id: (
                source_score(best_by_id[entity_id].get("source", "")),
                "wikidata" in best_by_id[entity_id].get("source", ""),
                -len(best_by_id[entity_id]["label"]),
                best_by_id[entity_id]["entity_id"],
            ),
        )
        for entity_id in ids:
            canonical_choice[entity_id] = canonical

    rows: List[Dict[str, str]] = []
    for entity_id, canonical_id in sorted(canonical_choice.items()):
        canonical_row = best_by_id[canonical_id]
        reason = "self"
        if entity_id != canonical_id:
            reason = "alias_or_surface_cluster"
        rows.append(
            {
                "entity_id": entity_id,
                "canonical_entity_id": canonical_id,
                "canonical_label": canonical_row["label"],
                "class_id": canonical_row["class_id_hint"],
                "reason": reason,
            }
        )
    return rows


def main() -> None:
    ensure_dirs()
    structured_rows = read_csv(EXTRACTED_DIR / "entities_candidates_structured.csv")
    structured_triples = read_csv(EXTRACTED_DIR / "triples_candidates_structured.csv")
    mention_rows = read_csv(EXTRACTED_DIR / "ner_mentions.csv")

    alias_index, kb_rows = build_alias_index(structured_rows, structured_triples)
    profiles = build_profiles(kb_rows, structured_triples)
    decisions = [choose_link(row, alias_index, profiles) for row in mention_rows]
    linked_ner_rows = build_linked_ner_rows(decisions, kb_rows)
    canonical_map = build_canonical_map(structured_rows, linked_ner_rows, alias_index)

    write_csv(
        EXTRACTED_DIR / "entity_linking_decisions.csv",
        decisions,
        DECISION_FIELDS,
    )
    write_csv(
        EXTRACTED_DIR / "entities_candidates_ner_linked.csv",
        linked_ner_rows,
        ENTITY_FIELDS,
    )
    write_csv(
        EXTRACTED_DIR / "entity_canonical_map.csv",
        canonical_map,
        CANONICAL_FIELDS,
    )
    print("Entity disambiguation completed.")


if __name__ == "__main__":
    main()
