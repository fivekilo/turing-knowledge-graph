#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from __future__ import annotations

from collections import defaultdict
import re
from typing import Dict, Iterable, List, Optional, Tuple

import torch
from pie_documents.annotations import LabeledSpan
from pie_documents.documents import TextDocumentWithLabeledSpansAndBinaryRelations
from pytorch_ie.models.transformer_text_classification import TransformerTextClassificationModel
from pytorch_ie.taskmodules.transformer_re_text_classification import (
    TransformerRETextClassificationTaskModule,
)

import pytorch_ie.models  # noqa: F401
import pytorch_ie.taskmodules  # noqa: F401

from auto_pipeline_utils import EXTRACTED_DIR, ensure_dirs, load_seeds, read_csv, unique_rows, write_csv


REPO_ID = "pie/example-re-textclf-tacred"

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

PREDICTION_FIELDS = [
    "seed_entity_id",
    "block_id",
    "original_sentence",
    "sentence",
    "rewritten",
    "head_entity_id",
    "head_label",
    "head_class_id",
    "tail_entity_id",
    "tail_label",
    "tail_class_id",
    "model_relation",
    "relation_confidence",
    "margin",
    "mapped_predicate",
    "status",
]

CLASS_TO_TACRED = {
    "Person": "PERSON",
    "Organization": "ORGANIZATION",
    "Place": "LOCATION",
    "Event": "MISC",
    "Work": "MISC",
    "Concept": "MISC",
    "Machine": "MISC",
    "Field": "MISC",
}

RELATION_TO_SCHEMA = {
    "per:schools_attended": ("studied_at", "Person", "Organization"),
    "per:employee_of": ("worked_at", "Person", "Organization"),
    "per:city_of_birth": ("born_in", "Person", "Place"),
    "per:stateorprovince_of_birth": ("born_in", "Person", "Place"),
    "per:country_of_birth": ("born_in", "Person", "Place"),
    "per:city_of_death": ("died_in", "Person", "Place"),
    "per:stateorprovince_of_death": ("died_in", "Person", "Place"),
    "per:country_of_death": ("died_in", "Person", "Place"),
    "org:city_of_headquarters": ("located_in", "Organization", "Place"),
    "org:stateorprovince_of_headquarters": ("located_in", "Organization", "Place"),
    "org:country_of_headquarters": ("located_in", "Organization", "Place"),
    "org:top_members/employees": ("worked_at", "Person", "Organization"),
}

RELATION_THRESHOLDS = {
    "per:schools_attended": 0.80,
    "per:employee_of": 0.78,
    "per:city_of_birth": 0.80,
    "per:stateorprovince_of_birth": 0.80,
    "per:country_of_birth": 0.80,
    "per:city_of_death": 0.80,
    "per:stateorprovince_of_death": 0.80,
    "per:country_of_death": 0.80,
    "org:city_of_headquarters": 0.75,
    "org:stateorprovince_of_headquarters": 0.75,
    "org:country_of_headquarters": 0.75,
    "org:top_members/employees": 0.72,
}


def load_linked_mentions() -> List[Dict[str, str]]:
    mention_rows = read_csv(EXTRACTED_DIR / "ner_mentions.csv")
    decision_rows = read_csv(EXTRACTED_DIR / "entity_linking_decisions.csv")
    if len(mention_rows) != len(decision_rows):
        raise RuntimeError("NER mentions and entity linking decisions are out of sync.")

    linked_mentions: List[Dict[str, str]] = []
    for mention, decision in zip(mention_rows, decision_rows):
        linked_mentions.append(
            {
                "seed_entity_id": mention["seed_entity_id"],
                "block_id": mention["block_id"],
                "context": mention["context"],
                "mention": mention["mention"],
                "start": mention["start"],
                "end": mention["end"],
                "resolved_entity_id": decision["resolved_entity_id"],
                "resolved_label": decision["resolved_label"],
                "class_id_hint": decision["class_id_hint"],
                "status": decision["status"],
            }
        )
    return linked_mentions


def group_mentions(rows: Iterable[Dict[str, str]]) -> Dict[Tuple[str, str, str], List[Dict[str, str]]]:
    grouped: Dict[Tuple[str, str, str], List[Dict[str, str]]] = defaultdict(list)
    for row in rows:
        key = (row["seed_entity_id"], row["block_id"], row["context"])
        grouped[key].append(row)
    return grouped


def sentence_has_seed_mention(seed_entity_id: str, mentions: List[Dict[str, str]]) -> bool:
    return any(item["resolved_entity_id"] == seed_entity_id for item in mentions)


def rewrite_sentence_with_seed(
    seed: Dict[str, str],
    sentence: str,
    mentions: List[Dict[str, str]],
) -> Tuple[str, List[Dict[str, str]], bool]:
    if sentence_has_seed_mention(seed["entity_id"], mentions):
        return sentence, mentions, False

    updated_mentions = [dict(item) for item in mentions]
    seed_label = seed["label"]
    seed_class = seed["class_id"]
    replacements: List[Tuple[int, int, str]] = []

    def add_matches(pattern: str, replacement: str) -> None:
        for match in re.finditer(pattern, sentence):
            replacements.append((match.start(), match.end(), replacement))

    if seed_class == "Person":
        add_matches(r"\b[Hh]e\b", seed_label)
        add_matches(r"\b[Hh]is\b", f"{seed_label}'s")
        add_matches(r"\b[Ss]he\b", seed_label)
        add_matches(r"\b[Hh]er\b", f"{seed_label}'s")

        surname = seed_label.split()[-1]
        surname_pattern = re.compile(rf"(?<!née )\b{re.escape(surname)}\b")
        for match in surname_pattern.finditer(sentence):
            replacements.append((match.start(), match.end(), seed_label))
    elif seed_class in {"Organization", "Concept", "Machine", "Work"}:
        add_matches(r"\b[Ii]t\b", seed_label)
        add_matches(r"\b[Ii]ts\b", f"{seed_label}'s")

    if not replacements:
        return sentence, updated_mentions, False

    replacements = sorted(replacements, key=lambda item: (item[0], item[1]))
    filtered_replacements: List[Tuple[int, int, str]] = []
    cursor = -1
    for start, end, replacement in replacements:
        if start < cursor:
            continue
        filtered_replacements.append((start, end, replacement))
        cursor = end

    rewritten = sentence
    inserted_seed_mentions: List[Tuple[int, int]] = []
    running_delta = 0
    for start, end, replacement in filtered_replacements:
        adj_start = start + running_delta
        adj_end = end + running_delta
        rewritten = rewritten[:adj_start] + replacement + rewritten[adj_end:]
        new_end = adj_start + len(replacement)
        inserted_seed_mentions.append((adj_start, new_end))
        delta = len(replacement) - (end - start)
        running_delta += delta

        for item in updated_mentions:
            item_start = int(item["start"])
            item_end = int(item["end"])
            if item_end <= start:
                continue
            if item_start >= end:
                item["start"] = str(item_start + delta)
                item["end"] = str(item_end + delta)
                continue
            item["drop_after_rewrite"] = "1"

    shifted_mentions = [item for item in updated_mentions if item.get("drop_after_rewrite") != "1"]
    for start, end in inserted_seed_mentions:
        shifted_mentions.append(
            {
                "seed_entity_id": seed["entity_id"],
                "block_id": updated_mentions[0]["block_id"] if updated_mentions else "",
                "context": rewritten,
                "mention": rewritten[start:end],
                "start": str(start),
                "end": str(end),
                "resolved_entity_id": seed["entity_id"],
                "resolved_label": seed_label,
                "class_id_hint": seed_class,
                "status": "seed_rewrite",
            }
        )
    return rewritten, shifted_mentions, True


def build_document(sentence: str, mentions: List[Dict[str, str]]) -> Tuple[TextDocumentWithLabeledSpansAndBinaryRelations, Dict[Tuple[int, int], Dict[str, str]]]:
    doc = TextDocumentWithLabeledSpansAndBinaryRelations(text=sentence)
    span_map: Dict[Tuple[int, int], Dict[str, str]] = {}
    seen = set()
    for item in sorted(mentions, key=lambda row: (int(row["start"]), int(row["end"]))):
        class_id = item["class_id_hint"]
        model_label = CLASS_TO_TACRED.get(class_id)
        if not model_label:
            continue
        start = int(item["start"])
        end = int(item["end"])
        signature = (start, end, model_label)
        if signature in seen:
            continue
        seen.add(signature)
        doc.labeled_spans.append(LabeledSpan(start=start, end=end, label=model_label))
        span_map[(start, end)] = item
    return doc, span_map


def choose_subject_object(
    relation_label: str,
    head: Dict[str, str],
    tail: Dict[str, str],
) -> Optional[Tuple[str, Dict[str, str], Dict[str, str]]]:
    mapping = RELATION_TO_SCHEMA.get(relation_label)
    if not mapping:
        return None
    predicate, subject_class, object_class = mapping

    if head["class_id_hint"] == subject_class and tail["class_id_hint"] == object_class:
        return predicate, head, tail
    if tail["class_id_hint"] == subject_class and head["class_id_hint"] == object_class:
        return predicate, tail, head
    return None


def load_model_components() -> Tuple[TransformerTextClassificationModel, TransformerRETextClassificationTaskModule]:
    model = TransformerTextClassificationModel.from_pretrained(REPO_ID)
    model.eval()
    taskmodule = TransformerRETextClassificationTaskModule.from_pretrained(
        REPO_ID,
        create_relation_candidates=True,
        relation_annotation="binary_relations",
    )
    return model, taskmodule


def predict_for_document(
    model: TransformerTextClassificationModel,
    taskmodule: TransformerRETextClassificationTaskModule,
    seed: Dict[str, str],
    seed_entity_id: str,
    block_id: str,
    original_sentence: str,
    sentence: str,
    doc: TextDocumentWithLabeledSpansAndBinaryRelations,
    span_map: Dict[Tuple[int, int], Dict[str, str]],
    rewritten: bool,
) -> Tuple[List[Dict[str, str]], List[Dict[str, str]]]:
    encodings = taskmodule.encode([doc])
    if not encodings:
        return [], []

    batch_inputs = taskmodule.collate(encodings)[0]
    with torch.no_grad():
        outputs = model(batch_inputs)
        probabilities = torch.softmax(outputs["logits"], dim=-1)

    prediction_rows: List[Dict[str, str]] = []
    triples: List[Dict[str, str]] = []
    for idx, encoding in enumerate(encodings):
        candidate = encoding.metadata["candidate_annotation"]
        head = span_map.get((candidate.head.start, candidate.head.end))
        tail = span_map.get((candidate.tail.start, candidate.tail.end))
        if head is None or tail is None:
            continue
        if head["resolved_entity_id"] != seed_entity_id and tail["resolved_entity_id"] != seed_entity_id:
            continue
        if head["resolved_entity_id"] == tail["resolved_entity_id"]:
            continue

        probs = probabilities[idx]
        values, indices = probs.topk(2)
        best_score = float(values[0].item())
        second_score = float(values[1].item()) if len(values) > 1 else 0.0
        best_label = taskmodule.id_to_label[int(indices[0].item())]
        margin = best_score - second_score

        mapped = choose_subject_object(best_label, head, tail)
        threshold = RELATION_THRESHOLDS.get(best_label, 0.80)
        status = "dropped_unmapped"
        mapped_predicate = ""
        if best_label == "no_relation":
            status = "dropped_no_relation"
        elif mapped is None:
            status = "dropped_type_mismatch"
        elif best_score < threshold or margin < 0.08:
            status = "dropped_low_confidence"
            mapped_predicate = mapped[0]
        else:
            predicate, subject, obj = mapped
            mapped_predicate = predicate
            status = "accepted"
            triples.append(
                {
                    "subject": subject["resolved_entity_id"],
                    "predicate": predicate,
                    "object": obj["resolved_entity_id"],
                    "object_label": obj["resolved_label"],
                    "object_type": "entity",
                    "object_datatype": "",
                    "object_class_hint": obj["class_id_hint"],
                    "source": "wikipedia_text_re_model",
                    "confidence": f"{best_score:.2f}",
                }
            )

        prediction_rows.append(
            {
                "seed_entity_id": seed_entity_id,
                "block_id": block_id,
                "original_sentence": original_sentence,
                "sentence": sentence,
                "rewritten": "yes" if rewritten else "no",
                "head_entity_id": head["resolved_entity_id"],
                "head_label": head["resolved_label"],
                "head_class_id": head["class_id_hint"],
                "tail_entity_id": tail["resolved_entity_id"],
                "tail_label": tail["resolved_label"],
                "tail_class_id": tail["class_id_hint"],
                "model_relation": best_label,
                "relation_confidence": f"{best_score:.4f}",
                "margin": f"{margin:.4f}",
                "mapped_predicate": mapped_predicate,
                "status": status,
            }
        )

    return prediction_rows, triples


def main() -> None:
    ensure_dirs()
    linked_mentions = load_linked_mentions()
    grouped_mentions = group_mentions(linked_mentions)
    seed_map = {item["entity_id"]: item for item in load_seeds()}
    model, taskmodule = load_model_components()

    prediction_rows: List[Dict[str, str]] = []
    triples: List[Dict[str, str]] = []
    for (seed_entity_id, block_id, sentence), mentions in grouped_mentions.items():
        seed = seed_map.get(seed_entity_id)
        if not seed:
            continue
        rewritten_sentence, rewritten_mentions, rewritten = rewrite_sentence_with_seed(seed, sentence, mentions)
        doc, span_map = build_document(rewritten_sentence, rewritten_mentions)
        if len(span_map) < 2:
            continue
        current_predictions, current_triples = predict_for_document(
            model=model,
            taskmodule=taskmodule,
            seed=seed,
            seed_entity_id=seed_entity_id,
            block_id=block_id,
            original_sentence=sentence,
            sentence=rewritten_sentence,
            doc=doc,
            span_map=span_map,
            rewritten=rewritten,
        )
        prediction_rows.extend(current_predictions)
        triples.extend(current_triples)

    write_csv(
        EXTRACTED_DIR / "relation_predictions_model.csv",
        prediction_rows,
        PREDICTION_FIELDS,
    )
    write_csv(
        EXTRACTED_DIR / "triples_candidates_text_model.csv",
        unique_rows(triples, ["subject", "predicate", "object", "source"]),
        TRIPLE_FIELDS,
    )
    print("Pretrained relation extraction completed.")


if __name__ == "__main__":
    main()
