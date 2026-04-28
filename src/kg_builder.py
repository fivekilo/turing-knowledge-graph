#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Turing Knowledge Graph Builder

按当前课程前 5 章的内容实现一个可扩展的知识图谱工程：
1. 模式层：类层级、关系约束（本体 / schema）
2. 实例层：实体与三元组
3. 验证层：检查 relation 的 domain / range
4. 导出层：CSV、RDF/Turtle、GraphML、可视化图片
"""

from __future__ import annotations

import csv
import json
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List

import matplotlib.pyplot as plt
import networkx as nx
from rdflib import Graph, Literal, Namespace, OWL, RDF, RDFS
from rdflib.namespace import XSD


ROOT_DIR = Path(__file__).resolve().parents[1]
DATA_DIR = ROOT_DIR / "data"
SCHEMA_DIR = DATA_DIR / "schema"
INSTANCE_DIR = DATA_DIR / "instances"
EXPORT_DIR = DATA_DIR / "exports"


@dataclass
class ClassDef:
    class_id: str
    label: str
    parent_class: str
    description: str


@dataclass
class RelationDef:
    relation_id: str
    label: str
    domain: str
    range: str
    property_type: str
    property_characteristics: str
    description: str


@dataclass
class EntityDef:
    entity_id: str
    label: str
    class_id: str
    description: str
    source: str


@dataclass
class TripleDef:
    subject: str
    predicate: str
    object_value: str
    object_type: str
    object_datatype: str
    source: str
    confidence: str


class TuringKnowledgeGraphBuilder:
    def __init__(self) -> None:
        self.class_defs: Dict[str, ClassDef] = {}
        self.relation_defs: Dict[str, RelationDef] = {}
        self.entity_defs: Dict[str, EntityDef] = {}
        self.triples: List[TripleDef] = []
        self.graph = nx.DiGraph()

    def load(self) -> None:
        self.class_defs = {
            item.class_id: item for item in self._read_classes(SCHEMA_DIR / "classes.csv")
        }
        self.relation_defs = {
            item.relation_id: item
            for item in self._read_relations(SCHEMA_DIR / "relations.csv")
        }
        self.entity_defs = {
            item.entity_id: item
            for item in self._read_entities(INSTANCE_DIR / "entities.csv")
        }
        self.triples = self._read_triples(INSTANCE_DIR / "triples.csv")

    def validate(self) -> None:
        for class_id, class_def in self.class_defs.items():
            if class_def.parent_class and class_def.parent_class not in self.class_defs:
                raise ValueError(
                    f"Class '{class_id}' references unknown parent_class '{class_def.parent_class}'."
                )

        for entity_id, entity in self.entity_defs.items():
            if entity.class_id not in self.class_defs:
                raise ValueError(
                    f"Entity '{entity_id}' references unknown class '{entity.class_id}'."
                )

        for relation_id, relation in self.relation_defs.items():
            if relation.domain not in self.class_defs:
                raise ValueError(
                    f"Relation '{relation_id}' references unknown domain '{relation.domain}'."
                )
            if relation.range != "Literal" and relation.range not in self.class_defs:
                raise ValueError(
                    f"Relation '{relation_id}' references unknown range '{relation.range}'."
                )
            if relation.property_type not in {"ObjectProperty", "DatatypeProperty"}:
                raise ValueError(
                    f"Relation '{relation_id}' must use property_type ObjectProperty or DatatypeProperty."
                )
            if relation.range == "Literal" and relation.property_type != "DatatypeProperty":
                raise ValueError(
                    f"Relation '{relation_id}' with Literal range must be DatatypeProperty."
                )
            if relation.range != "Literal" and relation.property_type != "ObjectProperty":
                raise ValueError(
                    f"Relation '{relation_id}' with entity range must be ObjectProperty."
                )

        for triple in self.triples:
            if triple.subject not in self.entity_defs:
                raise ValueError(f"Triple subject '{triple.subject}' is not defined.")
            if triple.predicate not in self.relation_defs:
                raise ValueError(f"Triple predicate '{triple.predicate}' is not defined.")

            relation = self.relation_defs[triple.predicate]
            subject_class = self.entity_defs[triple.subject].class_id
            if not self._is_instance_of(subject_class, relation.domain):
                raise ValueError(
                    f"Subject '{triple.subject}' with class '{subject_class}' "
                    f"does not satisfy relation domain '{relation.domain}'."
                )

            if triple.object_type == "entity":
                if triple.object_value not in self.entity_defs:
                    raise ValueError(f"Triple object '{triple.object_value}' is not defined.")
                if relation.range == "Literal":
                    raise ValueError(
                        f"Relation '{triple.predicate}' expects literal, but got entity."
                    )
                object_class = self.entity_defs[triple.object_value].class_id
                if not self._is_instance_of(object_class, relation.range):
                    raise ValueError(
                        f"Object '{triple.object_value}' with class '{object_class}' "
                        f"does not satisfy relation range '{relation.range}'."
                    )
            elif triple.object_type == "literal":
                if relation.range != "Literal":
                    raise ValueError(
                        f"Relation '{triple.predicate}' expects entity range '{relation.range}', "
                        f"but got literal."
                    )
            else:
                raise ValueError(f"Unknown object_type '{triple.object_type}'.")

    def build(self) -> None:
        self.graph.clear()
        for entity in self.entity_defs.values():
            self.graph.add_node(
                entity.entity_id,
                label=entity.label,
                class_id=entity.class_id,
                description=entity.description,
                source=entity.source,
            )

        for triple in self.triples:
            relation = self.relation_defs[triple.predicate]
            if triple.object_type == "entity":
                self.graph.add_edge(
                    triple.subject,
                    triple.object_value,
                    predicate=triple.predicate,
                    predicate_label=relation.label,
                    source=triple.source,
                    confidence=triple.confidence,
                )
            else:
                literal_key = f"{triple.predicate}__values"
                values = self.graph.nodes[triple.subject].setdefault(literal_key, [])
                values.append(
                    {
                        "value": triple.object_value,
                        "datatype": triple.object_datatype,
                        "source": triple.source,
                        "confidence": triple.confidence,
                    }
                )

    def export(self) -> Dict[str, Path]:
        EXPORT_DIR.mkdir(parents=True, exist_ok=True)
        nodes_path = EXPORT_DIR / "nodes.csv"
        edges_path = EXPORT_DIR / "edges.csv"
        ttl_path = EXPORT_DIR / "turing_kg.ttl"
        rdf_path = EXPORT_DIR / "turing_kg.rdf"
        graphml_path = EXPORT_DIR / "turing_kg.graphml"
        summary_path = EXPORT_DIR / "summary.json"

        self._export_nodes_csv(nodes_path)
        self._export_edges_csv(edges_path)
        self._export_rdf(ttl_path, rdf_path)
        nx.write_graphml(self._graphml_safe_graph(), graphml_path)
        self._export_summary(summary_path)

        return {
            "nodes_csv": nodes_path,
            "edges_csv": edges_path,
            "ttl": ttl_path,
            "rdf": rdf_path,
            "graphml": graphml_path,
            "summary": summary_path,
        }

    def _graphml_safe_graph(self) -> nx.DiGraph:
        safe_graph = nx.DiGraph()
        for node_id, attrs in self.graph.nodes(data=True):
            safe_attrs = {}
            for key, value in attrs.items():
                if isinstance(value, (list, dict)):
                    safe_attrs[key] = json.dumps(value, ensure_ascii=False)
                else:
                    safe_attrs[key] = value
            safe_graph.add_node(node_id, **safe_attrs)

        for source, target, attrs in self.graph.edges(data=True):
            safe_attrs = {}
            for key, value in attrs.items():
                if isinstance(value, (list, dict)):
                    safe_attrs[key] = json.dumps(value, ensure_ascii=False)
                else:
                    safe_attrs[key] = value
            safe_graph.add_edge(source, target, **safe_attrs)
        return safe_graph

    def _read_classes(self, path: Path) -> List[ClassDef]:
        return [
            ClassDef(
                class_id=row["class_id"].strip(),
                label=row["label"].strip(),
                parent_class=row["parent_class"].strip(),
                description=row["description"].strip(),
            )
            for row in self._read_csv(path)
        ]

    def _read_relations(self, path: Path) -> List[RelationDef]:
        return [
            RelationDef(
                relation_id=row["relation_id"].strip(),
                label=row["label"].strip(),
                domain=row["domain"].strip(),
                range=row["range"].strip(),
                property_type=row["property_type"].strip(),
                property_characteristics=row["property_characteristics"].strip(),
                description=row["description"].strip(),
            )
            for row in self._read_csv(path)
        ]

    def _read_entities(self, path: Path) -> List[EntityDef]:
        return [
            EntityDef(
                entity_id=row["entity_id"].strip(),
                label=row["label"].strip(),
                class_id=row["class_id"].strip(),
                description=row["description"].strip(),
                source=row["source"].strip(),
            )
            for row in self._read_csv(path)
        ]

    def _read_triples(self, path: Path) -> List[TripleDef]:
        return [
            TripleDef(
                subject=row["subject"].strip(),
                predicate=row["predicate"].strip(),
                object_value=row["object"].strip(),
                object_type=row["object_type"].strip(),
                object_datatype=row["object_datatype"].strip(),
                source=row["source"].strip(),
                confidence=row["confidence"].strip(),
            )
            for row in self._read_csv(path)
        ]

    def _read_csv(self, path: Path) -> List[Dict[str, str]]:
        with path.open("r", encoding="utf-8-sig", newline="") as file:
            return list(csv.DictReader(file))

    def _is_instance_of(self, class_id: str, target_class: str) -> bool:
        current = class_id
        while current:
            if current == target_class:
                return True
            current = self.class_defs[current].parent_class
        return False

    def _export_nodes_csv(self, path: Path) -> None:
        with path.open("w", encoding="utf-8-sig", newline="") as file:
            writer = csv.DictWriter(
                file,
                fieldnames=["id", "label", "class_id", "description", "source"],
            )
            writer.writeheader()
            for entity in self.entity_defs.values():
                writer.writerow(
                    {
                        "id": entity.entity_id,
                        "label": entity.label,
                        "class_id": entity.class_id,
                        "description": entity.description,
                        "source": entity.source,
                    }
                )

    def _export_edges_csv(self, path: Path) -> None:
        with path.open("w", encoding="utf-8-sig", newline="") as file:
            writer = csv.DictWriter(
                file,
                fieldnames=[
                    "subject",
                    "predicate",
                    "predicate_label",
                    "object",
                    "object_type",
                    "object_datatype",
                    "source",
                    "confidence",
                ],
            )
            writer.writeheader()
            for triple in self.triples:
                writer.writerow(
                    {
                        "subject": triple.subject,
                        "predicate": triple.predicate,
                        "predicate_label": self.relation_defs[triple.predicate].label,
                        "object": triple.object_value,
                        "object_type": triple.object_type,
                        "object_datatype": triple.object_datatype,
                        "source": triple.source,
                        "confidence": triple.confidence,
                    }
                )

    def _export_rdf(self, ttl_path: Path, rdf_path: Path) -> None:
        graph = Graph()
        ex = Namespace("http://example.org/turing/")
        graph.bind("ex", ex)
        graph.bind("owl", OWL)
        graph.bind("rdfs", RDFS)

        for class_def in self.class_defs.values():
            class_uri = ex[class_def.class_id]
            graph.add((class_uri, RDF.type, RDFS.Class))
            graph.add((class_uri, RDF.type, OWL.Class))
            graph.add((class_uri, RDFS.label, Literal(class_def.label, lang="zh")))
            if class_def.description:
                graph.add((class_uri, RDFS.comment, Literal(class_def.description, lang="zh")))
            if class_def.parent_class:
                graph.add((class_uri, RDFS.subClassOf, ex[class_def.parent_class]))

        for relation in self.relation_defs.values():
            relation_uri = ex[relation.relation_id]
            graph.add((relation_uri, RDF.type, RDF.Property))
            graph.add(
                (
                    relation_uri,
                    RDF.type,
                    OWL.ObjectProperty if relation.property_type == "ObjectProperty" else OWL.DatatypeProperty,
                )
            )
            graph.add((relation_uri, RDFS.label, Literal(relation.label, lang="zh")))
            graph.add((relation_uri, RDFS.domain, ex[relation.domain]))
            graph.add(
                (relation_uri, RDFS.range, RDFS.Literal if relation.range == "Literal" else ex[relation.range])
            )
            if relation.property_characteristics:
                for characteristic in relation.property_characteristics.split("|"):
                    characteristic = characteristic.strip()
                    if characteristic:
                        graph.add((relation_uri, RDF.type, OWL[characteristic]))
            if relation.description:
                graph.add((relation_uri, RDFS.comment, Literal(relation.description, lang="zh")))

        for entity in self.entity_defs.values():
            entity_uri = ex[entity.entity_id]
            graph.add((entity_uri, RDF.type, ex[entity.class_id]))
            graph.add((entity_uri, RDFS.label, Literal(entity.label)))
            if entity.description:
                graph.add((entity_uri, RDFS.comment, Literal(entity.description, lang="zh")))

        for triple in self.triples:
            subject_uri = ex[triple.subject]
            predicate_uri = ex[triple.predicate]
            if triple.object_type == "entity":
                obj = ex[triple.object_value]
            else:
                obj = self._literal_from_value(triple.object_value, triple.object_datatype)
            graph.add((subject_uri, predicate_uri, obj))

        graph.serialize(ttl_path, format="turtle")
        graph.serialize(rdf_path, format="xml")

    def _literal_from_value(self, value: str, datatype: str) -> Literal:
        datatype_map = {"date": XSD.date, "gYear": XSD.gYear, "string": XSD.string}
        return Literal(value, datatype=datatype_map.get(datatype, XSD.string))

    def _visualize(self, output_path: Path) -> None:
        plt.figure(figsize=(18, 12))
        color_map = {
            "Person": "#ff6b6b",
            "Organization": "#4ecdc4",
            "Place": "#ffd166",
            "Work": "#5c7cfa",
            "Concept": "#74c0fc",
            "Machine": "#8ce99a",
            "Event": "#f783ac",
            "Field": "#9775fa",
            "PublicationVenue": "#adb5bd",
        }

        labels = {}
        node_colors = []
        for node_id, attrs in self.graph.nodes(data=True):
            labels[node_id] = attrs.get("label", node_id)
            node_colors.append(color_map.get(attrs.get("class_id", "Entity"), "#ced4da"))

        pos = nx.spring_layout(self.graph, seed=42, k=1.4)
        nx.draw_networkx_nodes(self.graph, pos, node_color=node_colors, node_size=2200, alpha=0.92)
        nx.draw_networkx_labels(self.graph, pos, labels=labels, font_size=9)
        nx.draw_networkx_edges(
            self.graph,
            pos,
            arrows=True,
            arrowsize=18,
            width=1.6,
            edge_color="#666666",
            alpha=0.7,
        )
        plt.title("Turing Knowledge Graph", fontsize=18, fontweight="bold")
        plt.axis("off")
        plt.tight_layout()
        plt.savefig(output_path, dpi=300, bbox_inches="tight")
        plt.close()

    def _export_summary(self, path: Path) -> None:
        entity_distribution: Dict[str, int] = {}
        relation_distribution: Dict[str, int] = {}
        for entity in self.entity_defs.values():
            entity_distribution[entity.class_id] = entity_distribution.get(entity.class_id, 0) + 1
        for triple in self.triples:
            relation_distribution[triple.predicate] = relation_distribution.get(triple.predicate, 0) + 1

        summary = {
            "classes": len(self.class_defs),
            "relations": len(self.relation_defs),
            "entities": len(self.entity_defs),
            "triples": len(self.triples),
            "entity_distribution": entity_distribution,
            "relation_distribution": relation_distribution,
        }
        path.write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")


def main() -> None:
    builder = TuringKnowledgeGraphBuilder()
    builder.load()
    builder.validate()
    builder.build()
    outputs = builder.export()

    print("Knowledge graph build completed.")
    for name, path in outputs.items():
        print(f"- {name}: {path}")


if __name__ == "__main__":
    main()
