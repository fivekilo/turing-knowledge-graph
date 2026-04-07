#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from __future__ import annotations

import csv
import json
import re
import shutil
from pathlib import Path
from typing import Dict, Iterable, List


ROOT_DIR = Path(__file__).resolve().parents[1]
DATA_DIR = ROOT_DIR / "data"
RAW_DIR = DATA_DIR / "raw"
EXTRACTED_DIR = DATA_DIR / "extracted"
FUSED_DIR = DATA_DIR / "fused"
SCHEMA_DIR = DATA_DIR / "schema"
INSTANCE_DIR = DATA_DIR / "instances"
BACKUP_DIR = DATA_DIR / "manual_backup"

WIKIPEDIA_RAW_DIR = RAW_DIR / "wikipedia"
WIKIDATA_RAW_DIR = RAW_DIR / "wikidata"
SEEDS_PATH = RAW_DIR / "seed_entities.json"


DEFAULT_CLASS_HIERARCHY = [
    {"class_id": "Entity", "label": "实体", "parent_class": "", "description": "知识图谱中的基础实体类型"},
    {"class_id": "Person", "label": "人物", "parent_class": "Entity", "description": "真实世界中的人物"},
    {"class_id": "Organization", "label": "机构", "parent_class": "Entity", "description": "学校机构研究机构等组织"},
    {"class_id": "Place", "label": "地点", "parent_class": "Entity", "description": "城市建筑地理位置等地点"},
    {"class_id": "Work", "label": "作品", "parent_class": "Entity", "description": "论文文章等知识作品"},
    {"class_id": "Concept", "label": "概念", "parent_class": "Entity", "description": "理论概念测试模型等抽象概念"},
    {"class_id": "Machine", "label": "机器", "parent_class": "Entity", "description": "密码机计算机设计等设备或机器"},
    {"class_id": "Event", "label": "事件", "parent_class": "Entity", "description": "具有时间和背景的历史事件或项目"},
    {"class_id": "Field", "label": "学科", "parent_class": "Entity", "description": "知识领域或研究方向"},
    {"class_id": "PublicationVenue", "label": "发表载体", "parent_class": "Entity", "description": "论文发表的期刊或会议"},
]


DEFAULT_RELATION_SCHEMA = {
    "born_in": {"label": "出生于", "domain": "Person", "range": "Place", "property_type": "ObjectProperty", "property_characteristics": "", "description": "人物出生地点"},
    "died_in": {"label": "逝世于", "domain": "Person", "range": "Place", "property_type": "ObjectProperty", "property_characteristics": "", "description": "人物逝世地点"},
    "studied_at": {"label": "就读于", "domain": "Person", "range": "Organization", "property_type": "ObjectProperty", "property_characteristics": "", "description": "人物学习经历所属机构"},
    "worked_at": {"label": "工作于", "domain": "Person", "range": "Organization", "property_type": "ObjectProperty", "property_characteristics": "", "description": "人物工作经历所属机构"},
    "collaborated_with": {"label": "合作于", "domain": "Person", "range": "Person", "property_type": "ObjectProperty", "property_characteristics": "SymmetricProperty", "description": "人物之间的合作关系"},
    "authored": {"label": "撰写", "domain": "Person", "range": "Work", "property_type": "ObjectProperty", "property_characteristics": "", "description": "人物撰写作品"},
    "published_in": {"label": "发表于", "domain": "Work", "range": "PublicationVenue", "property_type": "ObjectProperty", "property_characteristics": "", "description": "作品发表的期刊或载体"},
    "proposed": {"label": "提出", "domain": "Person", "range": "Concept", "property_type": "ObjectProperty", "property_characteristics": "", "description": "人物提出某个理论或概念"},
    "designed": {"label": "设计", "domain": "Person", "range": "Machine", "property_type": "ObjectProperty", "property_characteristics": "", "description": "人物设计的机器或系统"},
    "contributed_to": {"label": "贡献于", "domain": "Person", "range": "Event", "property_type": "ObjectProperty", "property_characteristics": "", "description": "人物参与或贡献的事件项目"},
    "related_to_field": {"label": "相关领域", "domain": "Entity", "range": "Field", "property_type": "ObjectProperty", "property_characteristics": "", "description": "实体与学科方向的关联"},
    "foundation_of": {"label": "奠定基础", "domain": "Concept", "range": "Field", "property_type": "ObjectProperty", "property_characteristics": "", "description": "概念对学科的基础作用"},
    "located_in": {"label": "位于", "domain": "Entity", "range": "Place", "property_type": "ObjectProperty", "property_characteristics": "", "description": "实体所处地点"},
    "occurred_at": {"label": "发生于", "domain": "Event", "range": "Organization", "property_type": "ObjectProperty", "property_characteristics": "", "description": "事件发生的机构或场所"},
    "targeted_machine": {"label": "目标机器", "domain": "Event", "range": "Machine", "property_type": "ObjectProperty", "property_characteristics": "", "description": "事件或项目所针对的机器"},
    "has_birth_date": {"label": "出生日期", "domain": "Person", "range": "Literal", "property_type": "DatatypeProperty", "property_characteristics": "", "description": "人物出生日期"},
    "has_death_date": {"label": "逝世日期", "domain": "Person", "range": "Literal", "property_type": "DatatypeProperty", "property_characteristics": "", "description": "人物逝世日期"},
    "has_year": {"label": "年份", "domain": "Entity", "range": "Literal", "property_type": "DatatypeProperty", "property_characteristics": "", "description": "实体相关年份"},
    "has_nationality": {"label": "国籍", "domain": "Person", "range": "Literal", "property_type": "DatatypeProperty", "property_characteristics": "", "description": "人物国籍"},
    "has_alias": {"label": "别名", "domain": "Entity", "range": "Literal", "property_type": "DatatypeProperty", "property_characteristics": "", "description": "实体的别名或简称"},
}


def ensure_dirs() -> None:
    for path in [RAW_DIR, WIKIPEDIA_RAW_DIR, WIKIDATA_RAW_DIR, EXTRACTED_DIR, FUSED_DIR, BACKUP_DIR]:
        path.mkdir(parents=True, exist_ok=True)


def slugify(text: str) -> str:
    value = text.strip().lower().replace("’", "'").replace("–", "-")
    value = re.sub(r"[^a-z0-9\u4e00-\u9fff]+", "_", value)
    value = re.sub(r"_+", "_", value).strip("_")
    return value or "unknown"


def write_csv(path: Path, rows: List[Dict[str, str]], fieldnames: List[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8-sig", newline="") as file:
        writer = csv.DictWriter(file, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow(row)


def read_csv(path: Path) -> List[Dict[str, str]]:
    if not path.exists():
        return []
    with path.open("r", encoding="utf-8-sig", newline="") as file:
        return list(csv.DictReader(file))


def write_json(path: Path, data: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")


def read_json(path: Path) -> object:
    return json.loads(path.read_text(encoding="utf-8"))


def load_seeds() -> List[Dict[str, str]]:
    return list(read_json(SEEDS_PATH))  # type: ignore[arg-type]


def unique_rows(rows: Iterable[Dict[str, str]], keys: List[str]) -> List[Dict[str, str]]:
    seen = set()
    result = []
    for row in rows:
        signature = tuple(row.get(key, "") for key in keys)
        if signature in seen:
            continue
        seen.add(signature)
        result.append(row)
    return result


def parent_chain(class_id: str) -> List[str]:
    mapping = {item["class_id"]: item for item in DEFAULT_CLASS_HIERARCHY}
    chain = []
    current = class_id
    while current and current in mapping and current not in chain:
        chain.append(current)
        current = mapping[current]["parent_class"]
    return list(reversed(chain))


def backup_active_csvs() -> None:
    ensure_dirs()
    for source in [
        SCHEMA_DIR / "classes.csv",
        SCHEMA_DIR / "relations.csv",
        INSTANCE_DIR / "entities.csv",
        INSTANCE_DIR / "triples.csv",
    ]:
        if source.exists():
            shutil.copy2(source, BACKUP_DIR / source.name)
