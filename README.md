# 图灵知识图谱 | Turing Knowledge Graph

这是一个围绕图灵主题构建的课程作业项目。当前版本已经形成一条可重复执行的自动化知识图谱流水线，支持从 `Wikipedia + Wikidata` 获取原始数据，抽取实体和关系，完成归一化与融合，并导出为多种图谱格式。

## 主干流程

项目当前保留的工作主干是：

```text
raw -> extracted -> entity_disambiguation -> fused -> schema / instances -> exports
```

- `raw`
  - 原始来源数据
  - 包括 Wikipedia `summary.json`、`page.html` 和 Wikidata JSON
- `extracted`
  - 抽取阶段中间结果
  - 包括文本块、NER mentions、实体候选、关系候选
- `entity_disambiguation`
  - 实体消歧阶段
  - 对正文 mention 做候选发现、局部链接和 NIL 保留，并生成规范实体映射
- `fused`
  - 融合后的中间结果
  - 用于检查自动化融合质量
- `schema / instances`
  - 当前生效的知识图谱模式层和实例层
- `exports`
  - 面向展示和交换的导出结果

## 目录结构

```text
turing-knowledge-graph/
├─ data/
│  ├─ raw/
│  │  ├─ seed_entities.json
│  │  ├─ wikipedia/
│  │  └─ wikidata/
│  ├─ extracted/
│  │  ├─ text_blocks.csv
│  │  ├─ ner_mentions.csv
│  │  ├─ entities_candidates*.csv
│  │  └─ triples_candidates*.csv
│  ├─ fused/
│  │  ├─ classes_fused.csv
│  │  ├─ relations_fused.csv
│  │  ├─ entities_fused.csv
│  │  ├─ triples_fused.csv
│  │  └─ fusion_summary.json
│  ├─ schema/
│  │  ├─ classes.csv
│  │  └─ relations.csv
│  ├─ instances/
│  │  ├─ entities.csv
│  │  └─ triples.csv
│  └─ exports/
│     ├─ nodes.csv
│     ├─ edges.csv
│     ├─ turing_kg.ttl
│     ├─ turing_kg.rdf
│     ├─ turing_kg.graphml
│     ├─ turing_kg.png
│     └─ summary.json
├─ src/
│  ├─ auto_pipeline_utils.py
│  ├─ fetch_sources.py
│  ├─ extract_candidates.py
│  ├─ extract_text_blocks.py
│  ├─ ner_candidates.py
│  ├─ entity_disambiguation.py
│  ├─ merge_entity_candidates.py
│  ├─ text_relation_candidates.py
│  ├─ pretrained_relation_candidates.py
│  ├─ merge_triple_candidates.py
│  ├─ normalize_candidates.py
│  ├─ fuse_knowledge.py
│  ├─ kg_builder.py
│  └─ run_auto_pipeline.py
├─ requirements.txt
└─ README.md
```

## 核心脚本

- `fetch_sources.py`
  - 按种子实体抓取 Wikipedia 和 Wikidata 原始数据
- `extract_candidates.py`
  - 从信息框和 Wikidata claims 抽取结构化实体与关系
- `extract_text_blocks.py`
  - 从 Wikipedia 摘要和正文导语提取文本块
- `ner_candidates.py`
  - 使用 `spaCy en_core_web_sm` 做正文实体识别
- `entity_disambiguation.py`
  - 根据别名、表层相似度、类型和上下文做候选实体链接，并保留 NIL 实体
- `text_relation_candidates.py`
  - 基于规则从正文句子中抽取一部分关系
- `pretrained_relation_candidates.py`
  - 使用预训练 TACRED 关系分类模型对正文句子里的实体对做监督式关系抽取
- `normalize_candidates.py`
  - 对候选实体、类和关系做归一化统计
- `fuse_knowledge.py`
  - 生成最终 `schema` 和 `instances`
- `kg_builder.py`
  - 校验并导出知识图谱
- `run_auto_pipeline.py`
  - 一键运行完整流水线

## 当前实现对应课程内容

- 第 2 章
  - 三元组表示
  - RDF / RDFS / OWL 风格导出
- 第 3 章
  - `schema` 与 `instances` 分层
  - 自动抽取到融合的流程
- 第 4 章
  - 接入现成 NER 模型做正文实体识别
  - 同时保留规则法和预训练监督模型两条正文关系抽取链路
- 第 5 章
  - 保留 `source`、`confidence` 和规范化实体 ID

## 依赖安装

```bash
pip install -r requirements.txt
python -m spacy download en_core_web_sm
```

## 运行方式

仅重建导出结果：

```bash
python src/kg_builder.py
```

运行完整自动化流水线：

```bash
python src/run_auto_pipeline.py
```

## 主要输出文件

- `data/instances/entities.csv`
- `data/instances/triples.csv`
- `data/exports/turing_kg.png`
- `data/exports/turing_kg.ttl`
- `data/exports/turing_kg.rdf`
- `data/exports/summary.json`

## 说明

- 当前 `confidence` 是按来源和规则强弱设置的启发式分数，不是模型概率
- 当前正文抽取主要覆盖 Wikipedia 摘要和导语段落，还没有扩展到全文
- 当前保留了 `raw / extracted / fused / exports` 四层，便于课程汇报时展示完整流程
