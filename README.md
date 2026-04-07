# 图灵知识图谱 | Turing Knowledge Graph

这是一个面向课程作业的可扩展知识图谱项目，主题是“图灵”。当前版本严格按照前 5 章已经讲授的内容来组织架构，不追求一次做成大而全，而是优先保证：

- 现在就能构建出一个结构清晰、可展示的知识图谱
- 后续课程继续讲到抽取、融合、消歧时，可以直接在当前工程上扩展

## 当前设计思路

项目采用“模式层 + 实例层 + 导出层”的结构。

- 模式层：定义类层级和关系约束，对应课程中的本体、分类体系、关系定义、`Domain` / `Range`
- 实例层：存放图灵相关实体和三元组，对应课程中的实体、关系、知识表示
- 导出层：把图数据导出为 `CSV`、`RDF/Turtle`、`GraphML` 和图片，方便展示和后续接入 Neo4j、Protégé 等工具

这个架构和前 5 章课程内容是对应的：

- 第 1 章：明确知识图谱对象、范围和主题
- 第 2 章：使用三元组、本体、RDF 来表示知识
- 第 3 章：把 schema 和 instance 分开，方便后续融合与扩展
- 第 4 章：预留 `data/raw/` 目录，便于后续接实体抽取结果
- 第 5 章：实体采用规范 ID，并保留 `alias`、`source`、`confidence`，便于后续做消歧与链接

## 项目结构

```text
turing-knowledge-graph/
├─ data/
│  ├─ schema/
│  │  ├─ classes.csv
│  │  └─ relations.csv
│  ├─ instances/
│  │  ├─ entities.csv
│  │  └─ triples.csv
│  ├─ raw/
│  │  └─ README.md
│  ├─ exports/
│  │  ├─ nodes.csv
│  │  ├─ edges.csv
│  │  ├─ turing_kg.ttl
│  │  ├─ turing_kg.rdf
│  │  ├─ turing_kg.graphml
│  │  ├─ turing_kg.png
│  │  └─ summary.json
│  └─ nodes/
│     └─ .gitkeep
├─ src/
│  └─ kg_builder.py
├─ requirements.txt
├─ README.md
└─ 知识图谱PPT关键技术总结.md
```

## 已构建的知识范围

当前图谱围绕图灵的以下几类知识展开：

- 人物：Alan Turing、Joan Clarke、Alonzo Church
- 机构：King's College Cambridge、Princeton University、Bletchley Park、National Physical Laboratory
- 地点：Maida Vale、Wilmslow、Cambridge、Princeton、Bletchley、London
- 作品：`On Computable Numbers`、`Computing Machinery and Intelligence`
- 概念：`Turing Machine`、`Turing Test`、`Church-Turing Thesis`
- 机器：`Automatic Computing Engine`、`Bombe`、`Enigma Machine`
- 事件：`Allied Codebreaking`
- 学科：Computer Science、Artificial Intelligence、Cryptanalysis、Mathematical Logic

## 为什么这样设计

相比把所有节点和边直接写死在一个 Python 文件里，这种设计更适合课程作业持续迭代：

- 增加新实体：直接往 `entities.csv` 中追加
- 增加新关系：直接往 `triples.csv` 中追加
- 增加新类别或关系类型：修改 `schema` 层即可
- 接入后续讲到的信息抽取：可把抽取结果先放到 `data/raw/`，清洗后再入库
- 接入后续讲到的实体消歧：可基于现有规范 ID、别名、来源、置信度继续扩展

## 安装依赖

```bash
pip install -r requirements.txt
```

## 运行构建

```bash
python src/kg_builder.py
```

运行完成后会在 `data/exports/` 下生成：

- `nodes.csv`
- `edges.csv`
- `turing_kg.ttl`
- `turing_kg.rdf`
- `turing_kg.graphml`
- `turing_kg.png`
- `summary.json`

## 后续扩展建议

如果后面课程继续推进，可以沿着下面的方向扩展：

1. 增加更多图灵相关实体，例如导师、同事、奖项、历史事件、组织机构
2. 从百科文本中抽取新的属性和关系，补充到 `triples.csv`
3. 引入跨来源数据，开始做融合和对齐
4. 将别名、缩写、证据句、候选实体表单独建表，为实体消歧做准备
5. 接入 Neo4j 或 Protégé，做查询展示和语义建模

## 作者

- 李嘉轩
