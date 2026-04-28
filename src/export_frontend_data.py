#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from __future__ import annotations

import json
from pathlib import Path

from auto_pipeline_utils import read_csv, read_json


ROOT_DIR = Path(__file__).resolve().parents[1]
FRONTEND_DIR = ROOT_DIR / "frontend"
EXPORTS_DIR = ROOT_DIR / "data" / "exports"


def main() -> None:
    FRONTEND_DIR.mkdir(parents=True, exist_ok=True)

    nodes = read_csv(EXPORTS_DIR / "nodes.csv")
    edges = read_csv(EXPORTS_DIR / "edges.csv")
    summary = read_json(EXPORTS_DIR / "summary.json")

    payload = {
        "nodes": nodes,
        "edges": edges,
        "summary": summary,
    }
    content = "window.KG_DATA = " + json.dumps(payload, ensure_ascii=False, indent=2) + ";\n"
    (FRONTEND_DIR / "data.js").write_text(content, encoding="utf-8")
    print(f"Frontend data exported to {FRONTEND_DIR / 'data.js'}")


if __name__ == "__main__":
    main()
