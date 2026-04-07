#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from __future__ import annotations

import subprocess
import sys
from pathlib import Path


ROOT_DIR = Path(__file__).resolve().parents[1]
SRC_DIR = ROOT_DIR / "src"


def run_script(name: str) -> None:
    print(f"== Running {name} ==")
    subprocess.run([sys.executable, str(SRC_DIR / name)], check=True)


def main() -> None:
    for name in [
        "fetch_sources.py",
        "extract_candidates.py",
        "normalize_candidates.py",
        "fuse_knowledge.py",
        "kg_builder.py",
    ]:
        run_script(name)
    print("Auto pipeline completed.")


if __name__ == "__main__":
    main()
