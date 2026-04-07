#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from __future__ import annotations

from urllib.parse import quote

import requests

from auto_pipeline_utils import (
    WIKIDATA_RAW_DIR,
    WIKIPEDIA_RAW_DIR,
    ensure_dirs,
    load_seeds,
    slugify,
    write_json,
)


USER_AGENT = "turing-knowledge-graph-bot/1.0"


def fetch_text(url: str) -> str:
    response = requests.get(url, timeout=30, headers={"User-Agent": USER_AGENT})
    response.raise_for_status()
    return response.text


def fetch_json(url: str) -> dict:
    response = requests.get(url, timeout=30, headers={"User-Agent": USER_AGENT})
    response.raise_for_status()
    return response.json()


def main() -> None:
    ensure_dirs()
    for seed in load_seeds():
        title = seed["wikipedia_title"]
        slug = slugify(seed["entity_id"])

        summary_url = f"https://en.wikipedia.org/api/rest_v1/page/summary/{quote(title.replace(' ', '_'))}"
        summary = fetch_json(summary_url)
        write_json(WIKIPEDIA_RAW_DIR / f"{slug}.summary.json", summary)

        html_url = f"https://en.wikipedia.org/wiki/{quote(title.replace(' ', '_'))}"
        html = fetch_text(html_url)
        (WIKIPEDIA_RAW_DIR / f"{slug}.page.html").write_text(html, encoding="utf-8")

        wikidata_id = summary.get("wikibase_item")
        if wikidata_id:
            wikidata_url = f"https://www.wikidata.org/wiki/Special:EntityData/{wikidata_id}.json"
            write_json(WIKIDATA_RAW_DIR / f"{wikidata_id}.json", fetch_json(wikidata_url))

    print("Fetch completed.")


if __name__ == "__main__":
    main()
