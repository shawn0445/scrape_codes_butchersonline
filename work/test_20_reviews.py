#!/usr/bin/env python3
"""Continue the 20-shop test from reviews only."""

from __future__ import annotations

import asyncio
import csv
import json
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / "reviews_scraping_5"))

import paths
from scrape_main import load_shops, run_scraper

LIMIT = 20
MAX_REVIEWS = 10


def log(msg: str) -> None:
    print(f"[{time.strftime('%H:%M:%S')}] {msg}", flush=True)


def main() -> int:
    shops = [s for s in load_shops(paths.SHOPS_CSV) if s.get("name")][:LIMIT]
    log(f"STEP 5/5  Reviews for {len(shops)} shops, max {MAX_REVIEWS} each")
    started = time.time()
    asyncio.run(
        run_scraper(
            shops=shops,
            max_reviews=MAX_REVIEWS,
            headless=True,
            output_path=paths.REVIEWS_CSV,
            checkpoint_path=paths.REVIEWS_CHECKPOINT,
            resume=False,
            concurrency=2,
            max_concurrency=2,
            limit=LIMIT,
        )
    )
    rows = 0
    if paths.REVIEWS_CSV.exists():
        with paths.REVIEWS_CSV.open(encoding="utf-8-sig", newline="") as handle:
            rows = sum(1 for _ in csv.DictReader(handle))
    log(f"STEP 5 done: {rows} review rows in {time.time() - started:.0f}s -> {paths.REVIEWS_CSV}")
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as err:
        log(f"FATAL: {err}")
        raise
