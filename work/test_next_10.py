#!/usr/bin/env python3
"""Discover 10 more shops, then collect hours/services/summary/reviews."""

from __future__ import annotations

import asyncio
import csv
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / "All_shops_1"))
sys.path.insert(0, str(ROOT / "playwright_hours_2"))
sys.path.insert(0, str(ROOT / "service_ops_3"))
sys.path.insert(0, str(ROOT / "GenerateSummary_4"))
sys.path.insert(0, str(ROOT / "reviews_scraping_5"))

import csv_compat
import paths
from gensum_woid import generate_summary
from hours_scraper import scrape_hours
from scraper import scrape_google_maps
from service_ops import load_shops, run_scraper as run_service_ops

TARGET_SHOPS = 30
MAX_REVIEWS = 10


def log(msg: str) -> None:
    print(f"[{time.strftime('%H:%M:%S')}] {msg}", flush=True)


def on_progress(payload: dict) -> None:
    status = payload.get("status") or ""
    pct = payload.get("percent")
    shop = payload.get("shop") or payload.get("city") or ""
    extra = f" {shop}" if shop else ""
    if pct is not None:
        log(f"  {float(pct):5.1f}%  {status}{extra}")
    elif status:
        log(f"  {status}{extra}")


def count(path: Path) -> int:
    return csv_compat.count_csv_rows(path)


def main() -> int:
    paths.ensure_dirs()
    started = time.time()
    before = count(paths.SHOPS_CSV)
    log(f"NEXT-10 TEST  shops now={before}  target={TARGET_SHOPS}")

    cleaned = csv_compat.cleanup_shop_address_columns(paths.SHOPS_CSV)
    log(f"Cleaned Open/Closed off {cleaned} shop address(es)")

    log("STEP 1/5  Discover 10 more shops (no extra retry pass)")
    shops_result = scrape_google_maps(
        output=paths.SHOPS_CSV,
        existing_csv=paths.SHOPS_CSV if paths.SHOPS_CSV.exists() else None,
        limit=TARGET_SHOPS,
        max_per_query=20,
        headed=False,
        workers=3,
        browser="chromium",
        on_progress=on_progress,
        reset_state=True,
        enrich_missing=False,
    )
    csv_compat.ensure_shop_ids(paths.SHOPS_CSV)
    csv_compat.cleanup_shop_address_columns(paths.SHOPS_CSV)
    after = count(paths.SHOPS_CSV)
    log(f"STEP 1 done: {before} -> {after} shops | {shops_result.get('message')}")
    if after <= before:
        log("WARNING: no new shops were added")

    log("STEP 2/5  Hours (new + previously missing, retry fails after batch)")
    hours_result = scrape_hours(
        paths.SHOPS_CSV,
        paths.HOURS_CSV,
        failed_csv=paths.HOURS_FAILED_CSV,
        headed=False,
        workers=3,
        on_progress=on_progress,
    )
    log(f"STEP 2 done: {hours_result.get('message')}")

    log("STEP 3/5  Services (new + empty rows, retry fails after batch)")
    shops = load_shops(paths.SHOPS_CSV)
    services_result = asyncio.run(
        run_service_ops(
            shops=shops,
            headless=True,
            output_path=paths.SERVICE_OPTIONS_CSV,
            checkpoint_path=paths.SERVICE_CHECKPOINT,
            resume=True,
            concurrency=4,
            on_progress=on_progress,
        )
    )
    log(f"STEP 3 done: {services_result.get('message')}")

    log("STEP 4/5  Summaries")
    summary_result = generate_summary(
        paths.SHOPS_CSV,
        paths.HOURS_CSV,
        paths.SERVICE_OPTIONS_CSV,
        paths.SUMMARY_CSV,
    )
    log(f"STEP 4 done: {summary_result.get('message')}")

    log("STEP 5/5  Reviews (new shops + prior fails, dedupe, retry after batch)")
    from scrape_main import load_shops as load_review_shops
    from scrape_main import run_scraper as run_reviews

    review_shops = [s for s in load_review_shops(paths.SHOPS_CSV) if s.get("name")]
    log(f"  {len(review_shops)} shops in file, max {MAX_REVIEWS} reviews, resume=True")
    asyncio.run(
        run_reviews(
            shops=review_shops,
            max_reviews=MAX_REVIEWS,
            headless=True,
            output_path=paths.REVIEWS_CSV,
            checkpoint_path=paths.REVIEWS_CHECKPOINT,
            resume=True,
            concurrency=3,
            max_concurrency=3,
        )
    )
    log(f"STEP 5 done: reviews rows={count(paths.REVIEWS_CSV)}")

    elapsed = time.time() - started
    log("=" * 60)
    log(f"FINISHED in {elapsed/60:.1f} min")
    log(f"  shops     {count(paths.SHOPS_CSV)}")
    log(f"  hours     {count(paths.HOURS_CSV)}")
    log(f"  services  {count(paths.SERVICE_OPTIONS_CSV)}")
    log(f"  summary   {count(paths.SUMMARY_CSV)}")
    log(f"  reviews   {count(paths.REVIEWS_CSV)}")
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as err:
        log(f"FATAL: {err}")
        raise
