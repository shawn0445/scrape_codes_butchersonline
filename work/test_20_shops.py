#!/usr/bin/env python3
"""End-to-end test: 20 shops through all five pipeline steps."""

from __future__ import annotations

import asyncio
import csv
import json
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1] if Path(__file__).resolve().parent.name == "work" else Path(__file__).resolve().parent
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

LIMIT = 20
# Enough reviews to prove the step works without scraping thousands per shop.
MAX_REVIEWS = 10


def log(msg: str) -> None:
    print(f"[{time.strftime('%H:%M:%S')}] {msg}", flush=True)


def on_progress(payload: dict) -> None:
    status = payload.get("status") or ""
    pct = payload.get("percent")
    shop = payload.get("shop") or payload.get("city") or ""
    extra = f" {shop}" if shop else ""
    if pct is not None:
        log(f"  {pct:5.1f}%  {status}{extra}")
    elif status:
        log(f"  {status}{extra}")


def csv_info(path: Path) -> dict:
    if not path.exists():
        return {"exists": False, "rows": 0, "columns": [], "path": str(path)}
    with path.open(encoding="utf-8-sig", newline="") as handle:
        reader = csv.DictReader(handle)
        columns = list(reader.fieldnames or [])
        rows = list(reader)
    return {
        "exists": True,
        "rows": len(rows),
        "columns": columns,
        "path": str(path),
        "sample": rows[0] if rows else {},
    }


def main() -> int:
    paths.ensure_dirs()
    started = time.time()
    results: dict = {"limit": LIMIT, "steps": {}}

    log(f"TEST START — {LIMIT} shops, all five data files")
    log(f"Output folder: {paths.OUTPUT_DIR}")

    # 1. Shops
    log("STEP 1/5  Discover shops")
    shops_result = scrape_google_maps(
        output=paths.SHOPS_CSV,
        existing_csv=paths.SHOPS_CSV if paths.SHOPS_CSV.exists() else None,
        limit=LIMIT,
        max_per_query=20,
        headed=False,
        workers=3,
        browser="chromium",
        on_progress=on_progress,
        reset_state=True,
        enrich_missing=True,
        enrich_limit=LIMIT,
    )
    csv_compat.ensure_shop_ids(paths.SHOPS_CSV)
    results["steps"]["shops"] = {**shops_result, "file": csv_info(paths.SHOPS_CSV)}
    log(f"STEP 1 done: {shops_result.get('message')}")

    if not paths.SHOPS_CSV.exists() or csv_compat.count_csv_rows(paths.SHOPS_CSV) == 0:
        log("ABORT: no shops CSV")
        return 1

    # 2. Hours
    log("STEP 2/5  Opening hours")
    hours_result = scrape_hours(
        paths.SHOPS_CSV,
        paths.HOURS_CSV,
        failed_csv=paths.HOURS_FAILED_CSV,
        limit=LIMIT,
        headed=False,
        on_progress=on_progress,
    )
    results["steps"]["hours"] = {**hours_result, "file": csv_info(paths.HOURS_CSV)}
    log(f"STEP 2 done: {hours_result.get('message')}")

    # 3. Services
    log("STEP 3/5  Service options")
    shops = load_shops(paths.SHOPS_CSV)[:LIMIT]
    services_result = asyncio.run(
        run_service_ops(
            shops=shops,
            headless=True,
            output_path=paths.SERVICE_OPTIONS_CSV,
            checkpoint_path=paths.SERVICE_CHECKPOINT,
            resume=False,
            concurrency=3,
            limit=LIMIT,
            on_progress=on_progress,
        )
    )
    results["steps"]["services"] = {**services_result, "file": csv_info(paths.SERVICE_OPTIONS_CSV)}
    log(f"STEP 3 done: {services_result.get('message')}")

    # 4. Summary
    log("STEP 4/5  Listing summaries")
    summary_result = generate_summary(
        paths.SHOPS_CSV,
        paths.HOURS_CSV,
        paths.SERVICE_OPTIONS_CSV,
        paths.SUMMARY_CSV,
    )
    results["steps"]["summary"] = {**summary_result, "file": csv_info(paths.SUMMARY_CSV)}
    log(f"STEP 4 done: {summary_result.get('message')}")

    # 5. Reviews
    log("STEP 5/5  Customer reviews")
    from scrape_main import load_shops as load_review_shops
    from scrape_main import run_scraper as run_reviews

    review_shops = [s for s in load_review_shops(paths.SHOPS_CSV) if s.get("name")][:LIMIT]
    log(f"  Reviewing {len(review_shops)} shops, max {MAX_REVIEWS} reviews each")
    asyncio.run(
        run_reviews(
            shops=review_shops,
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
    results["steps"]["reviews"] = {
        "status": "completed",
        "file": csv_info(paths.REVIEWS_CSV),
    }
    log("STEP 5 done")

    elapsed = time.time() - started
    results["elapsed_seconds"] = round(elapsed, 1)
    summary_path = paths.WORK_DIR / "test_20_results.json"
    # Drop huge sample blobs for JSON
    slim = {
        "elapsed_seconds": results["elapsed_seconds"],
        "limit": LIMIT,
        "files": {},
    }
    for key, step in results["steps"].items():
        info = step.get("file") or {}
        slim["files"][key] = {
            "exists": info.get("exists"),
            "rows": info.get("rows"),
            "columns": info.get("columns"),
            "path": info.get("path"),
            "status": step.get("status"),
            "message": step.get("message"),
        }
    summary_path.write_text(json.dumps(slim, indent=2), encoding="utf-8")

    log("=" * 60)
    log(f"TEST FINISHED in {elapsed/60:.1f} min")
    for key, info in slim["files"].items():
        log(f"  {key:10s}  rows={info['rows']:<6}  {info['path']}")
    log(f"Summary JSON: {summary_path}")
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as err:
        log(f"FATAL: {err}")
        raise
