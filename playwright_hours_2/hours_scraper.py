"""Collect Monday–Sunday opening hours from Google Maps for each shop."""

from __future__ import annotations

import argparse
import queue
import random
import re
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Any, Callable
from urllib.parse import quote_plus

from playwright.sync_api import sync_playwright

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from csv_compat import (  # noqa: E402
    backfill_ids,
    canonical_shop,
    clean_maps_address,
    read_csv_rows,
    write_csv_rows,
)

DAYS = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]
DAY_COLUMNS = [f"hours_{d.lower()}" for d in DAYS]
IDENTITY_COLUMNS = [
    "id",
    "name",
    "address",
    "city",
    "province",
    "latitude",
    "longitude",
]
OUTPUT_COLUMNS = IDENTITY_COLUMNS + DAY_COLUMNS
ProgressCallback = Callable[[dict[str, Any]], None]
MAX_FAIL_RELOAD_PASSES = 2
DEFAULT_WORKERS = 3


def launch_browser(playwright, *, headed: bool = False):
    args = ["--no-sandbox", "--disable-blink-features=AutomationControlled"]
    try:
        return playwright.chromium.launch(channel="chrome", headless=not headed, args=args)
    except Exception:
        return playwright.chromium.launch(headless=not headed, args=args)


def apply_stealth_patches(context) -> None:
    context.add_init_script(
        """
        Object.defineProperty(navigator, 'webdriver', { get: () => undefined });
        window.navigator.chrome = { runtime: {} };
        Object.defineProperty(navigator, 'languages', { get: () => ['en-CA', 'en'] });
        Object.defineProperty(navigator, 'plugins', { get: () => [1, 2, 3, 4, 5] });
        """
    )


def handle_consent_popups(page) -> None:
    try:
        for selector in (
            'button:has-text("Accept all")',
            'button:has-text("I agree")',
            'form[action*="consent"] button',
        ):
            btn = page.query_selector(selector)
            if btn and btn.is_visible():
                btn.click()
                page.wait_for_timeout(400)
                break
    except Exception:
        pass


_DAY_HOURS_RE = re.compile(
    r"(Closed|Open 24 hours|\d{1,2}(?::\d{2})?\s*(?:AM|PM)\s*(?:-|to)\s*"
    r"\d{1,2}(?::\d{2})?\s*(?:AM|PM))",
    re.I,
)


def _normalize_hours_text(text: str) -> str:
    value = str(text or "")
    for src, dst in (
        ("\u202f", " "),
        ("\u00a0", " "),
        ("\u2009", " "),
        ("\u2013", "-"),
        ("\u2014", "-"),
        ("\n", " "),
    ):
        value = value.replace(src, dst)
    return re.sub(r"\s+", " ", value).strip()


def _leave_photo_view(page) -> None:
    url = page.url or ""
    if ",3a," not in url and "!1e2" not in url and "!1e3" not in url:
        return
    for selector in (
        'button[aria-label="Back"]',
        'button[aria-label*="Back" i]',
        'button[jsaction*="back"]',
        '[aria-label="Close"]',
    ):
        loc = page.locator(selector)
        try:
            if loc.count() == 0:
                continue
            loc.first.click(timeout=2500)
            page.wait_for_timeout(1000)
            return
        except Exception:
            continue
    try:
        page.keyboard.press("Escape")
        page.wait_for_timeout(600)
    except Exception:
        pass


def _open_place_panel(page) -> None:
    handle_consent_popups(page)
    _leave_photo_view(page)
    if "/maps/place/" in (page.url or "") and ",3a," not in (page.url or ""):
        return
    for selector in (
        "a.hfpxzc",
        'a[href*="/maps/place/"]',
        'div[role="feed"] a[href*="/maps/place/"]',
    ):
        loc = page.locator(selector)
        try:
            if loc.count() == 0:
                continue
            loc.first.click(timeout=4000)
            page.wait_for_timeout(1200)
            _leave_photo_view(page)
            return
        except Exception:
            continue


def _dismiss_hours_editor(page) -> None:
    try:
        if page.locator("text=Suggest new hours").count():
            page.keyboard.press("Escape")
            page.wait_for_timeout(400)
    except Exception:
        pass


def _hours_row_count(page) -> int:
    try:
        return page.locator("table.eK4R0e tr").count()
    except Exception:
        return 0


def _expand_hours_widget(page) -> None:
    """Open the weekly hours table without the 'suggest new hours' editor."""
    selectors = (
        "span.HlvSq",
        'button[data-item-id="oh"]',
        '[jsaction*="pane.wfvdle"]',
        'button:has-text("See more hours")',
        "div.MkV9",
        "div.t39EBf",
        'button[aria-label*="See more hours" i]',
    )
    for selector in selectors:
        loc = page.locator(selector)
        try:
            n = min(loc.count(), 6)
        except Exception:
            continue
        for i in range(n):
            el = loc.nth(i)
            try:
                aria = (el.get_attribute("aria-label") or "").lower()
                jsaction = (el.get_attribute("jsaction") or "").lower()
                if "copy" in aria or "copy" in jsaction:
                    continue
                el.click(timeout=3500)
                page.wait_for_timeout(1100)
            except Exception:
                continue
            if _hours_row_count(page) >= 1:
                return
    try:
        page.wait_for_function(
            "() => document.querySelectorAll('table.eK4R0e tr').length >= 1",
            timeout=2500,
        )
    except Exception:
        page.wait_for_timeout(400)


def extract_hours_from_maps_page(page) -> dict[str, str]:
    hours_dict = {col: "Not listed" for col in DAY_COLUMNS}
    try:
        _open_place_panel(page)
        _expand_hours_widget(page)
        parsed = page.evaluate(
            """() => {
              const DAYS = ["Monday","Tuesday","Wednesday","Thursday","Friday","Saturday","Sunday"];
              const norm = (s) => (s || "")
                .replace(/[\\u202f\\u00a0\\u2009]/g, " ")
                .replace(/[\\u2013\\u2014]/g, "-")
                .replace(/\\s+/g, " ")
                .trim();
              const out = {};
              const take = (day, value) => {
                if (!day || out[day] || !value) return;
                const v = norm(value);
                if (!v) return;
                out[day] = v;
              };
              const consider = (raw) => {
                const text = norm(raw);
                if (!text) return;
                for (const day of DAYS) {
                  if (out[day]) continue;
                  const re = new RegExp(
                    day + "\\\\s*[,:]?\\\\s*(Closed|Open 24 hours|\\\\d{1,2}(?::\\\\d{2})?\\\\s*(?:AM|PM)\\\\s*(?:-|to)\\\\s*\\\\d{1,2}(?::\\\\d{2})?\\\\s*(?:AM|PM))",
                    "i"
                  );
                  const m = text.match(re);
                  if (m) take(day, m[1]);
                }
              };
              document.querySelectorAll("table.eK4R0e tr, table tr, div[role='region'] tr").forEach((tr) => {
                const dayCell = tr.querySelector("td.ylH6lf, td:first-child");
                const hoursCell = tr.querySelector("li.G8aQO, td.mxowUb, td:nth-child(2)");
                const dayName = DAYS.find((d) => (dayCell && (dayCell.innerText || "").toLowerCase().includes(d.toLowerCase())) || norm(tr.innerText).toLowerCase().startsWith(d.toLowerCase()));
                if (dayName && hoursCell) {
                  const m = norm(hoursCell.innerText).match(/(Closed|Open 24 hours|\\d{1,2}(?::\\d{2})?\\s*(?:AM|PM)\\s*(?:-|to)\\s*\\d{1,2}(?::\\d{2})?\\s*(?:AM|PM))/i);
                  if (m) take(dayName, m[1]);
                }
                consider(tr.innerText);
              });
              document.querySelectorAll("[data-value]").forEach((el) => consider(el.getAttribute("data-value")));
              document.querySelectorAll("button[aria-label], [aria-label*='AM'], [aria-label*='PM']").forEach((el) => {
                const aria = el.getAttribute("aria-label") || "";
                if (/Monday|Tuesday|Wednesday|Thursday|Friday|Saturday|Sunday/i.test(aria)) {
                  consider(aria.replace(/;/g, " "));
                }
              });
              consider(document.body.innerText);
              return out;
            }"""
        )
        for day in DAYS:
            raw = str((parsed or {}).get(day) or "").strip()
            if not raw:
                continue
            cleaned = _normalize_hours_text(raw)
            match = _DAY_HOURS_RE.search(cleaned)
            value = match.group(1) if match else cleaned
            hours_dict[f"hours_{day.lower()}"] = (
                "Closed" if value.lower() == "closed" else value
            )
    except Exception as err:
        print(f"    [!] Parsing error: {err}")
    return hours_dict


def _hours_complete(row: dict[str, Any]) -> bool:
    """A week is complete only when every day has a real value (Closed counts)."""
    filled = 0
    for col in DAY_COLUMNS:
        val = str(row.get(col) or "").strip()
        if val and val.lower() != "not listed":
            filled += 1
    return filled >= 7


def _identity_from_shop(shop: dict[str, Any]) -> dict[str, Any]:
    identity = {col: shop.get(col, "") for col in IDENTITY_COLUMNS}
    identity["address"] = clean_maps_address(identity.get("address") or "")
    return identity


def _match_key(row: dict[str, Any]) -> str:
    shop = canonical_shop(row)
    lat = shop.get("latitude") or ""
    lng = shop.get("longitude") or ""
    if lat and lng:
        return f"{lat}|{lng}"
    return f"{shop.get('id')}|{shop.get('name')}|{shop.get('city')}".lower()


def _search_query(row: dict[str, Any]) -> str:
    name = str(row.get("name") or "").strip()
    address = clean_maps_address(row.get("address") or "")
    city = str(row.get("city") or "").strip()
    parts = [part for part in (name, address, city) if part]
    return ", ".join(parts)


def _maps_search_url(row: dict[str, Any]) -> str:
    query = _search_query(row)
    lat = str(row.get("latitude") or "").strip()
    lng = str(row.get("longitude") or "").strip()
    url = f"https://www.google.com/maps/search/{quote_plus(query)}"
    if lat and lng:
        url += f"/@{lat},{lng},17z"
    return url + "?hl=en"


def _fetch_hours_for_row(page, row: dict[str, Any]) -> tuple[bool, str]:
    query = _search_query(row)
    if not query:
        return False, "empty query"
    try:
        page.goto(_maps_search_url(row), wait_until="domcontentloaded", timeout=25000)
        page.wait_for_timeout(1400)
        try:
            page.wait_for_selector(
                'button[data-item-id="oh"], table.eK4R0e, h1, [role="main"]',
                timeout=8000,
            )
        except Exception:
            pass
        hours = extract_hours_from_maps_page(page)
        for col in DAY_COLUMNS:
            new_val = str(hours.get(col) or "").strip()
            old_val = str(row.get(col) or "").strip()
            if new_val and new_val.lower() != "not listed":
                row[col] = new_val
            elif old_val.lower() not in ("", "not listed"):
                continue
            else:
                row[col] = new_val or "Not listed"
        if not _hours_complete(row):
            name = quote_plus(str(row.get("name") or "place").strip() or "place")
            lat = str(row.get("latitude") or "").strip()
            lng = str(row.get("longitude") or "").strip()
            if lat and lng:
                page.goto(
                    f"https://www.google.com/maps/place/{name}/@{lat},{lng},17z?hl=en",
                    wait_until="domcontentloaded",
                    timeout=25000,
                )
                page.wait_for_timeout(1400)
                hours = extract_hours_from_maps_page(page)
                for col in DAY_COLUMNS:
                    new_val = str(hours.get(col) or "").strip()
                    if new_val and new_val.lower() != "not listed":
                        row[col] = new_val
        return _hours_complete(row), ""
    except Exception as err:
        return False, str(err)


def scrape_hours(
    input_csv: Path,
    output_csv: Path,
    *,
    failed_csv: Path | None = None,
    limit: int | None = None,
    headed: bool = False,
    workers: int = DEFAULT_WORKERS,
    fail_reload_passes: int = MAX_FAIL_RELOAD_PASSES,
    on_progress: ProgressCallback | None = None,
    stop_event: threading.Event | None = None,
) -> dict[str, Any]:
    input_csv = Path(input_csv)
    output_csv = Path(output_csv)
    result = {
        "status": "completed",
        "output": str(output_csv),
        "total": 0,
        "remaining": 0,
        "updated": 0,
        "failed": 0,
        "retried": 0,
        "message": "",
    }
    if not input_csv.exists():
        result.update(status="error", message=f"Shops CSV not found: {input_csv}")
        return result

    _fields, shop_rows = read_csv_rows(input_csv)
    backfill_ids(shop_rows)
    shops = [canonical_shop(row, i) for i, row in enumerate(shop_rows)]

    existing_by_key: dict[str, dict[str, Any]] = {}
    if output_csv.exists():
        _out_fields, existing_rows = read_csv_rows(output_csv)
        for row in existing_rows:
            existing_by_key[_match_key(row)] = row

    combined: list[dict[str, Any]] = []
    seen_keys: set[str] = set()
    for shop in shops:
        key = _match_key(shop)
        if key in seen_keys:
            continue
        seen_keys.add(key)
        identity = _identity_from_shop(shop)
        prior = existing_by_key.get(key) or {}
        row = {**identity}
        for col in DAY_COLUMNS:
            row[col] = prior.get(col) or "Not listed"
        combined.append(row)

    unprocessed = [i for i, row in enumerate(combined) if not _hours_complete(row)]
    if limit is not None and limit > 0:
        unprocessed = unprocessed[:limit]

    result["total"] = len(combined)
    result["remaining"] = len(unprocessed)
    workers = max(1, min(6, int(workers or 1)))

    def emit(payload: dict[str, Any]) -> None:
        if on_progress:
            on_progress(payload)

    def stopped() -> bool:
        return bool(stop_event and stop_event.is_set())

    if not unprocessed:
        write_csv_rows(output_csv, OUTPUT_COLUMNS, combined)
        result["message"] = (
            f"All {len(combined)} shops already have hours. Nothing to do."
        )
        emit({"percent": 100.0, "done": len(combined), "total": len(combined),
              "status": result["message"], "shop": ""})
        return result

    output_csv.parent.mkdir(parents=True, exist_ok=True)
    lock = threading.Lock()
    processed = 0

    def _save() -> None:
        write_csv_rows(output_csv, OUTPUT_COLUMNS, combined)

    def _run_indices(indices: list[int], pass_label: str) -> list[int]:
        nonlocal processed
        failed: list[int] = []
        job_q: queue.Queue[int] = queue.Queue()
        for idx in indices:
            job_q.put(idx)
        total = max(1, len(indices))

        def worker(worker_id: int) -> None:
            nonlocal processed
            with sync_playwright() as p:
                browser = launch_browser(p, headed=headed and worker_id == 0)
                context = browser.new_context(
                    locale="en-CA",
                    viewport={"width": 1280, "height": 800},
                    user_agent=(
                        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                        "AppleWebKit/537.36 (KHTML, like Gecko) "
                        "Chrome/122.0.0.0 Safari/537.36"
                    ),
                )
                apply_stealth_patches(context)
                page = context.new_page()
                try:
                    while True:
                        if stopped():
                            return
                        try:
                            idx = job_q.get_nowait()
                        except queue.Empty:
                            return
                        row = combined[idx]
                        name = row.get("name") or ""
                        ok, err = _fetch_hours_for_row(page, row)
                        with lock:
                            if ok:
                                result["updated"] += 1
                            else:
                                failed.append(idx)
                                result["failed"] += 1
                                if failed_csv and pass_label.startswith("retry"):
                                    entry = dict(row)
                                    entry["error_reason"] = err
                                    _append_failed(failed_csv, entry)
                            processed += 1
                            n = processed
                            should_save = n % 4 == 0
                        emit(
                            {
                                "percent": min(100.0, (n / max(len(unprocessed), 1)) * 100.0),
                                "done": n,
                                "total": len(unprocessed),
                                "status": f"{pass_label} {'ok' if ok else 'fail'}: {name}",
                                "shop": name,
                            }
                        )
                        print(
                            f"[{pass_label}] {name}: "
                            f"{'hours saved' if ok else 'failed' + (': ' + err if err else '')}"
                        )
                        if should_save:
                            _save()
                        time.sleep(random.uniform(0.25, 0.55))
                finally:
                    browser.close()

        with ThreadPoolExecutor(max_workers=workers, thread_name_prefix="hours") as pool:
            futs = [pool.submit(worker, i) for i in range(workers)]
            for fut in as_completed(futs):
                fut.result()
        return failed

    emit(
        {
            "percent": 0.0,
            "done": 0,
            "total": len(unprocessed),
            "status": f"Opening hours for {len(unprocessed)} shop(s) ({workers} workers)",
            "shop": "",
        }
    )

    pending = list(unprocessed)
    failed_idx = _run_indices(pending, "pass-1")
    reload_pass = 0
    while failed_idx and reload_pass < fail_reload_passes and not stopped():
        reload_pass += 1
        result["retried"] += len(failed_idx)
        result["failed"] = max(0, result["failed"] - len(failed_idx))
        print(
            f"\nRetry pass {reload_pass}/{fail_reload_passes}: "
            f"{len(failed_idx)} shop(s) after the rest of the batch."
        )
        emit(
            {
                "percent": 90.0,
                "status": f"Retrying {len(failed_idx)} failed hours shop(s)",
                "shop": "",
            }
        )
        failed_idx = _run_indices(failed_idx, f"retry-{reload_pass}")

    if stopped():
        result["status"] = "stopped"

    _save()
    still_missing = sum(1 for row in combined if not _hours_complete(row))
    result["failed"] = still_missing
    if result["status"] == "stopped":
        result["message"] = (
            f"Stopped after updating {result['updated']} shop(s). Saved {output_csv.name}."
        )
    else:
        result["message"] = (
            f"Hours finished: {result['updated']} updated, {still_missing} still missing, "
            f"{result['retried']} retried after batch, {len(combined)} shops in {output_csv.name}."
        )
    print(f"\n{result['message']}")
    return result


def _append_failed(path: Path, row: dict[str, Any]) -> None:
    exists = path.exists()
    fieldnames = list(row.keys())
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8-sig", newline="") as handle:
        writer = __import__("csv").DictWriter(
            handle, fieldnames=fieldnames, extrasaction="ignore"
        )
        if not exists:
            writer.writeheader()
        writer.writerow(row)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Scrape Google Maps opening hours using the shops CSV."
    )
    parser.add_argument("--input", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--failed", type=Path, default=None)
    parser.add_argument("--limit", type=int, default=None)
    parser.add_argument("--headed", action="store_true")
    parser.add_argument("--workers", type=int, default=DEFAULT_WORKERS)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    result = scrape_hours(
        args.input,
        args.output,
        failed_csv=args.failed,
        limit=args.limit,
        headed=args.headed,
        workers=args.workers,
    )
    if result.get("status") == "error":
        sys.exit(result.get("message") or "Hours scrape failed")


if __name__ == "__main__":
    main()
