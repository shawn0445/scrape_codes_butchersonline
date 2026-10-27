#!/usr/bin/env python3
"""Canada butcher shop discovery via Playwright Google Maps automation."""

from __future__ import annotations

import argparse
import csv
import json
import queue
import re
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Any, Callable
from urllib.parse import quote

import requests
from playwright.sync_api import (
    Page,
    TimeoutError as PlaywrightTimeoutError,
    sync_playwright,
)

from cities import TARGET_CITIES

BASE_DIR = Path(__file__).resolve().parent
DEFAULT_OUTPUT = BASE_DIR / "butcher_shops2.csv"
PROGRESS_FILE = BASE_DIR / "scrape_progress.json"
FAILED_URLS_FILE = BASE_DIR / "scrape_failed_urls.json"
DEFAULT_MAX_PER_QUERY = 20
DEFAULT_WORKERS = 3
DEFAULT_BROWSER = "chromium"
SUPPORTED_BROWSERS = ("chromium", "firefox")
TARGET_RECORD_LIMIT = None
MAX_FAILED_URL_ATTEMPTS = 5

_BROWSER_USER_AGENTS = {
    "chromium": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/122.0.0.0 Safari/537.36"
    ),
    "firefox": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:123.0) "
        "Gecko/20100101 Firefox/123.0"
    ),
}

SEARCH_KEYWORDS = [
    "butcher shop",
    "meat market",
    "custom butcher",
    "farm butcher",
    "halal butcher",
    "meat shop",
    "boucherie",
    "abattoir",
    "meat processing",
    "halal meat",
    "kosher butcher",
]

CSV_FIELDS = [
    "id",
    "Name",
    "Address",
    "City",
    "Province",
    "Latitude",
    "Longitude",
    "Postal_Code",
    "Phone",
    "Website",
    "Site_Type",
    "Rating",
    "Reviews",
]

CLEAN_POSTAL_RE = re.compile(r"[A-Z]\d[A-Z]\s*\d[A-Z]\d", re.I)
PHONE_LIKE_RE = re.compile(
    r"^\+?\d[\d\s().-]{6,}(?:\s*(?:ext\.?|x|extension)\s*\d+)?$",
    re.I,
)
RATING_LIKE_RE = re.compile(r"^\d\.\d(?:\s*\([\d,]+\))?$")
CATEGORY_LIKE_RE = re.compile(
    r"^(butcher shop|meat shop|meat market|grocery store|supermarket|"
    r"halal restaurant|restaurant|cafe|café|bakery|deli|food store|"
    r"convenience store|farmers?'? market|seafood market|fish market)$",
    re.I,
)

ProgressCallback = Callable[[dict[str, Any]], None]

all_shops: list[dict[str, Any]] = []
unique_shops: set[str] = set()
completed_cities: list[str] = []
failed_urls: dict[str, dict[str, Any]] = {}
_state_lock = threading.RLock()
_cities_finished = 0


class ScrapeController:
    """Thread-safe pause / resume / stop controller for GUI or CLI use."""

    def __init__(self) -> None:
        self._pause = threading.Event()
        self._stop = threading.Event()

    def pause(self) -> None:
        self._pause.set()

    def resume(self) -> None:
        self._pause.clear()

    def stop(self) -> None:
        self._stop.set()
        self._pause.clear()

    def reset(self) -> None:
        self._pause.clear()
        self._stop.clear()

    @property
    def is_paused(self) -> bool:
        return self._pause.is_set()

    @property
    def is_stopped(self) -> bool:
        return self._stop.is_set()

    def wait_if_paused(self) -> None:
        while self._pause.is_set() and not self._stop.is_set():
            time.sleep(0.15)

    def checkpoint(self) -> bool:
        """Return False if stop was requested. Blocks while paused."""
        self.wait_if_paused()
        return not self._stop.is_set()

    def interruptible_sleep(self, ms: float) -> bool:
        """Sleep in short slices. Returns False if stop was requested."""
        end = time.time() + (ms / 1000.0)
        while time.time() < end:
            if not self.checkpoint():
                return False
            remaining = end - time.time()
            time.sleep(min(0.15, max(remaining, 0)))
        return self.checkpoint()


_active_controller: ScrapeController | None = None
_progress_callback: ProgressCallback | None = None
_run_started_at: float | None = None


def reset_runtime_state() -> None:
    global all_shops, unique_shops, completed_cities, failed_urls, _cities_finished
    with _state_lock:
        all_shops = []
        unique_shops = set()
        completed_cities = []
        failed_urls = {}
        _cities_finished = 0


def sleep(ms: float) -> None:
    if _active_controller is not None:
        _active_controller.interruptible_sleep(ms)
    else:
        time.sleep(ms / 1000.0)


def _shop_count() -> int:
    with _state_lock:
        return len(all_shops)


def _limit_reached(effective_limit: int | None) -> bool:
    if effective_limit is None:
        return False
    with _state_lock:
        return len(all_shops) >= effective_limit


def _mark_city_finished() -> int:
    global _cities_finished
    with _state_lock:
        _cities_finished += 1
        return _cities_finished


def _try_add_shop(shop: dict[str, Any], unique_key: str, effective_limit: int | None) -> bool:
    """Register a shop if new and under limit. Returns True if added."""
    with _state_lock:
        if effective_limit is not None and len(all_shops) >= effective_limit:
            return False
        if unique_key in unique_shops:
            return False
        unique_shops.add(unique_key)
        all_shops.append(shop)
        return True


def update_progress_bar(
    current: int,
    total: int,
    status_text: str = "",
    *,
    city: str = "",
    query: str = "",
    workers: int = 1,
    shops: int | None = None,
) -> None:
    percentage = min(max(current / total if total else 0, 0), 1)
    percent = percentage * 100
    bar_length = 30
    completed_length = round(percentage * bar_length)
    progress_bar = "#" * completed_length + "-" * (bar_length - completed_length)

    elapsed = 0.0
    eta_seconds = None
    if _run_started_at is not None:
        elapsed = max(time.time() - _run_started_at, 0.0)
        done = max(current, 0)
        if 0 < done < total:
            per_city = elapsed / done
            eta_seconds = per_city * (total - done)
        elif current >= total:
            eta_seconds = 0.0

    with _state_lock:
        shop_count = len(all_shops) if shops is None else int(shops)

    line = (
        f"\rProgress: [{progress_bar}] {percent:.1f}% | "
        f"{current}/{total} Regions | shops={shop_count} | workers={workers} | {status_text}"
    )
    with _state_lock:
        sys.stdout.write(line)
        sys.stdout.flush()

    if _progress_callback is not None:
        _progress_callback(
            {
                "city_index": current,
                "total_cities": total,
                "percent": percent,
                "status": status_text,
                "shops": shop_count,
                "city": city,
                "query": query,
                "elapsed": elapsed,
                "eta_seconds": eta_seconds,
                "workers": workers,
                "paused": bool(_active_controller and _active_controller.is_paused),
                "stopped": bool(_active_controller and _active_controller.is_stopped),
            }
        )


def load_existing_shops(file_path: Path) -> None:
    if not file_path.exists():
        print(f"\nNo existing CSV found at {file_path}. Starting fresh.")
        return

    try:
        with file_path.open("r", encoding="utf-8", newline="") as f:
            reader = csv.DictReader(f)
            shops = list(reader)
    except OSError as err:
        print(f"\nCould not read existing CSV: {err}. Starting fresh.")
        return

    print(f"\nLoaded {len(shops)} existing shops from {file_path}")
    for shop in shops:
        lat = shop.get("Latitude") or shop.get("latitude")
        lng = shop.get("Longitude") or shop.get("longitude")
        if lat and lng:
            unique_shops.add(f"{lat},{lng}")
    all_shops.extend(shops)


def load_progress() -> None:
    global completed_cities

    if PROGRESS_FILE.exists():
        try:
            data = json.loads(PROGRESS_FILE.read_text(encoding="utf-8"))
            completed_cities = data.get("completedCities") or []
            loaded_shops = data.get("allShops") or []
            for shop in loaded_shops:
                key = f"{shop.get('Latitude')},{shop.get('Longitude')}"
                if key not in unique_shops:
                    unique_shops.add(key)
                    all_shops.append(shop)
            print("\nFound existing progress state file! Resuming...")
            print(f"Loaded {len(all_shops)} total records into memory.\n")
        except (OSError, json.JSONDecodeError):
            print("State tracking file was corrupted, starting fresh.")
    else:
        print("\nStarting Canada Butcher Extraction (Playwright / Google Maps)")


def _read_progress_file() -> dict[str, Any]:
    if not PROGRESS_FILE.exists():
        return {"completedCities": [], "allShops": []}
    try:
        data = json.loads(PROGRESS_FILE.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {"completedCities": [], "allShops": []}
    if not isinstance(data, dict):
        return {"completedCities": [], "allShops": []}
    cities = data.get("completedCities") or []
    shops = data.get("allShops") or []
    return {
        "completedCities": list(cities) if isinstance(cities, list) else [],
        "allShops": list(shops) if isinstance(shops, list) else [],
    }


def _write_progress_file(
    completed: list[str], shops: list[dict[str, Any]]
) -> None:
    state = {"completedCities": list(completed), "allShops": list(shops)}
    PROGRESS_FILE.write_text(json.dumps(state, indent=2), encoding="utf-8")


def get_progress_summary() -> dict[str, Any]:
    """Return counts and path for the on-disk progress file."""
    data = _read_progress_file()
    with _state_lock:
        pending_failed = len(failed_urls)
    if pending_failed == 0 and FAILED_URLS_FILE.exists():
        try:
            raw = json.loads(FAILED_URLS_FILE.read_text(encoding="utf-8"))
            if isinstance(raw, dict):
                pending_failed = len(raw.get("failedUrls") or {})
            elif isinstance(raw, list):
                pending_failed = len(raw)
        except (OSError, json.JSONDecodeError):
            pending_failed = 0
    return {
        "path": str(PROGRESS_FILE),
        "exists": PROGRESS_FILE.exists(),
        "completed_cities": len(data["completedCities"]),
        "saved_shops": len(data["allShops"]),
        "total_cities": len(TARGET_CITIES),
        "failed_urls": pending_failed,
    }


def load_failed_urls() -> None:
    global failed_urls
    if not FAILED_URLS_FILE.exists():
        with _state_lock:
            failed_urls = {}
        return
    try:
        data = json.loads(FAILED_URLS_FILE.read_text(encoding="utf-8"))
        if isinstance(data, dict) and isinstance(data.get("failedUrls"), dict):
            loaded = data["failedUrls"]
        elif isinstance(data, dict):
            # Allow raw url->entry mapping
            loaded = {
                k: v
                for k, v in data.items()
                if isinstance(v, dict) and k != "failedUrls"
            }
        elif isinstance(data, list):
            loaded = {
                item["url"]: item
                for item in data
                if isinstance(item, dict) and item.get("url")
            }
        else:
            loaded = {}
        with _state_lock:
            failed_urls = loaded
        if loaded:
            print(f"Loaded {len(loaded)} failed place URL(s) for retry.")
    except (OSError, json.JSONDecodeError):
        with _state_lock:
            failed_urls = {}
        print("Failed-URL queue file was corrupted; starting empty.")


def save_failed_urls() -> None:
    with _state_lock:
        payload = {"failedUrls": dict(failed_urls)}
    FAILED_URLS_FILE.write_text(json.dumps(payload, indent=2), encoding="utf-8")


def enqueue_failed_url(
    url: str,
    *,
    query: str = "",
    city_key: str = "",
    error: str = "",
) -> None:
    if not url:
        return
    key = url.split("?")[0]
    with _state_lock:
        existing = failed_urls.get(key) or {}
        attempts = int(existing.get("attempts") or 0) + 1
        if attempts > MAX_FAILED_URL_ATTEMPTS:
            # Keep a record but stop growing forever; still retryable later if cleared.
            attempts = MAX_FAILED_URL_ATTEMPTS
        failed_urls[key] = {
            "url": url,
            "query": query or existing.get("query") or "",
            "city_key": city_key or existing.get("city_key") or "",
            "attempts": attempts,
            "last_error": (error or existing.get("last_error") or "")[:500],
            "updated_at": time.strftime("%Y-%m-%dT%H:%M:%S"),
        }
    save_failed_urls()


def dequeue_failed_url(url: str) -> None:
    if not url:
        return
    key = url.split("?")[0]
    with _state_lock:
        if key not in failed_urls:
            return
        failed_urls.pop(key, None)
    save_failed_urls()


def clear_failed_urls() -> dict[str, Any]:
    """Clear the persisted failed-URL retry queue."""
    if not failed_urls and FAILED_URLS_FILE.exists():
        load_failed_urls()
    with _state_lock:
        before = len(failed_urls)
        failed_urls.clear()
    if FAILED_URLS_FILE.exists():
        FAILED_URLS_FILE.unlink()
    return {"cleared_failed_urls": before, **get_progress_summary()}


def clear_completed_cities() -> dict[str, Any]:
    """Keep saved shops, but mark all cities as incomplete so they can be re-run."""
    data = _read_progress_file()
    before = len(data["completedCities"])
    _write_progress_file([], data["allShops"])
    with _state_lock:
        completed_cities.clear()
    return {
        "cleared_cities": before,
        "remaining_shops": len(data["allShops"]),
        **get_progress_summary(),
    }


def clear_progress_shops() -> dict[str, Any]:
    """Keep completed-city marks, but remove saved shops from progress."""
    data = _read_progress_file()
    before = len(data["allShops"])
    _write_progress_file(data["completedCities"], [])
    with _state_lock:
        all_shops.clear()
        unique_shops.clear()
    return {
        "cleared_shops": before,
        "remaining_cities": len(data["completedCities"]),
        **get_progress_summary(),
    }


def clear_all_progress() -> dict[str, Any]:
    """Delete completed cities and saved shops from the progress file."""
    data = _read_progress_file()
    before_cities = len(data["completedCities"])
    before_shops = len(data["allShops"])
    if PROGRESS_FILE.exists():
        PROGRESS_FILE.unlink()
    failed_cleared = clear_failed_urls().get("cleared_failed_urls", 0)
    reset_runtime_state()
    return {
        "cleared_cities": before_cities,
        "cleared_shops": before_shops,
        "cleared_failed_urls": failed_cleared,
        **get_progress_summary(),
    }


def export_progress_shops_to_csv(file_path: Path) -> dict[str, Any]:
    """Write shops currently stored in scrape_progress.json to a CSV."""
    data = _read_progress_file()
    shops = data["allShops"]
    file_path = Path(file_path)
    file_path.parent.mkdir(parents=True, exist_ok=True)
    _backfill_shop_ids(shops)
    with file_path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=CSV_FIELDS, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(shops)
    return {"exported": len(shops), "path": str(file_path)}


def save_progress(city_key: str) -> None:
    with _state_lock:
        if city_key not in completed_cities:
            completed_cities.append(city_key)
        state = {"completedCities": list(completed_cities), "allShops": list(all_shops)}
    PROGRESS_FILE.write_text(json.dumps(state, indent=2), encoding="utf-8")


def _backfill_shop_ids(rows: list[dict[str, Any]]) -> None:
    """Assign sequential ids without changing shops that already have one."""
    used: set[int] = set()
    for shop in rows:
        raw = shop.get("id")
        try:
            n = int(float(raw)) if raw not in (None, "", "N/A") else 0
        except (TypeError, ValueError):
            n = 0
        if n > 0 and n not in used:
            shop["id"] = n
            used.add(n)
        else:
            shop["id"] = None
    next_id = 1
    for shop in rows:
        if shop.get("id"):
            continue
        while next_id in used:
            next_id += 1
        shop["id"] = next_id
        used.add(next_id)
        next_id += 1


def save_csv(is_final: bool = False, file_path: Path = DEFAULT_OUTPUT) -> None:
    file_path = Path(file_path)
    file_path.parent.mkdir(parents=True, exist_ok=True)
    with _state_lock:
        rows = list(all_shops)
    _backfill_shop_ids(rows)
    try:
        with file_path.open("w", encoding="utf-8", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=CSV_FIELDS, extrasaction="ignore")
            writer.writeheader()
            writer.writerows(rows)
    except OSError as err:
        if getattr(err, "errno", None) in (13, 32) or "being used" in str(err).lower():
            print(
                f"\n\nCSV LOCK WARNING: Close '{file_path.name}' so "
                "background data saves can finish writing!\n"
            )
        else:
            print(f"\n\nCSV write error: {err}\n")
        return

    if is_final:
        print(f"\n\nProcess Complete! Extracted Records Count: {len(rows)}")
        print(f"Destination: {file_path}\n")


def get_postal_code(lat: float, lon: float) -> str:
    try:
        response = requests.get(
            "https://nominatim.openstreetmap.org/reverse",
            params={"lat": lat, "lon": lon, "format": "json"},
            headers={"User-Agent": "canada-butcher-scraper"},
            timeout=8,
        )
        response.raise_for_status()
        return response.json().get("address", {}).get("postcode") or "N/A"
    except (requests.RequestException, ValueError, KeyError):
        return "N/A"


def analyze_website(url: str) -> dict[str, Any]:
    if (
        not url
        or url == "N/A"
        or "facebook.com" in url
        or "instagram.com" in url
    ):
        return {"isButcherShop": True, "siteType": "Regular (No Website)"}

    try:
        response = requests.get(
            url,
            headers={
                "User-Agent": (
                    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/122.0.0.0 Safari/537.36"
                )
            },
            timeout=10,
        )
        html = response.text.lower()
    except requests.RequestException:
        return {
            "isButcherShop": True,
            "siteType": "Regular (Site Offline/Unreachable)",
        }

    butcher_keywords = [
        "butcher",
        "meat",
        "sausage",
        "steak",
        "poultry",
        "beef",
        "pork",
        "charcuterie",
    ]
    contains_butcher = any(keyword in html for keyword in butcher_keywords)

    ecommerce_keywords = [
        "add to cart",
        "checkout",
        "shopping cart",
        "shopify.theme",
        "wc-embed",
        "cart-contents",
        "view cart",
    ]
    is_ecommerce = any(keyword in html for keyword in ecommerce_keywords)

    return {
        "isButcherShop": contains_butcher,
        "siteType": "E-Commerce" if is_ecommerce else "Regular Site",
    }


def extract_coords_from_url(url: str) -> dict[str, float] | None:
    if not url:
        return None

    match = re.search(r"!3d(-?\d+\.?\d*)!4d(-?\d+\.?\d*)", url)
    if match:
        return {
            "latitude": float(match.group(1)),
            "longitude": float(match.group(2)),
        }

    match = re.search(r"@(-?\d+\.?\d*),(-?\d+\.?\d*)", url)
    if match:
        return {
            "latitude": float(match.group(1)),
            "longitude": float(match.group(2)),
        }

    return None


def normalize_postal(raw: str | None) -> str:
    if not raw or raw == "N/A":
        return "N/A"
    postal_code = str(raw).replace("\u00a0", " ").upper().strip()
    match = CLEAN_POSTAL_RE.search(postal_code)
    if not match:
        return "N/A"
    postal_code = re.sub(r"\s+", "", match.group(0))
    if len(postal_code) == 6:
        postal_code = f"{postal_code[:3]} {postal_code[3:]}"
    return postal_code


def extract_postal_from_address(address: str | None) -> str:
    if not address:
        return "N/A"
    return normalize_postal(address)


def dismiss_consent(page: Page) -> None:
    candidates = [
        'button:has-text("Accept all")',
        'button:has-text("I agree")',
        'button:has-text("Accept")',
        'button:has-text("Reject all")',
        'form[action*="consent"] button',
    ]
    for selector in candidates:
        try:
            btn = page.locator(selector).first
            if btn.is_visible(timeout=1500):
                btn.click(timeout=3000)
                sleep(800)
                return
        except Exception:
            continue


def scroll_results_feed(page: Page, max_per_query: int) -> None:
    feed = page.locator('[role="feed"]').first
    try:
        feed.wait_for(state="visible", timeout=15000)
    except Exception:
        return

    stagnant = 0
    last_count = 0

    for _ in range(25):
        count = page.locator('a[href*="/maps/place/"]').count()
        if count >= max_per_query:
            break
        if count == last_count:
            stagnant += 1
            if stagnant >= 3:
                break
        else:
            stagnant = 0
            last_count = count

        try:
            feed.evaluate("(el) => el.scrollBy(0, el.clientHeight || 800)")
        except Exception:
            page.mouse.wheel(0, 2000)
        sleep(900)


def collect_place_urls(page: Page, max_per_query: int) -> list[str]:
    cards = collect_place_cards(page, max_per_query)
    return [c["url"] for c in cards if c.get("url")]


def collect_place_cards(page: Page, max_per_query: int) -> list[dict[str, Any]]:
    """
    Extract place candidates from the Maps results feed without opening each page.
    Pulls title, rating/reviews, address snippets, and coords embedded in hrefs.
    Never writes phone/rating/category text into the address field.
    """
    cards = page.locator('a[href*="/maps/place/"]').evaluate_all(
        """(anchors) => {
            const results = [];
            const seen = new Set();

            const parseCoords = (href) => {
                let m = href.match(/!3d(-?\\d+\\.?\\d*)!4d(-?\\d+\\.?\\d*)/);
                if (m) return { latitude: parseFloat(m[1]), longitude: parseFloat(m[2]) };
                m = href.match(/@(-?\\d+\\.?\\d*),(-?\\d+\\.?\\d*)/);
                if (m) return { latitude: parseFloat(m[1]), longitude: parseFloat(m[2]) };
                return { latitude: null, longitude: null };
            };

            const isPhone = (t) =>
                /^\\+?\\d[\\d\\s().-]{6,}(?:\\s*(?:ext\\.?|x|extension)\\s*\\d+)?$/i.test(t);
            const isRating = (t) => /^\\d\\.\\d(?:\\s*\\([\\d,]+\\))?$/.test(t);
            const isCategory = (t) =>
                /^(butcher shop|meat shop|meat market|grocery store|supermarket|halal restaurant|restaurant|cafe|café|bakery|deli|food store|convenience store|farmers?'? market|seafood market|fish market)$/i.test(
                    t
                );
            const isAddress = (t) => {
                if (!t || t.length < 8) return false;
                if (isPhone(t) || isRating(t) || isCategory(t)) return false;
                if (!/\\d/.test(t) || !/[A-Za-z]/.test(t)) return false;
                if (/star/i.test(t)) return false;
                return true;
            };

            for (const a of anchors) {
                const href = a.href || a.getAttribute("href") || "";
                if (!href.includes("/maps/place/")) continue;
                const absolute = href.startsWith("http")
                    ? href
                    : `https://www.google.com${href}`;
                const key = absolute.split("?")[0];
                if (seen.has(key)) continue;
                seen.add(key);

                const title = (a.getAttribute("aria-label") || "").trim();
                const coords = parseCoords(absolute);

                let root =
                    a.closest(".Nv2PK") ||
                    a.closest('[role="article"]') ||
                    a.closest("div.THOPZb") ||
                    a.parentElement;
                let address = "";
                let phone = "";
                let rating = "N/A";
                let reviews = 0;

                if (root) {
                    const ratingEl =
                        root.querySelector(".MW4etd") ||
                        root.querySelector('span[role="img"][aria-label*="star"]');
                    if (ratingEl) {
                        const aria = ratingEl.getAttribute("aria-label") || "";
                        const rm = aria.match(/([\\d.]+)\\s*star/i);
                        if (rm) rating = rm[1];
                        else if (/^\\d\\.\\d$/.test((ratingEl.textContent || "").trim())) {
                            rating = (ratingEl.textContent || "").trim();
                        }
                    }
                    const reviewEl = root.querySelector(".UY7F9");
                    if (reviewEl) {
                        const rm = (reviewEl.textContent || "").replace(/[(),]/g, "");
                        const n = parseInt(rm, 10);
                        if (!Number.isNaN(n)) reviews = n;
                    }
                    if (rating === "N/A" || !reviews) {
                        const blob = (root.innerText || "").replace(/\\s+/g, " ");
                        const combo = blob.match(/(\\d\\.\\d)\\s*\\(?([\\d,]+)\\)?/);
                        if (combo) {
                            if (rating === "N/A") rating = combo[1];
                            if (!reviews) reviews = parseInt(combo[2].replace(/,/g, ""), 10) || 0;
                        }
                    }

                    const lines = Array.from(root.querySelectorAll(".W4Efsd"))
                        .map((el) => (el.textContent || "").replace(/\\s+/g, " ").trim())
                        .filter(Boolean)
                        .flatMap((line) =>
                            line.split("·").map((part) => part.trim()).filter(Boolean)
                        );

                    for (const line of lines) {
                        if (!phone && isPhone(line)) {
                            phone = line;
                            continue;
                        }
                        if (rating === "N/A" && isRating(line)) {
                            const rm = line.match(/^(\\d\\.\\d)/);
                            if (rm) rating = rm[1];
                            const rev = line.match(/\\(([\\d,]+)\\)/);
                            if (rev && !reviews) {
                                reviews = parseInt(rev[1].replace(/,/g, ""), 10) || 0;
                            }
                            continue;
                        }
                        if (isCategory(line)) continue;
                        if (!address && isAddress(line)) {
                            address = line;
                        }
                    }
                }

                results.push({
                    url: absolute,
                    title,
                    address: address || "N/A",
                    phone: phone || "N/A",
                    website: "N/A",
                    rating,
                    reviews,
                    latitude: coords.latitude,
                    longitude: coords.longitude,
                });
            }
            return results;
        }"""
    )
    # Python-side sanitize as a second guard against misfiled card text.
    cleaned: list[dict[str, Any]] = []
    for card in cards[:max_per_query]:
        cleaned.append(sanitize_place_dict(card))
    return cleaned


def _list_card_needs_detail_visit(card: dict[str, Any]) -> bool:
    """Open the place page when list data is incomplete, including its website."""
    if not (card.get("title") or "").strip():
        return True
    if card.get("latitude") is None or card.get("longitude") is None:
        return True
    # Address from list cards is often missing; visit for a real street address.
    if _address_needs_refill(card.get("address")):
        return True
    if _field_blank(card.get("website")):
        return True
    return False


def looks_like_phone(text: Any) -> bool:
    value = str(text or "").strip()
    return bool(value.startswith("+") or PHONE_LIKE_RE.match(value))


def looks_like_rating(text: Any) -> bool:
    return bool(RATING_LIKE_RE.match(str(text or "").strip()))


def looks_like_category(text: Any) -> bool:
    return bool(CATEGORY_LIKE_RE.match(str(text or "").strip()))


def looks_like_address(text: Any) -> bool:
    value = str(text or "").strip()
    if _field_blank(value):
        return False
    if looks_like_phone(value) or looks_like_rating(value) or looks_like_category(value):
        return False
    if not re.search(r"\d", value) or not re.search(r"[A-Za-z]", value):
        return False
    if re.search(r"\bstars?\b", value, re.I):
        return False
    return len(value) >= 8


def sanitize_place_dict(raw: dict[str, Any]) -> dict[str, Any]:
    """
    Route misfiled values into the correct keys for a place/details dict
    (title/address/phone/website/rating/reviews/lat/lng/url).
    """
    address = str(raw.get("address") or "").strip()
    phone = str(raw.get("phone") or "").strip()
    rating = raw.get("rating")
    reviews = raw.get("reviews") or 0

    # Address accidentally holds a phone.
    if looks_like_phone(address):
        if _field_blank(phone):
            phone = address
        address = "N/A"

    # Address accidentally holds a rating like 4.4(1,375).
    if looks_like_rating(address):
        if _field_blank(rating) or str(rating).strip() in {"", "N/A"}:
            match = re.match(r"^(\d\.\d)", address)
            if match:
                rating = match.group(1)
            rev = re.search(r"\(([\d,]+)\)", address)
            if rev:
                try:
                    reviews = int(rev.group(1).replace(",", ""))
                except ValueError:
                    pass
        address = "N/A"

    # Address accidentally holds a Maps category label.
    if looks_like_category(address):
        address = "N/A"

    # Phone field accidentally holds non-phone text.
    if not _field_blank(phone) and not looks_like_phone(phone):
        if looks_like_address(phone) and not looks_like_address(address):
            address = phone
        phone = "N/A"

    if not looks_like_address(address):
        address = "N/A"
    elif address and address != "N/A":
        address = re.sub(
            r"(Portuguese-style butcher'?s store|butcher shop|meat market|meat shop)\s*$",
            "",
            str(address),
            flags=re.I,
        ).strip()
        address = re.sub(
            r"\s*(Permanently closed|Temporarily closed|Open|Closed)\s*$",
            "",
            address,
            flags=re.I,
        ).strip()
        address = re.sub(r"(?<=[A-Za-z0-9])(Open|Closed)$", "", address, flags=re.I).strip(" ,")
        if not address:
            address = "N/A"

    if _field_blank(phone) or not looks_like_phone(phone):
        phone = "N/A"
    if rating in (None, "") or looks_like_phone(rating) or looks_like_address(str(rating)):
        if not (
            isinstance(rating, (int, float))
            or RATING_LIKE_RE.match(str(rating or "").strip())
        ):
            rating = "N/A"

    try:
        reviews = int(reviews or 0)
    except (TypeError, ValueError):
        reviews = 0

    out = dict(raw)
    out["address"] = address or "N/A"
    out["phone"] = phone or "N/A"
    out["rating"] = rating if rating not in (None, "") else "N/A"
    out["reviews"] = reviews
    out["website"] = raw.get("website") or "N/A"
    out["title"] = (raw.get("title") or "").strip()
    return out


def _normalize_place_details(raw: dict[str, Any]) -> dict[str, Any]:
    cleaned = sanitize_place_dict(raw)
    coords = None
    if cleaned.get("latitude") is not None and cleaned.get("longitude") is not None:
        try:
            coords = {
                "latitude": float(cleaned["latitude"]),
                "longitude": float(cleaned["longitude"]),
            }
        except (TypeError, ValueError):
            coords = None
    if coords is None and cleaned.get("url"):
        coords = extract_coords_from_url(str(cleaned["url"]))

    return {
        "title": cleaned.get("title") or "",
        "address": cleaned.get("address") or "N/A",
        "phone": cleaned.get("phone") or "N/A",
        "website": cleaned.get("website") or "N/A",
        "rating": cleaned.get("rating") if cleaned.get("rating") not in (None, "") else "N/A",
        "reviews": cleaned.get("reviews") or 0,
        "latitude": coords["latitude"] if coords else None,
        "longitude": coords["longitude"] if coords else None,
        "url": cleaned.get("url") or "",
    }


def _safe_inner_text(locator) -> str:
    try:
        return locator.inner_text().strip()
    except Exception:
        return ""


def _safe_attr(locator, name: str) -> str | None:
    try:
        return locator.get_attribute(name)
    except Exception:
        return None


def extract_place_details(page: Page) -> dict[str, Any]:
    page.wait_for_timeout(1200)

    title = ""
    for sel in [
        "h1.DUwDvf",
        "h1.fontHeadlineLarge",
        'h1[class*="fontHeadline"]',
        "h1",
    ]:
        el = page.locator(sel).first
        if el.count():
            title = _safe_inner_text(el)
            if title:
                break

    address = "N/A"
    address_btn = page.locator('button[data-item-id="address"]').first
    if address_btn.count():
        address = (
            _safe_attr(address_btn, "aria-label")
            or _safe_inner_text(address_btn)
            or "N/A"
        )
        address = re.sub(r"^Address:\s*", "", address, flags=re.I).strip() or "N/A"

    phone = "N/A"
    phone_btn = page.locator('button[data-item-id^="phone:tel:"]').first
    if phone_btn.count():
        item_id = _safe_attr(phone_btn, "data-item-id") or ""
        from_id = item_id.replace("phone:tel:", "", 1).strip()
        phone = (
            from_id
            or _safe_attr(phone_btn, "aria-label")
            or _safe_inner_text(phone_btn)
            or "N/A"
        )
        phone = re.sub(r"^Phone:\s*", "", phone, flags=re.I).strip() or "N/A"

    website = "N/A"
    website_link = page.locator('a[data-item-id="authority"]').first
    try:
        website_link.wait_for(state="attached", timeout=5000)
    except PlaywrightTimeoutError:
        print(f"\nNo Maps website link found for {title or page.url}")
    else:
        href = website_link.get_attribute("href")
        if href and href.lower().startswith(("http://", "https://")):
            website = href
        else:
            print(f"\nMaps website link has no HTTP URL for {title or page.url}")

    rating = "N/A"
    rating_el = page.locator('div.F7nice span[aria-hidden="true"]').first
    if rating_el.count():
        text = _safe_inner_text(rating_el)
        if text:
            rating = text

    reviews = 0
    reviews_el = page.locator('div.F7nice span[aria-label*="review"]').first
    if reviews_el.count():
        label = (_safe_attr(reviews_el, "aria-label") or "").replace(",", "")
        match = re.search(r"([\d.]+)", label)
        if match:
            try:
                reviews = int(float(match.group(1)))
            except ValueError:
                reviews = 0

    coords = extract_coords_from_url(page.url)

    return {
        "title": title,
        "address": address,
        "phone": phone,
        "website": website,
        "rating": rating,
        "reviews": reviews,
        "latitude": coords["latitude"] if coords else None,
        "longitude": coords["longitude"] if coords else None,
    }


def _goto_with_retries(
    page: Page,
    url: str,
    *,
    timeout: int = 45000,
    attempts: int = 3,
    label: str = "Navigation",
) -> bool:
    """Navigate with retries for transient Firefox/network protocol errors."""
    last_err: Exception | None = None
    for attempt in range(1, attempts + 1):
        if _active_controller and not _active_controller.checkpoint():
            return False
        try:
            page.goto(url, wait_until="domcontentloaded", timeout=timeout)
            return True
        except Exception as err:
            last_err = err
            msg = str(err)
            transient = any(
                token in msg
                for token in (
                    "NS_ERROR_NET_HTTP3_PROTOCOL_ERROR",
                    "NS_ERROR_NET_PARTIAL_TRANSFER",
                    "NS_ERROR_NET_INTERRUPT",
                    "NS_ERROR_NET_RESET",
                    "NS_ERROR_ABORT",
                    "Timeout",
                    "net::ERR_",
                    "Navigation failed",
                )
            )
            if not transient or attempt >= attempts:
                break
            # Back off before retry; Firefox HTTP/3 flakes often clear on retry over H2.
            sleep(1200 * attempt)
    if last_err is not None:
        print(f"\n{label} failed after {attempts} attempts: {last_err}")
    return False


def _extract_place_from_url(
    page: Page,
    place_url: str,
    *,
    query: str = "",
    city_key: str = "",
    seed: dict[str, Any] | None = None,
) -> dict[str, Any] | None:
    """Visit a place page and extract details; queue URL on hard failure."""
    try:
        if not _goto_with_retries(
            page,
            place_url,
            timeout=45000,
            attempts=3,
            label="Place extract",
        ):
            enqueue_failed_url(
                place_url,
                query=query,
                city_key=city_key,
                error="navigation failed after retries",
            )
            return None
        dismiss_consent(page)
        details = _normalize_place_details(extract_place_details(page))
        details["url"] = place_url
        # Fill gaps from list-card seed when place page omits fields.
        if seed:
            for field in ("title", "address", "rating", "reviews", "latitude", "longitude"):
                if details.get(field) in (None, "", "N/A", 0) and seed.get(field) not in (
                    None,
                    "",
                    "N/A",
                ):
                    details[field] = seed[field]
        if (
            details["title"]
            and details["latitude"] is not None
            and details["longitude"] is not None
        ):
            dequeue_failed_url(place_url)
            return details
        enqueue_failed_url(
            place_url,
            query=query,
            city_key=city_key,
            error="missing title or coordinates after visit",
        )
        return None
    except Exception as err:
        enqueue_failed_url(
            place_url, query=query, city_key=city_key, error=str(err)
        )
        print(f"\nPlace extract failed: {err}")
        return None


def search_places_for_query(
    page: Page,
    query: str,
    max_per_query: int,
    *,
    city_key: str = "",
) -> list[dict[str, Any]]:
    if _active_controller and not _active_controller.checkpoint():
        return []

    search_url = f"https://www.google.com/maps/search/{quote(query)}?hl=en"
    if not _goto_with_retries(
        page, search_url, timeout=60000, attempts=3, label=f'Search "{query}"'
    ):
        return []
    dismiss_consent(page)
    sleep(2000)

    if _active_controller and not _active_controller.checkpoint():
        return []

    if "/maps/place/" in page.url:
        details = _normalize_place_details(extract_place_details(page))
        details["url"] = page.url
        if details["title"] and details["latitude"] is not None and details["longitude"] is not None:
            return [details]
        return []

    scroll_results_feed(page, max_per_query)
    cards = collect_place_cards(page, max_per_query)
    places: list[dict[str, Any]] = []
    list_hits = 0
    detail_visits = 0

    for card in cards:
        if _active_controller and not _active_controller.checkpoint():
            break
        place_url = card.get("url") or ""
        normalized = _normalize_place_details(card)

        if not _list_card_needs_detail_visit(normalized):
            places.append(normalized)
            list_hits += 1
            dequeue_failed_url(place_url)
            continue

        detail_visits += 1
        details = _extract_place_from_url(
            page,
            place_url,
            query=query,
            city_key=city_key,
            seed=normalized,
        )
        if details:
            places.append(details)
        sleep(700)

    if cards:
        print(
            f'  list-extract: "{query}" -> {list_hits} from feed, '
            f"{detail_visits} place visits, {len(places)} usable"
        )
    return places


def retry_failed_place_urls(
    page: Page,
    *,
    output: Path,
    effective_limit: int | None,
    workers: int = 1,
    max_retries: int | None = None,
) -> int:
    """
    Revisit queued failed place URLs. Returns number of newly saved shops.
    """
    with _state_lock:
        items = list(failed_urls.values())
    if not items:
        return 0

    if max_retries is not None:
        items = items[: max(0, max_retries)]

    print(f"\nRetrying {len(items)} failed place URL(s)...")
    added = 0
    total_cities = len(TARGET_CITIES)

    for item in items:
        if _active_controller and not _active_controller.checkpoint():
            break
        if _limit_reached(effective_limit):
            break

        place_url = item.get("url") or ""
        if not place_url:
            continue
        city_key = item.get("city_key") or ""
        query = item.get("query") or ""

        details = _extract_place_from_url(
            page, place_url, query=query, city_key=city_key
        )
        if not details:
            sleep(500)
            continue

        # Infer city/province from city_key "Name-PR" when possible.
        city_name, province = "", ""
        if city_key and "-" in city_key:
            city_name, province = city_key.rsplit("-", 1)

        postal_code = extract_postal_from_address(details.get("address"))
        if postal_code == "N/A" and details.get("latitude") is not None:
            postal_code = get_postal_code(details["latitude"], details["longitude"])
            sleep(1200)

        website_url = details.get("website") or "N/A"
        analysis = analyze_website(website_url)
        if not analysis["isButcherShop"]:
            dequeue_failed_url(place_url)
            continue

        unique_key = f"{details['latitude']},{details['longitude']}"
        shop = {
            "Name": details["title"],
            "Address": details.get("address") or "N/A",
            "City": city_name or "N/A",
            "Province": province or "N/A",
            "Latitude": details["latitude"],
            "Longitude": details["longitude"],
            "Postal_Code": postal_code,
            "Phone": details.get("phone") or "N/A",
            "Website": website_url,
            "Site_Type": analysis["siteType"],
            "Rating": details.get("rating") or "N/A",
            "Reviews": details.get("reviews") or 0,
        }
        if _try_add_shop(shop, unique_key, effective_limit):
            added += 1
            update_progress_bar(
                _cities_finished,
                total_cities,
                f"Retry saved: {_shop_count()} shops",
                city=f"{city_name}, {province}".strip(", "),
                query=query or "failed-url-retry",
                workers=workers,
            )
            save_csv(False, output)
        else:
            dequeue_failed_url(place_url)
        sleep(700)

    save_failed_urls()
    print(f"Failed-URL retry complete. Newly saved: {added}")
    return added


def _launch_browser_page(playwright, headed: bool, browser_name: str = DEFAULT_BROWSER):
    name = (browser_name or DEFAULT_BROWSER).strip().lower()
    if name not in SUPPORTED_BROWSERS:
        raise ValueError(
            f"Unsupported browser '{browser_name}'. "
            f"Choose one of: {', '.join(SUPPORTED_BROWSERS)}"
        )

    launch_kwargs: dict[str, Any] = {"headless": not headed}
    if name == "chromium":
        launch_kwargs["args"] = ["--disable-blink-features=AutomationControlled"]
    elif name == "firefox":
        # Google Maps + many Firefox workers often trip HTTP/3 (QUIC) protocol errors.
        launch_kwargs["firefox_user_prefs"] = {
            "network.http.http3.enabled": False,
            "network.http.http3.support_version1": False,
            "network.http.http2.enabled": True,
        }

    browser_type = getattr(playwright, name)
    browser = browser_type.launch(**launch_kwargs)
    context = browser.new_context(
        locale="en-CA",
        viewport={"width": 1365, "height": 900},
        user_agent=_BROWSER_USER_AGENTS[name],
    )
    return browser, context.new_page()


def process_city(
    page: Page,
    city_obj: dict[str, str],
    *,
    total_cities: int,
    max_per_query: int,
    effective_limit: int | None,
    output: Path,
    controller: ScrapeController | None,
    workers: int,
) -> str:
    """
    Scrape one city. Returns status: done | skipped | stopped | limit.
    """
    city_unique_key = f"{city_obj['name']}-{city_obj['province']}"
    city_label = f"{city_obj['name']}, {city_obj['province']}"

    with _state_lock:
        already_done = city_unique_key in completed_cities

    if already_done:
        done = _mark_city_finished()
        update_progress_bar(
            done,
            total_cities,
            f"Skipping completed city: {city_obj['name']}",
            city=city_label,
            workers=workers,
        )
        return "skipped"

    if controller and not controller.checkpoint():
        return "stopped"
    if _limit_reached(effective_limit):
        return "limit"

    done_now = _cities_finished
    update_progress_bar(
        done_now,
        total_cities,
        f"Entering: {city_label}",
        city=city_label,
        workers=workers,
    )

    stopped = False
    for keyword in SEARCH_KEYWORDS:
        if controller and not controller.checkpoint():
            stopped = True
            break
        if _limit_reached(effective_limit):
            break

        query = f"{keyword} in {city_obj['name']}, {city_obj['province']}, Canada"
        update_progress_bar(
            _cities_finished,
            total_cities,
            f"Searching: {query}",
            city=city_label,
            query=query,
            workers=workers,
        )

        try:
            places = search_places_for_query(
                page, query, max_per_query, city_key=city_unique_key
            )
            for place in places:
                if controller and not controller.checkpoint():
                    stopped = True
                    break
                if _limit_reached(effective_limit):
                    break
                if (
                    not place["title"]
                    or place["latitude"] is None
                    or place["longitude"] is None
                ):
                    continue

                unique_key = f"{place['latitude']},{place['longitude']}"
                with _state_lock:
                    already = unique_key in unique_shops
                if already:
                    continue

                place = sanitize_place_dict(place)
                postal_code = extract_postal_from_address(place["address"])
                if postal_code == "N/A":
                    postal_code = get_postal_code(
                        place["latitude"], place["longitude"]
                    )
                    sleep(1200)

                website_url = place.get("website") or "N/A"
                analysis = analyze_website(website_url)
                if not analysis["isButcherShop"]:
                    continue

                shop = {
                    "Name": place["title"],
                    "Address": place.get("address") or "N/A",
                    "City": city_obj["name"],
                    "Province": city_obj["province"],
                    "Latitude": place["latitude"],
                    "Longitude": place["longitude"],
                    "Postal_Code": postal_code,
                    "Phone": place.get("phone") or "N/A",
                    "Website": website_url,
                    "Site_Type": analysis["siteType"],
                    "Rating": place.get("rating") or "N/A",
                    "Reviews": place.get("reviews") or 0,
                }
                sanitize_shop_row(shop)
                if _try_add_shop(shop, unique_key, effective_limit):
                    update_progress_bar(
                        _cities_finished,
                        total_cities,
                        f"Extracted: {_shop_count()} shops",
                        city=city_label,
                        query=query,
                        workers=workers,
                    )
        except Exception as err:
            with _state_lock:
                sys.stdout.write("\n")
                print(f'Maps scrape error on query "{query}": {err}')
            save_progress(city_unique_key)
            save_csv(False, output)

        if stopped:
            break
        sleep(1000)

    save_progress(city_unique_key)
    save_csv(False, output)

    if stopped:
        return "stopped"

    done = _mark_city_finished()
    update_progress_bar(
        done,
        total_cities,
        f"Finished city: {city_label}",
        city=city_label,
        workers=workers,
    )
    if _limit_reached(effective_limit):
        return "limit"
    return "done"


def _worker_run(
    worker_id: int,
    job_queue: queue.Queue,
    *,
    total_cities: int,
    max_per_query: int,
    effective_limit: int | None,
    output: Path,
    headed: bool,
    browser_name: str,
    controller: ScrapeController | None,
    workers: int,
) -> str:
    """Pull cities from queue until empty/stop/limit. Returns last status."""
    last_status = "done"
    with sync_playwright() as p:
        browser, page = _launch_browser_page(
            p,
            headed=headed and worker_id == 0,
            browser_name=browser_name,
        )
        try:
            while True:
                if controller and not controller.checkpoint():
                    return "stopped"
                if _limit_reached(effective_limit):
                    return "limit"
                try:
                    city_obj = job_queue.get_nowait()
                except queue.Empty:
                    return last_status

                try:
                    last_status = process_city(
                        page,
                        city_obj,
                        total_cities=total_cities,
                        max_per_query=max_per_query,
                        effective_limit=effective_limit,
                        output=output,
                        controller=controller,
                        workers=workers,
                    )
                    if last_status in ("stopped", "limit"):
                        # Drain remaining so other workers can exit promptly
                        while True:
                            try:
                                job_queue.get_nowait()
                                job_queue.task_done()
                            except queue.Empty:
                                break
                        return last_status
                finally:
                    job_queue.task_done()
        finally:
            browser.close()
    return last_status


def _field_blank(val: Any) -> bool:
    text = str(val if val is not None else "").strip()
    return not text or text.upper() in {"N/A", "NAN", "NONE", "NULL", "-"}


def _address_needs_refill(address: Any) -> bool:
    return not looks_like_address(address)


def _postal_needs_refill(postal: Any) -> bool:
    text = str(postal if postal is not None else "").strip()
    if _field_blank(text):
        return True
    return CLEAN_POSTAL_RE.search(text) is None


def sanitize_shop_row(shop: dict[str, Any]) -> bool:
    """
    Fix misfiled CSV columns in-place (Address/Phone/Rating/Postal).
    Returns True if any meaningful field changed (ignores Reviews type-only churn).
    """
    shop.setdefault("Postal_Code", "N/A")
    try:
        reviews_before = int(shop.get("Reviews") or 0)
    except (TypeError, ValueError):
        reviews_before = 0
    before = (
        str(shop.get("Address") or "").strip(),
        str(shop.get("Phone") or "").strip(),
        str(shop.get("Rating") or "").strip(),
        str(shop.get("Postal_Code") or "").strip(),
    )

    placeish = sanitize_place_dict(
        {
            "address": shop.get("Address"),
            "phone": shop.get("Phone"),
            "rating": shop.get("Rating"),
            "reviews": reviews_before,
            "website": shop.get("Website"),
            "title": shop.get("Name"),
        }
    )
    shop["Address"] = placeish.get("address") or "N/A"
    shop["Phone"] = placeish.get("phone") or "N/A"
    shop["Rating"] = (
        placeish.get("rating") if placeish.get("rating") not in (None, "") else "N/A"
    )
    shop["Reviews"] = placeish.get("reviews") or 0

    # Pull postal out of a real address when Postal_Code is empty/junk.
    if _postal_needs_refill(shop.get("Postal_Code")) and looks_like_address(
        shop.get("Address")
    ):
        postal = extract_postal_from_address(shop.get("Address"))
        if not _postal_needs_refill(postal):
            shop["Postal_Code"] = postal

    after = (
        str(shop.get("Address") or "").strip(),
        str(shop.get("Phone") or "").strip(),
        str(shop.get("Rating") or "").strip(),
        str(shop.get("Postal_Code") or "").strip(),
    )
    return before != after


def _salvage_phone_from_address(shop: dict[str, Any]) -> bool:
    """Backward-compatible alias for sanitize_shop_row()."""
    return sanitize_shop_row(shop)


def shop_missing_fields(shop: dict[str, Any]) -> list[str]:
    """Return which enrichable fields are missing / garbage for a shop row."""
    missing: list[str] = []
    if _field_blank(shop.get("Phone")) or not looks_like_phone(shop.get("Phone")):
        missing.append("Phone")
    if _field_blank(shop.get("Website")):
        missing.append("Website")
    if _postal_needs_refill(shop.get("Postal_Code")):
        missing.append("Postal_Code")
    if _address_needs_refill(shop.get("Address")):
        missing.append("Address")
    if _field_blank(shop.get("Rating")):
        missing.append("Rating")
    site_type = str(shop.get("Site_Type") or "").strip()
    if _field_blank(shop.get("Website")) and site_type in {
        "",
        "N/A",
        "Regular (No Website)",
    }:
        missing.append("Site_Type")
    return missing


def count_shops_needing_enrichment(csv_path: Path) -> dict[str, Any]:
    path = Path(csv_path)
    if not path.exists():
        return {"path": str(path), "total": 0, "needs_enrichment": 0, "exists": False}
    with path.open(encoding="utf-8", newline="") as f:
        rows = list(csv.DictReader(f))
    need = sum(1 for row in rows if shop_missing_fields(row))
    return {
        "path": str(path),
        "total": len(rows),
        "needs_enrichment": need,
        "exists": True,
    }


def _merge_enriched_fields(
    shop: dict[str, Any], details: dict[str, Any]
) -> list[str]:
    """Fill blank/garbage shop fields from Maps details. Returns filled field names."""
    filled: list[str] = []

    if _address_needs_refill(shop.get("Address")):
        new_addr = details.get("address") or ""
        if new_addr and not _address_needs_refill(new_addr):
            shop["Address"] = new_addr
            filled.append("Address")

    if _field_blank(shop.get("Phone")) or not looks_like_phone(shop.get("Phone")):
        new_phone = details.get("phone") or ""
        if looks_like_phone(new_phone):
            shop["Phone"] = new_phone
            filled.append("Phone")

    if _field_blank(shop.get("Website")):
        new_web = details.get("website") or ""
        if new_web and not _field_blank(new_web):
            shop["Website"] = new_web
            filled.append("Website")
            analysis = analyze_website(new_web)
            shop["Site_Type"] = analysis["siteType"]
            filled.append("Site_Type")

    if _postal_needs_refill(shop.get("Postal_Code")):
        postal = extract_postal_from_address(details.get("address"))
        if postal == "N/A":
            postal = extract_postal_from_address(shop.get("Address"))
        if postal == "N/A":
            lat = details.get("latitude")
            lng = details.get("longitude")
            if lat is None:
                try:
                    lat = float(shop.get("Latitude"))
                    lng = float(shop.get("Longitude"))
                except (TypeError, ValueError):
                    lat = lng = None
            if lat is not None and lng is not None:
                postal = get_postal_code(float(lat), float(lng))
                sleep(400)
        if not _postal_needs_refill(postal):
            shop["Postal_Code"] = normalize_postal(postal)
            filled.append("Postal_Code")

    if _field_blank(shop.get("Rating")):
        new_rating = details.get("rating")
        if new_rating not in (None, "", "N/A"):
            shop["Rating"] = new_rating
            filled.append("Rating")

    reviews = shop.get("Reviews")
    try:
        reviews_n = int(reviews or 0)
    except (TypeError, ValueError):
        reviews_n = 0
    if reviews_n <= 0 and details.get("reviews"):
        shop["Reviews"] = details.get("reviews") or 0
        filled.append("Reviews")

    return filled


def _place_panel_ready(page: Page) -> bool:
    """True when Maps shows a place detail panel (URL or side panel)."""
    if "/maps/place/" in page.url:
        return True
    try:
        addr_btn = page.locator('button[data-item-id="address"]').first
        if addr_btn.count():
            return True
        title = page.locator("h1.DUwDvf, h1.fontHeadlineLarge, h1").first
        if title.count() and _safe_inner_text(title):
            phone_btn = page.locator('button[data-item-id^="phone:"]').first
            if phone_btn.count():
                return True
    except Exception:
        pass
    return False


def _open_maps_place_for_shop(page: Page, shop: dict[str, Any]) -> bool:
    """Navigate Maps to the shop place page via name+coords search, then click result."""
    name = str(shop.get("Name") or "").strip()
    city = str(shop.get("City") or "").strip()
    province = str(shop.get("Province") or "").strip()
    lat = shop.get("Latitude")
    lng = shop.get("Longitude")
    try:
        lat_f, lng_f = float(lat), float(lng)
        has_coords = True
    except (TypeError, ValueError):
        lat_f = lng_f = 0.0
        has_coords = False

    # Fast path: direct place URL often opens the panel without a search click.
    if name and has_coords:
        if _active_controller and not _active_controller.checkpoint():
            return False
        direct = (
            f"https://www.google.com/maps/place/{quote(name)}"
            f"/@{lat_f},{lng_f},17z?hl=en"
        )
        if _goto_with_retries(
            page, direct, timeout=45000, attempts=1, label=f'Enrich direct "{name}"'
        ):
            dismiss_consent(page)
            sleep(700)
            if _place_panel_ready(page):
                return True

    queries: list[str] = []
    if name:
        queries.append(" ".join(part for part in (name, city, province, "Canada") if part))
        if city:
            queries.append(f"{name} {city} Canada")
        queries.append(name)
    if has_coords:
        queries.append(f"{lat_f},{lng_f}")

    for query in queries:
        if _active_controller and not _active_controller.checkpoint():
            return False
        search_url = f"https://www.google.com/maps/search/{quote(query)}?hl=en"
        if has_coords and "," not in query:
            # Bias results near known shop coordinates.
            search_url = (
                f"https://www.google.com/maps/search/{quote(query)}"
                f"/@{lat_f},{lng_f},14z?hl=en"
            )
        if not _goto_with_retries(
            page, search_url, timeout=45000, attempts=2, label=f'Enrich "{name}"'
        ):
            continue

        dismiss_consent(page)
        sleep(900)
        if _place_panel_ready(page):
            return True

        try:
            links = page.locator('a[href*="/maps/place/"]')
            if links.count() > 0:
                links.first.click(timeout=8000)
                page.wait_for_timeout(1200)
                if _place_panel_ready(page):
                    return True
        except Exception:
            continue

    return False


def enrich_shops_csv(
    csv_path: Path,
    *,
    headed: bool = False,
    browser: str = DEFAULT_BROWSER,
    workers: int = DEFAULT_WORKERS,
    limit: int | None = None,
    controller: ScrapeController | None = None,
    on_progress: ProgressCallback | None = None,
    save_every: int = 15,
) -> dict[str, Any]:
    """
    Revisit incomplete shops in a CSV and refill Phone / Website / Postal / Address / etc.
    Uses parallel browser workers (same Workers setting as scrape).
    """
    global _active_controller, _progress_callback, _run_started_at

    path = Path(csv_path)
    result: dict[str, Any] = {
        "status": "completed",
        "path": str(path),
        "total": 0,
        "candidates": 0,
        "updated": 0,
        "maps_updated": 0,
        "salvaged": 0,
        "skipped": 0,
        "failed": 0,
        "workers": 1,
        "message": "",
    }
    if not path.exists():
        result.update(status="error", message=f"CSV not found: {path}")
        return result

    with path.open(encoding="utf-8", newline="") as f:
        rows = list(csv.DictReader(f))
    result["total"] = len(rows)

    # Normalize keys / ensure CSV fields exist.
    salvage_only = 0
    for row in rows:
        for field in CSV_FIELDS:
            if field == "id":
                row.setdefault(field, "")
            else:
                row.setdefault(field, "N/A" if field != "Reviews" else 0)
        # Normalize Reviews type without counting it as a salvage update.
        try:
            row["Reviews"] = int(row.get("Reviews") or 0)
        except (TypeError, ValueError):
            row["Reviews"] = 0
        if sanitize_shop_row(row):
            salvage_only += 1
    _backfill_shop_ids(rows)

    still_need = [i for i, row in enumerate(rows) if shop_missing_fields(row)]

    if salvage_only:
        with path.open("w", encoding="utf-8", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=CSV_FIELDS, extrasaction="ignore")
            writer.writeheader()
            writer.writerows(rows)

    if limit is not None and limit > 0:
        still_need = still_need[:limit]

    workers = max(1, min(8, int(workers or 1)))
    result["candidates"] = len(still_need)
    result["salvaged"] = salvage_only
    result["updated"] = salvage_only
    result["workers"] = workers

    if not still_need:
        result["message"] = (
            "No shops need Maps enrichment"
            + (f" (local field fixes on {salvage_only})" if salvage_only else "")
            + "."
        )
        result["skipped"] = len(rows) - salvage_only
        return result

    browser_name = (browser or DEFAULT_BROWSER).strip().lower()
    if browser_name not in SUPPORTED_BROWSERS:
        raise ValueError(
            f"Unsupported browser '{browser}'. "
            f"Choose one of: {', '.join(SUPPORTED_BROWSERS)}"
        )

    prev_controller = _active_controller
    prev_callback = _progress_callback
    prev_started = _run_started_at
    _active_controller = controller
    _progress_callback = on_progress
    _run_started_at = time.time()
    if controller is not None:
        controller.reset()

    print(
        f"\nEnriching {len(still_need)} shop(s) with {workers} worker(s) from {path}"
        + (f" (local fixes first: {salvage_only})" if salvage_only else "")
    )
    total = len(still_need)
    maps_updated = 0
    processed = 0
    enrich_lock = threading.RLock()
    job_queue: queue.Queue[int] = queue.Queue()
    for idx in still_need:
        job_queue.put(idx)

    def _save_rows() -> None:
        with path.open("w", encoding="utf-8", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=CSV_FIELDS, extrasaction="ignore")
            writer.writeheader()
            writer.writerows(rows)

    def _enrich_worker(worker_id: int) -> None:
        nonlocal maps_updated, processed
        with sync_playwright() as p:
            browser_obj, page = _launch_browser_page(
                p,
                headed=headed and worker_id == 0,
                browser_name=browser_name,
            )
            try:
                while True:
                    if controller and not controller.checkpoint():
                        with enrich_lock:
                            result["status"] = "stopped"
                        # Drain so sibling workers exit promptly.
                        while True:
                            try:
                                job_queue.get_nowait()
                                job_queue.task_done()
                            except queue.Empty:
                                break
                        return
                    try:
                        idx = job_queue.get_nowait()
                    except queue.Empty:
                        return

                    shop = rows[idx]
                    name = shop.get("Name") or f"row {idx + 1}"
                    missing_before = shop_missing_fields(shop)
                    try:
                        opened = _open_maps_place_for_shop(page, shop)
                        if not opened:
                            with enrich_lock:
                                result["failed"] += 1
                                processed += 1
                                n = processed
                                filled_count = maps_updated + salvage_only
                            update_progress_bar(
                                n,
                                total,
                                f"Enrich miss: {name}",
                                city=f"{shop.get('City')}, {shop.get('Province')}",
                                query=", ".join(missing_before) or "fields",
                                workers=workers,
                                shops=filled_count,
                            )
                            sleep(250)
                            continue

                        details = _normalize_place_details(extract_place_details(page))
                        filled = _merge_enriched_fields(shop, details)
                        meaningful = [
                            f
                            for f in filled
                            if f
                            in {
                                "Address",
                                "Phone",
                                "Website",
                                "Postal_Code",
                                "Site_Type",
                            }
                        ]
                        with enrich_lock:
                            if meaningful:
                                maps_updated += 1
                                rows[idx] = shop
                                result["maps_updated"] = maps_updated
                                result["updated"] = salvage_only + maps_updated
                            else:
                                result["skipped"] += 1
                            processed += 1
                            n = processed
                            filled_count = maps_updated + salvage_only
                            should_save = n % max(1, save_every) == 0
                            if should_save:
                                _save_rows()

                        update_progress_bar(
                            n,
                            total,
                            f"Enriching: {name}",
                            city=f"{shop.get('City')}, {shop.get('Province')}",
                            query=", ".join(missing_before) or "fields",
                            workers=workers,
                            shops=filled_count,
                        )
                    except Exception as err:
                        with enrich_lock:
                            result["failed"] += 1
                            processed += 1
                            n = processed
                            filled_count = maps_updated + salvage_only
                        print(f"\nEnrich failed for {name}: {err}")
                        update_progress_bar(
                            n,
                            total,
                            f"Enrich error: {name}",
                            city=f"{shop.get('City')}, {shop.get('Province')}",
                            query=", ".join(missing_before) or "fields",
                            workers=workers,
                            shops=filled_count,
                        )
                    finally:
                        job_queue.task_done()
                        sleep(300)
            finally:
                browser_obj.close()

    try:
        with ThreadPoolExecutor(
            max_workers=workers, thread_name_prefix="enrich"
        ) as pool:
            futures = [pool.submit(_enrich_worker, wid) for wid in range(workers)]
            for fut in as_completed(futures):
                fut.result()

        with enrich_lock:
            _save_rows()
            result["maps_updated"] = maps_updated
            result["updated"] = salvage_only + maps_updated

        update_progress_bar(
            processed if result["status"] != "stopped" else max(processed, 0),
            total,
            "Enrichment pass complete",
            workers=workers,
            shops=maps_updated + salvage_only,
        )

        result["message"] = (
            f"Enrichment {'stopped' if result['status'] == 'stopped' else 'finished'}: "
            f"maps_filled {maps_updated}, local_fixes {salvage_only}, "
            f"failed {result['failed']}, no-change {result['skipped']} "
            f"(candidates {len(still_need)} / {len(rows)} shops, workers={workers})"
        )
        print(f"\n{result['message']}")
        return result
    finally:
        _active_controller = prev_controller
        _progress_callback = prev_callback
        _run_started_at = prev_started


def scrape_google_maps(
    output: Path,
    existing_csv: Path | None,
    limit: int | None,
    max_per_query: int,
    headed: bool,
    *,
    workers: int = DEFAULT_WORKERS,
    browser: str = DEFAULT_BROWSER,
    controller: ScrapeController | None = None,
    on_progress: ProgressCallback | None = None,
    reset_state: bool = True,
    enrich_missing: bool = True,
    enrich_limit: int | None = None,
) -> dict[str, Any]:
    """
    Run the Maps scrape, optionally with parallel browser workers.

    Returns a result dict: {status, shops, output, message}.
    status is one of: completed, stopped, already_done, error
    """
    global _active_controller, _progress_callback, _run_started_at, _cities_finished

    if reset_state:
        reset_runtime_state()

    workers = max(1, int(workers or 1))
    browser_name = (browser or DEFAULT_BROWSER).strip().lower()
    if browser_name not in SUPPORTED_BROWSERS:
        raise ValueError(
            f"Unsupported browser '{browser}'. "
            f"Choose one of: {', '.join(SUPPORTED_BROWSERS)}"
        )
    _active_controller = controller
    _progress_callback = on_progress
    _run_started_at = time.time()
    if controller is not None:
        controller.reset()

    result: dict[str, Any] = {
        "status": "completed",
        "shops": 0,
        "output": str(output),
        "message": "",
    }

    try:
        effective_limit = limit if limit is not None else TARGET_RECORD_LIMIT

        if existing_csv:
            load_existing_shops(existing_csv)

        load_progress()
        load_failed_urls()

        total_cities = len(TARGET_CITIES)

        if effective_limit is not None and _shop_count() >= effective_limit:
            msg = (
                f"Already have {_shop_count()} shops >= limit of "
                f"{effective_limit}. No scraping needed."
            )
            print(f"\n{msg}")
            save_csv(True, output)
            result.update(status="already_done", shops=_shop_count(), message=msg)
            update_progress_bar(
                total_cities, total_cities, msg, city="", query="", workers=workers
            )
            return result

        print(
            f"\nStarting scrape with {workers} parallel worker(s), "
            f"browser={browser_name}, max_per_query={max_per_query}"
        )

        job_queue: queue.Queue = queue.Queue()
        for city_obj in TARGET_CITIES:
            job_queue.put(city_obj)

        statuses: list[str] = []
        with ThreadPoolExecutor(max_workers=workers, thread_name_prefix="maps") as pool:
            futures = [
                pool.submit(
                    _worker_run,
                    worker_id,
                    job_queue,
                    total_cities=total_cities,
                    max_per_query=max_per_query,
                    effective_limit=effective_limit,
                    output=output,
                    headed=headed,
                    browser_name=browser_name,
                    controller=controller,
                    workers=workers,
                )
                for worker_id in range(workers)
            ]
            for fut in as_completed(futures):
                try:
                    statuses.append(fut.result())
                except Exception as err:
                    statuses.append("error")
                    with _state_lock:
                        sys.stdout.write("\n")
                        print(f"Worker crashed: {err}")

        stopped = "stopped" in statuses or (controller and controller.is_stopped)

        # Final pass over failed place URLs (from this run or prior).
        with _state_lock:
            pending_failed = len(failed_urls)
        if pending_failed and not stopped and not _limit_reached(effective_limit):
            update_progress_bar(
                _cities_finished,
                total_cities,
                f"Retrying {pending_failed} failed place URL(s)...",
                workers=workers,
            )
            with sync_playwright() as p:
                browser, page = _launch_browser_page(
                    p, headed=headed, browser_name=browser_name
                )
                try:
                    retry_failed_place_urls(
                        page,
                        output=output,
                        effective_limit=effective_limit,
                        workers=workers,
                    )
                finally:
                    browser.close()

        if stopped:
            result["status"] = "stopped"
            save_csv(False, output)
            with _state_lock:
                still_failed = len(failed_urls)
            result["message"] = (
                f"Stopped. Saved {_shop_count()} shops so far"
                + (f" ({still_failed} failed URLs queued)." if still_failed else ".")
            )
        else:
            update_progress_bar(
                total_cities,
                total_cities,
                "Compiling final output...",
                workers=workers,
            )
            save_csv(True, output)
            if PROGRESS_FILE.exists() and "error" not in statuses:
                # Keep progress file if a worker errored mid-run
                if _cities_finished >= total_cities or _limit_reached(effective_limit):
                    PROGRESS_FILE.unlink()
            with _state_lock:
                still_failed = len(failed_urls)
            result["message"] = (
                f"Finished with {_shop_count()} shops using {workers} worker(s)"
                + (f"; {still_failed} failed URLs still queued" if still_failed else "")
            )

            if enrich_missing and not (controller and controller.is_stopped):
                update_progress_bar(
                    total_cities,
                    total_cities,
                    "Refilling missing Phone/Website/Postal fields...",
                    workers=workers,
                )
                enrich_result = enrich_shops_csv(
                    output,
                    headed=headed,
                    browser=browser_name,
                    workers=workers,
                    limit=enrich_limit,
                    controller=controller,
                    on_progress=on_progress,
                )
                result["enrichment"] = enrich_result
                result["message"] += f" | {enrich_result.get('message') or 'enrichment done'}"
                if enrich_result.get("status") == "stopped":
                    result["status"] = "stopped"

        result["shops"] = _shop_count()
        return result
    except Exception as err:
        result.update(status="error", shops=_shop_count(), message=str(err))
        raise
    finally:
        _active_controller = None
        _progress_callback = None
        _run_started_at = None


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Scrape Canadian butcher shops from Google Maps via Playwright."
    )
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT)
    parser.add_argument("--existing-csv", type=Path, default=None)
    parser.add_argument("--limit", type=int, default=None)
    parser.add_argument("--max-per-query", type=int, default=DEFAULT_MAX_PER_QUERY)
    parser.add_argument(
        "--workers",
        type=int,
        default=DEFAULT_WORKERS,
        help=f"Parallel browser workers (default: {DEFAULT_WORKERS}).",
    )
    parser.add_argument(
        "--headed",
        action="store_true",
        help="Show the browser window (default is headless).",
    )
    parser.add_argument(
        "--browser",
        choices=SUPPORTED_BROWSERS,
        default=DEFAULT_BROWSER,
        help=f"Playwright browser to use (default: {DEFAULT_BROWSER}).",
    )
    parser.add_argument(
        "--gui",
        action="store_true",
        help="Launch the desktop GUI instead of CLI mode.",
    )
    parser.add_argument(
        "--enrich-only",
        action="store_true",
        help="Only refill missing Phone/Website/Postal/Address fields in --output CSV.",
    )
    parser.add_argument(
        "--skip-enrich",
        action="store_true",
        help="Skip the post-scrape missing-field refill pass.",
    )
    parser.add_argument(
        "--enrich-limit",
        type=int,
        default=None,
        help="Max shops to enrich in the refill pass (default: all needing fields).",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    if args.gui:
        from gui import run_gui

        run_gui()
        return

    try:
        if args.enrich_only:
            result = enrich_shops_csv(
                args.output,
                headed=args.headed,
                browser=args.browser,
                workers=args.workers,
                limit=args.enrich_limit or args.limit,
            )
            print(result.get("message") or result)
            if result.get("status") == "error":
                sys.exit(1)
            return

        scrape_google_maps(
            output=args.output,
            existing_csv=args.existing_csv,
            limit=args.limit,
            max_per_query=args.max_per_query,
            headed=args.headed,
            workers=args.workers,
            browser=args.browser,
            enrich_missing=not args.skip_enrich,
            enrich_limit=args.enrich_limit,
        )
    except Exception as err:
        print(f"\nFatal scraper error: {err}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
