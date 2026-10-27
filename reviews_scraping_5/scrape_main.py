"""Scrape Google Maps reviews without an API key and write them to a CSV file."""

from __future__ import annotations

import argparse
import asyncio
import csv
import json
import re
import signal
import sys
import time
import urllib.error
import urllib.request
from pathlib import Path
from typing import Any
from urllib.parse import quote_plus

try:
    from gmaps_scraper import GoogleMapsScraper, ScrapeConfig
    from playwright.async_api import Error as PlaywrightError
    from playwright.async_api import TimeoutError as PlaywrightTimeoutError
except ImportError as error:
    raise SystemExit(
        "Scraper dependencies missing. Install with:\n"
        "  pip install google-maps-scraper playwright\n"
        "  playwright install firefox"
    ) from error

try:
    from tqdm import tqdm
except ImportError as error:
    raise SystemExit(
        "Progress bar dependency missing. Install with:\n"
        "  pip install tqdm"
    ) from error

import threading

tqdm.set_lock(threading.RLock())


CSV_COLUMNS = [
    "shopId",
    "shopName",
    "googlePlaceId",
    "googleReviewId",
    "authorName",
    "rating",
    "content",
    "relativeTime",
    "source",
    "error",
]

CHECKPOINT_DEFAULT_PATH = "scraper_checkpoint.json"
INFLIGHT_DEFAULT_PATH = "scraper_inflight.json"


# ==============================================================================
# Custom Exceptions for Graceful Exit
# ==============================================================================


class FatalScrapeError(Exception):
    """Base exception to trigger immediate graceful script exit without checkpointing."""


class CaptchaBlockedError(Exception):
    """Raised when Google presents a CAPTCHA, sorry page, or soft throttle.

    Handled by closing the browser and reopening a fresh incognito session,
    then retrying the same shop. Not fatal unless restart budget is exhausted.
    """


class RetryableShopError(Exception):
    """Shop failed in a way that should be retried (not checkpointed as done)."""


class NetworkDisconnectedError(FatalScrapeError):
    """Raised when internet connection is lost completely across retries."""


# Max times we restart the browser for CAPTCHA/throttle before giving up.
MAX_BROWSER_RESTARTS = 5
# Cool-down after reopening a fresh incognito browser (seconds).
BROWSER_RESTART_COOLDOWN_S = 5.0
# After all workers finish a pass, reload failed/incomplete shops this many times.
MAX_FAIL_RELOAD_PASSES = 2

# Auto-concurrency from download speed (Mbps → parallel shops).
# Each worker gets its own Firefox browser. Cap low — Maps is heavy (~1–2GB each).
SPEED_TEST_BYTES = 5_000_000  # ~5 MB probe
DEFAULT_MAX_CONCURRENCY = 3


# ==============================================================================
# Live Progress (single overall bar + status lines — PowerShell-safe)
# ==============================================================================


class ProgressHub:
    """One overall tqdm bar; per-shop updates go to postfix / tqdm.write."""

    def __init__(self, total_shops: int) -> None:
        self._lock = threading.RLock()
        self._active: dict[str, str] = {}
        self._shop_pct: dict[str, float] = {}
        self._worker_ids: dict[str, int] = {}
        self._last_emit = 0.0
        self._last_worker_emit: dict[int, float] = {}
        self.bar = tqdm(
            total=total_shops,
            desc="Shops",
            unit="shop",
            file=sys.stderr,
            leave=True,
            dynamic_ncols=True,
            mininterval=0.3,
            bar_format="{l_bar}{bar}| {n_fmt}/{total_fmt} [{elapsed}<{remaining}] {postfix}",
        )
        self._emit_progress("", "starting", force=True)

    def set_shop_status(
        self,
        shop_key: str,
        label: str,
        stage: str,
        percent: float,
        worker_id: int | None = None,
    ) -> None:
        with self._lock:
            self._active[shop_key] = f"{label}: {stage} ({percent:.0f}%)"
            self._shop_pct[shop_key] = max(0.0, min(100.0, float(percent)))
            if worker_id is not None:
                self._worker_ids[shop_key] = worker_id
            self._refresh_postfix()
            self._emit_progress(label, stage)
            if worker_id is not None:
                self._emit_worker_progress(
                    worker_id, percent, shop_key, label, stage
                )

    def clear_shop(self, shop_key: str) -> None:
        with self._lock:
            worker_id = self._worker_ids.pop(shop_key, None)
            self._active.pop(shop_key, None)
            self._shop_pct.pop(shop_key, None)
            self._refresh_postfix()
            if worker_id is not None:
                self._emit_worker_idle(worker_id)

    def _refresh_postfix(self) -> None:
        if not self._active:
            self.bar.set_postfix_str("", refresh=True)
            return
        # Keep postfix short so PowerShell doesn't wrap into garbage
        parts = list(self._active.values())
        if len(parts) == 1:
            text = parts[0]
        else:
            text = " | ".join(parts)
        if len(text) > 90:
            text = text[:87] + "..."
        self.bar.set_postfix_str(text, refresh=True)

    def overall_fraction(self) -> float:
        """Completed shops + in-flight shop fractions, as a 0..total units value."""
        done = float(self.bar.n or 0)
        # Each active shop contributes 0..1 of a shop unit (not averaged).
        in_flight = sum(p / 100.0 for p in self._shop_pct.values())
        total = float(self.bar.total or 0)
        if total <= 0:
            return 0.0
        return min(total, done + in_flight)

    def overall_percent(self) -> float:
        total = float(self.bar.total or 0)
        if total <= 0:
            return 0.0
        return 100.0 * self.overall_fraction() / total

    def _emit_progress(
        self,
        shop: str,
        stage: str,
        *,
        force: bool = False,
    ) -> None:
        """Emit authoritative overall % for the GUI.

        Format: ::progress::{pct}::{done}::{total}::{shop}::{stage}
        """
        now = time.monotonic()
        # Always emit stage changes that look like loading heartbeats
        stage_l = (stage or "").lower()
        important = stage_l.startswith("loading") or stage_l.startswith("collecting")
        if not force and not important and (now - self._last_emit) < 0.2:
            return
        if not force and important and (now - self._last_emit) < 0.35:
            # Still throttle heartbeats a bit so we don't flood the pipe
            return
        self._last_emit = now
        done = int(self.bar.n or 0)
        total = int(self.bar.total or 0)
        pct = self.overall_percent()
        shop_s = str(shop).replace("\n", " ").replace("::", ":")
        stage_s = str(stage).replace("\n", " ").replace("::", ":")
        tqdm.write(
            f"::progress::{pct:.2f}::{done}::{total}::{shop_s}::{stage_s}",
            file=sys.stderr,
        )

    def _emit_worker_progress(
        self,
        worker_id: int,
        percent: float,
        shop_key: str,
        label: str,
        stage: str,
        *,
        force: bool = False,
    ) -> None:
        """Emit per-Firefox-worker progress for the GUI.

        Format: ::worker_progress::{wid}::{pct}::{shop_key}::{shop}::{stage}
        """
        now = time.monotonic()
        last = self._last_worker_emit.get(worker_id, 0.0)
        stage_l = (stage or "").lower()
        important = stage_l.startswith("loading") or stage_l.startswith("collecting")
        if not force and not important and (now - last) < 0.2:
            return
        if not force and important and (now - last) < 0.35:
            return
        self._last_worker_emit[worker_id] = now
        pct = max(0.0, min(100.0, float(percent)))
        key_s = str(shop_key).replace("\n", " ").replace("::", ":")
        shop_s = str(label).replace("\n", " ").replace("::", ":")
        stage_s = str(stage).replace("\n", " ").replace("::", ":")
        tqdm.write(
            f"::worker_progress::{worker_id}::{pct:.1f}::{key_s}::{shop_s}::{stage_s}",
            file=sys.stderr,
        )

    def _emit_worker_idle(self, worker_id: int) -> None:
        tqdm.write(f"::worker_idle::{worker_id}", file=sys.stderr)

    def shop_done(self, label: str, ok: bool, detail: str) -> None:
        mark = "✅" if ok else "⚠️"
        with self._lock:
            tqdm.write(f"{mark} {label} — {detail}", file=sys.stderr)
            self.bar.update(1)
            self.bar.refresh()
            self._emit_progress(label, detail, force=True)
            # Structured line for the GUI end-of-run summary
            status = "ok" if ok else "fail"
            safe_detail = str(detail or "").replace("::", " - ").replace("\n", " ")
            tqdm.write(
                f"::shop_result::{status}::{label}::{safe_detail}",
                file=sys.stderr,
            )

    def write(self, message: str) -> None:
        with self._lock:
            tqdm.write(message, file=sys.stderr)

    def close(self) -> None:
        with self._lock:
            self.bar.close()


class ShopProgress:
    """Per-shop progress that reports into a shared ProgressHub (no stacked bars)."""

    def __init__(
        self,
        shop_name: str,
        hub: ProgressHub | None = None,
        shop_key: str | None = None,
        worker_id: int | None = None,
    ) -> None:
        label = (shop_name or "shop").strip() or "shop"
        if len(label) > 40:
            label = label[:37] + "..."
        self.label = label
        self.hub = hub
        self.shop_key = shop_key or label
        self.worker_id = worker_id
        self._percent = 0.0
        self._stage = "starting"
        self._closed = False
        self.set_stage("starting", 0)

    def set_stage(self, stage: str, percent: float) -> None:
        if self._closed:
            return
        self._percent = max(0.0, min(100.0, float(percent)))
        self._stage = stage
        if self.hub:
            self.hub.set_shop_status(
                self.shop_key,
                self.label,
                stage,
                self._percent,
                worker_id=self.worker_id,
            )

    def update_loading(self, current: int, target: int | None) -> None:
        if target and target > 0:
            frac = min(1.0, current / target)
            percent = 10.0 + frac * 70.0
            stage = f"loading {current}/{target}"
        else:
            percent = min(80.0, 10.0 + current * 0.4)
            stage = f"loading {current}"
        self.set_stage(stage, percent)

    def update_extracting(self, current: int, total: int) -> None:
        if total > 0:
            frac = min(1.0, current / total)
            percent = 80.0 + frac * 20.0
            stage = f"extracting {current}/{total}"
        else:
            percent = 100.0
            stage = "extracting"
        self.set_stage(stage, percent)

    def finish(self, ok: bool = True, detail: str = "") -> None:
        if self._closed:
            return
        self._closed = True
        stage = detail or ("done" if ok else "failed")
        self._stage = stage
        self._percent = 100.0 if ok else self._percent
        if self.hub:
            self.hub.clear_shop(self.shop_key)
            # When hub owns the completion count/line, skip duplicate write here
            if not getattr(self, "_hub_counts_completion", False):
                self.hub.write(f"{'✅' if ok else '⚠️'} {self.label} — {stage}")
        else:
            print(f"{'✅' if ok else '⚠️'} {self.label} — {stage}", file=sys.stderr)


# ==============================================================================
# Helper / Data Parsing Functions
# ==============================================================================


async def page_is_blocked_or_throttled(page) -> bool:
    """Detect CAPTCHA, /sorry/ interstitial, or soft throttle pages."""
    try:
        url = (page.url or "").lower()
        if "google.com/sorry/" in url or "/sorry/index" in url:
            return True
        if await page.locator('iframe[src*="recaptcha"]').count() > 0:
            return True
        body_text = ""
        try:
            body_text = (await page.locator("body").inner_text(timeout=2000)).lower()
        except Exception:
            pass
        throttle_markers = (
            "unusual traffic",
            "detected unusual",
            "our systems have detected",
            "why did this happen",
            "not a robot",
        )
        if any(marker in body_text for marker in throttle_markers):
            return True
    except Exception:
        return False
    return False


async def reopen_incognito_browser(scraper: GoogleMapsScraper) -> None:
    """Close the current browser entirely and launch a fresh incognito session."""
    print(
        "♻️  Closing current browser and opening a fresh incognito session...",
        file=sys.stderr,
    )
    await scraper.close()
    await scraper.start()
    print(
        f"  ✅ Fresh incognito browser ready. Cooling down {BROWSER_RESTART_COOLDOWN_S:.0f}s...",
        file=sys.stderr,
    )
    await asyncio.sleep(BROWSER_RESTART_COOLDOWN_S)


async def measure_download_speed_mbps(test_bytes: int = SPEED_TEST_BYTES) -> float | None:
    """Rough download speed probe via Cloudflare. Returns Mbps or None on failure."""

    def _measure() -> float:
        url = f"https://speed.cloudflare.com/__down?bytes={test_bytes}"
        request = urllib.request.Request(
            url,
            headers={"User-Agent": "reviews-scraper-speedcheck/1.0"},
        )
        start = time.perf_counter()
        with urllib.request.urlopen(request, timeout=30) as response:
            data = response.read()
        elapsed = time.perf_counter() - start
        if elapsed <= 0:
            raise RuntimeError("Speed probe returned zero elapsed time")
        return (len(data) * 8) / elapsed / 1_000_000

    try:
        print("📶 Measuring download speed to pick concurrency...", file=sys.stderr)
        mbps = await asyncio.to_thread(_measure)
        print(f"  ✅ Measured ~{mbps:.1f} Mbps download.", file=sys.stderr)
        return mbps
    except (urllib.error.URLError, OSError, RuntimeError) as error:
        print(f"  ⚠️ Speed check failed ({error}). Falling back to concurrency=1.", file=sys.stderr)
        return None


def concurrency_from_speed(mbps: float | None, max_concurrency: int = DEFAULT_MAX_CONCURRENCY) -> int:
    """Map download Mbps → parallel shop count (conservative for Google Maps)."""
    max_concurrency = max(1, min(max_concurrency, DEFAULT_MAX_CONCURRENCY))
    if mbps is None:
        return 1
    if mbps < 5:
        chosen = 1
    elif mbps < 15:
        chosen = 2
    elif mbps < 40:
        chosen = 3
    else:
        chosen = 4
    return max(1, min(chosen, max_concurrency))


async def resolve_concurrency(explicit: int | None, max_concurrency: int) -> int:
    """Use --concurrency if set, otherwise measure speed and auto-pick.

    Each parallel worker uses a separate Firefox browser (not shared tabs).
    """
    max_concurrency = max(1, min(max_concurrency, DEFAULT_MAX_CONCURRENCY))
    if explicit is not None and explicit > 0:
        chosen = max(1, min(explicit, max_concurrency))
        if explicit > chosen:
            print(
                f"⚙️  Requested concurrency={explicit}, capped at {chosen} "
                f"(max {max_concurrency} Firefox browsers).",
                file=sys.stderr,
            )
        else:
            print(
                f"⚙️  Using concurrency={chosen} "
                f"({chosen} separate Firefox browser(s)).",
                file=sys.stderr,
            )
        return chosen
    mbps = await measure_download_speed_mbps()
    chosen = concurrency_from_speed(mbps, max_concurrency=max_concurrency)
    print(
        f"⚙️  Auto concurrency set to {chosen} "
        f"({chosen} separate Firefox browser(s)).",
        file=sys.stderr,
    )
    return chosen


class FirefoxWorkerPool:
    """One GoogleMapsScraper (Firefox) per parallel worker — isolated sessions."""

    def __init__(self, config: ScrapeConfig, size: int) -> None:
        self.config = config
        self.size = max(1, size)
        self.scrapers: list[GoogleMapsScraper] = []
        self._available: asyncio.Queue[GoogleMapsScraper] = asyncio.Queue()

    async def start(self) -> None:
        print(
            f"::log::Launching {self.size} Firefox browser(s) for parallel scraping…",
            file=sys.stderr,
            flush=True,
        )
        for i in range(self.size):
            scraper = GoogleMapsScraper(self.config)
            await scraper.start()
            self.scrapers.append(scraper)
            await self._available.put(scraper)
            print(
                f"::log::  Firefox worker {i + 1}/{self.size} ready",
                file=sys.stderr,
                flush=True,
            )

    async def acquire(self) -> GoogleMapsScraper:
        return await self._available.get()

    async def release(self, scraper: GoogleMapsScraper) -> None:
        await self._available.put(scraper)

    async def restart(self, scraper: GoogleMapsScraper) -> None:
        await reopen_incognito_browser(scraper)

    async def restart_all(self) -> None:
        # Drain queue, restart every browser, refill
        held: list[GoogleMapsScraper] = []
        while not self._available.empty():
            held.append(await self._available.get())
        # If some workers still hold scrapers, restart those we have; callers
        # should only call this when all workers are idle (between batches).
        targets = held if held else list(self.scrapers)
        for scraper in targets:
            await reopen_incognito_browser(scraper)
            await self._available.put(scraper)

    async def close(self) -> None:
        for scraper in self.scrapers:
            try:
                await scraper.close()
            except Exception:
                pass
        self.scrapers.clear()


_HEX_PLACE_ID_RE = re.compile(r"^0x[0-9a-fA-F]+:0x[0-9a-fA-F]+$")
_CHIJ_PLACE_ID_RE = re.compile(r"^ChIJ[\w-]+$")


def build_search_query(shop: dict[str, Any]) -> str:
    """Build a text search query without duplicating city/province already in address."""
    name = (str(shop.get("name") or "").strip() or None)
    address = (str(shop.get("address") or "").strip() or None)
    city = (str(shop.get("city") or "").strip() or None)
    province = (str(shop.get("province") or "").strip() or None)

    parts: list[str] = []
    if name:
        parts.append(name)
    if address:
        parts.append(address)
        addr_l = address.lower()
        if city and city.lower() not in addr_l:
            parts.append(city)
        if province and province.lower() not in addr_l:
            parts.append(province)
    else:
        if city:
            parts.append(city)
        if province:
            parts.append(province)
    parts.append("Canada")
    return ", ".join(parts)


def build_maps_url(shop: dict[str, Any]) -> str:
    """Prefer a direct place deep-link when googlePlaceId is known."""
    place_id = (str(shop.get("googlePlaceId") or "").strip() or None)
    name = (str(shop.get("name") or "place").strip() or "place")
    slug = quote_plus(name)
    lat = shop.get("latitude")
    lng = shop.get("longitude")

    if place_id and _HEX_PLACE_ID_RE.match(place_id):
        # Feature-id deep link (common in this project's CSVs)
        if lat is not None and lng is not None:
            return (
                f"https://www.google.com/maps/place/{slug}/@{lat},{lng},17z/"
                f"data=!3m1!4b1!4m6!3m5!1s{place_id}!8m2!3d{lat}!4d{lng}"
            )
        return (
            f"https://www.google.com/maps/place/{slug}/data=!4m2!3m1!1s{place_id}"
        )

    if place_id and _CHIJ_PLACE_ID_RE.match(place_id):
        # Places API ChIJ… id
        return (
            "https://www.google.com/maps/search/?api=1"
            f"&query={slug}&query_place_id={quote_plus(place_id)}"
        )

    query = build_search_query(shop)
    if lat is not None and lng is not None:
        return (
            "https://www.google.com/maps/search/"
            f"{quote_plus(query)}/@{lat},{lng},17z"
        )
    return f"https://www.google.com/maps/search/{quote_plus(query)}"


def extract_place_id(url: str) -> str | None:
    match = re.search(r"!1s(0x[0-9a-fA-F]+:0x[0-9a-fA-F]+)", url)
    if match:
        return match.group(1)
    match = re.search(r"0x[0-9a-fA-F]+:0x[0-9a-fA-F]+", url)
    if match:
        return match.group(0)
    match = re.search(r"query_place_id=([^&]+)", url)
    if match:
        return match.group(1)
    return None


def parse_rating(raw: str | None) -> int | None:
    if not raw:
        return None
    match = re.search(r"(\d)", raw)
    if not match:
        return None
    rating = int(match.group(1))
    if 1 <= rating <= 5:
        return rating
    return None


async def get_total_reviews_count(page) -> int | None:
    """Read the place-level review total from the Reviews panel header.

    Ignores per-author lines like "17 reviews" inside individual review cards.
    Prefers the Reviews tab aria-label when present (avoids search-page noise).
    """
    try:
        total = await page.evaluate(
            """() => {
                const inCard = (el) => !!(el && el.closest && el.closest('div.jftiEf'));
                const parseCount = (text) => {
                    if (!text) return null;
                    let m = text.match(/([\\d\\s,.\\u00a0]+)\\s+reviews?/i);
                    if (m) {
                        const n = m[1].replace(/[^\\d]/g, '');
                        if (n) return parseInt(n, 10);
                    }
                    m = text.match(/\\(([\\d\\s,.\\u00a0]+)\\)/);
                    if (m) {
                        const n = m[1].replace(/[^\\d]/g, '');
                        if (n) return parseInt(n, 10);
                    }
                    return null;
                };

                // Strongest signal: Reviews tab label, e.g. "Reviews for X 120 reviews"
                const tabSels = [
                    'button[role="tab"][aria-label*="Reviews" i]',
                    'div[role="tab"][aria-label*="Reviews" i]',
                    'button[role="tab"][aria-label*="reviews"]',
                ];
                for (const sel of tabSels) {
                    for (const el of document.querySelectorAll(sel)) {
                        if (inCard(el)) continue;
                        const n = parseCount(el.getAttribute('aria-label') || '');
                        if (n && n > 0) return n;
                    }
                }

                const selectors = [
                    'div.jANuvb',
                    'div.fontBodySmall',
                    'button.fontBodySmall',
                    'span.fontBodySmall',
                    'div.F7nice',
                    'button[jsaction*="pane.rating"]',
                ];
                const found = [];
                for (const sel of selectors) {
                    for (const el of document.querySelectorAll(sel)) {
                        if (inCard(el)) continue;
                        const n = parseCount(el.getAttribute('aria-label') || el.textContent || '');
                        if (n && n > 0) found.push(n);
                    }
                }
                // Only scan near reviews header when a Reviews tab is selected / cards present
                const tabOn = [...document.querySelectorAll(
                    'button[role="tab"][aria-label*="Reviews" i], div[role="tab"][aria-label*="Reviews" i]'
                )].some(t => !inCard(t) && (
                    t.getAttribute('aria-selected') === 'true' ||
                    t.getAttribute('aria-pressed') === 'true'
                ));
                const hasCards = !!document.querySelector('div.jftiEf');
                if (tabOn || hasCards) {
                    const feed = document.querySelector('div[role="feed"]');
                    if (feed && feed.parentElement) {
                        for (const el of feed.parentElement.querySelectorAll('span, div, button')) {
                            if (inCard(el)) continue;
                            const t = (el.textContent || '').trim();
                            if (t.length > 40) continue;
                            const n = parseCount(t);
                            if (n && n > 0) found.push(n);
                        }
                    }
                }
                if (!found.length) return null;
                // Prefer modest place-level counts; avoid inflated search-page aggregates
                found.sort((a, b) => a - b);
                return found[found.length - 1];
            }"""
        )
        if isinstance(total, int) and total > 0:
            return total
    except Exception:
        pass

    # Fallback: previous locator approach, still skipping review cards
    selectors = [
        'div.fontBodySmall:has-text("reviews")',
        'button:has-text("reviews")',
        'div.jANuvb',
    ]
    for selector in selectors:
        try:
            elements = page.locator(selector)
            count = await elements.count()
            for i in range(count):
                el = elements.nth(i)
                if await el.locator("xpath=ancestor::div[contains(@class,'jftiEf')]").count() > 0:
                    continue
                text = await el.inner_text()
                match = re.search(r"([\d\s,\.]+)\s+reviews", text, re.IGNORECASE)
                if match:
                    raw_num = re.sub(r"[^\d]", "", match.group(1))
                    if raw_num.isdigit():
                        return int(raw_num)
        except Exception:
            continue
    return None


async def _page_has_reviews_tab(page) -> bool:
    """True when a place-level Reviews tab is present in the DOM."""
    try:
        return bool(
            await page.evaluate(
                """() => {
                    const sels = [
                        'button[role="tab"][aria-label*="Reviews" i]',
                        'div[role="tab"][aria-label*="Reviews" i]',
                        'button[role="tab"][aria-label*="reviews"]',
                        'button[role="tab"][aria-label*="Avis" i]',
                        'button[jsaction*="pane.rating.moreReviews"]',
                    ];
                    for (const sel of sels) {
                        for (const el of document.querySelectorAll(sel)) {
                            if (el.closest('div.jftiEf')) continue;
                            // offsetParent null can still be in a scrollable panel — check rect
                            const r = el.getBoundingClientRect();
                            if (r.width > 0 && r.height > 0) return true;
                            if (el.getAttribute('aria-label')) return true;
                        }
                    }
                    return false;
                }"""
            )
        )
    except Exception:
        return False


async def _page_looks_like_place_detail(page) -> bool:
    """True when the left panel is a usable place detail (tabs / h1), not merely a /place/ URL.

    A /maps/place/ deep-link can land with the right URL but an empty sidebar
    (no Reviews tab). Do NOT treat URL alone as confirmation.
    """
    try:
        return bool(
            await page.evaluate(
                """() => {
                    const inCard = (el) => !!(el && el.closest && el.closest('div.jftiEf'));
                    // Reviews / About / Overview tabs = real place panel
                    const tabs = document.querySelectorAll('button[role="tab"], div[role="tab"]');
                    let placeTabs = 0;
                    for (const tab of tabs) {
                        if (inCard(tab)) continue;
                        const label = (tab.getAttribute('aria-label') || tab.textContent || '').toLowerCase();
                        if (/review|avis|about|overview|photo|menu/.test(label)) {
                            placeTabs += 1;
                        }
                    }
                    if (placeTabs >= 1) return true;

                    // Place title h1 (not a search-results title)
                    const searchTitles = new Set([
                        'results', '結果', '検索結果', '搜尋結果', '搜索结果',
                    ]);
                    for (const sel of ['h1.DUwDvf', 'h1.lfPIob', 'div[role="main"] h1']) {
                        const h = document.querySelector(sel);
                        if (!h) continue;
                        const t = (h.textContent || '').trim();
                        if (t && !searchTitles.has(t) && t.length > 1) {
                            // Prefer h1 that sits with a rating / review affordance
                            const main = h.closest('div[role="main"]') || document;
                            const hasRating = !!main.querySelector(
                                'button[jsaction*="pane.rating"], div.F7nice, span[aria-label*="star" i]'
                            );
                            if (hasRating || placeTabs > 0) return true;
                        }
                    }
                    return false;
                }"""
            )
        )
    except Exception:
        return False


async def _dismiss_maps_overlays(page, progress: ShopProgress | None = None) -> None:
    """Dismiss consent / blocking modals only — never Escape/Close (those close the place panel)."""
    try:
        closed = await page.evaluate(
            """() => {
                let n = 0;
                // Consent / cookie only — do NOT click generic Close (closes place sidebar)
                const consent = document.querySelectorAll(
                    'button[aria-label*="Accept all" i],' +
                    'button[aria-label*="Accept" i],' +
                    'button[aria-label*="Agree" i],' +
                    'button[aria-label*="I agree" i],' +
                    'form[action*="consent"] button'
                );
                for (const el of consent) {
                    const label = (el.getAttribute('aria-label') || el.textContent || '').toLowerCase();
                    // Skip reject / customize
                    if (/reject|customize|manage|refuse/.test(label)) continue;
                    try { el.click(); n += 1; } catch (e) {}
                }
                // Only click modal backdrop if it covers most of the viewport
                for (const el of document.querySelectorAll(
                    'div.hoUMge[jsaction*="modal.backgroundClick"]'
                )) {
                    const r = el.getBoundingClientRect();
                    if (r.width > window.innerWidth * 0.5 && r.height > window.innerHeight * 0.5) {
                        try { el.click(); n += 1; } catch (e) {}
                    }
                }
                return n;
            }"""
        )
        if closed:
            _progress_log(progress, f"[nav] Dismissed {closed} consent/overlay control(s)")
            await page.wait_for_timeout(250)
    except Exception:
        pass
    # Do NOT press Escape — it collapses the Maps place panel / Reviews UI.


async def _wait_for_reviews_controls(page, timeout_ms: int = 12000) -> bool:
    """Poll until a Reviews tab / more-reviews control appears."""
    deadline = time.time() + timeout_ms / 1000.0
    while time.time() < deadline:
        if await _page_has_reviews_tab(page):
            return True
        state = await _reviews_panel_state(page)
        if state.get("cards", 0) >= 5:
            return True
        await page.wait_for_timeout(400)
    return await _page_has_reviews_tab(page)


async def _fallback_open_via_search(
    page,
    shop: dict[str, Any],
    scraper: Any,
    progress: ShopProgress | None = None,
) -> bool:
    """When a place deep-link has no Reviews tab, reopen via text search and click the result."""
    query = build_search_query(shop)
    lat = shop.get("latitude")
    lng = shop.get("longitude")
    if lat is not None and lng is not None:
        url = (
            "https://www.google.com/maps/search/"
            f"{quote_plus(query)}/@{lat},{lng},17z"
        )
    else:
        url = f"https://www.google.com/maps/search/{quote_plus(query)}"

    _progress_log(progress, f"[nav] Fallback search navigation: {url[:120]}")
    try:
        await page.goto(url, wait_until="domcontentloaded", timeout=30000)
    except Exception as err:
        _progress_log(progress, f"[nav] Fallback navigation failed: {err}")
        return False

    await scraper._handle_consent(page)
    await _dismiss_maps_overlays(page, progress)
    await scraper._wait_for_place_panel(page)
    ok = await ensure_place_detail(page, shop, scraper, progress=progress)
    if not ok:
        return False
    await page.wait_for_timeout(500)
    await _wait_for_reviews_controls(page, timeout_ms=10000)
    return await _page_has_reviews_tab(page) or await _page_looks_like_place_detail(page)


async def ensure_place_detail(
    page,
    shop: dict[str, Any],
    scraper: Any,
    progress: ShopProgress | None = None,
) -> bool:
    """If still on a search-results list, open the matching place (or first result)."""
    await _dismiss_maps_overlays(page, progress)

    if await _page_looks_like_place_detail(page):
        # Deep links can show a thin panel — wait briefly for tabs to hydrate
        if not await _page_has_reviews_tab(page):
            _progress_log(progress, "[nav] Place panel seen — waiting for Reviews tab…")
            await _wait_for_reviews_controls(page, timeout_ms=8000)
        if await _page_has_reviews_tab(page) or await _page_looks_like_place_detail(page):
            _progress_log(
                progress,
                "[nav] Place detail panel confirmed"
                + (" (Reviews tab present)" if await _page_has_reviews_tab(page) else ""),
            )
            return True

    _progress_log(
        progress,
        "[nav] Still on search/results — trying to open the place…",
    )

    # Prefer a result whose title matches the shop name
    shop_name = (shop.get("name") or "").strip().lower()
    try:
        clicked = await page.evaluate(
            """(shopName) => {
                const links = [...document.querySelectorAll(
                    'div[role="feed"] a[href*="/maps/place/"], a.hfpxzc, div[role="feed"] a'
                )];
                const norm = (s) => (s || '').toLowerCase().replace(/\\s+/g, ' ').trim();
                const target = norm(shopName);
                let best = null;
                for (const a of links) {
                    if (a.closest('div.jftiEf')) continue;
                    const label = norm(
                        a.getAttribute('aria-label') || a.textContent || ''
                    );
                    if (!label) continue;
                    if (target && (label.includes(target) || target.includes(label.split(',')[0]))) {
                        best = a;
                        break;
                    }
                    if (!best) best = a;
                }
                if (!best) return false;
                best.click();
                return true;
            }""",
            shop_name,
        )
        if clicked:
            _progress_log(progress, "[nav] Clicked matching search result")
            try:
                await page.wait_for_url("**/place/**", timeout=8000)
            except Exception:
                await page.wait_for_timeout(1000)
            await page.wait_for_timeout(400)
            await _wait_for_reviews_controls(page, timeout_ms=6000)
            if await _page_looks_like_place_detail(page) or await _page_has_reviews_tab(page):
                _progress_log(progress, "[nav] Place detail panel confirmed")
                return True
    except Exception as err:
        _progress_log(progress, f"[nav] Search-result click failed: {err}")

    # Library helper (clicks first result)
    try:
        await scraper._click_first_search_result(
            page,
            'div[role="feed"] a[href*="/maps/place/"], div[role="feed"] a, a.hfpxzc',
            [
                "h1.DUwDvf",
                "h1.lfPIob",
                'div[role="main"] h1',
                "h1.fontHeadlineLarge",
            ],
        )
        await page.wait_for_timeout(400)
        await _wait_for_reviews_controls(page, timeout_ms=6000)
        if await _page_looks_like_place_detail(page) or await _page_has_reviews_tab(page):
            _progress_log(progress, "[nav] Place detail via first-result fallback")
            return True
    except Exception as err:
        _progress_log(progress, f"[nav] First-result fallback failed: {err}")

    _progress_log(progress, "[nav] WARNING: Could not confirm place detail panel")
    return False


# ==============================================================================
# Browser Interactions
# ==============================================================================


async def _reviews_panel_state(page) -> dict[str, int]:
    """Count feed + review cards currently on the place panel."""
    feed_n = await page.locator('div[role="feed"]').count()
    cards_n = await page.locator("div.jftiEf").count()
    return {"feed": feed_n, "cards": cards_n}


def _reviews_panel_ready(state: dict[str, int]) -> bool:
    """Maps sometimes shows review cards without role=feed — both count as open."""
    return state.get("feed", 0) > 0 or state.get("cards", 0) > 0


async def _reviews_tab_selected(page) -> bool:
    """True when the place-panel Reviews tab is the active/selected tab."""
    try:
        selected = await page.evaluate(
            """() => {
                const tabs = document.querySelectorAll(
                    'button[role="tab"][aria-label*="Reviews" i],' +
                    'div[role="tab"][aria-label*="Reviews" i],' +
                    'button[role="tab"][aria-label*="reviews"]'
                );
                for (const tab of tabs) {
                    if (tab.closest('div.jftiEf')) continue;
                    const selected = tab.getAttribute('aria-selected');
                    if (selected === 'true') return true;
                    if (tab.getAttribute('aria-pressed') === 'true') return true;
                    if ((tab.className || '').toString().includes('HhsmVe')) return true;
                }
                return false;
            }"""
        )
        return bool(selected)
    except Exception:
        return False


async def open_full_reviews_panel(page, progress: ShopProgress | None = None) -> bool:
    """Clicks the place Reviews tab (not per-reviewer 'N reviews' profile links)."""
    _progress_log(progress, "Looking for Reviews tab...")
    await page.wait_for_timeout(500)

    # Already on reviews?
    # Overview teasers can show feed=1 with only ~3 cards — that is NOT the full list.
    # Only skip the tab click when we already have a solid card list AND the Reviews
    # tab looks selected (or we have enough cards that overview teasers are unlikely).
    state = await _reviews_panel_state(page)
    tab_on = await _reviews_tab_selected(page)
    _progress_log(
        progress,
        f"  initial state: feed={state['feed']}, cards={state['cards']}, "
        f"reviews_tab_selected={tab_on}",
    )
    if state["cards"] >= 8 and (tab_on or state["feed"] > 0):
        _progress_log(progress, "Reviews list already visible")
        return True
    if state["cards"] >= 5 and tab_on:
        _progress_log(progress, "Reviews list already visible (tab selected)")
        return True
    if state["cards"] > 0 and state["cards"] < 8 and not tab_on:
        _progress_log(
            progress,
            f"  overview teasers only ({state['cards']} cards) — clicking Reviews tab",
        )

    review_triggers = [
        'button[role="tab"][aria-label*="Reviews" i]',
        'button[role="tab"][aria-label*="reviews"]',
        'div[role="tab"][aria-label*="Reviews" i]',
        'button[role="tab"][aria-label*="Avis" i]',
        'button[aria-label*="More reviews" i]',
        'button[jsaction*="pane.rating.moreReviews"]',
        'button[jsaction*="moreReviews"]',
        'button[jsaction*="pane.reviewChart"]',
        'button[aria-label*=" reviews" i]',
        'div.F7nice button',
        'button[jsaction*="pane.rating"]',
    ]

    # Do not dismiss overlays here — Escape/Close was closing the place panel
    # right after we detected the Reviews tab.

    for selector in review_triggers:
        try:
            locators = page.locator(selector)
            count = await locators.count()
            if count == 0:
                continue
            for i in range(count):
                btn = locators.nth(i)
                if await btn.locator("xpath=ancestor::div[contains(@class,'jftiEf')]").count() > 0:
                    continue
                if await btn.is_visible():
                    _progress_log(progress, f"  clicking Reviews control ({selector})")
                    cards_before = state["cards"]
                    await btn.click(timeout=2000)
                    # Short waits — cards often appear immediately (even without role=feed)
                    for wait_i in range(5):
                        await page.wait_for_timeout(250)
                        state = await _reviews_panel_state(page)
                        tab_on = await _reviews_tab_selected(page)
                        # Full list usually jumps to ~8–10 cards; tab selected also counts
                        grew = state["cards"] > cards_before
                        opened = (
                            (tab_on and state["cards"] > 0)
                            or state["cards"] >= 8
                            or (grew and state["cards"] >= 5)
                        )
                        if opened:
                            _progress_log(
                                progress,
                                "Reviews panel open "
                                f"(feed={state['feed']}, cards={state['cards']})",
                            )
                            return True
                    # Click landed but list still thin — keep trying other selectors
                    _progress_log(
                        progress,
                        f"  click did not expand list "
                        f"(still {state['cards']} cards) — trying next control",
                    )
        except Exception as err:
            _progress_log(progress, f"  selector failed: {err}")
            continue

    try:
        _progress_log(progress, "  trying rating-summary fallback...")
        rating_summary = page.locator('div[role="main"] button[jsaction*="pane.rating"]').first
        if await rating_summary.count() > 0:
            try:
                visible = await rating_summary.is_visible(timeout=1000)
            except Exception:
                visible = False
            if visible:
                await rating_summary.click(timeout=2000)
                await page.wait_for_timeout(800)
                state = await _reviews_panel_state(page)
                if _reviews_panel_ready(state):
                    _progress_log(
                        progress,
                        f"Reviews open via rating summary "
                        f"(feed={state['feed']}, cards={state['cards']})",
                    )
                    return True
    except Exception as err:
        _progress_log(progress, f"  rating-summary fallback failed: {err}")

    # Last chance: cards may already be on the page from overview/reviews
    state = await _reviews_panel_state(page)
    if state["cards"] > 0:
        _progress_log(
            progress,
            f"Reviews cards present without role=feed "
            f"(cards={state['cards']}) — continuing anyway",
        )
        return True

    _progress_log(progress, "WARNING: Could not open Reviews panel")
    return False


def _progress_log(progress: ShopProgress | None, message: str) -> None:
    """Write a human message that shows in the GUI live log / terminal.

    ASCII-only so Windows/tkinter live log does not show \\uXXXX escapes.
    """
    # Drop non-ASCII (emoji etc.) for stable GUI display
    clean = str(message).encode("ascii", errors="ignore").decode("ascii").strip()
    if not clean:
        clean = str(message)
    if clean.startswith("::log::"):
        clean = clean[len("::log::") :].strip()
    # Prefix shop so concurrent workers don't look like only one is alive
    if progress and progress.label:
        name = re.sub(r"^\[\d+/\d+\]\s*", "", progress.label).strip() or progress.label
        if len(name) > 28:
            name = name[:25] + "..."
        clean = f"[{name}] {clean}"
    line = f"::log::{clean}"
    if progress and progress.hub:
        progress.hub.write(line)
    else:
        print(line.replace("::log::", "", 1), file=sys.stderr)


async def _expand_visible_more_buttons(page) -> int:
    """Click only review-body 'More' / 'See more'. Returns how many were clicked."""
    # Do this entirely in the page — avoids Playwright is_visible() 30s stalls
    try:
        clicked = await page.evaluate(
            """() => {
                let n = 0;
                const buttons = document.querySelectorAll(
                    'div.jftiEf button[jsaction*="review.expandText"],' +
                    'div.jftiEf button[aria-label="See more"],' +
                    'div.jftiEf button[aria-label="More"],' +
                    'div.jftiEf span.wiI7pd ~ button,' +
                    'div.jftiEf div.wiI7pd + button'
                );
                for (const btn of buttons) {
                    const aria = (btn.getAttribute('aria-label') || '').toLowerCase();
                    if (aria.includes('photo of') || aria.includes('menu')
                        || aria.includes('share') || aria.includes('like')
                        || aria.includes('actions') || aria.includes('options')) {
                        continue;
                    }
                    const label = (aria || (btn.textContent || '')).toLowerCase().trim();
                    if (label && !(label.includes('more') || label.includes('expand') || label === '')) {
                        // empty label next to body text is usually "More"
                        if (label.length > 12) continue;
                    }
                    try { btn.click(); n++; } catch (e) {}
                    if (n >= 15) break;
                }
                return n;
            }"""
        )
        return int(clicked or 0)
    except Exception:
        return 0


async def _extract_visible_review_cards(
    page, seen: set[str]
) -> list[dict[str, Any]]:
    """Parse visible review cards in one browser round-trip (no profile navigation)."""
    raw_cards: list[dict[str, Any]] = await page.evaluate(
        """() => {
            const cards = Array.from(document.querySelectorAll('div.jftiEf'));
            return cards.map(card => {
                const reviewId = card.getAttribute('data-review-id') || '';

                let authorName = '';
                const nameEl = card.querySelector('div.d4r55, div.TSUbDb, a[href*="/contrib/"]');
                if (nameEl && nameEl.textContent) {
                    authorName = nameEl.textContent.trim().split('\\n')[0].trim();
                }
                if (!authorName) {
                    const photo = card.querySelector(
                        'button[aria-label*="Photo of"], img[alt*="Photo of"], img[aria-label*="Photo of"]'
                    );
                    const raw = (photo && (photo.getAttribute('aria-label') || photo.getAttribute('alt'))) || '';
                    const m = raw.match(/Photo of\\s+(.+)/i);
                    if (m) authorName = m[1].trim();
                }
                if (/^\\d[\\d,.]*\\s+reviews?$/i.test(authorName)) authorName = '';

                let rating = null;
                const ratingEl = card.querySelector('span.kvMYJc[aria-label*="star"]');
                if (ratingEl) {
                    const label = ratingEl.getAttribute('aria-label') || '';
                    const rm = label.match(/(\\d+)/);
                    if (rm) rating = parseInt(rm[1], 10);
                }

                const contentEl = card.querySelector('span.wiI7pd');
                const content = contentEl ? contentEl.textContent.trim() : '';

                const timeEl = card.querySelector('span.rRecSc, span.rsqaWe');
                const relativeTime = timeEl ? timeEl.textContent.trim() : '';

                return { reviewId, authorName, rating, content, relativeTime };
            });
        }"""
    )

    reviews: list[dict[str, Any]] = []
    for item in raw_cards or []:
        try:
            review_id = str(item.get("reviewId") or "")
            author_name = str(item.get("authorName") or "").strip()
            content = str(item.get("content") or "").strip()
            relative_time = str(item.get("relativeTime") or "").strip()
            rating = item.get("rating")
            if rating is not None:
                try:
                    rating = int(rating)
                except (TypeError, ValueError):
                    rating = None

            if review_id and review_id in seen:
                continue
            if not review_id:
                review_id = f"anon:{author_name}|{relative_time}|{content[:80]}"
                if review_id in seen:
                    continue

            seen.add(review_id)
            reviews.append(
                {
                    "googleReviewId": review_id if not review_id.startswith("anon:") else "",
                    "authorName": author_name,
                    "rating": rating,
                    "content": content,
                    "relativeTime": relative_time,
                    "source": "google",
                    "error": None,
                }
            )
        except Exception as error:
            reviews.append(
                {
                    "googleReviewId": "",
                    "authorName": "",
                    "rating": None,
                    "content": "",
                    "relativeTime": "",
                    "source": "google",
                    "error": str(error),
                }
            )
    return reviews


async def _install_no_profile_guard(page) -> None:
    """Auto-close contributor profile popups/tabs if anything still triggers them."""

    async def _close_popup(popup) -> None:
        try:
            await popup.close()
        except Exception:
            pass

    def _on_popup(popup) -> None:
        asyncio.create_task(_close_popup(popup))

    page.on("popup", _on_popup)


_SCROLL_PROBE_JS = """() => {
    const feed = document.querySelector('div[role="feed"]');
    const cards = document.querySelectorAll('div.jftiEf');
    const card = cards.length ? cards[cards.length - 1] : null;
    const start = feed || card;
    if (!start) return null;
    let el = start;
    for (let i = 0; i < 16 && el; i++) {
        const style = window.getComputedStyle(el);
        const oy = style.overflowY;
        if ((oy === 'auto' || oy === 'scroll' || oy === 'overlay')
            && el.scrollHeight > el.clientHeight + 20) {
            return {
                tag: el.tagName,
                className: (el.className || '').toString().slice(0, 60),
                top: Math.round(el.scrollTop),
                height: el.scrollHeight,
                client: el.clientHeight,
            };
        }
        el = el.parentElement;
    }
    // Also probe main panel scrollers (Maps often scrolls a parent of feed)
    const mains = document.querySelectorAll('div[role="main"]');
    for (const main of mains) {
        let node = main;
        for (let i = 0; i < 8 && node; i++) {
            const style = window.getComputedStyle(node);
            const oy = style.overflowY;
            if ((oy === 'auto' || oy === 'scroll' || oy === 'overlay')
                && node.scrollHeight > node.clientHeight + 20) {
                return {
                    tag: node.tagName,
                    className: (node.className || '').toString().slice(0, 60),
                    top: Math.round(node.scrollTop),
                    height: node.scrollHeight,
                    client: node.clientHeight,
                };
            }
            node = node.parentElement;
        }
    }
    return feed ? {
        tag: 'FEED',
        className: '',
        top: Math.round(feed.scrollTop),
        height: feed.scrollHeight,
        client: feed.clientHeight,
    } : null;
}"""


async def _recover_stuck_reviews_feed(
    page, progress: ShopProgress | None = None
) -> None:
    """Nudge a stalled reviews list: scroll up/down, re-click tab, try sort."""
    _progress_log(progress, "Recovering stuck reviews feed...")
    await _dismiss_maps_overlays(page, progress)

    # 1) Peek upward then jump to bottom — re-arms Maps lazy-load observers
    try:
        await page.evaluate(
            """() => {
                const findScrollable = () => {
                    const feed = document.querySelector('div[role="feed"]');
                    const card = document.querySelector('div.jftiEf');
                    const starts = [feed, card].filter(Boolean);
                    for (const start of starts) {
                        let el = start;
                        for (let i = 0; i < 16 && el; i++) {
                            const style = window.getComputedStyle(el);
                            const oy = style.overflowY;
                            if ((oy === 'auto' || oy === 'scroll' || oy === 'overlay')
                                && el.scrollHeight > el.clientHeight + 20) {
                                return el;
                            }
                            el = el.parentElement;
                        }
                    }
                    return null;
                };
                const scroller = findScrollable();
                if (!scroller) return false;
                const mid = Math.max(0, scroller.scrollTop - scroller.clientHeight * 3);
                scroller.scrollTop = mid;
                scroller.dispatchEvent(new Event('scroll', { bubbles: true }));
                return true;
            }"""
        )
        await page.wait_for_timeout(400)
        await page.evaluate(
            """() => {
                const feed = document.querySelector('div[role="feed"]');
                const card = document.querySelector('div.jftiEf');
                let el = feed || card;
                for (let i = 0; i < 16 && el; i++) {
                    const style = window.getComputedStyle(el);
                    const oy = style.overflowY;
                    if ((oy === 'auto' || oy === 'scroll' || oy === 'overlay')
                        && el.scrollHeight > el.clientHeight + 20) {
                        el.scrollTop = el.scrollHeight;
                        el.dispatchEvent(new Event('scroll', { bubbles: true }));
                        return true;
                    }
                    el = el.parentElement;
                }
                return false;
            }"""
        )
        await page.wait_for_timeout(500)
    except Exception as err:
        _progress_log(progress, f"  recovery scroll nudge failed: {err}")

    # 2) Re-click Reviews tab if present (forces panel refresh without leaving place)
    try:
        tab = page.locator('button[role="tab"][aria-label*="Reviews" i]').first
        if await tab.count() > 0 and await tab.is_visible():
            await tab.click(timeout=2000)
            await page.wait_for_timeout(700)
            _progress_log(progress, "  re-clicked Reviews tab")
    except Exception as err:
        _progress_log(progress, f"  re-click Reviews tab failed: {err}")

    # 3) Toggle sort (Newest) — often reloads more of the feed when stuck
    try:
        sort_btn = page.locator(
            'button[aria-label*="Sort" i], button[data-value="Sort"], '
            'button[jsaction*="pane.reviewChart"], '
            'button[aria-label*="Most relevant" i], '
            'button[aria-label*="Newest" i]'
        ).first
        if await sort_btn.count() > 0 and await sort_btn.is_visible():
            await sort_btn.click(timeout=1500)
            await page.wait_for_timeout(350)
            newest = page.locator(
                'div[role="menuitemradio"][data-index="1"], '
                'div[role="menuitemradio"]:has-text("Newest"), '
                'div[role="option"]:has-text("Newest"), '
                'button:has-text("Newest")'
            ).first
            if await newest.count() > 0:
                await newest.click(timeout=1500)
                await page.wait_for_timeout(700)
                _progress_log(progress, "  switched sort to Newest")
    except Exception as err:
        _progress_log(progress, f"  sort toggle failed: {err}")

    await page.wait_for_timeout(400)


async def _scroll_reviews_feed(page) -> dict[str, Any]:
    """Scroll the reviews sidebar; return diagnostics for the live log."""
    info: dict[str, Any] = {
        "wheel": False,
        "scrollTop": False,
        "keyboard": False,
        "box": None,
        "scroll_before": None,
        "scroll_after": None,
        "error": None,
    }

    try:
        info["scroll_before"] = await page.evaluate(_SCROLL_PROBE_JS)
    except Exception as err:
        info["error"] = f"pre-scroll probe: {err}"

    try:
        # Prefer a card still in the viewport; fall back to feed if last card is off-screen
        cards = page.locator("div.jftiEf")
        n = await cards.count()
        target = None
        used = "card"
        viewport = page.viewport_size or {"width": 1280, "height": 900}
        vh = viewport.get("height", 900)

        if n > 0:
            # Walk from last card upward until we find one with a sensible y
            for idx in range(n - 1, max(-1, n - 8), -1):
                cand = cards.nth(idx)
                box = await cand.bounding_box()
                if not box:
                    continue
                if 0 <= box["y"] <= vh and box.get("width", 0) > 0:
                    target = cand
                    used = f"card[{idx}]"
                    break
            if target is None:
                # Last cards are off-screen — aim at feed / left panel center
                feed = page.locator('div[role="feed"]').first
                if await feed.count() > 0:
                    target = feed
                    used = "feed"
                else:
                    target = cards.nth(min(3, n - 1))
                    used = f"card[{min(3, n - 1)}]"
        else:
            target = page.locator('div[role="feed"]').first
            used = "feed"

        if target is not None and await target.count() > 0:
            box = await target.bounding_box()
            if box and box.get("width", 0) > 0 and box.get("height", 0) > 0:
                # Clamp wheel target into viewport so Playwright mouse events hit the panel
                x = box["x"] + box["width"] / 2
                y = box["y"] + min(box["height"] * 0.5, box["height"] - 10)
                y = max(80.0, min(float(y), float(vh) - 40.0))
                x = max(40.0, min(float(x), 500.0))
                info["box"] = {
                    "x": round(x, 1),
                    "y": round(y, 1),
                    "w": round(box["width"], 1),
                    "h": round(box["height"], 1),
                    "via": used,
                }
                await page.mouse.move(x, y)
                await page.mouse.click(x, y)
                # Fewer, larger wheel ticks — less settle time per scroll step
                for _ in range(4):
                    await page.mouse.wheel(0, 2400)
                    await page.wait_for_timeout(20)
                info["wheel"] = True
    except Exception as err:
        info["error"] = f"wheel: {err}"

    try:
        moved = await page.evaluate(
            """() => {
                const findScrollable = () => {
                    const feed = document.querySelector('div[role="feed"]');
                    const cards = document.querySelectorAll('div.jftiEf');
                    const card = cards.length ? cards[cards.length - 1] : null;
                    const starts = [feed, card].filter(Boolean);
                    for (const start of starts) {
                        let el = start;
                        for (let i = 0; i < 16 && el; i++) {
                            const style = window.getComputedStyle(el);
                            const oy = style.overflowY;
                            if ((oy === 'auto' || oy === 'scroll' || oy === 'overlay')
                                && el.scrollHeight > el.clientHeight + 20) {
                                return el;
                            }
                            el = el.parentElement;
                        }
                    }
                    const mains = document.querySelectorAll('div[role="main"]');
                    for (const main of mains) {
                        let node = main;
                        for (let i = 0; i < 8 && node; i++) {
                            const style = window.getComputedStyle(node);
                            const oy = style.overflowY;
                            if ((oy === 'auto' || oy === 'scroll' || oy === 'overlay')
                                && node.scrollHeight > node.clientHeight + 20) {
                                return node;
                            }
                            node = node.parentElement;
                        }
                    }
                    return null;
                };
                const scroller = findScrollable();
                if (!scroller) return false;
                const before = scroller.scrollTop;
                const step = Math.max(scroller.clientHeight * 2.2, 1200);
                scroller.scrollTop = Math.min(scroller.scrollTop + step, scroller.scrollHeight);
                scroller.dispatchEvent(new WheelEvent('wheel', {
                    bubbles: true, cancelable: true, deltaY: step,
                }));
                scroller.dispatchEvent(new Event('scroll', { bubbles: true }));
                return scroller.scrollTop !== before;
            }"""
        )
        info["scrollTop"] = bool(moved)
    except Exception as err:
        info["error"] = f"scrollTop: {err}"

    try:
        target = page.locator("div.jftiEf").last
        if await target.count() == 0:
            target = page.locator('div[role="feed"]').first
        if await target.count() > 0:
            await target.click(timeout=1000)
            await page.keyboard.press("PageDown")
            await page.keyboard.press("PageDown")
            await page.keyboard.press("End")
            info["keyboard"] = True
    except Exception as err:
        if not info.get("error"):
            info["error"] = f"keyboard: {err}"

    try:
        info["scroll_after"] = await page.evaluate(_SCROLL_PROBE_JS)
    except Exception:
        pass

    return info


async def collect_reviews(
    page,
    max_reviews: int,
    total_expected: int | None = None,
    progress: ShopProgress | None = None,
) -> list[dict[str, Any]]:
    """Scroll the reviews feed to the bottom, tallying unique reviews each step."""
    await page.wait_for_timeout(300)

    if total_expected is None:
        total_expected = await get_total_reviews_count(page)

    header_total = total_expected
    scrape_all = max_reviews is None or max_reviews <= 0
    if scrape_all:
        target_count = header_total
    else:
        target_count = (
            min(max_reviews, header_total) if header_total else max_reviews
        )

    feed_n = await page.locator('div[role="feed"]').count()
    cards_n = await page.locator("div.jftiEf").count()
    _progress_log(
        progress,
        f"collect_reviews start — feed={feed_n}, visible_cards={cards_n}, "
        f"header={header_total}, max_reviews={max_reviews}, target={target_count}",
    )

    if header_total:
        _progress_log(
            progress,
            f"Header shows {header_total} reviews — scrolling feed to load them…",
        )
    else:
        _progress_log(progress, "No header total — scrolling until feed stops growing…")

    # Focus the reviews panel so wheel / keyboard events hit it
    try:
        cards = page.locator("div.jftiEf")
        feed = page.locator('div[role="feed"]').first
        target = None
        used = ""
        if await cards.count() > 0:
            target = cards.first
            used = "card"
        elif await feed.count() > 0:
            target = feed
            used = "feed"
        if target is not None:
            box = await target.bounding_box()
            if box and box.get("width", 0) > 0:
                _progress_log(
                    progress,
                    f"Focusing reviews via {used} at "
                    f"({box['x']:.0f},{box['y']:.0f}) "
                    f"{box['width']:.0f}x{box['height']:.0f}",
                )
                await page.mouse.click(
                    box["x"] + min(40, box["width"] / 2),
                    box["y"] + min(40, box["height"] / 2),
                )
                await page.wait_for_timeout(150)
        else:
            _progress_log(progress, "WARNING: No feed or review cards to focus")
    except Exception as err:
        _progress_log(progress, f"WARNING: Feed focus failed: {err}")

    collected: list[dict[str, Any]] = []
    seen: set[str] = set()
    previous_unique = 0
    stagnant_rounds = 0
    recoveries_used = 0
    max_recoveries = 2
    stop_reason = "max_loops"

    base_stagnant_limit = 14
    if target_count:
        max_loops = max(120, int(target_count) * 5)
    else:
        max_loops = 500

    for step in range(1, max_loops + 1):
        # Expand "More" every few steps (and when stuck) — full text is nice but costly
        more_clicked = 0
        if step == 1 or step % 3 == 0 or stagnant_rounds >= 3:
            more_clicked = await _expand_visible_more_buttons(page)
        visible_before = await page.locator("div.jftiEf").count()
        batch = await _extract_visible_review_cards(page, seen)
        if batch:
            collected.extend(batch)

        unique_count = len(collected)
        new_n = len(batch)
        authors_preview = (
            ", ".join((r.get("authorName") or "?") for r in batch[:3]) if batch else "—"
        )

        if header_total is None and step in (1, 3, 8):
            header_total = await get_total_reviews_count(page)
            if header_total and scrape_all:
                target_count = header_total
                _progress_log(
                    progress,
                    f"Header shows {header_total} reviews — continuing scroll…",
                )

        if progress:
            denom = target_count or header_total
            progress.set_stage(
                f"loading {unique_count}"
                + (f"/{denom}" if denom else "")
                + f" · scroll {step}",
                (
                    (10.0 + 70.0 * unique_count / denom)
                    if denom and denom > 0
                    else min(80.0, 10.0 + unique_count * 0.4)
                ),
            )

        # Quiet log: only when progress happens, every 5th step, or when stuck
        if new_n > 0 or step == 1 or step % 5 == 0 or stagnant_rounds >= 3:
            _progress_log(
                progress,
                f"scroll {step}: visible={visible_before}, new=+{new_n}, "
                f"total={unique_count}"
                + (f"/{target_count}" if target_count else "")
                + f", more_btns={more_clicked}, authors=[{authors_preview}]",
            )

        if target_count is not None and unique_count >= target_count:
            stop_reason = "reached_target"
            _progress_log(
                progress,
                f"Scroll complete — collected {unique_count}/{target_count} reviews.",
            )
            break

        if unique_count <= previous_unique:
            stagnant_rounds += 1
        else:
            stagnant_rounds = 0
            previous_unique = unique_count

        if target_count and unique_count < int(target_count * 0.98):
            stagnant_limit = 24
        else:
            stagnant_limit = base_stagnant_limit

        if stagnant_rounds > 0 and stagnant_rounds % 5 == 0:
            _progress_log(
                progress,
                f"Still scrolling… {unique_count}"
                + (f"/{target_count}" if target_count else "")
                + f" (no new cards for {stagnant_rounds}/{stagnant_limit} scrolls)",
            )

        # Mid-stall recovery before hard stop (Maps often needs a nudge)
        if (
            stagnant_rounds in (8, 16)
            and recoveries_used < max_recoveries
            and target_count
            and unique_count < int(target_count * 0.95)
        ):
            await _recover_stuck_reviews_feed(page, progress)
            recoveries_used += 1
            stagnant_rounds = 0
            await page.wait_for_timeout(600)
            continue

        if stagnant_rounds >= stagnant_limit:
            under_target = (
                target_count is not None
                and unique_count < int(target_count * 0.95)
            )
            if under_target and recoveries_used < max_recoveries:
                await _recover_stuck_reviews_feed(page, progress)
                recoveries_used += 1
                stagnant_rounds = 0
                _progress_log(
                    progress,
                    f"Gave feed another chance after recovery "
                    f"({recoveries_used}/{max_recoveries})…",
                )
                await page.wait_for_timeout(700)
                continue

            stop_reason = "feed_ended"
            _progress_log(
                progress,
                f"Feed stopped growing after scroll {step} "
                f"({unique_count} unique reviews).",
            )
            break

        scroll_info = await _scroll_reviews_feed(page)
        # Log scroll mechanics only when stuck or periodically
        if stagnant_rounds >= 2 or step % 10 == 0:
            before = scroll_info.get("scroll_before") or {}
            after = scroll_info.get("scroll_after") or {}
            _progress_log(
                progress,
                "  scroll action: "
                f"wheel={scroll_info.get('wheel')} "
                f"scrollTopMoved={scroll_info.get('scrollTop')} "
                f"top {before.get('top')}→{after.get('top')} "
                f"(height {before.get('height')}→{after.get('height')})"
                + (
                    f" err={scroll_info.get('error')}"
                    if scroll_info.get("error")
                    else ""
                ),
            )
        # Shorter settle — Maps lazy-load usually catches up quickly
        if (
            not scroll_info.get("scrollTop")
            and target_count
            and unique_count < int(target_count * 0.9)
        ):
            await page.wait_for_timeout(700)
        else:
            await page.wait_for_timeout(400)

    if not scrape_all and target_count:
        collected = collected[:target_count]

    final_count = len(collected)

    if header_total is not None and scrape_all:
        if final_count != header_total:
            _progress_log(
                progress,
                f"Review count mismatch: header shows {header_total}, "
                f"collected {final_count} after scrolling "
                f"(stop={stop_reason}) — continuing with {final_count}.",
            )
        else:
            _progress_log(
                progress,
                f"Review count matches header: {final_count}/{header_total}.",
            )
    elif header_total is not None and not scrape_all:
        if final_count < (target_count or 0):
            _progress_log(
                progress,
                f"Wanted {target_count} reviews (header {header_total}), "
                f"only got {final_count} — continuing.",
            )

    named = sum(1 for r in collected if (r.get("authorName") or "").strip())
    rated = sum(1 for r in collected if r.get("rating") is not None)
    with_text = sum(1 for r in collected if (r.get("content") or "").strip())
    _progress_log(
        progress,
        f"Final tally: {final_count} reviews "
        f"(named={named}, rated={rated}, with_text={with_text}, stop={stop_reason})",
    )

    if progress:
        progress.update_extracting(final_count, max(final_count, 1))
    else:
        print(
            f"  Successfully collected {final_count} unique reviews.",
            file=sys.stderr,
        )
    return collected


# Keep thin wrappers for compatibility / clarity
async def scroll_reviews_panel(
    page,
    max_reviews: int,
    total_expected: int | None = None,
    progress: ShopProgress | None = None,
) -> None:
    """Deprecated path: scrolling alone cannot collect virtualized reviews."""
    await page.wait_for_timeout(500)


async def extract_reviews(
    page, max_reviews: int, progress: ShopProgress | None = None
) -> list[dict[str, Any]]:
    """Fallback one-shot extract of whatever cards are currently visible."""
    seen: set[str] = set()
    await _expand_visible_more_buttons(page)
    reviews = await _extract_visible_review_cards(page, seen)
    scrape_all = max_reviews is None or max_reviews <= 0
    if not scrape_all and max_reviews:
        reviews = reviews[:max_reviews]
    if progress:
        progress.update_extracting(len(reviews), max(len(reviews), 1))
    return reviews


# ==============================================================================
# Scraper & Network Retry Logic
# ==============================================================================


async def scrape_shop(
    scraper: GoogleMapsScraper,
    shop: dict[str, Any],
    max_reviews: int,
    max_retries: int = 3,
    progress: ShopProgress | None = None,
) -> dict[str, Any]:
    """Scrapes a single shop with built-in dynamic timeout and block detection."""
    result: dict[str, Any] = {
        "id": shop.get("id"),
        "name": shop.get("name"),
        "googlePlaceId": shop.get("googlePlaceId"),
        "reviews": [],
        "error": None,
    }

    url = build_maps_url(shop)
    if progress is None:
        progress = ShopProgress(str(shop.get("name") or "shop"))

    for attempt in range(1, max_retries + 1):
        page = await scraper._context.new_page()
        await _install_no_profile_guard(page)
        if progress:
            progress.set_stage(f"navigating (try {attempt})", 2)

        async def _single_attempt():
            if progress:
                progress.set_stage(f"loading maps (try {attempt})", 3)
            _progress_log(progress, f"[nav] Navigating to Maps... {url[:120]}")
            try:
                await page.goto(url, wait_until="domcontentloaded", timeout=30000)
            except Exception as goto_err:
                err_str = str(goto_err).lower()
                if "net::err_internet_disconnected" in err_str or "net::err_name_not_resolved" in err_str:
                    raise NetworkDisconnectedError("No internet connection detected during navigation.") from goto_err
                raise goto_err

            _progress_log(progress, f"[nav] Landed: {page.url[:140]}")

            # Check for CAPTCHA / throttle — triggers browser restart + shop retry
            if await page_is_blocked_or_throttled(page):
                raise CaptchaBlockedError(
                    "CAPTCHA / throttle detected after navigation."
                )

            if progress:
                progress.set_stage("consent / overlays", 5)
            _progress_log(progress, "[nav] Handling consent / cookies...")
            await scraper._handle_consent(page)
            await _dismiss_maps_overlays(page, progress)

            if progress:
                progress.set_stage("waiting place panel", 8)
            _progress_log(progress, "[nav] Waiting for place panel...")
            # Library wait can sit for minutes on throttled parallel Maps loads —
            # fail fast so this worker can retry instead of looking "stuck".
            try:
                await asyncio.wait_for(
                    scraper._wait_for_place_panel(page), timeout=30.0
                )
            except asyncio.TimeoutError as err:
                raise RetryableShopError(
                    "Timed out waiting for place panel (30s) — "
                    "often caused by parallel Maps throttle"
                ) from err
            _progress_log(progress, "[nav] Place panel ready")
            if progress:
                progress.set_stage("opening reviews", 12)

            if await page_is_blocked_or_throttled(page):
                raise CaptchaBlockedError(
                    "CAPTCHA / throttle detected after place panel load."
                )

            async def _open_reviews_ui() -> bool:
                # Search-results pages look "ready" but have no Reviews tab — force place detail
                await ensure_place_detail(page, shop, scraper, progress=progress)

                # Deep-link sometimes lands on /place/ URL with an empty sidebar (no tabs).
                if not await _page_has_reviews_tab(page):
                    _progress_log(
                        progress,
                        "[nav] No Reviews tab after place open — trying search fallback…",
                    )
                    await _fallback_open_via_search(
                        page, shop, scraper, progress=progress
                    )

                place_id = extract_place_id(page.url)
                if place_id:
                    result["googlePlaceId"] = place_id
                    _progress_log(progress, f"[nav] Place ID: {place_id}")
                elif shop.get("googlePlaceId"):
                    result["googlePlaceId"] = shop.get("googlePlaceId")
                    _progress_log(
                        progress,
                        f"[nav] Place ID (from input): {shop.get('googlePlaceId')}",
                    )
                else:
                    _progress_log(progress, "[nav] WARNING: No place ID in URL yet")

                opened_inner = await open_full_reviews_panel(page, progress=progress)
                if not opened_inner:
                    _progress_log(
                        progress,
                        "[nav] Reviews not open — search fallback + retry…",
                    )
                    await _fallback_open_via_search(
                        page, shop, scraper, progress=progress
                    )
                    await ensure_place_detail(page, shop, scraper, progress=progress)
                    opened_inner = await open_full_reviews_panel(
                        page, progress=progress
                    )
                return opened_inner

            try:
                opened = await asyncio.wait_for(_open_reviews_ui(), timeout=75.0)
            except asyncio.TimeoutError as err:
                raise RetryableShopError(
                    "Timed out opening place / Reviews panel (75s)"
                ) from err

            _progress_log(progress, f"[nav] Reviews panel opened={opened}")

            if not opened:
                raise RetryableShopError(
                    "Could not open Reviews panel (not on place detail)"
                )

            if await page_is_blocked_or_throttled(page):
                raise CaptchaBlockedError(
                    "CAPTCHA / throttle detected after opening reviews panel."
                )

            # Fetch total review count early to compute a dynamic timeout
            total_expected = await get_total_reviews_count(page)
            _progress_log(progress, f"[nav] Header review total: {total_expected}")
            
            if max_reviews and max_reviews > 0:
                expected_target = min(max_reviews, total_expected) if total_expected else max_reviews
            else:
                expected_target = total_expected or 500

            # Dynamic Timeout Formula: Base 120s + 0.7s per review (faster scroll loop)
            dynamic_timeout = max(240.0, 120.0 + (expected_target * 0.7))
            _progress_log(
                progress,
                f"[nav] Collect budget ~{int(dynamic_timeout)}s for ~{int(expected_target)} reviews",
            )

            if progress:
                progress.set_stage(
                    f"collecting ~{int(expected_target)} reviews",
                    10,
                )
            else:
                print(
                    f"  Dynamic timeout set to {int(dynamic_timeout)}s ({round(dynamic_timeout / 60, 1)} min) "
                    f"for ~{expected_target} expected reviews.",
                    file=sys.stderr,
                )

            async def _process_feed():
                return await collect_reviews(
                    page,
                    max_reviews,
                    total_expected=total_expected,
                    progress=progress,
                )

            reviews = await asyncio.wait_for(_process_feed(), timeout=dynamic_timeout)

            result["reviews"] = reviews
            result["error"] = "No reviews extracted" if not reviews else None
            if progress:
                detail = f"done · {len(reviews)} reviews"
                if result["error"]:
                    progress.finish(ok=False, detail=result["error"])
                else:
                    progress.finish(ok=True, detail=detail)
            return result

        try:
            return await asyncio.wait_for(_single_attempt(), timeout=3600.0)

        except CaptchaBlockedError:
            if progress:
                progress.finish(ok=False, detail="captcha — will retry")
            raise

        except RetryableShopError as err:
            if progress:
                progress.finish(ok=False, detail=str(err)[:80])
            raise

        except FatalScrapeError:
            if progress:
                progress.finish(ok=False, detail="fatal error")
            raise

        except (asyncio.TimeoutError, PlaywrightError, PlaywrightTimeoutError, OSError) as error:
            err_msg = "Attempt timed out" if isinstance(error, asyncio.TimeoutError) else str(error)
            err_lower = err_msg.lower()

            # Only treat genuine connectivity failures as fatal
            if (
                "net::err_internet_disconnected" in err_lower
                or "net::err_name_not_resolved" in err_lower
                or "net::err_network_changed" in err_lower
            ):
                if progress:
                    progress.finish(ok=False, detail="no internet")
                raise NetworkDisconnectedError("Internet connection lost.") from error

            if attempt < max_retries:
                backoff = attempt * 3
                if progress:
                    progress.set_stage(f"retry in {backoff}s", 5)
                else:
                    print(
                        f"  ⚠️  [Attempt {attempt}/{max_retries}] Error/Timeout: {err_msg[:80]}. Retrying in {backoff}s...",
                        file=sys.stderr,
                    )
                await asyncio.sleep(backoff)
            else:
                # Retries exhausted for a non-fatal error — record and continue to next shop
                result["error"] = f"Failed after {max_retries} attempts: {err_msg}"
                if progress:
                    progress.finish(ok=False, detail="failed")
                return result

        except Exception as error:
            result["error"] = str(error)
            if progress:
                progress.finish(ok=False, detail="error")
            return result
        finally:
            try:
                await page.close()
            except Exception:
                pass

    if progress:
        progress.finish(ok=False, detail="failed")
    return result


# ==============================================================================
# Checkpointing & Incremental Output
# ==============================================================================


def _checkpoint_entry(shop: dict[str, Any], index: int) -> dict[str, Any]:
    """id + name record stored in the checkpoint JSON."""
    return {
        "id": get_shop_key(shop, index),
        "name": str(shop.get("name") or f"shop_{index}"),
    }


def _entry_id_str(entry: Any) -> str:
    if isinstance(entry, dict):
        return str(entry.get("id", ""))
    return str(entry)


def _normalize_shop_entries(raw: Any) -> list[dict[str, Any]]:
    """Accept legacy bare ids or {id, name} objects."""
    if not isinstance(raw, list):
        return []
    out: list[dict[str, Any]] = []
    seen: set[str] = set()
    for item in raw:
        if isinstance(item, dict):
            eid = item.get("id")
            if eid is None or str(eid).strip() == "":
                continue
            entry = {
                "id": eid,
                "name": str(item.get("name") or ""),
            }
        else:
            if item is None or str(item).strip() == "":
                continue
            entry = {"id": item, "name": ""}
        key = _entry_id_str(entry)
        if key in seen:
            continue
        seen.add(key)
        out.append(entry)
    return out


def _upsert_shop_entry(entries: list[dict[str, Any]], entry: dict[str, Any]) -> None:
    key = _entry_id_str(entry)
    for i, existing in enumerate(entries):
        if _entry_id_str(existing) == key:
            # Prefer keeping a non-empty name
            name = entry.get("name") or existing.get("name") or ""
            entries[i] = {"id": entry.get("id", existing.get("id")), "name": name}
            return
    entries.append({"id": entry.get("id"), "name": str(entry.get("name") or "")})


def _remove_shop_entries(
    entries: list[dict[str, Any]], keys: set[int | str]
) -> int:
    key_strs = {str(k) for k in keys}
    before = len(entries)
    entries[:] = [e for e in entries if _entry_id_str(e) not in key_strs]
    return before - len(entries)


def _entry_key_set(entries: list[dict[str, Any]]) -> set[int | str]:
    keys: set[int | str] = set()
    for entry in entries:
        eid = entry.get("id")
        if eid is None:
            continue
        keys.add(eid)
    return keys


def load_checkpoint_state(
    checkpoint_path: Path,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    """Load completed_shops + incomplete_shops (migrates legacy key lists)."""
    legacy = checkpoint_path.with_name(".scraper_checkpoint.json")
    path = checkpoint_path
    if not path.exists() and legacy.exists() and legacy != path:
        path = legacy
        print(
            f"::log::Loading legacy checkpoint: {legacy}",
            file=sys.stderr,
            flush=True,
        )
    if not path.exists():
        return [], []
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
        completed = _normalize_shop_entries(data.get("completed_shops"))
        if not completed:
            # Legacy: completed_shop_keys: [id, ...]
            completed = _normalize_shop_entries(data.get("completed_shop_keys", []))
        incomplete = _normalize_shop_entries(
            data.get("incomplete_shops") or data.get("incomplete_shop_keys") or []
        )
        print(
            f"::log::Loaded checkpoint: {len(completed)} completed, "
            f"{len(incomplete)} incomplete ({path})",
            file=sys.stderr,
            flush=True,
        )
        return completed, incomplete
    except Exception as e:
        print(f"Warning: Failed to load checkpoint file: {e}", file=sys.stderr)
        return [], []


def load_checkpoint(checkpoint_path: Path) -> set[int | str]:
    """Back-compat helper: completed shop ids only."""
    completed, _incomplete = load_checkpoint_state(checkpoint_path)
    return _entry_key_set(completed)


def save_checkpoint_state(
    checkpoint_path: Path,
    completed_shops: list[dict[str, Any]],
    incomplete_shops: list[dict[str, Any]],
) -> None:
    completed_shops = _normalize_shop_entries(completed_shops)
    incomplete_shops = _normalize_shop_entries(incomplete_shops)
    # A shop cannot be in both lists — completed wins
    completed_ids = {_entry_id_str(e) for e in completed_shops}
    incomplete_shops = [
        e for e in incomplete_shops if _entry_id_str(e) not in completed_ids
    ]
    completed_shops = sorted(completed_shops, key=lambda e: str(e.get("id")))
    incomplete_shops = sorted(incomplete_shops, key=lambda e: str(e.get("id")))
    payload = {
        "completed_shops": completed_shops,
        "incomplete_shops": incomplete_shops,
    }
    checkpoint_path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = checkpoint_path.with_suffix(checkpoint_path.suffix + ".tmp")
    tmp_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    tmp_path.replace(checkpoint_path)
    print(
        f"::log::Checkpoint saved "
        f"({len(completed_shops)} completed, {len(incomplete_shops)} incomplete) "
        f"-> {checkpoint_path}",
        file=sys.stderr,
        flush=True,
    )


def save_checkpoint(checkpoint_path: Path, completed_keys: set[int | str]) -> None:
    """Back-compat: save completed ids only (names blank). Prefer save_checkpoint_state."""
    completed_shops = [{"id": k, "name": ""} for k in completed_keys]
    _, incomplete = load_checkpoint_state(checkpoint_path)
    save_checkpoint_state(checkpoint_path, completed_shops, incomplete)


def write_inflight_file(
    inflight_path: Path,
    inflight_by_worker: dict[int, dict[str, Any]],
) -> None:
    """Persist currently running shops so Stop can undo them after a hard kill."""
    shops = []
    for wid, info in sorted(inflight_by_worker.items()):
        shops.append(
            {
                "worker": wid,
                "key": info.get("key"),
                "id": info.get("id"),
                "name": info.get("name"),
            }
        )
    payload = {"shops": shops}
    inflight_path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = inflight_path.with_suffix(inflight_path.suffix + ".tmp")
    tmp_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    tmp_path.replace(inflight_path)


def clear_inflight_file(inflight_path: Path) -> None:
    try:
        if inflight_path.exists():
            inflight_path.unlink()
    except OSError:
        pass


def remove_keys_from_checkpoint(
    checkpoint_path: Path, keys_to_remove: set[int | str]
) -> int:
    """Remove shop keys from completed + incomplete lists. Returns how many removed."""
    if not keys_to_remove or not checkpoint_path.exists():
        return 0
    completed, incomplete = load_checkpoint_state(checkpoint_path)
    removed = _remove_shop_entries(completed, keys_to_remove)
    removed += _remove_shop_entries(incomplete, keys_to_remove)
    if removed:
        save_checkpoint_state(checkpoint_path, completed, incomplete)
    return removed


def purge_shop_rows_from_csv(output_path: Path, shop_keys: set[int | str]) -> int:
    """Delete review rows whose shopId matches any key. Returns rows removed."""
    if not shop_keys or not output_path.exists():
        return 0
    key_strs = {str(k) for k in shop_keys}
    try:
        with output_path.open(encoding="utf-8", newline="") as handle:
            reader = csv.DictReader(handle)
            fieldnames = reader.fieldnames
            if not fieldnames:
                return 0
            rows = list(reader)
    except OSError:
        return 0

    kept = [r for r in rows if str(r.get("shopId") or "") not in key_strs]
    removed = len(rows) - len(kept)
    if removed <= 0:
        return 0

    tmp_path = output_path.with_suffix(output_path.suffix + ".tmp")
    with tmp_path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(kept)
    tmp_path.replace(output_path)
    return removed


def append_reviews_to_csv(result: dict[str, Any], output_path: Path) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    rows = results_to_csv_rows([result])
    existing_ids: set[str] = set()
    existing_pairs: set[tuple[str, str, str]] = set()
    file_exists = output_path.exists() and output_path.stat().st_size > 0
    fieldnames = CSV_COLUMNS
    if file_exists:
        with output_path.open(encoding="utf-8", newline="") as handle:
            reader = csv.DictReader(handle)
            fieldnames = list(reader.fieldnames or CSV_COLUMNS)
            for old in reader:
                rid = str(old.get("googleReviewId") or "").strip()
                if rid:
                    existing_ids.add(rid)
                existing_pairs.add(
                    (
                        str(old.get("shopId") or ""),
                        str(old.get("authorName") or ""),
                        str(old.get("content") or "")[:180],
                    )
                )

    new_rows: list[dict[str, Any]] = []
    shop_ids_with_reviews: set[str] = set()
    for row in rows:
        rid = str(row.get("googleReviewId") or "").strip()
        pair = (
            str(row.get("shopId") or ""),
            str(row.get("authorName") or ""),
            str(row.get("content") or "")[:180],
        )
        if rid and rid in existing_ids:
            continue
        if pair in existing_pairs and pair != ("", "", ""):
            continue
        if rid:
            existing_ids.add(rid)
        existing_pairs.add(pair)
        new_rows.append(row)
        if str(row.get("content") or "").strip():
            shop_ids_with_reviews.add(str(row.get("shopId") or ""))

    if shop_ids_with_reviews and file_exists:
        with output_path.open(encoding="utf-8", newline="") as handle:
            reader = csv.DictReader(handle)
            fieldnames = list(reader.fieldnames or CSV_COLUMNS)
            kept = [
                old
                for old in reader
                if not (
                    str(old.get("shopId") or "") in shop_ids_with_reviews
                    and str(old.get("error") or "").strip()
                    and not str(old.get("content") or "").strip()
                )
            ]
        tmp = output_path.with_suffix(output_path.suffix + ".tmp")
        with tmp.open("w", encoding="utf-8", newline="") as handle:
            writer = csv.DictWriter(handle, fieldnames=fieldnames, extrasaction="ignore")
            writer.writeheader()
            writer.writerows(kept)
            writer.writerows(new_rows)
        tmp.replace(output_path)
        return

    if not new_rows:
        return

    with output_path.open("a", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames, extrasaction="ignore")
        if not file_exists:
            writer.writeheader()
        writer.writerows(new_rows)


def results_to_csv_rows(results: list[dict[str, Any]]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []

    for result in results:
        shop_id = result.get("id")
        shop_name = result.get("name")
        google_place_id = result.get("googlePlaceId")
        error = result.get("error")
        reviews = result.get("reviews") or []

        if not reviews:
            rows.append(
                {
                    "shopId": shop_id,
                    "shopName": shop_name,
                    "googlePlaceId": google_place_id,
                    "googleReviewId": None,
                    "authorName": None,
                    "rating": None,
                    "content": None,
                    "relativeTime": None,
                    "source": None,
                    "error": error,
                }
            )
            continue

        for review in reviews:
            rows.append(
                {
                    "shopId": shop_id,
                    "shopName": shop_name,
                    "googlePlaceId": google_place_id,
                    "googleReviewId": review.get("googleReviewId"),
                    "authorName": review.get("authorName"),
                    "rating": review.get("rating"),
                    "content": review.get("content"),
                    "relativeTime": review.get("relativeTime"),
                    "source": review.get("source"),
                    "error": error,
                }
            )

    return rows


# ==============================================================================
# Data Ingestion & Input Normalization
# ==============================================================================


def _parse_optional_float(value: Any) -> float | None:
    if value is None:
        return None
    text = str(value).strip()
    if not text:
        return None
    return float(text)


def _parse_optional_int(value: Any) -> int | None:
    if value is None:
        return None
    text = str(value).strip()
    if not text:
        return None
    return int(float(text))


def _ci_get(raw: dict[str, Any], *keys: str) -> Any:
    lookup = {str(k).lower(): v for k, v in raw.items()}
    for key in keys:
        val = lookup.get(key.lower())
        if val is not None and str(val).strip() != "":
            return val
    return None


def normalize_shop(raw: dict[str, Any]) -> dict[str, Any]:
    shop_id = _ci_get(raw, "id", "shopId")
    name = _ci_get(raw, "name", "shopName") or ""
    address = str(_ci_get(raw, "address") or "").strip()
    address = re.sub(
        r"(Portuguese-style butcher'?s store|butcher shop|meat market|meat shop)\s*$",
        "",
        address,
        flags=re.I,
    ).strip()
    address = re.sub(
        r"\s*(Permanently closed|Temporarily closed|Open|Closed)\s*$",
        "",
        address,
        flags=re.I,
    ).strip()
    address = re.sub(r"(?<=[A-Za-z0-9])(Open|Closed)$", "", address, flags=re.I).strip(" ,")
    return {
        "id": _parse_optional_int(shop_id),
        "name": str(name).strip() or None,
        "address": address or None,
        "city": (str(_ci_get(raw, "city") or "").strip() or None),
        "province": (str(_ci_get(raw, "province", "state") or "").strip() or None),
        "latitude": _parse_optional_float(_ci_get(raw, "latitude")),
        "longitude": _parse_optional_float(_ci_get(raw, "longitude")),
        "googlePlaceId": (str(_ci_get(raw, "googlePlaceId") or "").strip() or None),
    }


def load_shops_from_json(path: Path) -> list[dict[str, Any]]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    records = payload if isinstance(payload, list) else [payload]
    return [normalize_shop(record) for record in records]


def load_shops_from_csv(path: Path) -> list[dict[str, Any]]:
    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        reader = csv.DictReader(handle)
        return [normalize_shop(row) for row in reader]


def load_shops(path: Path | None) -> list[dict[str, Any]]:
    if path is None:
        if sys.stdin.isatty():
            raise SystemExit(
                "No shops input. Pass --shops path/to/shops.csv (or .json), "
                "or pipe JSON on stdin."
            )
        payload = json.load(sys.stdin)
        records = payload if isinstance(payload, list) else [payload]
        return [normalize_shop(record) for record in records]

    suffix = path.suffix.lower()
    if suffix == ".json":
        return load_shops_from_json(path)
    if suffix == ".csv":
        return load_shops_from_csv(path)
    raise SystemExit(f"Unsupported shops file type: {path.suffix} (use .csv or .json)")


def get_shop_key(shop: dict[str, Any], index: int) -> int | str:
    """Returns a unique identifier for state tracking."""
    return shop.get("id") if shop.get("id") is not None else f"idx_{index}_{shop.get('name')}"


# ==============================================================================
# Main Orchestration Loop
# ==============================================================================


async def run_scraper(
    shops: list[dict[str, Any]],
    max_reviews: int,
    headless: bool,
    output_path: Path,
    checkpoint_path: Path,
    resume: bool,
    concurrency: int | None = None,
    max_concurrency: int = DEFAULT_MAX_CONCURRENCY,
    limit: int | None = None,
) -> None:
    completed_shops: list[dict[str, Any]] = []
    incomplete_shops: list[dict[str, Any]] = []

    if resume:
        completed_shops, incomplete_shops = load_checkpoint_state(checkpoint_path)
        # Fill blank names from the shops file (legacy id-only checkpoints)
        name_by_id = {
            str(get_shop_key(shop, index)): str(shop.get("name") or "")
            for index, shop in enumerate(shops)
        }
        for entry in completed_shops + incomplete_shops:
            if not entry.get("name"):
                entry["name"] = name_by_id.get(str(entry.get("id")), "") or ""
    else:
        # Fresh Start — reset progress so we begin at the top of the file again
        if checkpoint_path.exists():
            try:
                checkpoint_path.unlink()
                print(
                    f"::log::Fresh run — cleared checkpoint {checkpoint_path}",
                    file=sys.stderr,
                    flush=True,
                )
            except OSError as err:
                print(
                    f"::log::Could not clear checkpoint: {err}",
                    file=sys.stderr,
                    flush=True,
                )
        print(
            f"::log::Fresh run — checkpoint file: {checkpoint_path}",
            file=sys.stderr,
            flush=True,
        )

    completed_keys = {_entry_id_str(e) for e in completed_shops}

    def _persist_checkpoint() -> None:
        save_checkpoint_state(checkpoint_path, completed_shops, incomplete_shops)
        completed_keys.clear()
        completed_keys.update(_entry_id_str(e) for e in completed_shops)

    def _is_completed(shop_key: int | str) -> bool:
        return str(shop_key) in completed_keys

    def _mark_completed(shop: dict[str, Any], index: int) -> None:
        entry = _checkpoint_entry(shop, index)
        _remove_shop_entries(incomplete_shops, {entry["id"]})
        _upsert_shop_entry(completed_shops, entry)
        _persist_checkpoint()

    def _mark_incomplete(shop: dict[str, Any], index: int) -> None:
        entry = _checkpoint_entry(shop, index)
        _remove_shop_entries(completed_shops, {entry["id"]})
        _upsert_shop_entry(incomplete_shops, entry)
        _persist_checkpoint()

    def _clear_shop_progress(shop_key: int | str) -> None:
        _remove_shop_entries(completed_shops, {shop_key})
        _remove_shop_entries(incomplete_shops, {shop_key})
        _persist_checkpoint()

    stop_requested = asyncio.Event()
    inflight_by_worker: dict[int, dict[str, Any]] = {}
    inflight_path = checkpoint_path.with_name(INFLIGHT_DEFAULT_PATH)
    clear_inflight_file(inflight_path)

    def handle_signal():
        print(
            "\n🛑 Pause/Stop requested by user! Discarding in-flight shops...",
            file=sys.stderr,
        )
        stop_requested.set()
        keys = [str(info.get("key")) for info in inflight_by_worker.values()]
        print(
            f"::stop_inflight::{','.join(keys)}",
            file=sys.stderr,
            flush=True,
        )

    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, handle_signal)
        except NotImplementedError:
            signal.signal(sig, lambda s, f: handle_signal())

    workers = await resolve_concurrency(concurrency, max_concurrency=max_concurrency)
    print(f"::workers::{workers}", file=sys.stderr, flush=True)

    config = ScrapeConfig(language="en", headless=headless)
    fatal_error_occurred = False
    browser_restarts = 0
    soft_retries: dict[int | str, int] = {}
    max_soft_retries = 2
    io_lock = asyncio.Lock()
    # Spread cold Maps navigations — opening 3 at once often leaves 2 hung
    nav_start_lock = asyncio.Lock()
    last_nav_start = 0.0
    nav_stagger_s = 1.2
    # Track final status per shop for the end-of-run summary
    shop_outcomes: dict[int | str, dict[str, Any]] = {}

    # Full-file pending list (original indices). Limit = this run's batch only.
    # Incomplete shops are NOT completed — they stay eligible for Resume.
    all_pending: list[tuple[int, dict[str, Any]]] = [
        (index, shop)
        for index, shop in enumerate(shops)
        if not _is_completed(get_shop_key(shop, index))
    ]
    already_done_n = len(shops) - len(all_pending)
    if limit is not None and limit > 0:
        pending = all_pending[:limit]
    else:
        pending = list(all_pending)
    left_after_batch = max(0, len(all_pending) - len(pending))

    print(
        f"::log::File has {len(shops)} shops · {already_done_n} already done · "
        f"this run will do {len(pending)}"
        + (f" · ~{left_after_batch} left after this batch" if left_after_batch else ""),
        file=sys.stderr,
        flush=True,
    )
    print(
        f"::batch_plan::{len(pending)}::{already_done_n}::{left_after_batch}::{len(shops)}",
        file=sys.stderr,
        flush=True,
    )

    if not pending:
        print(
            "::log::Nothing left to scrape — all shops in the file are already completed.",
            file=sys.stderr,
            flush=True,
        )
        if checkpoint_path.exists():
            try:
                checkpoint_path.unlink()
            except OSError:
                pass
        print(
            f"\nAll {len(shops)} shops already done. Output: '{output_path}'.",
            file=sys.stderr,
        )
        return

    total_pending_initial = len(pending)
    hub = ProgressHub(total_pending_initial)

    pool = FirefoxWorkerPool(config, size=workers)
    await pool.start()

    # Continuous queue: when one Firefox finishes, it immediately takes the next shop
    # (batch+gather waited for the slowest shop and felt "stuck").
    work_q: asyncio.Queue[tuple[int, dict[str, Any]]] = asyncio.Queue()
    for item in pending:
        work_q.put_nowait(item)
    pending.clear()

    active_jobs = 0
    active_lock = asyncio.Lock()
    workers_alive = workers

    async def _handle_success(
        index: int,
        shop: dict[str, Any],
        shop_key: int | str,
        result: dict[str, Any],
        label: str,
    ) -> None:
        detail = (
            result.get("error")
            or f"done · {len(result.get('reviews') or [])} reviews"
        )
        ok = not result.get("error")
        async with io_lock:
            if ok:
                append_reviews_to_csv(result, output_path)
                _mark_completed(shop, index)
                shop_outcomes[shop_key] = {
                    "index": index,
                    "name": shop.get("name") or f"shop_{index}",
                    "status": "ok",
                    "detail": str(detail),
                    "reviews": len(result.get("reviews") or []),
                }
                hub.shop_done(label, ok=True, detail=str(detail))
            else:
                # Soft failure (e.g. no reviews) — track as incomplete, not completed
                append_reviews_to_csv(result, output_path)
                _mark_incomplete(shop, index)
                shop_outcomes[shop_key] = {
                    "index": index,
                    "name": shop.get("name") or f"shop_{index}",
                    "status": "fail",
                    "detail": str(detail),
                    "reviews": len(result.get("reviews") or []),
                }
                hub.shop_done(label, ok=False, detail=str(detail))

    async def _fail_exhausted(
        index: int,
        shop: dict[str, Any],
        shop_key: int | str,
        err: BaseException,
    ) -> None:
        fail_result = {
            "id": shop.get("id"),
            "name": shop.get("name"),
            "googlePlaceId": shop.get("googlePlaceId"),
            "reviews": [],
            "error": str(err),
        }
        async with io_lock:
            append_reviews_to_csv(fail_result, output_path)
            # Failed shops must NOT go into completed_shops
            _mark_incomplete(shop, index)
            shop_outcomes[shop_key] = {
                "index": index,
                "name": shop.get("name") or f"shop_{index}",
                "status": "fail",
                "detail": str(err)[:120],
                "reviews": 0,
            }
            hub.shop_done(
                f"[{index + 1}/{len(shops)}] {shop.get('name')}",
                ok=False,
                detail=str(err)[:100],
            )

    async def _worker(worker_id: int) -> None:
        nonlocal active_jobs, browser_restarts, fatal_error_occurred, workers_alive
        nonlocal last_nav_start
        while not fatal_error_occurred:
            if stop_requested.is_set() and work_q.empty():
                break

            try:
                index, shop = work_q.get_nowait()
            except asyncio.QueueEmpty:
                async with active_lock:
                    if active_jobs == 0:
                        break
                # Another worker may still be running and might requeue — wait briefly
                try:
                    index, shop = await asyncio.wait_for(work_q.get(), timeout=1.0)
                except asyncio.TimeoutError:
                    async with active_lock:
                        if active_jobs == 0 and work_q.empty():
                            break
                    if stop_requested.is_set():
                        break
                    continue

            if stop_requested.is_set():
                # Put back so summary marks incomplete
                await work_q.put((index, shop))
                break

            shop_key = get_shop_key(shop, index)
            shop_name = shop.get("name")
            label = f"[{index + 1}/{len(shops)}] {shop_name}"
            progress = ShopProgress(
                label,
                hub=hub,
                shop_key=str(shop_key),
                worker_id=worker_id,
            )
            progress._hub_counts_completion = True

            async with active_lock:
                active_jobs += 1
                inflight_by_worker[worker_id] = {
                    "key": shop_key,
                    "id": shop.get("id"),
                    "name": shop_name,
                }
                write_inflight_file(inflight_path, inflight_by_worker)

            scraper = await pool.acquire()
            hub.write(
                f"::log::[Firefox {worker_id}/{workers_alive}] "
                f"scraping {shop_name}"
            )
            try:
                try:
                    # Stagger only the *start* of navigation; scrapes still run in parallel
                    async with nav_start_lock:
                        now = time.monotonic()
                        wait = nav_stagger_s - (now - last_nav_start)
                        if wait > 0:
                            progress.set_stage(
                                f"queued {wait:.0f}s (stagger)", 1
                            )
                            hub.write(
                                f"::log::[Firefox {worker_id}] waiting "
                                f"{wait:.1f}s before Maps navigate "
                                f"(avoids parallel throttle)"
                            )
                            await asyncio.sleep(wait)
                        last_nav_start = time.monotonic()

                    result = await scrape_shop(
                        scraper, shop, max_reviews, progress=progress
                    )
                    if stop_requested.is_set():
                        # User stopped while this shop was running — do not keep it.
                        hub.write(
                            f"::log::Stop — discarding in-flight result for "
                            f"'{shop_name}' (key={shop_key})"
                        )
                        progress.finish(ok=False, detail="stopped")
                        async with io_lock:
                            _clear_shop_progress(shop_key)
                            purged = purge_shop_rows_from_csv(
                                output_path, {shop_key}
                            )
                            if purged:
                                hub.write(
                                    f"::log::Removed {purged} review row(s) for "
                                    f"stopped shop '{shop_name}'"
                                )
                            _mark_incomplete(shop, index)
                            shop_outcomes[shop_key] = {
                                "index": index,
                                "name": shop.get("name") or f"shop_{index}",
                                "status": "incomplete",
                                "detail": "stopped (in-flight discarded)",
                                "reviews": 0,
                            }
                    else:
                        await _handle_success(index, shop, shop_key, result, label)
                    browser_restarts = 0
                except CaptchaBlockedError as err:
                    if stop_requested.is_set():
                        await work_q.put((index, shop))
                        progress.finish(ok=False, detail="stopped")
                        break
                    hub.write(
                        f"⚠️  [CAPTCHA/THROTTLE] {err} — "
                        f"'{shop_name}' will retry after browser restart."
                    )
                    try:
                        await pool.restart(scraper)
                    except Exception as restart_err:
                        hub.write(
                            f"::log::Failed to restart Firefox {worker_id}: {restart_err}"
                        )
                        fatal_error_occurred = True
                        await work_q.put((index, shop))
                        break
                    browser_restarts += 1
                    if browser_restarts >= MAX_BROWSER_RESTARTS:
                        hub.write(
                            f"::log::Exhausted {MAX_BROWSER_RESTARTS} browser restarts."
                        )
                        fatal_error_occurred = True
                        await work_q.put((index, shop))
                        break
                    await work_q.put((index, shop))
                    await asyncio.sleep(2.0)
                except RetryableShopError as err:
                    if stop_requested.is_set():
                        await work_q.put((index, shop))
                        progress.finish(ok=False, detail="stopped")
                        break
                    soft_retries[shop_key] = soft_retries.get(shop_key, 0) + 1
                    if soft_retries[shop_key] <= max_soft_retries:
                        hub.write(
                            f"::log::[RETRY {soft_retries[shop_key]}/{max_soft_retries}] "
                            f"{err} — '{shop_name}'"
                        )
                        await work_q.put((index, shop))
                        await asyncio.sleep(1.0)
                    else:
                        await _fail_exhausted(index, shop, shop_key, err)
                except FatalScrapeError as err:
                    hub.write(f"::log::[CRITICAL] {err}")
                    fatal_error_occurred = True
                    await work_q.put((index, shop))
                    break
                except Exception as err:
                    if stop_requested.is_set():
                        await work_q.put((index, shop))
                        progress.finish(ok=False, detail="stopped")
                        break
                    soft_retries[shop_key] = soft_retries.get(shop_key, 0) + 1
                    if soft_retries[shop_key] <= max_soft_retries:
                        hub.write(
                            f"⚠️  Unexpected error for '{shop_name}': {err} — retrying"
                        )
                        await work_q.put((index, shop))
                        await asyncio.sleep(2.0)
                    else:
                        await _fail_exhausted(index, shop, shop_key, err)
            finally:
                await pool.release(scraper)
                async with active_lock:
                    active_jobs -= 1
                    inflight_by_worker.pop(worker_id, None)
                    if inflight_by_worker:
                        write_inflight_file(inflight_path, inflight_by_worker)
                    else:
                        clear_inflight_file(inflight_path)

        hub.write(f"::log::[Firefox {worker_id}] worker idle / exiting")

    async def _drain_queue_to_pending() -> None:
        while not work_q.empty():
            try:
                pending.append(work_q.get_nowait())
            except asyncio.QueueEmpty:
                break

    def _mark_pending_incomplete() -> None:
        for index, shop in pending:
            key = get_shop_key(shop, index)
            if key not in shop_outcomes:
                shop_outcomes[key] = {
                    "index": index,
                    "name": shop.get("name") or f"shop_{index}",
                    "status": "incomplete",
                    "detail": "not finished (stopped or error)",
                    "reviews": 0,
                }
                _mark_incomplete(shop, index)
        pending.clear()

    def _sync_outcomes_into_checkpoint() -> None:
        """Ensure fail/incomplete outcomes live under incomplete_shops, not completed."""
        for index, shop in enumerate(shops):
            key = get_shop_key(shop, index)
            status = (shop_outcomes.get(key) or {}).get("status")
            if status in ("fail", "incomplete"):
                entry = _checkpoint_entry(shop, index)
                _remove_shop_entries(completed_shops, {entry["id"]})
                _upsert_shop_entry(incomplete_shops, entry)
            elif status == "ok":
                entry = _checkpoint_entry(shop, index)
                _remove_shop_entries(incomplete_shops, {entry["id"]})
                _upsert_shop_entry(completed_shops, entry)
        _persist_checkpoint()

    def _failed_or_incomplete_jobs() -> list[tuple[int, dict[str, Any]]]:
        jobs: list[tuple[int, dict[str, Any]]] = []
        for index, shop in enumerate(shops):
            key = get_shop_key(shop, index)
            status = (shop_outcomes.get(key) or {}).get("status")
            if status in ("fail", "incomplete"):
                jobs.append((index, shop))
        return jobs

    async def _prepare_fail_reload(
        retry_jobs: list[tuple[int, dict[str, Any]]],
        reload_pass: int,
    ) -> None:
        nonlocal hub, browser_restarts, last_nav_start
        keys: set[int | str] = {get_shop_key(shop, index) for index, shop in retry_jobs}
        names = [
            (shop.get("name") or f"shop_{index}") for index, shop in retry_jobs
        ]

        async with io_lock:
            purged = purge_shop_rows_from_csv(output_path, keys)
            for key in keys:
                soft_retries.pop(key, None)
                shop_outcomes.pop(key, None)
            for index, shop in retry_jobs:
                _clear_shop_progress(get_shop_key(shop, index))
            # Don't mark incomplete yet — they're being retried now
            _persist_checkpoint()

        print(
            f"::reload_pass::{reload_pass}::{len(retry_jobs)}",
            file=sys.stderr,
            flush=True,
        )
        hub.write(
            f"::log::========== RELOAD PASS {reload_pass}/{MAX_FAIL_RELOAD_PASSES} =========="
        )
        hub.write(
            f"::log::Re-running {len(retry_jobs)} failed/incomplete shop(s) "
            f"with all {workers} worker(s)"
            + (f" — cleared {purged} CSV row(s)" if purged else "")
        )
        for name in names:
            hub.write(f"::log::  reload: {name}")

        try:
            hub.close()
        except Exception:
            pass
        hub = ProgressHub(len(retry_jobs))
        browser_restarts = 0
        last_nav_start = 0.0
        for item in retry_jobs:
            work_q.put_nowait(item)

    try:
        reload_pass = 0
        while True:
            await asyncio.gather(
                *[_worker(i + 1) for i in range(workers)],
                return_exceptions=True,
            )
            await _drain_queue_to_pending()
            _mark_pending_incomplete()

            if stop_requested.is_set() or fatal_error_occurred:
                break

            retry_jobs = _failed_or_incomplete_jobs()
            if not retry_jobs:
                break
            if reload_pass >= MAX_FAIL_RELOAD_PASSES:
                hub.write(
                    f"::log::Still {len(retry_jobs)} failed/incomplete after "
                    f"{MAX_FAIL_RELOAD_PASSES} reload pass(es) — stopping retries."
                )
                # Keep them as incomplete_shops (not completed)
                for index, shop in retry_jobs:
                    _mark_incomplete(shop, index)
                break

            reload_pass += 1
            await _prepare_fail_reload(retry_jobs, reload_pass)
            # Brief pause so Maps throttle cools before all workers navigate again
            await asyncio.sleep(1.0)
    finally:
        await pool.close()
        clear_inflight_file(inflight_path)

    _sync_outcomes_into_checkpoint()

    def _emit_run_summary() -> None:
        rows = sorted(shop_outcomes.values(), key=lambda r: int(r.get("index", 0)))
        ok_n = sum(1 for r in rows if r.get("status") == "ok")
        fail_n = sum(1 for r in rows if r.get("status") == "fail")
        skip_n = sum(1 for r in rows if r.get("status") == "skipped_done")
        inc_n = sum(1 for r in rows if r.get("status") == "incomplete")
        print("::run_summary_begin::", file=sys.stderr, flush=True)
        print(
            f"::run_summary::{ok_n}::{fail_n}::{inc_n}::{skip_n}::{len(shops)}",
            file=sys.stderr,
            flush=True,
        )
        for r in rows:
            name = str(r.get("name") or "?").replace("::", " - ")
            detail = str(r.get("detail") or "").replace("::", " - ").replace("\n", " ")
            status = r.get("status") or "incomplete"
            idx = int(r.get("index", 0)) + 1
            reviews = r.get("reviews", "")
            print(
                f"::summary_row::{status}::{idx}::{len(shops)}::{name}::{reviews}::{detail}",
                file=sys.stderr,
                flush=True,
            )
        print("::run_summary_end::", file=sys.stderr, flush=True)
        # Human-readable block for the live log
        print("::log::========== RUN SUMMARY ==========", file=sys.stderr, flush=True)
        print(
            f"::log::Completed OK: {ok_n}  |  Failed: {fail_n}  |  "
            f"Incomplete: {inc_n}  |  Already done: {skip_n}  |  Total: {len(shops)}",
            file=sys.stderr,
            flush=True,
        )
        for r in rows:
            status = r.get("status")
            mark = {
                "ok": "OK",
                "fail": "FAIL",
                "incomplete": "INCOMPLETE",
                "skipped_done": "DONE(earlier)",
            }.get(status, status)
            print(
                f"::log::  [{mark}] {r.get('name')} — {r.get('detail')}",
                file=sys.stderr,
                flush=True,
            )
        print("::log::=================================", file=sys.stderr, flush=True)

    if fatal_error_occurred or stop_requested.is_set():
        hub.close()
        _emit_run_summary()
        print(
            "\nProcess paused gracefully. Run again with `--resume` to pick up where you left off.",
            file=sys.stderr,
        )
    else:
        hub.close()
        _emit_run_summary()
        still_pending = [
            (index, shop)
            for index, shop in enumerate(shops)
            if not _is_completed(get_shop_key(shop, index))
        ]
        if not still_pending:
            if checkpoint_path.exists():
                checkpoint_path.unlink()
                print(
                    f"::log::All shops done — cleared checkpoint {checkpoint_path}",
                    file=sys.stderr,
                    flush=True,
                )
            print(
                f"::batch_remaining::0::{len(shops)}",
                file=sys.stderr,
                flush=True,
            )
            print(
                f"\nFinished scraping all {len(shops)} shops. Saved results to '{output_path}'.",
                file=sys.stderr,
            )
        else:
            # Keep checkpoint so Resume picks the next unfinished batch
            # (incomplete_shops stay eligible; completed_shops are skipped)
            _persist_checkpoint()
            print(
                f"::batch_remaining::{len(still_pending)}::{len(shops)}",
                file=sys.stderr,
                flush=True,
            )
            print(
                f"::log::Batch finished — {len(still_pending)} shop(s) still remaining "
                f"({len(incomplete_shops)} incomplete/failed). "
                f"Click Resume for the next batch.",
                file=sys.stderr,
                flush=True,
            )
            print(
                f"\nBatch complete. {len(still_pending)} of {len(shops)} shops left. "
                f"Click Resume (shops-per-run still applies). Output: '{output_path}'.",
                file=sys.stderr,
            )



def main() -> None:
    parser = argparse.ArgumentParser(
        description="Scrape Google Maps reviews with pause, resume, and network retry support."
    )
    parser.add_argument(
        "--max-reviews",
        type=int,
        default=0,
        help="Max reviews per shop. Set to 0 or negative to scrape ALL available reviews (default: 0).",
    )
    parser.add_argument(
        "--headed",
        action="store_true",
        help="Run with a visible browser (default is headless).",
    )
    parser.add_argument(
        "--shops",
        "-s",
        help="Shops input file (.csv or .json). If omitted, reads JSON from stdin.",
    )
    parser.add_argument(
        "--limit",
        type=int,
        help=(
            "Max shops to scrape this run. With --resume, takes the next N "
            "not-yet-completed shops from the file (not always the first N)."
        ),
    )
    parser.add_argument("--offset", type=int, default=0, help="Skip the first N shops")
    parser.add_argument(
        "--output",
        "-o",
        default="butcher-shop-canada-reviews.csv",
        help="CSV output path (default: butcher-shop-canada-reviews.csv)",
    )
    parser.add_argument(
        "--resume",
        "-r",
        action="store_true",
        help="Resume scraping using state from the checkpoint file.",
    )
    parser.add_argument(
        "--checkpoint-file",
        default=CHECKPOINT_DEFAULT_PATH,
        help=f"Path to checkpoint progress file (default: {CHECKPOINT_DEFAULT_PATH})",
    )
    parser.add_argument(
        "--concurrency",
        type=int,
        default=0,
        help=(
            "Parallel shops. 0 = auto from internet speed (default). "
            f"Capped at {DEFAULT_MAX_CONCURRENCY}."
        ),
    )
    parser.add_argument(
        "--max-concurrency",
        type=int,
        default=DEFAULT_MAX_CONCURRENCY,
        help=f"Max parallel shops when using auto speed mode (default: {DEFAULT_MAX_CONCURRENCY}).",
    )

    args = parser.parse_args()

    sys.stdout.reconfigure(encoding="utf-8")
    shops_path = Path(args.shops).resolve() if args.shops else None
    shops = load_shops(shops_path)

    if args.offset:
        shops = shops[args.offset :]
    # Do NOT slice by --limit here — run_scraper applies it to the next
    # unfinished shops so Resume can advance to 51–100, etc.

    shops = [shop for shop in shops if shop.get("name")]
    if not shops:
        raise SystemExit("No shops to scrape")

    output_path = Path(args.output).resolve()
    checkpoint_path = Path(args.checkpoint_file).resolve()

    explicit_concurrency = args.concurrency if args.concurrency and args.concurrency > 0 else None

    asyncio.run(
        run_scraper(
            shops=shops,
            max_reviews=args.max_reviews,
            headless=not args.headed,
            output_path=output_path,
            checkpoint_path=checkpoint_path,
            resume=args.resume,
            concurrency=explicit_concurrency,
            max_concurrency=args.max_concurrency,
            limit=args.limit,
        )
    )


if __name__ == "__main__":
    main()