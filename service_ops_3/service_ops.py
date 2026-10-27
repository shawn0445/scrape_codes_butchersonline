import argparse
import asyncio
import csv
import json
import re
import sys
import threading
from pathlib import Path
from typing import Any, Callable, Optional
from urllib.parse import quote_plus

from playwright.async_api import (
    Browser,
    BrowserContext,
    Page,
    async_playwright,
    TimeoutError as PlaywrightTimeoutError,
)

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from csv_compat import canonical_shop, clean_maps_address, load_canonical_shops  # noqa: E402

CSV_COLUMNS = ["id", "name", "serviceOptions"]
CHECKPOINT_DEFAULT_PATH = "checkpoint.json"
DEFAULT_MAX_CONCURRENCY = 5
MAX_BROWSER_RESTARTS = 3
MAX_FAIL_RELOAD_PASSES = 2
ProgressCallback = Callable[[dict[str, Any]], None]

# Regex definitions for Google Place IDs
_HEX_PLACE_ID_RE = re.compile(r"^0x[0-9a-fA-F]+:0x[0-9a-fA-F]+$")
_CHIJ_PLACE_ID_RE = re.compile(r"^ChIJ[a-zA-Z0-9_-]+$")
_SHOPPING_OPTION_RE = re.compile(
    r"delivery|pick-?up|pickup|kerbside|curbside|in-store shopping|"
    r"on-site services|takeout|take-out|takeaway|dine-in|no-contact",
    re.I,
)
_ACCESS_OPTION_RE = re.compile(r"wheelchair|accessib", re.I)
_CANON_SERVICE_LABELS = (
    ("no-contact delivery", "No-contact delivery"),
    ("no contact delivery", "No-contact delivery"),
    ("kerbside", "Kerbside pickup"),
    ("curbside", "Curbside pickup"),
    ("in-store pick-up", "In-store pick-up"),
    ("in-store pickup", "In-store pick-up"),
    ("in store pick-up", "In-store pick-up"),
    ("pickup", "In-store pick-up"),
    ("in-store shopping", "In-store shopping"),
    ("on-site services", "On-site services"),
    ("onsite services", "On-site services"),
    ("take-out", "Takeout"),
    ("takeout", "Takeout"),
    ("takeaway", "Takeaway"),
    ("dine-in", "Dine-in"),
    ("delivery", "Delivery"),
)


class CaptchaBlockedError(Exception):
    """Raised when a CAPTCHA challenge is encountered."""
    pass


class SimpleBrowserPool:
    """Manages Playwright browser instances and context allocation across workers."""

    def __init__(self, headless: bool = True, max_workers: int = 5):
        self.headless = headless
        self.max_workers = max_workers
        self._playwright = None
        self._browser: Optional[Browser] = None
        self._contexts: list[BrowserContext] = []

    async def start(self) -> None:
        self._playwright = await async_playwright().start()
        args = [
            "--no-sandbox",
            "--disable-setuid-sandbox",
            "--disable-blink-features=AutomationControlled",
        ]
        try:
            self._browser = await self._playwright.chromium.launch(
                channel="chrome",
                headless=self.headless,
                args=args,
            )
        except Exception:
            self._browser = await self._playwright.chromium.launch(
                headless=self.headless,
                args=args,
            )

    async def acquire(self) -> Page:
        if not self._browser:
            await self.start()
        context = await self._browser.new_context(
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
            locale="en-CA",
            viewport={"width": 1400, "height": 1100},
        )
        self._contexts.append(context)
        return await context.new_page()

    async def release(self, page: Page) -> None:
        context = page.context
        await page.close()
        await context.close()
        if context in self._contexts:
            self._contexts.remove(context)

    async def restart(self, page: Page) -> None:
        await self.release(page)

    async def close(self) -> None:
        for ctx in list(self._contexts):
            await ctx.close()
        if self._browser:
            await self._browser.close()
        if self._playwright:
            await self._playwright.stop()


def build_search_query(shop: dict[str, Any]) -> str:
    """Builds a full fallback search query string using available shop metadata."""
    parts = []
    name = shop.get("name")
    if name:
        parts.append(str(name).strip())

    address = shop.get("address") or shop.get("street") or shop.get("location")
    if address:
        parts.append(clean_maps_address(address) or str(address).strip())

    city = shop.get("city")
    if city:
        parts.append(str(city).strip())

    state = shop.get("state") or shop.get("province")
    if state:
        parts.append(str(state).strip())

    postal = shop.get("postalCode") or shop.get("zip") or shop.get("zipCode")
    if postal:
        parts.append(str(postal).strip())

    return ", ".join(parts) if parts else "place"


def build_maps_url(shop: dict[str, Any]) -> str:
    """Prefer a direct place deep-link when googlePlaceId is known."""
    # Check if direct URL or link is explicitly provided in the entry
    if shop.get("url"):
        return str(shop["url"])
    if shop.get("link"):
        return str(shop["link"])

    place_id = str(shop.get("googlePlaceId") or "").strip() or None
    name = str(shop.get("name") or "place").strip() or "place"
    slug = quote_plus(name)
    lat = shop.get("latitude")
    lng = shop.get("longitude")

    if place_id and _HEX_PLACE_ID_RE.match(place_id):
        # Feature-id deep link
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


def load_shops(path: Optional[Path]) -> list[dict[str, Any]]:
    if not path or not path.exists():
        print(f"Error: Input shops file '{path}' not found.", file=sys.stderr)
        sys.exit(1)

    if path.suffix.lower() == ".json":
        with open(path, "r", encoding="utf-8") as f:
            payload = json.load(f)
        records = payload if isinstance(payload, list) else [payload]
        return [canonical_shop(record, i) for i, record in enumerate(records)]
    if path.suffix.lower() == ".csv":
        return load_canonical_shops(path)
    raise ValueError("Unsupported input format. Please specify a .json or .csv file.")


def load_checkpoint(checkpoint_path: Path) -> set[Any]:
    if checkpoint_path.exists():
        try:
            with open(checkpoint_path, "r", encoding="utf-8") as f:
                return set(json.load(f))
        except Exception:
            return set()
    return set()


def save_checkpoint(checkpoint_path: Path, completed_keys: set[Any]) -> None:
    with open(checkpoint_path, "w", encoding="utf-8") as f:
        json.dump(list(completed_keys), f, indent=2)


def upsert_result_to_csv(data: dict[str, Any], output_path: Path) -> None:
    """Insert or replace a shop row by id (or name) so retries never duplicate."""
    output_path = Path(output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    rows: list[dict[str, Any]] = []
    if output_path.exists() and output_path.stat().st_size > 0:
        with output_path.open(encoding="utf-8", newline="") as handle:
            rows = list(csv.DictReader(handle))
    new_row = {
        "id": data.get("id", ""),
        "name": data.get("name", ""),
        "serviceOptions": data.get("serviceOptions", ""),
    }
    key = str(new_row["id"] or "").strip()
    name = str(new_row["name"] or "").strip().lower()
    replaced = False
    for i, row in enumerate(rows):
        same_id = key and str(row.get("id") or "").strip() == key
        same_name = (not key) and str(row.get("name") or "").strip().lower() == name
        if same_id or same_name:
            rows[i] = new_row
            replaced = True
            break
    if not replaced:
        rows.append(new_row)
    tmp = output_path.with_suffix(output_path.suffix + ".tmp")
    with tmp.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=CSV_COLUMNS, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)
    tmp.replace(output_path)


def append_result_to_csv(data: dict[str, Any], output_path: Path) -> None:
    upsert_result_to_csv(data, output_path)


def get_shop_key(shop: dict[str, Any], index: int) -> str | int:
    return shop.get("id") or shop.get("googlePlaceId") or index


def canonicalize_service_label(text: str) -> str:
    value = re.sub(r"\s+", " ", str(text or "")).strip()
    value = re.sub(r"^(Has |No )", "", value, flags=re.I).strip(" ·-")
    value = re.sub(r"\s+for online orders$", "", value, flags=re.I).strip()
    if not value or _ACCESS_OPTION_RE.search(value):
        return ""
    lower = value.lower()
    if not _SHOPPING_OPTION_RE.search(lower):
        return ""
    for needle, canon in _CANON_SERVICE_LABELS:
        if needle in lower:
            return canon
    return value


def filter_service_options(items: list[str] | str) -> str:
    if isinstance(items, str):
        parts = re.split(r"[,·|]", items)
    else:
        parts = []
        for item in items:
            parts.extend(re.split(r"[,·|]", str(item or "")))
    seen: list[str] = []
    for part in parts:
        label = canonicalize_service_label(part)
        if label and label not in seen:
            seen.append(label)
    return ", ".join(seen)


def service_options_are_usable(text: str) -> bool:
    return bool(filter_service_options(text))


async def _accept_consent(page: Page) -> None:
    for selector in (
        'button:has-text("Accept all")',
        'button:has-text("I agree")',
        'form[action*="consent"] button',
    ):
        loc = page.locator(selector)
        try:
            if await loc.count() and await loc.first.is_visible():
                await loc.first.click(timeout=2000)
                await page.wait_for_timeout(500)
                return
        except Exception:
            continue


async def _leave_photo_view(page: Page) -> None:
    url = page.url or ""
    if ",3a," not in url and "!1e2" not in url and "!1e3" not in url:
        return
    for selector in (
        'button[aria-label="Back"]',
        'button[aria-label*="Back" i]',
        '[aria-label="Close"]',
    ):
        loc = page.locator(selector)
        try:
            if await loc.count() == 0:
                continue
            await loc.first.click(timeout=2500)
            await page.wait_for_timeout(1000)
            return
        except Exception:
            continue
    try:
        await page.keyboard.press("Escape")
        await page.wait_for_timeout(600)
    except Exception:
        pass


async def _open_place_panel(page: Page) -> None:
    await _leave_photo_view(page)
    if "/maps/place/" in (page.url or "") and ",3a," not in (page.url or ""):
        return
    for selector in (
        "a.hfpxzc",
        'a[href*="/maps/place/"]',
        'div[role="feed"] a[href*="/maps/place/"]',
    ):
        loc = page.locator(selector)
        try:
            if await loc.count() == 0:
                continue
            await loc.first.click(timeout=4000)
            await page.wait_for_timeout(1200)
            return
        except Exception:
            continue


async def extract_service_options(page: Page, label: str) -> str:
    """Read delivery/pickup from Overview chips; ignore Accessibility on About."""
    tag = f"[{label}]"
    try:
        await _open_place_panel(page)
        try:
            await page.wait_for_selector(
                "button.XJ8h0e, div.LTs0Rc, button[data-item-id='oh']",
                timeout=6000,
            )
        except Exception:
            pass

        chip = page.locator(
            "button.XJ8h0e, div.LTs0Rc, "
            'button:has-text("In-store"), button:has-text("Delivery")'
        )
        if await chip.count():
            print(f"{tag} STEP 1: Expanding Overview service chips...", file=sys.stderr)
            try:
                await chip.first.click(timeout=4000)
                await page.wait_for_timeout(800)
            except Exception:
                pass
        else:
            print(f"{tag} STEP 1: No Overview service chips yet.", file=sys.stderr)

        overview = await page.evaluate(
            """() => {
              const items = [];
              const add = (s) => {
                const t = (s || "").replace(/\\s+/g, " ").trim();
                if (t) items.push(t);
              };
              document.querySelectorAll("div.LTs0Rc, button.XJ8h0e, [aria-label^='Has '], [aria-label^='No ']").forEach((el) => {
                add(el.getAttribute("aria-label"));
                add(el.innerText);
              });
              return items;
            }"""
        )
        filtered = filter_service_options(overview or [])
        if filtered:
            print(f"{tag} STEP 2: Overview chips -> [{filtered}]", file=sys.stderr)
            return filtered

        print(f"{tag} STEP 2b: Checking hours panel for Delivery...", file=sys.stderr)
        for selector in ("span.HlvSq", "button[data-item-id='oh']"):
            loc = page.locator(selector)
            if await loc.count():
                try:
                    await loc.first.click(timeout=4000)
                    await page.wait_for_timeout(1400)
                    break
                except Exception:
                    continue
        hours_bits = await page.evaluate(
            """() => {
              const items = [];
              const add = (s) => {
                const t = (s || "").replace(/\\s+/g, " ").trim();
                if (t) items.push(t);
              };
              document.querySelectorAll("div.LTs0Rc, button.XJ8h0e, span.fontTitleSmall").forEach((el) => {
                add(el.getAttribute("aria-label"));
                add(el.innerText);
              });
              return items;
            }"""
        )
        filtered = filter_service_options(hours_bits or [])
        if not filtered:
            for name in (
                "Delivery",
                "In-store pick-up",
                "In-store shopping",
                "On-site services",
                "Kerbside pickup",
                "Curbside pickup",
                "No-contact delivery",
            ):
                loc = page.locator(f'text="{name}"')
                try:
                    if await loc.count():
                        filtered = filter_service_options(
                            [filtered, name] if filtered else [name]
                        )
                except Exception:
                    continue
        if filtered:
            print(f"{tag} STEP 2b: Hours panel -> [{filtered}]", file=sys.stderr)
            return filtered

        print(f"{tag} STEP 3: Checking About tab Service options section...", file=sys.stderr)
        about_tab = page.locator(
            'button[role="tab"][aria-label^="About"], '
            'button[aria-label="About"], '
            'div[role="tablist"] button[role="tab"]:has-text("About")'
        )
        if await about_tab.count() == 0:
            print(f"{tag} WARNING: About tab not found.", file=sys.stderr)
            return ""

        await about_tab.first.click()
        await page.wait_for_timeout(900)
        try:
            await page.mouse.wheel(0, 1400)
            await page.wait_for_timeout(400)
        except Exception:
            pass

        about_data = await page.evaluate(
            """() => {
              const items = [];
              const add = (s) => {
                const t = (s || "").replace(/\\s+/g, " ").trim();
                if (t) items.push(t);
              };
              const headers = Array.from(document.querySelectorAll("h2, h3, .fontTitleSmall, .iL3Qke"));
              const serviceHeader = headers.find((el) => /service options/i.test(el.textContent || ""));
              if (serviceHeader) {
                let root = serviceHeader.parentElement;
                for (let i = 0; i < 4 && root; i++) {
                  root.querySelectorAll("li.hpLkke, span[aria-label], div.iNvpkb").forEach((el) => {
                    add(el.getAttribute("aria-label"));
                    add(el.textContent);
                  });
                  root = root.parentElement;
                }
              }
              document.querySelectorAll("div.iP2t7d li.hpLkke, ul.ZQ6we li.hpLkke, li.hpLkke").forEach((li) => {
                const tick = li.querySelector("span.SwaGS");
                const invalid = li.querySelector("span.OazX1c");
                if (tick && !invalid) {
                  const span = li.querySelector("span[aria-label]") || li.querySelector("div.iNvpkb span:not(.google-symbols)");
                  add(span ? (span.getAttribute("aria-label") || span.textContent) : li.innerText);
                }
              });
              return items;
            }"""
        )
        filtered = filter_service_options(about_data or [])
        print(f"{tag} STEP 4: About filtered -> [{filtered}]", file=sys.stderr)
        return filtered
    except Exception as e:
        print(f"{tag} ERROR inside extract_service_options: {e}", file=sys.stderr)
        return ""


async def scrape_shop(page: Page, shop: dict[str, Any]) -> dict[str, Any]:
    """Navigates to place URL built via build_maps_url and extracts service options."""
    label = shop.get("name") or f"shop_{shop.get('id', '')}"
    tag = f"[{label}]"

    url = build_maps_url(shop)
    print(f"{tag} START: Navigating to {url}", file=sys.stderr)

    try:
        await page.goto(url, wait_until="domcontentloaded", timeout=25000)
        print(f"{tag} PAGE LOADED: buffering...", file=sys.stderr)
        await page.wait_for_timeout(1400)
        await _accept_consent(page)
        try:
            await page.wait_for_selector(
                'button.XJ8h0e, div.LTs0Rc, button[role="tab"], h1',
                timeout=8000,
            )
        except Exception:
            pass

        content = await page.content()
        if "google.com/sorry/index" in page.url or "recaptcha" in content.lower():
            print(f"{tag} BLOCKED: CAPTCHA detected on page.", file=sys.stderr)
            raise CaptchaBlockedError("Encountered Google CAPTCHA challenge.")

        service_options = await extract_service_options(page, label=label)
        return {
            "id": shop.get("id", ""),
            "name": shop.get("name", ""),
            "serviceOptions": service_options,
        }

    except PlaywrightTimeoutError:
        print(f"{tag} TIMEOUT: Page or component load timed out.", file=sys.stderr)
        return {"error": "Timeout loading page elements."}
    except CaptchaBlockedError:
        raise
    except Exception as e:
        print(f"{tag} ERROR: Exception raised during scrape: {e}", file=sys.stderr)
        return {"error": str(e)}


async def run_scraper(
    shops: list[dict[str, Any]],
    headless: bool = True,
    output_path: Path = Path("service_options_output.csv"),
    checkpoint_path: Path = Path(CHECKPOINT_DEFAULT_PATH),
    resume: bool = True,
    concurrency: Optional[int] = None,
    max_concurrency: int = DEFAULT_MAX_CONCURRENCY,
    limit: Optional[int] = None,
    on_progress: ProgressCallback | None = None,
    stop_event: threading.Event | None = None,
) -> dict[str, Any]:
    summary: dict[str, Any] = {
        "status": "completed",
        "output": str(output_path),
        "ok": 0,
        "fail": 0,
        "pending": 0,
        "message": "",
    }
    completed_keys = load_checkpoint(checkpoint_path) if resume else set()

    output_path = Path(output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    checkpoint_path = Path(checkpoint_path)
    checkpoint_path.parent.mkdir(parents=True, exist_ok=True)

    # Re-queue shops that were checkpointed but still have empty / accessibility-only options.
    if output_path.exists() and output_path.stat().st_size > 0:
        with output_path.open(encoding="utf-8", newline="") as handle:
            existing_rows = list(csv.DictReader(handle))
        empty_ids = {
            str(row.get("id") or "").strip()
            for row in existing_rows
            if not service_options_are_usable(str(row.get("serviceOptions") or ""))
        }
        if empty_ids:
            completed_keys = {k for k in completed_keys if str(k).strip() not in empty_ids}
            print(
                f"Re-queued {len(empty_ids)} shop(s) with empty or accessibility-only "
                f"service options for retry.",
                file=sys.stderr,
            )

    pending_shops = [
        (i, s) for i, s in enumerate(shops)
        if get_shop_key(s, i) not in completed_keys
    ]

    if limit:
        pending_shops = pending_shops[:limit]

    total_pending = len(pending_shops)
    summary["pending"] = total_pending
    print(f"Total pending shops to process: {total_pending} (Skipped completed: {len(completed_keys)})")

    if total_pending == 0:
        summary["message"] = "No pending shops left to scrape."
        print(summary["message"])
        return summary

    processed = 0
    retried = 0

    workers = min(concurrency or DEFAULT_MAX_CONCURRENCY, max_concurrency)
    pool = SimpleBrowserPool(headless=headless, max_workers=workers)
    await pool.start()

    work_q: asyncio.Queue = asyncio.Queue()
    for item in pending_shops:
        work_q.put_nowait(item)

    io_lock = asyncio.Lock()
    browser_restarts = 0
    shop_outcomes: dict[Any, dict[str, str]] = {}
    deferred_fails: list[tuple[int, dict[str, Any]]] = []

    async def _handle_success(index: int, shop: dict[str, Any], shop_key: Any, result: dict[str, Any], label: str):
        nonlocal processed
        options = filter_service_options(str(result.get("serviceOptions") or ""))
        if not options:
            async with io_lock:
                deferred_fails.append((index, shop))
                shop_outcomes[shop_key] = {"status": "retry"}
                print(f"[DEFER] {label}: no service options yet; retry after batch", file=sys.stderr)
            return
        success_result = {
            "id": shop.get("id", ""),
            "name": shop.get("name", ""),
            "serviceOptions": options,
        }
        async with io_lock:
            upsert_result_to_csv(success_result, output_path)
            completed_keys.add(shop_key)
            save_checkpoint(checkpoint_path, completed_keys)
            shop_outcomes[shop_key] = {"status": "ok"}
            processed += 1
            print(f"[SUCCESS] {label} -> [{options}]", file=sys.stderr)
            if on_progress:
                on_progress(
                    {
                        "percent": min(100.0, (processed / max(total_pending, 1)) * 100.0),
                        "done": processed,
                        "total": total_pending,
                        "status": f"Saved services: {label}",
                        "shop": label,
                    }
                )

    async def _defer_fail(index: int, shop: dict[str, Any], shop_key: Any, err: BaseException):
        label = shop.get("name") or f"shop_{index}"
        async with io_lock:
            deferred_fails.append((index, shop))
            shop_outcomes[shop_key] = {"status": "retry"}
            print(f"[DEFER] {label}: {err}", file=sys.stderr)

    async def _fail_exhausted(index: int, shop: dict[str, Any], shop_key: Any, err: BaseException):
        nonlocal processed
        fail_result = {
            "id": shop.get("id", ""),
            "name": shop.get("name", ""),
            "serviceOptions": "",
        }
        label = shop.get("name") or f"shop_{index}"
        async with io_lock:
            upsert_result_to_csv(fail_result, output_path)
            completed_keys.add(shop_key)
            save_checkpoint(checkpoint_path, completed_keys)
            shop_outcomes[shop_key] = {"status": "fail"}
            processed += 1
            print(f"[FAILED] {label}: {err}", file=sys.stderr)
            if on_progress:
                on_progress(
                    {
                        "percent": min(100.0, (processed / max(total_pending, 1)) * 100.0),
                        "done": processed,
                        "total": total_pending,
                        "status": f"Failed: {label}",
                        "shop": label,
                    }
                )

    async def worker_task(finalize_fails: bool):
        nonlocal browser_restarts
        while not work_q.empty():
            if stop_event and stop_event.is_set():
                summary["status"] = "stopped"
                while not work_q.empty():
                    try:
                        work_q.get_nowait()
                    except asyncio.QueueEmpty:
                        break
                break
            try:
                index, shop = work_q.get_nowait()
            except asyncio.QueueEmpty:
                break

            shop_key = get_shop_key(shop, index)
            label = shop.get("name") or f"shop_{index}"
            page = await pool.acquire()
            try:
                result = await scrape_shop(page, shop)
                if result.get("error") or not service_options_are_usable(
                    str(result.get("serviceOptions") or "")
                ):
                    err = Exception(result.get("error") or "empty service options")
                    if finalize_fails:
                        await _fail_exhausted(index, shop, shop_key, err)
                    else:
                        await _defer_fail(index, shop, shop_key, err)
                else:
                    await _handle_success(index, shop, shop_key, result, label)
            except CaptchaBlockedError as err:
                if browser_restarts < MAX_BROWSER_RESTARTS:
                    browser_restarts += 1
                    print(f"[CAPTCHA] Restarting browser ({browser_restarts}/{MAX_BROWSER_RESTARTS})...", file=sys.stderr)
                    await pool.restart(page)
                    await work_q.put((index, shop))
                elif finalize_fails:
                    await _fail_exhausted(index, shop, shop_key, err)
                else:
                    await _defer_fail(index, shop, shop_key, err)
            except Exception as err:
                if finalize_fails:
                    await _fail_exhausted(index, shop, shop_key, err)
                else:
                    await _defer_fail(index, shop, shop_key, err)
            finally:
                await pool.release(page)

    async def _run_pass(finalize_fails: bool) -> None:
        tasks = [
            asyncio.create_task(worker_task(finalize_fails)) for _ in range(workers)
        ]
        await asyncio.gather(*tasks, return_exceptions=True)

    await _run_pass(finalize_fails=False)

    reload_pass = 0
    while deferred_fails and reload_pass < MAX_FAIL_RELOAD_PASSES and summary["status"] != "stopped":
        reload_pass += 1
        retry_jobs = list(deferred_fails)
        deferred_fails.clear()
        retried += len(retry_jobs)
        print(
            f"\nRetry pass {reload_pass}/{MAX_FAIL_RELOAD_PASSES}: "
            f"{len(retry_jobs)} shop(s) after the rest of the batch.",
            file=sys.stderr,
        )
        if on_progress:
            on_progress(
                {
                    "percent": 90.0,
                    "status": f"Retrying {len(retry_jobs)} failed service shop(s)",
                    "shop": "",
                }
            )
        for item in retry_jobs:
            work_q.put_nowait(item)
        last_pass = reload_pass >= MAX_FAIL_RELOAD_PASSES
        await _run_pass(finalize_fails=last_pass)

    if deferred_fails and summary["status"] != "stopped":
        for index, shop in deferred_fails:
            await _fail_exhausted(
                index, shop, get_shop_key(shop, index), Exception("empty after retries")
            )
        deferred_fails.clear()

    await pool.close()

    ok_count = sum(1 for o in shop_outcomes.values() if o["status"] == "ok")
    fail_count = sum(1 for o in shop_outcomes.values() if o["status"] == "fail")
    summary["ok"] = ok_count
    summary["fail"] = fail_count
    summary["retried"] = retried
    verb = "Stopped" if summary["status"] == "stopped" else "Finished"
    summary["message"] = (
        f"{verb}: {ok_count}/{total_pending} shops with service options "
        f"({fail_count} failed, {retried} retried after batch). Saved to '{output_path}'."
    )
    print(f"\n{summary['message']}")
    return summary


def main() -> None:
    parser = argparse.ArgumentParser(description="Scrape Google Maps service options into CSV.")
    parser.add_argument("--shops", type=Path, required=True, help="Path to input file (.csv or .json).")
    parser.add_argument("--headless", action=argparse.BooleanOptionalAction, default=True, help="Run browser in headless mode.")
    parser.add_argument("--output", type=Path, default=Path("service_options_output.csv"), help="Output CSV file path.")
    parser.add_argument("--checkpoint", type=Path, default=Path(CHECKPOINT_DEFAULT_PATH), help="Checkpoint JSON path.")
    parser.add_argument("--resume", action=argparse.BooleanOptionalAction, default=True, help="Resume from checkpoint.")
    parser.add_argument("--concurrency", type=int, default=DEFAULT_MAX_CONCURRENCY, help="Parallel worker limit.")
    parser.add_argument("--limit", type=int, default=None, help="Limit number of shops processed.")

    args = parser.parse_args()

    shops = load_shops(args.shops)
    asyncio.run(
        run_scraper(
            shops=shops,
            headless=args.headless,
            output_path=args.output,
            checkpoint_path=args.checkpoint,
            resume=args.resume,
            concurrency=args.concurrency,
            limit=args.limit,
        )
    )


if __name__ == "__main__":
    main()