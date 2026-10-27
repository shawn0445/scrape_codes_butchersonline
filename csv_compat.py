"""Case-insensitive CSV helpers so every step can read the shops file."""

from __future__ import annotations

import csv
import re
from pathlib import Path
from typing import Any, Iterable


BLANK = {"", "n/a", "nan", "none", "null", "-"}
_CATEGORY_TAIL_RE = re.compile(
    r"(Portuguese-style butcher'?s store|butcher shop|meat market|meat shop)\s*$",
    re.I,
)
_STATUS_TAIL_RE = re.compile(
    r"\s*(Permanently closed|Temporarily closed|Opens soon|Open 24 hours|Open|Closed)\s*$",
    re.I,
)
_GLUED_STATUS_RE = re.compile(
    r"(?<=[A-Za-z0-9])(Opens soon|Open 24 hours|Open|Closed)$",
    re.I,
)
_GLUED_PHONE_RE = re.compile(r"\+1[\s\-]?\d[\d\s().-]{7,}$")


def field(row: dict[str, Any], *names: str, default: str = "") -> str:
    """Return the first non-blank value among case-insensitive column names."""
    lookup = {str(k).lower(): k for k in row}
    for name in names:
        key = lookup.get(name.lower())
        if key is None:
            continue
        val = row.get(key)
        if val is None:
            continue
        text = str(val).strip()
        if text and text.lower() not in BLANK:
            return text
    return default


def clean_maps_address(text: Any) -> str:
    """Strip Maps list-card junk glued onto street addresses (Open/Closed, category)."""
    value = str(text or "").strip()
    if not value or value.lower() in BLANK:
        return ""
    value = _CATEGORY_TAIL_RE.sub("", value).strip()
    value = _STATUS_TAIL_RE.sub("", value).strip()
    value = _GLUED_STATUS_RE.sub("", value).strip()
    value = _GLUED_PHONE_RE.sub("", value).strip()
    return value.strip(" ,")


def cleanup_shop_address_columns(path: Path) -> int:
    """Rewrite Address/address columns in a shops CSV. Returns rows changed."""
    path = Path(path)
    if not path.exists():
        return 0
    fieldnames, rows = read_csv_rows(path)
    changed = 0
    for row in rows:
        for key in list(row.keys()):
            if key.lower() != "address":
                continue
            cleaned = clean_maps_address(row.get(key))
            if cleaned and cleaned != str(row.get(key) or "").strip():
                row[key] = cleaned
                changed += 1
    if changed:
        write_csv_rows(path, fieldnames, rows)
    return changed


def count_csv_rows(path: Path) -> int:
    if not path.exists():
        return 0
    try:
        with path.open(encoding="utf-8-sig", newline="") as handle:
            reader = csv.reader(handle)
            next(reader, None)
            return sum(1 for _ in reader)
    except OSError:
        return 0


def read_csv_rows(path: Path) -> tuple[list[str], list[dict[str, str]]]:
    with path.open(encoding="utf-8-sig", newline="") as handle:
        reader = csv.DictReader(handle)
        fieldnames = list(reader.fieldnames or [])
        rows = [{k: (v if v is not None else "") for k, v in row.items()} for row in reader]
    return fieldnames, rows


def write_csv_rows(path: Path, fieldnames: Iterable[str], rows: list[dict[str, Any]]) -> None:
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    names = list(fieldnames)
    tmp = path.with_suffix(path.suffix + ".tmp")
    with tmp.open("w", encoding="utf-8-sig", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=names, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)
    for _ in range(6):
        try:
            tmp.replace(path)
            return
        except PermissionError:
            import time

            time.sleep(0.25)
    tmp.replace(path)


def backfill_ids(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Give each shop a stable sequential id, keeping any ids already present."""
    used: set[int] = set()
    parsed: list[int | None] = []
    for row in rows:
        raw = field(row, "id", "shopId")
        try:
            n = int(float(raw)) if raw else 0
        except (TypeError, ValueError):
            n = 0
        if n > 0 and n not in used:
            parsed.append(n)
            used.add(n)
            row["id"] = n
        else:
            parsed.append(None)
            row.pop("id", None)

    next_id = 1
    for row, existing in zip(rows, parsed):
        if existing is not None:
            continue
        while next_id in used:
            next_id += 1
        row["id"] = next_id
        used.add(next_id)
        next_id += 1
    return rows


def ensure_shop_ids(path: Path) -> int:
    """Add an `id` column to the shops CSV if it is missing. Returns row count."""
    path = Path(path)
    if not path.exists():
        return 0
    fieldnames, rows = read_csv_rows(path)
    backfill_ids(rows)
    if "id" not in fieldnames:
        fieldnames = ["id", *[n for n in fieldnames if n.lower() != "id"]]
    write_csv_rows(path, fieldnames, rows)
    return len(rows)


def canonical_shop(row: dict[str, Any], index: int = 0) -> dict[str, Any]:
    """Normalize a shops-CSV row into lowercase keys used by later scrapers."""
    shop_id = field(row, "id", "shopId")
    try:
        parsed_id: int | str = int(float(shop_id)) if shop_id else index + 1
    except (TypeError, ValueError):
        parsed_id = shop_id or index + 1
    return {
        "id": parsed_id,
        "name": field(row, "name", "shopName"),
        "address": clean_maps_address(field(row, "address")),
        "city": field(row, "city"),
        "province": field(row, "province", "state"),
        "state": field(row, "state", "province"),
        "postalCode": field(row, "postalCode", "Postal_Code", "zip", "zipCode"),
        "latitude": field(row, "latitude"),
        "longitude": field(row, "longitude"),
        "phone": field(row, "phone"),
        "website": field(row, "website", "url", "link"),
        "url": field(row, "url", "link", "website"),
        "googlePlaceId": field(row, "googlePlaceId"),
        "rating": field(row, "rating"),
        "reviews": field(row, "reviews"),
        "siteType": field(row, "Site_Type", "siteType"),
    }


def load_canonical_shops(path: Path) -> list[dict[str, Any]]:
    _fields, rows = read_csv_rows(path)
    backfill_ids(rows)
    return [canonical_shop(row, i) for i, row in enumerate(rows)]
