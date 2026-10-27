"""Central folders and CSV names for the Canada butcher pipeline."""

from __future__ import annotations

from pathlib import Path

ROOT = Path(__file__).resolve().parent

ALL_SHOPS_DIR = ROOT / "All_shops_1"
HOURS_DIR = ROOT / "playwright_hours_2"
SERVICE_OPS_DIR = ROOT / "service_ops_3"
SUMMARY_DIR = ROOT / "GenerateSummary_4"
REVIEWS_DIR = ROOT / "reviews_scraping_5"

OUTPUT_DIR = ROOT / "output"
WORK_DIR = ROOT / "work"

# Five deliverable CSVs — one per pipeline step.
SHOPS_CSV = OUTPUT_DIR / "01_shops.csv"
HOURS_CSV = OUTPUT_DIR / "02_hours.csv"
SERVICE_OPTIONS_CSV = OUTPUT_DIR / "03_service_options.csv"
SUMMARY_CSV = OUTPUT_DIR / "04_summary.csv"
REVIEWS_CSV = OUTPUT_DIR / "05_reviews.csv"

OUTPUT_FILES = {
    "shops": SHOPS_CSV,
    "hours": HOURS_CSV,
    "services": SERVICE_OPTIONS_CSV,
    "summary": SUMMARY_CSV,
    "reviews": REVIEWS_CSV,
}

# Working / resume files (not the five deliverables).
SHOPS_PROGRESS = WORK_DIR / "shops_progress.json"
SHOPS_FAILED_URLS = WORK_DIR / "shops_failed_urls.json"
HOURS_FAILED_CSV = WORK_DIR / "hours_failed.csv"
SERVICE_CHECKPOINT = WORK_DIR / "service_ops_checkpoint.json"
REVIEWS_CHECKPOINT = WORK_DIR / "reviews_checkpoint.json"
REVIEWS_INFLIGHT = WORK_DIR / "reviews_inflight.json"

STEP_META = [
    {
        "key": "shops",
        "number": 1,
        "title": "Discover shops across Canada",
        "short": "Shops",
        "csv": SHOPS_CSV,
        "csv_name": "01_shops.csv",
        "depends_on": None,
        "summary": (
            "Search Google Maps in 270+ Canadian cities for butcher shops, "
            "meat markets, and related businesses. This is the master list "
            "every later step reads from."
        ),
        "produces": "Name, address, city, phone, website, rating, and coordinates.",
    },
    {
        "key": "hours",
        "number": 2,
        "title": "Collect opening hours",
        "short": "Hours",
        "csv": HOURS_CSV,
        "csv_name": "02_hours.csv",
        "depends_on": "shops",
        "summary": (
            "Open each shop from the master CSV on Google Maps and copy "
            "Monday–Sunday hours into a dedicated hours file."
        ),
        "produces": "One row per shop with hours_monday … hours_sunday.",
    },
    {
        "key": "services",
        "number": 3,
        "title": "Collect service options",
        "short": "Services",
        "csv": SERVICE_OPTIONS_CSV,
        "csv_name": "03_service_options.csv",
        "depends_on": "shops",
        "summary": (
            "Open each Google Maps listing and record delivery, pickup, and "
            "in-store shopping from the Overview service chips."
        ),
        "produces": "id, name, and a comma-separated serviceOptions list.",
    },
    {
        "key": "summary",
        "number": 4,
        "title": "Generate listing summaries",
        "short": "Summary",
        "csv": SUMMARY_CSV,
        "csv_name": "04_summary.csv",
        "depends_on": "shops",
        "summary": (
            "Merge shops + hours + service options into a listing title and "
            "a readable description paragraph for each shop."
        ),
        "produces": "Original shop columns plus title and description.",
    },
    {
        "key": "reviews",
        "number": 5,
        "title": "Scrape customer reviews",
        "short": "Reviews",
        "csv": REVIEWS_CSV,
        "csv_name": "05_reviews.csv",
        "depends_on": "shops",
        "summary": (
            "Collect Google Maps reviews for each shop in the master CSV. "
            "Supports pause, resume, and a shops-per-run batch size."
        ),
        "produces": "One row per review (author, rating, text, relative time).",
    },
]


def ensure_dirs() -> None:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    WORK_DIR.mkdir(parents=True, exist_ok=True)
