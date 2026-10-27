#!/usr/bin/env python3
import csv
from collections import Counter
from pathlib import Path

out = Path(r"D:\Mdata\work_08_14\Scrape_Codes\output")


def load(name):
    p = out / name
    with p.open(encoding="utf-8-sig", newline="") as handle:
        rows = list(csv.DictReader(handle))
    return p, rows


def blank(value):
    text = str(value or "").strip().lower()
    return text in ("", "n/a", "nan", "none", "not listed")


print("=== FILE SIZES ===")
for path in sorted(out.glob("*.csv")):
    print(f"{path.name:28s} {path.stat().st_size / 1024:7.1f} KB")

_, shops = load("01_shops.csv")
print("\n=== 01_shops.csv ===")
print("rows", len(shops))
print("cities", dict(Counter(r["City"] for r in shops)))
for field in ["Address", "Phone", "Website", "Postal_Code", "Rating", "Reviews"]:
    filled = sum(1 for r in shops if not blank(r.get(field)))
    print(f"  filled {field:12s} {filled}/{len(shops)}")
open_closed = sum(
    1 for r in shops if str(r.get("Address", "")).endswith(("Open", "Closed"))
)
print("  addresses ending Open/Closed", open_closed)
print("  sample:")
for row in shops[:4]:
    print(
        "   ",
        row["id"],
        row["Name"],
        "|",
        row["City"],
        "|",
        row["Phone"],
        "| rating",
        row["Rating"],
    )

_, hours = load("02_hours.csv")
days = [
    "hours_monday",
    "hours_tuesday",
    "hours_wednesday",
    "hours_thursday",
    "hours_friday",
    "hours_saturday",
    "hours_sunday",
]
print("\n=== 02_hours.csv ===")
print("rows", len(hours))
any_hours = sum(1 for r in hours if any(not blank(r.get(d)) for d in days))
all7 = sum(1 for r in hours if all(not blank(r.get(d)) for d in days))
print("  any hours", any_hours, "/", len(hours))
print("  all 7 days", all7, "/", len(hours))
print(
    "  no hours:",
    [r["name"] for r in hours if all(blank(r.get(d)) for d in days)],
)

_, svcs = load("03_service_options.csv")
print("\n=== 03_service_options.csv ===")
print("rows", len(svcs))
filled_s = [r for r in svcs if not blank(r.get("serviceOptions"))]
empty_s = [r["name"] for r in svcs if blank(r.get("serviceOptions"))]
print("  with options", len(filled_s), "/", len(svcs))
print("  empty", empty_s)
opts = Counter()
for row in filled_s:
    for item in row["serviceOptions"].split(","):
        opts[item.strip()] += 1
print("  option counts", dict(opts))

_, summaries = load("04_summary.csv")
print("\n=== 04_summary.csv ===")
print("rows", len(summaries))
print("  titles", sum(1 for r in summaries if not blank(r.get("title"))))
print("  descriptions", sum(1 for r in summaries if not blank(r.get("description"))))
print("  sample title:", (summaries[0].get("title") or "")[:140])
print("  sample desc:", (summaries[0].get("description") or "")[:240])

_, revs = load("05_reviews.csv")
print("\n=== 05_reviews.csv ===")
print("rows", len(revs))
by_shop = Counter(r.get("shopName") for r in revs)
print("  shops represented", len(by_shop))
print("  per shop", dict(by_shop))
ok = [r for r in revs if not blank(r.get("content"))]
err = [r for r in revs if not blank(r.get("error"))]
print("  with text", len(ok))
print("  with error", len(err))
if err:
    print("  error sample", err[0].get("shopName"), err[0].get("error"))
if ok:
    print("  sample", ok[0].get("shopName"), ok[0].get("authorName"), ok[0].get("rating"))
