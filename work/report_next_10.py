#!/usr/bin/env python3
import csv
from collections import Counter
from pathlib import Path

out = Path(r"D:\Mdata\work_08_14\Scrape_Codes\output")


def load(name):
    with (out / name).open(encoding="utf-8-sig", newline="") as handle:
        return list(csv.DictReader(handle))


def blank(value):
    return str(value or "").strip().lower() in ("", "n/a", "nan", "none", "not listed")


shops = load("01_shops.csv")
hours = load("02_hours.csv")
svcs = load("03_service_options.csv")
summaries = load("04_summary.csv")
revs = load("05_reviews.csv")
days = [
    "hours_monday",
    "hours_tuesday",
    "hours_wednesday",
    "hours_thursday",
    "hours_friday",
    "hours_saturday",
    "hours_sunday",
]

print("shops", len(shops), "ids", [r["id"] for r in shops[-10:]])
print("cities", dict(Counter(r["City"] for r in shops)))
open_closed = sum(
    1 for r in shops if str(r.get("Address", "")).endswith(("Open", "Closed"))
)
print("addresses still Open/Closed", open_closed)
print("phones", sum(1 for r in shops if not blank(r.get("Phone"))), "/", len(shops))
print("new shop sample:")
for row in shops[20:]:
    print(" ", row["id"], row["Name"], "|", row["City"], "|", row["Address"][:50])

any_h = sum(1 for r in hours if any(not blank(r.get(d)) for d in days))
print("\nhours rows", len(hours), "with hours", any_h)
print("missing hours", [r["name"] for r in hours if all(blank(r.get(d)) for d in days)])

filled_s = [r for r in svcs if not blank(r.get("serviceOptions"))]
print("\nservices rows", len(svcs), "with options", len(filled_s))
print("empty", [r["name"] for r in svcs if blank(r.get("serviceOptions"))])
ids = [str(r.get("id")) for r in svcs]
print("service id dupes", len(ids) - len(set(ids)))

print("\nsummary rows", len(summaries), "titles", sum(1 for r in summaries if not blank(r.get("title"))))

print("\nreviews rows", len(revs))
rids = [str(r.get("googleReviewId") or "") for r in revs if str(r.get("googleReviewId") or "").strip()]
print("review ids", len(rids), "unique", len(set(rids)))
by = Counter(r.get("shopName") for r in revs)
print("shops in reviews", len(by))
err = [r for r in revs if not blank(r.get("error"))]
print("error rows", len(err), [r.get("shopName") for r in err[:5]])
new_names = {r["Name"] for r in shops[20:]}
print("new shops with reviews", sum(1 for n in new_names if by.get(n)))
for n in sorted(new_names):
    print(" ", n, by.get(n, 0))
