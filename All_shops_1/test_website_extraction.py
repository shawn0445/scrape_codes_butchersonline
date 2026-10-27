import contextlib
import csv
import io
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from playwright.sync_api import TimeoutError as PlaywrightTimeoutError

import scraper
from scraper import _list_card_needs_detail_visit, extract_place_details


class FakeLocator:
    def __init__(self, *, href=None, text="", timeout=False):
        self.href = href
        self.text = text
        self.timeout = timeout

    @property
    def first(self):
        return self

    def count(self):
        return int(bool(self.href or self.text))

    def inner_text(self):
        return self.text

    def get_attribute(self, name):
        if name == "href":
            return self.href
        return None

    def wait_for(self, *, state, timeout):
        if self.timeout:
            raise PlaywrightTimeoutError("website link not present")


class FakePage:
    url = "https://www.google.com/maps/place/Crocmignon/@46.8,-71.3,17z"

    def __init__(self, website_href=None):
        self.website = FakeLocator(
            href=website_href,
            timeout=website_href is None,
        )

    def wait_for_timeout(self, _milliseconds):
        pass

    def locator(self, selector):
        if selector == 'a[data-item-id="authority"]':
            return self.website
        if selector == "h1.DUwDvf":
            return FakeLocator(text="Crocmignon")
        return FakeLocator()


class FakeSearchPage:
    url = "https://www.google.com/maps/search/butcher+shop"


class WebsiteExtractionTests(unittest.TestCase):
    def test_missing_website_triggers_detail_visit_with_complete_card(self):
        card = {
            "title": "Crocmignon",
            "latitude": 46.8,
            "longitude": -71.3,
            "address": "123 Rue Example, Montreal, QC",
            "website": "N/A",
        }

        self.assertTrue(_list_card_needs_detail_visit(card))

    def test_card_with_website_does_not_need_website_detail_visit(self):
        card = {
            "title": "Crocmignon",
            "latitude": 46.8,
            "longitude": -71.3,
            "address": "123 Rue Example, Montreal, QC",
            "website": "https://crocmignon.com/",
        }

        self.assertFalse(_list_card_needs_detail_visit(card))

    def test_extracts_authority_link_href(self):
        page = FakePage("http://crocmignon.com/")

        details = extract_place_details(page)

        self.assertEqual(details["website"], "http://crocmignon.com/")

    def test_missing_authority_link_is_reported_as_no_website(self):
        page = FakePage()
        output = io.StringIO()

        with contextlib.redirect_stdout(output):
            details = extract_place_details(page)

        self.assertEqual(details["website"], "N/A")
        self.assertIn("No Maps website link found", output.getvalue())

    def test_scrape_visits_card_and_writes_detail_website_to_csv(self):
        scraper.reset_runtime_state()
        self.addCleanup(scraper.reset_runtime_state)
        card = {
            "url": "https://www.google.com/maps/place/Crocmignon/@46.8,-71.3,17z",
            "title": "Crocmignon",
            "address": "123 Rue Example, Montreal, QC H2X 1Y4",
            "phone": "514-555-0100",
            "website": "N/A",
            "rating": "4.9",
            "reviews": 227,
            "latitude": 46.8,
            "longitude": -71.3,
        }
        details = {
            **card,
            "website": "http://crocmignon.com/",
        }

        with tempfile.TemporaryDirectory() as temp_dir:
            output_path = Path(temp_dir) / "01_shops.csv"
            with (
                patch("scraper.SEARCH_KEYWORDS", ["butcher shop"]),
                patch("scraper._goto_with_retries", return_value=True),
                patch("scraper.dismiss_consent"),
                patch("scraper.sleep"),
                patch("scraper.scroll_results_feed"),
                patch("scraper.collect_place_cards", return_value=[card]),
                patch("scraper._extract_place_from_url", return_value=details) as visit,
                patch(
                    "scraper.analyze_website",
                    return_value={
                        "isButcherShop": True,
                        "siteType": "Regular Site",
                    },
                ),
                patch("scraper.save_progress"),
            ):
                scraper.process_city(
                    FakeSearchPage(),
                    {"name": "Montreal", "province": "QC"},
                    total_cities=1,
                    max_per_query=1,
                    effective_limit=None,
                    output=output_path,
                    controller=None,
                    workers=1,
                )

            visit.assert_called_once()
            with output_path.open(encoding="utf-8", newline="") as csv_file:
                saved_shop = next(csv.DictReader(csv_file))
            self.assertEqual(saved_shop["Website"], "http://crocmignon.com/")


if __name__ == "__main__":
    unittest.main()
