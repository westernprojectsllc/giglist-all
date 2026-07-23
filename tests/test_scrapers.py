"""Smoke tests for every scraper in both regions.

These do NOT test correctness — they test *liveness*. Each scraper hits
its real source over the network and we assert it returns a non-empty
list of valid Show objects with the basic fields populated. When a venue
redesigns their site and our scraper silently breaks, this test goes
red and CI emails the maintainer.

Run: pytest tests/test_scrapers.py -v
"""

import os
import sys
from datetime import date

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "mn"))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "tn"))

import mn.scraper as mn_scraper
import tn.scraper as tn_scraper
from giglist.models import Show

TM_API_KEY = os.environ.get("TM_API_KEY", "")

MN_SCRAPERS = [
    ("mn/first_avenue", mn_scraper.scrape_first_avenue),
    ("mn/dakota", mn_scraper.scrape_dakota),
    ("mn/cedar", mn_scraper.scrape_cedar),
    ("mn/orchestra", mn_scraper.scrape_orchestra),
    ("mn/ticketmaster", lambda: mn_scraper.scrape_ticketmaster(TM_API_KEY)),
    ("mn/myth", mn_scraper.scrape_myth),
    ("mn/white_squirrel", mn_scraper.scrape_white_squirrel),
    ("mn/icehouse", mn_scraper.scrape_icehouse),
    ("mn/331_club", mn_scraper.scrape_331),
    ("mn/skyway", mn_scraper.scrape_skyway),
    ("mn/pilllar", mn_scraper.scrape_pilllar),
    ("mn/underground", mn_scraper.scrape_underground),
    ("mn/zhora_darling", mn_scraper.scrape_zhora_darling),
    ("mn/cloudland", mn_scraper.scrape_cloudland),
    ("mn/parkway", mn_scraper.scrape_parkway),
    ("mn/berlin", mn_scraper.scrape_berlin),
    ("mn/uptown_vfw", mn_scraper.scrape_uptown_vfw),
    ("mn/aster_cafe", mn_scraper.scrape_aster_cafe),
    ("mn/green_room", mn_scraper.scrape_green_room),
]

TN_SCRAPERS = [
    ("tn/ticketmaster", lambda: tn_scraper.scrape_ticketmaster(TM_API_KEY)),
    ("tn/station_inn", tn_scraper.scrape_station_inn),
    ("tn/skydeck", tn_scraper.scrape_skydeck),
    ("tn/drkmttr", tn_scraper.scrape_drkmttr),
    ("tn/the_end", tn_scraper.scrape_the_end),
    ("tn/night_we_met", tn_scraper.scrape_night_we_met),
    ("tn/caverns", tn_scraper.scrape_caverns),
    ("tn/cobra", tn_scraper.scrape_cobra),
    ("tn/skinny_dennis", tn_scraper.scrape_skinny_dennis),
    ("tn/fogg_street", tn_scraper.scrape_fogg_street),
    ("tn/rudys", tn_scraper.scrape_rudys),
    ("tn/pinnacle", tn_scraper.scrape_pinnacle),
    ("tn/cannery_hall", tn_scraper.scrape_cannery_hall),
    ("tn/ascend", tn_scraper.scrape_ascend),
    ("tn/city_winery", tn_scraper.scrape_city_winery),
    ("tn/blue_room", tn_scraper.scrape_blue_room),
    ("tn/bluebird", tn_scraper.scrape_bluebird),
    ("tn/five_spot", tn_scraper.scrape_five_spot),
    ("tn/dees", tn_scraper.scrape_dees),
    ("tn/the_office", tn_scraper.scrape_the_office),
    # tn/analog intentionally absent — see scrape_analog docstring.
]

ALL_SCRAPERS = MN_SCRAPERS + TN_SCRAPERS


@pytest.mark.parametrize("name,fn", ALL_SCRAPERS, ids=[s[0] for s in ALL_SCRAPERS])
def test_scraper_returns_shows(name, fn):
    if "ticketmaster" in name and not TM_API_KEY:
        pytest.skip("TM_API_KEY not set")

    shows = fn()

    assert isinstance(shows, list), f"{name} did not return a list"
    assert len(shows) > 0, f"{name} returned 0 shows — site may have broken"

    s = shows[0]
    assert isinstance(s, Show), f"{name} returned non-Show item: {type(s).__name__}"
    assert s.title, f"{name} first show has empty title"
    assert s.venue, f"{name} first show has empty venue"
    assert isinstance(s.sort_date, date), f"{name} first show has non-date sort_date"
