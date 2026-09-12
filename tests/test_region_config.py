"""Guard against cross-region config shadowing.

mn/ and tn/ each hold a module named ``config``. Anything that puts both
region directories on sys.path and imports both scrapers — as
tests/test_scrapers.py does — makes a bare ``from config import ...``
ambiguous: the first region imported wins for the whole process, and the
second silently gets the first one's values.

That is not hypothetical. mn/scraper.py read tn/config.py this way, which
left mn_scraper.SHOWS_JSON pointing at tn/shows.json, and it raised
nothing because the two configs share most of their top-level names. The
scrapers now load their config by path (giglist/region_config.py).

These are pure import checks — no network — so they run on every push.
"""

import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "mn"))
# tn/ inserted last, so it sits *ahead* of mn/ — the exact ordering that
# made the bare import resolve to the wrong region.
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "tn"))

import mn.scraper as mn_scraper
import tn.scraper as tn_scraper


def test_each_region_loads_its_own_config_file():
    assert mn_scraper._config.__file__.endswith(os.path.join("mn", "config.py"))
    assert tn_scraper._config.__file__.endswith(os.path.join("tn", "config.py"))
    assert mn_scraper._config is not tn_scraper._config


def test_region_paths_do_not_cross():
    assert mn_scraper.REGION_DIR.name == "mn"
    assert tn_scraper.REGION_DIR.name == "tn"
    assert mn_scraper.SHOWS_JSON.parent.name == "mn"
    assert tn_scraper.SHOWS_JSON.parent.name == "tn"


def test_no_region_config_cached_under_bare_name():
    """A bare ``config`` in sys.modules is what made this ambiguous."""
    bare = sys.modules.get("config")
    assert bare is None or bare not in (mn_scraper._config, tn_scraper._config)
