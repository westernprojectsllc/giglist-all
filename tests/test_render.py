"""Unit tests for the region-agnostic render helpers.

Covers the small pure functions in giglist.render so that renderer
refactors don't silently break day/time sorting or the ledger label
formats (see DESIGN.md). Full-page rendering is exercised via
end-to-end `render.py` runs in CI, not here.
"""

from datetime import date

from giglist.models import Show
from giglist.render import (
    _day_bar_label,
    _get_week_monday,
    _ledger_time,
    _parse_time_minutes,
    _row_html,
    _show_sort_key,
    _week_label,
    _week_section_html,
)


# --- _get_week_monday ---------------------------------------------------

def test_get_week_monday_returns_same_day_for_monday():
    d = date(2026, 4, 20)  # Monday
    assert _get_week_monday(d) == d


def test_get_week_monday_snaps_wednesday_back_to_monday():
    d = date(2026, 4, 22)  # Wednesday
    assert _get_week_monday(d) == date(2026, 4, 20)


def test_get_week_monday_snaps_sunday_back_six_days():
    d = date(2026, 4, 26)  # Sunday
    assert _get_week_monday(d) == date(2026, 4, 20)


# --- _parse_time_minutes -----------------------------------------------

def test_parse_time_minutes_basic():
    assert _parse_time_minutes("7pm") == 19 * 60
    assert _parse_time_minutes("7:30pm") == 19 * 60 + 30
    assert _parse_time_minutes("11am") == 11 * 60


def test_parse_time_minutes_noon_and_midnight_edges():
    assert _parse_time_minutes("12am") == 0
    assert _parse_time_minutes("12pm") == 12 * 60


def test_parse_time_minutes_returns_none_on_bad_input():
    assert _parse_time_minutes(None) is None
    assert _parse_time_minutes("") is None
    assert _parse_time_minutes("garbage") is None
    assert _parse_time_minutes("25pm") is not None  # we don't validate range; doc this


# --- _show_sort_key -----------------------------------------------------

def test_show_sort_key_orders_by_venue_then_time_then_title():
    early = Show(title="A", sort_date=date(2026, 5, 1), venue="Exit/In", time="7pm")
    late = Show(title="B", sort_date=date(2026, 5, 1), venue="Exit/In", time="10pm")
    other_venue = Show(title="C", sort_date=date(2026, 5, 1), venue="Ryman Auditorium", time="6pm")
    rows = sorted([late, other_venue, early], key=_show_sort_key)
    assert [r.title for r in rows] == ["A", "B", "C"]


def test_show_sort_key_falls_back_to_doors_when_time_missing():
    has_time = Show(title="A", sort_date=date(2026, 5, 1), venue="V", time="8pm")
    has_doors = Show(title="B", sort_date=date(2026, 5, 1), venue="V", doors="7pm")
    rows = sorted([has_time, has_doors], key=_show_sort_key)
    # doors=7pm should sort before time=8pm
    assert [r.title for r in rows] == ["B", "A"]


def test_show_sort_key_pushes_untimed_last():
    timed = Show(title="A", sort_date=date(2026, 5, 1), venue="V", time="8pm")
    untimed = Show(title="B", sort_date=date(2026, 5, 1), venue="V")
    rows = sorted([untimed, timed], key=_show_sort_key)
    assert [r.title for r in rows] == ["A", "B"]


# --- ledger labels (formats fixed by DESIGN.md + mockup) ----------------

def test_ledger_time_show_only():
    s = Show(title="A", sort_date=date(2026, 7, 23), venue="V", time="7:30pm")
    assert _ledger_time(s) == "7:30PM"


def test_ledger_time_doors_and_show():
    s = Show(title="A", sort_date=date(2026, 7, 23), venue="V",
             time="8pm", doors="7pm")
    assert _ledger_time(s) == "7PM/8PM"


def test_ledger_time_doors_only():
    s = Show(title="A", sort_date=date(2026, 7, 23), venue="V", doors="7pm")
    assert _ledger_time(s) == "DRS7PM"


def test_ledger_time_unknown():
    s = Show(title="A", sort_date=date(2026, 7, 23), venue="V")
    assert _ledger_time(s) == "·"


def test_week_label_same_month():
    assert _week_label(date(2026, 7, 20)) == "WEEK OF JUL 20 – 26"


def test_week_label_crossing_months():
    assert _week_label(date(2026, 7, 27)) == "WEEK OF JUL 27 – AUG 2"


def test_day_bar_label_zero_pads_day():
    assert _day_bar_label(date(2026, 7, 6)) == "MONDAY — JUL 06"
    assert _day_bar_label(date(2026, 7, 23)) == "THURSDAY — JUL 23"


# --- row + week-section structure ---------------------------------------

def test_row_html_links_and_flags():
    s = Show(title="Harvey Street", sort_date=date(2026, 7, 23),
             venue="7th St Entry", url="https://example.com/e",
             time="8pm", doors="7pm", supports=["Bright Young Things"],
             sold_out=True)
    html = _row_html(s, {"7th St Entry": "https://example.com/v"})
    assert '<span class="t">7PM/8PM</span>' in html
    assert '<a href="https://example.com/v">7th St Entry</a>' in html
    assert '<a href="https://example.com/e">Harvey Street</a>' in html
    assert '<span class="sup">+ Bright Young Things</span>' in html
    assert '<span class="flag">Sold out</span>' in html


def test_row_html_plain_text_without_urls():
    s = Show(title="A & B", sort_date=date(2026, 7, 23), venue="V")
    html = _row_html(s, {})
    assert "<a " not in html
    assert "A &amp; B" in html
    assert '<span class="flag">' not in html


def test_week_section_has_anchor_day_bars_and_counts():
    monday = date(2026, 7, 20)
    shows = [
        Show(title="A", sort_date=date(2026, 7, 23), venue="V", time="7pm"),
        Show(title="B", sort_date=date(2026, 7, 23), venue="W", time="9pm"),
        Show(title="C", sort_date=date(2026, 7, 24), venue="V"),
    ]
    html = _week_section_html(monday, _week_label(monday), shows, {})
    assert 'id="week-2026-07-20"' in html
    assert '<h2 class="week-h">WEEK OF JUL 20 – 26</h2>' in html
    assert html.count('class="day-h"') == 2
    assert 'data-count="2"' in html  # Jul 23 has two shows
    assert 'data-count="1"' in html  # Jul 24 has one
    # No rendered timestamps anywhere in a week section (byte-stability).
    assert "Updated" not in html
