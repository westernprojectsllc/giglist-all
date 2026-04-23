"""Unit tests for the region-agnostic render helpers.

Covers the small pure functions in giglist.render so that renderer
refactors don't silently break day/time sorting or the week navigation
grouping. Full-page rendering is exercised via end-to-end `render.py`
runs in CI, not here.
"""

from datetime import date

from giglist.models import Show
from giglist.render import (
    _build_week_nav,
    _get_week_monday,
    _parse_time_minutes,
    _show_sort_key,
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


# --- _build_week_nav ----------------------------------------------------

def test_build_week_nav_groups_by_month_and_highlights():
    weeks = [
        (date(2026, 4, 20), "Apr 20 - Apr 26", "4/20"),
        (date(2026, 4, 27), "Apr 27 - May 3",  "4/27"),
        (date(2026, 5, 4),  "May 4 - May 10",  "5/4"),
    ]
    html = _build_week_nav(weeks, highlight="Apr 27 - May 3")
    # All three weeks appear
    assert "Apr 20 - Apr 26" in html
    assert "Apr 27 - May 3" in html
    assert "May 4 - May 10" in html
    # The highlighted week is bold, the others are links
    assert "<strong>Apr 27 - May 3</strong>" in html
    assert '<a href="week-2026-04-20.html">' in html
    assert '<a href="week-2026-05-04.html">' in html
    # Weeks are grouped by month header line
    assert html.count('<div class="month-line">') == 2  # April, May
