"""Unit tests for the region-agnostic scrape_utils helpers.

These are pure (no network) and guard against regressions in the dedupe
passes, junk/sports/non-music filters, and the small parsing helpers.

Run: pytest tests/ -v
"""

import re
from datetime import date

import pytest

from giglist.models import Show
from giglist.scrape_utils import (
    _dedup_first_token,
    deduplicate,
    filter_junk_and_sports,
    find_duplicate_suspects,
    format_local_time,
    normalize_time,
    normalize_title,
    parse_loose_time,
)


D = date(2026, 5, 1)


def mk(title, venue="Exit/In", d=D, time="8pm", supports=None):
    return Show(title=title, sort_date=d, venue=venue, time=time,
                supports=supports or [])


# --- normalize_time -----------------------------------------------------

@pytest.mark.parametrize("raw,expected", [
    ("7:30 PM", "7:30pm"),
    ("7pm", "7pm"),
    ("show 7pm", "7pm"),
    ("Doors 6:00 pm, Show 7 PM", "6pm"),  # first match wins
    ("11:00 AM", "11am"),
    ("12:00 PM", "12pm"),
    ("12 AM", "12am"),
])
def test_normalize_time_extracts_first_time(raw, expected):
    assert normalize_time(raw) == expected


def test_normalize_time_returns_none_when_missing():
    assert normalize_time("no time here") is None
    assert normalize_time("") is None
    assert normalize_time(None) is None


# --- parse_loose_time ---------------------------------------------------

@pytest.mark.parametrize("raw,expected", [
    ("8:30 pm", "8:30pm"),
    ("9PM", "9pm"),
    ("9 P.M.", "9pm"),
    ("7:00pm", "7pm"),
])
def test_parse_loose_time(raw, expected):
    assert parse_loose_time(raw) == expected


def test_parse_loose_time_returns_none_on_garbage():
    assert parse_loose_time("not a time") is None
    assert parse_loose_time("") is None
    assert parse_loose_time(None) is None


# --- filter_junk_and_sports --------------------------------------------

JUNK = ["suite deposit", "premium seat", "concert upgrade", "betmgm",
        "event ticket required"]
SPORTS_VENUES = {"Bridgestone Arena"}
SPORTS_KW = ["hockey", "basketball"]
NON_MUSIC_RE = re.compile(
    r"\b(meditation|bingo|comedy|sober open mic|sex ed|stop the bleed)\b",
    re.IGNORECASE,
)
NON_MUSIC_EXEMPT = ("first aid kit", "bingo players")


def _f(shows):
    return filter_junk_and_sports(
        shows,
        junk_keywords=JUNK,
        sports_venues=SPORTS_VENUES,
        sports_keywords=SPORTS_KW,
        non_music_re=NON_MUSIC_RE,
        non_music_exempt=NON_MUSIC_EXEMPT,
    )


@pytest.mark.parametrize("title", [
    "Zen Posers Community Meditation",
    "Bingo Loco",
    "Up Top Comedy",
    "Sober Open Mic",
])
def test_non_music_titles_are_filtered(title):
    assert _f([mk(title)]) == []


@pytest.mark.parametrize("title", ["First Aid Kit", "Bingo Players"])
def test_exempt_artists_survive_non_music_filter(title):
    assert len(_f([mk(title)])) == 1


@pytest.mark.parametrize("title", [
    "Sex Education Soundtrack",   # word boundary: "\bsex ed\b" must not match
    "Open Mic Night",             # "\bsober open mic\b" must not match bare "open mic"
])
def test_non_music_word_boundaries(title):
    assert len(_f([mk(title)])) == 1


@pytest.mark.parametrize("title", [
    "Premium Seat Package",
    "Suite Deposit",
    "Florence + The Machine BetMGM Dinner Reservation",
    "VIP Concert Upgrade",
])
def test_junk_upsells_are_filtered(title):
    assert _f([mk(title, venue="Bridgestone Arena")]) == []


def test_junk_filter_is_venue_agnostic():
    rows = [mk("Suite Deposit", venue=v) for v in ("Exit/In", "Bridgestone Arena")]
    assert _f(rows) == []


def test_sports_filter_scoped_to_sports_venues():
    hockey_at_arena = mk("Predators Hockey", venue="Bridgestone Arena")
    hockey_at_club = mk("Hockey Dad", venue="Exit/In")  # band, not sport
    assert _f([hockey_at_arena]) == []
    assert len(_f([hockey_at_club])) == 1


def test_plain_music_titles_survive():
    assert len(_f([mk("Tyler Childers")])) == 1


# --- _dedup_first_token -------------------------------------------------

@pytest.mark.parametrize("title,expected", [
    ("*SOLD OUT* Sons Of Legion Night 1", "sons"),
    ("Good Dye Young Presents: Hayley Williams At A Bachelorette Party", "hayley"),
    ("The Protomen - Night One", "protomen"),
    ("Flyleaf with Lacey Sturm - 20th Anniversary Tour", "flyleaf"),
    ("& The Band", "band"),  # leading stopwords stripped
])
def test_dedup_first_token(title, expected):
    assert _dedup_first_token(title) == expected


# --- deduplicate: exact + substring passes ------------------------------

def test_dedupe_collapses_substring_title_keeps_richer():
    short = mk("Flyleaf", venue="Marathon Music Works")
    long = mk("Flyleaf with Lacey Sturm - 20th Anniversary Tour",
              venue="Marathon Music Works", supports=["Lacey Sturm"])
    result = deduplicate([short, long])
    assert len(result) == 1
    assert result[0].title == long.title


def test_dedupe_preserves_substring_titles_with_different_times():
    early = mk("Flyleaf", time="7pm")
    late = mk("Flyleaf with Lacey Sturm - 20th Anniversary Tour", time="10pm",
              supports=["Lacey Sturm"])
    result = deduplicate([early, late])
    assert len(result) == 2


def test_dedupe_collapses_exact_duplicate_keeps_richer():
    bare = mk("Tyler Childers", time=None)
    rich = mk("Tyler Childers", time="8pm", supports=["S. G. Goodman"])
    result = deduplicate([bare, rich])
    assert len(result) == 1
    assert result[0].time == "8pm"


def test_dedupe_preserves_different_artists_at_same_venue_date():
    a = mk("Tyler Childers", venue="Exit/In", time="7pm")
    b = mk("Molly Tuttle", venue="Exit/In", time="10pm")
    assert len(deduplicate([a, b])) == 2


# --- deduplicate: same_artist_pass (opt-in) -----------------------------

def test_dedupe_same_artist_pass_collapses_first_token_match():
    a = mk("Flyleaf w/ Lacey Sturm", venue="Marathon Music Works", time="8pm")
    b = mk("Flyleaf - 20th Anniversary", venue="Marathon Music Works", time="8pm")
    result = deduplicate([a, b], same_artist_pass=True)
    assert len(result) == 1


def test_dedupe_same_artist_pass_off_keeps_non_substring_pairs():
    a = mk("Flyleaf w/ Lacey Sturm", venue="Marathon Music Works", time="8pm")
    b = mk("Flyleaf - 20th Anniversary", venue="Marathon Music Works", time="8pm")
    result = deduplicate([a, b], same_artist_pass=False)
    assert len(result) == 2


# --- find_duplicate_suspects --------------------------------------------

def test_find_duplicate_suspects_flags_unresolved_conflicts():
    a = mk("David Peterson & 1946", venue="Station Inn", time="9pm")
    b = mk("Shannon Slaughter & County Clare", venue="Station Inn", time="9pm")
    solo = mk("Tyler Childers", time="8pm")
    result = deduplicate([a, b, solo])
    suspects = find_duplicate_suspects(result)
    assert len(suspects) == 1
    (d, v, t), rows = suspects[0]
    assert (d, v, t) == (D, "Station Inn", "9pm")
    assert {r.title for r in rows} == {a.title, b.title}


def test_find_duplicate_suspects_ignores_timeless_shows():
    a = mk("Band A", time=None)
    b = mk("Band B", time=None)
    assert find_duplicate_suspects([a, b]) == []


# --- normalize_title ----------------------------------------------------

def test_normalize_title_strips_punctuation_and_lowers():
    assert normalize_title("Hello, World!") == "hello world"


def test_normalize_title_collapses_nbsp_and_whitespace():
    assert normalize_title("foo\xa0\xa0  bar") == "foo bar"


def test_normalize_title_applies_prefix_re():
    prefix = re.compile(r"^first ave(nue)? presents\s*", re.IGNORECASE)
    assert normalize_title("First Ave presents Tyler Childers", prefix) == "tyler childers"


# --- format_local_time --------------------------------------------------

def test_format_local_time_minutes_and_noon_edge_cases():
    from datetime import datetime
    assert format_local_time(datetime(2026, 5, 1, 19, 30)) == "7:30pm"
    assert format_local_time(datetime(2026, 5, 1, 7, 0)) == "7am"
    assert format_local_time(datetime(2026, 5, 1, 0, 0)) == "12am"
    assert format_local_time(datetime(2026, 5, 1, 12, 0)) == "12pm"
