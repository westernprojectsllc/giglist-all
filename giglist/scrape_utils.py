"""Scraper helpers shared by both regions: time parsing, generic
venue-fetch scaffolding (Tribe Events, Ticketmaster, Dice), dedupe,
and the junk/sports filter."""

import os
import re
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date, datetime
from html import unescape
from zoneinfo import ZoneInfo

import requests

from .http import (
    BROWSER_HEADERS, DEFAULT_HEADERS, DEFAULT_TIMEOUT, USER_AGENT,
    get_with_retry,
)
from .models import Show


CENTRAL_TZ = ZoneInfo("America/Chicago")

WS_RE = re.compile(r"\s+")
_NORM_DROP_RE = re.compile(r"[^\w\s]")


# ---------- time parsing ----------

def format_local_time(dt_local):
    """Format a local datetime as e.g. '7:30pm' or '7pm'."""
    h12 = dt_local.hour % 12 or 12
    ampm = "am" if dt_local.hour < 12 else "pm"
    if dt_local.minute:
        return f"{h12}:{dt_local.minute:02d}{ampm}"
    return f"{h12}{ampm}"


def parse_loose_time(s):
    """Parse a loose time string like '8:30 pm', '9PM', '9 P.M.' into the
    canonical '8:30pm' / '9pm' format. Returns None on failure."""
    if not s:
        return None
    cleaned = re.sub(r"[.\s]", "", s).upper()
    for fmt in ("%I:%M%p", "%I%p"):
        try:
            return format_local_time(datetime.strptime(cleaned, fmt))
        except ValueError:
            continue
    return None


_TIME_RE = re.compile(r"(\d{1,2})(?::(\d{2}))?\s*([ap])\.?\s*m\.?", re.I)


def normalize_time(raw):
    """Pull the first time out of a string like "7:30 PM" or "show 7pm"
    and return '7:30pm' / '7pm'. Returns None when no time is found."""
    if not raw:
        return None
    m = _TIME_RE.search(raw)
    if not m:
        return None
    h = int(m.group(1)) % 12
    mm = m.group(2)
    ampm = m.group(3).lower() + "m"
    if ampm == "pm":
        h += 12
    h12 = h % 12 or 12
    if mm and mm != "00":
        return f"{h12}:{mm}{ampm}"
    return f"{h12}{ampm}"


_TIME_TOKEN = r"\d{1,2}(?::\d{2})?\s*[ap]\.?\s*m\.?"


def find_time(text, label, label_before=True):
    """Extract a time near a label (e.g. 'Doors', 'Show'). ``label_before``
    picks which side of the time the label sits on — most sites put it
    before the time, Skydeck puts it after."""
    if not text:
        return None
    pattern = (
        rf"{label}\W*({_TIME_TOKEN})" if label_before
        else rf"({_TIME_TOKEN})\s*{label}"
    )
    m = re.search(pattern, text, re.I)
    return normalize_time(m.group(1)) if m else None


def infer_upcoming_date(month_str, day):
    """Some venues show month+day with no year. Pick the soonest year
    (this year or next) that still puts the date in the future."""
    today = date.today()
    for fmt in ("%b %d %Y", "%B %d %Y"):
        for year in (today.year, today.year + 1):
            try:
                dt = datetime.strptime(f"{month_str} {day} {year}", fmt).date()
            except ValueError:
                continue
            if dt >= today:
                return dt
    return None


# ---------- generic venue fetchers ----------

def scrape_tribe_events(base_url, venue_name, headers=None):
    """Generic WordPress "The Events Calendar" REST scraper. Page 1 tells
    us total_pages, so we fetch the remaining pages in parallel."""
    today_str = date.today().strftime("%Y-%m-%d")
    req_headers = headers or DEFAULT_HEADERS

    def fetch(page):
        url = f"{base_url}?per_page=50&page={page}&start_date={today_str}"
        print(f"  Fetching {venue_name} page {page}...")
        return requests.get(url, headers=req_headers, timeout=DEFAULT_TIMEOUT).json()

    try:
        first = fetch(1)
    except Exception as e:
        print(f"  Error fetching {venue_name} page 1: {e}")
        return []

    pages = [first]
    total_pages = first.get("total_pages", 1)
    if total_pages > 1:
        with ThreadPoolExecutor(max_workers=min(total_pages - 1, 4)) as pool:
            futures = [pool.submit(fetch, p) for p in range(2, total_pages + 1)]
            for future in as_completed(futures):
                try:
                    pages.append(future.result())
                except Exception as e:
                    print(f"  Error fetching {venue_name}: {e}")

    shows = []
    for data in pages:
        for event in data.get("events", []):
            try:
                dt = datetime.strptime(event.get("start_date", ""), "%Y-%m-%d %H:%M:%S")
            except ValueError:
                continue
            show_time = format_local_time(dt) if dt.hour != 0 else None
            shows.append(Show(
                title=unescape(event.get("title", "Unknown")),
                sort_date=dt.date(),
                venue=venue_name,
                url=event.get("url", ""),
                time=show_time,
            ))
    return shows


def _scrape_ticketmaster_venue(session, api_key, venue_name, venue_id, today_str):
    print(f"  Fetching {venue_name}...")
    shows = []
    page = 0
    while True:
        url = (
            f"https://app.ticketmaster.com/discovery/v2/events.json"
            f"?apikey={api_key}&venueId={venue_id}&startDateTime={today_str}"
            f"&size=50&page={page}&sort=date,asc"
        )
        try:
            data = get_with_retry(
                url, session=session, headers={}, retries=3, expect_json=True,
            )
        except Exception as e:
            # Keep whatever pages succeeded rather than dropping them all.
            print(f"  [TM {venue_name}] page {page} failed, keeping {len(shows)}: {e}")
            break

        events = data.get("_embedded", {}).get("events", [])
        if not events:
            break

        for event in events:
            date_str = event.get("dates", {}).get("start", {}).get("localDate", "")
            try:
                sort_date = datetime.strptime(date_str, "%Y-%m-%d").date()
            except ValueError:
                continue

            event_url = event.get("url", "")
            if not event_url:
                continue

            show_time = None
            local_time = event.get("dates", {}).get("start", {}).get("localTime", "")
            if local_time:
                try:
                    t = datetime.strptime(local_time, "%H:%M:%S")
                    show_time = t.strftime("%-I:%M%p").lower().replace(":00", "")
                except ValueError:
                    pass

            attractions = event.get("_embedded", {}).get("attractions", [])
            supports = (
                [a.get("name", "") for a in attractions[1:] if a.get("name")]
                if len(attractions) > 1 else []
            )

            sold_out = (
                event.get("dates", {}).get("status", {}).get("code", "") == "offsale"
            )

            shows.append(Show(
                title=event.get("name", "Unknown"),
                sort_date=sort_date,
                venue=venue_name,
                url=event_url,
                sold_out=sold_out,
                time=show_time,
                supports=supports,
            ))

        page_info = data.get("page", {})
        if page >= page_info.get("totalPages", 1) - 1:
            break
        page += 1

    return shows


def scrape_ticketmaster(venue_ids, api_key, max_workers=3):
    """Fetch Ticketmaster events for a dict of {venue_name: venue_id}.

    TM's rate limit is 5 req/sec, so 3 workers with in-flight 429 retries
    stays comfortably under the burst threshold while cutting wall-clock
    ~5x vs. sequential."""
    if not api_key:
        print("  Skipping Ticketmaster (no TM_API_KEY set)")
        return []

    today_str = date.today().strftime("%Y-%m-%dT00:00:00Z")
    shows = []
    venue_futures = {}
    with requests.Session() as session, ThreadPoolExecutor(max_workers=max_workers) as pool:
        for name, vid in venue_ids.items():
            f = pool.submit(_scrape_ticketmaster_venue, session, api_key, name, vid, today_str)
            venue_futures[f] = name
        for future in as_completed(venue_futures):
            name = venue_futures[future]
            try:
                shows.extend(future.result())
            except Exception as e:
                print(f"  [TM {name}] FAILED: {e}")
    return shows


DICE_API_URL = "https://partners-endpoint.dice.fm/api/v2/events"
# Dice's "partners" key is embedded in their own browser JS, so it isn't
# a secret — but keeping a live credential in source is still bad hygiene
# (the key can be revoked upstream, at which point overriding locally via
# env is the escape hatch). Prefer DICE_API_KEY from env; fall back to
# the public one so the scraper keeps working out of the box.
_DICE_PUBLIC_FALLBACK = "nJgJNUHjJM4Yuzmwo4LIe7nu1JDqGqnl8icHUeC9"
DICE_API_KEY = os.environ.get("DICE_API_KEY") or _DICE_PUBLIC_FALLBACK


def scrape_dice(venue_name, dice_venues, dice_promoters=None, exclude_tags=None):
    """Generic Dice.fm partners API scraper. ``dice_venues`` is the list
    of Dice venue names to filter by; ``dice_promoters`` is optional.
    ``exclude_tags`` is a set of Dice type_tags to drop (e.g. film)."""
    exclude_tags = set(exclude_tags or [])
    print(f"  Fetching {venue_name} (Dice)...")
    params = [("page[size]", "100"), ("types", "linkout,event")]
    for v in dice_venues:
        params.append(("filter[venues][]", v))
    for p in dice_promoters or []:
        params.append(("filter[promoters][]", p))

    try:
        response = requests.get(
            DICE_API_URL,
            params=params,
            headers={"x-api-key": DICE_API_KEY, "User-Agent": USER_AGENT},
            timeout=DEFAULT_TIMEOUT,
        )
        data = response.json()
    except Exception as e:
        print(f"  Error: {e}")
        return []

    today = date.today()
    shows = []

    for ev in data.get("data", []):
        name = (ev.get("name") or "").strip()
        date_str = ev.get("date")
        if not name or not date_str:
            continue
        if exclude_tags and any(t in exclude_tags for t in (ev.get("type_tags") or [])):
            continue

        try:
            dt_utc = datetime.strptime(date_str, "%Y-%m-%dT%H:%M:%SZ").replace(
                tzinfo=ZoneInfo("UTC")
            )
        except ValueError:
            continue
        dt_local = dt_utc.astimezone(CENTRAL_TZ)
        sort_date = dt_local.date()
        if sort_date < today:
            continue

        show_time = None
        doors = None
        for entry in ev.get("lineup") or []:
            label = (entry.get("details") or "").lower()
            t = entry.get("time")
            if not t:
                continue
            if "door" in label:
                doors = parse_loose_time(t)
            elif "show" in label and not show_time:
                show_time = parse_loose_time(t)

        if not show_time:
            show_time = format_local_time(dt_local)

        if doors == show_time:
            doors = None

        supports = []
        for artist in ev.get("artists") or []:
            if artist and artist.lower() not in name.lower():
                supports.append(artist)

        shows.append(Show(
            title=name,
            sort_date=sort_date,
            venue=venue_name,
            url=ev.get("url") or "",
            sold_out=bool(ev.get("sold_out")),
            time=show_time,
            supports=supports,
            doors=doors,
        ))

    return shows


# ---------- title normalization + dedupe ----------

def normalize_title(title, prefix_re=None):
    """Lowercased, depunctuated, whitespace-collapsed title for dedupe.
    ``prefix_re`` is an optional pre-compiled regex for stripping region-
    specific prefixes (e.g. "First Ave(nue) presents ")."""
    t = _NORM_DROP_RE.sub(" ", title.lower().replace("\xa0", " "))
    t = WS_RE.sub(" ", t).strip()
    if prefix_re:
        t = prefix_re.sub("", t)
    return t


def score(s):
    """Prefer the most enriched record; longer title breaks ties."""
    return (bool(s.time), bool(s.doors), len(s.supports or []), len(s.title))


_DEDUP_STOPWORDS = {"the", "a", "an", "and", "of", "with", "w"}
_STATUS_PREFIX_RE = re.compile(r"^\s*\*[^*]+\*\s*")
_PROMOTER_PREFIX_RE = re.compile(r"^.+?\bpresents:\s*", re.IGNORECASE)


def _dedup_first_token(title):
    t = _STATUS_PREFIX_RE.sub("", title)
    t = _PROMOTER_PREFIX_RE.sub("", t)
    for tok in normalize_title(t).split():
        if tok not in _DEDUP_STOPWORDS:
            return tok
    return None


def deduplicate(shows, same_artist_pass=False, prefix_re=None):
    """Three-stage dedupe:
      1. Exact match on (date, normalized-title, venue).
      2. Substring match at (date, venue) — collapse "X" vs "X - Tour"
         unless both carry explicit differing times (early/late shows).
      3. Optional same-artist pass: at (date, venue, time), collapse
         pairs whose first substantive token matches but aren't
         substrings of each other (e.g. "Flyleaf w/ Lacey Sturm" vs
         "Flyleaf with Lacey Sturm - 20th Anniversary Tour").
    """
    seen = {}
    for s in shows:
        key = (s.sort_date, normalize_title(s.title, prefix_re), s.venue)
        if key not in seen or score(s) > score(seen[key]):
            seen[key] = s
    shows = list(seen.values())

    by_dv = {}
    for i, s in enumerate(shows):
        by_dv.setdefault((s.sort_date, s.venue), []).append(i)
    drop = set()
    for idxs in by_dv.values():
        if len(idxs) < 2:
            continue
        norms = {i: normalize_title(shows[i].title, prefix_re) for i in idxs}
        for i in idxs:
            for j in idxs:
                if i >= j or i in drop or j in drop:
                    continue
                a, b = shows[i], shows[j]
                an, bn = norms[i], norms[j]
                if an == bn or (an not in bn and bn not in an):
                    continue
                if a.time and b.time and a.time != b.time:
                    continue
                drop.add(j if score(a) >= score(b) else i)

    if same_artist_pass:
        by_dvt = {}
        for i, s in enumerate(shows):
            if i in drop or not s.time:
                continue
            by_dvt.setdefault((s.sort_date, s.venue, s.time), []).append(i)
        for idxs in by_dvt.values():
            if len(idxs) < 2:
                continue
            tokens = {i: _dedup_first_token(shows[i].title) for i in idxs}
            for i in idxs:
                for j in idxs:
                    if i >= j or i in drop or j in drop:
                        continue
                    fa, fb = tokens[i], tokens[j]
                    if fa and fb and fa == fb:
                        drop.add(j if score(shows[i]) >= score(shows[j]) else i)

    return [s for i, s in enumerate(shows) if i not in drop]


# ---------- junk/sports filter ----------

COMMON_JUNK_KEYWORDS = [
    "select fee", "suite deposit", "suite rental", "parking pass",
    "vip upgrade", "fast lane", "locker rental", "merchandise",
    "gift card", "donation", "membership", "season ticket",
    "premium seat", "club access", "hospitality",
    "pre-show upgrade", "pre-show upsell",
]

COMMON_SPORTS_KEYWORDS = [
    "hockey", "basketball", "football", "baseball", "softball",
    "volleyball", "wrestling", "soccer", "lacrosse", "tennis",
    "nhl", "nba", "nfl", "mlb", "wnba", "mls", "ncaa",
    "high school", "state tournament",
    "harlem globetrotters", "monster jam", "monster truck",
    "wwe", "ufc", "paw patrol", "disney on ice", "ice show",
]


def filter_junk_and_sports(shows, *, junk_keywords, sports_venues,
                           sports_keywords, non_music_re=None,
                           non_music_exempt=()):
    """Drop upsell/merch junk, filter sports-venue events to music only,
    and optionally filter out non-music programming like bingo/comedy."""
    filtered = []
    for show in shows:
        title_lower = show.title.lower()
        if any(kw in title_lower for kw in junk_keywords):
            continue
        if show.venue in sports_venues and any(kw in title_lower for kw in sports_keywords):
            continue
        if non_music_re and non_music_re.search(title_lower):
            if not any(a in title_lower for a in non_music_exempt):
                continue
        filtered.append(show)
    return filtered


def find_duplicate_suspects(shows):
    """Return (date, venue, time) groups with >1 surviving show.

    The dedupe pass only merges same-time pairs whose first substantive
    token matches. Anything that survives with a shared slot is either a
    legit concurrent bill (multi-room venue) or a scrape glitch that
    needs human eyes — this surfaces them for review."""
    groups = {}
    for s in shows:
        if not s.time:
            continue
        groups.setdefault((s.sort_date, s.venue, s.time), []).append(s)
    return [(k, v) for k, v in groups.items() if len(v) > 1]


def normalize_titles(shows):
    """Collapse whitespace + NBSP across titles in place."""
    for s in shows:
        s.title = WS_RE.sub(" ", s.title.replace("\xa0", " ")).strip()
