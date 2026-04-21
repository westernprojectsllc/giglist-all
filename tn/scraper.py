import os
import re
import json
import time
import requests
from bs4 import BeautifulSoup
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, date
from html import unescape
from zoneinfo import ZoneInfo
from dotenv import load_dotenv

from models import Show

load_dotenv()

REQUEST_TIMEOUT = 15
USER_AGENT = "Mozilla/5.0"
HTTP_HEADERS = {"User-Agent": USER_AGENT}

# Some venues (SpaceCraft-hosted ones like The Caverns) 403 a bare
# "Mozilla/5.0" UA but accept a fuller browser UA.
BROWSER_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36"
    )
}


def _infer_upcoming_date(month_str, day):
    """Some venues show month+day but no year. Pick the soonest year
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


_TIME_RE = re.compile(r"(\d{1,2})(?::(\d{2}))?\s*([ap])\.?\s*m\.?", re.I)


def _normalize_time(raw):
    """Normalize strings like "7:30 PM" / "7pm" / "8:00pm" to "7:30pm"."""
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


def _format_local_time(dt_local):
    """Format a local datetime as e.g. '7:30pm' or '7pm'."""
    h12 = dt_local.hour % 12 or 12
    ampm = "am" if dt_local.hour < 12 else "pm"
    if dt_local.minute:
        return f"{h12}:{dt_local.minute:02d}{ampm}"
    return f"{h12}{ampm}"


def _fetch_soup(venue_name, url, headers=None):
    """Shared fetch+parse used by the HTML-scraping venues."""
    print(f"  Fetching {venue_name}...")
    r = requests.get(url, headers=headers or HTTP_HEADERS, timeout=REQUEST_TIMEOUT)
    return BeautifulSoup(r.text, "html.parser")


_TIME_TOKEN = r"\d{1,2}(?::\d{2})?\s*[ap]\.?\s*m\.?"


def _find_time(text, label, label_before=True):
    """Pull a time ("7:30pm") out of free text, anchored by a label like
    "Doors" or "Show". `label_before` picks which side of the time the
    label sits on (Skydeck puts it after; most others put it before)."""
    if not text:
        return None
    if label_before:
        pattern = rf"{label}\W*({_TIME_TOKEN})"
    else:
        pattern = rf"({_TIME_TOKEN})\s*{label}"
    m = re.search(pattern, text, re.I)
    return _normalize_time(m.group(1)) if m else None


def _scrape_tribe_events(base_url, venue_name, headers=None):
    """Generic WordPress "The Events Calendar" REST scraper. Station Inn
    and Cobra both host this plugin. Some installs 403 a bare UA, so
    callers can pass BROWSER_HEADERS.

    Page 1 tells us total_pages, so we fetch the remaining pages in
    parallel instead of walking them sequentially."""
    today_str = date.today().strftime("%Y-%m-%d")
    req_headers = headers or HTTP_HEADERS

    def fetch(page):
        url = f"{base_url}?per_page=50&page={page}&start_date={today_str}"
        print(f"  Fetching {venue_name} page {page}...")
        return requests.get(url, headers=req_headers, timeout=REQUEST_TIMEOUT).json()

    try:
        first = fetch(1)
    except Exception as e:
        print(f"  Error fetching {venue_name} page 1: {e}")
        return []

    pages = [first]
    total_pages = first.get("total_pages", 1)
    if total_pages > 1:
        with ThreadPoolExecutor(max_workers=min(total_pages - 1, 4)) as pool:
            for future in as_completed([pool.submit(fetch, p) for p in range(2, total_pages + 1)]):
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
            show_time = _format_local_time(dt) if dt.hour != 0 else None
            shows.append(Show(
                title=unescape(event.get("title", "Unknown")),
                sort_date=dt.date(),
                venue=venue_name,
                url=event.get("url", ""),
                time=show_time,
            ))
    return shows


def scrape_station_inn():
    return _scrape_tribe_events(
        "https://stationinn.com/wp-json/tribe/events/v1/events",
        "Station Inn",
    )


def scrape_cobra():
    """Cobra uses the same tribe-events plugin as Station Inn, but its
    WordPress install 403s a bare "Mozilla/5.0" UA. Titles are prefixed
    with "Venue: " (main room) or "Front Bar: " (second room); the
    "Venue: " prefix is noise — strip it — but the "Front Bar: "
    prefix is signal (different physical room) so keep it."""
    shows = _scrape_tribe_events(
        "https://cobranashville.com/wp-json/tribe/events/v1/events",
        "Cobra",
        headers=BROWSER_HEADERS,
    )
    for s in shows:
        if s.title.startswith("Venue: "):
            s.title = s.title[len("Venue: "):]
    return shows


def scrape_skydeck():
    """Assembly Food Hall lists events for all its rooms on one page
    with a `data-venue` attribute on each card. Skydeck is the
    "rooftop" venue. Year isn't in the markup — we infer it."""
    # Cards with "this-week" timerange get re-rendered in the
    # "this-month" tab, so the same event shows up twice. The global
    # deduplicate() step at the end handles the duplicates.
    soup = _fetch_soup("Skydeck", "https://www.assemblyfoodhall.com/events?venue=rooftop")

    shows = []
    for card in soup.select('.m-event-card[data-venue="rooftop"]'):
        title_a = card.select_one(".m-event-card-text-container h3.title a")
        if not title_a:
            continue
        title = unescape(title_a.get_text(strip=True))
        event_url = title_a.get("href", "")

        month_el = card.select_one(".m-event-card-date-container .label.month")
        day_el = card.select_one(".m-event-card-date-container .title.day")
        if not (month_el and day_el):
            continue
        try:
            day_num = int(day_el.get_text(strip=True))
        except ValueError:
            continue
        dt = _infer_upcoming_date(month_el.get_text(strip=True), day_num)
        if not dt:
            continue

        # Time info sits in free-text like "6:00 PM Doors | 7:00 PM Showtime".
        text_blob = card.get_text(" ", strip=True)
        show_time = _find_time(text_blob, "Showtime", label_before=False)
        doors = _find_time(text_blob, "Doors", label_before=False)

        shows.append(Show(
            title=title,
            sort_date=dt,
            venue="Skydeck",
            url=event_url,
            time=show_time,
            doors=doors,
        ))
    return shows


def scrape_drkmttr():
    """DRKMTTR renders /shows as a Webflow collection. Each event is a
    `.ec-col-item`. Title, start_date ("May 2, 2026"), and a webflow
    slug link are in fixed child divs. The Calendar and Grid tabs both
    render the same items; the global deduplicate() handles that."""
    soup = _fetch_soup("DRKMTTR", "https://www.drkmttrcollective.com/shows")

    shows = []
    today = date.today()
    for item in soup.select(".ec-col-item.w-dyn-item"):
        title_el = item.select_one(".title")
        date_el = item.select_one(".start-date")
        link_el = item.select_one("a.webflow-link")
        if not (title_el and date_el):
            continue
        title = unescape(title_el.get_text(" ", strip=True))
        date_str = date_el.get_text(strip=True)
        try:
            dt = datetime.strptime(date_str, "%B %d, %Y").date()
        except ValueError:
            continue
        # Webflow returns past events on the same collection, so drop them.
        if dt < today:
            continue
        event_url = ""
        if link_el and link_el.get("href"):
            event_url = "https://www.drkmttrcollective.com" + link_el["href"]

        shows.append(Show(
            title=title,
            sort_date=dt,
            venue="DRKMTTR",
            url=event_url,
        ))
    return shows


def scrape_the_end():
    """The End uses Rockhouse Partners' WordPress events plugin. The
    /events/ page lists every upcoming show server-rendered, with a
    month separator preceding each month's block (the only place the
    year appears). We walk the doc in order, tracking the current year
    from the separators, then read each .eventWrapper."""
    soup = _fetch_soup("The End", "https://endnashville.com/events/")

    shows = []
    current_year = date.today().year
    nodes = soup.select(".rhp-events-list-separator-month, .eventWrapper.rhpSingleEvent")
    for el in nodes:
        classes = el.get("class") or []
        if "rhp-events-list-separator-month" in classes:
            try:
                current_year = datetime.strptime(el.get_text(strip=True), "%B %Y").year
            except ValueError:
                pass
            continue

        title_a = el.select_one("#eventTitle") or el.select_one(".eventTitleDiv a")
        date_el = el.select_one(".eventDateListTop #eventDate")
        if not (title_a and date_el):
            continue
        title = unescape(title_a.get("title") or title_a.get_text(" ", strip=True))
        event_url = title_a.get("href", "")

        # date_el text looks like "Mon, Apr 20"
        date_text = date_el.get_text(" ", strip=True).replace(",", "")
        parts = date_text.split()
        if len(parts) < 3:
            continue
        month_str, day_str = parts[1], parts[2]
        try:
            dt = datetime.strptime(
                f"{month_str} {day_str} {current_year}", "%b %d %Y"
            ).date()
        except ValueError:
            continue

        time_el = el.select_one(".rhp-event__time-text--list")
        raw = time_el.get_text(" ", strip=True) if time_el else ""
        show_time = _find_time(raw, "Show")
        doors = _find_time(raw, "Doors")

        cta = el.select_one(".rhp-event-cta")
        cta_classes = cta.get("class", []) if cta else []
        sold_out = "sold-out" in cta_classes

        shows.append(Show(
            title=title,
            sort_date=dt,
            venue="The End",
            url=event_url,
            time=show_time,
            doors=doors,
            sold_out=sold_out,
        ))
    return shows


def scrape_night_we_met():
    """Night We Met's /calendar page embeds a Shotgun events-listing
    widget. The widget JS pulls from this JSON endpoint, which returns
    all upcoming events for the organizer. startTime is a Unix epoch
    in the event's timezone field."""
    print("  Fetching Night We Met...")
    url = "https://shotgun.live/api/data/organizers/235887/events-listing-widget"
    r = requests.get(url, headers=HTTP_HEADERS, timeout=REQUEST_TIMEOUT)
    data = r.json()

    shows = []
    for event in data.get("events", []):
        start_ts = event.get("startTime")
        if not start_ts:
            continue
        try:
            tz = ZoneInfo(event.get("timezone") or "America/Chicago")
        except Exception:
            tz = ZoneInfo("America/Chicago")
        dt_local = datetime.fromtimestamp(start_ts, tz=tz)

        shows.append(Show(
            title=unescape(event.get("name", "Unknown")),
            sort_date=dt_local.date(),
            venue="Night We Met",
            url=event.get("permalink", ""),
            sold_out=bool(event.get("isSoldOut", False)),
            time=_format_local_time(dt_local),
        ))
    return shows


def scrape_caverns():
    """The Caverns runs on SpaceCraft CMS. The /shows page server-
    renders the next batch of events. SpaceCraft's CDN rejects bare
    'Mozilla/5.0', so we use a fuller Chrome UA."""
    soup = _fetch_soup("The Caverns", "https://www.thecaverns.com/shows", BROWSER_HEADERS)

    shows = []
    for item in soup.select(".eventColl-item"):
        title_a = item.select_one(".eventColl-eventInfo a")
        if not title_a:
            continue
        title = unescape(title_a.get_text(" ", strip=True))
        href = title_a.get("href", "")
        event_url = ("https://www.thecaverns.com" + href) if href.startswith("/") else href

        month_el = item.select_one(".eventColl-month")
        date_el = item.select_one(".eventColl-date")
        if not (month_el and date_el):
            continue
        try:
            day_num = int(date_el.get_text(strip=True))
        except ValueError:
            continue
        dt = _infer_upcoming_date(month_el.get_text(strip=True), day_num)
        if not dt:
            continue

        # .eventColl-detail--doors holds the show time (confusingly
        # named); actual doors time sits inside --restrictions.
        show_el = item.select_one(".eventColl-detail--doors")
        show_time = _normalize_time(show_el.get_text(" ", strip=True)) if show_el else None
        restr_el = item.select_one(".eventColl-detail--restrictions")
        doors = _find_time(restr_el.get_text(" ", strip=True), "Doors") if restr_el else None

        sold_out = item.get("data-event-status") == "soldout"

        shows.append(Show(
            title=title,
            sort_date=dt,
            venue="The Caverns",
            url=event_url,
            time=show_time,
            doors=doors,
            sold_out=sold_out,
        ))
    return shows


def scrape_fogg_street():
    """Fogg Street is a Squarespace site. Each event on /calendar is a
    stack of Squarespace blocks (no outer wrapper per event), so we
    extract titles and dates as two parallel streams in document order
    and zip them. Date strings look like "SAT, JUNE 6" / "WED, SEPT 19"
    — no year, and "SEPT" / "JUNE" don't fit %b or %B, so we normalize
    the month to its first 3 chars before inferring the year."""
    soup = _fetch_soup("Fogg Street", "https://www.foggstreet.live/calendar", BROWSER_HEADERS)

    titles = []
    for h3 in soup.select("h3"):
        strong = h3.find("strong")
        if not strong:
            continue
        t = strong.get_text(strip=True)
        # Event titles on this page are uppercase; incidental headers
        # ("UPCOMING SHOWS") are filtered out below by requiring a date.
        if t:
            titles.append(unescape(t))

    dates = []
    for p in soup.select("p.sqsrte-large"):
        strong = p.find("strong")
        if not strong:
            continue
        m = re.match(r"^[A-Z]+,\s*([A-Z]+)\s+(\d{1,2})$", strong.get_text(strip=True))
        if not m:
            continue
        dt = _infer_upcoming_date(m.group(1)[:3].title(), int(m.group(2)))
        if dt:
            dates.append(dt)

    shows = []
    for title, dt in zip(titles, dates):
        if title.upper() == "TBA":
            continue
        shows.append(Show(
            title=title,
            sort_date=dt,
            venue="Fogg Street Lawn Club",
            url="https://www.foggstreet.live/calendar",
        ))
    return shows


def scrape_rudys():
    """Rudy's Jazz Room uses the Tiva Events Calendar WordPress plugin.
    All events — past and upcoming — are embedded in a FullCalendar
    config block as a JavaScript `events: [...]` array. Each entry's
    `start` has a bogus +00:00 offset but is actually a Central Time
    wall-clock value (6pm/9pm sets), so we strip the tz info."""
    print("  Fetching Rudy's Jazz Room...")
    r = requests.get("https://rudysjazzroom.com/calendar", headers=BROWSER_HEADERS, timeout=REQUEST_TIMEOUT)
    m = re.search(r"events:\s*(\[.+?\]),", r.text, re.S)
    if not m:
        print("  [Rudy's] events array not found")
        return []
    events = json.loads(m.group(1))

    today = date.today()
    shows = []
    for event in events:
        try:
            dt = datetime.fromisoformat(event.get("start", "")).replace(tzinfo=None)
        except ValueError:
            continue
        if dt.date() < today:
            continue
        shows.append(Show(
            title=unescape(event.get("title") or "Unknown"),
            sort_date=dt.date(),
            venue="Rudy's Jazz Room",
            url=event.get("eventurl") or "",
            time=_format_local_time(dt),
        ))
    return shows


def scrape_skinny_dennis():
    """Skinny Dennis books exclusively through DICE. Their DICE venue
    page is a Next.js app that embeds all upcoming events in a
    __NEXT_DATA__ JSON blob, so we pull and parse that directly."""
    print("  Fetching Skinny Dennis...")
    url = "https://dice.fm/venue/skinny-dennis-nashville-2ww96"
    r = requests.get(url, headers=BROWSER_HEADERS, timeout=REQUEST_TIMEOUT)
    m = re.search(r'<script id="__NEXT_DATA__"[^>]*>(.*?)</script>', r.text, re.S)
    if not m:
        print("  [Skinny Dennis] __NEXT_DATA__ not found")
        return []
    data = json.loads(m.group(1))
    sections = data.get("props", {}).get("pageProps", {}).get("profile", {}).get("sections", [])

    shows = []
    for section in sections:
        for event in section.get("events") or []:
            start_str = (event.get("dates") or {}).get("event_start_date")
            if not start_str:
                continue
            try:
                dt = datetime.fromisoformat(start_str)
            except ValueError:
                continue
            perm = event.get("perm_name") or ""
            shows.append(Show(
                title=unescape((event.get("name") or "Unknown").strip()),
                sort_date=dt.date(),
                venue="Skinny Dennis",
                url=f"https://dice.fm/event/{perm}" if perm else "",
                sold_out=bool(event.get("sold_out")) or event.get("status") == "sold-out",
                time=_format_local_time(dt),
            ))
    return shows


# Ticketmaster venue IDs for Nashville-area venues. Looked up via the TM
# Discovery API venues endpoint (keyword + stateCode=TN). Every venue here
# returned nonzero upcoming events at the time this was written.
TM_VENUE_IDS = {
    "Bridgestone Arena":           "KovZpZA6taAA",
    "Ryman Auditorium":            "KovZpa61Ge",
    "Ascend Amphitheater":         "KovZpZAEet7A",
    "Grand Ole Opry House":        "KovZpa3Jbe",
    "Brooklyn Bowl Nashville":     "KovZ917APep",
    "FirstBank Amphitheater":      "KovZ917AJek",
    "Nissan Stadium":              "KovZpZA7AnJA",
    "The Pinnacle":                "KovZ917ARXe",
    "Cannery Hall":                "KovZ917A_O0",
    "Exit/In":                     "KovZpZAFaFnA",
    "The Basement East":           "KovZ917ACl7",
    "The Basement":                "KovZpZAkdn6A",
    "Marathon Music Works":        "KovZpZAJnJlA",
    "3rd & Lindsley":              "KovZpZA16IvA",
    "Eastside Bowl":               "Z7r9jZa7r1",
    "Schermerhorn Symphony Center": "KovZpZAEvF7A",
    "TPAC":                        "KovZpZA1nl6A",
}


def _tm_get(session, url):
    """Ticketmaster returns 429 when we burst above 5 req/sec. Retry a
    couple of times with backoff before giving up on the page."""
    for attempt in range(3):
        response = session.get(url, timeout=REQUEST_TIMEOUT)
        if response.status_code == 429:
            time.sleep(0.5 * (attempt + 1))
            continue
        return response.json()
    raise RuntimeError(f"Ticketmaster rate-limited after retries: {url[:80]}")


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
            data = _tm_get(session, url)
        except Exception as e:
            # Keep whatever shows we already gathered on earlier pages
            # rather than throwing away a successful multi-page fetch
            # because of a transient failure partway through.
            print(f"  [TM {venue_name}] page {page} failed, keeping {len(shows)} shows: {e}")
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
            supports = [a.get("name", "") for a in attractions[1:] if a.get("name")] if len(attractions) > 1 else []

            shows.append(Show(
                title=event.get("name", "Unknown"),
                sort_date=sort_date,
                venue=venue_name,
                url=event_url,
                sold_out=event.get("dates", {}).get("status", {}).get("code", "") == "offsale",
                time=show_time,
                supports=supports,
            ))

        page_info = data.get("page", {})
        if page >= page_info.get("totalPages", 1) - 1:
            break
        page += 1

    return shows


def scrape_ticketmaster(api_key):
    if not api_key:
        print("  Skipping Ticketmaster (no TM_API_KEY set)")
        return []

    today_str = date.today().strftime("%Y-%m-%dT00:00:00Z")
    shows = []
    # TM's rate limit is 5 req/sec. 3 workers with in-flight retries
    # stays comfortably under the burst threshold while still cutting
    # wall-clock ~5x vs. sequential.
    venue_futures = {}
    with requests.Session() as session, ThreadPoolExecutor(max_workers=3) as pool:
        for name, vid in TM_VENUE_IDS.items():
            f = pool.submit(_scrape_ticketmaster_venue, session, api_key, name, vid, today_str)
            venue_futures[f] = name
        for future in as_completed(venue_futures):
            name = venue_futures[future]
            try:
                shows.extend(future.result())
            except Exception as e:
                print(f"  [TM {name}] FAILED: {e}")
    return shows


_WS_RE = re.compile(r"\s+")
_NORM_DROP_RE = re.compile(r"[^\w\s]")


def _normalize_title(title):
    """Lowercased, depunctuated, whitespace-collapsed title for dedupe."""
    t = _NORM_DROP_RE.sub(" ", title.lower().replace("\xa0", " "))
    return _WS_RE.sub(" ", t).strip()


# Strip *SOLD OUT* / "X Presents:" before picking the artist token so
# those prefixes don't mask same-artist same-time duplicates.
_STATUS_PREFIX_RE = re.compile(r"^\s*\*[^*]+\*\s*")
_PROMOTER_PREFIX_RE = re.compile(r"^.+?\bpresents:\s*", re.IGNORECASE)
_DEDUP_STOPWORDS = {"the", "a", "an", "and", "of", "with", "w"}


def _dedup_first_token(title):
    t = _STATUS_PREFIX_RE.sub("", title)
    t = _PROMOTER_PREFIX_RE.sub("", t)
    for tok in _normalize_title(t).split():
        if tok not in _DEDUP_STOPWORDS:
            return tok
    return None


def _score(s):
    # Prefer the most enriched record; longer title breaks ties.
    return (bool(s.time), bool(s.doors), len(s.supports or []), len(s.title))


def deduplicate(shows):
    # Exact dedupe by (date, normalized title, venue).
    seen = {}
    for s in shows:
        key = (s.sort_date, _normalize_title(s.title), s.venue)
        if key not in seen or _score(s) > _score(seen[key]):
            seen[key] = s
    shows = list(seen.values())

    # Substring dedupe at same (date, venue): collapse "X" vs "X - Tour"
    # unless both carry explicit times that differ (early/late shows).
    by_dv = {}
    for i, s in enumerate(shows):
        by_dv.setdefault((s.sort_date, s.venue), []).append(i)
    drop = set()
    for idxs in by_dv.values():
        if len(idxs) < 2:
            continue
        norms = {i: _normalize_title(shows[i].title) for i in idxs}
        for i in idxs:
            for j in idxs:
                if i >= j or i in drop or j in drop:
                    continue
                a, b = shows[i], shows[j]
                an, bn = norms[i], norms[j]
                if an not in bn and bn not in an:
                    continue
                if a.time and b.time and a.time != b.time:
                    continue
                drop.add(j if _score(a) >= _score(b) else i)

    # Same-artist dedupe at (date, venue, time): catches pairs that aren't
    # substrings of each other but share an artist (e.g. "Flyleaf w/ Lacey
    # Sturm" vs "Flyleaf with Lacey Sturm - 20th Anniversary Tour"). Time
    # must match so early/late double-headers aren't collapsed.
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
                    drop.add(j if _score(shows[i]) >= _score(shows[j]) else i)

    return [s for i, s in enumerate(shows) if i not in drop]


SPORTS_KEYWORDS = [
    "hockey", "basketball", "football", "baseball", "softball",
    "volleyball", "wrestling", "soccer", "lacrosse", "tennis",
    "titans", "predators", "nashville sc", "sounds",
    "vanderbilt", "commodores", "belmont bruins",
    "nhl", "nba", "nfl", "mlb", "wnba", "mls", "ncaa",
    "high school", "state tournament",
    "harlem globetrotters", "monster jam", "monster truck",
    "wwe", "ufc", "paw patrol", "disney on ice", "ice show",
]

JUNK_KEYWORDS = [
    "select fee", "suite deposit", "suite rental", "parking pass",
    "vip upgrade", "fast lane", "locker rental", "merchandise",
    "gift card", "donation", "membership", "season ticket",
    "premium seat", "club access", "hospitality",
    "pre-show upgrade", "pre-show upsell",
    "betmgm", "concert upgrade", "event ticket required",
]

SPORTS_VENUES = {"Bridgestone Arena", "Nissan Stadium"}

# Word-boundary matched so "meditation" won't hit mid-word substrings.
NON_MUSIC_PATTERNS = [
    r"\bmeditation\b",
    r"\bco[- ]?working\b",
    r"\bbingo\b",
    r"\bcomedy\b",
    r"\bsex ed\b",
    r"\bstop the bleed\b",
    r"\bsober open mic\b",
]

# Music acts whose names collide with NON_MUSIC_PATTERNS — never filter.
# "first aid kit" is a defensive entry for a future "\bfirst aid\b" pattern.
NON_MUSIC_EXEMPT_ARTISTS = [
    "first aid kit",
    "bingo players",
]

_NON_MUSIC_RE = re.compile("|".join(NON_MUSIC_PATTERNS), re.IGNORECASE)


def filter_junk_and_sports(shows):
    filtered = []
    for show in shows:
        title_lower = show.title.lower()
        if any(kw in title_lower for kw in JUNK_KEYWORDS):
            continue
        if show.venue in SPORTS_VENUES:
            if any(kw in title_lower for kw in SPORTS_KEYWORDS):
                continue
        if (_NON_MUSIC_RE.search(title_lower)
                and not any(a in title_lower for a in NON_MUSIC_EXEMPT_ARTISTS)):
            continue
        filtered.append(show)
    return filtered


def find_duplicate_suspects(shows):
    """Return (date, venue, time) groups with >1 surviving show.

    The dedupe pass only merges same-time pairs whose first substantive
    token matches. Anything that survives with a shared slot is either a
    legit concurrent bill (multi-room venue) or a scrape glitch that
    needs human eyes — this surfaces them for review.
    """
    groups = {}
    for s in shows:
        if not s.time:
            continue
        groups.setdefault((s.sort_date, s.venue, s.time), []).append(s)
    return [(k, v) for k, v in groups.items() if len(v) > 1]



if __name__ == "__main__":
    TM_API_KEY = os.environ.get("TM_API_KEY", "")

    # Each scraper is network-bound, so run them all in parallel.
    scrapers = [
        ("Ticketmaster venues", lambda: scrape_ticketmaster(TM_API_KEY)),
        ("Station Inn", scrape_station_inn),
        ("Skydeck", scrape_skydeck),
        ("DRKMTTR", scrape_drkmttr),
        ("The End", scrape_the_end),
        ("Night We Met", scrape_night_we_met),
        ("The Caverns", scrape_caverns),
        ("Cobra", scrape_cobra),
        ("Skinny Dennis", scrape_skinny_dennis),
        ("Fogg Street Lawn Club", scrape_fogg_street),
        ("Rudy's Jazz Room", scrape_rudys),
    ]

    shows = []
    with ThreadPoolExecutor(max_workers=len(scrapers)) as executor:
        futures = {executor.submit(fn): name for name, fn in scrapers}
        for future in as_completed(futures):
            name = futures[future]
            try:
                result = future.result()
                shows += result
                print(f"  [{name}] {len(result)} shows")
            except Exception as e:
                print(f"  [{name}] FAILED: {e}")

    # Stable secondary sort so parallel scrapers can't change the
    # output file's row order between runs when the data is identical.
    shows.sort(key=lambda x: (x.sort_date, x.venue, x.title))
    for s in shows:
        s.title = _WS_RE.sub(" ", s.title.replace("\xa0", " ")).strip()
    shows = deduplicate(shows)
    shows = filter_junk_and_sports(shows)

    suspects = find_duplicate_suspects(shows)
    if suspects:
        print(f"\n  [warn] {len(suspects)} same-(date,venue,time) group(s) survived dedupe — review:")
        for (d, v, t), rows in sorted(suspects):
            print(f"    {d.isoformat()}  {v} @ {t}")
            for r in rows:
                print(f"      - {r.title}")

    # Dump to shows.json for render.py to consume. Splitting scrape from
    # render lets you re-render the HTML without re-scraping every venue.
    out_path = "shows.json"
    with open(out_path, "w") as f:
        json.dump([s.to_json_dict() for s in shows], f, indent=2)
    print(f"\nWrote {len(shows)} shows to {out_path}")
