"""Nashville scraper. Pulls events from ~17 venues in parallel, dedupes,
filters out junk/sports/non-music, and writes shows.json for render.py."""

import json
import os
import re
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date, datetime
from html import unescape
from pathlib import Path
from zoneinfo import ZoneInfo

import requests
from bs4 import BeautifulSoup
from dotenv import load_dotenv

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from giglist.http import (
    BROWSER_HEADERS, DEFAULT_HEADERS, DEFAULT_TIMEOUT,
)
from giglist.models import Show
from giglist.scrape_utils import (
    deduplicate, filter_junk_and_sports, find_duplicate_suspects,
    find_time, format_local_time, infer_upcoming_date, normalize_time,
    normalize_titles, scrape_ticketmaster as _scrape_tm, scrape_tribe_events,
)

from config import (
    JUNK_KEYWORDS, NON_MUSIC_EXEMPT_ARTISTS, NON_MUSIC_RE, REGION_DIR,
    SPORTS_KEYWORDS, SPORTS_VENUES, TICKETMASTER_VENUES,
)

load_dotenv()


SHOWS_JSON = REGION_DIR / "shows.json"


def _fetch_soup(venue_name, url, headers=None):
    """Shared fetch+parse used by the HTML-scraping venues."""
    print(f"  Fetching {venue_name}...")
    r = requests.get(url, headers=headers or DEFAULT_HEADERS, timeout=DEFAULT_TIMEOUT)
    return BeautifulSoup(r.text, "html.parser")


def scrape_station_inn():
    return scrape_tribe_events(
        "https://stationinn.com/wp-json/tribe/events/v1/events",
        "Station Inn",
    )


def scrape_cobra():
    """Cobra uses the same tribe-events plugin as Station Inn, but its
    WordPress install 403s a bare "Mozilla/5.0" UA. Titles are prefixed
    with "Venue: " (main room) or "Front Bar: " (second room); the
    "Venue: " prefix is noise — strip it — but the "Front Bar: "
    prefix is signal (different physical room) so keep it."""
    shows = scrape_tribe_events(
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
        dt = infer_upcoming_date(month_el.get_text(strip=True), day_num)
        if not dt:
            continue

        # Time info sits in free-text like "6:00 PM Doors | 7:00 PM Showtime".
        text_blob = card.get_text(" ", strip=True)

        shows.append(Show(
            title=title,
            sort_date=dt,
            venue="Skydeck",
            url=event_url,
            time=find_time(text_blob, "Showtime", label_before=False),
            doors=find_time(text_blob, "Doors", label_before=False),
        ))
    return shows


def scrape_drkmttr():
    """DRKMTTR renders /shows as a Webflow collection. Each event is a
    `.ec-col-item`. Title, start_date, and a webflow slug link are in
    fixed child divs. The Calendar and Grid tabs both render the same
    items; the global deduplicate() handles that."""
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
        try:
            dt = datetime.strptime(date_el.get_text(strip=True), "%B %d, %Y").date()
        except ValueError:
            continue
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

        parts = date_el.get_text(" ", strip=True).replace(",", "").split()
        if len(parts) < 3:
            continue
        try:
            dt = datetime.strptime(
                f"{parts[1]} {parts[2]} {current_year}", "%b %d %Y"
            ).date()
        except ValueError:
            continue

        time_el = el.select_one(".rhp-event__time-text--list")
        raw = time_el.get_text(" ", strip=True) if time_el else ""

        cta = el.select_one(".rhp-event-cta")
        sold_out = "sold-out" in (cta.get("class", []) if cta else [])

        shows.append(Show(
            title=title,
            sort_date=dt,
            venue="The End",
            url=event_url,
            time=find_time(raw, "Show"),
            doors=find_time(raw, "Doors"),
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
    r = requests.get(url, headers=DEFAULT_HEADERS, timeout=DEFAULT_TIMEOUT)
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
            time=format_local_time(dt_local),
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
        dt = infer_upcoming_date(month_el.get_text(strip=True), day_num)
        if not dt:
            continue

        # .eventColl-detail--doors holds the show time (confusingly
        # named); actual doors time sits inside --restrictions.
        show_el = item.select_one(".eventColl-detail--doors")
        show_time = normalize_time(show_el.get_text(" ", strip=True)) if show_el else None
        restr_el = item.select_one(".eventColl-detail--restrictions")
        doors = find_time(restr_el.get_text(" ", strip=True), "Doors") if restr_el else None

        shows.append(Show(
            title=title,
            sort_date=dt,
            venue="The Caverns",
            url=event_url,
            time=show_time,
            doors=doors,
            sold_out=item.get("data-event-status") == "soldout",
        ))
    return shows


def scrape_fogg_street():
    """Fogg Street is a Squarespace site. Each event on /calendar is a
    stack of Squarespace blocks (no outer wrapper per event), so we
    extract titles and dates as two parallel streams in document order
    and zip them."""
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
        dt = infer_upcoming_date(m.group(1)[:3].title(), int(m.group(2)))
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
    r = requests.get("https://rudysjazzroom.com/calendar", headers=BROWSER_HEADERS, timeout=DEFAULT_TIMEOUT)
    m = re.search(r"events:\s*(\[.+?\]),", r.text, re.S)
    if not m:
        print("  [Rudy's] events array not found")
        return []
    try:
        events = json.loads(m.group(1))
    except json.JSONDecodeError as e:
        print(f"  [Rudy's] failed to parse events JSON: {e}")
        return []

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
            time=format_local_time(dt),
        ))
    return shows


def scrape_skinny_dennis():
    """Skinny Dennis books exclusively through DICE. Their DICE venue
    page is a Next.js app that embeds all upcoming events in a
    __NEXT_DATA__ JSON blob, so we pull and parse that directly."""
    print("  Fetching Skinny Dennis...")
    url = "https://dice.fm/venue/skinny-dennis-nashville-2ww96"
    r = requests.get(url, headers=BROWSER_HEADERS, timeout=DEFAULT_TIMEOUT)
    m = re.search(r'<script id="__NEXT_DATA__"[^>]*>(.*?)</script>', r.text, re.S)
    if not m:
        print("  [Skinny Dennis] __NEXT_DATA__ not found")
        return []
    try:
        data = json.loads(m.group(1))
    except json.JSONDecodeError as e:
        print(f"  [Skinny Dennis] failed to parse __NEXT_DATA__ JSON: {e}")
        return []
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
                time=format_local_time(dt),
            ))
    return shows


def scrape_ticketmaster(api_key):
    return _scrape_tm(TICKETMASTER_VENUES, api_key)


def scrape_pinnacle():
    """The Pinnacle is AEG-managed, so its events are published as a
    public CDN-cached JSON feed populated by AEG's CMS. Same feed the
    venue's own website fetches. Richer than the TM API: includes door
    times and explicit support-act lists."""
    print("  Fetching The Pinnacle...")
    url = "https://aegwebprod.blob.core.windows.net/json/events/334/events.json"
    r = requests.get(url, headers=DEFAULT_HEADERS, timeout=DEFAULT_TIMEOUT)
    data = r.json()

    today = date.today()
    shows = []
    for ev in data.get("events", []):
        title_obj = ev.get("title") or {}
        title = (title_obj.get("headlinersText")
                 or title_obj.get("eventTitleText") or "").strip()
        if not title:
            continue

        iso = ev.get("eventDateTimeISO") or ev.get("eventDateTime")
        if not iso:
            continue
        try:
            dt = datetime.fromisoformat(iso)
        except ValueError:
            continue
        # Strip tz so .date() reflects the venue-local wall-clock date.
        dt_local = dt.replace(tzinfo=None)
        if dt_local.date() < today:
            continue

        # Supporting text often has a "with " prefix or comma-separated list.
        supports_raw = (title_obj.get("supportingText") or "").strip()
        supports_raw = re.sub(r"^(with|w/|featuring|feat\.?)\s+", "",
                              supports_raw, flags=re.I)
        supports = [s.strip() for s in re.split(r",|/| & |\s+and\s+",supports_raw)
                    if s.strip()]

        ticketing = ev.get("ticketing") or {}
        event_url = ticketing.get("eventUrl") or ticketing.get("url") or ""
        sold_out = ticketing.get("status", "").lower() in {"sold out", "soldout"}

        door_iso = ev.get("doorDateTime")
        doors = None
        if door_iso:
            try:
                doors = format_local_time(datetime.fromisoformat(door_iso))
            except ValueError:
                pass

        shows.append(Show(
            title=title,
            sort_date=dt_local.date(),
            venue="The Pinnacle",
            url=event_url,
            sold_out=sold_out,
            time=format_local_time(dt_local) if dt_local.hour else None,
            doors=doors,
            supports=supports,
        ))
    return shows


_CANNERY_KNOWN_ROOMS = {
    "Cannery Hall - Mainstage",
    "Cannery Hall - Row One Stage",
    "Cannery Hall - The Mil",
}


def _parse_cannery_card(card, today):
    """Pull a single Show out of a `.pk-eachevent` card. Shared between
    the initial /calendar fetch and the AJAX load-more pages."""
    title_el = card.select_one(".pk-headline")
    if not title_el:
        return None
    title = unescape(title_el.get_text(" ", strip=True))
    if not title:
        return None

    link_el = card.select_one("a.pk-title-link") or card.select_one("a.pk-link")
    event_url = link_el.get("href", "") if link_el else ""

    month_el = card.select_one(".pk-date")
    if not month_el:
        return None
    m = re.match(r"^([A-Za-z]+)\s+(\d{1,2})$", month_el.get_text(strip=True))
    if not m:
        return None
    dt = infer_upcoming_date(m.group(1)[:3].title(), int(m.group(2)))
    if not dt or dt < today:
        return None

    times_el = card.select_one(".pk-times")
    time_blob = times_el.get_text(" ", strip=True) if times_el else ""
    # "Doors 7:00pm, Start 8:00pm" — labels precede times.
    show_time = find_time(time_blob, "Start") or find_time(time_blob, "Show")
    doors = find_time(time_blob, "Doors")

    sub_el = card.select_one(".pksubtitle")
    sub_raw = sub_el.get_text(" ", strip=True) if sub_el else ""
    sub_raw = re.sub(r"^(with|w/|featuring|feat\.?)\s+", "", sub_raw, flags=re.I)
    supports = [s.strip() for s in re.split(r",|/| & |\s+and\s+", sub_raw) if s.strip()]

    # Cannery Hall has three rooms (Mainstage / Row One Stage / The Mil);
    # the venue card carries the room in `.pk-venue-name`. Surface that
    # so the rendered listing can distinguish concurrent bookings.
    venue_el = card.select_one(".pk-venue-name")
    venue = venue_el.get_text(" ", strip=True) if venue_el else "Cannery Hall"
    if venue not in _CANNERY_KNOWN_ROOMS and not venue.startswith("Cannery Hall"):
        venue = "Cannery Hall"

    card_classes = " ".join(card.get("class") or []).lower()
    link_text = link_el.get_text(" ", strip=True).lower() if link_el else ""
    sold_out = ("sold-out" in card_classes
                or "soldout" in card_classes
                or "sold out" in link_text)

    return Show(
        title=title,
        sort_date=dt,
        venue=venue,
        url=event_url,
        sold_out=sold_out,
        time=show_time,
        doors=doors,
        supports=supports,
    )


def scrape_cannery_hall():
    """Cannery Hall sells through AXS, so the TM API only catches the
    rare TM-cross-listed show. The venue's own /calendar page server-
    renders the first 30 events as `.pk-eachevent` cards (the AXS
    Events Infinite Scroll plugin), with subsequent pages loaded via
    POST to /wp-admin/admin-ajax.php using a per-session nonce embedded
    in page 1. We follow that paginated chain to capture the full ~9-
    month horizon, splitting the result by room (Mainstage / Row One
    Stage / The Mil)."""
    print("  Fetching Cannery Hall...")
    session = requests.Session()
    session.headers.update(BROWSER_HEADERS)

    r = session.get("https://canneryhall.com/calendar", timeout=DEFAULT_TIMEOUT)
    soup = BeautifulSoup(r.text, "html.parser")

    today = date.today()
    shows = []
    for card in soup.select(".pk-eachevent"):
        s = _parse_cannery_card(card, today)
        if s:
            shows.append(s)

    nonce_match = re.search(r'"nonce":"([a-f0-9]+)"', r.text)
    if not nonce_match:
        return shows  # Page 1 only — nonce missing, can't paginate.
    nonce = nonce_match.group(1)

    page = 2
    while page <= 20:  # hard ceiling so a runaway loop can't hang the run
        try:
            r2 = session.post(
                "https://canneryhall.com/wp-admin/admin-ajax.php",
                data={
                    "action": "load_more_axs_events",
                    "page": page,
                    "rows": 30,
                    "nonce": nonce,
                    "majorCat": "",
                },
                headers={
                    "X-Requested-With": "XMLHttpRequest",
                    "Referer": "https://canneryhall.com/calendar",
                },
                timeout=DEFAULT_TIMEOUT,
            )
            data = r2.json()
        except Exception as e:
            print(f"  [Cannery Hall] page {page} failed, keeping {len(shows)}: {e}")
            break
        if not (isinstance(data, dict) and data.get("success") and data.get("data")):
            break
        soup_p = BeautifulSoup(data["data"], "html.parser")
        new_cards = soup_p.select(".pk-eachevent")
        if not new_cards:
            break
        for card in new_cards:
            s = _parse_cannery_card(card, today)
            if s:
                shows.append(s)
        page += 1
    return shows


def scrape_ascend():
    """Ascend Amphitheater is also AXS-ticketed. Its /events page server-
    renders `.eventItem.entry` cards with full year+time in plain text.
    The first item on the page is a Vue.js template (has `v-html`/`{{ }}`
    placeholders) — skip it."""
    soup = _fetch_soup("Ascend Amphitheater", "https://www.ascendamphitheater.com/events", BROWSER_HEADERS)

    today = date.today()
    shows = []
    for card in soup.select(".eventItem.entry"):
        # Skip the Vue template row.
        if card.select_one("[v-html]") or "{{" in card.get_text():
            continue

        title_a = card.select_one(".title a")
        if not title_a:
            continue
        title = unescape(title_a.get_text(" ", strip=True))
        if not title:
            continue

        href = title_a.get("href", "")
        if href.startswith("/"):
            event_url = "https://www.ascendamphitheater.com" + href
        else:
            event_url = href

        # The href encodes the start date as /event/YYYY-MM-DD-… which
        # is the most reliable source — the visible .date can be a
        # range ("May 1 - 2, 2026") for multi-day stands.
        m = re.search(r"/event/(\d{4})-(\d{2})-(\d{2})-", event_url)
        if m:
            try:
                dt = date(int(m.group(1)), int(m.group(2)), int(m.group(3)))
            except ValueError:
                continue
        else:
            date_el = card.select_one(".date")
            if not date_el:
                continue
            date_text = date_el.get_text(" ", strip=True)
            dm = re.match(r"^([A-Za-z]+)\s+(\d{1,2}).*?(\d{4})", date_text)
            if not dm:
                continue
            try:
                dt = datetime.strptime(
                    f"{dm.group(1)[:3]} {dm.group(2)} {dm.group(3)}", "%b %d %Y"
                ).date()
            except ValueError:
                continue
        if dt < today:
            continue

        date_el = card.select_one(".date")
        date_text = date_el.get_text(" ", strip=True) if date_el else ""
        show_time = normalize_time(date_text.split("|", 1)[1]) if "|" in date_text else None

        tagline_el = card.select_one(".tagline")
        tagline = tagline_el.get_text(" ", strip=True) if tagline_el else ""
        tagline = re.sub(r"^(with|w/|featuring|feat\.?)\s+", "",
                         tagline, flags=re.I)
        supports = [s.strip() for s in re.split(r",|/| & |\s+and\s+",tagline) if s.strip()]

        btn = card.select_one(".tickets")
        btn_classes = " ".join(btn.get("class") or []).lower() if btn else ""
        sold_out = "soldout" in btn_classes or "sold-out" in btn_classes

        shows.append(Show(
            title=title,
            sort_date=dt,
            venue="Ascend Amphitheater",
            url=event_url,
            sold_out=sold_out,
            time=show_time,
            supports=supports,
        ))
    return shows


# ---------- main ----------

if __name__ == "__main__":
    TM_API_KEY = os.environ.get("TM_API_KEY", "")

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
        ("The Pinnacle", scrape_pinnacle),
        ("Cannery Hall", scrape_cannery_hall),
        ("Ascend Amphitheater", scrape_ascend),
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
    normalize_titles(shows)
    shows = deduplicate(shows, same_artist_pass=True)
    shows = filter_junk_and_sports(
        shows,
        junk_keywords=JUNK_KEYWORDS,
        sports_venues=SPORTS_VENUES,
        sports_keywords=SPORTS_KEYWORDS,
        non_music_re=NON_MUSIC_RE,
        non_music_exempt=NON_MUSIC_EXEMPT_ARTISTS,
    )

    suspects = find_duplicate_suspects(shows)
    if suspects:
        print(f"\n  [warn] {len(suspects)} same-(date,venue,time) group(s) survived dedupe — review:")
        for (d, v, t), rows in sorted(suspects):
            print(f"    {d.isoformat()}  {v} @ {t}")
            for r in rows:
                print(f"      - {r.title}")

    with open(SHOWS_JSON, "w") as f:
        json.dump([s.to_json_dict() for s in shows], f, indent=2)
    print(f"\nWrote {len(shows)} shows to {SHOWS_JSON}")
