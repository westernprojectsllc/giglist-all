"""Minnesota scraper. Pulls events from ~20 venues in parallel, dedupes,
filters out junk/sports, enriches First Avenue shows with doors + time,
and writes shows.json for render.py to consume."""

import json
import os
import re
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date, datetime, timedelta
from functools import partial
from html import unescape
from pathlib import Path
from zoneinfo import ZoneInfo

import requests
from bs4 import BeautifulSoup
from dateutil.relativedelta import relativedelta
from dotenv import load_dotenv

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from giglist.http import (
    BROWSER_HEADERS, DEFAULT_HEADERS, DEFAULT_TIMEOUT, USER_AGENT,
)
from giglist.models import Show
from giglist.scrape_utils import (
    CENTRAL_TZ, WS_RE, deduplicate, filter_junk_and_sports,
    find_duplicate_suspects, format_local_time, normalize_titles,
    parse_loose_time, scrape_dice, scrape_ticketmaster as _scrape_tm,
    scrape_tribe_events,
)

from config import (
    JUNK_KEYWORDS, MONTHS_AHEAD, REGION_DIR, SPORTS_KEYWORDS,
    SPORTS_VENUES, TICKETMASTER_VENUES,
)

load_dotenv()


SHOWS_JSON = REGION_DIR / "shows.json"
BASE_URL = "https://first-avenue.com/shows"


# ---------- First Avenue ----------

def scrape_month(start_date):
    date_str = start_date.strftime("%Y%m%d")
    url = f"{BASE_URL}?post_type=event&start_date={date_str}"
    response = requests.get(url, headers=DEFAULT_HEADERS, timeout=DEFAULT_TIMEOUT)
    soup = BeautifulSoup(response.text, "html.parser")

    shows = []
    # First Ave's monthly listing can spill into the next calendar year
    # (e.g. a December page showing early January shows). Anchor on the
    # requested month and bump the year for any event whose month is
    # earlier than start_date's month.
    for event in soup.select(".show_list_item"):
        title_tag = event.select_one("h4 a")
        month = event.select_one(".month")
        day = event.select_one(".day")
        venue = event.select_one(".venue_name")

        if not (title_tag and month and day):
            continue

        try:
            parsed = datetime.strptime(
                f"{month.get_text(strip=True)} {day.get_text(strip=True)} {start_date.year}",
                "%b %d %Y",
            ).date()
        except ValueError:
            continue
        if parsed.month < start_date.month:
            parsed = parsed.replace(year=start_date.year + 1)

        supports = []
        h5 = event.select_one("h5")
        if h5:
            support_text = h5.get_text(separator=" ", strip=True)
            for prefix in ("with ", "With ", "w/ "):
                if support_text.startswith(prefix):
                    support_text = support_text[len(prefix):]
                    break
            if support_text:
                supports = [
                    s.strip() for s in support_text.replace(" and ", ", ").split(",")
                    if s.strip()
                ]

        # Sold-out badge: First Ave reuses .badge-sold-out for cancelled
        # shows, so detect by literal span text instead of class.
        sold_out = False
        for badge in event.select(".status.badge span, .badge span"):
            if badge.get_text(strip=True).lower() == "sold out":
                sold_out = True
                break

        venue_name = venue.get_text(strip=True) if venue else "First Avenue"
        # Normalize venue names so the deduper can collapse cross-promoted
        # shows with the dedicated scrapers.
        venue_name = {
            "Armory": "The Armory",
            "The Cedar Cultural Center": "Cedar Cultural Center",
            "icehouse MPLS": "Ice House",
        }.get(venue_name, venue_name)

        shows.append(Show(
            title=title_tag.get_text(separator=" ", strip=True),
            sort_date=parsed,
            venue=venue_name,
            url=title_tag["href"],
            sold_out=sold_out,
            supports=supports,
        ))

    return shows


def scrape_first_avenue():
    all_shows = []
    seen_urls = set()
    start_month = datetime.today().replace(day=1)
    months = [start_month + relativedelta(months=i) for i in range(MONTHS_AHEAD)]

    print(f"Scraping First Avenue ({MONTHS_AHEAD} months in parallel)...")

    def fetch(month):
        try:
            return scrape_month(month)
        except Exception as e:
            print(f"  Error scraping {month.strftime('%B %Y')}: {e}")
            return []

    with ThreadPoolExecutor(max_workers=MONTHS_AHEAD) as executor:
        for shows in executor.map(fetch, months):
            for show in shows:
                if show.url not in seen_urls:
                    seen_urls.add(show.url)
                    all_shows.append(show)

    return all_shows


FIRST_AVE_VENUES = {
    "First Avenue", "7th St Entry", "Palace Theatre",
    "The Fitzgerald Theater", "Fine Line", "Turf Club",
    "Amsterdam Bar & Hall", "The Armory",
}


def _enrich_one(session, show):
    """Fetch a single First Ave show page and update the show in place
    with doors and show time. Retries once on transient failure."""
    url = show.url if show.url.startswith("http") else "https://first-avenue.com" + show.url

    for attempt in range(2):
        try:
            resp = session.get(url, headers=DEFAULT_HEADERS, timeout=DEFAULT_TIMEOUT)
            soup = BeautifulSoup(resp.text, "html.parser")
            break
        except Exception:
            if attempt == 1:
                return

    for h6 in soup.find_all("h6"):
        label = h6.get_text(strip=True).lower()
        h2 = h6.find_next("h2")
        if not h2:
            continue
        value = h2.get_text(strip=True)
        if "doors" in label:
            show.doors = value.lower()
        elif "show" in label:
            show.time = value.lower()


def _load_enrichment_cache(path):
    """Load URL -> cached doors/time from a prior shows.json so distant
    future shows don't re-fetch every day."""
    if not path.exists():
        return {}
    try:
        with open(path) as f:
            raw = json.load(f)
    except (json.JSONDecodeError, OSError):
        return {}
    cache = {}
    for d in raw:
        url = d.get("url")
        if url and (d.get("time") or d.get("doors")):
            cache[url] = {"doors": d.get("doors"), "time": d.get("time")}
    return cache


def enrich_show_details(shows, cache=None, max_workers=16, fetch_within_days=21):
    """Scrape individual First Avenue show pages in parallel for doors
    and show time. Shows more than ``fetch_within_days`` out that already
    have cached times from a prior run are skipped — times rarely change
    that far in advance and FA's show pages are the bulk of the daily
    scrape."""
    today = date.today()
    cache = cache or {}
    cutoff = today + timedelta(days=fetch_within_days)

    candidates = [
        s for s in shows
        if s.sort_date >= today and s.venue in FIRST_AVE_VENUES and s.url
    ]

    to_enrich = []
    cache_hits = 0
    for s in candidates:
        cached = cache.get(s.url)
        if cached:
            s.doors = s.doors or cached.get("doors")
            s.time = s.time or cached.get("time")
        if cached and s.sort_date > cutoff and (cached.get("time") or cached.get("doors")):
            cache_hits += 1
            continue
        to_enrich.append(s)

    print(f"\nEnriching {len(to_enrich)} FA shows (cache hits: {cache_hits})...")
    if not to_enrich:
        return shows

    # One pooled HTTPS session — every enrichment request hits
    # first-avenue.com, so reusing the TCP/TLS connection avoids
    # hundreds of fresh handshakes.
    session = requests.Session()
    adapter = requests.adapters.HTTPAdapter(
        pool_connections=max_workers, pool_maxsize=max_workers,
    )
    session.mount("https://", adapter)
    fetch = partial(_enrich_one, session)

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        for i, _ in enumerate(executor.map(fetch, to_enrich), start=1):
            if i % 20 == 0:
                print(f"  Enriched {i}/{len(to_enrich)}...")
    session.close()

    print(f"  Done enriching {len(to_enrich)} shows")
    return shows


_FA_PRESENTS_RE = re.compile(r"^first ave(nue)? presents ")


# ---------- venue scrapers ----------

def scrape_dakota():
    return scrape_tribe_events(
        "https://www.dakotacooks.com/wp-json/tribe/events/v1/events",
        "Dakota Jazz Club",
    )


def scrape_cedar():
    url = "https://www.thecedar.org/events"
    print("  Fetching Cedar Cultural Center...")
    try:
        response = requests.get(url, headers=DEFAULT_HEADERS, timeout=DEFAULT_TIMEOUT)
    except Exception as e:
        print(f"  Error: {e}")
        return []
    soup = BeautifulSoup(response.text, "html.parser")

    shows = []
    seen = set()

    for ev in soup.select("article.eventlist-event--upcoming"):
        a_tag = ev.select_one("a.eventlist-title-link")
        date_tag = ev.select_one("time.event-date")
        if not a_tag or not date_tag:
            continue

        href = a_tag.get("href", "")
        if href in seen:
            continue
        seen.add(href)

        try:
            sort_date = datetime.strptime(
                date_tag.get("datetime", ""), "%Y-%m-%d"
            ).date()
        except ValueError:
            continue

        show_time = None
        start_tag = ev.select_one(
            "time.event-time-localized-start, time.event-time-localized"
        )
        if start_tag:
            try:
                dt = datetime.strptime(start_tag.get_text(strip=True), "%I:%M %p")
                show_time = format_local_time(dt)
            except ValueError:
                pass

        title = a_tag.get_text(separator=" ", strip=True)
        # Cedar marks sold-out shows with a ❗SOLD OUT❗ prefix in the title.
        sold_out = bool(re.search(r"sold\s*out", title, re.I))
        if sold_out:
            title = re.sub(r"❗?\s*sold\s*out\s*❗?", "", title, flags=re.I).strip()

        shows.append(Show(
            title=title,
            sort_date=sort_date,
            venue="Cedar Cultural Center",
            url="https://www.thecedar.org" + href,
            sold_out=sold_out,
            time=show_time,
        ))

    return shows


def scrape_orchestra():
    today = date.today()

    def fetch(mos):
        url = f"https://www.minnesotaorchestra.org/api/event-feed/{mos}"
        try:
            response = requests.get(url, headers=DEFAULT_HEADERS, timeout=DEFAULT_TIMEOUT)
        except Exception:
            return []
        if response.status_code != 200:
            return []
        try:
            return response.json()
        except ValueError:
            return []

    seen_ids = set()
    shows = []
    with ThreadPoolExecutor(max_workers=MONTHS_AHEAD) as pool:
        for events in pool.map(fetch, range(1, MONTHS_AHEAD + 1)):
            for event in events:
                event_id = event.get("id")
                if event_id in seen_ids:
                    continue

                perf_date = event.get("perf_date", "")
                if not perf_date:
                    continue
                try:
                    dt = datetime.fromisoformat(perf_date)
                    sort_date = dt.date()
                except ValueError:
                    continue
                if sort_date < today:
                    continue

                seen_ids.add(event_id)
                event_url = event.get("event_page_url", "")
                if event_url and not event_url.startswith("http"):
                    event_url = "https://www.minnesotaorchestra.org" + event_url

                show_time = format_local_time(dt) if (dt.hour or dt.minute) else None

                shows.append(Show(
                    title=event.get("title", "Unknown"),
                    sort_date=sort_date,
                    venue="Orchestra Hall",
                    url=event_url,
                    time=show_time,
                ))

    return shows


def scrape_ticketmaster(api_key):
    return _scrape_tm(TICKETMASTER_VENUES, api_key)


def scrape_myth():
    url = "https://mythlive.com/"
    print("  Fetching Myth Live...")
    try:
        response = requests.get(url, headers=DEFAULT_HEADERS, timeout=DEFAULT_TIMEOUT)
    except Exception as e:
        print(f"  Error: {e}")
        return []
    soup = BeautifulSoup(response.text, "html.parser")

    shows = []
    current_year = date.today().year

    for event in soup.select(".eventWrapper"):
        link = event.select_one("a.url")
        date_div = event.select_one(".eventMonth")
        if not (link and date_div):
            continue

        date_text = date_div.get_text(strip=True)
        try:
            sort_date = datetime.strptime(f"{date_text} {current_year}", "%a, %b %d %Y").date()
            if sort_date < date.today():
                sort_date = sort_date.replace(year=current_year + 1)
        except ValueError:
            continue

        # RHP Events tags its CTA element with a status class: on-sale,
        # sold-out, off-sale, Canceled, coming-soon, etc.
        cta = event.select_one(".rhp-event-cta")
        sold_out = bool(cta and "sold-out" in cta.get("class", []))

        # Doors / show times live in .rhp-event__time--list e.g.
        # "Doors: 8:30 pm // Show: 9:30 pm"
        doors = show_time = None
        time_el = event.select_one(".rhp-event__time--list")
        if time_el:
            ttext = time_el.get_text(" ", strip=True)
            dm = re.search(r"doors?\s*:?\s*(\d{1,2}(?::\d{2})?\s*[ap]\.?m\.?)", ttext, re.I)
            sm = re.search(r"show\s*:?\s*(\d{1,2}(?::\d{2})?\s*[ap]\.?m\.?)", ttext, re.I)
            doors = parse_loose_time(dm.group(1)) if dm else None
            show_time = parse_loose_time(sm.group(1)) if sm else None
        if doors == show_time:
            doors = None

        shows.append(Show(
            title=link.get("title", "Unknown"),
            sort_date=sort_date,
            venue="Myth Live",
            url=link["href"],
            sold_out=sold_out,
            time=show_time,
            doors=doors,
        ))

    return shows


def scrape_white_squirrel():
    return scrape_tribe_events(
        "https://whitesquirrelbar.com/wp-json/tribe/events/v1/events",
        "White Squirrel",
    )


def scrape_icehouse():
    url = "https://icehouse.turntabletickets.com/"
    print("  Fetching Ice House...")
    try:
        response = requests.get(url, headers=DEFAULT_HEADERS, timeout=DEFAULT_TIMEOUT)
    except Exception as e:
        print(f"  Error: {e}")
        return []

    # Ice House embeds data as a Python dict literal containing JSON values.
    # Extract just the pagination JSON object, which contains "performances".
    match = re.search(r"'pagination':\s*", response.text)
    if not match:
        print("  Could not find pagination data in Ice House page")
        return []

    try:
        decoder = json.JSONDecoder()
        data, _ = decoder.raw_decode(response.text[match.end():])
    except (json.JSONDecodeError, ValueError):
        print("  Failed to parse Ice House JSON")
        return []

    shows = []
    today = date.today()

    for perf in data.get("performances", []):
        show = perf.get("show", {})
        title = show.get("name", "Unknown")
        show_id = show.get("id")

        dt_str = perf.get("datetime", "")
        if not dt_str:
            continue
        try:
            dt_utc = datetime.strptime(dt_str, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=ZoneInfo("UTC"))
            dt_local = dt_utc.astimezone(CENTRAL_TZ)
            sort_date = dt_local.date()
        except ValueError:
            continue

        if sort_date < today:
            continue

        show_url = (
            f"https://icehouse.turntabletickets.com/shows/{show_id}/"
            if show_id else url
        )

        shows.append(Show(
            title=title,
            sort_date=sort_date,
            venue="Ice House",
            url=show_url,
            sold_out=bool(perf.get("sold")),
            time=format_local_time(dt_local),
        ))

    return shows


_331_TIME_RE = re.compile(
    r"^\s*\d+(?::\d+)?\s*(?:[-–]\s*\d+(?::\d+)?)?\s*(?:am|pm)\s*$",
    re.I,
)
_331_TIME_PARSE = re.compile(r"^(\d+(?::\d+)?)(?:[-–]\d+(?::\d+)?)?(am|pm)$")
_331_BR_RE = re.compile(r"<br\s*/?>")


def scrape_331():
    """331 Club's homepage contains a full calendar of upcoming shows in
    .event divs with month/date/day spans. The /event/ subpage only
    renders one upcoming show server-side."""
    url = "https://331club.com/"
    print("  Fetching 331 Club...")
    try:
        response = requests.get(url, headers=DEFAULT_HEADERS, timeout=DEFAULT_TIMEOUT)
    except Exception as e:
        print(f"  Error: {e}")
        return []

    soup = BeautifulSoup(response.text, "html.parser")
    shows = []
    today = date.today()

    months = {
        "Jan": 1, "Feb": 2, "Mar": 3, "Apr": 4, "May": 5, "Jun": 6,
        "Jul": 7, "Aug": 8, "Sep": 9, "Oct": 10, "Nov": 11, "Dec": 12,
    }

    for event_div in soup.select("div.event"):
        date_div = event_div.find("div", class_="event-date")
        if not date_div:
            continue
        month_el = date_div.find("span", class_="month")
        day_el = date_div.find("span", class_="date")
        if not month_el or not day_el:
            continue
        try:
            month = months[month_el.get_text(strip=True)[:3]]
            day = int(day_el.get_text(strip=True))
        except (KeyError, ValueError):
            continue

        sort_date = date(today.year, month, day)
        if sort_date < today - timedelta(days=14):
            sort_date = sort_date.replace(year=today.year + 1)

        content = event_div.find("div", class_="event-content")
        if not content:
            continue

        for p in content.find_all("p"):
            chunks = _331_BR_RE.split(p.decode_contents())
            lines = []
            for chunk in chunks:
                sub = BeautifulSoup(chunk, "html.parser")
                text = sub.get_text(" ", strip=True).replace("\xa0", " ").strip()
                if not text:
                    continue
                a = sub.find("a")
                href = a.get("href") if a and a.get("href") else None
                lines.append((text, href))

            if not lines:
                continue

            show_time = None
            title_lines = lines[:]
            for i in range(len(lines) - 1, -1, -1):
                if _331_TIME_RE.match(lines[i][0]):
                    raw_time = lines[i][0].lower().replace(" ", "")
                    m = _331_TIME_PARSE.match(raw_time)
                    if m:
                        show_time = m.group(1) + m.group(2)
                    title_lines = lines[:i]
                    break

            if not title_lines:
                continue

            cleaned = [
                (text, href) for text, href in title_lines
                if text.lower() not in ("free", "no cover", "tba", "tbd")
            ]
            if not cleaned:
                continue

            title, title_href = cleaned[0]
            supports = [t for t, _ in cleaned[1:]]

            href = title_href or next((h for _, h in cleaned if h), url)

            shows.append(Show(
                title=title,
                sort_date=sort_date,
                venue="331 Club",
                url=href,
                time=show_time,
                supports=supports,
            ))

    return shows


def scrape_skyway():
    """Skyway Theatre's events page embeds a FullCalendar config with
    all events as JSON inside an inline <script>."""
    url = "https://skywaytheatre.com/events/"
    print("  Fetching Skyway Theatre...")
    try:
        response = requests.get(url, headers=DEFAULT_HEADERS, timeout=DEFAULT_TIMEOUT)
    except Exception as e:
        print(f"  Error: {e}")
        return []

    match = re.search(r"eventSources:\s*", response.text)
    if not match:
        print("  Could not find eventSources in Skyway page")
        return []

    try:
        decoder = json.JSONDecoder()
        sources, _ = decoder.raw_decode(response.text[match.end():])
    except (json.JSONDecodeError, ValueError) as e:
        print(f"  Failed to parse Skyway JSON: {e}")
        return []

    events = []
    for src in sources:
        if isinstance(src, list):
            events.extend(src)
        elif isinstance(src, dict):
            events.append(src)

    shows = []
    today = date.today()
    seen = set()

    for ev in events:
        title = ev.get("title", "").strip()
        start = ev.get("start", "")
        if not title or not start:
            continue

        try:
            dt = datetime.fromisoformat(start)
            sort_date = dt.date()
        except ValueError:
            continue
        if sort_date < today:
            continue

        details = (ev.get("details", "") or "").lower()
        venue_name = "The Loft at Skyway Theatre" if "loft" in details else "Skyway Theatre"
        sold_out = "sold out" in details

        title = unescape(title)

        key = (title, sort_date, venue_name)
        if key in seen:
            continue
        seen.add(key)

        shows.append(Show(
            title=title,
            sort_date=sort_date,
            venue=venue_name,
            url=ev.get("permalink", url),
            sold_out=sold_out,
            time=format_local_time(dt),
        ))

    return shows


_PILLLAR_DATE_RE = re.compile(r"(\d{1,2})/(\d{1,2})/(\d{4})")
_PILLLAR_TIME_RE = re.compile(r"music[^0-9]*(\d{1,2})(?::(\d{2}))?\s*(am|pm)?", re.I)
_PILLLAR_DOORS_RE = re.compile(r"doors[^0-9]*(\d{1,2})(?::(\d{2}))?\s*(am|pm)?", re.I)


def _format_pilllar_time(hour, minute, ampm):
    """Pilllar listings are evening shows, so default to pm when am/pm
    is missing."""
    hour = int(hour)
    minute = int(minute) if minute else 0
    ampm = ampm.lower() if ampm else "pm"
    if minute:
        return f"{hour}:{minute:02d}{ampm}"
    return f"{hour}{ampm}"


def scrape_pilllar():
    """Pilllar Forum sells tickets through a Shopify products.json endpoint.
    Each product is one show; the title contains the artist + date and the
    body_html has structured Date/Time/Lineup fields."""
    url = "https://www.pilllar.com/collections/tickets/products.json?limit=250"
    print("  Fetching Pilllar Forum...")
    try:
        response = requests.get(url, headers=DEFAULT_HEADERS, timeout=DEFAULT_TIMEOUT)
        data = response.json()
    except Exception as e:
        print(f"  Error: {e}")
        return []

    today = date.today()
    shows = []

    for product in data.get("products", []):
        title = product.get("title", "").strip()
        handle = product.get("handle", "")
        body = product.get("body_html", "") or ""

        clean_title = re.sub(r"^\s*music\s*:\s*", "", title, flags=re.I)
        m = _PILLLAR_DATE_RE.search(clean_title)
        if not m:
            continue
        try:
            sort_date = date(int(m.group(3)), int(m.group(1)), int(m.group(2)))
        except ValueError:
            continue
        if sort_date < today:
            continue

        artist = _PILLLAR_DATE_RE.sub("", clean_title).strip(" -–—")

        body_text = BeautifulSoup(body, "html.parser").get_text(" ", strip=True)

        show_time = doors = None
        tm = _PILLLAR_TIME_RE.search(body_text)
        if tm:
            show_time = _format_pilllar_time(tm.group(1), tm.group(2), tm.group(3))
        dm = _PILLLAR_DOORS_RE.search(body_text)
        if dm:
            doors = _format_pilllar_time(dm.group(1), dm.group(2), dm.group(3))

        supports = []
        lineup_match = re.search(
            r"lineup\s*:\s*(.+?)(?=\s+(?:time|date|cost|doors|all\s+ages|tickets|please)\s*:|$)",
            body_text,
            re.I,
        )
        if lineup_match:
            acts = []
            for a in lineup_match.group(1).split(","):
                a = a.strip()
                if a.lower().startswith("and "):
                    a = a[4:].strip()
                if a:
                    acts.append(a)
            supports = [a for a in acts if a.lower() != artist.lower()]

        shows.append(Show(
            title=artist,
            sort_date=sort_date,
            venue="Pilllar Forum",
            url=f"https://www.pilllar.com/products/{handle}",
            sold_out=not product.get("variants", [{}])[0].get("available", True),
            time=show_time,
            supports=supports,
            doors=doors,
        ))

    return shows


_UNDERGROUND_EMBED_RE = re.compile(r"promoter\.skeletix\.com/events/(\d+)")
_UNDERGROUND_DATE_RE = re.compile(
    r"(?:Mon|Tue|Wed|Thu|Fri|Sat|Sun),\s+([A-Z][a-z]{2})\s+(\d{1,2}),\s+(\d{4})"
)


def scrape_underground():
    """Underground Music Venue's site embeds Skeletix iframes for each show.
    We pull the embed URLs from the events page, then fetch each embed for
    the title and date."""
    url = "https://www.undergroundmusicvenue.com/events"
    print("  Fetching Underground Music Venue...")
    try:
        response = requests.get(url, headers=DEFAULT_HEADERS, timeout=DEFAULT_TIMEOUT)
    except Exception as e:
        print(f"  Error: {e}")
        return []

    event_ids = sorted(set(_UNDERGROUND_EMBED_RE.findall(response.text)))
    if not event_ids:
        return []

    today = date.today()

    def fetch_event(event_id):
        embed_url = f"https://promoter.skeletix.com/events/{event_id}/embed"
        try:
            r = requests.get(embed_url, headers=DEFAULT_HEADERS, timeout=DEFAULT_TIMEOUT)
            soup = BeautifulSoup(r.text, "html.parser")
        except Exception:
            return None

        title_tag = soup.select_one(".card-title")
        desc_tag = soup.select_one(".card-desc")
        link_tag = soup.select_one("a.card")
        if not title_tag or not desc_tag:
            return None

        title = title_tag.get_text(strip=True)
        desc = desc_tag.get_text(" ", strip=True)
        m = _UNDERGROUND_DATE_RE.search(desc)
        if not m:
            return None
        try:
            sort_date = datetime.strptime(
                f"{m.group(1)} {m.group(2)} {m.group(3)}", "%b %d %Y"
            ).date()
        except ValueError:
            return None
        if sort_date < today:
            return None

        href = link_tag["href"] if link_tag and link_tag.get("href") else embed_url
        return Show(
            title=title,
            sort_date=sort_date,
            venue="Underground Music Venue",
            url=href,
        )

    shows = []
    with ThreadPoolExecutor(max_workers=min(8, len(event_ids))) as executor:
        for show in executor.map(fetch_event, event_ids):
            if show is not None:
                shows.append(show)

    return shows


def scrape_zhora_darling():
    return scrape_dice(
        "Zhora Darling",
        dice_venues=["Zhora Darling"],
        dice_promoters=["Bonnie McMurray LLC dba Zhora Darling"],
    )


def scrape_cloudland():
    return scrape_dice("Cloudland Theater", dice_venues=["Cloudland Theater"])


def scrape_parkway():
    return scrape_dice(
        "The Parkway Theater",
        dice_venues=["The Parkway Theater"],
        exclude_tags={"culture:film"},
    )


def scrape_berlin():
    """Berlin's calendar is a Squarespace event collection rendered as
    eventlist articles with semantic <time> tags."""
    url = "https://www.berlinmpls.com/calendar"
    print("  Fetching Berlin...")
    try:
        response = requests.get(url, headers=DEFAULT_HEADERS, timeout=DEFAULT_TIMEOUT)
        soup = BeautifulSoup(response.text, "html.parser")
    except Exception as e:
        print(f"  Error: {e}")
        return []

    today = date.today()
    shows = []
    seen = set()

    for ev in soup.select("article.eventlist-event--upcoming"):
        title_a = ev.select_one(".eventlist-title a, h1 a, h2 a, h3 a")
        date_tag = ev.select_one("time.event-date")
        if not title_a or not date_tag:
            continue

        try:
            sort_date = datetime.strptime(date_tag.get("datetime", ""), "%Y-%m-%d").date()
        except ValueError:
            continue
        if sort_date < today:
            continue

        title = title_a.get_text(strip=True)
        href = title_a.get("href", "")
        if href and not href.startswith("http"):
            href = "https://www.berlinmpls.com" + href

        sold_out = bool(re.search(r"sold\s*out", title, re.I))
        if sold_out:
            title = re.sub(r"❗?\s*sold\s*out\s*❗?", "", title, flags=re.I).strip()

        show_time = None
        start_tag = ev.select_one(
            "time.event-time-localized-start, time.event-time-localized"
        )
        if start_tag:
            try:
                dt = datetime.strptime(start_tag.get_text(strip=True), "%I:%M %p")
                show_time = format_local_time(dt)
            except ValueError:
                pass

        key = (title, sort_date)
        if key in seen:
            continue
        seen.add(key)

        shows.append(Show(
            title=title,
            sort_date=sort_date,
            venue="Berlin",
            url=href,
            sold_out=sold_out,
            time=show_time,
        ))

    return shows


_VFW_DOORS_SHOW_RE = re.compile(
    r"doors?\s*:\s*([0-9: ]+(?:am|pm))\s*[-–]\s*show\s*:\s*([0-9: ]+(?:am|pm))",
    re.I,
)


def scrape_uptown_vfw():
    """Uptown VFW lists events on an Opendate.io shows page that's rendered
    server-side. Each event is a .confirm-card div."""
    url = "https://app.opendate.io/c/uptown-vfw-681"
    print("  Fetching Uptown VFW...")
    try:
        response = requests.get(url, headers=DEFAULT_HEADERS, timeout=DEFAULT_TIMEOUT)
        soup = BeautifulSoup(response.text, "html.parser")
    except Exception as e:
        print(f"  Error: {e}")
        return []

    today = date.today()
    shows = []

    for card in soup.select("div.confirm-card"):
        link = card.select_one("a.stretched-link")
        if not link:
            continue
        title = link.get_text(strip=True)
        href = link.get("href", "")

        paragraphs = card.find_all("p")
        supports = []
        sort_date = None
        show_time = doors = None

        for p in paragraphs:
            text = p.get_text(" ", strip=True)
            if not text:
                continue
            tl = text.lower()

            if tl.startswith("with "):
                acts = [a.strip() for a in re.split(r",\s*", text[5:].strip()) if a.strip()]
                cleaned = []
                for a in acts:
                    if a.lower().startswith("and "):
                        a = a[4:].strip()
                    if a:
                        cleaned.append(a)
                supports = cleaned
                continue

            if not sort_date:
                try:
                    sort_date = datetime.strptime(text, "%B %d, %Y").date()
                    continue
                except ValueError:
                    pass

            m = _VFW_DOORS_SHOW_RE.search(text)
            if m:
                for raw, assign in (
                    (m.group(1).replace(" ", "").lower(), "doors"),
                    (m.group(2).replace(" ", "").lower(), "show"),
                ):
                    try:
                        formatted = format_local_time(datetime.strptime(raw, "%I:%M%p"))
                    except ValueError:
                        formatted = None
                    if assign == "doors":
                        doors = formatted
                    else:
                        show_time = formatted

        if not sort_date or sort_date < today:
            continue

        if doors == show_time:
            doors = None

        shows.append(Show(
            title=title,
            sort_date=sort_date,
            venue="Uptown VFW",
            url=href,
            time=show_time,
            supports=supports,
            doors=doors,
        ))

    return shows


_ASTER_DATE_PREFIX_RE = re.compile(r"^\s*\d{1,2}/\d{1,2}\s*[-–]\s*")
_ASTER_WEEKDAYS = [
    "monday", "tuesday", "wednesday", "thursday",
    "friday", "saturday", "sunday",
]


def scrape_aster_cafe():
    """Aster Cafe books live music via Toast Tables. The public booking API
    returns 'experiences' (recurring or one-off events) with a list of
    active dates and per-weekday shift hours."""
    print("  Fetching Aster Cafe...")
    restaurant_guid = "e8feb07f-35e7-4478-8808-323010818c1f"
    api_url = "https://ws.toasttab.com/booking/v1/public/experiences"
    today = date.today()
    start_param = today.strftime("%Y-%m-%dT00:00:00+00:00")
    headers = {
        **DEFAULT_HEADERS,
        "Accept": "application/json",
        "Toast-Restaurant-External-ID": restaurant_guid,
    }
    try:
        resp = requests.get(
            api_url, headers=headers,
            params={"startDate": start_param},
            timeout=DEFAULT_TIMEOUT,
        )
        data = resp.json()
    except Exception as e:
        print(f"  Error: {e}")
        return []

    cutoff = today + relativedelta(months=MONTHS_AHEAD)
    shows = []

    for exp in data.get("results", []):
        name = exp.get("name", "").strip()
        if not name:
            continue
        title = _ASTER_DATE_PREFIX_RE.sub("", name).strip()

        slug = exp.get("slug")
        url = (
            f"https://tables.toasttab.com/aster-cafe/experiences/{slug}"
            if slug else "https://tables.toasttab.com/aster-cafe/experiences"
        )

        shifts = exp.get("shifts") or [{}]
        hours = (shifts[0] or {}).get("hours", {}) or {}

        for d_str in exp.get("datesActive", []) or []:
            try:
                sort_date = datetime.strptime(d_str, "%Y-%m-%d").date()
            except ValueError:
                continue
            if sort_date < today or sort_date > cutoff:
                continue

            day_hours = hours.get(_ASTER_WEEKDAYS[sort_date.weekday()]) or {}
            show_time = None
            start = day_hours.get("start") if day_hours.get("enabled") else None
            if start:
                try:
                    h, m = start.split(":", 2)[:2]
                    show_time = format_local_time(datetime(2000, 1, 1, int(h), int(m)))
                except (ValueError, TypeError):
                    pass

            shows.append(Show(
                title=title,
                sort_date=sort_date,
                venue="Aster Cafe",
                url=url,
                time=show_time,
            ))

    return shows


# ---------- main ----------

if __name__ == "__main__":
    TM_API_KEY = os.environ.get("TM_API_KEY", "")

    scrapers = [
        ("First Avenue (all venues)", scrape_first_avenue),
        ("Dakota Jazz Club", scrape_dakota),
        ("Cedar Cultural Center", scrape_cedar),
        ("Orchestra Hall", scrape_orchestra),
        ("Ticketmaster venues", lambda: scrape_ticketmaster(TM_API_KEY)),
        ("Myth Live", scrape_myth),
        ("White Squirrel", scrape_white_squirrel),
        ("Ice House", scrape_icehouse),
        ("331 Club", scrape_331),
        ("Skyway Theatre", scrape_skyway),
        ("Pilllar Forum", scrape_pilllar),
        ("Underground Music Venue", scrape_underground),
        ("Zhora Darling", scrape_zhora_darling),
        ("Cloudland Theater", scrape_cloudland),
        ("The Parkway Theater", scrape_parkway),
        ("Berlin", scrape_berlin),
        ("Uptown VFW", scrape_uptown_vfw),
        ("Aster Cafe", scrape_aster_cafe),
    ]

    # Load prior shows.json to skip re-enriching distant FA shows.
    enrichment_cache = _load_enrichment_cache(SHOWS_JSON)

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

    shows.sort(key=lambda x: (x.sort_date, x.venue, x.title))
    normalize_titles(shows)
    shows = deduplicate(shows, prefix_re=_FA_PRESENTS_RE)
    shows = filter_junk_and_sports(
        shows,
        junk_keywords=JUNK_KEYWORDS,
        sports_venues=SPORTS_VENUES,
        sports_keywords=SPORTS_KEYWORDS,
    )
    shows = enrich_show_details(shows, cache=enrichment_cache)

    suspects = find_duplicate_suspects(shows)
    if suspects:
        print(f"\n  [warn] {len(suspects)} same-(date,venue,time) group(s) survived dedupe:")
        for (d, v, t), rows in sorted(suspects):
            print(f"    {d.isoformat()}  {v} @ {t}")
            for r in rows:
                print(f"      - {r.title}")

    with open(SHOWS_JSON, "w") as f:
        json.dump([s.to_json_dict() for s in shows], f, indent=2)
    print(f"\nWrote {len(shows)} shows to {SHOWS_JSON}")
