"""Region-agnostic static-site renderer.

Takes a RegionConfig + a list of Show, writes index.html / list.html /
week-*.html / sitemap.xml / style CSS into the region's
output directory.

Layout is the "Tight Ledger" design — see DESIGN.md at the repo root and
the reference mockup in mockups/giglist-bw-mockup.html. The region index
is one continuous ledger of every upcoming week (anchored per week for
deep links); week-*.html pages carry the same ledger one week at a time.
"""

import json
import shutil
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from html import escape
from pathlib import Path
from typing import Dict, List, Optional
from zoneinfo import ZoneInfo

from dateutil.relativedelta import relativedelta

from .models import Show


ASSETS_DIR = Path(__file__).parent / "assets"

# Both MN and TN sit in US Central, and the daily scrape runs in UTC on
# GitHub Actions — anchoring "today" to America/Chicago keeps the rendered
# current-day view correct regardless of when the runner fires.
CENTRAL_TZ = ZoneInfo("America/Chicago")


def _today_central() -> date:
    return datetime.now(CENTRAL_TZ).date()


@dataclass
class RegionConfig:
    region_key: str                    # URL path segment, e.g. "mn"
    display_name: str                  # e.g. "Minnesota Gig List"
    short_title: str                   # e.g. "MN GIG LIST" (HTML <title>)
    region_label: str                  # e.g. "Minnesota" (banner label)
    venue_urls: Dict[str, str]
    output_dir: Path
    months_ahead: int = 10
    base_url: str = "https://giglist.info"
    favicon_path: Optional[Path] = None  # unused; kept for backward compat


FAVICON_TAGS = (
    '<link rel="icon" type="image/x-icon" href="/favicon.ico">\n'
    '  <link rel="icon" type="image/svg+xml" href="/favicon.svg">\n'
    '  <link rel="icon" type="image/png" sizes="96x96" href="/favicon-96x96.png">\n'
    '  <link rel="apple-touch-icon" sizes="180x180" href="/apple-touch-icon.png">\n'
    '  <link rel="manifest" href="/site.webmanifest">'
)


def _get_week_monday(d):
    return d - timedelta(days=d.weekday())


def _parse_time_minutes(time_str):
    """Parse '7pm', '7:30pm', '12am' etc. into minutes from midnight."""
    if not time_str:
        return None
    s = time_str.strip().lower()
    if s.endswith("pm"):
        period = "pm"
        s = s[:-2]
    elif s.endswith("am"):
        period = "am"
        s = s[:-2]
    else:
        return None
    try:
        if ":" in s:
            h_str, m_str = s.split(":", 1)
            h, m = int(h_str), int(m_str)
        else:
            h, m = int(s), 0
    except ValueError:
        return None
    if period == "pm" and h != 12:
        h += 12
    elif period == "am" and h == 12:
        h = 0
    return h * 60 + m


def _show_sort_key(show):
    t = _parse_time_minutes(show.time)
    if t is None:
        t = _parse_time_minutes(show.doors)
    return (show.venue, t if t is not None else 10**6, show.title)


# ---------- ledger formatting (labels match the DESIGN.md mockup) ----------

def _ledger_time(show):
    """Ledger time cell: '7PM', doors+show '7PM/8PM', doors-only
    'DRS7PM', unknown '·'."""
    def up(t):
        return t.upper().replace(" ", "") if t else None

    doors, show_time = up(show.doors), up(show.time)
    if doors and show_time:
        # A few sources report doors == showtime; showing "9PM/9PM" is noise.
        if doors == show_time:
            return show_time
        return f"{doors}/{show_time}"
    if show_time:
        return show_time
    if doors:
        return f"DRS{doors}"
    return "·"


def _week_label(monday):
    """'WEEK OF JUL 20 – 26', crossing months 'WEEK OF JUL 27 – AUG 2'."""
    sunday = monday + timedelta(days=6)
    a = f"{monday.strftime('%b').upper()} {monday.day}"
    b = (
        f"{sunday.day}" if sunday.month == monday.month
        else f"{sunday.strftime('%b').upper()} {sunday.day}"
    )
    return f"WEEK OF {a} – {b}"


def _day_bar_label(d):
    """'THURSDAY — JUL 23' (zero-padded day, per the mockup)."""
    return (
        f"{d.strftime('%A').upper()} — "
        f"{d.strftime('%b').upper()} {d.strftime('%d')}"
    )


# ---------- page fragments ----------

def _row_html(show, venue_urls):
    venue_safe = escape(show.venue)
    title_safe = escape(show.title)
    venue_url = venue_urls.get(show.venue, "")
    ven = (
        f'<a href="{escape(venue_url)}">{venue_safe}</a>' if venue_url
        else venue_safe
    )
    act = (
        f'<a href="{escape(show.url)}">{title_safe}</a>' if show.url
        else title_safe
    )
    if show.supports:
        support_str = escape(", ".join(show.supports))
        act += f' <span class="sup">+ {support_str}</span>'
    flag = '<span class="flag">Sold out</span>' if show.sold_out else ""
    return (
        f'<div class="row" data-venue="{venue_safe}">'
        f'<span class="t">{escape(_ledger_time(show))}</span>'
        f'<span class="ven">{ven}</span>'
        f'<span class="act">{act}</span>'
        f'<span>{flag}</span>'
        f'</div>'
    )


def _week_section_html(monday, label, week_shows, venue_urls, heading=True):
    """One <section> of the ledger: week heading, then a black day bar +
    rows per day. ``heading`` stays on for the no-JS fallback; the
    stage-2 banner JS hides it when the banner carries the label."""
    days = {}
    for show in week_shows:
        days.setdefault(show.sort_date, []).append(show)

    fname = f"week-{monday.strftime('%Y-%m-%d')}"
    parts = [f'<section class="wk-sec" id="{fname}" data-label="{escape(label)}">']
    if heading:
        parts.append(f'<h2 class="week-h">{escape(label)}</h2>')
    for day_date in sorted(days.keys()):
        day_shows = sorted(days[day_date], key=_show_sort_key)
        parts.append(
            f'<h3 class="day-h" data-count="{len(day_shows)}">'
            f'{_day_bar_label(day_date)}</h3>'
        )
        parts.extend(_row_html(s, venue_urls) for s in day_shows)
    parts.append("</section>")
    return "\n".join(parts)


def _gl_data_json(config, all_weeks):
    """Compact JSON blob consumed by ledger.js to build the sidebar
    (week/month nav) on every page. Depends only on the week set, so
    it changes when weeks roll on/off — not on every render."""
    data = {
        "region": config.region_key,
        "weeks": [
            {"id": f"week-{monday.strftime('%Y-%m-%d')}", "label": label}
            for monday, label in all_weeks
        ],
    }
    return json.dumps(data, separators=(",", ":"))


def _page_shell(config, favicon, title, body, gl_data, banner_label):
    """Shared skeleton: Klein-blue banner (gl mark links home, week
    label centered) over the ledger column. No page title in the
    content — per DESIGN.md the first day bar leads.

    Without JS the banner is a plain home link and the week headings
    show. ledger.js progressively enhances: the banner becomes the menu
    button, its right label follows the week in view, and the sidebar
    (venue show/highlight/hide states, week/month/region/view nav) is
    built client-side from the #gl-data blob + row data-venue attrs."""
    return f"""<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>{escape(title)}</title>
  {favicon}
  <link rel="stylesheet" href="ledger.css">
</head>
<body>
  <a id="banner" href="/" aria-label="giglist home"><span class="mark">gl</span><span class="rlabel">{escape(banner_label)}</span></a>
  <main class="ledger">
{body}
    <p class="empty-note" hidden>All venues hidden &mdash; open the menu to bring some back.</p>
  </main>
  <script type="application/json" id="gl-data">{gl_data}</script>
  <script src="ledger.js" defer></script>
</body>
</html>"""


def _index_page_html(config, favicon, all_weeks, weeks, upcoming_count, updated):
    sections = [
        _week_section_html(monday, label, weeks[monday], config.venue_urls)
        for monday, label in all_weeks
    ]
    foot = (
        f'<p class="foot">Updated {updated} '
        f'&middot; {upcoming_count} shows</p>'
    )
    body = "\n".join(sections + [foot])
    first_label = all_weeks[0][1] if all_weeks else config.short_title
    return _page_shell(config, favicon, config.short_title, body,
                       _gl_data_json(config, all_weeks), first_label)


def _week_page_html(config, favicon, monday, label, week_shows, prev_monday,
                    next_monday, all_weeks):
    """Week pages intentionally omit the 'Updated:' timestamp so the
    file's content is stable across daily scrapes when no shows moved —
    otherwise every week page diffs every single day."""
    nav = ['<nav class="subnav">']
    if prev_monday:
        nav.append(
            f'<a href="week-{prev_monday.strftime("%Y-%m-%d")}.html">'
            f'&larr; Prev week</a>'
        )
    nav.append('<a href="./">All weeks</a>')
    if next_monday:
        nav.append(
            f'<a href="week-{next_monday.strftime("%Y-%m-%d")}.html">'
            f'Next week &rarr;</a>'
        )
    nav.append("</nav>")
    section = _week_section_html(monday, label, week_shows, config.venue_urls)
    body = "\n".join(["\n".join(nav), section])
    title = f'{config.short_title} - {label.replace("WEEK OF ", "")}'
    return _page_shell(config, favicon, title, body,
                       _gl_data_json(config, all_weeks), label)


def _list_stub_html(config):
    """list.html was the old table view; the region index now carries the
    full list, so keep the URL alive as a redirect stub."""
    return f"""<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <meta http-equiv="refresh" content="0; url=./">
  <link rel="canonical" href="./">
  <title>{escape(config.short_title)}</title>
</head>
<body>
  <p><a href="./">Moved &mdash; the full list is now the front page.</a></p>
</body>
</html>"""


def _sitemap_xml(config, all_weeks):
    today_str = _today_central().strftime("%Y-%m-%d")
    prefix = f"{config.base_url}/{config.region_key}"
    entries = [
        f'  <url><loc>{prefix}/</loc><lastmod>{today_str}</lastmod><changefreq>daily</changefreq></url>',
    ]
    for monday, _label in all_weeks:
        fname = f"week-{monday.strftime('%Y-%m-%d')}.html"
        entries.append(
            f'  <url><loc>{prefix}/{fname}</loc><lastmod>{today_str}</lastmod><changefreq>daily</changefreq></url>'
        )
    return (
        '<?xml version="1.0" encoding="UTF-8"?>\n'
        '<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">\n'
        + "\n".join(entries)
        + "\n</urlset>"
    )


def _copy_assets(output_dir: Path):
    for name in ("ledger.css", "ledger.js"):
        src = ASSETS_DIR / name
        if src.exists():
            shutil.copyfile(src, output_dir / name)
    # The pre-ledger stylesheets; remove stale copies from output dirs.
    for old in ("page.css", "table.css"):
        stale = output_dir / old
        if stale.exists():
            stale.unlink()


def write_site(config: RegionConfig, shows: List[Show]):
    """Render the full static site for one region."""
    output_dir = config.output_dir
    output_dir.mkdir(parents=True, exist_ok=True)

    _copy_assets(output_dir)
    favicon = FAVICON_TAGS

    now_central = datetime.now(CENTRAL_TZ)
    updated = now_central.strftime("%b %-d, %Y")
    today = now_central.date()

    upcoming = [s for s in shows if s.sort_date >= today]

    # Group upcoming by week
    weeks = {}
    for show in upcoming:
        monday = _get_week_monday(show.sort_date)
        weeks.setdefault(monday, []).append(show)

    cutoff_date = today + relativedelta(months=config.months_ahead)
    all_weeks = [
        (monday, _week_label(monday))
        for monday in sorted(weeks.keys())
        if monday <= cutoff_date
    ]

    current_week_files = set()
    for i, (monday, label) in enumerate(all_weeks):
        fname = f"week-{monday.strftime('%Y-%m-%d')}.html"
        current_week_files.add(fname)
        prev_monday = all_weeks[i - 1][0] if i > 0 else None
        next_monday = all_weeks[i + 1][0] if i < len(all_weeks) - 1 else None
        html = _week_page_html(
            config, favicon, monday, label, weeks[monday],
            prev_monday, next_monday, all_weeks,
        )
        (output_dir / fname).write_text(html)

    stale = 0
    for old in output_dir.glob("week-*.html"):
        if old.name not in current_week_files:
            old.unlink()
            stale += 1
    if stale:
        print(f"Removed {stale} stale week page(s)")

    (output_dir / "index.html").write_text(
        _index_page_html(config, favicon, all_weeks, weeks, len(upcoming), updated)
    )

    (output_dir / "list.html").write_text(_list_stub_html(config))

    (output_dir / "sitemap.xml").write_text(_sitemap_xml(config, all_weeks))

    print(f"Wrote index.html: continuous ledger, {len(all_weeks)} weeks, "
          f"{len(upcoming)} shows")
    print(f"Wrote {len(all_weeks)} week pages")
    print("Wrote list.html redirect stub")
    print("Wrote sitemap.xml")
