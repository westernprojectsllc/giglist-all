"""Region-agnostic static-site renderer.

Takes a RegionConfig + a list of Show, writes index.html / list.html /
past.html / week-*.html / sitemap.xml / style CSS into the region's
output directory.
"""

import shutil
from dataclasses import dataclass, field
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
    region_label: str                  # e.g. "Minnesota" (subtitle)
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


def _favicon_tag(config: RegionConfig) -> str:
    return FAVICON_TAGS


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


def _venue_show_html(show, venue_urls):
    venue_safe = escape(show.venue)
    title_safe = escape(show.title)
    venue_url = venue_urls.get(show.venue, "")
    if venue_url:
        venue_html = f'<span class="venue-link"><a href="{escape(venue_url)}">{venue_safe}</a></span>'
    else:
        venue_html = f'<span class="venue-link">{venue_safe}</span>'
    if show.url:
        show_html = f'<span class="show-link"><a href="{escape(show.url)}">{title_safe}</a></span>'
    else:
        show_html = f'<span class="show-link">{title_safe}</span>'
    return venue_html, show_html


def _build_day_rows(week_shows, venue_urls):
    days = {}
    for show in week_shows:
        days.setdefault(show.sort_date, []).append(show)

    rows = []
    for day_date in sorted(days.keys()):
        day_label = day_date.strftime("%a %b %-d")
        rows.append(f'<li><span>{day_label}</span>')
        rows.append('<ul class="shows">')
        for show in sorted(days[day_date], key=_show_sort_key):
            venue_html, show_html = _venue_show_html(show, venue_urls)

            if show.supports:
                support_str = ", ".join(escape(s) for s in show.supports)
                show_html += f' <span class="supports">with {support_str}</span>'

            extras = []
            if show.doors and show.time:
                extras.append(f'<span class="time">{escape(show.doors)}/{escape(show.time)}</span>')
            elif show.doors:
                extras.append(f'<span class="time">doors {escape(show.doors)}</span>')
            elif show.time:
                extras.append(f'<span class="time">{escape(show.time)}</span>')
            if show.sold_out:
                extras.append('<span class="sold-out">sold out</span>')

            line = venue_html + " " + show_html
            if extras:
                line += " - " + " ".join(extras)
            rows.append(f"<li>{line}</li>")
        rows.append("</ul></li>")
    return rows


def _build_week_nav(all_weeks, highlight=None):
    months = {}
    for wdate, wlabel, _short in all_weeks:
        month_key = wdate.strftime("%B %Y")
        months.setdefault(month_key, [])
        fname = f"week-{wdate.strftime('%Y-%m-%d')}.html"
        if wlabel == highlight:
            months[month_key].append(f'<strong>{wlabel}</strong>')
        else:
            months[month_key].append(f'<a href="{fname}">{wlabel}</a>')
    return "\n".join(
        f'<div class="month-line">{" | ".join(links)}</div>'
        for links in months.values()
    )


def _build_table(shows):
    months = {}
    for show in shows:
        months.setdefault(show.sort_date.strftime("%B %Y"), []).append(show)

    rows = []
    for month_name, month_shows in months.items():
        rows.append(f'  <tr class="month-header"><td colspan="3">{month_name}</td></tr>')
        for show in month_shows:
            date_display = show.sort_date.strftime("%b %-d")
            day_name = show.sort_date.strftime("%a")
            title_safe = escape(show.title)
            venue_safe = escape(show.venue)
            if show.url:
                title_cell = f'<a href="{escape(show.url)}">{title_safe}</a>'
            else:
                title_cell = title_safe
            rows.append(
                f'  <tr>'
                f'<td>{day_name} {date_display}</td>'
                f'<td>{venue_safe}</td>'
                f'<td>{title_cell}</td>'
                f'</tr>'
            )
    return "\n".join(rows)


def _week_page_html(config, favicon, week_shows, week_label, short_label, all_weeks):
    """Week pages intentionally omit the 'Updated:' timestamp so the
    file's content is stable across daily scrapes when no shows moved —
    otherwise every week page diffs every single day."""
    rows = _build_day_rows(week_shows, config.venue_urls)
    week_nav_html = _build_week_nav(all_weeks, highlight=week_label)
    return f"""<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <title>{escape(config.short_title)} - {escape(short_label)}</title>
  {favicon}
  <link rel="stylesheet" href="page.css">
</head>
<body>
  <h1><a href="index.html">{escape(config.display_name)}</a></h1>
  <nav><a href="list.html">List View</a> | <a href="past.html">Past Shows</a></nav>
  <div class="week-nav">{week_nav_html}</div>
  <h2>{escape(week_label)}</h2>
  <ul class="days">
{"".join(rows)}
  </ul>
</body>
</html>"""


def _index_page_html(config, favicon, upcoming_count, past_count, updated,
                     all_weeks, this_week_shows):
    week_nav_html = _build_week_nav(all_weeks)
    this_week_html = ""
    if this_week_shows:
        rows = _build_day_rows(this_week_shows, config.venue_urls)
        this_week_html = f"""  <h2 style="margin-top: 24px;">This Week</h2>
  <ul class="days">
{"".join(rows)}
  </ul>"""

    return f"""<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <title>{escape(config.short_title)}</title>
  {favicon}
  <link rel="stylesheet" href="page.css">
</head>
<body>
  <h1>{escape(config.display_name)}</h1>
  <p class="subtitle">Updated: {updated} &mdash; {upcoming_count} upcoming shows &mdash; <a href="past.html">Past Shows ({past_count})</a></p>
  <nav><a href="list.html">List View</a></nav>
  <h2>Concerts By Week</h2>
  <div class="week-nav">{week_nav_html}</div>
{this_week_html}
</body>
</html>"""


def _list_page_html(config, favicon, upcoming, past_count, updated):
    return f"""<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <title>{escape(config.short_title)}</title>
  {favicon}
  <link rel="stylesheet" href="table.css">
</head>
<body>
  <h1>{escape(config.display_name)}</h1>
  <p class="subtitle">Updated: {updated} &mdash; {len(upcoming)} upcoming shows across {escape(config.region_label)}</p>
  <nav>
    <a href="past.html">Past Shows ({past_count})</a>
    <a href="index.html">Weekly View</a>
  </nav>
  <table>
{_build_table(upcoming)}
  </table>
</body>
</html>"""


def _past_page_html(config, favicon, past, updated):
    past_days = {}
    for show in past:
        past_days.setdefault(show.sort_date, []).append(show)

    rows = []
    for day_date in sorted(past_days.keys(), reverse=True):
        day_label = day_date.strftime("%a %b %-d, %Y")
        rows.append(f'<li><span>{day_label}</span><ul class="shows">')
        for show in sorted(past_days[day_date], key=_show_sort_key):
            venue_html, show_html = _venue_show_html(show, config.venue_urls)
            rows.append(f"<li>{venue_html} {show_html}</li>")
        rows.append("</ul></li>")

    return f"""<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <title>{escape(config.short_title)} - PAST</title>
  {favicon}
  <link rel="stylesheet" href="page.css">
</head>
<body>
  <h1><a href="index.html">{escape(config.display_name)}</a> — Past Shows</h1>
  <p class="subtitle">Updated: {updated} &mdash; {len(past)} past shows</p>
  <nav><a href="index.html">← Upcoming Shows</a></nav>
  <ul class="days">
{"".join(rows)}
  </ul>
</body>
</html>"""


def _sitemap_xml(config, all_weeks):
    today_str = _today_central().strftime("%Y-%m-%d")
    prefix = f"{config.base_url}/{config.region_key}"
    entries = [
        f'  <url><loc>{prefix}/</loc><lastmod>{today_str}</lastmod><changefreq>daily</changefreq></url>',
        f'  <url><loc>{prefix}/list.html</loc><lastmod>{today_str}</lastmod><changefreq>daily</changefreq></url>',
        f'  <url><loc>{prefix}/past.html</loc><lastmod>{today_str}</lastmod><changefreq>daily</changefreq></url>',
    ]
    for monday, _label, _short in all_weeks:
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
    for name in ("page.css", "table.css"):
        src = ASSETS_DIR / name
        if src.exists():
            shutil.copyfile(src, output_dir / name)


def write_site(config: RegionConfig, shows: List[Show]):
    """Render the full static site for one region."""
    output_dir = config.output_dir
    output_dir.mkdir(parents=True, exist_ok=True)

    _copy_assets(output_dir)
    favicon = _favicon_tag(config)

    now_central = datetime.now(CENTRAL_TZ)
    updated = now_central.strftime("%B %d, %Y at %I:%M %p")
    today = now_central.date()
    one_month_ago = today - timedelta(days=31)

    upcoming = [s for s in shows if s.sort_date >= today]
    past = [s for s in shows if one_month_ago <= s.sort_date < today]

    # List view
    (output_dir / "list.html").write_text(
        _list_page_html(config, favicon, upcoming, len(past), updated)
    )

    # Group upcoming by week
    weeks = {}
    for show in upcoming:
        monday = _get_week_monday(show.sort_date)
        weeks.setdefault(monday, []).append(show)

    cutoff_date = today + relativedelta(months=config.months_ahead)
    all_weeks = []
    for monday in sorted(weeks.keys()):
        if monday > cutoff_date:
            continue
        sunday = monday + timedelta(days=6)
        label = f"{monday.strftime('%b %-d')} - {sunday.strftime('%b %-d')}"
        short_label = f"{monday.strftime('%-m/%-d')} to {sunday.strftime('%-m/%-d')}"
        all_weeks.append((monday, label, short_label))

    for monday, label, short_label in all_weeks:
        html = _week_page_html(
            config, favicon, weeks[monday], label, short_label, all_weeks,
        )
        (output_dir / f"week-{monday.strftime('%Y-%m-%d')}.html").write_text(html)

    window_end = today + timedelta(days=6)
    this_week = [s for s in upcoming if s.sort_date <= window_end]
    (output_dir / "index.html").write_text(
        _index_page_html(config, favicon, len(upcoming), len(past), updated, all_weeks, this_week)
    )

    (output_dir / "past.html").write_text(
        _past_page_html(config, favicon, past, updated)
    )

    (output_dir / "sitemap.xml").write_text(_sitemap_xml(config, all_weeks))

    print(f"Wrote list.html ({len(upcoming)} upcoming shows, table view)")
    print(f"Wrote index.html with {len(all_weeks)} weeks (weekly view)")
    print(f"Wrote {len(all_weeks)} week pages")
    print(f"Wrote past.html with {len(past)} past shows")
    print("Wrote sitemap.xml")
