"""Read shows.json (produced by scraper.py) and render the giglist HTML.

Splitting rendering out of the scraper means you can iterate on layout
or styles without re-fetching every venue. Run `python render.py` after
a scrape to rebuild only the HTML.
"""

import json
import sys
import urllib.parse
from datetime import datetime, date, timedelta
from html import escape
from pathlib import Path
from dateutil.relativedelta import relativedelta

from models import Show, MONTHS_AHEAD, VENUE_URLS

# Inline the favicon as a data URI so it works over file:// in browsers
# (notably Safari) that don't reliably fetch sibling SVG favicons locally.
_FAVICON_SVG = (Path(__file__).parent / "favicon.svg").read_text()
FAVICON_TAG = (
    f'<link rel="icon" type="image/svg+xml" '
    f'href="data:image/svg+xml,{urllib.parse.quote(_FAVICON_SVG)}">'
)

SHOWS_JSON = "shows.json"


def get_week_monday(d):
    return d - timedelta(days=d.weekday())


PAGE_STYLE = """
    body { font-family: serif; padding: 20px; background: #fff; max-width: 1100px; margin: 0 auto; font-size: 1.15em; }
    h1 { font-size: 2em; margin-bottom: 4px; text-decoration: underline; }
    h2 { font-size: 1.4em; margin-top: 0; }
    .subtitle { color: #666; font-size: 0.9em; margin-bottom: 16px; }
    nav { margin-bottom: 16px; font-size: 1em; }
    nav a { color: #c00; margin-right: 12px; }
    ul.weeks { list-style: none; padding: 0; margin: 0; }
    ul.weeks li { margin: 4px 0; }
    ul.weeks li a { color: #00c; font-size: 1.1em; }
    ul.days { list-style: disc; padding-left: 20px; }
    ul.days > li { margin: 10px 0; font-weight: bold; }
    ul.shows { list-style: circle; padding-left: 24px; font-weight: normal; }
    ul.shows li { margin: 3px 0; }
    .venue-link { font-weight: bold; }
    .venue-link a { color: #00c; text-decoration: underline; }
    .show-link a { color: #00c; }
    .supports { color: #333; }
    .time { color: #555; }
    .sold-out { color: #c00; font-weight: bold; }
    a { color: #00c; }
    .week-nav { margin-bottom: 16px; font-size: 1em; }
    .week-nav a { color: #00c; }
    .week-nav strong { color: #000; }
    .month-line { margin: 4px 0; }
"""


def build_week_nav(all_weeks, highlight=None):
    """Build week navigation HTML grouped by month, one line per month."""
    months = {}
    for wdate, wlabel, _short in all_weeks:
        month_key = wdate.strftime("%B %Y")
        if month_key not in months:
            months[month_key] = []
        fname = f"week-{wdate.strftime('%Y-%m-%d')}.html"
        if wlabel == highlight:
            months[month_key].append(f'<strong>{wlabel}</strong>')
        else:
            months[month_key].append(f'<a href="{fname}">{wlabel}</a>')
    lines = []
    for month_name, links in months.items():
        lines.append(f'<div class="month-line">{" | ".join(links)}</div>')
    return "\n".join(lines)


def _venue_show_html(show):
    """Render a show's "[venue] [title]" HTML fragment with linked venue
    and title if URLs are available. Shared by the weekly and past views."""
    venue_safe = escape(show.venue)
    title_safe = escape(show.title)
    venue_url = VENUE_URLS.get(show.venue, "")
    if venue_url:
        venue_html = f'<span class="venue-link"><a href="{escape(venue_url)}">{venue_safe}</a></span>'
    else:
        venue_html = f'<span class="venue-link">{venue_safe}</span>'
    if show.url:
        show_html = f'<span class="show-link"><a href="{escape(show.url)}">{title_safe}</a></span>'
    else:
        show_html = f'<span class="show-link">{title_safe}</span>'
    return venue_html, show_html


def build_day_rows(week_shows):
    """Build HTML list items for a set of shows grouped by day."""
    days = {}
    for show in week_shows:
        days.setdefault(show.sort_date, []).append(show)

    rows = []
    for day_date in sorted(days.keys()):
        day_label = day_date.strftime("%a %b %-d")
        rows.append(f'<li><span>{day_label}</span>')
        rows.append('<ul class="shows">')
        for show in sorted(days[day_date], key=lambda s: s.venue):
            venue_html, show_html = _venue_show_html(show)

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


def build_week_html(week_shows, week_label, updated, all_weeks, short_label):
    rows = build_day_rows(week_shows)
    week_nav_html = build_week_nav(all_weeks, highlight=week_label)

    return f"""<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <title>MN GIG LIST - {short_label}</title>
  {FAVICON_TAG}
  <style>{PAGE_STYLE}</style>
</head>
<body>
  <h1><a href="index.html">Minnesota Gig List</a></h1>
  <p class="subtitle">Updated: {updated}</p>
  <nav><a href="list.html">List View</a> | <a href="past.html">Past Shows</a></nav>
  <div class="week-nav">{week_nav_html}</div>
  <h2>{week_label}</h2>
  <ul class="days">
{"".join(rows)}
  </ul>
</body>
</html>"""


TABLE_STYLE = """
    body { font-family: monospace; padding: 20px; background: #fff; max-width: 1100px; margin: 0 auto; }
    h1 { font-size: 1.4em; margin-bottom: 2px; }
    .subtitle { color: #666; font-size: 0.85em; margin-bottom: 20px; }
    nav { margin-bottom: 16px; font-size: 0.9em; }
    nav a { color: #c00; margin-right: 12px; }
    table { border-collapse: collapse; width: 100%; }
    td { padding: 4px 12px 4px 0; vertical-align: top; font-size: 0.9em; }
    a { color: #000; }
    a:hover { color: #c00; }
    tr:hover { background: #f5f5f5; }
    .month-header td {
      font-weight: bold;
      font-size: 1.1em;
      padding: 16px 0 6px 0;
      border-bottom: 2px solid #000;
    }
    .month-header:hover { background: none; }
"""


def build_table(shows):
    months = {}
    for show in shows:
        month_key = show.sort_date.strftime("%B %Y")
        if month_key not in months:
            months[month_key] = []
        months[month_key].append(show)

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


def write_table_html(upcoming, past, updated):
    upcoming_html = f"""<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <title>MN GIG LIST</title>
  {FAVICON_TAG}
  <style>
{TABLE_STYLE}
  </style>
</head>
<body>
  <h1>Minnesota Gig List</h1>
  <p class="subtitle">Updated: {updated} &mdash; {len(upcoming)} upcoming shows across Minnesota</p>
  <nav>
    <a href="past.html">Past Shows ({len(past)})</a>
    <a href="index.html">Weekly View</a>
  </nav>
  <table>
{build_table(upcoming)}
  </table>
</body>
</html>"""

    with open("list.html", "w") as f:
        f.write(upcoming_html)


def write_html(shows):
    updated = datetime.now().strftime("%B %d, %Y at %I:%M %p")
    today = date.today()
    week_start = get_week_monday(today)

    one_month_ago = today - timedelta(days=31)
    upcoming = [s for s in shows if s.sort_date >= week_start]
    past = [s for s in shows if one_month_ago <= s.sort_date < week_start]

    write_table_html(upcoming, past, updated)

    # Group upcoming shows by week
    weeks = {}
    for show in upcoming:
        monday = get_week_monday(show.sort_date)
        if monday not in weeks:
            weeks[monday] = []
        weeks[monday].append(show)

    # Build week label list for navigation (limit to MONTHS_AHEAD ahead)
    cutoff_date = today + relativedelta(months=MONTHS_AHEAD)
    all_weeks = []
    for monday in sorted(weeks.keys()):
        if monday > cutoff_date:
            continue
        sunday = monday + timedelta(days=6)
        label = f"{monday.strftime('%b %-d')} - {sunday.strftime('%b %-d')}"
        short_label = f"{monday.strftime('%-m/%-d')} to {sunday.strftime('%-m/%-d')}"
        all_weeks.append((monday, label, short_label))

    # Write each week page
    for monday, label, short_label in all_weeks:
        week_shows = weeks[monday]
        html = build_week_html(week_shows, label, updated, all_weeks, short_label)
        fname = f"week-{monday.strftime('%Y-%m-%d')}.html"
        with open(fname, "w") as f:
            f.write(html)

    # Build index page
    week_nav_html = build_week_nav(all_weeks)

    # Build "This Week" section
    this_week_shows = weeks.get(week_start, [])
    this_week_rows = build_day_rows(this_week_shows) if this_week_shows else []
    this_week_html = ""
    if this_week_rows:
        this_week_html = f"""  <h2 style="margin-top: 24px;">This Week</h2>
  <ul class="days">
{"".join(this_week_rows)}
  </ul>"""

    weekly_html = f"""<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <title>MINNESOTA GIG LIST</title>
  {FAVICON_TAG}
  <style>{PAGE_STYLE}</style>
</head>
<body>
  <h1>Minnesota Gig List</h1>
  <p class="subtitle">Updated: {updated} &mdash; {len(upcoming)} upcoming shows &mdash; <a href="past.html">Past Shows ({len(past)})</a></p>
  <nav><a href="list.html">List View</a></nav>
  <h2>Concerts By Week</h2>
  <div class="week-nav">{week_nav_html}</div>
{this_week_html}
</body>
</html>"""

    with open("index.html", "w") as f:
        f.write(weekly_html)

    # Build past page
    past_days = {}
    for show in past:
        past_days.setdefault(show.sort_date, []).append(show)

    past_rows = []
    for day_date in sorted(past_days.keys(), reverse=True):
        day_label = day_date.strftime("%a %b %-d, %Y")
        past_rows.append(f'<li><span>{day_label}</span><ul class="shows">')
        for show in sorted(past_days[day_date], key=lambda s: s.venue):
            venue_html, show_html = _venue_show_html(show)
            past_rows.append(f"<li>{venue_html} {show_html}</li>")
        past_rows.append("</ul></li>")

    past_html = f"""<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <title>MN GIG LIST - PAST</title>
  {FAVICON_TAG}
  <style>{PAGE_STYLE}</style>
</head>
<body>
  <h1><a href="index.html">Minnesota Gig List</a> — Past Shows</h1>
  <p class="subtitle">Updated: {updated} &mdash; {len(past)} past shows</p>
  <nav><a href="index.html">← Upcoming Shows</a></nav>
  <ul class="days">
{"".join(past_rows)}
  </ul>
</body>
</html>"""

    with open("past.html", "w") as f:
        f.write(past_html)

    # Build sitemap.xml
    base_url = "https://giglist.info"
    today_str = today.strftime("%Y-%m-%d")
    sitemap_urls = [
        f'  <url><loc>{base_url}/</loc><lastmod>{today_str}</lastmod><changefreq>daily</changefreq></url>',
        f'  <url><loc>{base_url}/list.html</loc><lastmod>{today_str}</lastmod><changefreq>daily</changefreq></url>',
        f'  <url><loc>{base_url}/past.html</loc><lastmod>{today_str}</lastmod><changefreq>daily</changefreq></url>',
    ]
    for monday, label, _short in all_weeks:
        fname = f"week-{monday.strftime('%Y-%m-%d')}.html"
        sitemap_urls.append(f'  <url><loc>{base_url}/{fname}</loc><lastmod>{today_str}</lastmod><changefreq>daily</changefreq></url>')

    sitemap = f"""<?xml version="1.0" encoding="UTF-8"?>
<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">
{chr(10).join(sitemap_urls)}
</urlset>"""

    with open("sitemap.xml", "w") as f:
        f.write(sitemap)

    # Build robots.txt
    with open("robots.txt", "w") as f:
        f.write(f"User-agent: *\nAllow: /\nSitemap: {base_url}/sitemap.xml\n")

    print(f"Wrote list.html ({len(upcoming)} upcoming shows, table view)")
    print(f"Wrote index.html with {len(all_weeks)} weeks (weekly view)")
    print(f"Wrote {len(all_weeks)} week pages")
    print(f"Wrote past.html with {len(past)} past shows")
    print("Wrote sitemap.xml and robots.txt")


def load_shows(path=SHOWS_JSON):
    with open(path) as f:
        raw = json.load(f)
    return [Show.from_json_dict(d) for d in raw]


if __name__ == "__main__":
    path = sys.argv[1] if len(sys.argv) > 1 else SHOWS_JSON
    shows = load_shows(path)
    print(f"Loaded {len(shows)} shows from {path}")
    write_html(shows)
