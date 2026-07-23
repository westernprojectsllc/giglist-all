# giglist-all

Static music-events site for giglist.info (GitHub Pages, CNAME in repo).
One repo drives every region: shared code in `giglist/`, one directory per
region (`mn/` = Minneapolis/St. Paul, `tn/` = Nashville).

## Architecture

- `giglist/models.py` — the `Show` dataclass (title, sort_date, venue, url,
  time, doors, supports, sold_out).
- `giglist/http.py` — `get_with_retry` (retries exceptions/429/5xx); all
  venue fetches go through it.
- `giglist/scrape_utils.py` — shared fetchers (Tribe Events, Ticketmaster,
  Dice), dedupe passes, junk/sports/non-music filter, and
  `check_venue_dropouts` (fails the run rather than publish a gutted list
  when >2 venues suddenly return zero shows).
- `giglist/render.py` + `giglist/assets/ledger.{css,js}` — the renderer.
- `<region>/config.py` — venue URLs, Ticketmaster IDs, filter keywords,
  RegionConfig. `<region>/scraper.py` — venue scrapers + main. Both
  regions' scrapers write `shows.json`; `render.py` reads it.

## Design is specified, not improvised

**Read `DESIGN.md` before touching anything visual.** It records the chosen
"Tight Ledger" aesthetic (B/W monospace ledger, Klein-blue banner, Stone
highlight) including explicitly rejected alternatives; the reference mockup
is `mockups/giglist-bw-mockup.html`. The root `index.html` (Klein-blue
50-state index) is hand-authored — the renderer never writes it.
`ledger.js` is a progressive enhancement: region pages must remain complete
and readable with JavaScript disabled.

## Pipeline

- GitHub Action "Scrape and Commit" runs daily at 12:00 UTC: scrapes both
  regions in parallel, re-renders, commits as `Daily scrape YYYY-MM-DD`.
  Pages redeploys on push.
- "Smoke Tests" Action runs 1h later: `tests/test_scrapers.py` hits every
  venue live and fails if any scraper returns zero shows.
- **Generated files** (`mn/*.html`, `tn/*.html`, `*/shows.json`,
  `*/sitemap.xml`, `*/ledger.css`, `*/ledger.js`) are bot-written — never
  hand-edit them; change the renderer/scrapers instead.
- The local checkout is usually behind origin (the bot commits daily):
  `git pull --ff-only` before diagnosing anything.

## Local commands

```bash
pip install -r requirements.txt
cd mn && python scraper.py && python render.py   # same for tn/
pytest tests/test_render.py tests/test_scrape_utils.py   # pure unit tests
pytest tests/test_scrapers.py                            # live-network smoke tests
```

`TM_API_KEY` (Ticketmaster Discovery) lives in GitHub Actions secrets; runs
without it skip TM venues and the dropout guard knows to ignore them.

## Conventions

- Renderer output is byte-stable: unchanged data must produce unchanged
  files (no timestamps in week pages) so daily commits stay minimal.
- New venue scraper checklist: `scrape_*()` returning `list[Show]` using
  `get_with_retry`; register in the main block; add `VENUE_URLS` entry in
  config.py (the ledger links venue names); add a smoke-test entry in
  `tests/test_scrapers.py`.
- Prefer a venue's JSON source (Tribe REST, Dice, Shopify products.json,
  embedded JSON blobs) over HTML parsing; music events only — reuse the
  junk/sports/non-music filters.
