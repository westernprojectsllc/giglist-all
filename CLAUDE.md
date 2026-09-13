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
  junk/sports/non-music filters. Verify a paginated source actually
  paginates: Dakota's Tribe endpoint ignores `page`/`per_page`/`start_date`
  and serves the same 10 of 129 events to every request, so it published
  8 shows and looked healthy. `scrape_tribe_events` now checks the haul
  against the `total` the API reports.

## Don't get locked out

Being blocked costs a venue's entire listing, and the failure is quiet —
a challenge page or a 429 body parses to zero shows and reads exactly
like a venue with nothing booked.

- **All fetching goes through `giglist/http.py`.** Never call
  `requests.get`/`session.get` directly: `get_with_retry` (and the curl /
  curl_cffi helpers) route through a per-host gate that caps concurrency
  and spacing, honours `Retry-After`, and backs off exponentially.
- **Tune `HOST_LIMITS`, not `max_workers`.** The gate is what a site
  actually sees; a thread pool is only wall-clock. Add a `HOST_LIMITS`
  entry for any venue that rate-limits or looks fragile, and keep it
  conservative — a slower scrape costs seconds on a once-a-day job.
- **Never let an error response pass as data.** `get_with_retry` returns
  the final 429/5xx response by default, which is fine for callers that
  just yield zero shows. Anywhere an error body is indistinguishable from
  an empty one — a paginator that stops on "no results", a per-month or
  per-page fan-out — pass `raise_on_exhausted=True` and handle it.
- **A unit that failed to fetch is not a unit with nothing in it.** If a
  scraper covers several months/pages/venues, a partial result keeps the
  venue non-empty, so neither `check_venue_dropouts` nor the smoke test
  notices. First Avenue and Dakota both raise rather than return a
  partial listing.
