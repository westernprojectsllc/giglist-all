# Giglist redesign — aesthetic preferences

Recorded 2026-07-23 from the mockup exploration session. These are Will's
chosen directions for a future visual redesign of the region pages
(scrapers and shows.json stay exactly as they are; only rendering changes).

Living mockup (all options toggleable): https://claude.ai/code/artifact/af8d8bb6-43ee-4628-a8ae-be157ce80ac6
Mockup source file (session scratchpad, may not persist): `giglist-bw-mockups.html`

## Chosen setup

**Layout: "Tight Ledger"** — strict black-and-white, monospace throughout
(`ui-monospace / SF Mono / Menlo` stack), maximum quick-reference density.

- No page title. Content starts directly with the first day bar.
- Days labeled as full-width black bars, white uppercase text
  (e.g. `MONDAY — JUL 06`). No show counts displayed in the bars
  (count kept as a `data-count` attribute, not rendered).
- One ruled line per show: time | venue | artist (+ supports) | sold-out flag.
  Compact padding (~3px rows), 1px black rules between rows.
- No horizontal rule above the week heading.

**Header: "Banner + Week"** — full-width International Klein Blue banner,
**no black or white border**.

- Left: the lowercase `gl` logo exactly as on the landing page
  (Helvetica Neue 800, 15px, letter-spacing -0.03em, color `#C7CCD6`),
  24px from the left edge.
- Centered: the week label (`WEEK OF JUL 6 – 12`) in the ledger monospace,
  11px uppercase, letter-spacing .22em, white. (Amended 2026-07-23 from
  right-aligned to centered; server-rendered so it shows without JS —
  the index carries the first week's label until scrolling updates it.)
- The in-page week heading is omitted in this mode — the banner carries it.
- Clicking the banner opens the sidebar (the logo is the menu button).

**Sidebar** — inverted (black background, white text), compact (~208px),
slides in from the left.

- Categories are click-to-expand dropdowns with +/− indicators, in this
  order: Venues, Weeks, Months, Region, Views (Views last).
- Venues opens by default; each venue has one square icon that cycles
  through four states on click:
  1. show — solid white square
  2. highlight (blue) — solid Klein blue square
  3. highlight (grey) — solid Stone square
  4. hide — solid black square with white outline
- Hidden venues: struck-through name, rows removed from listings.
- A small "reset" link restores all venues to show.

## Colors

| Role | Value | Notes |
|---|---|---|
| Paper | `#FFFFFF` | page ground (inverts to black in dark mode) |
| Ink | `#000000` | text, rules, day bars |
| Klein blue | `#002FA7` | matches landing page; banner, logo tile, blue highlight rows (white text) |
| Grey highlight | **Stone `#E4E4E2`** | chosen from six candidates; black text; a whisper-subtle tint that doesn't compete with the blue |
| Logo mark | `#C7CCD6` | the silvery "gl" on Klein blue, from the landing page |

Highlighted rows are full-bleed blocks (background color across the whole
row), not chips or side bars.

## Rejected along the way (for the record)

- Popover/drop menu for venue state (replaced by cycle-on-click).
- MENU text button, black GL button, blue GL box (replaced by logo-as-banner).
- Day-count numerals at the right end of day bars.
- Thick black rule above week headings.
- Greys: Warm `#8A8A8A`, Slate `#A9AFBC`, Cool `#7A7E87`, Silver `#C7C7C7`,
  Charcoal `#3C3C3E` — all lost to Stone.
- Poster and Grid layout directions; Rail and Columns ledger variants.
