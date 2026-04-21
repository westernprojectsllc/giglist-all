"""Tennessee-specific config consumed by both scraper.py and render.py."""

import re
from pathlib import Path

from giglist.render import RegionConfig
from giglist.scrape_utils import COMMON_JUNK_KEYWORDS, COMMON_SPORTS_KEYWORDS


REGION_DIR = Path(__file__).parent


VENUE_URLS = {
    "Bridgestone Arena":            "https://www.bridgestonearena.com/",
    "Ryman Auditorium":             "https://ryman.com/",
    "Ascend Amphitheater":          "https://www.ascendamphitheater.com/",
    "Grand Ole Opry House":         "https://www.opry.com/",
    "Brooklyn Bowl Nashville":      "https://www.brooklynbowl.com/nashville",
    "FirstBank Amphitheater":       "https://www.firstbankamphitheater.com/",
    "Nissan Stadium":               "https://nissanstadium.com/",
    "The Pinnacle":                 "https://thepinnaclenashville.com/",
    "Cannery Hall":                 "https://canneryhall.com/",
    "Exit/In":                      "https://exitin.com/",
    "The Basement East":            "https://thebasementnashville.com/the-basement-east/",
    "The Basement":                 "https://thebasementnashville.com/",
    "Marathon Music Works":         "https://marathonmusicworks.com/",
    "3rd & Lindsley":               "https://www.3rdandlindsley.com/",
    "Eastside Bowl":                "https://www.eastsidebowl.com/",
    "Schermerhorn Symphony Center": "https://www.nashvillesymphony.org/",
    "TPAC":                         "https://www.tpac.org/",
    "Station Inn":                  "https://stationinn.com/",
    "Skydeck":                      "https://www.assemblyfoodhall.com/skydeck/",
    "DRKMTTR":                      "https://www.drkmttrcollective.com/",
    "The End":                      "https://endnashville.com/",
    "Night We Met":                 "https://nightwemetnashville.com/",
    "The Caverns":                  "https://www.thecaverns.com/",
    "Cobra":                        "https://cobranashville.com/",
    "Skinny Dennis":                "https://skinnydennisnashville.com/",
    "Fogg Street Lawn Club":        "https://www.foggstreet.live/",
    "Rudy's Jazz Room":             "https://rudysjazzroom.com/",
}


# Ticketmaster venue IDs for Nashville-area venues. Looked up via the TM
# Discovery API venues endpoint (keyword + stateCode=TN).
TICKETMASTER_VENUES = {
    "Bridgestone Arena":            "KovZpZA6taAA",
    "Ryman Auditorium":             "KovZpa61Ge",
    "Ascend Amphitheater":          "KovZpZAEet7A",
    "Grand Ole Opry House":         "KovZpa3Jbe",
    "Brooklyn Bowl Nashville":      "KovZ917APep",
    "FirstBank Amphitheater":       "KovZ917AJek",
    "Nissan Stadium":               "KovZpZA7AnJA",
    "The Pinnacle":                 "KovZ917ARXe",
    "Cannery Hall":                 "KovZ917A_O0",
    "Exit/In":                      "KovZpZAFaFnA",
    "The Basement East":            "KovZ917ACl7",
    "The Basement":                 "KovZpZAkdn6A",
    "Marathon Music Works":         "KovZpZAJnJlA",
    "3rd & Lindsley":               "KovZpZA16IvA",
    "Eastside Bowl":                "Z7r9jZa7r1",
    "Schermerhorn Symphony Center": "KovZpZAEvF7A",
    "TPAC":                         "KovZpZA1nl6A",
}


SPORTS_VENUES = {"Bridgestone Arena", "Nissan Stadium"}

SPORTS_KEYWORDS = COMMON_SPORTS_KEYWORDS + [
    "titans", "predators", "nashville sc", "sounds",
    "vanderbilt", "commodores", "belmont bruins",
]

JUNK_KEYWORDS = COMMON_JUNK_KEYWORDS + [
    "betmgm", "concert upgrade", "event ticket required",
]


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
NON_MUSIC_EXEMPT_ARTISTS = [
    "first aid kit",
    "bingo players",
]
NON_MUSIC_RE = re.compile("|".join(NON_MUSIC_PATTERNS), re.IGNORECASE)


MONTHS_AHEAD = 10


CONFIG = RegionConfig(
    region_key="tn",
    display_name="Nashville Gig List",
    short_title="TN GIG LIST",
    region_label="Nashville",
    venue_urls=VENUE_URLS,
    output_dir=REGION_DIR,
    months_ahead=MONTHS_AHEAD,
)
