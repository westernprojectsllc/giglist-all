"""Shared data model and config for the giglist pipeline.

Both scraper.py and render.py import from here, so the Show dataclass
and the venue/URL config live in one place.
"""

from dataclasses import dataclass, field, asdict
from datetime import date
from typing import List, Optional


# How many months ahead to scrape and render. Used by both the scraper
# (to bound month-by-month listings) and the renderer (to cap the
# "weeks ahead" navigation).
MONTHS_AHEAD = 10


# Map venue display name → venue homepage URL. Used by the renderer to
# build the "venue" link on each show row, and by the scrapers to
# normalize venue names.
VENUE_URLS = {
    "First Avenue":           "https://first-avenue.com",
    "7th St Entry":           "https://first-avenue.com/venue/7th-st-entry/",
    "Palace Theatre":         "https://first-avenue.com/venue/palace-theatre/",
    "The Fitzgerald Theater": "https://first-avenue.com/venue/the-fitzgerald-theater/",
    "Fine Line":              "https://first-avenue.com/venue/fine-line/",
    "Turf Club":              "https://first-avenue.com/venue/turf-club/",
    "Amsterdam Bar & Hall":   "https://www.amsterdambar.com/",
    "The Armory":             "https://armorymn.com/",
    "Cedar Cultural Center":  "https://www.thecedar.org",
    "Dakota Jazz Club":       "https://www.dakotacooks.com",
    "Orchestra Hall":         "https://www.minnesotaorchestra.org",
    "Orpheum Theatre":        "https://hennepinarts.org/venues/orpheum-theatre/",
    "State Theatre":          "https://hennepinarts.org/venues/state-theatre/",
    "Xcel Energy Center":     "https://www.xcelenergycenter.com",
    "Roy Wilkins Auditorium": "https://www.rivercentre.org/roy-wilkins-auditorium",
    "Fillmore Minneapolis":   "https://www.fillmoreminneapolis.com",
    "Varsity Theater":        "https://www.varsitytheater.com",
    "Target Center":          "https://www.targetcenter.com",
    "U.S. Bank Stadium":      "https://www.usbankstadium.com",
    "Myth Live":              "https://mythlive.com",
    "Ice House":              "https://www.icehousempls.com/",
    "White Squirrel":         "https://whitesquirrelbar.com/",
    "331 Club":               "https://331club.com/",
    "Skyway Theatre":         "https://skywaytheatre.com/",
    "The Loft at Skyway Theatre": "https://skywaytheatre.com/",
    "Pilllar Forum":          "https://www.pilllar.com/pages/events",
    "Underground Music Venue": "https://www.undergroundmusicvenue.com/events",
    "Zhora Darling":          "https://www.zhoradarling.com/events",
    "Cloudland Theater":      "https://www.cloudlandtheater.com/",
    "The Parkway Theater":    "https://theparkwaytheater.com/live-events",
    "Berlin":                 "https://www.berlinmpls.com/calendar",
    "Uptown VFW":             "https://app.opendate.io/c/uptown-vfw-681",
    "Aster Cafe":             "https://astercafe.com/live-music-calendar/",
}


@dataclass
class Show:
    title: str
    sort_date: date
    venue: str
    url: str = ""
    sold_out: bool = False
    time: Optional[str] = None
    doors: Optional[str] = None
    supports: List[str] = field(default_factory=list)

    def to_json_dict(self) -> dict:
        d = asdict(self)
        d["sort_date"] = self.sort_date.isoformat()
        return d

    @classmethod
    def from_json_dict(cls, d: dict) -> "Show":
        return cls(
            title=d["title"],
            sort_date=date.fromisoformat(d["sort_date"]),
            venue=d["venue"],
            url=d.get("url", ""),
            sold_out=bool(d.get("sold_out", False)),
            time=d.get("time"),
            doors=d.get("doors"),
            supports=list(d.get("supports") or []),
        )
