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
    "Bridgestone Arena":           "https://www.bridgestonearena.com/",
    "Ryman Auditorium":            "https://ryman.com/",
    "Ascend Amphitheater":         "https://www.ascendamphitheater.com/",
    "Grand Ole Opry House":        "https://www.opry.com/",
    "Brooklyn Bowl Nashville":     "https://www.brooklynbowl.com/nashville",
    "FirstBank Amphitheater":      "https://www.firstbankamphitheater.com/",
    "Nissan Stadium":              "https://nissanstadium.com/",
    "The Pinnacle":                "https://thepinnaclenashville.com/",
    "Cannery Hall":                "https://canneryhall.com/",
    "Exit/In":                     "https://exitin.com/",
    "The Basement East":           "https://thebasementnashville.com/the-basement-east/",
    "The Basement":                "https://thebasementnashville.com/",
    "Marathon Music Works":        "https://marathonmusicworks.com/",
    "3rd & Lindsley":              "https://www.3rdandlindsley.com/",
    "Eastside Bowl":               "https://www.eastsidebowl.com/",
    "Schermerhorn Symphony Center": "https://www.nashvillesymphony.org/",
    "TPAC":                        "https://www.tpac.org/",
    "Station Inn":                 "https://stationinn.com/",
    "Skydeck":                     "https://www.assemblyfoodhall.com/skydeck/",
    "DRKMTTR":                     "https://www.drkmttrcollective.com/",
    "The End":                     "https://endnashville.com/",
    "Night We Met":                "https://nightwemetnashville.com/",
    "The Caverns":                 "https://www.thecaverns.com/",
    "Cobra":                       "https://cobranashville.com/",
    "Skinny Dennis":               "https://skinnydennisnashville.com/",
    "Fogg Street Lawn Club":       "https://www.foggstreet.live/",
    "Rudy's Jazz Room":            "https://rudysjazzroom.com/",
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
