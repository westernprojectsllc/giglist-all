"""Minnesota-specific config consumed by both scraper.py and render.py."""

from pathlib import Path

from giglist.render import RegionConfig
from giglist.scrape_utils import COMMON_JUNK_KEYWORDS, COMMON_SPORTS_KEYWORDS


REGION_DIR = Path(__file__).parent


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


TICKETMASTER_VENUES = {
    "Orpheum Theatre":        "KovZpakSUe",
    "State Theatre":          "KovZpZAF76tA",
    "Xcel Energy Center":     "Za5ju3rKuqZDd2d33RAGt6algGyxXPO0TZ",
    "Roy Wilkins Auditorium": "KovZpZAF7IAA",
    "Fillmore Minneapolis":   "KovZ917AxCO",
    "Varsity Theater":        "KovZpa3eBe",
    "Target Center":          "KovZpZAE7evA",
    "U.S. Bank Stadium":      "KovZpZAF6ttA",
}


SPORTS_VENUES = {"Target Center", "U.S. Bank Stadium"}

SPORTS_KEYWORDS = COMMON_SPORTS_KEYWORDS + [
    "timberwolves", "wolves", "lynx", "twins", "vikings", "wild",
    "minnesota united", "loons", "bulldogs", "gophers",
    "umd hockey",
]

JUNK_KEYWORDS = list(COMMON_JUNK_KEYWORDS)


MONTHS_AHEAD = 10


CONFIG = RegionConfig(
    region_key="mn",
    display_name="Minnesota Gig List",
    short_title="MN GIG LIST",
    region_label="Minnesota",
    venue_urls=VENUE_URLS,
    output_dir=REGION_DIR,
    months_ahead=MONTHS_AHEAD,
)
