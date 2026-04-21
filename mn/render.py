"""Read shows.json (produced by scraper.py) and render the MN giglist HTML.

Splitting rendering out of the scraper means you can iterate on layout
or styles without re-fetching every venue."""

import json
import sys
from pathlib import Path

# Make the repo-root giglist/ package importable when this script is
# invoked directly from the mn/ working directory (as in CI).
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from giglist.models import Show
from giglist.render import write_site

from config import CONFIG


SHOWS_JSON = CONFIG.output_dir / "shows.json"


def load_shows(path):
    with open(path) as f:
        raw = json.load(f)
    return [Show.from_json_dict(d) for d in raw]


if __name__ == "__main__":
    path = Path(sys.argv[1]) if len(sys.argv) > 1 else SHOWS_JSON
    shows = load_shows(path)
    print(f"Loaded {len(shows)} shows from {path}")
    write_site(CONFIG, shows)
