"""Shared giglist package.

Anchors the whole pipeline to US Central time on import. Both MN and TN
sit in Central, and the daily scrape runs on GitHub Actions in UTC — if
the scraper fires after ~7pm Central (past UTC midnight), a UTC-based
`date.today()` returns tomorrow, which silently drops that evening's
shows from things like First Avenue's enrichment cutoff and renderer's
"this week" window. Setting TZ here flows through every date.today()
and datetime.now() call in scrapers, render, and scrape_utils.
"""

import os
import time

os.environ["TZ"] = "America/Chicago"
if hasattr(time, "tzset"):
    time.tzset()
