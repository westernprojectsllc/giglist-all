"""HTTP helpers shared by the region scrapers."""

import time
import requests

USER_AGENT = "Mozilla/5.0"
BROWSER_UA = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36"
)
DEFAULT_HEADERS = {"User-Agent": USER_AGENT}
BROWSER_HEADERS = {"User-Agent": BROWSER_UA}
DEFAULT_TIMEOUT = 15


def get_with_retry(url, *, session=None, headers=None, timeout=DEFAULT_TIMEOUT,
                   retries=3, backoff=0.5, expect_json=False):
    """GET with retries on transient failures and 429 rate-limits.

    Returns the Response (or parsed JSON if expect_json=True). Raises the
    final exception if every attempt fails."""
    getter = session.get if session else requests.get
    headers = headers if headers is not None else DEFAULT_HEADERS
    last_exc = None
    for attempt in range(retries):
        try:
            response = getter(url, headers=headers, timeout=timeout)
            if response.status_code == 429:
                time.sleep(backoff * (attempt + 1))
                continue
            if expect_json:
                return response.json()
            return response
        except Exception as e:
            last_exc = e
            time.sleep(backoff * (attempt + 1))
    if last_exc:
        raise last_exc
    raise RuntimeError(f"Rate-limited after {retries} retries: {url[:80]}")
