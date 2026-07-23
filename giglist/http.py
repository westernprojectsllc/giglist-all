"""HTTP helpers shared by the region scrapers."""

import subprocess
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


def get_with_retry(url, *, session=None, headers=None, params=None,
                   timeout=DEFAULT_TIMEOUT, retries=3, backoff=0.5,
                   expect_json=False):
    """GET with retries on transient failures: exceptions, 429
    rate-limits, and 5xx responses.

    Returns the Response (or parsed JSON if expect_json=True). Raises the
    final exception if every attempt raised; returns the last response if
    attempts got responses but all were 429/5xx (callers that parse an
    error page just yield zero shows, which the dropout guard surfaces)."""
    getter = session.get if session else requests.get
    headers = headers if headers is not None else DEFAULT_HEADERS
    last_exc = None
    last_response = None
    for attempt in range(retries):
        try:
            response = getter(url, headers=headers, params=params, timeout=timeout)
            if response.status_code == 429 or response.status_code >= 500:
                last_response = response
                time.sleep(backoff * (attempt + 1))
                continue
            if expect_json:
                return response.json()
            return response
        except Exception as e:
            last_exc = e
            time.sleep(backoff * (attempt + 1))
    if last_response is not None:
        return last_response.json() if expect_json else last_response
    raise last_exc


def curl_get_text(url, *, timeout=DEFAULT_TIMEOUT, retries=2):
    """Fetch a page via the system curl binary.

    Some Cloudflare-fronted sites (e.g. analognashville.com) 403 every
    python-requests call regardless of headers — they fingerprint the
    TLS handshake — but accept curl. Used only where requests cannot
    get through; curl ships on macOS and the ubuntu CI runners."""
    cmd = [
        "curl", "-sS", "--compressed", "--max-time", str(timeout),
        "-A", BROWSER_UA, url,
    ]
    last_exc = None
    for attempt in range(retries):
        try:
            out = subprocess.run(
                cmd, capture_output=True, timeout=timeout + 5, check=True,
            )
            return out.stdout.decode("utf-8", errors="replace")
        except Exception as e:
            last_exc = e
            time.sleep(0.5 * (attempt + 1))
    raise last_exc
