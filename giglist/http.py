"""HTTP helpers shared by the region scrapers."""

import atexit
import os
import subprocess
import tempfile
import threading
import time
from contextlib import contextmanager
from pathlib import Path
from urllib.parse import urlsplit

import certifi
import requests

USER_AGENT = "Mozilla/5.0"
BROWSER_UA = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36"
)
DEFAULT_HEADERS = {"User-Agent": USER_AGENT}
BROWSER_HEADERS = {"User-Agent": BROWSER_UA}
DEFAULT_TIMEOUT = 15


# ---------- per-host politeness ----------
#
# Venue sites are small, and several of them rate-limit bursts. Firing ten
# simultaneous requests at first-avenue.com reliably 429s one of them, and
# because get_with_retry hands back the final error response rather than
# raising, the caller parsed an error page as "no shows" and silently
# dropped a month (~100 shows) with nothing in the logs.
#
# Rather than leave each scraper to remember its own limit, every fetch
# routes through a per-host gate: at most N concurrent requests to a host,
# and an optional minimum gap between consecutive ones. Scrapers may still
# use whatever pool size suits them; the gate is what actually bounds what
# a host sees, so a new scraper cannot accidentally hammer a site.
#
# Keep these conservative. Getting blocked costs a venue's entire listing,
# while a slower scrape costs seconds on a job that runs once a day.

DEFAULT_HOST_CONCURRENCY = 4
DEFAULT_HOST_INTERVAL = 0.0

# host suffix -> (max concurrent, min seconds between requests)
HOST_LIMITS = {
    # 429s under a 10-way burst; 4 tested clean across all 10 month pages.
    "first-avenue.com": (4, 0.0),
    # Documented limit is 5 requests/sec. Stay well under it.
    "app.ticketmaster.com": (3, 0.25),
    # Small WordPress installs — be gentle, they page a lot.
    "dakotacooks.com": (2, 0.3),
    "stationinn.com": (3, 0.1),
    "whitesquirrelbar.com": (3, 0.1),
    "cobranashville.com": (3, 0.1),
    # Already bot-sensitive; never burst these.
    "thecaverns.com": (1, 0.5),
    "axs.com": (1, 0.5),
    "analognashville.com": (1, 0.5),
}

_host_gates = {}
_host_gates_lock = threading.Lock()


class _HostGate:
    """Bounds concurrency and spacing for one host."""

    def __init__(self, concurrency, interval):
        self.semaphore = threading.BoundedSemaphore(concurrency)
        self.interval = interval
        self.lock = threading.Lock()
        self.next_allowed = 0.0

    def wait_turn(self):
        if self.interval <= 0:
            return
        with self.lock:
            now = time.monotonic()
            delay = max(0.0, self.next_allowed - now)
            self.next_allowed = max(now, self.next_allowed) + self.interval
        if delay:
            time.sleep(delay)


def _limits_for(host):
    for suffix, limits in HOST_LIMITS.items():
        if host == suffix or host.endswith("." + suffix):
            return limits
    return (DEFAULT_HOST_CONCURRENCY, DEFAULT_HOST_INTERVAL)


def _gate_for(url):
    host = (urlsplit(url).hostname or "").lower()
    with _host_gates_lock:
        gate = _host_gates.get(host)
        if gate is None:
            gate = _HostGate(*_limits_for(host))
            _host_gates[host] = gate
        return gate


@contextmanager
def host_slot(url):
    """Hold a concurrency slot for ``url``'s host for the request's duration."""
    gate = _gate_for(url)
    gate.semaphore.acquire()
    try:
        gate.wait_turn()
        yield
    finally:
        gate.semaphore.release()


def retry_after_seconds(response, default):
    """Honour a 429/503 Retry-After header when the server sends one.

    Capped so a server asking for a long pause cannot stall the whole run.
    """
    raw = (response.headers.get("Retry-After") or "").strip()
    if raw:
        try:
            return max(0.0, min(float(raw), 30.0))
        except ValueError:
            pass
    return default


class RetriesExhausted(RuntimeError):
    """Every attempt returned 429/5xx.

    get_with_retry returns the last error response by default, which is
    fine for callers that just yield zero shows. Callers that would
    otherwise mistake an error payload for "no more results" pass
    raise_on_exhausted=True and get this instead.
    """

EXTRA_CA_DIR = Path(__file__).resolve().parent / "certs"
_extra_ca_bundle = None


def ca_bundle_with_extras():
    """Path to a CA bundle of certifi plus giglist/certs/*.pem.

    Some venues serve a chain that only verifies if the client chases the
    intermediate's authorityInfoAccess URI to a cross-signed root. macOS
    curl does that; OpenSSL and Python's ssl module do not, so those hosts
    fail with CERTIFICATE_VERIFY_FAILED (curl exit 60) on the Linux CI
    runners while working fine on a developer laptop. Shipping the
    cross-signs in giglist/certs and appending them to certifi's bundle
    makes verification platform-independent. Each PEM documents what it is
    and why; see giglist/certs/ for provenance.

    The combined bundle is written to a temp file once per process and
    removed at exit.
    """
    global _extra_ca_bundle
    if _extra_ca_bundle is not None:
        return _extra_ca_bundle

    extras = sorted(EXTRA_CA_DIR.glob("*.pem")) if EXTRA_CA_DIR.is_dir() else []
    if not extras:
        _extra_ca_bundle = certifi.where()
        return _extra_ca_bundle

    fd, path = tempfile.mkstemp(prefix="giglist-ca-", suffix=".pem")
    with os.fdopen(fd, "w", encoding="utf-8") as out:
        out.write(Path(certifi.where()).read_text(encoding="utf-8"))
        for pem in extras:
            out.write("\n")
            out.write(pem.read_text(encoding="utf-8"))

    atexit.register(lambda: Path(path).unlink(missing_ok=True))
    _extra_ca_bundle = path
    return _extra_ca_bundle


def get_with_retry(url, *, session=None, headers=None, params=None,
                   timeout=DEFAULT_TIMEOUT, retries=3, backoff=0.5,
                   expect_json=False, verify=None, raise_on_exhausted=False):
    """GET with retries on transient failures: exceptions, 429
    rate-limits, and 5xx responses.

    Returns the Response (or parsed JSON if expect_json=True). Raises the
    final exception if every attempt raised; returns the last response if
    attempts got responses but all were 429/5xx (callers that parse an
    error page just yield zero shows, which the dropout guard surfaces).

    Pass raise_on_exhausted=True to get a RetriesExhausted instead of that
    final error response. Use it wherever an error payload is
    indistinguishable from a legitimately empty one — a paginator that
    stops on "no results" would otherwise read a 429 as the end of the
    list and silently truncate.

    Every request passes through the host gate (see HOST_LIMITS), so
    concurrency and spacing are bounded per host no matter how wide the
    caller's thread pool is. 429/503 responses honour Retry-After and back
    off exponentially."""
    getter = session.get if session else requests.get
    headers = headers if headers is not None else DEFAULT_HEADERS
    last_exc = None
    last_response = None
    for attempt in range(retries):
        try:
            kwargs = {"headers": headers, "params": params, "timeout": timeout}
            if verify is not None:
                kwargs["verify"] = verify
            with host_slot(url):
                response = getter(url, **kwargs)
            if response.status_code == 429 or response.status_code >= 500:
                last_response = response
                # Exponential, not linear: a host that is already shedding
                # load should see us retreat, not keep a steady drumbeat.
                time.sleep(retry_after_seconds(response, backoff * (2 ** attempt)))
                continue
            if expect_json:
                return response.json()
            return response
        except Exception as e:
            last_exc = e
            time.sleep(backoff * (2 ** attempt))
    if last_response is not None:
        if raise_on_exhausted:
            raise RetriesExhausted(
                f"HTTP {last_response.status_code} after {retries} attempts: {url}"
            )
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
            with host_slot(url):
                out = subprocess.run(
                    cmd, capture_output=True, timeout=timeout + 5, check=True,
                )
            return out.stdout.decode("utf-8", errors="replace")
        except Exception as e:
            last_exc = e
            time.sleep(0.5 * (2 ** attempt))
    raise last_exc


def cffi_get_json(url, *, timeout=DEFAULT_TIMEOUT, retries=3, backoff=0.5,
                  referer=None, impersonate="chrome124"):
    """GET JSON from a host guarded by a Cloudflare *managed challenge* —
    the kind that 403s python-requests AND the plain curl binary alike
    (e.g. AXS's unifiedapisearch Discovery API). curl_cffi replays a real
    Chrome TLS/HTTP2 (JA3) fingerprint, which the challenge accepts with
    no JS solving. Use only where curl_get_text also gets a 403.

    curl_cffi is imported lazily so a missing wheel only breaks the one
    scraper that needs it, not every scraper that imports this module."""
    from curl_cffi import requests as cffi  # heavy optional dep; lazy

    headers = {"Accept": "application/json"}
    if referer:
        headers["Referer"] = referer
    last_exc = None
    for attempt in range(retries):
        try:
            with host_slot(url):
                r = cffi.get(url, headers=headers, timeout=timeout,
                             impersonate=impersonate)
            if r.status_code == 429 or r.status_code >= 500:
                time.sleep(retry_after_seconds(r, backoff * (2 ** attempt)))
                continue
            r.raise_for_status()
            return r.json()
        except Exception as e:
            last_exc = e
            time.sleep(backoff * (2 ** attempt))
    raise last_exc
