"""Tests for the shared HTTP layer: per-host politeness and the
error-vs-empty distinction.

These matter because both failure modes they guard were silent. A burst
of ten simultaneous requests 429'd first-avenue.com, and get_with_retry
handed the 429 body back as if it were data, so a whole month of shows
parsed to zero with nothing in the logs.

No network: a stub getter stands in for requests.get.
"""

import threading
import time

import pytest

from giglist import http as H


class FakeResponse:
    def __init__(self, status_code=200, headers=None, payload=None):
        self.status_code = status_code
        self.headers = headers or {}
        self._payload = payload if payload is not None else {}
        self.text = "ok"

    def json(self):
        return self._payload


# ---------- host limit lookup ----------

@pytest.mark.parametrize("host,expected", [
    ("first-avenue.com", (4, 0.0)),
    ("www.first-avenue.com", (4, 0.0)),      # subdomains inherit
    ("app.ticketmaster.com", (3, 0.25)),
    ("www.thecaverns.com", (1, 0.5)),
    ("example.org", (H.DEFAULT_HOST_CONCURRENCY, H.DEFAULT_HOST_INTERVAL)),
])
def test_limits_for_host(host, expected):
    assert H._limits_for(host) == expected


def test_unrelated_host_does_not_match_by_substring():
    """"notfirst-avenue.com.evil.test" must not inherit first-avenue's slot."""
    assert H._limits_for("evil-first-avenue.com.test") == (
        H.DEFAULT_HOST_CONCURRENCY, H.DEFAULT_HOST_INTERVAL,
    )


# ---------- concurrency gate ----------

def test_host_slot_bounds_concurrency(monkeypatch):
    monkeypatch.setattr(H, "_host_gates", {})
    monkeypatch.setitem(H.HOST_LIMITS, "gate.test", (2, 0.0))

    peak = 0
    current = 0
    lock = threading.Lock()

    def worker():
        nonlocal peak, current
        with H.host_slot("https://gate.test/x"):
            with lock:
                current += 1
                peak = max(peak, current)
            time.sleep(0.05)
            with lock:
                current -= 1

    threads = [threading.Thread(target=worker) for _ in range(8)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    assert peak <= 2, f"gate allowed {peak} concurrent requests, limit was 2"


def test_host_slot_spaces_requests(monkeypatch):
    monkeypatch.setattr(H, "_host_gates", {})
    monkeypatch.setitem(H.HOST_LIMITS, "spaced.test", (1, 0.05))

    start = time.monotonic()
    for _ in range(3):
        with H.host_slot("https://spaced.test/x"):
            pass
    # Three requests at 50ms spacing cannot finish instantly.
    assert time.monotonic() - start >= 0.08


# ---------- Retry-After ----------

@pytest.mark.parametrize("header,default,expected", [
    ({"Retry-After": "2"}, 9.0, 2.0),
    ({"Retry-After": "not-a-number"}, 1.5, 1.5),   # fall back
    ({}, 1.5, 1.5),                                # absent
    ({"Retry-After": "9999"}, 1.0, 30.0),          # capped
])
def test_retry_after_seconds(header, default, expected):
    assert H.retry_after_seconds(FakeResponse(429, header), default) == expected


# ---------- error vs empty ----------

def _stub_getter(responses, monkeypatch):
    calls = {"n": 0}

    def fake_get(url, **kwargs):
        r = responses[min(calls["n"], len(responses) - 1)]
        calls["n"] += 1
        return r

    monkeypatch.setattr(H.requests, "get", fake_get)
    monkeypatch.setattr(H.time, "sleep", lambda *_: None)
    return calls


def test_returns_last_error_response_by_default(monkeypatch):
    """Default behaviour is unchanged: callers that just yield zero shows
    keep getting the error response rather than an exception."""
    _stub_getter([FakeResponse(429)], monkeypatch)
    result = H.get_with_retry("https://plain.test/x", retries=2)
    assert result.status_code == 429


def test_raise_on_exhausted_turns_a_429_into_an_error(monkeypatch):
    _stub_getter([FakeResponse(429)], monkeypatch)
    with pytest.raises(H.RetriesExhausted):
        H.get_with_retry("https://plain.test/x", retries=2,
                         raise_on_exhausted=True)


def test_raise_on_exhausted_does_not_fire_on_success(monkeypatch):
    _stub_getter([FakeResponse(200, payload={"ok": True})], monkeypatch)
    data = H.get_with_retry("https://plain.test/x", expect_json=True,
                            raise_on_exhausted=True)
    assert data == {"ok": True}


def test_retries_then_succeeds(monkeypatch):
    calls = _stub_getter(
        [FakeResponse(503), FakeResponse(200, payload={"ok": 1})], monkeypatch,
    )
    data = H.get_with_retry("https://plain.test/x", expect_json=True,
                            raise_on_exhausted=True)
    assert data == {"ok": 1}
    assert calls["n"] == 2
