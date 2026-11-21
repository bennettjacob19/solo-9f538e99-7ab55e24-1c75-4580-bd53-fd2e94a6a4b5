"""Self-tests for per-site billed download windows.

Covers: opening windows without touching the origin, per-request body-byte
reservations (cache hits and repeats included), 429 before a slow back-fill,
409 for stale windows (including requests that were mid-flight when the window
was replaced), refunds when no 206 is sent, and concurrency accounting.
"""

from __future__ import annotations

import asyncio

import httpx
import pytest
from asgi_lifespan import LifespanManager

from app.common.firmware import FIRMWARE_PATH, VERSIONS, build_firmware
from app.gateway.main import create_app as create_gateway
from app.origin.main import create_app as create_origin

FIRMWARE_LENGTH = 64 * 1024
CHUNK_SIZE = 16 * 1024
SITE = "remote-site-a"


@pytest.fixture
async def clients():
    origin_app = create_origin(firmware_length=FIRMWARE_LENGTH)
    origin_transport = httpx.ASGITransport(app=origin_app)
    gateway_app = create_gateway(
        origin_base_url="http://origin-test",
        chunk_size=CHUNK_SIZE,
        transport=origin_transport,
    )
    async with (
        LifespanManager(origin_app),
        LifespanManager(gateway_app),
        httpx.AsyncClient(transport=origin_transport, base_url="http://origin-test") as origin,
        httpx.AsyncClient(
            transport=httpx.ASGITransport(app=gateway_app),
            base_url="http://gateway-test",
        ) as gateway,
    ):
        yield origin, gateway, gateway_app


async def open_window(gateway, budget, site=SITE):
    resp = await gateway.post(
        "/billing/windows", json={"site": site, "budget_bytes": budget}
    )
    assert resp.status_code == 201, resp.text
    body = resp.json()
    assert resp.headers["X-Download-Window"] == str(body["window_id"])
    return body["window_id"]


async def usage(gateway, site=SITE):
    resp = await gateway.get(f"/billing/windows/{site}")
    assert resp.status_code == 200, resp.text
    return resp.json()


def billed_path(site=SITE, window=None):
    return f"/billing/firmware.bin?site={site}&window={window}"


async def download(gateway, etag, rng, site=SITE, window=None, path=None):
    return await gateway.get(
        path or billed_path(site, window),
        headers={"If-Match": etag, "Range": rng},
    )


# ----------------------------------------------------------------------
# 1. Window opening is origin-free; a download is billed by body bytes
# ----------------------------------------------------------------------

async def test_open_window_does_not_touch_origin(clients):
    origin, gateway, _ = clients
    await origin.post("/admin/reset-stats")
    await origin.post("/admin/delay", json={"delay": 1.0})
    try:
        loop = asyncio.get_running_loop()
        start = loop.time()
        window = await open_window(gateway, 1000)
        elapsed = loop.time() - start
        assert elapsed < 0.5  # no 1s origin delay
        assert isinstance(window, int) and window >= 1
        stats = (await origin.get("/admin/state")).json()["stats"]
        assert stats["range_requests"] == 0
        assert stats["full_requests"] == 0
    finally:
        await origin.post("/admin/delay", json={"delay": 0})


async def test_download_charges_exact_body_bytes(clients):
    origin, gateway, _ = clients
    etag = (await gateway.get("/firmware/meta")).json()["etag"]
    window = await open_window(gateway, 10 * CHUNK_SIZE)

    resp = await download(gateway, etag, "bytes=100-40000", window=window)
    assert resp.status_code == 206
    assert resp.headers["X-Download-Window"] == str(window)
    assert resp.headers["X-Download-Site"] == SITE
    assert resp.content == build_firmware(VERSIONS[0], FIRMWARE_LENGTH)[100:40001]

    reported = await usage(gateway)
    assert reported["window_id"] == window
    assert reported["used_bytes"] == 40000 - 100 + 1
    assert reported["inflight_bytes"] == 0
    assert reported["remaining_bytes"] == 10 * CHUNK_SIZE - (40000 - 100 + 1)


async def test_cache_hit_and_repeat_are_billed(clients):
    origin, gateway, _ = clients
    etag = (await gateway.get("/firmware/meta")).json()["etag"]
    window = await open_window(gateway, 10 * CHUNK_SIZE)
    rng = f"bytes=0-{CHUNK_SIZE - 1}"

    first = await download(gateway, etag, rng, window=window)
    assert first.status_code == 206
    await origin.post("/admin/reset-stats")

    # Slow origin must not delay a cache hit that has budget.
    await origin.post("/admin/delay", json={"delay": 1.0})
    try:
        loop = asyncio.get_running_loop()
        start = loop.time()
        second = await download(gateway, etag, rng, window=window)
        elapsed = loop.time() - start
    finally:
        await origin.post("/admin/delay", json={"delay": 0})
    assert second.status_code == 206
    assert elapsed < 0.5
    assert (await origin.get("/admin/state")).json()["stats"]["range_requests"] == 0

    reported = await usage(gateway)
    # Cache hits and repeated downloads count, exactly like misses.
    assert reported["used_bytes"] == 2 * CHUNK_SIZE


# ----------------------------------------------------------------------
# 2. Budget exhaustion: 429 before a missing segment is back-filled
# ----------------------------------------------------------------------

async def test_insufficient_budget_returns_429_before_backfill(clients):
    origin, gateway, _ = clients
    etag = (await gateway.get("/firmware/meta")).json()["etag"]
    window = await open_window(gateway, 100)  # far less than one chunk
    await origin.post("/admin/reset-stats")
    await origin.post("/admin/delay", json={"delay": 1.0})
    try:
        loop = asyncio.get_running_loop()
        start = loop.time()
        resp = await download(
            gateway, etag, f"bytes=0-{CHUNK_SIZE - 1}", window=window
        )
        elapsed = loop.time() - start
    finally:
        await origin.post("/admin/delay", json={"delay": 0})

    assert resp.status_code == 429
    assert elapsed < 0.5  # rejected before paying the 1s origin delay
    assert (await origin.get("/admin/state")).json()["stats"]["range_requests"] == 0
    reported = await usage(gateway)
    assert reported["used_bytes"] == 0
    assert reported["inflight_bytes"] == 0


async def test_exhausted_window_429_even_on_cache_hit(clients):
    _, gateway, _ = clients
    etag = (await gateway.get("/firmware/meta")).json()["etag"]
    rng = f"bytes=0-{CHUNK_SIZE - 1}"
    window = await open_window(gateway, CHUNK_SIZE)

    ok = await download(gateway, etag, rng, window=window)
    assert ok.status_code == 206
    blocked = await download(gateway, etag, rng, window=window)
    assert blocked.status_code == 429
    assert (await usage(gateway))["used_bytes"] == CHUNK_SIZE


# ----------------------------------------------------------------------
# 3. No 206 => reservation returned; 409 => nothing charged
# ----------------------------------------------------------------------

async def test_failed_request_refunds_reservation(clients):
    origin, gateway, _ = clients
    etag = (await gateway.get("/firmware/meta")).json()["etag"]
    window = await open_window(gateway, 4 * CHUNK_SIZE)

    # Warm the first chunk under the old version.
    warm = await download(gateway, etag, f"bytes=0-{CHUNK_SIZE - 1}", window=window)
    assert warm.status_code == 206
    used_before = (await usage(gateway))["used_bytes"]

    # Switch: a missing old-version segment cannot be completed -> 410 with no
    # body, and the reservation for that body must be returned.
    await origin.post("/admin/switch", json={"version": VERSIONS[1]})
    gone = await download(
        gateway, etag, f"bytes={3 * CHUNK_SIZE}-{4 * CHUNK_SIZE - 1}", window=window
    )
    assert gone.status_code == 410
    reported = await usage(gateway)
    assert reported["used_bytes"] == used_before
    assert reported["inflight_bytes"] == 0

    # A validation failure (416) never reserves either.
    bad = await download(gateway, etag, "bytes=999999-1000000", window=window)
    assert bad.status_code == 416
    assert (await usage(gateway))["inflight_bytes"] == 0


# ----------------------------------------------------------------------
# 4. Window replacement: 409 for stale ids, origin-free, old completion safe
# ----------------------------------------------------------------------

async def test_old_window_id_returns_409(clients):
    origin, gateway, _ = clients
    etag = (await gateway.get("/firmware/meta")).json()["etag"]
    old = await open_window(gateway, 10 * CHUNK_SIZE)
    new = await open_window(gateway, 10 * CHUNK_SIZE)
    assert new != old

    stale = await download(gateway, etag, "bytes=0-99", window=old)
    assert stale.status_code == 409
    assert (await usage(gateway))["window_id"] == new
    assert (await usage(gateway))["used_bytes"] == 0

    fresh = await download(gateway, etag, "bytes=0-99", window=new)
    assert fresh.status_code == 206


async def test_inflight_old_window_request_gets_409_and_leaves_new_window_clean(
    clients,
):
    origin, gateway, _ = clients
    etag = (await gateway.get("/firmware/meta")).json()["etag"]
    old = await open_window(gateway, 10 * CHUNK_SIZE)

    await origin.post("/admin/delay", json={"delay": 0.8})
    try:
        # Start a request that will be waiting on the slow origin...
        task = asyncio.create_task(
            download(
                gateway, etag, f"bytes={2 * CHUNK_SIZE}-{3 * CHUNK_SIZE - 1}",
                window=old,
            )
        )
        await asyncio.sleep(0.2)  # let it reserve and enter the slow fetch
        old_usage = await usage(gateway)
        assert old_usage["inflight_bytes"] == CHUNK_SIZE  # reserved, not yet used

        # ...then open a new window while it is still working.
        loop = asyncio.get_running_loop()
        start = loop.time()
        new = await open_window(gateway, 10 * CHUNK_SIZE)
        assert loop.time() - start < 0.5  # window opening never waits on origin
        assert new != old

        resp = await task
        assert resp.status_code == 409
    finally:
        await origin.post("/admin/delay", json={"delay": 0})

    # The old request's completion must not touch the new window's accounting.
    reported = await usage(gateway)
    assert reported["window_id"] == new
    assert reported["used_bytes"] == 0
    assert reported["inflight_bytes"] == 0

    # The new window's full budget is intact.
    full = await download(
        gateway, etag, f"bytes=0-{10 * CHUNK_SIZE - 1}", window=new
    )
    assert full.status_code == 206


async def test_old_window_completion_after_switch_does_not_change_new_window(
    clients,
):
    """A request that already reserved on the old window releases there only."""
    _, gateway, _ = clients
    etag = (await gateway.get("/firmware/meta")).json()["etag"]

    # Unit-level: a reservation on the old window settles on the old object
    # only, even after a new window exists.
    from app.gateway.billing import BillingLedger

    direct = BillingLedger()
    w1 = direct.open_window(SITE, 100)
    reservation = direct.reserve(SITE, w1.window_id, 40)
    w2 = direct.open_window(SITE, 100)
    reservation.refund()  # old request ends without 206
    assert w2.used_bytes == 0 and w2.inflight_bytes == 0
    assert w1.inflight_bytes == 0  # released on the old window
    assert direct.current_window(SITE) is w2

    # A reservation that reaches 206 after a switch is not committed either.
    w3 = direct.open_window(SITE, 100)
    late = direct.reserve(SITE, w3.window_id, 40)
    w4 = direct.open_window(SITE, 100)
    assert late.commit_if_current() is False
    assert w3.used_bytes == 0 and w4.used_bytes == 0
    assert w3.inflight_bytes == 0

    # And a real request on a real new window is unaffected.
    gateway_new = await open_window(gateway, 10 * CHUNK_SIZE)
    fresh = await download(gateway, etag, "bytes=0-9", window=gateway_new)
    assert fresh.status_code == 206
    assert (await usage(gateway))["used_bytes"] == 10


# ----------------------------------------------------------------------
# 5. Concurrency: no over-commit; every 206 is charged exactly once
# ----------------------------------------------------------------------

async def test_concurrent_downloads_never_overcommit_budget(clients):
    origin, gateway, _ = clients
    etag = (await gateway.get("/firmware/meta")).json()["etag"]
    n = 6
    body_len = CHUNK_SIZE
    budget = 3 * body_len + 10  # room for exactly 3 full chunks
    window = await open_window(gateway, budget)
    rng = f"bytes=0-{CHUNK_SIZE - 1}"

    await origin.post("/admin/delay", json={"delay": 0.2})
    try:
        results = await asyncio.gather(
            *[download(gateway, etag, rng, window=window) for _ in range(n)]
        )
    finally:
        await origin.post("/admin/delay", json={"delay": 0})
    codes = [r.status_code for r in results]
    assert codes.count(206) == 3, codes
    assert codes.count(429) == 3, codes

    reported = await usage(gateway)
    assert reported["used_bytes"] == 3 * body_len
    assert reported["inflight_bytes"] == 0
    assert reported["used_bytes"] <= budget
    # All 206 bodies are byte-correct.
    expected = build_firmware(VERSIONS[0], FIRMWARE_LENGTH)[:body_len]
    for r in results:
        if r.status_code == 206:
            assert r.content == expected


async def test_concurrent_misses_share_origin_but_each_is_billed(clients):
    origin, gateway, _ = clients
    etag = (await gateway.get("/firmware/meta")).json()["etag"]
    window = await open_window(gateway, 100 * CHUNK_SIZE)
    await origin.post("/admin/reset-stats")
    await origin.post("/admin/delay", json={"delay": 0.2})
    try:
        results = await asyncio.gather(
            *[
                download(
                    gateway, etag,
                    f"bytes={CHUNK_SIZE}-{2 * CHUNK_SIZE - 1}", window=window,
                )
                for _ in range(4)
            ]
        )
    finally:
        await origin.post("/admin/delay", json={"delay": 0})
    assert all(r.status_code == 206 for r in results)
    # One upstream fetch (coalesced), but 4 billed responses.
    assert (await origin.get("/admin/state")).json()["stats"]["range_requests"] == 1
    assert (await usage(gateway))["used_bytes"] == 4 * CHUNK_SIZE


# ----------------------------------------------------------------------
# 6. Billing identity plumbing and the /firmware.bin billed mode
# ----------------------------------------------------------------------

async def test_firmware_path_bills_when_site_and_window_given(clients):
    _, gateway, _ = clients
    etag = (await gateway.get("/firmware/meta")).json()["etag"]
    window = await open_window(gateway, 10 * CHUNK_SIZE)
    path = f"{FIRMWARE_PATH}?site={SITE}&window={window}"

    resp = await download(gateway, etag, "bytes=0-9", window=window, path=path)
    assert resp.status_code == 206
    assert (await usage(gateway))["used_bytes"] == 10

    # Same path with an old window id is rejected.
    stale_path = f"{FIRMWARE_PATH}?site={SITE}&window={window}"
    new = await open_window(gateway, 10 * CHUNK_SIZE)
    stale = await download(gateway, etag, "bytes=0-9", window=new, path=stale_path)
    assert stale.status_code == 409


async def test_billed_endpoint_requires_site_and_window(clients):
    _, gateway, _ = clients
    etag = (await gateway.get("/firmware/meta")).json()["etag"]
    headers = {"If-Match": etag, "Range": "bytes=0-9"}
    r = await gateway.get("/billing/firmware.bin", headers=headers)
    assert r.status_code == 400
    r = await gateway.get(
        "/billing/firmware.bin?site=x", headers=headers
    )
    assert r.status_code == 400
    r = await gateway.get(
        "/billing/firmware.bin?window=1", headers=headers
    )
    assert r.status_code == 400
    r = await gateway.get(
        "/billing/firmware.bin?site=x&window=abc", headers=headers
    )
    assert r.status_code == 400


async def test_header_identity_is_accepted(clients):
    _, gateway, _ = clients
    etag = (await gateway.get("/firmware/meta")).json()["etag"]
    window = await open_window(gateway, 10 * CHUNK_SIZE)
    r = await gateway.get(
        "/billing/firmware.bin",
        headers={
            "If-Match": etag,
            "Range": "bytes=0-9",
            "X-Download-Site": SITE,
            "X-Download-Window": str(window),
        },
    )
    assert r.status_code == 206
    assert (await usage(gateway))["used_bytes"] == 10


async def test_usage_for_unknown_site_is_404(clients):
    _, gateway, _ = clients
    r = await gateway.get("/billing/windows/nope")
    assert r.status_code == 404


async def test_send_time_window_switch_turns_206_into_409():
    """If a window is replaced after the body was built but before the 206
    response head is sent, the request must answer 409 and charge nothing."""
    from app.gateway.billing import BillingLedger
    from app.gateway.main import BilledDownloadResponse

    ledger = BillingLedger()
    old = ledger.open_window(SITE, 100)
    reservation = ledger.reserve(SITE, old.window_id, 10)

    response = BilledDownloadResponse(
        content=b"0123456789",
        reservation=reservation,
        ledger=ledger,
        site=SITE,
        window_id=old.window_id,
        headers={"ETag": '"firmware-v1"'},
    )
    new = ledger.open_window(SITE, 100)

    sent: list[dict] = []

    async def receive():
        return {"type": "http.request", "body": b"", "more_body": False}

    async def send(message):
        sent.append(message)

    await response(
        {"type": "http", "method": "GET", "path": "/billing/firmware.bin",
         "headers": []},
        receive,
        send,
    )
    assert sent[0]["type"] == "http.response.start"
    assert sent[0]["status"] == 409
    assert new.used_bytes == 0 and new.inflight_bytes == 0
    assert old.used_bytes == 0 and old.inflight_bytes == 0


async def test_windows_are_isolated_per_site(clients):
    _, gateway, _ = clients
    etag = (await gateway.get("/firmware/meta")).json()["etag"]
    w_a = await open_window(gateway, 1000, site="site-a")
    w_b = await open_window(gateway, 1000, site="site-b")

    await download(gateway, etag, "bytes=0-99", site="site-a", window=w_a)
    assert (await usage(gateway, "site-a"))["used_bytes"] == 100
    assert (await usage(gateway, "site-b"))["used_bytes"] == 0

    # Replacing site-a's window does not make site-b's window stale.
    w_a2 = await open_window(gateway, 1000, site="site-a")
    assert w_a2 != w_a
    ok_b = await download(gateway, etag, "bytes=0-9", site="site-b", window=w_b)
    assert ok_b.status_code == 206
    stale_a = await download(gateway, etag, "bytes=0-9", site="site-a", window=w_a)
    assert stale_a.status_code == 409
    listing = (await gateway.get("/billing/windows")).json()["windows"]
    assert {w["site"] for w in listing} == {"site-a", "site-b"}
