"""FastAPI download gateway with segmented, version-aware caching.

Flow for a terminal download request:

1. The terminal learns the current firmware's strong ETag and length from
   ``GET /firmware/meta`` (visible on the ``/docs`` page).
2. It requests a byte interval with ``If-Match: <ETag>`` plus ``Range``.
3. The gateway answers 206 from cached segments, issuing HTTP Range requests
   to the origin only for missing chunks. If a missing chunk can no longer be
   completed for that ETag (the origin switched version), the gateway answers
   **410 before any body bytes are sent**, so the terminal can re-fetch the
   current version metadata.
"""

from __future__ import annotations

import json
import os
from contextlib import asynccontextmanager
from dataclasses import dataclass

import httpx
from fastapi import FastAPI, Query, Request, Response
from pydantic import BaseModel, Field

from app.common.firmware import (
    FIRMWARE_PATH,
    ByteRange,
    MalformedPrecondition,
    RangeNotSatisfiable,
    content_range_header,
    parse_if_match,
    parse_range_header,
)
from app.gateway.billing import (
    BillingLedger,
    BudgetExceeded,
    Reservation,
    StaleWindow,
)
from app.gateway.cache import SegmentCache, VersionGone

ORIGIN_BASE_URL = os.getenv("ORIGIN_BASE_URL", "http://origin:8080")
CHUNK_SIZE = int(os.getenv("GATEWAY_CHUNK_SIZE", str(256 * 1024)))
REQUEST_TIMEOUT = float(os.getenv("GATEWAY_TIMEOUT", "30"))


class UpstreamError(Exception):
    def __init__(self, status_code: int, detail: str) -> None:
        super().__init__(detail)
        self.status_code = status_code
        self.detail = detail


@dataclass
class PreparedDownload:
    """Everything needed to emit a 206, computed before any body is sent."""

    etag: str
    length: int
    byte_range: ByteRange


def _error(status: int, detail: str, headers: dict[str, str] | None = None) -> Response:
    return Response(
        content=json.dumps({"detail": detail}),
        status_code=status,
        media_type="application/json",
        headers=headers,
    )


def _gone(message: str, etag: str) -> Response:
    return Response(
        content=json.dumps({"detail": 410, "message": message}),
        status_code=410,
        media_type="application/json",
        headers={"ETag": etag},
    )


def _stale_window_response(exc: StaleWindow) -> Response:
    headers = {"X-Download-Site": exc.site}
    if exc.current_window_id is not None:
        headers["X-Download-Window"] = str(exc.current_window_id)
    return _error(
        409,
        f"stale download window: {exc}",
        headers=headers,
    )


class _WindowSupersededDuringSend(Exception):
    """The window was replaced between building the 206 and sending its head."""


class BilledDownloadResponse(Response):
    """A 206 whose reservation is committed only when the head is sent.

    The reservation stays in the window's ``inflight_bytes`` while the body is
    being assembled.  At the moment the 206 response headers are handed to the
    server the reservation moves to ``used_bytes``.  If the window has been
    superseded by then (a new window was opened while this request was still
    working), the request is answered with 409 instead and nothing is charged
    to either window.  If the request ends before any 206 head is sent, the
    reservation is released.
    """

    def __init__(
        self,
        *,
        content: bytes,
        reservation: Reservation,
        ledger: BillingLedger,
        site: str,
        window_id: int,
        **kwargs,
    ) -> None:
        super().__init__(content=content, status_code=206, **kwargs)
        self.reservation = reservation
        self.ledger = ledger
        self.site = site
        self.window_id = window_id

    async def __call__(self, scope, receive, send) -> None:
        committed = False

        async def tracked_send(message) -> None:
            nonlocal committed
            sending_head = message["type"] == "http.response.start" and not committed
            if sending_head:
                if not self.reservation.commit_if_current():
                    raise _WindowSupersededDuringSend
                committed = True
            try:
                await send(message)
            except BaseException:
                if sending_head:
                    # The 206 head never reached the client: do not charge it.
                    self.reservation.uncommit()
                raise

        try:
            await super().__call__(scope, receive, tracked_send)
        except _WindowSupersededDuringSend:
            current = self.ledger.current_window(self.site)
            await _stale_window_response(
                StaleWindow(
                    self.site,
                    self.window_id,
                    current.window_id if current else None,
                )
            )(scope, receive, send)
        except BaseException:
            self.reservation.refund()
            raise


class WindowRequest(BaseModel):
    site: str = Field(min_length=1, description="Paying site identifier")
    budget_bytes: int = Field(
        ge=0, description="Total response body bytes allowed in this window"
    )


def _billing_identity(
    request: Request,
) -> tuple[str | None, int | None, Response | None]:
    """Extract ``(site, window_id)`` from query parameters or headers.

    Both values are optional here; callers decide whether they are required
    and whether a half-specified pair is an error.
    """
    site = (
        request.query_params.get("site")
        or request.headers.get("x-download-site")
        or request.headers.get("x-site")
    )
    raw_window = (
        request.query_params.get("window")
        or request.query_params.get("window_id")
        or request.headers.get("x-download-window")
        or request.headers.get("x-billing-window")
        or request.headers.get("x-window-id")
    )
    if site is not None:
        site = site.strip()
        if not site:
            return None, None, _error(400, "site must be a non-empty string")
    window_id: int | None = None
    if raw_window is not None:
        try:
            window_id = int(raw_window)
        except ValueError:
            return None, None, _error(400, f"invalid window id: {raw_window!r}")
        if window_id < 1:
            return None, None, _error(400, f"invalid window id: {raw_window!r}")
    return site, window_id, None


def create_app(
    origin_base_url: str = ORIGIN_BASE_URL,
    chunk_size: int = CHUNK_SIZE,
    transport: httpx.AsyncBaseTransport | None = None,
) -> FastAPI:
    cache = SegmentCache(chunk_size=chunk_size)
    billing = BillingLedger()

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        client = httpx.AsyncClient(
            base_url=origin_base_url,
            timeout=REQUEST_TIMEOUT,
            transport=transport,
        )
        app.state.http = client
        app.state.cache = cache
        try:
            yield
        finally:
            await client.aclose()

    app = FastAPI(
        title="Firmware download gateway",
        version="1.1.0",
        description=(
            "Segmented firmware download gateway. Call `GET /firmware/meta` "
            "for the current strong ETag and length, then request byte "
            "intervals with `If-Match` + `Range`.\n\n"
            "**Billed downloads (per remote site):** open a window with "
            "`POST /billing/windows` (site + total response body-byte budget), "
            "then download from `GET /billing/firmware.bin?site=<site>"
            "&window=<id>` with `If-Match` + `Range`. Each request reserves the "
            "exact body bytes it will return before any missing segment is "
            "fetched; cache hits and repeats are billed too. Insufficient "
            "budget answers 429 before the back-fill, a stale window id "
            "answers 409, and a 206 head marks the reservation as used. "
            "Inspect `GET /billing/windows/{site}` for used/in-flight bytes."
        ),
        lifespan=lifespan,
    )

    async def fetch_current_meta() -> tuple[str, int]:
        """Fresh metadata straight from the origin (never cached)."""
        client: httpx.AsyncClient = app.state.http
        resp = await client.head(FIRMWARE_PATH)
        if resp.status_code != 200:
            raise UpstreamError(502, f"origin HEAD failed: {resp.status_code}")
        etag = resp.headers.get("ETag")
        length_text = resp.headers.get("Content-Length")
        if not etag or not length_text:
            raise UpstreamError(502, "origin HEAD missing ETag/Content-Length")
        cache.remember_length(etag, int(length_text))
        return etag, int(length_text)

    async def ensure_length(etag: str) -> int:
        length = cache.known_length(etag)
        if length is not None:
            return length
        # Unknown ETag: ask the origin once, protected by a per-ETag lock.
        async with cache.meta_lock_for(etag):
            length = cache.known_length(etag)
            if length is not None:
                return length
            client: httpx.AsyncClient = app.state.http
            resp = await client.get(
                FIRMWARE_PATH,
                headers={"If-Match": etag, "Range": "bytes=0-0"},
            )
            if resp.status_code == 412:
                raise VersionGone(f"origin does not serve {etag} anymore")
            if resp.status_code not in (200, 206):
                raise UpstreamError(502, f"origin probe failed: {resp.status_code}")
            if resp.headers.get("ETag") != etag:
                # Defensive: origin answered a different representation.
                raise VersionGone(f"ETag mismatch probing {etag}")
            if resp.status_code == 206:
                cr = resp.headers.get("Content-Range", "")
                total = int(cr.rsplit("/", 1)[1])
            else:
                total = int(resp.headers["Content-Length"])
            cache.remember_length(etag, total)
            # Keep the probe's payload if useful (first byte of the blob).
            return total

    async def origin_fetch(etag: str, chunk: ByteRange) -> tuple[bytes, int]:
        """Perform exactly one upstream Range request for one missing chunk."""
        client: httpx.AsyncClient = app.state.http
        resp = await client.get(
            FIRMWARE_PATH,
            headers={
                "If-Match": etag,
                "Range": f"bytes={chunk.start}-{chunk.end}",
            },
        )
        if resp.status_code == 412:
            raise VersionGone(f"origin no longer serves {etag}")
        if resp.status_code == 416:
            raise UpstreamError(502, "origin reported range not satisfiable")
        if resp.status_code != 206:
            raise UpstreamError(502, f"unexpected origin status {resp.status_code}")
        if resp.headers.get("ETag") != etag:
            raise VersionGone(f"origin returned wrong ETag for {etag}")
        content_range = resp.headers.get("Content-Range", "")
        try:
            span, total_text = content_range.split("/", 1)
            start_text, end_text = span.replace("bytes ", "").split("-")
            start, end, total = int(start_text), int(end_text), int(total_text)
        except (ValueError, IndexError):
            raise UpstreamError(502, "origin sent malformed Content-Range")
        if start != chunk.start or end > chunk.end or len(resp.content) != end - start + 1:
            raise UpstreamError(502, "origin range does not match the request")
        return resp.content, total

    @app.get("/", include_in_schema=False)
    def root() -> dict:
        return {
            "service": "firmware-gateway",
            "origin": origin_base_url,
            "chunk_size": cache.chunk_size,
            "docs": "/docs",
            "meta_endpoint": "/firmware/meta",
            "billing": {
                "open_window": "POST /billing/windows",
                "download": "/billing/firmware.bin?site=<site>&window=<id>",
                "usage": "GET /billing/windows/{site}",
            },
        }

    @app.get("/health")
    async def health() -> Response:
        try:
            client: httpx.AsyncClient = app.state.http
            resp = await client.get("/health")
            upstream = "ok" if resp.status_code == 200 else "degraded"
        except httpx.HTTPError:
            upstream = "unreachable"
        return Response(
            content=f'{{"status":"ok","origin":"{upstream}"}}',
            media_type="application/json",
        )

    @app.get("/firmware/meta", tags=["firmware"])
    async def firmware_meta() -> dict:
        """Current firmware metadata: strong ETag, length and download path."""
        etag, length = await fetch_current_meta()
        return {
            "etag": etag,
            "length": length,
            "firmware_path": FIRMWARE_PATH,
            "usage": "send If-Match: <etag> and a Range header to the firmware path",
        }

    async def prepare_download(
        request: Request,
        if_match_header: str | None = None,
        range_header: str | None = None,
    ) -> PreparedDownload | Response:
        """Validate method-independent download headers without fetching data.

        Every failure here happens before a single body byte is produced.
        """
        if if_match_header is None:
            if_match_header = request.headers.get("if-match")
        if range_header is None:
            range_header = request.headers.get("range")
        try:
            if_match = parse_if_match(if_match_header)
        except MalformedPrecondition as exc:
            return _error(400, f"malformed If-Match: {exc}")
        if if_match is None:
            return _error(400, "If-Match header with a strong ETag is required")

        if range_header is None:
            return _error(400, "Range header is required (segmented download only)")

        # Resolve the requested ETag ("*" means whatever the origin serves now).
        if if_match == ["*"]:
            try:
                etag, _ = await fetch_current_meta()
            except UpstreamError as exc:
                return _error(exc.status_code, f"upstream error: {exc.detail}")
        elif len(if_match) == 1:
            etag = if_match[0]  # a single strong ETag is the supported case
        else:
            return _error(400, "exactly one ETag (or *) is supported in If-Match")

        # All failure checks happen before any body bytes are produced.
        try:
            length = await ensure_length(etag)
        except VersionGone:
            return _gone(
                "requested firmware version is no longer available; "
                "re-fetch /firmware/meta",
                etag,
            )
        except UpstreamError as exc:
            return _error(exc.status_code, f"upstream error: {exc.detail}")

        try:
            byte_range = parse_range_header(range_header, length)
        except RangeNotSatisfiable as exc:
            return Response(
                status_code=416,
                headers={"Content-Range": f"bytes */{exc.size}", "ETag": etag},
            )
        if byte_range is None:
            return _error(400, "unsupported or malformed Range header")

        return PreparedDownload(etag=etag, length=length, byte_range=byte_range)

    async def read_download_body(
        prepared: PreparedDownload,
    ) -> bytes | Response:
        etag, byte_range = prepared.etag, prepared.byte_range

        async def fetch(chunk: ByteRange) -> tuple[bytes, int]:
            return await origin_fetch(etag, chunk)

        try:
            return await cache.read_range(etag, byte_range, fetch)
        except VersionGone:
            return _gone(
                "missing segment cannot be completed after origin version "
                "switch; re-fetch /firmware/meta",
                etag,
            )
        except UpstreamError as exc:
            return _error(exc.status_code, f"upstream error: {exc.detail}")

    def download_headers(prepared: PreparedDownload, body_len: int) -> dict[str, str]:
        return {
            "ETag": prepared.etag,
            "Content-Type": "application/octet-stream",
            "Content-Length": str(body_len),
            "Content-Range": content_range_header(prepared.byte_range, prepared.length),
            "Accept-Ranges": "bytes",
            "Cache-Control": "no-cache",
        }

    async def billed_download(
        request: Request, site: str, window_id: int
    ) -> Response:
        # 1. Stale window ids are rejected before touching cache or origin.
        try:
            billing.require_current(site, window_id)
        except StaleWindow as exc:
            return _stale_window_response(exc)

        prepared = await prepare_download(request)
        if isinstance(prepared, Response):
            return prepared

        # 2. Reserve exactly the body bytes this response would return.  This
        #    happens before any missing segment is fetched, and cache hits are
        #    billed identically.  Reservation is atomic (no await in between).
        body_len = prepared.byte_range.length
        try:
            reservation = billing.reserve(site, window_id, body_len)
        except StaleWindow as exc:
            return _stale_window_response(exc)
        except BudgetExceeded as exc:
            return _error(
                429,
                f"site {site!r} window {window_id} budget exhausted: "
                f"{exc.window.remaining_bytes} bytes remaining, "
                f"{body_len} bytes requested",
                headers={
                    "X-Download-Site": site,
                    "X-Download-Window": str(window_id),
                    "X-Budget-Remaining": str(exc.window.remaining_bytes),
                    "X-Budget-Requested": str(body_len),
                },
            )

        # 3. Assemble the body.  The reservation is held in inflight_bytes for
        #    the whole time, including slow origin back-fills.  Any exit before
        #    a 206 head is sent releases it (refund is idempotent).
        try:
            body = await read_download_body(prepared)
        except BaseException:
            reservation.refund()
            raise

        # 4. If the window was replaced while we were working, this request
        #    must answer 409 (even if the body build also failed) and charge
        #    nothing to either the old or the new window.  The response object
        #    repeats the check atomically at send time.
        if not billing.is_current(reservation.window):
            reservation.refund()
            current = billing.current_window(site)
            return _stale_window_response(
                StaleWindow(site, window_id, current.window_id if current else None)
            )
        if isinstance(body, Response):
            reservation.refund()
            return body
        if len(body) != body_len:
            # Defensive: never bill a body that disagrees with Content-Range.
            reservation.refund()
            return _error(
                502,
                f"internal body length mismatch: expected {body_len}, got "
                f"{len(body)}",
            )

        return BilledDownloadResponse(
            content=body,
            reservation=reservation,
            ledger=billing,
            site=site,
            window_id=window_id,
            headers={
                **download_headers(prepared, len(body)),
                "X-Download-Site": site,
                "X-Download-Window": str(window_id),
            },
        )

    async def plain_download(request: Request) -> Response:
        prepared = await prepare_download(request)
        if isinstance(prepared, Response):
            return prepared
        body = await read_download_body(prepared)
        if isinstance(body, Response):
            return body
        return Response(
            content=body,
            status_code=206,
            headers=download_headers(prepared, len(body)),
        )

    @app.post("/billing/windows", tags=["billing"], status_code=201)
    async def open_billing_window(body: WindowRequest) -> Response:
        """Open a new download window for a site (no origin round-trip).

        Fill in ``site`` and the total response body-byte budget
        (``budget_bytes``). The window number is returned in the body and in
        ``X-Download-Window``.

        Opening a window supersedes the site's previous window immediately:
        requests still carrying the old window number get 409, including old
        requests that were mid-flight and had not yet sent a 206 head.
        """
        window = billing.open_window(body.site, body.budget_bytes)
        payload = json.dumps(
            {
                **window.snapshot(),
                "download_path": "/billing/firmware.bin",
                "usage": (
                    "GET /billing/firmware.bin?site=<site>&window=<window_id> "
                    "with If-Match and Range headers"
                ),
            }
        )
        return Response(
            content=payload,
            status_code=201,
            media_type="application/json",
            headers={"X-Download-Window": str(window.window_id)},
        )

    @app.get("/billing/windows/{site}", tags=["billing"])
    async def window_usage(site: str) -> Response:
        """Current window usage for a site: used and in-flight body bytes."""
        window = billing.current_window(site)
        if window is None:
            return _error(404, f"no download window for site {site!r}")
        return Response(
            content=json.dumps(window.snapshot()),
            media_type="application/json",
            headers={"X-Download-Window": str(window.window_id)},
        )

    @app.get("/billing/windows", tags=["billing"])
    async def list_windows() -> dict:
        """All sites' current windows and their byte accounting."""
        return {"windows": billing.snapshots()}

    @app.get(FIRMWARE_PATH, tags=["firmware"])
    async def download(request: Request) -> Response:
        """Download a byte range (billed when ``site`` + ``window`` are given)."""
        site, window_id, error = _billing_identity(request)
        if error is not None:
            return error
        if site is None and window_id is None:
            return await plain_download(request)
        if site is None or window_id is None:
            return _error(
                400,
                "billing requires both the site and the window id "
                "(query params site/window or headers X-Download-Site/"
                "X-Download-Window)",
            )
        return await billed_download(request, site, window_id)

    @app.get("/billing/firmware.bin", tags=["billing"])
    async def billed_download_entry(
        request: Request,
        site: str | None = Query(
            default=None, description="Paying site identifier"
        ),
        window: str | None = Query(
            default=None,
            description="Window number returned by POST /billing/windows",
        ),
    ) -> Response:
        """Billed download entry: requires ``site`` + ``window`` and bills bytes.

        Uses exactly the same cache and origin path as ``/firmware.bin``.
        Headers are the same: ``If-Match: <strong ETag>`` + ``Range``.
        ``site``/``window`` may also be sent as ``X-Download-Site`` /
        ``X-Download-Window`` headers.
        """
        del site, window  # declared for /docs; parsed uniformly below
        site_value, window_id, error = _billing_identity(request)
        if error is not None:
            return error
        if site_value is None or window_id is None:
            return _error(
                400,
                "billed download requires the site and the window id "
                "(query params site/window or headers X-Download-Site/"
                "X-Download-Window)",
            )
        return await billed_download(request, site_value, window_id)

    @app.get("/cache/stats", tags=["observability"])
    def cache_stats() -> dict:
        return cache.stats_snapshot()

    return app


app = create_app()
