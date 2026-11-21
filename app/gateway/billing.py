"""Per-site body-byte budgets for billed downloads.

A *site* (a remote site that pays for traffic) owns at most one **current
download window**.  A window carries a total body-byte budget.  Every billed
download request reserves the exact number of response body bytes it is about
to return *before* any missing segment is fetched from the origin:

* the reservation sits in ``inflight_bytes`` until the gateway sends the 206
  response headers, when it moves to ``used_bytes``;
* if the request ends without sending 206 (validation/upstream/version error,
  cancellation, or a window switch) the reservation is released;
* cache hits and repeated downloads are charged exactly like origin misses.

Opening a new window is a pure in-memory operation (no origin round-trip).  It
immediately supersedes the old window: requests that still carry the old
window id get **409 Conflict**, and requests that were already in flight but
had not sent 206 headers also get 409 when they try to send them.

Reservations keep a reference to the :class:`Window` object they were made
against.  When an old request finishes it only releases/settles bytes on that
old object, never on the new window, so a window switch cannot be corrupted by
late completions.
"""

from __future__ import annotations

import time
from dataclasses import dataclass, field


@dataclass
class Window:
    site: str
    window_id: int
    budget_bytes: int
    created_at: float = field(default_factory=time.time)
    used_bytes: int = 0
    inflight_bytes: int = 0
    superseded: bool = False

    @property
    def remaining_bytes(self) -> int:
        return self.budget_bytes - self.used_bytes - self.inflight_bytes

    def snapshot(self) -> dict:
        return {
            "site": self.site,
            "window_id": self.window_id,
            "budget_bytes": self.budget_bytes,
            "used_bytes": self.used_bytes,
            "inflight_bytes": self.inflight_bytes,
            "remaining_bytes": self.remaining_bytes,
            "created_at": self.created_at,
        }


class StaleWindow(Exception):
    """The request carries a window id that is not the site's current window."""

    def __init__(self, site: str, requested_window_id: int | None,
                 current_window_id: int | None) -> None:
        super().__init__(
            f"window {requested_window_id} is not current for site {site!r} "
            f"(current: {current_window_id})"
        )
        self.site = site
        self.requested_window_id = requested_window_id
        self.current_window_id = current_window_id


class BudgetExceeded(Exception):
    """The window's remaining body-byte budget cannot cover this response."""

    def __init__(self, window: Window, requested_bytes: int) -> None:
        super().__init__(
            f"site {window.site!r} window {window.window_id} has "
            f"{window.remaining_bytes} bytes left, {requested_bytes} requested"
        )
        self.window = window
        self.requested_bytes = requested_bytes


@dataclass
class Reservation:
    """A byte reservation owned by exactly one (possibly old) window."""

    ledger: "BillingLedger"
    window: Window
    nbytes: int
    _settled: bool = field(default=False, init=False)
    _committed: bool = field(default=False, init=False)

    def commit_if_current(self) -> bool:
        """Commit as used if the window is still current.

        Executed without ``await``, so the current-window check and the
        accounting update are atomic in the event loop.  Returns ``False``
        when the window was superseded; the reservation is released on the
        old window in either case.
        """
        if self._settled:
            return False
        self._settled = True
        self.window.inflight_bytes -= self.nbytes
        if self.ledger.is_current(self.window):
            self.window.used_bytes += self.nbytes
            self._committed = True
            return True
        return False

    def uncommit(self) -> None:
        """Undo a commit whose 206 head could not actually be delivered."""
        if not self._committed:
            return
        self.window.used_bytes -= self.nbytes
        self.window.inflight_bytes += self.nbytes
        self._committed = False
        self._settled = False

    def refund(self) -> None:
        """Release the reservation without charging the window."""
        if self._settled:
            return
        self._settled = True
        self.window.inflight_bytes -= self.nbytes

    @property
    def settled(self) -> bool:
        return self._settled


class BillingLedger:
    """In-memory registry of the current window per site.

    All methods are synchronous and free of ``await`` points; billing
    endpoints are ``async`` so every mutation runs on the event-loop thread.
    That makes check-and-reserve atomic even under concurrent requests.
    """

    def __init__(self) -> None:
        self._windows: dict[str, Window] = {}
        self._last_id: dict[str, int] = {}

    def open_window(self, site: str, budget_bytes: int) -> Window:
        if not site:
            raise ValueError("site must be a non-empty string")
        if budget_bytes < 0:
            raise ValueError("budget_bytes must be >= 0")
        window_id = self._last_id.get(site, 0) + 1
        self._last_id[site] = window_id
        old = self._windows.get(site)
        if old is not None:
            old.superseded = True
        window = Window(site=site, window_id=window_id, budget_bytes=budget_bytes)
        self._windows[site] = window
        return window

    def current_window(self, site: str) -> Window | None:
        return self._windows.get(site)

    def is_current(self, window: Window) -> bool:
        return self._windows.get(window.site) is window and not window.superseded

    def require_current(self, site: str, window_id: int) -> Window:
        window = self._windows.get(site)
        if window is None or window.window_id != window_id:
            raise StaleWindow(site, window_id, window.window_id if window else None)
        return window

    def reserve(self, site: str, window_id: int, nbytes: int) -> Reservation:
        """Reserve ``nbytes`` of body budget or raise StaleWindow/BudgetExceeded."""
        window = self.require_current(site, window_id)
        if nbytes > window.remaining_bytes:
            raise BudgetExceeded(window, nbytes)
        window.inflight_bytes += nbytes
        return Reservation(ledger=self, window=window, nbytes=nbytes)

    def snapshots(self) -> list[dict]:
        return [w.snapshot() for w in self._windows.values()]
