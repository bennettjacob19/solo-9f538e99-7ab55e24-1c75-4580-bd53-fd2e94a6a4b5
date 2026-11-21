"""Real-HTTP checks for billed windows: concurrent deduction, slow-origin switch.

Run while both uvicorn servers are up (same ports as smoke_real_http.py):

  uvicorn app.origin.main:app  --port 18080 &
  ORIGIN_BASE_URL=http://127.0.0.1:18080 uvicorn app.gateway.main:app --port 18000 &
  python -I tests/smoke_billing_real_http.py
"""

import json
import os
import threading
import time
import urllib.error
import urllib.request

GW = os.getenv("SMOKE_GATEWAY", "http://127.0.0.1:18000")
ORIGIN = os.getenv("SMOKE_ORIGIN", "http://127.0.0.1:18080")
fails = []


def req(url, method="GET", headers=None, data=None):
    r = urllib.request.Request(url, method=method, headers=headers or {}, data=data)
    try:
        with urllib.request.urlopen(r, timeout=20) as resp:
            return resp.status, {k.lower(): v for k, v in resp.headers.items()}, resp.read()
    except urllib.error.HTTPError as e:
        return e.code, {k.lower(): v for k, v in e.headers.items()}, e.read()


def check(name, cond, detail=""):
    print(f"[{'PASS' if cond else 'FAIL'}] {name} {detail}")
    if not cond:
        fails.append(name)


def window(site, budget):
    s, _, b = req(GW + "/billing/windows", "POST",
                  {"Content-Type": "application/json"},
                  json.dumps({"site": site, "budget_bytes": budget}).encode())
    assert s == 201, (s, b)
    return json.loads(b)["window_id"]


def usage(site):
    return json.loads(req(GW + f"/billing/windows/{site}")[2])


etag = json.loads(req(GW + "/firmware/meta")[2])["etag"]
CHUNK = 16384  # the 6-way concurrency check is chunk-size agnostic
BIG_CHUNK = 262144  # gateway default chunk size (docker compose)

# ---- A. concurrent deduction: budget fits exactly 3 of 6 identical chunks ----
site = f"conc-{time.time_ns()}"
w = window(site, 3 * CHUNK + 5)
results = []


def worker():
    results.append(req(
        GW + f"/billing/firmware.bin?site={site}&window={w}",
        headers={"If-Match": etag, "Range": f"bytes=0-{CHUNK - 1}"})[0])


threads = [threading.Thread(target=worker) for _ in range(6)]
[t.start() for t in threads]
[t.join() for t in threads]
check("concurrent: exactly 3x206", results.count(206) == 3, str(results))
check("concurrent: exactly 3x429", results.count(429) == 3, str(results))
u = usage(site)
check("concurrent: used == 3 chunks", u["used_bytes"] == 3 * CHUNK, str(u))
check("concurrent: inflight drained", u["inflight_bytes"] == 0, str(u))
check("concurrent: never over budget", u["used_bytes"] <= u["budget_bytes"], str(u))

# ---- B. slow origin + window switch while a miss is in flight ----
# Pick a grid chunk that is still uncached on this gateway (a previous run may
# have warmed some chunks). Chunk 0 was warmed by part A; try others until the
# in-flight reservation is observable.
etag_length = json.loads(req(GW + "/firmware/meta")[2])["length"]
chunk_starts = [
    start
    for start in range(BIG_CHUNK, etag_length, BIG_CHUNK)
]
req(ORIGIN + "/admin/delay", "POST", {"Content-Type": "application/json"},
    b'{"delay":1.5}')
time.sleep(0.2)

chosen = None
for start in chunk_starts:
    site2 = f"switch-{time.time_ns()}"
    w1 = window(site2, 2 * BIG_CHUNK)
    holder = {}

    def slow_download(start=start, site2=site2, w1=w1):
        # Last byte of the chunk forces a full-chunk miss.
        end = min(start + BIG_CHUNK - 1, etag_length - 1)
        holder["resp"] = req(
            GW + f"/billing/firmware.bin?site={site2}&window={w1}",
            headers={"If-Match": etag, "Range": f"bytes={start}-{end}"})

    t = threading.Thread(target=slow_download)
    t.start()
    time.sleep(0.4)  # let it reserve and enter the slow origin fetch
    inflight = usage(site2)["inflight_bytes"]
    if inflight != BIG_CHUNK:
        # Already cached: this run cannot exercise the in-flight switch.
        t.join()
        req(ORIGIN + "/admin/delay", "POST",
            {"Content-Type": "application/json"}, b'{"delay":0}')
        time.sleep(0.05)
        req(ORIGIN + "/admin/delay", "POST",
            {"Content-Type": "application/json"}, b'{"delay":1.5}')
        continue
    chosen = (site2, w1, holder, t)
    break

check(
    "switch: in-flight bytes visible",
    chosen is not None,
    "" if chosen is not None else "no uncached chunk; restart the gateway",
)
if chosen is None:
    req(ORIGIN + "/admin/delay", "POST", {"Content-Type": "application/json"},
        b'{"delay":0}')
    raise SystemExit("FAILED: no uncached chunk for the slow-origin switch check")
site2, w1, holder, t = chosen

t0 = time.monotonic()
w2 = window(site2, 2 * BIG_CHUNK)
open_elapsed = time.monotonic() - t0
check("switch: new window opens without waiting for origin", open_elapsed < 0.5,
      f"{open_elapsed:.2f}s")
check("switch: new window id differs", w2 != w1, f"{w1} vs {w2}")

t.join()
req(ORIGIN + "/admin/delay", "POST", {"Content-Type": "application/json"},
    b'{"delay":0}')
status, _, body = holder["resp"]
check("switch: in-flight old request -> 409", status == 409, f"status={status}")
u2 = usage(site2)
check("switch: new window untouched", u2["used_bytes"] == 0, str(u2))
check("switch: new window inflight empty", u2["inflight_bytes"] == 0, str(u2))

s, _, _ = req(GW + f"/billing/firmware.bin?site={site2}&window={w1}",
              headers={"If-Match": etag, "Range": "bytes=0-9"})
check("switch: old window id now 409", s == 409, f"status={s}")
s, _, b = req(GW + f"/billing/firmware.bin?site={site2}&window={w2}",
              headers={"If-Match": etag, "Range": "bytes=0-9"})
check("switch: new window downloads", s == 206 and len(b) == 10, f"status={s}")
check("switch: only new window charged", usage(site2)["used_bytes"] == 10,
      str(usage(site2)))

print()
if fails:
    raise SystemExit(f"FAILED: {fails}")
print("BILLING SMOKE OK")
