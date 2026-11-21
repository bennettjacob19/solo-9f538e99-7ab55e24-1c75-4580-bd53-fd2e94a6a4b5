# 固件分段下载网关（FastAPI）

现场终端分段下载固件，多个区间请求可共用已取回的字节。网关只在缺段时向源站
发起 HTTP Range 请求；源站换版后，旧版本已缓存区间仍可下载，缺段补不齐时在
发送下载正文前返回 **410 Gone**，终端可重新获取当前版本。

## 架构

```
终端 (分段 Range + If-Match)
        │
        ▼
┌─────────────────────────────┐
│ gateway  (FastAPI, :8000)   │  内存分段缓存，键 = (强 ETag, 网格起点, 网格终点)
│  /firmware/meta  /docs      │  缺段才回源；同段并发合并为一次 Range
└──────────────┬──────────────┘
               │ HTTP Range + If-Match
               ▼
┌─────────────────────────────┐
│ origin   (FastAPI, :8080)   │  同一固件两个等长版本 v1/v2，可换版、可延迟
│  /admin/switch /admin/delay │
└─────────────────────────────┘
```

- 固件为确定性生成的二进制（同版本字节固定，两版本等长且内容不同），无需附带
  二进制文件，容器内即可复现。
- 缓存以 **强 ETag 标识版本**：`"firmware-v1"`、`"firmware-v2"`，天然隔离不同
  版本的分段。
- 分段按固定网格对齐（默认 256 KiB，`GATEWAY_CHUNK_SIZE` 可调）。请求区间覆盖
  哪些网格块，就只对缺失的网格块发 Range；已缓存的块直接内存读取。
- 同一缺失块的并发请求通过 per-chunk 异步锁合并为 **一次** 回源。

## 用 docker compose 启动

```bash
docker compose up --build
```

- 网关：http://localhost:8000/docs
- 源站：http://localhost:8080/docs

## 终端下载流程

1. 在网关 `/docs` 或 `GET /firmware/meta` 查询当前固件的 **强 ETag 和长度**：

   ```bash
   curl -s http://localhost:8000/firmware/meta
   # {"etag":"\"firmware-v1\"","length":1048576,...}
   ```

2. 带 ETag 请求字节区间（`If-Match` + `Range`）：

   ```bash
   curl -D - -o part.bin \
     -H 'If-Match: "firmware-v1"' \
     -H 'Range: bytes=100-300000' \
     http://localhost:8000/firmware.bin
   # HTTP/1.1 206 Partial Content
   # ETag: "firmware-v1"
   # Content-Range: bytes 100-300000/1048576
   ```

   - 成功 **206**：正文与 `Content-Range` 严格对应请求区间，`ETag` 与正文同版。
   - 源站换版导致缺段补不齐：**410 Gone**（先于任何下载正文返回），终端应重新
     调用 `/firmware/meta` 取新版。
   - 已缓存的旧版区间：换版后仍返回 206（带旧 ETag）。
   - 区间越界：416 + `Content-Range: bytes */<长度>`；缺 `If-Match`/`Range` 或
     格式错误：400。

按站点计费时改用 `/billing/firmware.bin?site=<站点>&window=<窗口号>`，见
[按站点计费的下载窗口](#按站点计费的下载窗口)。

## 按站点计费的下载窗口

偏远站点按流量付费：每个站点同时只有一个**当前下载窗口**，窗口带一个**响应正文
总字节预算**。计费入口复用同一套分段缓存与回源逻辑，只是多了正文字节的预占与
记账。

1. 在网关 `/docs` 里调用 `POST /billing/windows`（或在终端 curl），填站点名和
   正文总字节预算，拿到窗口号：

   ```bash
   curl -s -X POST http://localhost:8000/billing/windows \
     -H 'Content-Type: application/json' \
     -d '{"site":"site-a","budget_bytes":4000000}'
   # 201 {"site":"site-a","window_id":1,"budget_bytes":4000000,
   #      "used_bytes":0,"inflight_bytes":0,"remaining_bytes":4000000,...}
   # 响应头 X-Download-Window: 1
   ```

   开窗是纯内存操作，**不会**回源，慢回源时也能立即返回。

2. 终端带站点、窗口号、`If-Match` 和 `Range` 下载：

   ```bash
   curl -D - -o part.bin \
     -H 'If-Match: "firmware-v1"' \
     -H 'Range: bytes=100-300000' \
     'http://localhost:8000/billing/firmware.bin?site=site-a&window=1'
   # HTTP/1.1 206 Partial Content
   # X-Download-Site: site-a
   # X-Download-Window: 1
   ```

   站点/窗口也可用请求头 `X-Download-Site: site-a`、
   `X-Download-Window: 1` 传递。`/firmware.bin?site=...&window=...` 同样计费；
   不带这两个参数时 `/firmware.bin` 保持原有不计费行为。

3. 查站点当前窗口的已用与在途字节：

   ```bash
   curl -s http://localhost:8000/billing/windows/site-a
   # {"site":"site-a","window_id":1,"budget_bytes":4000000,
   #  "used_bytes":299901,"inflight_bytes":0,"remaining_bytes":3700099,...}
   ```

   `GET /billing/windows` 列出所有站点当前窗口。

### 计费语义

- **按应返回的正文字节数预占**：请求通过校验后，先按本次 206 将返回的正文长度
  （即区间长度）预占额度，再决定是否补段。**缓存命中和重复下载同样计费**，与
  是否回源无关。
- **额度不足在补段前回 429**：预占失败立即返回 429（带
  `X-Budget-Remaining`/`X-Budget-Requested`），不会为缺段发起慢回源。
  **有额度的缓存命中无需等慢回源**，直接返回。
- **发出 206 响应头时记为已用**：预占的在途字节在 206 响应头写出时转入
  `used_bytes`。
- **未发出 206 就结束则退回**：410/416/429/上游错误、客户端取消等不会留下占用。
- **开新窗口无需等回源**：换窗立即生效，旧窗口被作废。
- **旧窗口号回 409**：任何带非当前窗口号的请求返回 409。
- **换窗时仍在处理、尚未发出 206 响应头的请求回 409**：即使它已经预占额度、
  正在等慢回源；此时不向任何窗口计费。
- **旧请求结束不得改动新窗口额度**：预占绑定在它开窗时的窗口对象上，旧请求
  结束只在旧窗口上释放/结算，不会写入新窗口。

## 源站管理接口

| 接口 | 作用 |
| --- | --- |
| `POST /admin/switch` `{"version":"v2"}` | 切换当前版本（v1/v2，等长） |
| `POST /admin/delay` `{"delay":1.5}` | 给每个 206 区间响应加延迟（秒），验证慢回源 |
| `GET  /admin/state` | 当前版本、ETag、延迟与回源统计 |
| `POST /admin/reset-stats` | 清零统计 |

### 慢回源不影响缓存命中

源站加延迟后，完全命中缓存的请求不回源、立即返回；只有真正缺段的请求等待。
可观察网关 `GET /cache/stats` 的 `hits/misses/origin_range_requests`。

## 配置（环境变量）

| 变量 | 默认值 | 说明 |
| --- | --- | --- |
| `ORIGIN_BASE_URL` | `http://origin:8080` | 源站地址 |
| `GATEWAY_CHUNK_SIZE` | `262144` | 缓存/回源网格块大小（字节） |
| `GATEWAY_TIMEOUT` | `30` | 回源 HTTP 超时（秒） |
| `FIRMWARE_LENGTH` | `1048576` | 每个固件版本的长度（两版本等长） |

## 本地开发与自测

```bash
python -m venv .venv && . .venv/bin/activate
pip install -r requirements.txt
pip install pytest pytest-asyncio asgi-lifespan

# 1) 进程内集成测试（httpx ASGI，29 个用例：网关 12 + 计费 17）
python -I -m pytest

# 2) 真实 uvicorn 进程的端到端冒烟
uvicorn app.origin.main:app  --port 18080 &
ORIGIN_BASE_URL=http://127.0.0.1:18080 \
  uvicorn app.gateway.main:app --port 18000 &
python -I tests/smoke_real_http.py
# 计费专项：并发扣额、慢回源换窗（需网关为新启动，保证有未缓存段）
python -I tests/smoke_billing_real_http.py
```

## 目录

```
app/common/firmware.py   强 ETag、Range/If-Match 解析、固件生成
app/origin/main.py       本地源站：两等长版本、换版、区间延迟、If-Match 412
app/gateway/cache.py     分段缓存：网格对齐、缺段合并、版本隔离
app/gateway/billing.py   按站点计费：窗口、正文预算预占/结算/退回
app/gateway/main.py      网关：/firmware/meta、206/410/416/400、回源仅 Range、
                         /billing/* 计费窗口与计费下载
tests/                   pytest 集成测试 + 真实 HTTP 冒烟脚本
docker-compose.yml       origin + gateway 两服务
```

## 设计说明与边界

- 网关仅支持分段下载（必须带 `Range`）；`If-Match: *` 表示取源站当前版本。
- 缓存为进程内内存，重启清空；多副本部署时各副本独立（演示范围内未引入共享缓存）。
- 计费窗口与额度同样为进程内内存，重启清空；演示范围内为单副本。
- 计费按**响应正文字节数**（区间长度）而非回源流量计费；额度的校验、预占与
  结算都在事件循环线程内无 `await` 完成，因此并发请求不会超额分配。
- 不支持 multipart/byteranges 与多 ETag 的 `If-Match`，分别返回 400。
- 源站始终用强 ETag（无 `W/` 前缀），保证字节级版本一致性。
