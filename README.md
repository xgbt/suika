# Suika

A Bilibili live-stream recording service built on
[Kratos](https://github.com/go-kratos/kratos) (go-kratos/v3). Suika
monitors a set of live rooms and, the moment a streamer goes live,
records the original-quality FLV stream plus every danmaku event to
disk, splits long streams into segments, and merges each finished
session's segments into a single FLV file (pure Go, no external tools).
Rooms are managed through a CRUD API and a web dashboard.

The dashboard also includes an Eino ADK **智能助手** tab for streamed
DeepSeek conversations and read-only room queries. Automated cutting and
publishing workflows are not implemented.

## Features

- **Live detection**: resident danmaku WebSocket per room (reconnect +
  room-state re-probe built in), with a jittered polling fallback.
- **Recording**: FLV pulled and parsed in pure Go — no external tools
  involved at all, so files stay valid even if the process dies.
- **Segmentation**: time-based splits on video keyframes (default
  120 min/part); every part gets the stream headers re-injected and is
  independently playable.
- **Danmaku**: per-part JSONL covering danmaku, gifts, SuperChat,
  guard purchases, and entry effects (raw payload kept).
- **Resilience**: auto-reconnect on stream drops (separate
  CDN-transient budget), health watchdog, crash-safe `meta.json`,
  startup recovery for interrupted merges.
- **Session merge**: at session end, all segment FLVs are merged into a
  single file and the danmaku JSONL files concatenated — pure Go, no
  ffmpeg (default on; set `merge_enabled: false` to keep the parts).
- **Room management**: sqlite-backed room CRUD API (HTTP + gRPC) and a
  React + Ant Design dashboard.
- **Risk-control handling**: WBI signing, buvid fingerprints, one-shot
  refresh-and-retry, legacy API fallback, per-room cooldown ladder.

## Requirements

- Go 1.25+ with **cgo** (the sqlite driver is `mattn/go-sqlite3`).
- No external binaries: recording and the session merge are pure Go.
- Node.js only if you want to run the web dashboard.

## Quick start

Run the backend from the project root, including when debugging. The default
config directory is `./configs`; database and recording paths are relative
to the working directory, not to the config directory. Running from
`cmd/suika/` would create a separate database and recording directory.

```bash
make init        # once: installs wire + buf

go run ./cmd/suika                    # HTTP :8000, gRPC :9000
```

The Bilibili login cookie is not a config item — open the web dashboard
(`web/`, see below) and scan the QR code in the header's account bar.
The credential is persisted in the sqlite `credentials` table and
hot-swapped into the recorder without a restart (source quality requires
a logged-in account).

Add a room to monitor (the recorder picks it up immediately, no restart):

```bash
curl -X POST localhost:8000/v1/rooms/create \
     -d '{"room":{"room_id":123456,"record_enabled":true}}'
```

Recordings land under `./recordings/<room_id>_<streamer>/<date>/`;
each session's `meta.json` is the history source of truth.

## API

`RoomService`'s five RPCs are exposed on both HTTP and gRPC; HTTP routes
are all `POST` with a JSON body:

| RPC | HTTP route | Purpose |
|---|---|---|
| CreateRoom | `POST /v1/rooms/create` | Register a room by its platform `room_id` |
| ListRooms | `POST /v1/rooms/list` | Paginated list; optional exact-match filters (`room_id` / `streamer_name` / `room_title` / `record_enabled`) |
| GetRoom | `POST /v1/rooms/get` | One room plus its live runtime state |
| UpdateRoom | `POST /v1/rooms/update` | Toggle `record_enabled` via `update_mask` |
| DeleteRoom | `POST /v1/rooms/delete` | Delete a room |

`CreateRoom` only requires `room_id` and `record_enabled`. The streamer name
and room title are output-only and are filled from Bilibili room information;
the runtime fields (`live_status`, `record_status`, write progress, the
granted stream quality `granted_qn` / `granted_qn_desc`, …) are output-only
as well. `UpdateRoom` only toggles `record_enabled` via `update_mask`.

`AccountService` manages the Bilibili account the recorder acts as
(`api/account/v1/account.proto`, also HTTP + gRPC):

| RPC | HTTP route | Purpose |
|---|---|---|
| CreateQRLogin | `POST /v1/account/qr-login/create` | Generate a QR login session (~180s expiry) |
| PollQRLogin | `POST /v1/account/qr-login/poll` | Poll scan status; on confirm, persist the cookie and hot-swap it |
| GetAccountStatus | `POST /v1/account/status/get` | Report the logged-in account (verified against Bilibili) |
| Logout | `POST /v1/account/logout` | Local logout: delete the stored credential |

`ChatService` (`api/chat/v1/chat.proto`) contains only the server-streaming
`Chat` RPC. `ModelService` (`api/chat/v1/model.proto`) contains
the four unary model-discovery/configuration RPCs:

| RPC | HTTP route | Purpose |
|---|---|---|
| Chat | `POST /v1/chat` | Full conversation history in; SSE text deltas out |
| ListModels | `POST /v1/chat/models/list` | Discover official models with a supplied or stored key; does not save |
| GetModelConfig | `POST /v1/chat/config/get` | Provider, model ID and configured state; never returns the key |
| SaveModelConfig | `POST /v1/chat/config/save` | Validate against the official list, then save; blank key retains the stored value |
| DeleteModelConfig | `POST /v1/chat/config/delete` | Delete the configuration and disable new chats |

HTTP routes and JSON shapes are unchanged. gRPC configuration methods now use
`/chat.v1.ModelService/{ListModels,GetModelConfig,SaveModelConfig,DeleteModelConfig}`
instead of `ChatService`; regenerate/update gRPC clients accordingly.

Chat is server-streaming over gRPC and SSE over HTTP (`fetch`, not
`EventSource`). HTTP roles are numeric: `1` user, `2` assistant.
Concatenate `ChatEvent.text_delta` values in order; normal EOF completes the
answer, while failures after streaming starts arrive as `event: error`.
Disconnecting cancels the upstream model request, including before the first
token. The read-only `list_rooms` tool returns at most 50 rooms per page and
provides `next_offset` to continue with the same filters.
HTTP/gRPC unary RPCs use Kratos's native 15-second timeout, without a custom
timeout middleware. SSE chat retains connection cancellation but is not cut
off by the unary timeout; the DeepSeek model-list client has its own 15-second
timeout.

The generated `openapi.yaml` at the repo root is regenerated by
`make api`. Its current generator describes chat event JSON, not the SSE
framing; `chat.proto` and the streaming contract above are authoritative.

## Configuration

`-conf` points at a directory; every yaml inside is merged into one
config (the repo ships only `config.yaml`). Key settings live under
`recorder:` — `record_root` and
`merge_enabled` (merge segments into one file per session at session
end, default true). The deprecated `cookie` field is ignored — the
credential comes from web QR login (sqlite `credentials` table).
Behavioral tuning (segment length, reconnect policy, poll interval,
stream quality) is code constants, not config fields. See
`docs/design/bili-recorder.md` §7 for the full list and defaults.

## Development

```bash
make api       # regenerate api/ proto stubs + openapi.yaml
make config    # regenerate internal/conf from conf.proto
make all       # api + config + wire + go mod tidy
make build     # build all packages into ./bin/

go test -mod=mod ./...     # all tests (sqlite via t.TempDir())
```

### Web dashboard

```bash
cd web
npm install
npm run dev    # vite dev server, proxies /v1 -> http://localhost:8000
```

### Assistant setup

1. Open **智能助手** below **工作流**, then **模型配置**.
2. Enter the DeepSeek API key and click **获取官方模型列表**. Suika calls
   the authenticated official `GET https://api.deepseek.com/models`; the
   browser never calls DeepSeek directly.
3. Select an official model ID and save. `deepseek-flash` is the initial
   selection when available; an unavailable selection is replaced by the
   first returned model. Existing configurations automatically load the
   list using the stored key.

The single provider/model/key configuration lives in SQLite `model_configs`.
Only DeepSeek is currently supported; `DEEPSEEK_API_KEY` is no longer read.
Discovery does not save the supplied key. Saving rechecks the selected ID
against the official list; leaving the key blank retains the stored value.
Saves/deletes affect new requests immediately, not in-flight chats. Without
configuration chat returns 503, while room management and recording still work.

The key is stored **unencrypted** and never returned by the API. Protect the
database; this unauthenticated service is for trusted local single-user use,
not public exposure. Use HTTPS for remote access. Room information and
conversation history used by the assistant are sent to DeepSeek.

Conversations survive tab switches, not reloads. Stopped or failed turns are
excluded from subsequent context. Enter sends; Shift+Enter inserts a newline.
Frontend SSE tests: `cd web && node --test tests/chat.test.ts` (Node 22.18+).

## Docker

```bash
docker build -t suika .
docker run --rm \
  -p 8000:8000 -p 9000:9000 \
  -v /path/to/configs:/data/conf \
  -v /path/to/db:/app/data \
  -v /path/to/recordings:/app/recordings \
  suika
```

The image needs no external tools — recording and merging are pure Go.
Mount your config directory at `/data/conf` (it must contain
`config.yaml`); `/app/data`
(sqlite) and `/app/recordings` are the runtime data directories.

## Documentation

- [`docs/design/bili-recorder.md`](docs/design/bili-recorder.md) —
  deep-dive technical doc (Chinese): runtime model, core flows, risk
  control, on-disk layout, config, failure handling.
- [`docs/design/architecture-diagrams.md`](docs/design/architecture-diagrams.md) —
  architecture diagrams: system/app architecture, sequence diagrams,
  state machines, ER view.
- [`CLAUDE.md`](CLAUDE.md) — repo conventions: layering, naming,
  build commands, the add-a-resource checklist.
- [`CONTEXT.md`](CONTEXT.md) — domain glossary.
- [`docs/adr/0007-eino-assistant.md`](docs/adr/0007-eino-assistant.md) —
  minimal Eino assistant, streaming and database-backed model configuration.

## Project layout

```text
api/room/v1/          RoomService proto (DTO) + generated bindings
api/account/v1/       AccountService proto (DTO) + generated bindings
api/chat/v1/          ChatService / ModelService protos + generated bindings
cmd/suika/            Entrypoint + Wire wiring
configs/              Deployment config only; credentials live in SQLite
docs/adr/             Architectural decisions
docs/design/          Technical design docs
internal/server/      HTTP / gRPC servers + recorder Daemon
internal/service/     DTO <-> DO conversion, request validation
internal/biz/         Domain objects, usecases, repo/client interfaces
internal/data/        sqlite repos + thin Wire constructors
internal/data/deepseek/  DeepSeek SDK adapter + official model discovery
internal/data/bili/   Bilibili API client + danmaku WS
internal/data/recorder/  FLV recording pump + session merge
web/                  React + Ant Design dashboard
openapi.yaml          Generated OpenAPI document
```
