# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

This is a Kratos (go-kratos/v3) service for managing Bilibili live rooms and
recording sessions. The canonical API contract is
`api/room/v1/room.proto`; the app entrypoint is `cmd/suika/` and the Go module
name is `suika`.

## Commands

```bash
make init      # install generators (wire, buf) — needed once
make api       # regenerate api/ proto stubs + openapi.yaml (buf.gen.yaml)
make config    # regenerate internal/conf from conf.proto (buf.gen.config.yaml)
make all       # make api + make config + go generate (Wire) + go mod tidy
make build     # build all packages into ./bin/

go run ./cmd/suika -conf ./configs   # run locally (HTTP :8000, gRPC :9000)

go test -mod=mod ./...                                          # all tests
go test -mod=mod ./internal/service/                            # one package
go test -mod=mod ./internal/service/ -run TestRoomServiceCRUD   # one test
```

There is no configured linter. Proto codegen uses buf v2 with
buf.build/googleapis/googleapis as a BSR dependency (buf.lock committed).
All protoc plugins are pinned and invoked as `go run <plugin>@<version>`
from `buf.gen.yaml` / `buf.gen.config.yaml`, so nothing besides `buf` and
`wire` needs installing. Wire regeneration happens via `go generate` from
`cmd/suika/wire_gen.go`, so `make all` covers it.

All direct `go build` / `go test` / `go vet` invocations pass `-mod=mod`;
vendor mode is never used. The sqlite driver (`mattn/go-sqlite3`) requires
cgo. There are no external binary dependencies — recording and the
session-end merge are pure Go; the checked-in `config.yaml` ships with
`merge_enabled: true`. There is no `third_party/` directory — buf resolves googleapis
from the BSR.

Never hand-edit generated files: `*.pb.go`, `*_grpc.pb.go`, `*_http.pb.go`,
`wire_gen.go`, `openapi.yaml`. Regenerated files belong in the same commit
as their source.

## High-level architecture

A Kratos app wired together only in `cmd/` via Wire:
`wireApp(bc.Server, bc.Data, bc.Recorder, logger)` builds from four
ProviderSets — `server`, `service`, `biz`, `data` — plus `newApp`
(main.go). Runtime config (`configs/config.yaml`, merged with any other
yaml in the directory) is loaded and scanned into the generated
`internal/conf` proto types. Three `transport.Server`s are started —
HTTP, gRPC, and the recorder daemon (`server.Daemon`); the HTTP
server applies `recovery` and a `validate` middleware that enforces
`google.api.field_behavior` required fields on proto messages.

Three model shapes flow through three layers. `biz` owns the DO, `data`
owns the PO; `service` is a pass-through that converts at its boundary.

```
   client ──► DTO ──► service ──► DO ──► biz ──► DO ──► data ──► PO ──► storage
                                  ▲                ▲
                                  │ declares       │ implements
                                  └─── repo IF ────┘

   DTO  Data Transfer Object — proto request / response.
   DO   Domain Object        — pure biz model, no proto, no storage tags.
   PO   Persistent Object    — storage shape, owned by `data`.
```

| Layer   | Owns | Speaks at boundary | Never speaks            |
|---------|------|--------------------|-------------------------|
| service | —    | DTO ↔ DO          | PO, storage client      |
| biz     | DO   | DO                 | DTO, PO, storage client |
| data    | PO   | DO ↔ PO           | DTO                     |

- `service` imports `api/room/v1` (DTO) and `biz` (DO). Never `data`.
- `biz` imports `api/...` only for error reason enums. Never `service`,
  never `data`. The repo interface declared here is the inversion seam.
- `data` imports `biz` to implement the repo interface. Never `service`,
  never DTOs.
- `cmd` is the only place that wires all layers via Wire.

A change crossing these arrows the wrong way is a layering bug; fix the
design rather than add the import.

### Layer responsibilities

**service (DTO ↔ DO)**

- `convertRoom` parses `room_id` and `record_enabled` into `biz.Room`; the
  reverse direction is `convertRoomReply`, which maps `biz.RoomRuntime` to
  the API `Room` message.
- `CreateRoom`, `GetRoom`, `ListRooms`, and `UpdateRoom` return their
  corresponding response wrapper. `DeleteRoom` returns
  `DeleteRoomResponse{empty: google.protobuf.Empty}`.
- Embed `Unimplemented<Resource>ServiceServer`.
- Parse AIP list requests via `pagination` and apply `fieldmask.Update`
  for partial updates (both go.einride.tech/aip). List filtering uses
  the request's typed optional fields (translated to `biz.ListQuery`),
  not AIP filter strings; there is no order_by.
- Validate request inputs at the service boundary before delegating to
  the usecase. Return `biz` errors. No business rules, no storage
  access, no PO.
- The room API is unary-only; it does not define streaming RPCs.

**biz (DO only)**

- Owns the DO (`type <Resource> struct` — no proto, no storage tags),
  the usecase, and the repo interface (`type <Resource>Repo interface`).
- Owns typed errors built with `errors.NotFound` / `errors.BadRequest`
  plus the API error reason enum (`ErrRoomNotFound`,
  `v1.ErrorReason_ERROR_REASON_NOT_FOUND`).
- Owns `ListQuery` — optional equality filters (`RoomID` /
  `StreamerName` / `RoomTitle` / `RecordEnabled`) plus `Offset` / `Limit` —
  so callers compose queries without leaking storage primitives.

**data (DO ↔ PO)**

- _Repo shape_: implement `biz.<Resource>Repo`. The constructor returns
  the interface, never the concrete type:
  `func New<Resource>Repo(d *Data) biz.<Resource>Repo`.
- _PO and conversion_: define a PO when the storage shape diverges from
  the DO. PO types stay inside `data`. Use free functions
  `to<Resource>PO` (DO → PO, write) and `to<Resource>DO` (PO → DO,
  read). Driver-specific builder types never leave `data`.
- _Shared clients_: `*Data` (internal/data/data.go) holds long-lived
  storage clients. Repos receive `*Data` and never construct their own
  clients. `Data` owns the SQLite/GORM handle and the shared platform and
  recorder clients; `roomRepo` persists rooms in the `rooms` table.
- _Querying_: translate `ListQuery`'s optional equality fields into the
  storage driver's query language inside the repo; list ordering is
  fixed there (`room_id ASC`).
- _Errors_: map driver errors to `biz` typed errors so callers above
  never branch on the driver.

**server**

- Construct HTTP/gRPC servers, apply middleware, register services. No
  translation, no business logic.

### Recorder daemon

Room CRUD is one half of the service; the recorder daemon is the other.
It runs as a third `transport.Server` (`server/daemon.go`) sharing
the app lifecycle with HTTP/gRPC: `Start` launches
`biz.RecorderUsecase.Run` in a goroutine on a context derived from
`context.Background()` (Start's own ctx may be cancelled after it
returns), and `Stop` cancels the loop with a bounded 45s wait.

The same layering and inversion pattern applies, with two more seams
declared in `biz` and implemented in `data`:

- `LiveClient` — the platform seam; ALL live-room Bilibili traffic goes
  through it (room info, stream URLs, danmaku websocket). Implemented in
  the `data/bili/` subpackage by `live.go` / `danmaku.go` plus the
  risk-control helpers `wbi.go` (WBI signing) and `buvid.go`. All risk
  orchestration lives in the single `riskGuard` module (`risk.go`):
  cooldown gates, 412/403/429 and
  -352 refresh-and-retry, legacy-API fallback, error classification, and
  the per-room cooldown ladder. Endpoint code only builds requests,
  parses responses, and translates business codes — never retries or
  sleeps on risk itself.
- `RecorderRepo` — the storage seam; session directory layout, FLV
  parsing/writing (`flv/`), danmaku JSONL, per-session `meta.json`, and
  the session-end merge (`merge.go`). Implemented across `internal/data/recorder*.go`
  (`recorder.go` session lifecycle + recovery, `recorder_segment.go`
  segment files, `recorder_session.go` `meta.json` bookkeeping,
  `recorder_stats.go` write-progress stats).
- `PassportClient` — the account platform seam; QR-login and nav traffic
  (passport.bilibili.com / api.bilibili.com) goes through it, explicitly
  outside `riskGuard` (no WBI signing, no retry). Implemented in
  `data/bili/passport.go`. Login cookies are captured from the poll response's
  Set-Cookie headers.
- `CredentialRepo` — the credential storage seam; persists the single
  Bilibili login cookie in the `credentials` table and, on
  save/delete, hot-swaps the in-memory cookie held by the `bili.Client`
  inside `*Data` so the recorder picks up a new login without restart.
  Implemented in `data/credential.go`. See ADR-0003.

`RecorderUsecase` (biz/recorder.go) makes decisions only: its `Run` is a
supervisor loop reconciling the registry's change notifications against
per-room monitor goroutines, room monitoring (the danmaku WS is the primary
live-detection channel; fallback polling is the backup),
session lifecycles, and the stream-drop/reconnect decision tree. Byte-level
IO belongs to the seams.

Session start/stop/resume decisions are NOT inline in the monitor: they
live in one stateful module, `sessionPolicy` (biz/session_policy.go,
ADR-0001). The monitor's (`watchRoom`) select arms only deliver inputs —
room info arrived, `record_enabled` flipped, session finished — and execute the
returned decision (Start/Stop/None); phases (idle/running/finishing) and
the resume-after-finish rule live inside the module. The decision matrix
is `.scratch/session-policy/spec.md` and every matrix row has a test —
when changing policy, update the matrix and its tests together. `watchRoom`
remains the only owner of goroutines, contexts, and the danmaku
connection; "reconcile" stays reserved for the supervisor loop's
monitor-set level. Recordings land under `record_root` (default
`./recordings`, gitignored); each session's `meta.json` is the history
source of truth, and `RecoverPending` finalizes sessions interrupted by a
crash or restart at startup.

## Room API Contract

`RoomService` is defined in `api/room/v1/room.proto` and exposes both HTTP
and gRPC transports:

| RPC | HTTP route | Purpose |
|---|---|---|
| `CreateRoom` | `POST /v1/rooms/create` | Create a room using the caller-provided `room_id`. |
| `ListRooms` | `POST /v1/rooms/list` | Query rooms with optional equality filters and pagination. |
| `GetRoom` | `POST /v1/rooms/get` | Get one room and its current runtime state. |
| `UpdateRoom` | `POST /v1/rooms/update` | Toggle `record_enabled`; platform identity fields are read-only. |
| `DeleteRoom` | `POST /v1/rooms/delete` | Delete a room by `room_id`. |

### Account API

`AccountService` is defined in `api/account/v1/account.proto` and manages the
Bilibili account the recorder acts as. The login credential is obtained via
QR login and stored in the sqlite `credentials` table — the only cookie source.

| RPC | HTTP route | Purpose |
|---|---|---|
| `CreateQRLogin` | `POST /v1/account/qr-login/create` | Generate a QR login session (`url`, `qrcode_key`, ~180s expiry). |
| `PollQRLogin` | `POST /v1/account/qr-login/poll` | Poll scan status; on confirm, persist the cookie and hot-swap it live. |
| `GetAccountStatus` | `POST /v1/account/status/get` | Report the logged-in account (verified against Bilibili nav). |
| `Logout` | `POST /v1/account/logout` | Local logout: delete the stored credential and clear memory. |

`PollQRLogin` returns a `QRLoginStatus` enum (`NOT_SCANNED`, `SCANNED`,
`EXPIRED`, `CONFIRMED`); expiry/scanned are normal outcomes, not errors.
Platform failures map to `ERROR_REASON_UNAVAILABLE` (503).

### Room message

The create request accepts `room_id` and `record_enabled`. `streamer_name` and
`room_title` are output-only fields populated from Bilibili. The following fields
are `OUTPUT_ONLY` and must be populated by the service:

- `live_status`: `LIVE_STATUS_UNSPECIFIED`, `LIVE_STATUS_PREPARING`, or
  `LIVE_STATUS_LIVE`.
- `record_status`: `RECORD_STATUS_UNSPECIFIED`, `RECORD_STATUS_IDLE`,
  `RECORD_STATUS_RECORDING`, `RECORD_STATUS_REMUXING`, or
  `RECORD_STATUS_ERROR`.
- `granted_qn`, `granted_qn_desc`: the stream quality Bilibili actually
  granted for the current recording session (zero/empty when not recording).
- `current_file`, `bytes_written`, `download_speed_bps`,
  `session_started_at`, `last_error`, `create_time`, and `update_time`.

`room_id` is the caller-provided unique platform room ID and is immutable.
`CreateRoomRequest.room` is required. Invalid IDs return
`ERROR_REASON_INVALID_ARGUMENT`; duplicate IDs return
`ERROR_REASON_ALREADY_EXISTS`; missing rooms return
`ERROR_REASON_NOT_FOUND`.

`ListRoomsRequest` uses AIP pagination plus four optional exact-match query
fields: `room_id`, `streamer_name`, `room_title`, and `record_enabled` (unset
fields don't filter; set fields combine with AND). There is no filter
string and no ordering parameter — the repo orders by `room_id ASC`. The
default page size is 20. The response uses `rooms` and
`next_page_token`.

### Room runtime and recording

`RoomRegistry` loads persisted rooms at startup, is kept in sync by room
CRUD after each successful persist (`Add` / `Update` / `Remove`), and holds
mutable live and recording state. The recorder updates the registry; room
reads merge the registry snapshot with persisted fields. For an actively
recording room, `SessionStatsRepo` best-effort supplies `current_file`,
`bytes_written`, and `download_speed_bps`; the granted stream quality
(`granted_qn` / `granted_qn_desc`) is registry state itself —
`SetStreamQuality` records what each opened stream actually got, and the
quality resets when a session starts and when it finishes. CRUD changes take effect on the recorder in real time via
its supervisor loop (no restart): created rooms are monitored immediately
regardless of `record_enabled`, deleting a room stops its monitor immediately
(gracefully stopping any active session, recorded files are kept), and
`record_enabled` gates only recording — turning it off stops an active recording,
turning it on starts recording when the room is live. Platform room-info
refreshes (danmaku WS events or the fallback poll) flow through
`RoomRegistry.ApplyRoomInfo`: they update the live status and overwrite
the in-memory `streamer_name` / `room_title` with the platform's non-empty
values, then persist the room via `RoomRepo.UpdateRoom` — platform data
wins over previously stored values; a failed persist only logs a warning
and the in-memory snapshot keeps the new values.

### Web frontend

`web/` is a React + TypeScript + Vite + Ant Design SPA and the only
graphical consumer of the HTTP API (`RoomList`: table with runtime
status, create, recording on/off confirmation, delete confirmation,
auto-refresh; the recording badge of an actively recording room carries a
tooltip with the granted quality). The header also carries an account bar (`AccountBar`) with
a QR-login modal (`QRLoginModal`) for obtaining the Bilibili credential,
plus status display and logout. It is decoupled from the Go build —
`npm install` / `npm run dev`, with vite proxying `/v1` to
`http://localhost:8000`. Frontend types mirror `room.proto` by hand
(`web/src/api/rooms.ts`) and `account.proto` by hand
(`web/src/api/auth.ts`), so proto changes must be synced there.

## Add-a-resource checklist

1. **DTO**: define `Create<Resource>` / `Get<Resource>` /
   `List<Resources>` / `Update<Resource>` / `Delete<Resource>` in
  `api/<domain>/<version>/`, then `make api`.
2. **DO + repo interface**: declare both in `biz`; build the usecase on
   top of the interface.
3. **Repo impl**: implement in `data` returning `biz.<Resource>Repo`;
   add a PO and the matching conversion helpers when storage shape
   diverges from DO.
4. **Wiring**: register the repo constructor in `data.ProviderSet`, the
   usecase in `biz.ProviderSet`, the service in `service.ProviderSet`;
   register HTTP/gRPC services in `internal/server`.
5. **Regenerate**: `make all` to refresh Wire and `go.mod`.

## Testing seam

Tests live beside the code they cover (`*_test.go`). Test layers in
isolation: service tests fake the usecase, biz tests fake the repo, data
tests exercise repo implementations at the storage boundary. Room service
tests in `internal/service/room_test.go` exercise CRUD, pagination,
optional query filtering, runtime merging, and validation against real sqlite
(`t.TempDir()` db file, wired like `wireApp`; pass `MergeEnabled: false`
to disable the session-end merge). Data-layer tests use the real
filesystem; the merge is pure Go, so nothing external is needed.

## Naming & error reasons

- Resource: `<Resource>` (the current resource is `Room`); collection RPC:
  `List<Resources>`.
- Types: repo `<Resource>Repo`, usecase `<Resource>Usecase`, service
  `<Resource>Service`. PO types live inside `internal/data/`; convert
  with `to<Resource>PO(do)` / `to<Resource>DO(po)` free functions.
- Error reasons: declared in `api/<domain>/<version>/error_reason.proto`,
  surfaced as `Err<Resource><Cause>` in `biz`.

## Commits & security

- Conventional Commits: `feat:`, `fix:`, `refactor:`, `chore(deps):`,
  `docs:`, `test:`.
- The Bilibili login credential (cookie) is obtained via web QR login and
  stored in the sqlite `credentials` table — it is the only cookie source.
  The config field `recorder.cookie` is deprecated and ignored;
  `configs/credentials.yaml` is no longer needed (see
  `credentials.example.yaml`). See ADR-0003.
- Config governance: `config.yaml` holds only deployment-varying items
  (addrs, database path, record root, concurrency cap, merge switch). The
  `cookie` field is a deprecated placeholder kept for compatibility and is
  ignored — the credential comes from QR login. Behavioral tuning (segment
  length, reconnect policy, poll interval, stream quality) is code
  constants in `biz` / `data`, not config fields — see
  `docs/design/bili-recorder.md` §7 before adding a new config field.

## Design docs

`docs/design/bili-recorder.md` (Chinese) is the deep-dive on the
recorder service: goroutine structure, room states, stream-drop decision
tree, on-disk layout (`meta.json`, danmaku JSONL), risk control, config
defaults, and failure handling. `docs/design/architecture-diagrams.md` is
its companion diagram set: system/app architecture, sequence diagrams for
room CRUD / live detection / recording / query, state machines, and the ER
view. `docs/design/recorder-comparison.md` is a research note comparing
the core recording pipeline against the four upstream recorders in the
workspace (BililiveRecorder / biliup / blrec(bilive) / DDTV) with
candidate improvements — it is analysis, not decisions; anything adopted
from it gets its own ADR. All three defer repo-level conventions
(layering, naming, build commands) to this file — when changing template
rules, keep them consistent.

## Agent skills

### Issue tracker

Issues live as markdown files under `.scratch/<feature>/` in this repo. See `docs/agents/issue-tracker.md`.

### Triage labels

Default five-role vocabulary (`needs-triage`, `needs-info`, `ready-for-agent`, `ready-for-human`, `wontfix`), used as `Status:` values in issue files. See `docs/agents/triage-labels.md`.

### Domain docs

Single-context: `CONTEXT.md` at repo root, ADRs in `docs/adr/`. See `docs/agents/domain.md`.

`CONTEXT.md` is also the canonical glossary — Room, Session, Monitor,
Fallback poll, Session policy, Reconcile — each with an explicit "avoid"
list (e.g. don't call the per-room goroutine a watcher/poller; don't use
"reconcile" for session-level start/stop). Use these exact terms in code
names, comments, tests, and issues; new architectural decisions go into
`docs/adr/`, not into the pre-existing `docs/design/` docs.
