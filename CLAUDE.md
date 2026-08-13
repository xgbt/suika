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
cgo. Setting `remux_enabled: true` with no `ffmpeg` in PATH fails startup
by design (startup probe); the checked-in `config.yaml` ships with it
`false`.

Note: README.md and the Dockerfile are stale: README still describes the
original Kratos template and its since-removed Todo sample (the app is
`cmd/suika`, README says `cmd/server`), `make build` produces
`bin/suika` (Dockerfile CMD runs `./server`), and there is no
`third_party/` directory. Trust the Makefile and this file.

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
HTTP, gRPC, and the recorder daemon (`server.RecorderJob`); the HTTP
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

- `convertRoom` parses the writable proto fields into `biz.Room`; the
  reverse direction is `convertRoomReply`, which maps `biz.RoomRuntime` to
  the API `Room` message.
- `CreateRoom`, `GetRoom`, `ListRooms`, and `UpdateRoom` return their
  corresponding response wrapper. `DeleteRoom` returns
  `DeleteRoomResponse{empty: google.protobuf.Empty}`.
- Embed `Unimplemented<Resource>ServiceServer`.
- Parse AIP list requests via `filtering` / `ordering` / `pagination`
  (go.einride.tech/aip); apply `fieldmask.Update` for partial updates.
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
- Owns `ListOption` helpers — `ListFilter`, `ListOrderBy`, `ListOffset`,
  `ListLimit` — so callers compose queries without leaking storage
  primitives.

**data (DO ↔ PO)**

- _Repo shape_: implement `biz.<Resource>Repo`. The constructor returns
  the interface, never the concrete type:
  `func New<Resource>Repo(d *Data) biz.<Resource>Repo`.
- _PO and conversion_: define a PO when the storage shape diverges from
  the DO. PO types stay inside `data`. Use free functions
  `new<Resource>` (DO → PO, write) and `toBiz` (PO → DO, read).
  Driver-specific builder types never leave `data`.
- _Shared clients_: `*Data` (internal/data/data.go) holds long-lived
  storage clients. Repos receive `*Data` and never construct their own
  clients. `Data` owns the SQLite/GORM handle and the shared platform and
  recorder clients; `roomRepo` persists rooms in the `rooms` table.
- _Querying_: translate `ListOptions.Filter` and `ListOptions.OrderBy`
  into the storage driver's query language inside the repo.
- _Errors_: map driver errors to `biz` typed errors so callers above
  never branch on the driver.

**server**

- Construct HTTP/gRPC servers, apply middleware, register services. No
  translation, no business logic.

### Recorder daemon

Room CRUD is one half of the service; the recorder daemon is the other.
It runs as a third `transport.Server` (`server/recorder_job.go`) sharing
the app lifecycle with HTTP/gRPC: `Start` launches
`biz.RecorderUsecase.Run` in a goroutine on a context derived from
`context.Background()` (Start's own ctx may be cancelled after it
returns), and `Stop` cancels the loop with a bounded 45s wait.

The same layering and inversion pattern applies, with two more seams
declared in `biz` and implemented in `data`:

- `LiveClient` — the platform seam; ALL Bilibili traffic goes through it
  (room info, stream URLs, danmaku websocket). Implemented in `data` by
  `bili_api.go` / `danmaku.go` plus the risk-control helpers `wbi.go`
  (WBI signing) and `buvid.go`.
- `RecorderRepo` — the storage seam; session directory layout, FLV
  parsing/writing (`flv/`), danmaku JSONL, per-session `meta.json`, and
  remux (`remux.go`).

`RecorderUsecase` (biz/recorder.go) makes decisions only: room
monitoring (the danmaku WS is the primary live-detection channel;
`fallback_poll_interval` polling is the backup), session lifecycles, and
the stream-drop/reconnect decision tree. Byte-level IO belongs to the
seams. Recordings land under `record_root` (default `./recordings`,
gitignored); each session's `meta.json` is the history source of truth,
and `RecoverPending` finalizes sessions interrupted by a crash or
restart at startup.

## Room API Contract

`RoomService` is defined in `api/room/v1/room.proto` and exposes both HTTP
and gRPC transports:

| RPC | HTTP route | Purpose |
|---|---|---|
| `CreateRoom` | `POST /v1/rooms/create` | Create a room using the caller-provided `room_id`. |
| `ListRooms` | `POST /v1/rooms/list` | Query rooms with filtering, ordering, and pagination. |
| `GetRoom` | `POST /v1/rooms/get` | Get one room and its current runtime state. |
| `UpdateRoom` | `POST /v1/rooms/update` | Partially update a room. |
| `DeleteRoom` | `POST /v1/rooms/delete` | Delete a room by `room_id`. |

### Room message

Writable fields are `room_id`, `name`, and `enabled`. The following fields
are `OUTPUT_ONLY` and must be populated by the service:

- `live_status`: `LIVE_STATUS_UNSPECIFIED`, `LIVE_STATUS_PREPARING`, or
  `LIVE_STATUS_LIVE`.
- `record_status`: `RECORD_STATUS_UNSPECIFIED`, `RECORD_STATUS_IDLE`,
  `RECORD_STATUS_RECORDING`, `RECORD_STATUS_REMUXING`, or
  `RECORD_STATUS_ERROR`.
- `current_file`, `bytes_written`, `session_started_at`, `last_error`,
  `create_time`, and `update_time`.

`room_id` is the caller-provided unique platform room ID and is immutable.
`CreateRoomRequest.room` and `UpdateRoomRequest.room` are required;
`UpdateRoomRequest.update_mask` is also required. Updates currently allow
only the `name` and `enabled` paths. Invalid IDs or unsupported update paths
return `ERROR_REASON_INVALID_ARGUMENT`; duplicate IDs return
`ERROR_REASON_ALREADY_EXISTS`; missing rooms return
`ERROR_REASON_NOT_FOUND`.

`ListRoomsRequest` uses AIP pagination, filtering, and ordering. The default
page size is 20. Filterable and orderable persisted fields are `room_id`,
`name`, `enabled`, `create_time`, and `update_time`; runtime fields are not
accepted by storage filters or ordering. The response uses `rooms` and
`next_page_token`.

### Room runtime and recording

`RoomRegistry` loads persisted rooms at startup and holds mutable live and
recording state. The recorder updates the registry; room reads merge the
registry snapshot with persisted fields. For an actively recording room,
`SessionStatsRepo` best-effort supplies `current_file` and `bytes_written`.
CRUD changes are persisted immediately but newly created or updated rooms
are picked up by the recorder after the next restart. A platform live-state
refresh can backfill an empty room name and persists that change.

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
filtering, ordering, runtime merging, and validation against real sqlite
(`t.TempDir()` db file, wired like `wireApp`; pass `RemuxEnabled: false`
to skip the ffmpeg probe). Data-layer tests use the real filesystem and
a scripted fake ffmpeg binary, so no real ffmpeg is needed.

## Naming & error reasons

- Resource: `<Resource>` (the current resource is `Room`); collection RPC:
  `List<Resources>`.
- Types: repo `<Resource>Repo`, usecase `<Resource>Usecase`, service
  `<Resource>Service`. PO types live inside `internal/data/`; convert
  with `new<Resource>(do)` / `toBiz(po)` free functions.
- Error reasons: declared in `api/<domain>/<version>/error_reason.proto`,
  surfaced as `Err<Resource><Cause>` in `biz`.

## Commits & security

- Conventional Commits: `feat:`, `fix:`, `refactor:`, `chore(deps):`,
  `docs:`, `test:`.
- Real credentials (the Bilibili cookie) live only in
  `configs/credentials.yaml` (gitignored; copy
  `credentials.example.yaml`). Kratos merges every yaml in the `-conf`
  directory into one Bootstrap, so `config.yaml` keeps the sensitive
  fields empty. The `redis` block in `config.yaml` is a vestigial
  template placeholder — nothing reads it.

## Design docs

`docs/design/bili-recorder.md` (Chinese) is the deep-dive on the
recorder service: goroutine structure, room states, stream-drop decision
tree, on-disk layout (`meta.json`, danmaku JSONL), risk control, config
defaults, and failure handling. It defers repo-level conventions
(layering, naming, build commands) to this file — when changing template
rules, keep the two consistent.
