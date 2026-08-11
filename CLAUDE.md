# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

This is a Kratos (go-kratos/v3) service template. The included `Todo`
resource is reference code demonstrating API shape, layering, code
generation, and testing — replace it with the real domain model when
building a real service. Go module name is `suika`; the app entrypoint is
`cmd/suika/`.

## Commands

```bash
make init      # install generators (wire, buf) — needed once
make api       # regenerate api/ proto stubs + openapi.yaml (buf.gen.yaml)
make config    # regenerate internal/conf from conf.proto (buf.gen.config.yaml)
make all       # make api + make config + go generate (Wire) + go mod tidy
make build     # build all packages into ./bin/

go run ./cmd/suika -conf ./configs   # run locally (HTTP :8000, gRPC :9000)

go test ./...                                          # all tests
go test ./internal/service/                            # one package
go test ./internal/service/ -run TestTodoServiceCRUD   # one test
```

There is no configured linter. Proto codegen uses buf v2 with
buf.build/googleapis/googleapis as a BSR dependency (buf.lock committed).
All protoc plugins are pinned and invoked as `go run <plugin>@<version>`
from `buf.gen.yaml` / `buf.gen.config.yaml`, so nothing besides `buf` and
`wire` needs installing. Wire regeneration happens via `go generate` from
`cmd/suika/wire_gen.go`, so `make all` covers it.

Note: README.md and the Dockerfile predate the entrypoint rename and are
stale — the app is `cmd/suika` (README says `cmd/server`), `make build`
produces `bin/suika` (Dockerfile CMD runs `./server`), and there is no
`third_party/` directory. Trust the Makefile and this file.

Never hand-edit generated files: `*.pb.go`, `*_grpc.pb.go`, `*_http.pb.go`,
`wire_gen.go`, `openapi.yaml`. Regenerated files belong in the same commit
as their source.

## High-level architecture

A Kratos app wired together only in `cmd/` via Wire:
`wireApp(bc.Server, bc.Data, logger)` builds from four ProviderSets —
`server`, `service`, `biz`, `data` — plus `newApp` (main.go). Runtime
config (`configs/config.yaml`) is loaded and scanned into the generated
`internal/conf` proto types. Both HTTP and gRPC servers are started; the
HTTP server applies `recovery` and a `validate` middleware that enforces
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

- `service` imports `api/...` (DTO) and `biz` (DO). Never `data`.
- `biz` imports `api/...` only for error reason enums. Never `service`,
  never `data`. The repo interface declared here is the inversion seam.
- `data` imports `biz` to implement the repo interface. Never `service`,
  never DTOs.
- `cmd` is the only place that wires all layers via Wire.

A change crossing these arrows the wrong way is a layering bug; fix the
design rather than add the import.

### Layer responsibilities

**service (DTO ↔ DO)**

- `convert<Resource>` parses an incoming proto into a DO; the reverse
  direction is `convert<Resource>Reply` (DO → DTO), a free function so
  unary handlers and streaming helpers (e.g. `newTodoEvent`) share it.
  The reply type is whatever the proto declares — usually the resource
  itself (`return convert<Resource>Reply(do), nil`), sometimes a list
  wrapper (`*v1.<Resources>Set`), or `&emptypb.Empty{}` for deletes.
- Embed `Unimplemented<Resource>ServiceServer`.
- Parse AIP list requests via `filtering` / `ordering` / `pagination`
  (go.einride.tech/aip); apply `fieldmask.Update` for partial updates.
- Validate request inputs at the service boundary before delegating to
  the usecase. Return `biz` errors. No business rules, no storage
  access, no PO.
- The sample proto also demonstrates server-streaming and
  bidirectional-streaming RPCs.

**biz (DO only)**

- Owns the DO (`type <Resource> struct` — no proto, no storage tags),
  the usecase, and the repo interface (`type <Resource>Repo interface`).
- Owns typed errors built with `errors.NotFound` / `errors.BadRequest`
  plus the API error reason enum (e.g. `ErrTodoNotFound` from
  `v1.ErrorReason_TODO_NOT_FOUND`).
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
  clients. (The sample `Data` struct is empty: the sample repo is a
  mutex-protected in-memory map storing DOs directly — no PO, no
  conversion helpers — intentionally simple to show the flow, not a real
  query engine. The `database`/`redis` entries in `configs/config.yaml`
  are unused template placeholders.)
- _Querying_: translate `ListOptions.Filter` and `ListOptions.OrderBy`
  into the storage driver's query language inside the repo.
- _Errors_: map driver errors to `biz` typed errors so callers above
  never branch on the driver.

**server**

- Construct HTTP/gRPC servers, apply middleware, register services. No
  translation, no business logic.

### Add-a-resource checklist

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

### Testing seam

Tests live beside the code they cover (`*_test.go`). Test layers in
isolation: service tests fake the usecase, biz tests fake the repo, data
tests exercise repo implementations at the storage boundary. The existing
service test (`internal/service/todo_test.go`) drives the service
end-to-end through the real in-memory repo.

## Naming & error reasons

- Resource: `<Resource>` (e.g., `Todo`); collection RPC:
  `List<Resources>`.
- Types: repo `<Resource>Repo`, usecase `<Resource>Usecase`, service
  `<Resource>Service`. PO types live inside `internal/data/`; convert
  with `new<Resource>(do)` / `toBiz(po)` free functions.
- Error reasons: declared in `api/<domain>/<version>/error_reason.proto`,
  surfaced as `Err<Resource><Cause>` in `biz`.

## Commits & security

- Conventional Commits: `feat:`, `fix:`, `refactor:`, `chore(deps):`,
  `docs:`, `test:`.
- Never commit real credentials in `configs/config.yaml`.
- AGENTS.md carries the same layering contract for other agents; keep
  the two in sync when changing template rules.

## Agent skills

### Issue tracker

Issues live in GitHub Issues (github.com/xgbt/suika), operated via the `gh` CLI. See `docs/agents/issue-tracker.md`.

### Triage labels

Five canonical triage roles map 1:1 to same-named labels (`needs-triage`, `needs-info`, `ready-for-agent`, `ready-for-human`, `wontfix`). See `docs/agents/triage-labels.md`.

### Domain docs

Single-context: root `CONTEXT.md` + `docs/adr/` (created lazily by skills when needed). See `docs/agents/domain.md`.
