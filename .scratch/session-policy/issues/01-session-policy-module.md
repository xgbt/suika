# 01 — sessionPolicy: session start/stop policy into one home

**What to build:** The recorder's session start/stop/resume policy lives in one stateful session policy module that the Monitor's watch loop consults: the select arms deliver inputs (room info arrived, enabled flipped, session finished) and execute the returned decision (Start / Stop / None). The four duplicated decision branches and the four policy locals in the watch loop disappear, and the fallback-poll path gains its first end-to-end test coverage. Behaviour is preserved bit-for-bit; the module itself is covered by pure table-driven tests over the full decision matrix recorded in this feature's spec.

**Blocked by:** None — can start immediately

**Status:** resolved

- [x] The session policy exists as one concrete biz module: it owns the enabled flag, the latest RoomInfo, the session status (idle / running / finishing), and the resume-on-finish flag; it accepts the three inputs and returns Start(RoomInfo) / Stop / None per the feature spec's decision table. No interface, no registry or storage access, no mutex (owned by one Monitor goroutine).
- [x] Every row of the decision table in the feature spec has a test case, and the three must-preserve quirks are tested: stale-live resume, coalesced enable/disable signal netting out, and a stop trigger arriving while already finishing. Tests are pure and synchronous — no goroutines, no wall-clock waits, no fakes (prior art: the record loop's decision-tree tests).
- [x] The Monitor's watch loop contains no session start/stop/resume decision logic: arms only apply room info to the registry, deliver inputs to the module, and execute its decisions; the enabled cache, latest-RoomInfo, and resume-flag locals are gone.
- [x] The three existing watchRoom integration tests (offline control cancels the session; enabled gates sessions while live status stays visible; enable-during-stop resumes after finishing) pass unchanged as behaviour locks.
- [x] One new wiring test proves the fallback-poll path end-to-end: poll timer (shrunk via the existing same-package delay-knob pattern) fires → room info fetched → session starts.
- [x] The full test suite is green under `-mod=mod` (never vendor mode).
- [x] Naming respects the domain glossary and ADR-0001: "reconcile" stays reserved for the supervisor loop's monitor-set reconciliation; the session level is the session policy.

## Comments

- 2026-08-18 — Produced by `/to-tickets` from the grill-with-docs session and its spec. Originally drafted as two tickets (module+matrix tests, then delegation+wiring test); the maintainer chose a single unsplit ticket — the whole change fits one context window and one review.
- 2026-08-18 — Implemented via TDD (one red-green cycle per event type). New files: `internal/biz/session_policy.go` (module), `internal/biz/session_policy_test.go` (22 table-driven cases over the full decision matrix + the three quirks). `watchRoom` is now a dispatcher consulting the policy via `executeDecision`; behaviour locks pass unchanged, and `TestWatchRoomFallbackPollStartsSession` covers the poll path end-to-end (`watchClient` gained optional `pollInfo`/`pollCalls`, zero-value behaviour unchanged). Also fixed a pre-existing order-dependent assertion in `TestNewRoomRegistryLoadsRooms` (`Rooms()` iterates a map; the test asserted index order, failing ~1/10 runs) — required for the green-suite criterion; test-only change, separate `test:` commit.
