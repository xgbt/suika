# Spec: sessionPolicy — one home for the session start/stop policy

Status: ready-for-agent

## Problem Statement

The recorder decides when a room's Session starts, stops, and resumes inside `watchRoom`'s single select statement — and that policy is written **four times**, once per arm (danmaku room-state arrival, fallback poll, session finished, enabled flip). Answering "when does a session start?" requires reading all four branches, and the branches have already begun to drift: the fallback-poll copy is a verbatim duplicate that has **never executed in any test** (the default poll interval is 600s and no test overrides it, so its timer never fires). Every behavioural change to the recorder's core control flow risks silently diverging one copy from the others — and the commit history shows this file is reworked more often than any other in the repo.

## Solution

Collapse the four copies into one stateful **session policy** module. The Monitor's select arms become pure plumbing: they deliver inputs (room info arrived, enabled flipped, session finished) and execute the returned decision (start / stop / none). The resume-after-finish rule, the enabled gate, and the session phase (idle / running / finishing) live inside the module. `watchRoom` shrinks to a dispatcher that owns only goroutines, contexts, and the danmaku connection. Behaviour is preserved exactly — this is a pure refactor — and the fallback-poll decision path is tested for the first time.

## User Stories

1. As a recorder developer, I want the session start/stop/resume policy to have exactly one body, so that I can answer "when does a session start?" by reading one place.
2. As a recorder developer, I want to change the start/stop policy without hunting down four duplicated branches, so that my change cannot silently diverge one copy from the others.
3. As a recorder developer, I want the fallback-poll decision path covered by tests, so that the backup live-detection channel is no longer the least-trusted code in the recorder.
4. As a recorder developer, I want to test the session policy without spawning goroutines or waiting on wall-clock timers, so that the policy's full decision matrix runs fast and deterministically.
5. As a recorder developer, I want the resume-after-finish rule (enable arriving while a session is still finishing) encoded explicitly, so that this race window cannot regress when the code is reworked again.
6. As a recorder developer, I want the session's three phases (idle / running / finishing) represented explicitly, so that the "cancel sent, done not yet fired" window is a named state instead of an implicit invariant.
7. As a recorder maintainer, I want the existing watchRoom integration tests kept unchanged as behaviour locks, so that the refactor proves itself against today's observable behaviour.
8. As a recorder maintainer, I want strictly behaviour-preserving refactoring, so that recorded output, session boundaries, and room runtime states are bit-for-bit what they were before.
9. As a recorder maintainer, I want the term "reconcile" reserved for the supervisor loop's monitor-set reconciliation, so that the two levels of decision-making never get conflated in code or review.
10. As a recorder maintainer, I want no new interface introduced for the session policy, so that the codebase does not grow a seam with only one adapter.
11. As a recorder maintainer, I want the module to stay free of registry and storage access, so that the hidden persistence inside `ApplyRoomInfo` remains a separate, later concern.
12. As a code reviewer, I want the four select arms reduced to input-delivery and decision-execution, so that a diff touching session policy is obviously scoped.
13. As a code reviewer, I want the decision table recorded (in tests and this spec), so that I can check any future change against an explicit matrix instead of re-deriving it from code.
14. As an operator of the recorder daemon, I want disabled rooms to keep receiving and applying room-state updates, so that live status stays visible even while recording is gated off.
15. As an operator of the recorder daemon, I want creating a room to start its Monitor immediately and deleting a room to stop it immediately, exactly as today, so that CRUD changes keep taking effect in real time without restarts.
16. As an operator of the recorder daemon, I want disabling a room to stop an active recording and enabling to start one when the room is live, exactly as today, so that the enable/disable contract of the room API is unchanged.
17. As an implementer picking up this ticket, I want the complete state-and-event decision table in the spec, so that I never have to guess at an edge case the design session already settled.
18. As an implementer picking up this ticket, I want the known quirks of current behaviour listed as must-preserve, so that I don't "fix" them and break the behaviour-lock contract.
19. As a future developer adding a fifth input source for room info, I want one module to feed it into, so that the new source inherits the entire existing policy for free.
20. As a future developer writing the recorder's ADR history, I want this decision recorded in an ADR, so that the "why one stateful module and not a pure function" trade-off survives the next refactor.
21. As a reader of the domain glossary, I want "session policy" defined in the project's ubiquitous language, so that reviews and tickets use one vocabulary for this concept.
22. As a test author, I want the poll wiring verified end-to-end once (timer → room-info fetch → policy input → session start), so that module-level coverage is backed by proof that the Monitor actually consults the module.

## Implementation Decisions

- **One new stateful module in the biz layer: the session policy.** It is a concrete type with a single consumer (`watchRoom`); **no interface** is introduced for it — a seam with one adapter is not justified. It is owned exclusively by one Monitor goroutine and therefore needs no mutex.
- **Module state** (all previously scattered across `watchRoom` locals): the room's enabled flag, the latest known RoomInfo, the session phase (`idle` / `running` / `finishing`), and the resume-on-finish flag. The constructor takes the room's initial enabled flag.
- **Input vocabulary** — three events, delivered by the Monitor:
  1. *room info arrived* (carrying a RoomInfo) — shared by the danmaku room-state arm and the fallback-poll arm; the arms apply it to the RoomRegistry **before** feeding it to the module (registry application stays in the arms, out of the module).
  2. *enabled flipped* (carrying the new value, read from the registry by the arm) — no-op decision when the value equals the module's current state, which absorbs coalesced/duplicate signals.
  3. *session finished* — delivered when the session goroutine's done channel fires.
- **Output alphabet** — a three-value decision: `Start(RoomInfo)` / `Stop` / `None`. Resume is **not** a distinct decision: to the Monitor it is the same action as start (launch a session with the freshest known RoomInfo). `Start` carries the RoomInfo to use, so the Monitor never chooses between RoomInfo copies.
- **Complete decision table.** This table was derived line-by-line from current behaviour during the grilling session and is the acceptance criterion — every cell is a required test case:

| Event | Condition | Decision | State change |
|---|---|---|---|
| room info arrived | live · enabled · idle | **Start(info)** | phase → running |
| room info arrived | live · enabled · finishing | None | — |
| room info arrived | live · disabled (any phase) | None | — |
| room info arrived | not live · running | **Stop** | phase → finishing |
| room info arrived | not live · finishing | None (today: redundant idempotent cancel) | — |
| room info arrived | not live · idle | None | — |
| room info arrived | (all rows) | | latest ← arrived info |
| enabled flipped on | was off · idle · latest says live | **Start(latest)** | phase → running |
| enabled flipped on | was off · finishing | None | resume-on-finish ← true |
| enabled flipped on | was already on | None | — |
| enabled flipped off | was on · running | **Stop** | phase → finishing, resume-on-finish ← false |
| enabled flipped off | was on · finishing or idle | None (today: redundant idempotent cancel) | resume-on-finish ← false |
| session finished | resume-on-finish · latest says live | **Start(latest)** | phase → running, flag cleared |
| session finished | otherwise (incl. natural end — flag necessarily false) | None | phase → idle |

- **`watchRoom` becomes a dispatcher.** Arms keep only: input collection (including applying room info to the registry, noting poll errors on the registry, resetting the jittered poll timer), decision execution (`Start` → launch the session goroutine; `Stop` → cancel it), and non-policy plumbing (context cancellation, dropping danmaku events while idle). Its four policy locals (enabled cache, latest RoomInfo, resume flag, and active-session semantics) disappear.
- **Strict behaviour preservation.** Quirks that must survive verbatim, even where they look like bugs:
  - *Stale-live resume*: enable during finishing → finish completes → resume happens even if the stream is actually dead; the fresh session's record loop fails at stream-open and ends gracefully.
  - *Disabled-room visibility*: room-state events are still applied to the registry for disabled rooms; only session starts are gated.
  - *Signal coalescing*: a disable→enable pair coalesced into one signal nets out exactly as today.
- **Vocabulary decision**: "reconcile" remains reserved for the supervisor loop's monitor-set reconciliation; the session level is "policy" (recorded in the domain glossary and ADR-0001).
- **No change** to the RoomRegistry, the room CRUD path, the record loop's stream-drop decision tree, session storage, or any API/proto/web surface.

## Testing Decisions

- **What makes a good test here**: test external behaviour — the decision produced by an event sequence — never internal field layout. The policy module is deterministic, so its tests are pure table-driven sequences with no goroutines, no wall-clock waits, and no fakes at all.
- **Module under test: the session policy.** Table-driven coverage of every cell of the decision table above, plus the must-preserve quirk scenarios (stale-live resume, coalesced flips, stop arriving while already finishing). Prior art: the record loop's decision-tree tests in biz — pure, synchronous, no goroutines.
- **Behaviour locks: the three existing watchRoom integration tests** (offline control event cancels the session; enabled gates sessions while live status stays visible; enable-during-stop resumes after finishing) must pass unchanged. Prior art: they are the prior art — goroutine-driven, scripted fake danmaku connection, registry status polling.
- **One new wiring test at the Monitor level** proves the fallback-poll path end-to-end: timer fires → room-info fetch → fed to the policy → session starts. It reuses the existing fake danmaku connection / repo fakes and the existing same-package pattern of shrinking a delay knob (here the poll interval) to milliseconds, exactly as the reconnect-delay and redial-delay knobs are already shrunk in tests.
- **No new fakes and no new seams** are introduced by this work.

## Out of Scope

- The other five cards of the architecture review: RoomRegistry split (RoomCatalog / status board), the risk-control ladder combinator, patch-shaped partial updates, the meta.json state-machine owner, and the clock/dialer seams.
- The review's small deepenings: the dead `StreamHandle.URL` field, the stats-pump entry leak on room deletion, the daemon stop-timeout vs finish-grace-period contract, and the four-place DanmakuEvent shape.
- Any behaviour change. This spec's contract is pure refactor; suspected behaviour bugs get their own issues.
- Making the persistence inside `ApplyRoomInfo` visible — that belongs to the RoomRegistry-split card.
- Room CRUD, proto/API contracts, and the web frontend are untouched.

## Further Notes

- Recorded alongside this work: a domain glossary entry set for the recorder vocabulary (Room, Session, Monitor, fallback poll, session policy, Reconcile) and ADR-0001 (session start/stop policy lives in one stateful module). Future changes in this area must respect both.
- The decision table doubles as the review checklist: a change to session policy that does not correspond to a cell change is a structural regression by definition.
- Build/test conventions: `go test -mod=mod` (never vendor mode); the sqlite driver needs cgo, but nothing in this change touches storage.
- Suggested commit shape per repo convention: `refactor:` for the extraction (module + watchRoom), `test:` for the new policy and wiring tests — or a single `refactor:` commit containing both, since regenerated files are not involved.

## Comments

- 2026-08-18 — Produced by a `/grill-with-docs` session (grilling + domain-modeling) on the architecture review's top recommendation (C1). Design tree fully traversed; all decisions settled by the maintainer; behaviour-preservation contract confirmed. See ADR-0001 and CONTEXT.md.
