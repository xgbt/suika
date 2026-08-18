# 01 — sessionPolicy: session start/stop policy into one home

**What to build:** The recorder's session start/stop/resume policy lives in one stateful session policy module that the Monitor's watch loop consults: the select arms deliver inputs (room info arrived, enabled flipped, session finished) and execute the returned decision (Start / Stop / None). The four duplicated decision branches and the four policy locals in the watch loop disappear, and the fallback-poll path gains its first end-to-end test coverage. Behaviour is preserved bit-for-bit; the module itself is covered by pure table-driven tests over the full decision matrix recorded in this feature's spec.

**Blocked by:** None — can start immediately

**Status:** ready-for-agent

- [ ] The session policy exists as one concrete biz module: it owns the enabled flag, the latest RoomInfo, the session phase (idle / running / finishing), and the resume-on-finish flag; it accepts the three inputs and returns Start(RoomInfo) / Stop / None per the feature spec's decision table. No interface, no registry or storage access, no mutex (owned by one Monitor goroutine).
- [ ] Every row of the decision table in the feature spec has a test case, and the three must-preserve quirks are tested: stale-live resume, coalesced enable/disable signal netting out, and a stop trigger arriving while already finishing. Tests are pure and synchronous — no goroutines, no wall-clock waits, no fakes (prior art: the record loop's decision-tree tests).
- [ ] The Monitor's watch loop contains no session start/stop/resume decision logic: arms only apply room info to the registry, deliver inputs to the module, and execute its decisions; the enabled cache, latest-RoomInfo, and resume-flag locals are gone.
- [ ] The three existing watchRoom integration tests (offline control cancels the session; enabled gates sessions while live status stays visible; enable-during-stop resumes after finishing) pass unchanged as behaviour locks.
- [ ] One new wiring test proves the fallback-poll path end-to-end: poll timer (shrunk via the existing same-package delay-knob pattern) fires → room info fetched → session starts.
- [ ] The full test suite is green under `-mod=mod` (never vendor mode).
- [ ] Naming respects the domain glossary and ADR-0001: "reconcile" stays reserved for the supervisor loop's monitor-set reconciliation; the session level is the session policy.

## Comments

- 2026-08-18 — Produced by `/to-tickets` from the grill-with-docs session and its spec. Originally drafted as two tickets (module+matrix tests, then delegation+wiring test); the maintainer chose a single unsplit ticket — the whole change fits one context window and one review.
