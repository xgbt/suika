# 02 — level-triggered sessionPolicy: dissolve the resume-on-finish flag into one criterion

**What to build:** The session policy stops branching per event. Every input (room info arrived, enabled flipped, session finished) updates the module's view of the world and evaluates one criterion — `shouldRecord`: enabled gate open and latest info says live — against the session status: idle and should-record → Start, running and should-not → Stop, finishing → no decision (stop is async). Resume after finishing needs no flag: it falls out of which status the session ended from — a stopped session (one that went through finishing) resumes iff the world says record at end time; a naturally ended session is itself the freshest evidence against recording and never restarts on stale live state. One decision-matrix row changes behaviour (deliberately): a live event arriving during a natural-stop finish now resumes recording after the finish completes instead of waiting up to a fallback-poll interval.

**Blocked by:** None — can start immediately

**Status:** resolved

- [x] `sessionPolicy` exposes the same three inputs and Start/Stop/None output alphabet; `watchRoom` is untouched and its integration tests pass unchanged.
- [x] One shared `decide` handles the idle→Start and running→Stop transitions for both room-info and enabled-flip inputs; the per-event branches and the resume-on-finish flag are gone.
- [x] Resume derivation: `SessionFinished` distinguishes stopped (status was finishing) from natural end; only a stopped session may resume, and only when `shouldRecord` holds at end time. Stale live state after a natural end (record loop gave up; the policy never sees the loop's probe) never restarts a session.
- [x] The feature spec's decision table is updated to the new truth, including the one changed row; every row still has a test. The three preserved quirks (stale-live resume after enable-during-finishing, coalesced signals netting out, no redundant stop while finishing) still hold and stay locked — stale-live resume and coalescing now fall out of the recompute instead of flag handling.
- [x] ADR-0002 records the decision; CONTEXT.md's "Session policy" glossary entry is updated.
- [x] The full test suite is green under `-mod=mod`, plus a `-race` run of the biz package.

## Comments

- 2026-08-22 — Motivated by an architecture review of the recorder: the edge-triggered design needed a flag-shaped patch ("enable arrived while we weren't looking") plus per-event branches; the level-triggered form replaces the patch class with one criterion. The first attempt recomputed unconditionally on `SessionFinished` and failed two matrix tests: after a natural end the policy's `latest` is stale (the record loop's probe reaches the registry but not the policy), so unconditional recompute restarted sessions that would immediately fail at stream-open and finish again — a start/finish flap creating session directories until the next fresh room info. The finishing-status marker at end time fixes it: status already distinguishes "externally stopped" from "gave up by itself". Net diff vs the extraction: `session_policy.go` 115 → ~120 lines (comment-heavy; logic roughly halved), one behaviour row changed, flag and its clearing rules gone.
