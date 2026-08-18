# Suika

Suika manages Bilibili live rooms and records their broadcasts. Two concerns share one in-memory registry: room management (room CRUD) and recording execution (the recorder daemon).

## Language

**Room**:
A Bilibili live room under management, identified by the platform's `room_id`. Carries persisted identity (streamer name, title, enabled flag) plus mutable live and recording runtime state.
_Avoid_: channel; "stream" refers to the media flow, not the room

**Session** (录制会话):
One recording session: everything recorded during one continuous broadcast of one room, from live start to end. A room has at most one session at a time.
_Avoid_: recording (ambiguous between the act and the files), broadcast

**Monitor** (监控协程):
The per-room goroutine that holds the room's danmaku connection and translates room events into session starts and stops. Every room has exactly one monitor, regardless of its enabled flag.
_Avoid_: watcher, poller (the fallback poll is only one of a monitor's inputs)

**Fallback poll** (回退轮询):
Periodic polling of the platform's room-info API that backs up the danmaku connection as the live-detection channel.
_Avoid_: heartbeat, health check

**Session policy** (会话启停策略):
The rules deciding when a room's session starts, stops, and resumes: gated by the room's enabled flag, driven by live status, with a resume rule for an enable that arrives while a session is still finishing.
_Avoid_: scheduler, controller

**Reconcile** (调和):
The supervisor loop's act of bringing the set of monitors in line with the room registry — adding, removing, and signaling monitors. The word is reserved for this level.
_Avoid_: using "reconcile" for session-level start/stop decisions — that is the session policy
