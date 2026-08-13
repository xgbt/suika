# Suika DDD 领域模型设计

本文抽离自录播技术文档，专注描述 suika 当前实现的 DDD 领域建模。

## 1. 子域划分

当前实现可抽象为两个协作子域：

- 房间管理子域（Room Management）：管理可录制房间的持久化配置与查询视图。
- 录制执行子域（Recording Execution）：负责开播感知、录制场次编排、断流重连与收尾。

## 2. 领域模型图

```mermaid
classDiagram
direction LR

class Room {
  +int64 RoomID
  +string Name
  +bool Enabled
  +time CreateTime
  +time UpdateTime
}

class RoomRuntime {
  +Room Room
  +LiveState Live
  +RecordState Record
  +string CurrentFile
  +int64 BytesWritten
  +time SessionStartedAt
  +string LastError
}

class RoomRegistry {
  +Rooms() []Room
  +Room(roomID) Room
  +ApplyRoomInfo(ctx, roomID, info)
  +StartRecording(roomID)
  +SetRemuxing(roomID)
  +FailRecording(roomID, err)
  +FinishRecording(roomID)
  +NoteError(roomID, err)
}

class RoomUsecase {
  +GetRoom(ctx, roomID) RoomRuntime
  +ListRooms(ctx, opts) []RoomRuntime
  +CreateRoom(ctx, room) RoomRuntime
  +UpdateRoom(ctx, room) RoomRuntime
  +DeleteRoom(ctx, roomID)
}

class RecorderUsecase {
  +Run(ctx)
}

class Session {
  +int64 RoomID
  +string RoomName
  +string Title
  +time LiveStartTime
  +StreamQuality Quality
}

class RoomInfo {
  +int64 RoomID
  +bool Live
  +string Title
  +string StreamerName
  +time LiveStartTime
}

class SessionStats {
  +string CurrentFile
  +int64 BytesWritten
}

class RoomRepo {
  <<repository interface>>
  +FindByRoomID(ctx, roomID) Room
  +ListRooms(ctx, opts) []Room
  +CreateRoom(ctx, room) Room
  +UpdateRoom(ctx, room) Room
  +DeleteRoom(ctx, roomID)
}

class SessionStatsRepo {
  <<repository interface>>
  +SessionStats(ctx, roomID) SessionStats
}

class RecorderRepo {
  <<repository interface>>
  +PrepareSession(ctx, session)
  +RecordSession(ctx, session, stream, events)
  +FinishSession(ctx, session)
  +RecoverPending(ctx)
}

class LiveClient {
  <<acl interface>>
  +RoomStatus(ctx, roomID) RoomInfo
  +OpenStream(ctx, roomID)
  +DanmakuConn(ctx, roomID)
}

RoomRuntime *-- Room : composed from
RoomUsecase ..> RoomRepo : write/read persisted state
RoomUsecase ..> RoomRegistry : merge runtime snapshot
RoomUsecase ..> SessionStatsRepo : enrich recording progress

RoomRegistry o-- Room : holds startup snapshot
RoomRegistry ..> RoomRepo : backfill streamer name
RoomRegistry ..> RoomInfo : apply live info

RecorderUsecase ..> RoomRegistry : update runtime state machine
RecorderUsecase ..> RecorderRepo : recording IO
RecorderUsecase ..> LiveClient : platform access
RecorderUsecase ..> Session : session lifecycle

SessionStatsRepo ..> SessionStats
```

## 3. 建模说明

- Room 是持久化领域对象；RoomRuntime 是查询视图（读模型），由 Room 与运行时状态拼装。
- RoomRegistry 是运行时状态容器，不直接替代持久化；CRUD 仍以 RoomRepo 为准。
- RecorderUsecase 只做编排决策；平台与存储 IO 分别经 LiveClient 与 RecorderRepo 两条缝完成。
- SessionStatsRepo 是面向房间查询的窄接口，避免房间用例依赖完整录制仓储能力。
