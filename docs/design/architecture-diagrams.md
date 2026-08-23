# Suika 架构图集

基于当前代码（2026-08 快照）绘制。Suika 是一个 Kratos（go-kratos/v3）单体服务，负责 B 站直播间管理与直播录制。术语遵循仓库根目录 `CONTEXT.md`（Monitor = 每房间监控协程；Reconcile 仅指监督循环的监控集合调和；会话启停决策属于 sessionPolicy）。

| 分类 | 图表 | 核心作用 / 解决什么问题 |
|---|---|---|
| 宏观架构 | [系统/部署架构图](#1-系统部署架构图) | 划分服务边界：单进程、SQLite、录制文件目录、外部 B 站平台如何连接 |
| 代码分层 | [应用架构图](#2-应用架构图) | 规定 server / service / biz / data 的调用层级与依赖方向 |
| 动态交互 | [时序图](#3-时序图) | 梳理房间 CRUD、开播检测、录制会话的调用链，明确同步/异步与入参出参 |
| 状态流转 | [状态机图](#4-状态机图) | Room 录制状态、会话策略阶段、meta.json 会话状态的变化与触发事件 |
| 数据建模 | [ER 图](#5-er-图) | rooms 表结构与录制会话（meta.json 逻辑实体）的一对多关系 |

配套深读：`docs/design/bili-recorder.md`（录制器细节）、`docs/adr/0001-session-policy-module.md`（会话策略模块决策）。

---

## 1. 系统/部署架构图

**解决的问题**：服务边界在哪、依赖什么基础设施。

Suika 是**单进程、单机部署**：一个 `kratos.App` 内并行运行三个 `transport.Server` —— HTTP（:8000）、gRPC（:9000）与录制守护进程（`server.Daemon`）。没有 API 网关、没有消息队列，也不依赖其他基础设施。

```mermaid
flowchart TB
    subgraph CLIENT["客户端"]
        WEB["浏览器 · React SPA（web/）<br/>vite dev 代理 /v1 → localhost:8000<br/>5s 自动刷新"]
        GRPCCLI["gRPC 客户端（可选，当前无消费方）"]
    end

    subgraph SUIKA["suika 进程 · kratos.App（go run ./cmd/suika -conf ./configs）"]
        HS["HTTP Server :8000<br/>中间件：recovery + validate（field_behavior REQUIRED）"]
        GS["gRPC Server :9000"]
        DM["录制守护进程 Daemon<br/>监督循环 + 每房间 Monitor"]
        HS -.- GS
        HS -.- DM
    end

    DB[("SQLite 文件 ./data/suika.db<br/>唯一表 rooms · GORM 单连接")]
    REC[("录制目录 ./recordings/**<br/>FLV/MP4 + 弹幕 JSONL + meta.json")]
    FF["ffmpeg 子进程（可选）<br/>remux_enabled=true 时 FLV→MP4<br/>启动期探测，缺失则启动失败"]

    subgraph BILI["B 站平台（外部依赖，全部经 LiveClient 出入）"]
        API["api.live.bilibili.com<br/>房间信息 getInfoByRoom<br/>流地址 getRoomPlayInfo<br/>弹幕 token getDanmuInfo/getConf"]
        CDN["直播 CDN<br/>FLV 流 HTTP 长连接"]
        DMWS["弹幕 WebSocket<br/>broadcastlv.chat.bilibili.com"]
    end

    WEB -- "POST /v1/rooms/{create,list,get,update,delete}" --> HS
    GRPCCLI -- "RoomService RPC" --> GS
    HS -- "房间 CRUD" --> DB
    DM -- "平台身份信息回写（rooms 表）" --> DB
    DM -- "会话目录 / 分段 / meta.json 读写" --> REC
    DM -- "exec 转封装" --> FF
    DM -- "HTTPS（WBI 签名 + cookie + buvid，riskGuard 统一风控）" --> API
    DM -- "HTTPS 长连接拉流（固定请求 10000 原画）" --> CDN
    DM -- "WSS：弹幕事件 + 房间状态事件（开播主探测通道）" --> DMWS
```

要点：

- **唯一的图化消费方是 Web SPA**；HTTP 与 gRPC 暴露同一份 `api/room/v1/room.proto` 契约。
- 所有 B 站流量收敛在 `LiveClient` 一个缝（`data/bili_api.go`、`danmaku.go`、`wbi.go`、`buvid.go`，风控编排集中在 `risk.go` 的 `riskGuard`：冷却门、412/403/429 与 -352 刷新重试、旧接口降级）。
- 录制产物是**文件系统**而非数据库；`meta.json` 是录制历史的唯一事实源，重启后由 `RecoverPending` 扫描恢复。

---

## 2. 应用架构图

**解决的问题**：进程内部的分层与依赖方向。对应通用三层概念：service ≈ Controller 边界（DTO↔DO），biz ≈ Service（领域决策），data ≈ DAO（PO 与 IO）。

三种模型形状跨层流转：**DTO**（`api/room/v1` proto 请求/响应）→ **DO**（`biz` 纯领域对象）→ **PO**（`data` 存储形状 `roomPO`）。

```mermaid
flowchart TB
    subgraph CMD["cmd/suika —— 唯一装配点（Wire）"]
        WIRE["wireApp(bc.Server, bc.Data, bc.Recorder, logger)"]
    end

    subgraph SERVER["internal/server —— 只做传输，无业务逻辑"]
        SHTTP["NewHTTPServer"]
        SGRPC["NewGRPCServer"]
        SDMN["Daemon<br/>Start: 派生 context.Background 跑 Run<br/>Stop: cancel + 45s 上限等待"]
    end

    subgraph SERVICE["internal/service —— DTO ↔ DO"]
        RS["RoomService<br/>convertRoom / convertRoomReply<br/>AIP pagination + fieldmask"]
    end

    subgraph BIZ["internal/biz —— DO 与决策，不碰存储客户端"]
        RU["RoomUsecase<br/>房间 CRUD + 运行时合并"]
        RECU["RecorderUsecase<br/>监督循环 / Monitor / 断流决策树<br/>只做决策，不做字节级 IO"]
        REG["RoomRegistry<br/>房间配置 + 运行时状态的唯一事实源"]
        POL["sessionPolicy（ADR-0001）<br/>会话启停决策：Start / Stop / None"]
        IF1[/"RoomRepo 接口"/]
        IF2[/"RecorderRepo 接口"/]
        IF3[/"LiveClient 接口"/]
        IF4[/"SessionStatsRepo 接口"/]
    end

    subgraph DATA["internal/data —— PO 与全部 IO"]
        RR["roomRepo · roomPO → rooms 表<br/>toRoomPO / toRoomDO"]
        RREPO["recorderRepo<br/>会话目录 · FLV 解析写入（flv/）<br/>弹幕 JSONL · meta.json · remux"]
        LC["liveClient<br/>bili_api · danmaku · wbi · buvid · risk"]
    end

    DTO["api/room/v1<br/>proto DTO（RoomService 契约）"]

    WIRE --> SHTTP & SGRPC & SDMN
    SHTTP & SGRPC --> RS
    RS -- "DTO（请求/响应）" --- DTO
    RS -- "DO" --> RU
    SDMN --> RECU
    RU --> REG
    RECU --> REG
    RECU --> POL
    RU --> IF1 & IF4
    RECU --> IF2 & IF3
    IF1 -. 实现 .-> RR
    IF4 -. 实现（同一 recorderRepo） .-> RREPO
    IF2 -. 实现 .-> RREPO
    IF3 -. 实现 .-> LC
    RR --> SQLITE[("SQLite / GORM")]
    RREPO --> FS[("recordings/ 文件目录 + ffmpeg")]
    LC --> BILI[("B 站 API / CDN / 弹幕 WS")]
```

分层纪律（违反箭头方向即分层错误）：

| 层 | 拥有 | 边界语言 | 禁止 |
|---|---|---|---|
| service | — | DTO ↔ DO | PO、存储客户端、业务规则 |
| biz | DO、用例、仓储接口 | DO | DTO、PO、存储客户端 |
| data | PO、`toXxxPO/toXxxDO` | DO ↔ PO | DTO |
| server | 传输装配 | — | 转换与业务逻辑 |

两条关键倒置缝都在 `biz` 声明、`data` 实现：`LiveClient`（平台 IO）与 `RecorderRepo`（磁盘 IO）；`RoomRepo` 同理。`RoomRegistry` 是被两侧共享的运行时状态中枢：房间 CRUD 落库后同步 `Add/Update/Remove`，录制守护进程经 `Subscribe` 的合并式信号实时调和监控集合，无需重启。

---

## 3. 时序图

四张时序图覆盖全部调用链：① 房间创建（同步 CRUD 全链）、② 开播检测与会话启动（异步事件驱动）、③ 录制会话与断流决策树、④ 房间查询（运行时状态合并）。

### 3.1 房间创建：CreateRoom 同步链

入参 `CreateRoomRequest{room}`，出参 `CreateRoomResponse{room}`；重复 `room_id` 返回 `ERROR_REASON_ALREADY_EXISTS`（HTTP 409）。

```mermaid
sequenceDiagram
    autonumber
    participant C as 客户端（Web SPA）
    participant H as HTTP Server
    participant S as RoomService
    participant U as RoomUsecase
    participant R as RoomRepo（SQLite）
    participant G as RoomRegistry
    participant D as 监督循环（RecorderUsecase.Run）

    C->>+H: POST /v1/rooms/create　body=CreateRoomRequest{room}
    Note over H: 中间件 recovery + validate<br/>（校验 REQUIRED 字段）
    H->>+S: CreateRoom(ctx, req)
    Note over S: toRoomDO：DTO → DO
    S->>+U: CreateRoom(ctx, *biz.Room)
    U->>U: 校验 room_id > 0
    U->>+R: CreateRoom(ctx, room)
    alt room_id 已存在
        R-->>U: ErrRoomAlreadyExists
        U-->>S: 错误（ALREADY_EXISTS）
        S-->>H: 错误响应
        H-->>C: 409
    else 落库成功
        R-->>-U: *Room（含 create_time / update_time）
        U->>G: Add(room)
        G--)D: 合并式唤醒信号（异步通道，最多积压一个）
        Note over D: reconcile：为该房间启动 Monitor<br/>（无论 record_enabled，监控立即开始）
        U-->>-S: *RoomRuntime
        Note over S: toRoomDTO：DO → DTO
        S-->>-H: CreateRoomResponse{room}
        H-->>-C: 200 OK
    end
```

同构变体：

- **UpdateRoom**：service 先 `GetRoom` 读当前值 → `fieldmask.Update` 合并（仅允许 `streamer_name` / `room_title` / `record_enabled` 路径）→ 落库 → `registry.Update`。`record_enabled` 翻转经监督循环以重评估信号送达 Monitor，实时启停录制。
- **DeleteRoom**：落库 → `registry.Remove` → reconcile 立即取消该房间 Monitor（活跃会话优雅停止，已录文件保留），返回 `DeleteRoomResponse{empty}`。

### 3.2 开播检测与会话启动（异步事件驱动）

Monitor（`watchRoom`）的 select 分支只投递输入，启停裁决全部来自 `sessionPolicy`：Start(info) / Stop / None。

```mermaid
sequenceDiagram
    autonumber
    participant M as Monitor（watchRoom）
    participant LC as LiveClient
    participant DC as DanmakuConn（弹幕 WS）
    participant P as sessionPolicy
    participant G as RoomRegistry
    participant R as RoomRepo（SQLite）
    participant Sess as 会话协程（runSession）

    M->>LC: DanmakuConn(ctx, roomID)
    LC->>DC: 取 token（getDanmuInfo，降级 getConf）→ 拨号 + 认证
    LC-->>M: DanmakuConn
    Note over M: 启动兜底轮询定时器（默认 600s ± 抖动）

    par 主通道：弹幕 WS 房间状态事件
        DC--)M: RoomStateUpdates：RoomInfo{Live:true, Title, StreamerName}
        M->>G: ApplyRoomInfo(roomID, info)
        G->>R: UpdateRoom 回写（失败仅 warn，内存保留新值）
        M->>P: RoomInfoArrived(info)
        P-->>M: Start(info)　（record_enabled 且 phase=idle 时）
        M->>Sess: launchSession：启动会话协程
    and 兜底通道：轮询定时器到期
        M->>LC: GetRoomInfo(roomID)
        LC-->>M: RoomInfo
        M->>G: ApplyRoomInfo(roomID, info)
        M->>P: RoomInfoArrived(info)
        P-->>M: Start / Stop / None
    and 重评估信号：record_enabled 翻转
        Note over M: 监督循环 reconcile 投递 roomChanged
        M->>P: RecordEnabledFlipped(record_enabled)
        P-->>M: 开启录制且在播 → Start；关闭录制且在录 → Stop；其余 None
    end
```

### 3.3 录制会话：拉流、断流决策树、收尾

会话协程独占完整生命周期：槽位 → 准备 → 录制循环 → 收尾/转封装。录制状态写入 `RoomRegistry`（RECORDING → REMUXING → IDLE/ERROR，见 §4.1）。

```mermaid
sequenceDiagram
    autonumber
    participant Sess as 会话协程（runSession）
    participant G as RoomRegistry
    participant RR as RecorderRepo
    participant LC as LiveClient
    participant CDN as 直播 CDN
    participant FS as recordings/ 文件目录
    participant FF as ffmpeg

    Sess->>Sess: acquireSlot（max_concurrent=2，满则阻塞等待）
    Sess->>G: StartRecording(roomID)
    Sess->>RR: PrepareSession(session)
    RR->>FS: mkdir 会话目录；写 meta.json（status=recording）

    loop recordLoop 断流决策树（每轮一路新分段）
        Sess->>LC: OpenLiveStream(roomID)
        LC->>CDN: getRoomPlayInfo 选流 → GET FLV 长连接
        LC-->>Sess: LiveStream{URL, Quality, Body}
        Sess->>RR: RecordSession(session, stream, events)
        RR->>FS: 开分段写 FLV（按关键帧切分，默认 120min）<br/>弹幕事件写 JSONL；健康检查（30s × 3 轮无新数据即失败）<br/>速度采样（1s）更新 pumpStats
        RR-->>Sess: RecordingResult{BytesWritten, Parts}, err
        Sess->>LC: GetRoomInfo(roomID)　探测是否仍在播
        LC-->>Sess: RoomInfo
        alt 未在播 / 探测失败 / ctx 取消
            Note over Sess: 退出循环，进入收尾
        else 仍在播 且 ErrStreamTransient（404/连接重置）
            Note over Sess: CDN 瞬时预算--（默认 5）<br/>指数退避 2s→60s 后重连
        else 仍在播 且其他错误
            Note over Sess: 次数<3：等 10s 重连<br/>否则带已录内容收尾
        end
    end

    Sess->>G: SetRemuxing(roomID)
    Sess->>RR: FinishSession（脱离运行 ctx，30s 宽限）
    RR->>FS: meta.json status=remuxing，记 end_time
    RR->>FF: 逐分段 FLV→MP4（remux_enabled=true；失败保留 flv）
    FF-->>RR: MP4 产物（校验非空）
    RR->>FS: meta.json status=done（全成功）/ partial（有失败）
    Sess->>G: FinishRecording(roomID)
    Sess->>Sess: releaseSlot
```

### 3.4 房间查询：运行时状态合并（同步读链）

`GetRoom` 与 `ListRooms` 共用 `withRuntime` 合并：SQLite 持久化字段 + `RoomRegistry` 运行时快照 + （录制中时）`SessionStatsRepo` 写入进度。

```mermaid
sequenceDiagram
    autonumber
    participant C as 客户端
    participant S as RoomService
    participant U as RoomUsecase
    participant R as RoomRepo（SQLite）
    participant G as RoomRegistry
    participant RR as RecorderRepo（pumpStats 原子计数）

    C->>+S: ListRoomsRequest{page_size, page_token,<br/>可选 room_id / streamer_name / room_title / record_enabled}
    Note over S: 解析 AIP 分页；组装 biz.ListQuery<br/>（可选字段等值 AND，缺省不筛选）
    S->>+U: ListRoomRuntimes(ctx, query)
    U->>+R: ListRooms(query)　ORDER BY room_id ASC
    R-->>-U: []*Room（PO → DO）
    loop 每个房间
        U->>G: runtime(roomID)：live/record 状态快照
        opt record_status == RECORDING
            U->>RR: SessionStats(roomID)
            RR-->>U: {current_file, bytes_written, download_speed_bps}
        end
    end
    U-->>-S: []*RoomRuntime
    S-->>-C: ListRoomsResponse{rooms, next_page_token}
```

（`GetRoom` 同链，仅以 `room_id` 单查；`page_size` 缺省 20。）

---

## 4. 状态机图

### 4.1 Room 录制状态 RecordStatus（核心实体状态）

状态由 `RoomRegistry` 持有，经房间 API 读取（`GetRoom`/`ListRooms` 的 OUTPUT_ONLY 字段 `record_status`）。`NoteError` 只记 `last_error`，**不**改变录制状态。

```mermaid
stateDiagram-v2
    [*] --> IDLE : 房间登记进注册表
    IDLE --> RECORDING : StartRecording（会话协程取得槽位后）
    RECORDING --> REMUXING : SetRemuxing（录制循环结束）
    REMUXING --> IDLE : FinishRecording（FinishSession 成功）
    RECORDING --> ERROR : FailRecording（PrepareSession 失败）
    REMUXING --> ERROR : FailRecording（FinishSession 失败）
    ERROR --> RECORDING : 下一次会话启动（StartRecording 覆盖）
```

伴生的直播状态 `LiveStatus`：`UNSPECIFIED → PREPARING / LIVE`，由 `ApplyRoomInfo` 依据平台 `RoomInfo.Live` 双向切换。

### 4.2 会话策略阶段 sessionPolicy.phase（ADR-0001）

每个 Monitor 独享一个策略实例；阶段 + `record_enabled` 门控 + 最新房间信息共同裁决。"收尾中重新开启录制"由 `resumeOnFinish` 标志承接。

```mermaid
stateDiagram-v2
    [*] --> idle : newSessionPolicy(record_enabled)
    idle --> running : RoomInfoArrived(在播) 且 record_enabled；或 RecordEnabledFlipped(true) 且最新信息在播
    running --> finishing : RoomInfoArrived(停播)；或 RecordEnabledFlipped(false)
    finishing --> running : SessionFinished 且 resumeOnFinish 且仍在播（恢复录制）
    finishing --> idle : SessionFinished（无恢复标志）
```

### 4.3 录制会话状态（meta.json 的 `status` 字段）

会话级事实源是磁盘上的 `meta.json`；进程崩溃/重启后由 `RecoverPending` 扫描 `recordings/*/*/*.meta.json` 续做。

```mermaid
stateDiagram-v2
    [*] --> recording : PrepareSession（开播建目录时）
    recording --> remuxing : FinishSession；或重启恢复（RecoverPending）
    remuxing --> done : 所有分段转封装成功
    remuxing --> partial : 至少一个分段失败（保留 flv）
    partial --> done : RecoverPending 重试 flv_kept 的失败分段
```

分段级 `remux_status`：`pending → ok / failed`（失败且 `flv_kept=true` 才可在下次启动时重试）。

---

## 5. ER 图

数据库中**只有一张表 `rooms`**（GORM AutoMigrate，SQLite 单连接）。录制会话与分段不落库，而是以 `meta.json` + 媒体文件持久化在录制目录，图中作为逻辑实体给出，关系均为一对多：

```
recordings/<room_id>_<主播名>/<开播日期>/<日期_时间_标题>.meta.json
                                            .<日期_时间_标题>_partN.flv|.mp4
                                            .<日期_时间_标题>_partN.danmu.jsonl
```

```mermaid
erDiagram
    ROOMS ||--o{ SESSION : "一个房间 N 次开播（文件系统，按 room_id+开播时间定位）"
    SESSION ||--|{ SEGMENT : "一次会话 N 个分段（断流重连/按时长切分产生）"
    SEGMENT ||--o| DANMAKU : "每个分段配一个弹幕文件"

    ROOMS {
        int64 room_id PK "平台房间 ID，调用方提供，不可变"
        string streamer_name "主播名（可被平台回写覆盖）"
        string room_title "房间标题（可被平台回写覆盖）"
        bool record_enabled "是否录制该房间（仅门控录制，不影响监控）"
        datetime create_time "GORM autoCreateTime"
        datetime update_time "GORM autoUpdateTime"
    }

    SESSION {
        int64 room_id "所属房间"
        string room_name "主播名（上限 32 字符）"
        string title "直播标题（上限 64 字符）"
        int64 live_start_time "开播时间，决定目录与文件名前缀"
        int64 end_time "收尾时间"
        int32 quality_qn "实际授予的清晰度"
        string status "recording / remuxing / done / partial"
    }

    SEGMENT {
        int part "分段编号，会话内单调递增（扫描目录推导）"
        string video "flv 或 mp4 文件名"
        bool flv_kept "转封装失败时保留源 flv"
        int64 wall_start "墙钟开始（unix）"
        int64 wall_end "墙钟结束（unix）"
        int64 ts_start "FLV 时间戳起点（ms）"
        int64 ts_end "FLV 时间戳终点（ms）"
        int64 bytes "分段字节数"
        string remux_status "pending / ok / failed"
    }

    DANMAKU {
        int64 ts "事件时间（unix）"
        string type "danmaku / gift / superchat / guard / entry_effect / interact_word"
        int64 uid "用户 ID"
        string text "弹幕 / SC / 进场文本"
        json raw "原始 JSON 载荷"
    }
```

说明：

- `SESSION` / `SEGMENT` / `DANMAKU` 三个实体对应 `meta.json` 的 `sessionMeta` / `segmentMeta` 结构与弹幕 JSONL 行（`danmuLine`），由 `data` 层独占读写，**没有外键约束**——关联键是目录路径与文件名约定，而非数据库引用。
- `rooms` 表的 `streamer_name` / `room_title` 会被录制守护进程经 `RoomRegistry.ApplyRoomInfo` 用平台非空值覆盖回写；回写失败只记 warn，不影响内存快照。
- 运行时的写入进度（`current_file` / `bytes_written` / `download_speed_bps`）不在任何持久层，来自 `recorderRepo` 内存中的 `pumpStats` 原子计数，仅在 `record_status=RECORDING` 时 best-effort 提供给查询。
