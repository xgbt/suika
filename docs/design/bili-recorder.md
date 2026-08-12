# Bilibili 直播自动录播服务 — 技术文档

基于当前实现代码（2026-08 快照）。suika 是一个 Kratos (go-kratos/v3)
常驻进程，Bilibili 直播录播是其唯一业务域：Todo 样例资源已整体移除。
仓库级的分层契约、命名约定与构建命令见根目录 CLAUDE.md，本文只描述
录播服务本身"现在是什么、怎么跑"。

---

## 1. 概述

```
监控多个直播间（房间清单持久化在 sqlite，经 Room CRUD API 管理；
  常驻弹幕 WS + 轮询兜底）
  → 检测到开播：拉取原画 FLV 流直接落盘 + 同步录制全部弹幕事件（JSONL）
  → 录制中：断流自动重连（独立 CDN 瞬态预算）、按关键帧定时切段、健康巡检
  → 下播/收尾：meta.json 定稿，FLV remux 为 MP4（注入容器元数据），删除源 FLV
  → 文件落本地磁盘（record_root，默认 ./recordings）
  → 房间 CRUD + 运行状态 API：RoomService（HTTP/gRPC）
```

明确不做：弹幕合并进视频、切片、上传、转码、外部通知、标题过滤
（无条件全录）、录像自动删除、录制的手动起止（房间增删改查已由 CRUD
API 提供，见 §8）、历史查询 API（磁盘上的 meta.json 即历史）。

---

## 2. 架构

### 2.1 进程模型

录播守护进程实现 Kratos 的 `transport.Server` 接口（`Start(ctx)/Stop(ctx)`），
作为第三个 server 与 HTTP/gRPC 并列注册进 `kratos.Server(gs, hs, rj)`，
同生命周期启停（`internal/server/recorder_job.go` 的 `RecorderJob`）：

- `Start` 在 goroutine 里运行 `usecase.Run(rctx)` 并**立即返回**。
  `rctx` 派生自 `context.Background()` 而非传入的 ctx——Start 返回后
  kratos 可能取消其 ctx，守护循环不能因此被杀；取消句柄由 job 自己持有。
- `Stop` 取消 `rctx` 并等待主循环退出，等待上限为传入 ctx 与
  `stopWaitTimeout = 45s` 中先到者，保证停机永不卡死。45s 覆盖了 biz
  层的 `finishGracePeriod = 30s`（见 §3.4）加余量。

### 2.2 代码布局（逐文件）

```
api/room/v1/
  room.proto             DTO：RoomService 五个 CRUD RPC（AIP-13x 风格）/
                         Room（持久字段 + OUTPUT_ONLY 运行时字段）/ RoomSet 分页信封 /
                         LiveStatus / RecordStatus
  error_reason.proto     ErrorReason 枚举（ROOM_INTERNAL / ROOM_NOT_FOUND /
                         ROOM_INVALID_ARGUMENT / ROOM_ALREADY_EXISTS）
  *.pb.go / *_grpc.pb.go / *_http.pb.go   make api 生成，禁止手改

internal/biz/
  room.go                DO：Room（RoomID/Name/Enabled/CreateTime/UpdateTime）/
                         LiveState / RecordState 枚举 / RoomRuntime / roomState
                         类型化错误：ErrRoomNotFound（404）/ ErrRoomInvalidArgument（400）/
                         ErrRoomAlreadyExists（409，errors.Conflict）
                         RoomRepo 接口（FindByRoomID / ListRooms / CreateRoom /
                         UpdateRoom / DeleteRoom）+ ListOption（filter / order_by /
                         offset / limit 组合查询）——房间持久化缝
                         RoomRegistry：启动时从 RoomRepo 全量加载房间 + 运行时状态
                         （mutex）；daemon 写状态，room API 读快照；ApplyRoomInfo
                         回填主播名并经 repo 写回 sqlite
                         SessionStatsRepo —— 窄统计缝（biz 声明，room API 专用）
                         RoomUsecase：房间 CRUD（写走 repo，读合并 registry 运行时状态）
  recorder.go            DO：RoomInfo / StreamQuality / StreamHandle(opaque)
                         DanmakuEvent / Session / SessionResult / SessionStats
                         事件类型常量、默认值常量
                         类型化错误：ErrRoomInternal（errors.InternalServer + reason 枚举）
                         哨兵错误：ErrStreamTransient / ErrRiskControl（供决策树分类）
                         两条反向依赖缝（声明在 biz）：
                           RecorderRepo —— 存储缝；LiveClient —— 外部平台缝
                         DanmakuConn —— 常驻弹幕 WS 的 biz 侧抽象
                         ReconnectPolicy；RecorderUsecase：房间监控编排、场次生命周期、
                         断流决策树（纯控制流，不做字节级 IO；无 proto、无存储 tag）

internal/data/
  data.go                Data：db（gorm sqlite，单连接）/ apiClient(15s 超时) /
                         streamClient(无超时) / cookie / WBI signer / buvid store /
                         解析后的 recorder 配置项
                         NewData(c *conf.Data, rc *conf.Recorder) (*Data, func(), error)：
                         打开 sqlite（openDatabase）→ AutoMigrate rooms 表 →
                         启动探测 ffmpeg（remux 开启而缺失 → 启动失败）；
                         cleanup 关闭数据库连接
  room_repo.go           roomPO（rooms 表）/ newRoom(DO→PO) / toBiz(PO→DO)；
                         roomRepo 实现 biz.RoomRepo：CRUD + 过滤/排序翻译
                         （AIP filter AST 与 order_by 白名单映射为 SQL，见 §8），
                         分页（offset/limit）、重复 room_id → ErrRoomAlreadyExists
  bili_api.go            liveClient 实现 biz.LiveClient：RoomStatus / OpenStream /
                         DanmakuConn 构造；getRoomPlayInfo 候选排序与降档、
                         风控门/阶梯冷却、-352 刷新重试
  wbi.go                 WBI 签名（nav API 取密钥，1h 缓存，w_rid/wts）
  buvid.go               buvid3/buvid4 指纹（spi，24h 缓存，cookie 注入替换语义）
  danmaku.go             danmakuConn 实现 biz.DanmakuConn：二进制包协议、认证、
                         30s 心跳、90s 读超时、protover3 brotli / protover2 zlib、
                         断线指数退避重连、事件解析与过滤、cmd 分发
  recorder_repo.go       recorderRepo 实现 biz.RecorderRepo：目录布局、
                         sessionMeta/danmuLine PO、RecordSession 泵送循环、
                         切段与 part 续号、meta.json 簿记、RecoverPending、SessionStats
  remux.go               ffmpeg shell-out（stream copy + 元数据注入 + discardcorrupt 重试）
  flv/                   FLV tag 解析子包：FileHeader / Tag 读写、关键帧与
                         sequence header 识别（切段点的判定依据）

internal/service/
  room.go                RoomService：嵌入 v1.UnimplementedRoomServiceServer，
                         五个 CRUD handler；einride aip 的 filtering / ordering /
                         pagination / fieldmask（filter 字段声明、order_by 路径校验、
                         page_size 默认 20、update_mask 仅限 name/enabled，读-改-写）；
                         convertRoom（DTO→DO）/ convertRoomReply（DO→DTO 自由函数，
                         五个 RPC 共用）；只调 RoomUsecase

internal/server/
  http.go                NewHTTPServer：recovery + validate（field_behavior）中间件，
                         v1.RegisterRoomServiceHTTPServer
  grpc.go                NewGRPCServer：recovery 中间件，v1.RegisterRoomServiceServer
  recorder_job.go        RecorderJob（transport.Server，见 §2.1）
  server.go              ProviderSet

internal/conf/
  conf.proto             Bootstrap{server, data, recorder}；Recorder 与
                         Data.Database 消息全字段见 §7（房间列表不在配置里）
  conf.pb.go             make config 生成，禁止手改

cmd/suika/
  main.go                配置加载（file source 目录合并）→ wireApp(bc.Server, bc.Data,
                         bc.Recorder, logger) → newApp(logger, gs, hs, rj)
  wire.go / wire_gen.go  Wire 接线（wire_gen.go 禁止手改，go generate 重新生成）

configs/
  config.yaml            data.database 指向 sqlite（./data/suika.db）；recorder 段
                         不再含房间列表（房间在 sqlite 的 rooms 表，经 CRUD API 管理）
  credentials.example.yaml  cookie 占位模板（进 git）
  credentials.yaml       真实 cookie（gitignore，file source 自动合并）
```

### 2.3 三条缝与决策/IO 分工

| 缝 | 声明（biz） | 实现（data） | 职责 |
|---|---|---|---|
| 文件存储缝 | `RecorderRepo`（daemon 用：PrepareSession / RecordSession / FinishSession / RecoverPending）；窄接口 `SessionStatsRepo`（仅 SessionStats，room API 专用） | `recorderRepo`（`NewRecorderRepo(d *Data, c *conf.Recorder)` 返回接口）；`SessionStatsRepo` 由同一个 `recorderRepo` 实例经转发 provider `NewSessionStatsRepo(repo biz.RecorderRepo)` 实现 | 文件布局、FLV 泵送、meta.json、JSONL、remux |
| 房间存储缝 | `RoomRepo`（FindByRoomID / ListRooms / CreateRoom / UpdateRoom / DeleteRoom，配 `ListOption`） | `roomRepo`（`NewRoomRepo(d *Data)` 返回接口；gorm + mattn sqlite） | rooms 表 CRUD、AIP filter/order_by → SQL 翻译 |
| 平台缝 | `LiveClient` | `liveClient`（`NewLiveClient(d *Data)` 返回接口） | 全部 B 站 HTTP API 与弹幕 WS 流量、风控 |

控制流/IO 分工：**biz 只做决定**（何时开录、是否重连、何时收尾），
**data 做全部 IO**（HTTP、WS、FLV 解析、文件、ffmpeg）。
`StreamHandle` 是 biz 层的 opaque 类型：由 `LiveClient.OpenStream` 产出、
原样交给 `RecorderRepo.RecordSession` 消费，biz 不解其内部
（`Body io.ReadCloser` + URL + Quality，同 `*sql.Rows` 穿过业务层的经典形态）。
`DanmakuConn` 同理：biz 只消费 `Events()`/`Control()` 两个通道。

### 2.4 Wire 接线

ProviderSet：

| 包 | ProviderSet |
|---|---|
| data | `wire.NewSet(NewData, NewRecorderRepo, NewSessionStatsRepo, NewLiveClient, NewRoomRepo)` |
| biz | `wire.NewSet(NewRoomRegistry, NewRecorderUsecase, NewRoomUsecase)` |
| service | `wire.NewSet(NewRoomService)` |
| server | `wire.NewSet(NewGRPCServer, NewHTTPServer, NewRecorderJob)` |

`wire_gen.go` 的实际构造顺序：
`NewData → NewRoomRepo → NewRoomRegistry → NewRecorderRepo →
NewSessionStatsRepo → NewRoomUsecase → NewRoomService → NewGRPCServer /
NewHTTPServer → NewLiveClient → NewRecorderUsecase → NewRecorderJob → newApp`。
`NewData` 依据 `conf.Data` 打开 sqlite 并 AutoMigrate rooms 表，
`NewRoomRepo(d *Data)` 挂在其上；`NewRoomRegistry(repo)` 改吃
`biz.RoomRepo`（不再解析配置），启动时全量加载房间，返回 error，
加载失败即启动失败。`NewRoomUsecase(repo, reg, stats)` 注入 repo 与
registry：CRUD 写 repo，读合并 registry 运行时状态；recorder 回填的
主播名也由 registry 持有的 repo 写回 sqlite（§3.2）。`conf.Recorder`
注入 `NewData`、`NewRecorderRepo`、`NewRecorderUsecase` 三处，各自取
自己负责的字段并套用默认值（§7.2）。

---

## 3. 运行时模型

### 3.1 goroutine 结构

```
App.Run
 └─ RecorderJob.Start → goroutine: RecorderUsecase.Run(rctx)
     ├─ repo.RecoverPending            启动补跑：补完上次遗留的 remux
     └─ for each enabled room（registry 启动快照）→ monitorRoom goroutine
         └─ watchRoom（持有一条 danmakuConn）
             ├─ danmakuConn.run goroutine（data 层，内部自动重连 WS）
             │    ├─ readLoop goroutine（解包、分发）
             │    └─ 30s 心跳 ticker
             ├─ 兜底轮询 timer（默认 600s ±10% 抖动）
             └─ 开播时 → launchSession goroutine
                 ├─ acquireSlot（max_concurrent 并发槽）
                 ├─ repo.PrepareSession
                 ├─ recordLoop：OpenStream → repo.RecordSession 泵送 → 断流决策树
                 │    └─ RecordSession 内部：tag 读取 goroutine（chan 缓冲 512）
                 └─ repo.FinishSession（30s grace，脱离运行 ctx）→ remux
```

- `monitorRoom`：`watchRoom` 返回错误且 ctx 未取消时记错误、等
  `redialDelay = 10s` 后重建弹幕连接（防御性循环；当前
  `liveClient.DanmakuConn` 构造不会失败，重连都在 conn 内部完成）。
- `watchRoom` 的 select 四路：ctx 取消 / 弹幕事件排空（无活动场次时丢弃）/
  场次结束（`active.done`）/ Control 房态事件 / 轮询定时器。

### 3.2 房间状态

房间集合来源是 sqlite：`NewRoomRegistry(repo)` 在启动时从 RoomRepo
全量加载一次（加载顺序 = repo 默认序 room_id 升序），运行期间不再
重载。因此经 CRUD API 对房间集合的增删改要**重启后**才对录制进程
生效（设计取舍，见 §8.1）。biz 在 `RoomRegistry` 里为每个加载的房间
维护 `roomState`（mutex 保护）：

| 字段 | 取值 | 说明 |
|---|---|---|
| `live` | `LiveUnknown` / `LivePreparing` / `LiveOnAir` | 平台侧开播状态 |
| `record` | `RecordIdle` / `RecordRecording` / `RecordRemuxing` / `RecordError` | 录制器自身状态 |
| `sessionStartedAt` | time | 当前场次开始时刻（收尾成功清零） |
| `lastError` | string | 最近一次错误（供状态 API 展示） |

房态来源只有 `getInfoByRoom`：WS 控制命令（LIVE/PREPARING/ROUND/
ROOM_CHANGE）与兜底轮询都只是触发/执行一次房态复查（registry 的
`ApplyRoomInfo`）。`ApplyRoomInfo` 还会在库里没存主播名时用 API 返回
的 `StreamerName` 回填 `room.Name`，并经 registry 持有的 RoomRepo
`UpdateRoom` 写回 sqlite——回填的名字重启不丢；写回失败只记 warn，
不影响运行时状态。

启动后才创建的房间不在 registry 里：daemon 不为它跑监控，API 读取时
持久字段来自 repo、运行时字段返回默认值（§8）。

### 3.3 场次生命周期

`runSession` 端到端拥有一个场次：

1. **acquireSlot**：`max_concurrent > 0` 时占并发槽；槽满则排队等待
   （记日志），ctx 取消则放弃。`max_concurrent = 0` 表示不限。
2. **组装 Session**：`RoomName = firstNonEmpty(库存名, API 主播名, roomID)`，
   `Title`、`LiveStartTime` 取启动时刻的房态快照（场次中途标题变化
   不改名；`LiveStartTime` 决定目录，重连续录落回同一场次）。
3. **PrepareSession**：创建（或重启后重定位）场次目录与 meta.json。
4. **recordLoop**：见 §4.5。
5. **FinishSession**：先置 `RecordRemuxing`，再用
   `context.WithoutCancel(ctx)` + `finishGracePeriod = 30s` 的脱离 ctx 执行，
   保证停机路径上 meta 的 `remuxing` 标记也能落盘；未完成的 remux 由
   下次启动 `RecoverPending` 补跑。成功后回 `RecordIdle`，失败置 `RecordError`。
6. **releaseSlot**。

录制中房间状态为 `RecordRecording`；`ListRooms` 只对该状态的房间追加
`SessionStatsRepo.SessionStats`（当前 part 路径 + 累计字节，原子计数，零额外采集）。

### 3.4 优雅停机

```
SIGTERM → kratos 触发各 server.Stop
  → RecorderJob.Stop 取消 rctx
    → watchRoom：cancel 活动场次并等待
      → recordLoop 因 ctx.Err() 返回（当前 part 已刷盘，FLV 至最后完整 tag 有效）
      → FinishSession 用脱离 ctx（30s grace）标 remuxing 并尽量 remux
    → monitorRoom 退出
  → Stop 等待上限 45s（或 kratos 传入的停机 ctx），超时则继续走后续关停
未完成的 remux → 下次启动 RecoverPending 补跑
```

---

## 4. 核心流程

### 4.1 开播检测（WS 事件驱动 + 轮询兜底）

1. `getDanmuInfo`（WBI 签名）取 token + 接入节点列表；仍被风控（-352）
   则降级到旧接口 `getConf`（无需 WBI）；两者都失败 → 该房间进风控冷却。
   节点列表为空时用保底地址 `wss://broadcastlv.chat.bilibili.com:2245/sub`。
2. 拨号：节点列表**随机打乱**，每个节点依次尝试 protover 3（brotli）、
   2（zlib）；op7 认证包携带 `uid=0 / roomid / protover / platform=web /
   type=2 / key=token / buvid`（buvid 优先取 cookie 里的 buvid3，缺则 spi 现取）；
   等 op8 认证回复（5s 超时，`code==0` 才算成功）。
3. **每次（重）连接成功后先调 `getInfoByRoom` 重建房态**并从 Control
   通道重发（`pushRoomState`），覆盖断线/休眠期间错过的 LIVE/PREPARING。
4. 常驻期间处理 cmd（B 站会在 cmd 后挂变体后缀如 `DANMU_MSG:4:0:3:`，
   先按 `:` 截断再分发）：
   - `LIVE` / `PREPARING` / `ROUND` / `ROOM_CHANGE` → `pushRoomState` 复查；
   - 各事件 cmd → 解析后投递 Events 通道（§4.4）。
   - biz 侧对 Control 事件幂等：已在录时收到"在播"不重复开场次，
     未录时收到"未开播"无副作用。
5. WS 保活：30s 心跳（op2）；读超时 90s（约 3 个心跳周期）杀半开连接，
   进入重连；重连指数退避 2s → 30s 封顶。
6. 兜底轮询：每 `fallback_poll_interval`（默认 600s）±10% 抖动执行一次
   `RoomStatus`，发现"在播但无活动场次"立即启动录制，"未开播但有活动
   场次"则取消场次。轮询请求走风控层（§5）。

### 4.2 拉流

- `getRoomPlayInfo`：`protocol=0,1 & format=0,1,2 & codec=0,1 & qn=<quality_qn>
  & platform=web`。候选展开 stream×format×codec×url_info，过滤
  `base_url` 含 `.flv` 的候选（录制必须 FLV），avc 优先级 100、其他 90，
  取最高优先级 URL = `host + base_url + extra`。
- 返回清晰度不足请求值时**接受最高可得档位**（自动降档，记 warn 日志，
  实际档位写入 meta.json）。清晰度描述优先用 API 的 `g_qn_desc`，缺则查
  内置表（20000=4K、10000=原画、400=蓝光、250=超清、150=高清、80=流畅）。
- data 层用 `streamClient`（无超时，长读连接，取消走请求 ctx）打开流 URL，
  注入桌面 Chrome UA / `Referer: https://live.bilibili.com/{room}` / 原始 cookie。
  打开失败或 HTTP 非 2xx → 包装为 `biz.ErrStreamTransient`。
  **ffmpeg 不参与拉流。**

### 4.3 录制引擎（Go 解析 FLV 直接落盘）

```
HTTP body（原始字节，LiveClient 打开）
  → RecordSession 泵送（data/recorder_repo.go）
      ├─ flv.ParseHeader 读 9 字节文件头 + PreviousTagSize0
      ├─ tag 读取 goroutine：flv.ReadTag 逐个送入 chan（缓冲 512）
      ├─ headerCache 缓存 onMetaData / AVC sequence header / AAC sequence header
      ├─ 首个 tag 到达时 openNewSegment：part 号 = 目录扫描续号，
      │     新 part = FLV 文件头 + 缓存的三类头 tag + 后续 tag（可独立播放）
      ├─ 切段判定 shouldSplit：段时长达 segment_minutes（默认 120，0=不切）
      │     且当前 tag 是视频关键帧；或超出 splitOverrun = 15s 强制切
      │     （时间戳保持流内原值，不重置；startTs = 该 part 首个正文 tag）
      ├─ 缓存时机：开/切段判定之后才更新缓存——触发新段的 tag 不会被
      │     重复注入（否则 openSegment 注入一次、泵送又写一次）
      ├─ 弹幕事件同步写当前 part 的 JSONL（无活动段时丢弃）
      ├─ 健康巡检：每 health_check_interval（默认 60s）检查累计字节，
      │     连续 health_check_fail_rounds（默认 3）轮无增长 → 中止本次连接
      │     （返回普通错误 → 走决策树普通重连分支）
      └─ 统计：pumpStats（atomic 文件路径/字节数），字节数跨重连续泵累加
```

为什么不用 ffmpeg 录制：切段必须发生在 FLV tag 层（重启 ffmpeg 拿不到
sequence header，新 part 不可播）；纯 Go 落盘还换来抗崩溃（FLV 无 moov
问题，进程猝死文件仍有效）与 ffmpeg 解耦（录制期 ffmpeg 崩溃零影响）。

写失败（磁盘满/权限）：记 meta errors，中止泵送，错误不带
`ErrStreamTransient` 标记 → 决策树按普通中断处理；重连后开新段大概率
再次失败，耗尽预算后场次收尾为 ERROR，已写文件保留，下次开播自然恢复。

### 4.4 弹幕录制（与检测复用同一条 WS）

录制的事件（`biz` 常量 → JSONL `type` 字段；每条一行，`ts` = 接收时刻
unix 毫秒，`raw` 附原始 JSON 兜底；空字段按 omitempty 省略）：

| cmd | type | 解析出的字段 |
|---|---|---|
| `DANMU_MSG` | `danmaku` | text / uid / uname / mode / color（空文本丢弃） |
| `SEND_GIFT` | `gift` | gift_name / num / price / coin_type（免费礼物全录） |
| `SUPER_CHAT_MESSAGE` | `superchat` | price / text / duration |
| `GUARD_BUY` | `guard` | level / num |
| `ENTRY_EFFECT` | `entry_effect` | text（进场特效文案） |
| `INTERACT_WORD` | `interact_word` | uid / uname（默认关闭） |

`INTERACT_WORD` 与点赞类量级约为弹幕 10 倍、切片价值≈0，默认不录，
开关为 `recorder.danmaku.record_interact_word`。过滤发生在 data 的
`danmakuConn.dispatch`，biz 与 repo 只见已过滤事件。

投递语义：Events 通道缓冲 4096、Control 缓冲 16，`emit` 非阻塞——
缓冲满（只可能发生在无场次消费时）直接丢弃，永不阻塞读包循环。
弹幕文件与视频 part **一一对应**：切段时同步切换 JSONL 输出文件。

### 4.5 断流决策树（biz.recordLoop）

```
每轮开始：lc.OpenStream（重取 URL，可能换 CDN 节点）
  ├─ 失败 → 记 lastError，结束场次（不重试）
  └─ 成功 → session.Quality = 实际档位 → repo.RecordSession 泵送
泵送返回（EOF / 读错误 / 巡检中止 / 写失败 / ctx 取消）
  ├─ ctx 已取消 → 返回（停机路径）
  └─ lc.RoomStatus 复查
      ├─ 失败 → 记错误，结束场次
      ├─ 已下播 → 正常收尾
      └─ 仍在播：
          ├─ err 是 ErrStreamTransient（CDN 瞬态：打开失败/HTTP 非 2xx/
          │   FLV 头解析失败/读错误）
          │   ├─ cdn_transient_budget（默认 5）未耗尽 →
          │   │   指数退避 min(2s << attempt, 60s) → 下一轮
          │   └─ 耗尽 → 保留已录内容收尾（记成功，非失败）
          ├─ auto_reconnect = false → 收尾
          ├─ 重连次数 < max_reconnect（默认 3）→ 等 reconnect_delay（默认 10s）→ 下一轮
          └─ 配额耗尽 → 保留已录内容收尾
```

`ErrStreamTransient` 与 `ErrRiskControl` 是 biz 声明的哨兵错误，data 在
错误源头包装（`fmt.Errorf("%w: ...")`），决策树用 `errors.Is` 分类。
预算、延迟参数来自 `conf.Recorder.ReconnectOptions`（§7）。

### 4.6 场次收尾与 remux（repo.FinishSession）

1. meta.json：`status = remuxing`、写 `end_time`、刷新 title 与 quality，
   随即落盘（崩溃安全：之后逐段持久化）。
2. `finalizeSegments` 逐 part 串行（stream copy，不重编码）：

   ```
   ffmpeg -hide_banner -loglevel error -y [-fflags +discardcorrupt] \
     -i <part>.flv -c copy \
     -metadata title=<直播标题> -metadata artist=<主播名> -metadata date=<开播时间> \
     <part>.mp4
   ```

   - 首次失败 → 加 `-fflags +discardcorrupt` 重试一次。
   - **删除前必验证**：mp4 存在且非空才删源 FLV；否则记 `failed`、保留 FLV。
   - `remux_enabled = false`：段直接标 `ok` + `flv_kept = true`，不转封装。
   - FLV 已不在但 mp4 存在（上次崩溃在删除后、落盘前）→ 补标 `ok`；
     两者都不在 → `failed`（"source flv missing"）。
   - 每段处理完立即持久化 meta.json，进度可崩溃恢复。
3. 全部成功 → `status = done`；有失败段 → `partial`。**绝不删除未验证文件。**

### 4.7 启动补跑（repo.RecoverPending）

`Run` 的第一步（错误只记日志不致命）：glob
`<record_root>/*/*/*.meta.json`，逐文件处理：

| meta.status | 动作 |
|---|---|
| `recording` / `remuxing` | 视为被中断的场次：补 end_time → finalizeSegments |
| `partial` / `done` | 仅当存在 `failed 且 flv_kept` 的可重试段时重跑 finalize |

---

## 5. 风控层（data）

所有 B 站请求统一走 `fetchJSON`：桌面 Chrome UA +
`Referer: https://live.bilibili.com/<room>` + `Origin` + cookie；
HTTP 412/403/429 → `errHTTPRiskControl`。

**WBI 签名**（`wbi.go`，移植 hikami-go）：`/x/web-interface/nav` 取
img_key/sub_key → 64 位置换表混出 32 字符 mixin_key（缓存 1h）；签名即
按 key 排序的查询串（剔除 `!'()*`）+ mixin_key 取 MD5，附加 `w_rid`/`wts`。
`getDanmuInfo`、`getInfoByRoom`、`getRoomPlayInfo` 都会先 WBI 签名。

**buvid 指纹**（`buvid.go`，移植 hikami-go）：`/x/frontend/finger/spi`
取 buvid3/buvid4，按 cookie 键缓存 24h；注入 cookie 时先删旧 buvid3/4
再追加（B 站取同名第一个，替换语义保证新指纹生效）。

**-352 / HTTP 风控处理**：

1. 首次命中 → `refreshRisk()`（强刷 WBI 密钥 + 作废 buvid 缓存）→ 原请求重试一次。
2. `getDanmuInfo` 二次仍 -352 → 降级旧接口 `getConf`（无 WBI）。
3. 仍失败 → 该房间进**阶梯冷却** 5min → 10min → 20min（按连续失败次数
   进阶，封顶 20min）；冷却期内 `enterRiskGate` 直接拒绝该房间的
   RoomStatus/OpenStream/getDanmuInfo 调用（返回 `ErrRiskControl`）。
4. 任一 API 成功 → `noteSuccess` 清零该房间冷却。

cookie 过期不是错误：表现为拉流拿不到原画 → 自动降档并记录 meta
（运维动作：换 cookie）。无 cookie 也能运行（启动记 warn），但更易触发风控。

---

## 6. 磁盘数据结构

### 6.1 目录与命名

```
<record_root>/                              默认 ./recordings，可配置
  <room_id>_<主播名>/                        主播名清洗后 ≤32 rune
    <YYYY-MM-DD>/                           开播日期（live_start_time）
      <YYYYMMDD>_<HHMM>_<直播标题>_part1.flv    → remux 后 .mp4
      <YYYYMMDD>_<HHMM>_<直播标题>_part1.danmu.jsonl
      <YYYYMMDD>_<HHMM>_<直播标题>_part2.flv
      <YYYYMMDD>_<HHMM>_<直播标题>_part2.danmu.jsonl
      <YYYYMMDD>_<HHMM>_<直播标题>.meta.json
```

- `YYYYMMDD_HHMM` 与日期目录均取自 API 的 `live_start_time`（真实开播
  时间，非检测时间；API 未给时退化为本机当前时间）；进程重启续录落回
  同一场次目录。标题清洗后为空则 `untitled`。
- part 编号 = 目录扫描续号（正则 `_part(\d+)\.(flv|mp4)$`，取最大值 +1），
  同时覆盖断流重连与崩溃重启两种来源。
- 清洗规则（标题 ≤64 rune、主播名 ≤32 rune）：`\/:*?"<>|`、控制字符
  （<0x20、0x7f）、连续空白 → 单个 `_`，连续 `_` 压缩、首尾 `_` 修剪。

### 6.2 meta.json（每场次一个，文件系统即真相）

字段与 `data.sessionMeta` PO 一一对应（`json` tag 为准）：

```json
{
  "room_id": 123456,
  "room_name": "某主播",
  "title": "今天的直播标题",
  "live_start_time": 1754912400,
  "end_time": 1754923200,
  "quality": { "qn": 10000, "desc": "原画" },
  "status": "recording | remuxing | done | partial",
  "segments": [
    {
      "part": 1,
      "video": "..._part1.mp4",
      "flv_kept": false,
      "danmaku": "..._part1.danmu.jsonl",
      "wall_start": 1754912400,
      "wall_end": 1754919600,
      "ts_start": 12340,
      "ts_end": 7199840,
      "bytes": 4831838208,
      "remux_status": "ok | pending | failed",
      "remux_error": ""
    }
  ],
  "errors": [ { "time": 1754915000, "stage": "record", "msg": "..." } ],
  "updated_at": 1754923260
}
```

- `wall_*`（墙上时钟 unix 秒）+ `ts_*`（流内时间戳毫秒；start = 该 part
  首个正文 tag，end = 末个 tag）双时间轴，是第二阶段切片对齐的依据。
- 段在 opened 时入 meta（`pending`，含 wall_start），closed 时补
  wall_end/ts/bytes；所有 meta 写入经 tmp 文件 + rename 原子替换。

### 6.3 弹幕 JSONL（每 part 一个）

```json
{"ts":1754912401234,"type":"danmaku","uid":123,"uname":"某人","text":"666","color":16777215,"mode":1,"raw":{...}}
{"ts":1754912402345,"type":"gift","uid":456,"uname":"某人","gift_name":"小心心","num":1,"price":0,"coin_type":"silver","raw":{...}}
{"ts":1754912403456,"type":"superchat","uid":789,"uname":"某人","price":30,"text":"...","duration":60,"raw":{...}}
{"ts":1754912404567,"type":"guard","uid":789,"uname":"某人","level":3,"num":1,"raw":{...}}
{"ts":1754912405678,"type":"entry_effect","uid":789,"uname":"某人","text":"...","raw":{...}}
```

---

## 7. 配置

### 7.1 conf.proto（internal/conf，make config 重新生成）

```proto
message Bootstrap {
  Server server = 1;
  Data data = 2;
  Recorder recorder = 3;
}

message Data {
  message Database {
    string driver = 1;   // 只接受 "sqlite"，其他值启动失败
    string source = 2;   // sqlite 文件路径；父目录缺失时自动创建
  }
  message Redis { ... }  // 模板遗留占位，本服务未使用
  Database database = 1;
  Redis redis = 2;
}

message Recorder {
  // 监控的房间在 sqlite 的 rooms 表里，经 Room CRUD API 管理，
  // 配置不再持有房间（原 repeated Room rooms = 1 已删除，字段号 1 空置）。
  message DanmakuOptions {
    bool record_interact_word = 1;  // 默认 false
  }
  message ReconnectOptions {
    optional bool auto_reconnect = 1;                      // 未设置默认 true
    int32 max_reconnect = 2;                               // 0 → 默认 3
    google.protobuf.Duration reconnect_delay = 3;          // 未设置默认 10s
    int32 cdn_transient_budget = 4;                        // 0 → 默认 5
    google.protobuf.Duration health_check_interval = 5;    // 未设置默认 60s
    int32 health_check_fail_rounds = 6;                    // 0 → 默认 3
  }
  string cookie = 2;                                 // 含 SESSDATA；放 credentials.yaml
  string record_root = 3;                            // 默认 ./recordings
  google.protobuf.Duration fallback_poll_interval = 4; // 默认 600s
  int32 quality_qn = 5;                              // 默认 10000；不足时自动降档
  optional int32 segment_minutes = 6;                // 未设置默认 120；显式 0 = 不切段
  int32 max_concurrent = 7;                          // 0 = 不限
  optional bool remux_enabled = 8;                   // 未设置默认 true；显式 false = 只录 FLV
  DanmakuOptions danmaku = 9;
  ReconnectOptions reconnect = 10;
}
```

`auto_reconnect` / `segment_minutes` / `remux_enabled` 用 `optional`，
使"显式 false/0"与"未设置"可区分（proto 标量零值歧义）。

数据库：`openDatabase` 只接受 `driver = "sqlite"`（其他值或 source 为
空都启动失败），source 即 sqlite 文件路径（config.yaml 配
`./data/suika.db`）；gorm 连接池固定单连接，避免嵌入式库上的
SQLITE_BUSY。开库成功后 `NewData` 立即 AutoMigrate `rooms` 表。
db 文件是运行期数据，不进 git（`/data/` 已加入 .gitignore）。

### 7.2 默认值与应用位置

| 配置项 | 代码默认 | 应用位置 |
|---|---|---|
| fallback_poll_interval | 600s | biz.NewRecorderUsecase |
| auto_reconnect | true | biz（optional，nil→true） |
| max_reconnect | 3 | biz |
| reconnect_delay | 10s | biz |
| cdn_transient_budget | 5 | biz |
| max_concurrent | 0（不限） | biz |
| record_root | ./recordings | data.NewRecorderRepo |
| segment_minutes | 120 | data.NewRecorderRepo（optional，nil→120） |
| health_check_interval | 60s | data.NewRecorderRepo |
| health_check_fail_rounds | 3 | data.NewRecorderRepo |
| quality_qn | 10000 | data.NewData |
| remux_enabled | true | data.NewData（optional，nil→true） |
| record_interact_word | false | data.NewData |

biz 层硬编码常量（不可配）：CDN 退避基数 2s、封顶 60s，监控重建间隔
10s，FinishSession 脱离 grace 30s，轮询抖动 ±10%。data 层硬编码：
切段关键帧等待上限 15s，弹幕事件缓冲 4096，心跳 30s、读超时 90s，
WS 重连退避 2s→30s，apiClient 超时 15s。

### 7.3 凭证

- 真实 cookie 写入 `configs/credentials.yaml`（**已 gitignore**）。
  Kratos `config/file.NewSource(dir)` 遍历目录下所有非点开头文件逐一
  加载合并，`-conf ./configs` 时两文件字段自动汇入同一棵 Bootstrap，
  代码无感知。文件名不要以 `.` 开头（会被 file source 跳过）。
- `configs/credentials.example.yaml` 是进 git 的占位模板：

```yaml
# configs/credentials.yaml（gitignored）
recorder:
  cookie: "SESSDATA=xxx; buvid3=xxx; ..."
```

### 7.4 现网 config.yaml 与代码默认的差异（有意为之）

- 配置里没有任何房间：房间清单在 sqlite（`data.database.source`）的
  rooms 表里，经 CRUD API 管理；全新安装首次启动时 rooms 表为空，
  recorder 记 warn 零房间空转，CreateRoom 加房后重启生效（§8）；
- `remux_enabled: false`（开发机未装 ffmpeg；装了再改 true）；
- `max_concurrent: 2`、`health_check_interval: 30s` 为本地调过的值；
- `cookie: ""` 显式留空，真实值只进 credentials.yaml。

---

## 8. 房间 CRUD API

`api/room/v1/room.proto`（package `room.v1`）按 AIP-13x 声明五个 RPC：

| RPC | HTTP | 语义 |
|---|---|---|
| CreateRoom | `POST /v1/rooms/create`（body=room） | 注册新房间；响应回填 create_time / update_time，运行时字段为默认值；room_id 重复 → 409 |
| ListRooms | `GET /v1/rooms/list` | 分页列表，支持 filter / order_by；合并运行时状态返回 |
| GetRoom | `GET /v1/rooms/{room_id}` | 按平台房间号取单个房间，合并运行时状态；不存在 → 404 |
| UpdateRoom | `PUT /v1/rooms/update`（body=room） | update_mask 部分更新，仅可改 `name` / `enabled`，room_id 不可变；不存在 → 404 |
| DeleteRoom | `DELETE /v1/rooms/{room_id}` | 删除房间，返回 `google.protobuf.Empty`；不存在 → 404 |

- `Room` 消息 = 持久字段（room_id / name / enabled / create_time /
  update_time）+ 运行时字段（live_status / record_status / current_file /
  bytes_written / session_started_at / last_error，全部标注
  OUTPUT_ONLY）。**运行时字段只在 Get/List 响应中由 registry 合并返回；
  Create/Update 的响应里是默认值**（LIVE_STATUS_UNSPECIFIED /
  IDLE / 零值；Delete 返回 Empty），也不参与 filter 与 order_by。
- 五个 RPC 同时注册 HTTP 与 gRPC；中间件沿用 recovery + validate
  （Create/Update 的 `room` 字段与 update_mask 声明为 REQUIRED）。
  根目录 `openapi.yaml` 由 `make api` 一并重新生成。

**ListRooms 细则**：

- filter 与 order_by 白名单只有五个持久字段：`room_id` / `name` /
  `enabled` / `create_time` / `update_time`（service 层字段声明与 data
  层列名映射双重把关），运行时字段参与过滤/排序直接报错。
- filter 走 einride aip 标准语法：`=` / `!=` / `<` / `<=` / `>` / `>=`、
  `AND` / `OR` / `NOT`；`name:"子串"` 是子串匹配（翻译为 LIKE，输入里的
  `%` / `_` 已转义按字面匹配）；裸标识符 `enabled` / `NOT enabled` 是
  布尔测试；时间字段接受 `timestamp("…")` 或裸 RFC3339 字符串；常量也
  可以放在比较符左侧（操作符自动翻转）。
- 分页：`page_size` 未设置（或 ≤0）默认 20；`page_token` 为 offset 型
  token（einride pagination），整页返回时附 `next_page_token`。
- 排序：`order_by` 逗号分隔多字段，后缀 ` desc` 降序；未指定时回退
  `room_id ASC`，保证分页顺序确定。
- service 解析失败（filter / order_by / page_token 任一非法）一律
  INVALID_ARGUMENT。

**UpdateRoom 细则**：service 先校验 update_mask 非空且路径 ⊆
{name, enabled}，再 GetRoom 取当前值（顺带得到 404），
`fieldmask.Update` 覆盖后经 usecase 落库；repo 只更新 name / enabled
两列（update_time 由 gorm 自动刷新），读-改-写全程在单进程内完成。

错误码：

| 场景 | HTTP | ErrorReason |
|---|---|---|
| Get/Update/Delete 的 room_id 不存在 | 404 | ROOM_NOT_FOUND |
| room_id ≤ 0、update_mask 为空或含不支持字段、filter/order_by/page_token 非法、offset/limit 越界 | 400 | ROOM_INVALID_ARGUMENT |
| CreateRoom 的 room_id 已存在 | 409 | ROOM_ALREADY_EXISTS |
| recorder 内部非法状态（如空流句柄） | 500 | ROOM_INTERNAL |

biz 层对应 `ErrRoomNotFound`（errors.NotFound）/
`ErrRoomInvalidArgument`（errors.BadRequest）/
`ErrRoomAlreadyExists`（errors.Conflict，409）；重复 room_id 由 sqlite
主键约束兜底（driver 约束错误 → AlreadyExists）。
`ErrStreamTransient` / `ErrRiskControl` 仅作内部分类哨兵，不出 API。

biz ↔ proto 枚举映射（`service.convertRoomReply`，五个 RPC 共用）：

| biz | proto LiveStatus | biz | proto RecordStatus |
|---|---|---|---|
| LiveUnknown | LIVE_STATUS_UNSPECIFIED | RecordIdle | IDLE |
| LivePreparing | PREPARING | RecordRecording | RECORDING |
| LiveOnAir | LIVE | RecordRemuxing | REMUXING |
| | | RecordError | ERROR |

数据源：持久字段来自 sqlite（repo），运行时字段来自 `RoomRegistry`
快照（mutex）+ 仅录制中房间追加 `SessionStatsRepo.SessionStats`
（泵送层原子计数，stats 出错静默跳过只丢进度）。

**路由注册序 quirk**：proto 里 ListRooms 刻意声明在 GetRoom 之前。
Kratos HTTP 路由走 gorilla/mux，按注册顺序匹配；若
`/v1/rooms/{room_id}` 先注册，`GET /v1/rooms/list` 会被通配路由吞掉
（room_id 匹配到字符串 "list"）。日后增改路由须保持字面路径先于
通配路径注册。

### 8.1 已接受的设计取舍（CRUD 与录制进程的时序）

1. CRUD 变更立即落 sqlite，但**重启后才对录制进程生效**——registry
   只在启动时全量加载一次，运行期不热载。
2. 由此：已删除的房间重启前**继续录制**；新建/启用的房间重启后才
   开始录制；停用（enabled=false）同样要重启才停止监控。
3. 平台回填的主播名会经 repo 写回 sqlite，重启不丢（写回失败只记
   warn，不影响录制）。

启动后创建的房间 Get/List 照常可查（持久字段来自 sqlite，运行时字段
为默认值），只是录制进程不认识它，直到下次重启。

---

## 9. 失败处理与边缘情况

| 场景 | 行为 |
|---|---|
| 断流（仍在播） | 决策树重连，新 part（§4.5）；预算耗尽则保内容收尾 |
| 正常 EOF 但仍在播 | 视同断流重连（CDN 掐长连接是常态） |
| 文件/tag 停止增长 | 巡检连续 3 轮无增长 → 中止 → 决策树普通重连分支 |
| 风控 -352/412/403/429 | 刷 WBI+buvid 重试一次 → getDanmuInfo 再降级 getConf → 失败则房间冷却 5/10/20min（§5） |
| 进程崩溃/重启 | FLV 保留至最后完整 tag；重启后 WS 重连重查房态，在播则续录，part 目录扫描续号，meta 原子写无半更新；RecoverPending 补 remux |
| 优雅停机（SIGTERM） | §3.4：FLV 已有效 → meta 标 remuxing（30s grace）→ remux 遗留下次补跑；Stop 等待上限 45s |
| 磁盘写失败 | 中止泵送，保留已写文件，meta 记 errors；重连大概率再失败，耗尽预算后房间状态 ERROR，下次开播自然恢复（无重试风暴） |
| ffmpeg 缺失 | `remux_enabled=true`（含未设置）→ NewData 启动探测失败，进程起不来；显式 false → 只录 FLV 不转封装 |
| remux 输出缺失/为空 | 不删源 FLV，段标 failed，meta 置 partial，下次启动重试 |
| cookie 过期 | 拉流降档（qn 自动降档 + meta 记录 + warn 日志）；运维动作：换 cookie |
| WS 假死（半开连接） | 90s 读超时强制重连；兜底轮询（600s±10%）保底发现开播 |
| 多主播同时开播 | 并行录制；`max_concurrent` 达上限时新开播排队等待（记日志） |
| recorder 配置缺失/rooms 表为空 | NewData/NewRecorderRepo/NewRecorderUsecase 均容忍 nil recorder conf；rooms 表空 → Run 记 warn 零房间空转，进程其余部分不受影响；经 CRUD 加房后重启生效（§8.1） |
| data.database 缺失或 driver 非 sqlite | NewData 启动失败：只接受 `driver: "sqlite"` 且 source 非空；sqlite 打不开同样启动失败 |
| CRUD 改了房间但未重启 | 录制进程沿用启动时快照：删的房间继续录、新房间不监控（§8.1，已接受的取舍） |
| 无活动场次时弹幕到达 | Events 缓冲（4096）满即丢弃，不阻塞 WS 读循环 |
| watchRoom 收到重复"在播" | 幂等：已有活动场次则忽略 |
| 场次中途改标题/轮次 | 目录与文件名沿用开播快照，不重命名；ROUND/ROOM_CHANGE 仅刷新房态 |

---

## 10. 测试

测试与被测代码同包同目录（`*_test.go`），分层隔离（CLAUDE.md 纪律），
共 57 个测试函数。运行：`go test -mod=mod ./...`（本仓库一律 `-mod=mod`）。

| 层 | 文件 | fake 什么 / 测什么 |
|---|---|---|
| biz | `recorder_test.go`（11） | repo + LiveClient 全脚本化 fake（队列式返回、末条粘滞）；决策树各分支：下播停录、在播重连、预算耗尽保内容、auto_reconnect=false、CDN 瞬态独立预算、OpenStream/复查失败终止、ctx 取消即停、nil/覆盖配置、抖动区间；`cdnBackoffBase`/`redialDelay` 字段供测试压缩时延 |
| biz | `room_test.go`（8） | fakeRoomRepo 脚本化：NewRoomRegistry 全量加载（room_id 序）、nil repo 空 registry、加载失败即启动错误；ApplyRoomInfo 回填名字经 repo 写回、写回失败只降级不丢状态；fakeStatsRepo；ListRooms 合并状态与 stats；RoomUsecase 参数校验与 repo 错误透传 |
| service | `room_test.go`（6） | 真 sqlite 端到端：`t.TempDir()` 临时 db 文件 + `data.NewData`（RemuxEnabled=false 免 ffmpeg 探测），按 wireApp 同款链路搭 roomEnv；CRUD 全流程（建/取/改名/停用/删、时间戳回填、响应运行时字段默认值、空 name 允许）、分页翻页、filter/order_by（子串+布尔组合、desc、RFC3339 时间范围）、校验（0/负 room_id、重复创建 409、空/越权 update_mask、不存在 404、坏 page_token、运行时字段禁过滤排序）；**同一 Data 上重建第二套 env 模拟重启**：验证 registry 重载持久房间、合并运行时状态、"启动后 CRUD 的房间以默认运行时值返回、录制进程不实时生效"；convertRoomReply 枚举映射 |
| data | `recorder_repo_test.go`（24） | `t.TempDir()` 真文件系统：meta 往返/损坏 JSON、标题清洗、part 续号、切段判定、配置映射、路径推导、重启续录保段/更新标题、新段头注入且不重复写、弹幕事件落盘、nil 流拒绝、单段/切段全流程、收尾（无 meta noop / remux 关保 FLV / 成功替换 / 失败保留 / 空 ffmpegPath）、缺源恢复、RecoverPending |
| data | `remux_test.go`（4） | 假 ffmpeg shell 脚本（`writeFakeFFmpeg`：记录参数、可控失败次数、写出非空产物），不依赖真 ffmpeg 验证重试与参数构造 |
| data/flv | `flv_test.go`（4） | 构造字节流 fixture：头往返、坏签名、tag 流（含扩展字节时间戳）、截断 |

---

## 11. 依赖与运行要求

Go 依赖（go.mod，录播特有）：`github.com/gorilla/websocket` v1.5.3
（弹幕 WS）、`github.com/andybalholm/brotli` v1.2.2（protover3 解压）、
`gorm.io/gorm` v1.31.2 + `gorm.io/driver/sqlite` v1.6.0（房间持久化；
driver 底层是 `github.com/mattn/go-sqlite3`，**需 cgo**）、
`go.einride.tech/aip` v0.86.3（CRUD 的 filtering / ordering /
pagination / fieldmask）；框架为 `github.com/go-kratos/kratos/v3`。

运行时会在工作目录按 `data.database.source` 打开 sqlite 文件
（默认 `./data/suika.db`）：父目录缺失自动创建，AutoMigrate rooms 表，
单连接访问。`/data/` 已入 .gitignore，db 文件不进仓库。

外部二进制：

| 二进制 | 要求 |
|---|---|
| ffmpeg | `remux_enabled` 为 true（含未设置）时启动探测，缺失即启动失败；只用于 remux（stream copy），不参与拉流/录制 |
| ffprobe | 当前代码未实际调用，缺失仅 warn（预留给后续校验增强） |

本地运行：

```bash
make init                                  # 首次安装 wire/buf
cp configs/credentials.example.yaml configs/credentials.yaml   # 填 cookie
# 编辑 configs/config.yaml：装了 ffmpeg 后 remux_enabled: true
go run ./cmd/suika -conf ./configs         # HTTP :8000 / gRPC :9000，
                                           # 首次运行自动建 ./data/suika.db
curl localhost:8000/v1/rooms/list          # 冒烟检查（全新安装返回空列表）
curl -X POST localhost:8000/v1/rooms/create \
     -d '{"room":{"room_id":123456,"enabled":true}}'   # 加房间
# 房间重启后才对录制进程生效（§8.1）
```

---

## 12. 为第二阶段（切片）预留的扩展点

第一阶段刻意落盘、当前即可被切片消费的材料：

- MP4 容器元数据（title/artist/date）→ `ffprobe` 直接可读素材身份；
- meta.json 的 `wall_*` / `ts_*` 双时间轴 → 弹幕↔视频任意精度对齐；
- 全事件 JSONL → SC/礼物/上舰是高光定位的最强信号，弹幕密度切片与
  事件驱动切片都可直接消费；
- part 化的目录结构 → 切片素材溯源与"已使用"标记天然有落点；
- 数据库已经就位（sqlite + gorm，当前只存房间）；素材使用记录等
  第二阶段数据直接在同一个 data 层加表（AutoMigrate 已在 NewData）。

---

## 附录 A：移植来源映射

| 本服务组件 | 来源仓库 | 原始位置 | 移植方式 |
|---|---|---|---|
| WBI 签名 | hikami-go | `internal/biliutil/wbi.go` | Go 直接移植（data/wbi.go） |
| buvid 指纹 | hikami-go | `internal/biliutil/buvid.go` | Go 直接移植（data/buvid.go） |
| 开播检查/拉流/URL 拼装/候选排序 | hikami-go | `internal/live_record/bilibili.go` | Go 移植（data/bili_api.go） |
| 弹幕 WS 协议（包头/认证/心跳/brotli） | hikami-go | `internal/live_record/danmaku.go` | 移植 + 扩展事件类型（data/danmaku.go） |
| 断流决策树/预算/巡检 | hikami-go | `internal/live_record/manager.go` | 参考重写，决策移入 biz（biz/recorder.go） |
| 风控阶梯冷却 | hikami-go | `internal/live_record/manager.go` | Go 直接移植（data/bili_api.go） |
| FLV tag 切段/头注入 | blrec | `blrec/flv/*`、`blrec/core/operators/*` | Go 重写（data/flv、data/recorder_repo.go） |
| LIVE/PREPARING 事件驱动检测 | blrec | `blrec/bili/live_monitor.py` | Go 重写（biz + data/danmaku.go） |
| remux 元数据注入 | blrec | `blrec/core/metadata_provider.py` | 思路照搬（data/remux.go） |

hikami-go：Go 单机服务，录直播音频+弹幕 → ASR → AI 总结（刻意不保存
视频）；blrec（bilive 内置录制内核）：纯 Python FLV 下载器。两者录制
内核都不能直接复用，但组合起来覆盖本服务全部需求。

## 附录 B：B 站接口速查

| 接口 | 用途 | 代码位置 |
|---|---|---|
| `GET api.live.bilibili.com/xlive/web-room/v1/index/getInfoByRoom?room_id=` | 房间/开播状态、标题、live_start_time、主播名 | bili_api.go roomStatus |
| `GET api.live.bilibili.com/xlive/web-room/v2/index/getRoomPlayInfo?room_id=&protocol=0,1&format=0,1,2&codec=0,1&qn=&platform=web` | 流地址（候选排序取 FLV+avc 优先） | bili_api.go selectStreamURL |
| `GET api.live.bilibili.com/xlive/web-room/v1/index/getDanmuInfo?id=&type=0` | 弹幕 token + 接入节点（WBI 签名） | danmaku.go danmuInfo |
| `GET api.live.bilibili.com/room/v1/Danmu/getConf?room_id=&platform=pc&player=web` | 弹幕 token 降级通道（无 WBI） | danmaku.go danmuConf |
| `GET api.bilibili.com/x/web-interface/nav` | WBI 密钥（兼判断登录态） | wbi.go fetchKeys |
| `GET api.bilibili.com/x/frontend/finger/spi` | buvid3/buvid4 | buvid.go getBuvids |
| `wss://<host>:<wss_port>/sub`（保底 `broadcastlv.chat.bilibili.com:2245`） | 弹幕事件流：16 字节头二进制包，op2 心跳 / op5 消息 / op7 认证 / op8 认证回复；protover 3=brotli、2=zlib | danmaku.go |

清晰度档位：20000=4K、10000=原画、400=蓝光、250=超清、150=高清、80=流畅。
