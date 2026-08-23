| 合并失败（分段损坏/缺失） | 不删源分段，meta 记 merge 错误、置 partial，下次启动经 RecoverPending 重试 |# Bilibili 直播自动录播服务 — 技术文档

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
  → 下播/收尾：meta.json 定稿，所有分段合并为单个 FLV（纯 Go，无外部工具），删除源分段
  → 文件落本地磁盘（record_root，默认 ./recordings）
  → 房间 CRUD + 运行状态 API：RoomService（HTTP/gRPC）
  → Web 管理界面：React + Ant Design SPA（web/，调 HTTP API）
```

明确不做：弹幕合并进视频、切片、上传、转码、外部通知、标题过滤
（无条件全录）、录像自动删除、录制的手动起止（房间增删改查已由 CRUD
API 提供，见 §8）、历史查询 API（磁盘上的 meta.json 即历史）。

---

## 2. 架构

### 2.1 进程模型

录播守护进程实现 Kratos 的 `transport.Server` 接口（`Start(ctx)/Stop(ctx)`），
作为第三个 server 与 HTTP/gRPC 并列注册进 `kratos.Server(gs, hs, daemon)`，
同生命周期启停（`internal/server/daemon.go` 的 `Daemon`，旧名
RecorderJob）：

- `Start` 在 goroutine 里运行 `recorder.Run(rctx)` 并**立即返回**。
  `rctx` 派生自 `context.Background()` 而非传入的 ctx——Start 返回后
  kratos 可能取消其 ctx，守护循环不能因此被杀；取消句柄（`cancel`）与
  退出信号（`done` channel）由 Daemon 自己持有。Run 带错退出记 error，
  正常退出记 info。
- `Stop` 取消 `rctx` 后三路等待：`done`（主循环退出）/ 传入 ctx 取消 /
  `stopWaitTimeout = 45s` 超时，后两者只记 warn 不报错，保证停机永不
  卡死。45s 覆盖了 biz 层的 `finishGracePeriod = 30s`（见 §3.4）加余量。

### 2.2 代码布局（逐文件）

```
api/room/v1/
  room.proto             DTO：RoomService 五个 CRUD RPC（全部 POST，见 §8）/
                         Room（room_id / streamer_name / room_title / record_enabled
                         + OUTPUT_ONLY 运行时字段）/ LiveStatus / RecordStatus /
                         ListRoomsRequest（分页 + 四个 optional 查询字段）
  error_reason.proto     ErrorReason 枚举（ERROR_REASON_INTERNAL /
                         ERROR_REASON_NOT_FOUND / ERROR_REASON_INVALID_ARGUMENT /
                         ERROR_REASON_ALREADY_EXISTS）
  *.pb.go / *_grpc.pb.go / *_http.pb.go   make api 生成，禁止手改

api/account/v1/
  account.proto          DTO：AccountService 四个 RPC（QR 登录创建/轮询、
                         账号状态、登出；全部 POST）——录制器登录态的唯一
                         获取通道（ADR-0003）
  error_reason.proto / *.pb.go   同上，make api 生成

internal/biz/
  room.go                DO：Room（RoomID / StreamerName / RoomTitle / RecordEnabled /
                         CreateTime / UpdateTime）/ LiveStatus / RecordStatus 枚举 /
                         RoomRuntime（读模型：Room + 运行时字段）
                         类型化错误：ErrRoomNotFound（404）/ ErrRoomInvalidArgument（400）/
                         ErrRoomAlreadyExists（409，errors.Conflict）
                         RoomRepo 接口（GetByRoomID / ListRooms / CreateRoom /
                         UpdateRoom / DeleteRoom）
                         + ListQuery（optional 等值过滤 + offset/limit）——房间持久化缝
                         SessionStatsRepo —— 窄统计缝（biz 声明，room API 专用）
                         RoomUsecase：房间 CRUD（写走 repo，读经 withRuntime
                         合并 registry 运行时状态与 stats）
  room_registry.go       RoomRegistry：启动时从 RoomRepo 全量加载房间，
                         持有每个房间的 roomState（Room 快照 + liveStatus /
                         recordStatus / quality 授予清晰度，mutex 保护，
                         repo IO 在锁外）；
                         daemon 写状态，room API 读快照；ApplyRoomInfo
                         更新房态、用平台非空值覆盖 streamer_name /
                         room_title，并经 RoomRepo.UpdateRoom 持久化写回
                         sqlite；SetStreamQuality 记录当前会话实际获得的
                         流清晰度（StartRecording / FinishRecording 清零）
  recorder.go            DO：RoomInfo / StreamQuality / LiveStream
                         DanmakuEvent / RecordingSession / RecordingResult / SessionStats
                         事件类型常量、默认值常量
                         类型化错误：ErrRoomInternal（errors.InternalServer + reason 枚举）
                         哨兵错误：ErrStreamTransient / ErrRiskControl（供决策树分类）
                         两条反向依赖缝（声明在 biz）：
                           RecorderRepo —— 存储缝；LiveClient —— 外部平台缝
                         DanmakuConn —— 常驻弹幕 WS 的 biz 侧抽象
                         （Events / RoomStateUpdates 两个只读通道）
                         ReconnectPolicy；RecorderUsecase：房间监控编排、场次生命周期、
                         断流决策树（纯控制流，不做字节级 IO；无 proto、无存储 tag）
  session_policy.go      sessionPolicy：会话启停决策矩阵（电平触发，
                         ADR-0001/0002）——阶段 idle / running / finishing、
                         record_enabled 门控与收尾后续录规则；watchRoom 只
                         投递输入（房态到达 / 开关翻转 / 场次结束）并执行其
                         Start / Stop / None 决策
  account.go             AccountUsecase + CredentialRepo / PassportClient 缝
                         声明（biz 持有接口，data 实现）：扫码登录（轮询确认
                         才持久化凭据）、账号状态核验（平台不可达不误报登出、
                         凭据失效不删凭据）、本地登出（ADR-0003）

internal/data/
  data.go                Data：db（gorm sqlite，单连接）/
                         bili.Client（bili 子包：全部 B 站流量与登录态）/
                         解析后的 recorder 配置项（mergeEnabled）
                         NewData(c *conf.Data, rc *conf.Recorder) (*Data, func(), error)：
                         打开 sqlite（openDatabase，source 路径校验见 §7.1）→
                         AutoMigrate rooms/credentials 表 → 载入凭据 cookie →
                         构建 bili.Client；cleanup 关闭数据库连接
  room.go                roomPO（rooms 表：streamer_name / room_title 列）/
                         toRoomPO(DO→PO) / toRoomDO(PO→DO)；
                         roomRepo 实现 biz.RoomRepo：CRUD（GetByRoomID /
                         ListRooms / CreateRoom / UpdateRoom / DeleteRoom）、
                         ListQuery → SQL 等值过滤（固定 room_id ASC 排序）、
                         重复 room_id → ErrRoomAlreadyExists（sqlite 主键约束）
  credential.go          credentialPO（credentials 表单例行）/
                         credentialRepo 实现 biz.CredentialRepo
                         （NewCredentialRepo(d *Data) 返回接口）：
                         Get / Save（singleton upsert）/ Delete（幂等）；
                         Save/Delete 落库成功后热替换 *Data 内存 cookie，
                         新登录无需重启即被录制器拾取
  bili/client.go         Client：与 B 站交互的共享长生命周期状态——
                         apiClient(15s 超时) / streamClient(无超时) /
                         passportHTTP(无 cookie jar)、唯一登录态
                         （Cookie/SetCookie 热替换）、WBI 签名器与 buvid 存储；
                         injectAntiRisk / signURL / fetchJSON 风控基础设施
  bili/live.go           liveClient 实现 biz.LiveClient：GetRoomInfo / OpenLiveStream /
                         DanmakuConn 构造；getRoomPlayInfo 候选排序与降档
                         （pickFLVStream 纯函数）；风控编排统一委托 riskGuard
  bili/risk.go           riskGuard：全部 B 站 API 流量的风控编排深模块——
                         冷却闸门、412/-352 刷新重试、兜底调用、错误分类与
                         每房间阶梯冷却；端点只构造请求、解析响应、翻译业务码
  bili/wbi.go            WBI 签名（nav API 取密钥，1h 缓存，w_rid/wts）
  bili/buvid.go          buvid3/buvid4 指纹（spi，24h 缓存，cookie 注入替换语义）
  bili/danmaku.go        danmakuConn 实现 biz.DanmakuConn：二进制包协议、认证、
                         30s 心跳、90s 读超时、protover3 brotli / protover2 zlib、
                         断线指数退避重连、事件解析与过滤、cmd 分发；
                         房态命令（LIVE/PREPARING/ROUND/ROOM_CHANGE）触发
                         pushRoomState → getInfoByRoom 复查 → RoomStateUpdates
                         通道投递 *RoomInfo
  bili/passport.go       passportClient 实现 biz.PassportClient：QR 登录
                         二维码生成/轮询（确认时从 Set-Cookie 捕获登录
                         cookie）、nav 账号核验；刻意不走 riskGuard
                         （无 WBI 签名、无重试）
  recorder.go            recorderRepo 实现 biz.RecorderRepo（NewRecorderRepo
                         返回接口；NewSessionStatsRepo 把同一实例转发为
                         biz.SessionStatsRepo）：会话目录/文件名基座推导、
                         PrepareSession（重启续录复用 + 在途 stats 清零）、
                         RecordSession 泵送循环（切段判定、健康巡检）、
                         FinishSession / finalizeSession 收尾合并、
                         RecoverPending 启动补跑
  recorder_segment.go    segmentFile：FLV part + 弹幕 JSONL 文件对，头标签
                         缓存与重注入，writeTag / writeEvent / close
  recorder_session.go    sessionMeta / segmentMeta / danmuLine PO：meta.json
                         读写（tmp+rename 原子写）、分段簿记
                         （append/finishSegmentMeta）、errors 追加
  recorder_stats.go      pumpStats（原子 file/bytes）与 SessionStats 读取
  merge.go               纯 Go 收尾合并：分段 FLV → 单文件（跳 onMetaData、
                         边界平移序列头时间戳）、弹幕 JSONL 拼接、
                         临时文件+字节数校验+原子改名，验证后才删源
  flv/                   FLV tag 解析子包：FileHeader / Tag 读写、关键帧与
                         sequence header 识别（切段点的判定依据）

internal/service/
  room.go                RoomService：嵌入 v1.UnimplementedRoomServiceServer，
                         五个 CRUD handler；einride aip 的 pagination / fieldmask
                         （page_token 解析、page_size 默认 20、
                         update_mask 仅限 record_enabled，读-改-写；
                         fieldbehavior 校验在 server/http.go 的
                         validate 中间件里）；convertRoom（DTO→DO）/
                         convertRoomReply（DO→DTO，含枚举映射与运行时字段
                         （含 granted_qn / granted_qn_desc），五个 RPC 共用）；
                         只调 RoomUsecase
  account.go             AccountService：QR 登录创建/轮询、账号状态、登出四个
                         handler，DTO↔DO 转换，只调 AccountUsecase；
                         平台失败映射 ERROR_REASON_UNAVAILABLE（503）

internal/server/
  http.go                NewHTTPServer：recovery + validate（field_behavior）中间件，
                         v1.RegisterRoomServiceHTTPServer +
                         accountv1.RegisterAccountServiceHTTPServer
  grpc.go                NewGRPCServer：recovery 中间件，
                         v1.RegisterRoomServiceServer +
                         accountv1.RegisterAccountServiceServer
  daemon.go              Daemon（transport.Server，见 §2.1）
  server.go              ProviderSet（NewGRPCServer / NewHTTPServer / NewDaemon）

internal/conf/
  conf.proto             Bootstrap{server, data, recorder}；Recorder 与
                         Data.Database 消息全字段见 §7（房间列表不在配置里）
  conf.pb.go             make config 生成，禁止手改

cmd/suika/
  main.go                配置加载（file source 目录合并）→ wireApp(bc.Server, bc.Data,
                         bc.Recorder, logger) → newApp(logger, gs, hs, daemon)；
                         日志为 slog TextHandler + otel tracing 属性提取
  wire.go / wire_gen.go  Wire 接线（wire_gen.go 禁止手改，go generate 重新生成）

configs/
  config.yaml            data.database 指向 sqlite（./data/suika.db）；recorder 段
                         不含房间列表（房间在 sqlite 的 rooms 表，经 CRUD API 管理）；
                         cookie 字段已废弃不再读取（§7.3）
  credentials.example.yaml  说明性占位（进 git）：凭据不再经配置文件提供，
                         唯一来自 Web 扫码登录（写入 credentials 表）

web/                     管理界面前端（React 19 + TypeScript + Vite + Ant Design 6）：
  src/api/rooms.ts       与 room.proto 对齐的类型 + fetch 封装（全部 POST）
  src/api/auth.ts        与 account.proto 对齐的类型 + fetch 封装
  src/components/RoomList.tsx  房间表格：分页、状态徽标（录制中徽标带授予
                         清晰度 tooltip、下载速度 sparkline）、5s 自动刷新、
                         添加弹窗、record_enabled 启停确认、删除确认
  src/components/AccountBar.tsx / QRLoginModal.tsx  顶栏登录态 + 扫码登录弹窗
  vite.config.ts         开发代理 /v1 → http://localhost:8000
```

### 2.2.1 领域模型与架构图

领域对象关系、分层与仓储/防腐层缝的图示见
`docs/design/architecture-diagrams.md`（应用架构图与 ER 图）；
本文只保留运行时与实现细节。

### 2.3 五条缝与决策/IO 分工

| 缝 | 声明（biz） | 实现（data） | 职责 |
|---|---|---|---|
| 文件存储缝 | `RecorderRepo`（daemon 用：PrepareSession / RecordSession / FinishSession / RecoverPending）；窄接口 `SessionStatsRepo`（仅 SessionStats，room API 专用） | `recorderRepo`（`NewRecorderRepo(d *Data, c *conf.Recorder)` 返回接口，实现分布在 recorder.go / recorder_segment.go / recorder_session.go / recorder_stats.go）；`SessionStatsRepo` 由同一个 `recorderRepo` 实例经转发 provider `NewSessionStatsRepo(repo biz.RecorderRepo)` 实现 | 文件布局、FLV 泵送、meta.json、JSONL、收尾合并 |
| 房间存储缝 | `RoomRepo`（GetByRoomID / ListRooms(ListQuery) / CreateRoom / UpdateRoom / DeleteRoom） | `roomRepo`（`NewRoomRepo(d *Data)` 返回接口；gorm + mattn sqlite） | rooms 表 CRUD、ListQuery → SQL 等值过滤；UpdateRoom 仅供平台信息回写 |
| 平台缝 | `LiveClient` | `liveClient`（`NewLiveClient(d *Data)` 返回接口） | 全部 B 站直播 HTTP API 与弹幕 WS 流量、风控 |
| 凭据存储缝 | `CredentialRepo`（GetCredential / SaveCredential / DeleteCredential） | `credentialRepo`（`NewCredentialRepo(d *Data)` 返回接口；credentials 表单例行） | 登录凭据持久化；Save/Delete 落库后热替换内存 cookie |
| 账号平台缝 | `PassportClient`（CreateQRLogin / PollQRLogin / AccountInfo） | `passportClient`（`NewPassportClient(d *Data)` 返回接口；实现在 bili/passport.go） | passport QR 登录与 nav 核验；刻意不走 riskGuard（无 WBI 签名、无重试） |

控制流/IO 分工：**biz 只做决定**（何时开录、是否重连、何时收尾），
**data 做全部 IO**（HTTP、WS、FLV 解析、文件）。
`LiveStream` 是 biz 层表示外部直播输入的类型：由 `LiveClient.OpenLiveStream` 产出、
原样交给 `RecorderRepo.RecordSession` 消费，biz 不解其内部
（`Body io.ReadCloser` + URL + Quality，同 `*sql.Rows` 穿过业务层的经典形态）。
`DanmakuConn` 同理：biz 只消费 `Events()`（弹幕事件）与
`RoomStateUpdates()`（房态复查结果 `*RoomInfo`）两个通道。

### 2.4 Wire 接线

ProviderSet：

| 包 | ProviderSet |
|---|---|
| data | `wire.NewSet(NewData, NewRecorderRepo, NewSessionStatsRepo, NewLiveClient, NewRoomRepo, NewCredentialRepo, NewPassportClient)` |
| biz | `wire.NewSet(NewRoomRegistry, NewRecorderUsecase, NewRoomUsecase, NewAccountUsecase)` |
| service | `wire.NewSet(NewRoomService, NewAccountService)` |
| server | `wire.NewSet(NewGRPCServer, NewHTTPServer, NewDaemon)` |

`wire_gen.go` 的实际构造顺序：
`NewData → NewRoomRepo → NewRoomRegistry → NewRecorderRepo →
NewSessionStatsRepo → NewRoomUsecase → NewRoomService →
NewPassportClient → NewCredentialRepo → NewAccountUsecase →
NewAccountService → NewGRPCServer / NewHTTPServer → NewLiveClient →
NewRecorderUsecase → NewDaemon → newApp`。
`NewData` 依据 `conf.Data` 打开 sqlite 并 AutoMigrate rooms /
credentials 表，
`NewRoomRepo(d *Data)` 挂在其上；`NewRoomRegistry(repo)` 改吃
`biz.RoomRepo`（不再解析配置），启动时全量加载房间，返回 error，
加载失败即启动失败。`NewRoomUsecase(repo, reg, stats)` 注入 repo 与
registry：CRUD 写 repo，读合并 registry 运行时状态；recorder 从平台
拿到的主播名/房间标题也由 registry 经 repo 的 `UpdateRoom` 覆盖写回
sqlite（§3.2）。`conf.Recorder` 注入 `NewData`、`NewRecorderRepo`、
`NewRecorderUsecase` 三处，各自取自己负责的字段并套用默认值（§7.2）。

---

## 3. 运行时模型

### 3.1 goroutine 结构

```
App.Run
 └─ Daemon.Start → goroutine: RecorderUsecase.Run(rctx)
     ├─ repo.RecoverPending            启动补跑：补完上次遗留的合并
     └─ 监督循环（订阅 registry 变更通知，reconcile 快照 ↔ 监控集合）
         └─ registry 中每个房间（无论 record_enabled）→ monitorRoom goroutine
             └─ watchRoom（持有一条 danmakuConn）
                 ├─ danmakuConn.run goroutine（data 层，内部自动重连 WS）
                 │    ├─ readLoop goroutine（解包、分发）
                 │    └─ 30s 心跳 ticker
                 ├─ 兜底轮询 timer（默认 600s ±10% 抖动）
                 └─ 开播且 record_enabled 时 → launchSession goroutine（sessionHandle：cancel + done）
                     ├─ acquireSlot（max_concurrent 并发槽）
                     ├─ registry.StartRecording + repo.PrepareSession
                     ├─ recordLoop：OpenLiveStream → repo.RecordSession 泵送 → 断流决策树
                     │    └─ RecordSession 内部：tag 读取 goroutine（chan 缓冲 512）
                     └─ SetMerging → repo.FinishSession（30s grace，脱离运行 ctx）→ 合并
```

- `Run`：先 `RecoverPending`（失败只记日志），然后订阅 RoomRegistry 的
  变更通知，作为监督循环维护 roomID → monitor 协程的映射（reconcile）：
  新增房间无论是否配置录制立即启动监控，删除的房间立即取消监控（移入
  retired 自行优雅收尾），record_enabled 翻转不增删协程、只投递重评估信号。
  rooms 为空时记 warn 空转，但对后续变更保持响应。
- `monitorRoom`：`watchRoom` 返回错误且 ctx 未取消时记错误、
  `registry.NoteError`，等 `redialDelay = 10s` 后重建弹幕连接
  （防御性循环；当前 `liveClient.DanmakuConn` 构造不会失败，重连都在
  conn 内部完成）。
- `watchRoom` 的 select 六路：ctx 取消（cancel 活动场次并等 done）/
  弹幕事件排空（无活动场次时丢弃；有活动场次时该分支是 nil channel，
  事件由 RecordSession 直接消费）/ 场次结束（`active.done` → 清 active）/
  `RoomStateUpdates` 房态事件 / 轮询定时器 / roomChanged 重评估信号。
- 房态事件与轮询共用同一套动作：`registry.ApplyRoomInfo` 记录房态；
  "在播、record_enabled 且无活动场次" → `launchSession`；"未在播但有活动场次" →
  `active.cancel()`。轮询失败只 warn + NoteError（ctx 取消引起的失败
  除外——属停机/删房间的正常路径），不重置定时器之外的任何状态。
- roomChanged 重评估信号（监督循环在 record_enabled 翻转时投递）：重读注册表
  最新开关，投递该房间 sessionPolicy 的 `RecordEnabledFlipped`——关闭录制
  且在录 → Stop（取消活跃会话）；开启录制且最新房态在播 → Start；其余 None。
  场次结束事件同样经 `SessionFinished` 决策（收尾中仍在播 → 恢复录制）。
  仅名称/标题变更不触发重评估。

### 3.2 房间状态

房间集合来源是 sqlite：`NewRoomRegistry(repo)` 在启动时从 RoomRepo
全量加载一次（`ListQuery{Offset: 0, Limit: math.MaxInt32}`），存入
roomID → roomState 的 map；此后房间 API 的增删改在落库成功后同步回写
注册表（`Add` / `Update` / `Remove`），并经合并式变更通知唤醒订阅者。
录制守护进程订阅该通知，实时调和监控集合，**无需重启**（语义见 §8.1）。
nil repo 容忍为空 registry（记 warn）；加载失败即启动失败。

biz 在 `RoomRegistry` 里为每个加载的房间维护 `roomState`（单一 mutex
保护容器与状态字段；快照方法持锁拷贝，变更方法持锁完成读-改-写，
repo IO 必须在锁外）：

| 字段 | 取值 | 说明 |
|---|---|---|
| `room` | `Room` 快照 | 持久字段的内存副本（含平台刷新后的 streamer_name / room_title） |
| `liveStatus` | `LiveStatusUnknown` / `LiveStatusPreparing` / `LiveStatusOnAir` | 平台侧开播状态（ApplyRoomInfo 只会写后两者） |
| `recordStatus` | `RecordStatusIdle` / `RecordStatusRecording` / `RecordStatusMerging` / `RecordStatusError` | 录制器自身状态 |
| `quality` | `StreamQuality` | 当前会话 B 站实际授予的流清晰度（recordLoop 拉流成功后经 `SetStreamQuality` 写入，StartRecording / FinishRecording 清零；是 room API `granted_qn` / `granted_qn_desc` 的数据源） |
| `sessionStartedAt` | time | 当前场次开始时刻（StartRecording 置 now，FinishRecording 清零） |
| `lastError` | string | 最近一次错误（StartRecording 清零；NoteError/FailRecording 写入） |

房态来源只有 `getInfoByRoom`：WS 房态命令（LIVE/PREPARING/ROUND/
ROOM_CHANGE）与兜底轮询都只是触发/执行一次房态复查。复查结果经
`RoomStateUpdates` 通道以 `*RoomInfo` 形式交给 biz 的
`ApplyRoomInfo`：

1. 更新 live 状态（在播 → `LiveStatusOnAir`，否则 `LiveStatusPreparing`）；
   房间不在 registry（如刚被删除）则整体忽略。
2. 用平台非空值覆盖内存里的 `streamer_name` / `room_title`，随后在锁外
   调 `repo.UpdateRoom` 写回 sqlite。**覆盖语义**：平台数据优先于库里
  已有值。覆盖后的值重启不丢。
3. 写回失败只记 warn，内存快照仍然更新（降级不丢状态）。

启动后创建的房间在落库成功后即由 `RoomUsecase` 登记进 registry，录制
守护进程随即开始监控（record_enabled 只决定是否录制，见 §8.1），API 读取的
运行时字段从默认值开始随监控更新。

### 3.3 场次生命周期

`runSession` 端到端拥有一个场次（由 `launchSession` 派生可取消 ctx 并
起 goroutine，`sessionHandle{cancel, done}` 供 watchRoom 管理）：

1. **acquireSlot**：`max_concurrent > 0` 时占并发槽；槽满则排队等待
   （记日志），ctx 取消则放弃。`max_concurrent = 0` 表示不限。
2. **组装 RecordingSession**：`registry.Room(roomID)` 取库存快照，
   `RoomName = firstNonEmpty(库存 streamer_name, API 主播名, roomID)`，
   `Title`、`LiveStartTime` 取触发开播的房态快照（场次中途标题变化
   不改名；`LiveStartTime` 决定目录，重连续录落回同一场次）。
3. **StartRecording**：置 `RecordStatusRecording`、清零授予清晰度、刷新
   sessionStartedAt、清 lastError。
4. **PrepareSession**：创建（或重启后重定位）场次目录与 meta.json，
   并把该房间的在途 stats（当前文件/字节数）清零——否则新场次的字节
   会累加到上一场的计数上。失败 → `FailRecording` 返回。
5. **recordLoop**：见 §4.5。
6. **收尾**：先置 `RecordStatusMerging`，再用
   `context.WithoutCancel(ctx)` + `finishGracePeriod = 30s` 的脱离 ctx
   执行 `FinishSession`，   未完成的合并由下次启动 `RecoverPending` 补跑。成功 →
   `FinishRecording`（回 `RecordStatusIdle`、清 sessionStartedAt），失败 →
   `FailRecording`。
7. **releaseSlot**（defer）。

录制中房间状态为 `RecordStatusRecording`；Get/List 只对该状态的房间追加
`SessionStatsRepo.SessionStats`（当前 part 路径 + 累计字节，原子计数，零额外采集）。
授予清晰度（`granted_qn` / `granted_qn_desc`）则始终来自 registry 快照，
录制中为实际档位，非录制时为默认零值。

### 3.4 优雅停机

```
SIGTERM → kratos 触发各 server.Stop
  → Daemon.Stop 取消 rctx
    → watchRoom：cancel 活动场次并等待 done
      → recordLoop 因 ctx.Err() 返回（当前 part 已刷盘，FLV 至最后完整 tag 有效）
      → FinishSession 用脱离 ctx（30s grace）标 merging 并尽量完成合并
    → monitorRoom 退出，Run 返回
  → Stop 等待 done / 传入停机 ctx / 45s 三者先到，超时仅 warn 继续关停
未完成的合并 → 下次启动 RecoverPending 补跑
```

---

## 4. 核心流程

### 4.1 开播检测（WS 事件驱动 + 轮询兜底）

1. `getDanmuInfo`（WBI 签名）取 token + 接入节点列表；仍被风控（-352）
   则降级到旧接口 `getConf`（无需 WBI）；两者都失败 → 该房间进风控冷却。
   节点列表为空时用保底地址 `wss://broadcastlv.chat.bilibili.com:2245/sub`。
2. 拨号：节点列表**随机打乱**，每个节点依次尝试 protover 3（brotli）、
   2（zlib）；op7 认证包携带 `uid / roomid / protover / platform=web /
   type=2 / key=token / buvid`（uid 取生效 cookie 的 DedeUserID，未登录
   为 0：登录后 token 与账号绑定，uid 不一致会被服务器断连；buvid 优先
   取 cookie 里的 buvid3，缺则 spi 现取）；
   等 op8 认证回复（5s 超时，`code==0` 才算成功）。
3. **每次（重）连接成功后先调 `getInfoByRoom` 重建房态**（`pushRoomState`），
   结果以 `*RoomInfo` 投递到 `RoomStateUpdates` 通道，覆盖断线/休眠期间
   错过的 LIVE/PREPARING。
4. 常驻期间处理 cmd（B 站会在 cmd 后挂变体后缀如 `DANMU_MSG:4:0:3:`，
   先按 `:` 截断再分发）：
   - `LIVE` / `PREPARING` / `ROUND` / `ROOM_CHANGE` → `pushRoomState` 复查；
   - 各事件 cmd → 解析后投递 Events 通道（§4.4）。
   - biz 侧对房态事件幂等：已在录时收到"在播"不重复开场次，
     未录时收到"未开播"只取消不存在的场次（无副作用）。
5. WS 保活：30s 心跳（op2）；读超时 90s（约 3 个心跳周期）杀半开连接，
   进入重连；重连指数退避 2s → 30s 封顶。
6. 兜底轮询：每 600s ±10% 抖动执行一次（间隔为代码常量）
   `GetRoomInfo`，发现"在播但无活动场次"立即启动录制，"未开播但有活动
   场次"则取消场次。轮询请求走风控层（§5）。

### 4.2 拉流

- `getRoomPlayInfo`：`protocol=0,1 & format=0,1,2 & codec=0,1 & qn=10000
  & platform=web`（qn 固定请求原画；请求不到时平台自动授予次高档位）。
  候选展开 stream×format×codec×url_info，过滤
  `base_url` 含 `.flv` 的候选（录制必须 FLV），avc 优先级 100、其他 90，
  取最高优先级 URL = `host + base_url + extra`，并记录该 codec 行的
  `current_qn`（平台实际授予的档位）。
- 授予档位优先取选中 codec 行的 `current_qn`，缺失退用 playurl 顶层
  `current_qn`；两者都拿不到则清晰度未知（desc 为空、不记降档日志）。
  授予档位已知且低于请求值时**接受最高可得档位**（自动降档，记 warn 日志，
  实际档位写入 meta.json，并经 `registry.SetStreamQuality` 登记，
  即 room API `granted_qn` / `granted_qn_desc` 的数据源）。清晰度描述
  优先用 API 的 `g_qn_desc`，缺则查内置表（20000=4K、10000=原画、
  400=蓝光、250=超清、150=高清、80=流畅）。
- 房态 API 的标题兜底：`getInfoByRoom` 返回的 `title` 为空时退用主播名；
  `live_start_time ≤ 0` 时退化为本机当前时间。
- data 层用 `streamClient`（无超时，长读连接，取消走请求 ctx）打开流 URL，
  注入桌面 Chrome UA / `Referer: https://live.bilibili.com/{room}` / 原始 cookie。
  打开失败或 HTTP 非 2xx → 包装为 `biz.ErrStreamTransient`。
  拉流为纯 Go HTTP 长读，不经任何外部工具。

### 4.3 录制引擎（Go 解析 FLV 直接落盘）

```
HTTP body（原始字节，LiveClient 打开）
  → RecordSession 泵送（data/recorder.go）
      ├─ flv.ParseHeader 读 9 字节文件头 + PreviousTagSize0
      ├─ tag 读取 goroutine：flv.ReadTag 逐个送入 chan（缓冲 512）
      ├─ 泵送开始时把实际清晰度写回 meta.json（quality 字段）
      ├─ headerCache 缓存 onMetaData / AVC sequence header / AAC sequence header
      ├─ 首个 tag 到达时 openNewSegment：part 号 = 目录扫描续号，
      │     新 part = FLV 文件头 + 缓存的三类头 tag + 后续 tag（可独立播放）
      ├─ 切段判定 shouldSplit：段时长达 120 分钟（代码常量）
      │     且当前 tag 是视频关键帧；或超出 splitOverrun = 15s 强制切
      │     （时间戳保持流内原值，不重置；startTs = 该 part 首个正文 tag）
      ├─ 缓存时机：开/切段判定之后才更新缓存——触发新段的 tag 不会被
      │     重复注入（否则 openSegment 注入一次、泵送又写一次）
      ├─ 弹幕事件同步写当前 part 的 JSONL（无活动段时丢弃）
      ├─ 健康巡检：每 30s 检查累计字节，
      │     连续 3 轮无增长 → 中止本次连接
      │     （返回普通错误 → 走决策树普通重连分支）
      └─ 统计：pumpStats（atomic 文件路径/字节数），字节数跨重连续泵累加
          （baseBytes + 本次泵送量；PrepareSession 在新场次开始时清零）
```

为什么用纯 Go 录制而不依赖外部工具：切段必须发生在 FLV tag 层（重启外部进程拿不到
sequence header，新 part 不可播）；纯 Go 落盘还换来抗崩溃（FLV 无 moov
问题，进程猝死文件仍有效）与零外部依赖（收尾合并同样是纯 Go，见 §4.6）。

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

`INTERACT_WORD`（进场词）与点赞类量级约为弹幕 10 倍、切片价值≈0，
不录制：`danmakuConn.dispatch` 直接忽略该命令，biz 与 repo 只见已过滤
事件。

投递语义：Events 通道缓冲 4096、RoomStateUpdates 缓冲 16，`emit` 均
非阻塞——Events 缓冲满（只可能发生在无场次消费时）直接丢弃，永不阻塞
读包循环；RoomStateUpdates 满时丢弃本次房态（下一个命令会再次触发复查）。
弹幕文件与视频 part **一一对应**：切段时同步切换 JSONL 输出文件。

### 4.5 断流决策树（biz.recordLoop）

```
每轮开始：lc.OpenLiveStream（重取 URL，可能换 CDN 节点）
  ├─ 失败：
  │   ├─ 非瞬时故障（风控拒绝等）→ 记 lastError，结束场次（不重试）
  │   └─ ErrStreamTransient → lc.GetRoomInfo 复查
  │       ├─ 失败 → 记错误，结束场次（失败由 ctx 取消引起则静默返回，
  │       │   不记错误：监控已因下播事件取消了本场次）
  │       ├─ 已下播 → 正常收尾（主播刚下播、流已被撤属正常结束，
  │       │   不记 lastError、不按错误展示）
  │       └─ 仍在播 → 按 cdn_transient_budget（代码常量 5）指数退避重试；
  │           耗尽 → 保留已录内容收尾
  └─ 成功 → session.Quality = 实际档位 → registry.SetStreamQuality 登记
      → repo.RecordSession 泵送
泵送返回（EOF / 读错误 / 巡检中止 / 写失败 / ctx 取消）
  ├─ ctx 已取消 → 返回（停机路径）
  └─ lc.GetRoomInfo 复查
      ├─ 失败 → 记错误，结束场次（失败由 ctx 取消引起则静默返回，不记错误）
      ├─ 已下播 → ApplyRoomInfo 后正常收尾
      └─ 仍在播：
          ├─ err 是 ErrStreamTransient（CDN 瞬态：打开失败/HTTP 非 2xx/
          │   FLV 头解析失败/读错误）
          │   ├─ cdn_transient_budget（5）未耗尽 →
          │   │   指数退避 min(2s << attempt, 60s) → 下一轮
          │   └─ 耗尽 → 保留已录内容收尾（记成功，非失败）
          ├─ auto_reconnect = false → 收尾（代码内恒为 true）
          ├─ 重连次数 < max_reconnect（3）→ 等 reconnect_delay（10s）→ 下一轮
          └─ 配额耗尽 → 保留已录内容收尾
```

`ErrStreamTransient` 与 `ErrRiskControl` 是 biz 声明的哨兵错误，data 在
错误源头包装（`fmt.Errorf("%w: ...")`），决策树用 `errors.Is` 分类。
预算、延迟参数来自 `conf.Recorder.ReconnectOptions`（§7）。

### 4.6 场次收尾与合并（repo.FinishSession）

1. meta.json：`status = merging`、写 `end_time`、刷新 title 与 quality，
   随即落盘（崩溃安全：合并结果之后持久化）。meta 不存在视为无录制内容，
   直接 noop 成功。
2. `finalizeSession` 把整场会话的分段合并为单个文件（纯 Go，无任何外部
   工具）：

   - `merge_enabled = false`：所有段标 `flv_kept = true`，直接 `done`，
     保留散装分段。
   - `merge_enabled = true`：`mergeSessionFiles` 将全部 `_partN.flv` 合并
     为 `{base}.flv`，弹幕 JSONL 按 part 顺序拼接为 `{base}.danmu.jsonl`。
     FLV 合并规则：
     - 第 2 段起跳过 FLV 文件头；所有分段的 onMetaData 脚本标签一律跳过
       （不写元数据，文件名自带日期与标题）。
     - 第 2 段起，段首重新注入的序列头时间戳平移到合并边界，保证全片
       时间戳单调不回跳（分段本就用绝对毫秒时间戳，跨段连续）。
     - 单段场次同样走完整合并路径，行为一致。
   - **删除前必验证**：输出先写临时文件，校验字节数与逐标签累加值一致
     后原子改名；改名成功后才删除源分段与源弹幕。
   - 合并失败（分段损坏/缺失等）：错误记入 meta（`stage = merge`）、
     源分段全部保留、`status = partial`，由下次启动补跑重试。
3. 合并成功 → `status = done`，合并产物文件名记入 `merged_video` /
   `merged_danmaku`。**绝不删除未验证文件。**

### 4.7 启动补跑（repo.RecoverPending）

`Run` 的第一步（错误只记日志不致命）：glob
`<record_root>/*/*/*.meta.json`，逐文件处理：

| meta.status | 动作 |
|---|---|
| `recording` / `merging` | 视为被中断的场次：补 end_time → finalizeSession |
| `partial` | 仅当所有源分段仍在磁盘上时重跑 finalize |
| `done` | 无需处理 |
| 其他状态（旧版本遗留，如 `remuxing`） | 跳过 + 警告日志，原样保留（不兼容旧数据） |

---

## 5. 风控层（data）

所有 B 站请求统一走 `fetchJSON`：桌面 Chrome UA +
`Referer: https://live.bilibili.com/<room>` + `Origin` + cookie；
HTTP 412/403/429 → `errHTTPRiskControl`。

**WBI 签名**（`bili/wbi.go`，移植 hikami-go）：`/x/web-interface/nav` 取
img_key/sub_key → 64 位置换表混出 32 字符 mixin_key（缓存 1h）；签名即
按 key 排序的查询串（剔除 `!'()*`）+ mixin_key 取 MD5，附加 `w_rid`/`wts`。
`getDanmuInfo`、`getInfoByRoom`、`getRoomPlayInfo` 都会先 WBI 签名；
签名失败降级为不签名继续请求（记 warn）。

**buvid 指纹**（`bili/buvid.go`，移植 hikami-go）：`/x/frontend/finger/spi`
取 buvid3/buvid4，按 cookie 键缓存 24h；注入 cookie 时先删旧 buvid3/4
再追加（B 站取同名第一个，替换语义保证新指纹生效）。buvid 获取失败
降级为裸 cookie。

**-352 / HTTP 风控处理**（统一由 `riskGuard` 编排，`bili/risk.go`）：

1. 风控命中（-352 或 HTTP 412/403/429）→ `refreshRisk()`（强刷 WBI 密钥 +
   作废 buvid 缓存）→ 原请求重试一次。
2. `getDanmuInfo` 二次仍 -352 → 降级旧接口 `getConf`（无 WBI，guard 的
   可选 fallback 钩子）。
3. 仍失败 → 该房间进**阶梯冷却** 5min → 10min → 20min（按连续失败次数
   进阶，封顶 20min）；冷却期内 guard 直接拒绝该房间的
  GetRoomInfo/OpenLiveStream/getDanmuInfo 调用（返回 `ErrRiskControl`）。
4. 任一 API 成功 → `noteSuccess` 清零该房间冷却。

cookie 过期不是错误：表现为拉流拿不到原画 → 自动降档并记录 meta
（运维动作：Web 页重新扫码登录，凭据热替换即时生效，§7.3）。
未登录也能运行（启动记 warn），但更易触发风控且拿不到原画。

---

## 6. 磁盘数据结构

### 6.1 目录与命名

```
<record_root>/                              默认 ./recordings，可配置
  <room_id>_<主播名>/                        主播名清洗后 ≤32 rune
    <YYYY-MM-DD>/                           开播日期（live_start_time）
      <YYYYMMDD>_<HHMM>_<直播标题>_part1.flv
      <YYYYMMDD>_<HHMM>_<直播标题>_part1.danmu.jsonl
      <YYYYMMDD>_<HHMM>_<直播标题>_part2.flv
      <YYYYMMDD>_<HHMM>_<直播标题>_part2.danmu.jsonl
      <YYYYMMDD>_<HHMM>_<直播标题>.meta.json

收尾合并后（§4.6）：所有 _partN.flv 合并为 {base}.flv、弹幕拼接为
{base}.danmu.jsonl，源分段与源弹幕删除（meta.json 记录产物文件名）。
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
  "status": "recording | merging | done | partial",
  "segments": [
    {
      "part": 1,
      "video": "..._part1.flv",
      "flv_kept": false,
      "danmaku": "..._part1.danmu.jsonl",
      "wall_start": 1754912400,
      "wall_end": 1754919600,
      "ts_start": 12340,
      "ts_end": 7199840,
      "bytes": 4831838208,
    }
  ],
  "merged_video": "..._base.flv",
  "merged_danmaku": "..._base.danmu.jsonl",
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

message Server {
  message HTTP { string addr = 2; }   // 未设置走 kratos 默认
  message GRPC { string addr = 2; }
  HTTP http = 1;
  GRPC grpc = 2;
}

message Data {
  message Database {
    string source = 2;   // sqlite 文件路径；父目录缺失时自动创建
  }
  Database database = 1;
}

message Recorder {
  // 监控的房间在 sqlite 的 rooms 表里，经 Room CRUD API 管理，
  // 配置不持有房间（字段号 1 空置保留）。
  string cookie = 2 [deprecated = true];  // 已废弃：凭据来自扫码登录写入
                                          // credentials 表，此字段不再被读取
  string record_root = 3;     // 默认 ./recordings
  int32 max_concurrent = 7;   // 0 = 不限
  optional bool merge_enabled = 8;  // 未设置默认 true；显式 false = 保留散装分段
}
```

配置治理原则：**只保留随部署环境变化的项**（路径、端口、并发上限、
收尾是否合并；凭据不再是配置项，见 §7.3）。行为调优不做配置，默认值写死在代码里（§7.2）；被移除的
字段在 proto 中 `reserved` 其字段号与名称。`merge_enabled` 用
`optional`，使"显式 false"与"未设置"可区分（proto 标量零值歧义）。

**数据库**：只支持 sqlite（driver 不做配置），`openDatabase` 在 source
为空时启动失败；source 即 sqlite 文件路径（config.yaml 配
`./data/suika.db`）；gorm 连接池固定单连接，避免嵌入式库上的
SQLITE_BUSY。source 的路径校验规则（`sqliteFilePath`）：

- 容忍并剥离单个 `file:` 前缀；拒绝带 authority 的 `file://...` URI；
- 拒绝查询参数（`?mode=...`）——只支持纯文件路径，配置语义简单确定；
- 拒绝目录式输入（以 `/` 结尾、clean 后为 `.`/`/`、或已存在的目录）。

父目录缺失时 `ensureSQLiteDir` 自动创建；db 文件本身由 sqlite 首次打开
时创建。开库成功后 `NewData` 立即 AutoMigrate `rooms` 表。db 文件是
运行期数据，不进 git（`/data/` 已加入 .gitignore）。

### 7.2 代码默认值与应用位置

配置项只剩四个有默认值的（其余必填或由环境决定）：

| 配置项 | 代码默认 | 应用位置 |
|---|---|---|
| record_root | ./recordings | data.NewRecorderRepo |
| max_concurrent | 0（不限） | biz.NewRecorderUsecase |
| merge_enabled | true | data.NewData（optional，nil→true） |
| server http/grpc addr | kratos 内置默认 | server.NewHTTPServer / NewGRPCServer |

行为调优不做配置，全部是代码常量：

| 常量 | 值 | 所在层 |
|---|---|---|
| 兜底轮询间隔 | 600s（±10% 抖动） | biz |
| 自动重连 | 恒开 | biz（ReconnectPolicy） |
| 最大重连次数 | 3 | biz |
| 重连延迟 | 10s | biz |
| CDN 瞬时故障重试预算 | 5 | biz |
| CDN 退避基数 / 封顶 | 2s / 60s | biz |
| 监控重建（重拨）间隔 | 10s | biz |
| FinishSession 脱离 grace | 30s | biz |
| 分段时长 | 120 分钟 | data.NewRecorderRepo |
| 健康检查间隔 / 失败轮数 | 30s / 3 轮 | data.NewRecorderRepo |
| 请求清晰度 | 10000（原画；不足时平台自动降档） | data/bili（live） |
| 切段关键帧等待上限 | 15s | data |
| 弹幕事件缓冲 / 房态更新缓冲 | 4096 / 16 | data |
| WS 心跳 / 读超时 | 30s / 90s | data |
| WS 重连退避 | 2s→30s | data |
| apiClient 超时 | 15s | data |

另有 room repo 的 ListRooms 对 `Limit ≤ 0` 兜底 20、`Offset < 0` 报
ErrRoomInvalidArgument。

### 7.3 凭据（扫码登录，ADR-0003）

- 凭据不再来自配置文件：`recorder.cookie` 已废弃、不再被读取（配置里
  填了值启动时只记 warn）；`configs/credentials.yaml` 退役，
  `credentials.example.yaml` 只留说明性占位。
- 唯一来源是 sqlite `credentials` 表的单例行，经 Web 管理页扫码登录获取
  （AccountService `/v1/account/qr-login/*`，调 B 站 passport 接口，
  确认时从轮询响应的 Set-Cookie 捕获登录 cookie）。
  `AccountUsecase.PollQRLogin` 只在轮询确认时持久化；
  `credentialRepo` 做 singleton upsert。
- 即时生效是 `CredentialRepo` Save/Delete 的副作用：先落库，再热替换
  `bili.Client` 内存 cookie（mutex 保护，经 `Data.Cookie()` 统一读取；
  WBI 签名器持有 cookie provider 而非启动快照），录制器无需重启。
  在途连接（弹幕 WS、拉流中的流）沿用旧 cookie，下次重拨时更新。
- 启动时 `NewData` 经 `loadCredentialCookie` 读取单例行；无行 = 空
  cookie 启动（记 warn，可运行但拿不到原画、更易触发风控）。登出是本地
  语义：删表行 + 清内存，不调 B 站登出接口；凭据失效（账号状态核验失败）
  不删凭据，由用户重新扫码或手动登出。

### 7.4 现网 config.yaml 说明

- 配置里没有任何房间：房间清单在 sqlite（`data.database.source`）的
  rooms 表里，经 CRUD API 管理；全新安装首次启动时 rooms 表为空，
  recorder 记 warn 空转但对后续加房保持响应，CreateRoom 加房后立即
  开始监控（§8.1）；
- `merge_enabled: true`（收尾合并分段；设 false 则保留散装分段）；
- `max_concurrent: 10` 按机器性能调过；
- `cookie: ""` 废弃占位，不再被读取（凭据来自扫码登录，§7.3）。

---

## 8. 房间 CRUD API

`api/room/v1/room.proto`（package `room.v1`）声明五个 RPC，HTTP 映射
**全部为 POST、body=`*`**（无路径参数、无 GET/PUT/DELETE；gRPC 同名
方法照常）：

| RPC | HTTP | 语义 |
|---|---|---|
| CreateRoom | `POST /v1/rooms/create` | 注册新房间；响应回填 create_time / update_time，运行时字段为默认值；room_id 重复 → 409 |
| ListRooms | `POST /v1/rooms/list` | 分页列表，支持四个 optional 等值查询字段；合并运行时状态返回 |
| GetRoom | `POST /v1/rooms/get` | body 传 room_id，合并运行时状态；不存在 → 404 |
| UpdateRoom | `POST /v1/rooms/update` | 通过 update_mask 更新 `record_enabled`；主播名和房间标题只读；不存在 → 404 |
| DeleteRoom | `POST /v1/rooms/delete` | body 传 room_id，返回 `google.protobuf.Empty`；不存在 → 404 |

- `Room` 消息 = 持久字段（room_id / streamer_name / room_title /
  record_enabled / create_time / update_time；主播名与房间标题由 B 站信息
  回填，接口侧为只读字段）
  + 运行时字段（live_status / record_status / current_file /
  bytes_written / download_speed_bps / granted_qn / granted_qn_desc /
  session_started_at / last_error，全部标注
  OUTPUT_ONLY）。**运行时字段只在 Get/List 响应中由 registry 合并返回；
  Create/Update 的响应里是默认值**（LIVE_STATUS_UNSPECIFIED /
  IDLE / 零值；Delete 返回 Empty），也不参与查询过滤。
- 五个 RPC 同时注册 HTTP 与 gRPC；中间件沿用 recovery + validate
  （Create/Update 的 `room` 字段与 update_mask、Get/Delete 的 `room_id` 声明为 REQUIRED）。根目录 `openapi.yaml` 由 `make api`
  一并重新生成。
- 所有路由都是字面 POST 路径，不存在通配路由，注册顺序无歧义。

**ListRooms 细则**：

- 查询不走 AIP filter 字符串，而是请求体里的四个 optional 字段：
  `room_id`（int64）/ `record_enabled`（bool）/ `streamer_name`（string）/
  `room_title`（string），均为**等值匹配**（AND 组合，未设置即不过滤）。
- 排序固定 `room_id ASC`（repo 层写死），不支持 order_by。
- 分页：`page_size` 未设置（或 ≤0）默认 20；`page_token` 为 offset 型
  token（einride pagination），返回行数 ≥ page_size 时附 `next_page_token`。
- page_token 非法 → INVALID_ARGUMENT。

**UpdateRoom 细则**：service 仅允许 `update_mask` 包含 `record_enabled`，用于
切换房间录制开关；`streamer_name` / `room_title` 由平台回填，不提供手动更新。

**平台刷新语义**：recorder 经 `ApplyRoomInfo` → `repo.UpdateRoom` 写回
主播名/房间标题时是**覆盖**语义——平台非空值优先于库里已有值。
写回失败只记 warn，内存快照仍然更新
（service 测试 TestRoomServicePlatformRefreshOverridesStreamerName 专门
覆盖该语义）。

错误码：

| 场景 | HTTP | ErrorReason |
|---|---|---|
| Get/Update/Delete 的 room_id 不存在 | 404 | ERROR_REASON_NOT_FOUND |
| room_id ≤ 0、update_mask 为空或含不支持字段、page_token 非法、offset 越界 | 400 | ERROR_REASON_INVALID_ARGUMENT |
| CreateRoom 的 room_id 已存在 | 409 | ERROR_REASON_ALREADY_EXISTS |
| recorder 内部非法状态（如空流句柄） | 500 | ERROR_REASON_INTERNAL |

biz 层对应 `ErrRoomNotFound`（errors.NotFound）/
`ErrRoomInvalidArgument`（errors.BadRequest）/
`ErrRoomAlreadyExists`（errors.Conflict，409）；重复 room_id 由 sqlite
主键约束兜底（mattn/go-sqlite3 的 ErrConstraint → AlreadyExists）。
`ErrStreamTransient` / `ErrRiskControl` 仅作内部分类哨兵，不出 API。

biz ↔ proto 枚举映射（`service.convertRoomReply`，五个 RPC 共用）：

| biz | proto LiveStatus | biz | proto RecordStatus |
|---|---|---|---|
| LiveStatusUnknown | LIVE_STATUS_UNSPECIFIED | RecordStatusIdle | RECORD_STATUS_IDLE |
| LiveStatusPreparing | LIVE_STATUS_PREPARING | RecordStatusRecording | RECORD_STATUS_RECORDING |
| LiveStatusOnAir | LIVE_STATUS_LIVE | RecordStatusMerging | RECORD_STATUS_MERGING |
| | | RecordStatusError | RECORD_STATUS_ERROR |

`RECORD_STATUS_REMUXING` 为历史遗留死值（旧转封装流程），新流程不再产生，保留仅为避免破坏性变更。

数据源：持久字段来自 sqlite（repo）；运行时字段来自 `RoomRegistry`
快照（mutex；授予清晰度 granted_qn / granted_qn_desc 由录制器经
`SetStreamQuality` 写入，仅录制中非零）+ 仅录制中房间追加
`SessionStatsRepo.SessionStats`
（泵送层原子计数，stats 出错静默跳过只丢进度）。时间戳字段为零值时
不出现在响应里（convertRoomReply 逐个判零）。

**Web 管理界面**（`web/`）：React 19 + TypeScript + Ant Design 6 +
Vite 的 SPA，是 HTTP API 目前唯一的图形化消费者。`RoomList` 组件提供
房间表格（live/record 状态徽标——录制中徽标带授予清晰度
（granted_qn / granted_qn_desc）tooltip、已写字节、下载速度
sparkline、最近错误、房间 ID 直达 B 站直播间、5s 自动刷新）、
offset token 栈式翻页、添加弹窗
（room_id / record_enabled）与删除确认
（Popconfirm）。顶栏另有账号栏（`AccountBar`）：扫码登录弹窗
（`QRLoginModal`）、登录状态显示与登出。开发模式经 vite 代理
`/v1` → `http://localhost:8000`。
前端类型与 room.proto 手工对齐（`web/src/api/rooms.ts`）、与
account.proto 手工对齐（`web/src/api/auth.ts`），改 proto
时需同步。

### 8.1 CRUD 与录制进程的时序（实时生效）

1. CRUD 变更立即落 sqlite，落库成功后同步回写 registry（`Add` /
   `Update` / `Remove`）；录制守护进程的监督循环订阅变更通知实时调和，
   **无需重启**。
2. **监控跟随房间存在**：新建房间无论 record_enabled 与否立即开始监控（弹幕
   WS + 兜底轮询），删除房间立即停止监控——若删除时正在录制，先优雅
   停止会话（关 FLV、刷弹幕、finalize meta、收尾合并跑完），再删房间
   记录，已录制的文件保留在磁盘上。
3. **record_enabled 只决定是否录制**：关闭正在录制房间的录制立即优雅停止
   会话（正在合并的收尾不中断，跑完为止），监控保留；开启录制时若在播
   则立即开录。录制开关翻转与会话收尾竞态时，收尾完成后仍在播即恢复录制。
4. 平台刷新的主播名/房间标题会经 `ApplyRoomInfo` → `UpdateRoom` 覆盖
   写回 sqlite，重启不丢（写回失败只记 warn，内存仍更新，不影响录制）；
  名称和标题以平台返回值为准，用户侧不提供手动编辑入口。

启动后创建的房间 Get/List 照常可查，且立即被监控；反复启停房间只影响
录制会话，不断开弹幕连接（监控协程不随 record_enabled 翻转重启）。

---

## 9. 失败处理与边缘情况

| 场景 | 行为 |
|---|---|
| 断流（仍在播） | 决策树重连，新 part（§4.5）；预算耗尽则保内容收尾 |
| 正常 EOF 但仍在播 | 视同断流重连（CDN 掐长连接是常态） |
| 文件/tag 停止增长 | 巡检连续 3 轮无增长 → 中止 → 决策树普通重连分支 |
| 风控 -352/412/403/429 | 刷 WBI+buvid 重试一次 → getDanmuInfo 再降级 getConf → 失败则房间冷却 5/10/20min（§5） |
| 进程崩溃/重启 | FLV 保留至最后完整 tag；重启后 WS 重连重查房态，在播则续录，part 目录扫描续号，meta 原子写无半更新；RecoverPending 补合并 |
| 优雅停机（SIGTERM） | §3.4：FLV 已有效 → meta 标 merging（30s grace）→ 合并遗留下次补跑；Stop 等待上限 45s |
| 磁盘写失败 | 中止泵送，保留已写文件，meta 记 errors；重连大概率再失败，耗尽预算后房间状态 ERROR，下次开播自然恢复（无重试风暴） |
| 合并失败（分段损坏/缺失） | 不删源分段，meta 记 merge 错误、置 partial，下次启动经 RecoverPending 重试 |
| cookie 过期 | 拉流降档（qn 自动降档 + meta 记录 + warn 日志）；运维动作：Web 重新扫码登录（热替换即时生效，§7.3） |
| WS 假死（半开连接） | 90s 读超时强制重连；兜底轮询（600s±10%）保底发现开播 |
| 多主播同时开播 | 并行录制；`max_concurrent` 达上限时新开播排队等待（记日志） |
| recorder 配置缺失/rooms 表为空 | NewData/NewRecorderRepo/NewRecorderUsecase 均容忍 nil recorder conf；rooms 表空 → Run 记 warn 空转但对后续变更保持响应；经 CRUD 加房后立即开始监控（§8.1） |
| data.database.source 缺失或非法 | NewData 启动失败：source 非空且通过路径校验（§7.1）；sqlite 打不开同样启动失败 |
| 录制中关闭房间的录制 | 优雅停止会话：关 FLV、刷弹幕、finalize meta、合并若开启则跑完（30s grace），监控保留；再开启录制时若在播立即恢复录制（§8.1） |
| 录制中删除房间 | 先优雅停止会话，再停止监控、删除房间记录；已录制文件保留，迟到的注册表状态写入自动忽略 |
| 无活动场次时弹幕到达 | Events 缓冲（4096）满即丢弃，不阻塞 WS 读循环 |
| watchRoom 收到重复"在播" | 幂等：已有活动场次则忽略 |
| 场次中途改标题/轮次 | 目录与文件名沿用开播快照，不重命名；ROUND/ROOM_CHANGE 仅刷新房态 |
| 平台刷新覆盖已有身份 | ApplyRoomInfo 覆盖语义：平台非空值优先，经 UpdateRoom 写回（§8） |
| 新场次字节计数 | PrepareSession 清零在途 stats，新场次进度不叠加旧场次（重启续录不受影响，stats 本是内存态） |

---

## 10. 测试

测试与被测代码同包同目录（`*_test.go`），分层隔离（CLAUDE.md 纪律），
共 157 个测试函数。运行：`go test -mod=mod ./...`（本仓库一律 `-mod=mod`）。

| 层 | 文件 | fake 什么 / 测什么 |
|---|---|---|
| biz | `recorder_test.go`（20） | repo + LiveClient 全脚本化 fake（队列式返回、末条粘滞）；决策树各分支：下播停录、在播重连、预算耗尽保内容、auto_reconnect=false、CDN 瞬态独立预算、OpenLiveStream/复查失败终止、拉流瞬时失败复查已下播静默收尾（不记错误）/仍在播按预算重试/复查失败终止、复查因 ctx 取消失败静默收尾（不记错误）、ctx 取消即停、nil/覆盖配置、抖动区间；watchRoom 收到"未开播"房态更新取消活动场次；**record_enabled 门控（关闭录制只监控不录制、开启立即开录）、停止中再开启录制收尾后续录、Run 监督循环对注册表增删的实时 reconcile**；`cdnBackoffBase`/`redialDelay` 字段供测试压缩时延 |
| biz | `room_test.go`（10） | fakeRoomRepo 脚本化：NewRoomRegistry 全量加载（room_id 序）、nil repo 空 registry、加载失败即启动错误；**registry Add/Update/Remove 实时同步与合并式变更通知（含退订）**、**RoomUsecase CRUD 落库后同步 registry（持久化失败不回写）**；ApplyRoomInfo 覆盖主播名/标题并经 UpdateRoom 写回（二次上报再覆盖）、写回失败只降级内存仍更新；fakeStatsRepo；ListRoomRuntimes 合并状态与 stats；RoomUsecase 参数校验与 repo 错误透传 |
| biz | `session_policy_test.go`（4） | 决策矩阵逐行覆盖（`.scratch/session-policy/spec.md`）：RoomInfoArrived / RecordEnabledFlipped / SessionFinished 三种输入 × 阶段（idle / running / finishing）转移，收尾后续录（resumeOnFinish）语义（ADR-0001） |
| biz | `account_test.go`（5） | fake PassportClient + CredentialRepo 脚本化：轮询确认才持久化凭据、未确认状态不落库、参数校验、账号状态（无凭据=已登出）、本地登出 |
| service | `room_test.go`（7） | 真 sqlite 端到端：`t.TempDir()` 临时 db 文件 + `data.NewData`（MergeEnabled=false 关闭收尾合并），按 wireApp 同款链路搭 roomEnv；CRUD 全流程（建/取/删、时间戳回填、响应运行时字段默认值）、分页翻页、optional 查询字段、运行时状态合并、校验（0/负 room_id、重复创建 409、坏 page_token）、**平台刷新回填 streamer_name**（重建第二套 env 模拟重启验证 registry 重载）；convertRoomReply 枚举映射 |
| service | `account_test.go`（5） | 真 sqlite 端到端：QR 登录创建/轮询全流程、凭据跨重启持久化、空 qrcode_key 校验、过期凭据的状态行为、平台错误传播（503） |
| data | `recorder_test.go`（28） | `t.TempDir()` 真文件系统：meta 往返/缺失/损坏 JSON、标题清洗、part 续号、切段判定、配置映射、路径推导、重启续录保段/更新标题变体、**场次间 stats 清零**、新段头注入且不重复写（单段/切段各一）、弹幕事件落盘、nil 流拒绝、单段/切段全流程、收尾合并（无 meta noop / 禁用合并保分段 / 单段产物与源删除 / 多段边界时间戳平移与单调 / 失败保留源与临时文件清理 / 缺源标 partial）、RecoverPending（中断补跑 / 旧状态跳过 / partial 源齐重试与源缺保留） |
| data | `data_test.go`（4） | sqlite source 路径校验（file: 前缀容忍/查询参数拒绝）、父目录自动创建、既有 db 文件上 AutoMigrate rooms 表 |
| data | `credential_test.go`（5） | 空库读取、单例行 upsert、Save/Delete 热替换 `Data.Cookie`、删除幂等、并发读 Cookie 安全 |
| data/bili | `live_test.go`（8） | pickFLVStream 纯函数：avc 优先 / 同优先级首个 / 过滤非 FLV 与空 URL、授予清晰度三级来源（选中 codec `current_qn` → playurl `current_qn` → 未知）、g_qn_desc 描述、接受降档 |
| data/bili | `danmaku_test.go`（24） | 包编解码往返、zlib/brotli 嵌套解包、事件解析（弹幕/礼物/SC/上舰/进场）、认证包 uid 跟随 cookie（登录/匿名） |
| data/bili | `risk_test.go`（16） | riskGuard：成功清冷却、冷却闸门拦截、HTTP 风控与 -352 刷新重试一次/耗尽、fallback 成功/失败/非零码、非零码不记账、阶梯冷却升级、并发安全 |
| data/bili | `wbi_test.go`（5） | mixin_key 已知向量/短输入/32 截断、签名值 sanitize、URL 提取密钥 |
| data/bili | `buvid_test.go`（5） | 注入替换语义（替换已有/空串追加/跳过空值/修剪空白）、cookie 取值 |
| data/bili | `passport_test.go`（7） | assembleLoginCookie 拼装、轮询状态码映射、QR 创建（成功/平台错误）、轮询确认捕获 Set-Cookie / 各 pending 态、账号信息核验 |
| data/flv | `flv_test.go`（4） | 构造字节流 fixture：头往返、坏签名、tag 流（含扩展字节时间戳）、截断 |

---

## 11. 依赖与运行要求

Go 依赖（go.mod 直接依赖）：`github.com/gorilla/websocket` v1.5.3
（弹幕 WS）、`github.com/andybalholm/brotli` v1.2.2（protover3 解压）、
`gorm.io/gorm` v1.31.2 + `gorm.io/driver/sqlite` v1.6.0（房间持久化；
driver 底层是 `github.com/mattn/go-sqlite3`，**需 cgo**，go.mod 中为
直接依赖）、`go.einride.tech/aip` v0.86.3（CRUD 的 pagination /
fieldmask / fieldbehavior）、`go.uber.org/automaxprocs` v1.6.0、
`google.golang.org/{protobuf,grpc}` 与 genproto googleapis/api；框架为
`github.com/go-kratos/kratos/v3`（另有 contrib/otel 用于日志 trace
属性提取）。Go 1.25.x。

前端依赖（web/，与 Go 构建无关）：React 19 / react-dom、Ant Design 6
（antd + @ant-design/icons）、Vite 8、TypeScript 6；`npm install` +
`npm run dev` 起开发服务器（API 经 vite 代理转发，见 §8）。

运行时会在工作目录按 `data.database.source` 打开 sqlite 文件
（默认 `./data/suika.db`）：路径校验 + 父目录自动创建，AutoMigrate
rooms / credentials 表，单连接访问。`/data/` 已入 .gitignore，db 文件不进仓库。

外部二进制：无。录制与收尾合并全部为纯 Go 实现，不依赖任何外部工具。

本地运行：

```bash
make init                                  # 首次安装 wire/buf
go run ./cmd/suika -conf ./configs         # HTTP :8000 / gRPC :9000，
                                           # 首次运行自动建 ./data/suika.db
# 登录态：cd web && npm install && npm run dev 起前端，
# 顶栏"登录"用哔哩哔哩 App 扫码（凭据进 credentials 表即时生效，§7.3）
curl -X POST localhost:8000/v1/rooms/list -d '{}'              # 冒烟检查
curl -X POST localhost:8000/v1/rooms/create \
     -d '{"room":{"room_id":123456,"record_enabled":true}}'    # 加房间
# 房间立即开始监控；record_enabled 决定是否录制（§8.1）
```

---

## 12. 为第二阶段（切片）预留的扩展点

第一阶段刻意落盘、当前即可被切片消费的材料：

- 文件名约定 `{日期}_{时间}_{标题}` + meta.json 的 title/room_name/
  live_start_time → 素材身份脚本可读（收尾不再写容器元数据，见 §4.6）；
- meta.json 的 `wall_*` / `ts_*` 双时间轴 → 弹幕↔视频任意精度对齐；
- 全事件 JSONL → SC/礼物/上舰是高光定位的最强信号，弹幕密度切片与
  事件驱动切片都可直接消费；
- part 化的目录结构 → 切片素材溯源与"已使用"标记天然有落点；
- 数据库已经就位（sqlite + gorm，rooms / credentials 表）；素材使用记录等
  第二阶段数据直接在同一个 data 层加表
  （AutoMigrate 已在 NewData）。

---

## 附录 A：移植来源映射

| 本服务组件 | 来源仓库 | 原始位置 | 移植方式 |
|---|---|---|---|
| WBI 签名 | hikami-go | `internal/biliutil/wbi.go` | Go 直接移植（data/bili/wbi.go） |
| buvid 指纹 | hikami-go | `internal/biliutil/buvid.go` | Go 直接移植（data/bili/buvid.go） |
| 开播检查/拉流/URL 拼装/候选排序 | hikami-go | `internal/live_record/bilibili.go` | Go 移植（data/bili/live.go） |
| 弹幕 WS 协议（包头/认证/心跳/brotli） | hikami-go | `internal/live_record/danmaku.go` | 移植 + 扩展事件类型（data/bili/danmaku.go） |
| 断流决策树/预算/巡检 | hikami-go | `internal/live_record/manager.go` | 参考重写，决策移入 biz（biz/recorder.go） |
| 风控阶梯冷却 | hikami-go | `internal/live_record/manager.go` | Go 直接移植（data/bili/live.go） |
| FLV tag 切段/头注入 | blrec | `blrec/flv/*`、`blrec/core/operators/*` | Go 重写（data/flv、data/recorder*.go） |
| LIVE/PREPARING 事件驱动检测 | blrec | `blrec/bili/live_monitor.py` | Go 重写（biz + data/bili/danmaku.go） |

hikami-go：Go 单机服务，录直播音频+弹幕 → ASR → AI 总结（刻意不保存
视频）；blrec（bilive 内置录制内核）：纯 Python FLV 下载器。两者录制
内核都不能直接复用，但组合起来覆盖本服务全部需求。

## 附录 B：B 站接口速查

| 接口 | 用途 | 代码位置 |
|---|---|---|
| `GET api.live.bilibili.com/xlive/web-room/v1/index/getInfoByRoom?room_id=` | 房间/开播状态、标题、live_start_time、主播名 | bili/live.go roomStatus |
| `GET api.live.bilibili.com/xlive/web-room/v2/index/getRoomPlayInfo?room_id=&protocol=0,1&format=0,1,2&codec=0,1&qn=&platform=web` | 流地址（候选排序取 FLV+avc 优先） | bili/live.go selectStreamURL |
| `GET api.live.bilibili.com/xlive/web-room/v1/index/getDanmuInfo?id=&type=0` | 弹幕 token + 接入节点（WBI 签名） | bili/danmaku.go danmuInfo |
| `GET api.live.bilibili.com/room/v1/Danmu/getConf?room_id=&platform=pc&player=web` | 弹幕 token 降级通道（无 WBI） | bili/danmaku.go danmuConf |
| `GET api.bilibili.com/x/web-interface/nav` | WBI 密钥（兼判断登录态） | bili/wbi.go fetchKeys |
| `GET api.bilibili.com/x/frontend/finger/spi` | buvid3/buvid4 | bili/buvid.go getBuvids |
| `wss://<host>:<wss_port>/sub`（保底 `broadcastlv.chat.bilibili.com:2245`） | 弹幕事件流：16 字节头二进制包，op2 心跳 / op5 消息 / op7 认证 / op8 认证回复；protover 3=brotli、2=zlib | bili/danmaku.go |

清晰度档位：20000=4K、10000=原画、400=蓝光、250=超清、150=高清、80=流畅。
