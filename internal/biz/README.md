# Biz

`internal/biz` 是领域逻辑层（DO + usecase + repo 接口），不依赖 DTO 或存储实现。

本文档梳理三个核心模块：

| 模块 | 文件 | 职责 |
|------|------|------|
| Account | `account.go` | B 站账号扫码登录、登录态核验、登出 |
| Room / Registry | `room.go`、`room_registry.go` | 房间 CRUD（落库）+ 内存运行时注册表 |
| Recorder | `recorder*.go` | 录制守护进程：监控开播、录制、断流重连、收尾合并 |

## 0. 总览：Room 与 Recorder 如何协作

```mermaid
flowchart LR
    API["管理后台 / API"] --> RU["RoomUsecase"]
    RU -->|"1. 落库"| REPO[("repo")]
    RU -->|"2. Add / Update / Remove<br/>+ notify"| REG["RoomRegistry<br/>(内存: 房间 + 运行时状态)"]
    REG -.->|"Subscribe 唤醒"| SUP["RecorderUsecase.Run<br/>reconcile"]
    SUP -->|"每房间一个 goroutine"| MON["runMonitor<br/>监控开播"]
    MON -->|"应该录制"| SES["runSession<br/>录制会话"]
    MON -->|"ApplyRoomInfo"| REG
    SES -->|"StartRecording / SetMerging / ..."| REG
    SES --> REPO
```

> Account 相对独立：登录凭据落库后，由 data 层热替换 `liveClient` 使用的 Cookie，无需重启录制。

---

## 1. Account 模块（`account.go`）

`CreateQRLogin` 直接调用 `client.CreateQRLogin`，返回二维码 URL / QRCodeKey / 过期时间；`Logout` 调用 `repo.DeleteCredential`（删库凭据 + 清内存登录态）。下面是两个有分支的流程。

### 1a. 轮询扫码结果 `PollQRLogin`

```mermaid
flowchart TD
    A(["PollQRLogin(qrcodeKey)"]) --> B{"qrcodeKey 为空?"}
    B -->|是| E1["ErrAccountInvalidArgument"]
    B -->|否| C["client.PollQRLogin"]
    C --> D{"Confirmed?"}
    D -->|"否"| R1["原样返回状态<br/>NotScanned / Scanned / Expired"]
    D -->|"是"| F{"cred.Cookie 非空?"}
    F -->|否| E2["ErrPassportUnavailable"]
    F -->|是| G["repo.SaveCredential<br/>持久化 + 热替换内存 Cookie"]
    G --> R2["返回 Confirmed"]
```

### 1b. 查询登录状态 `AccountStatus`

```mermaid
flowchart TD
    A(["AccountStatus"]) --> B["repo.GetCredential"]
    B -->|"ErrCredentialNotFound"| O1["AccountLoggedOut"]
    B -->|"其他错误"| E1["返回 err"]
    B -->|"成功"| C["client.AccountInfo(cred.Cookie)<br/>向平台核验"]
    C -->|"调用失败"| E2["返回 err<br/>平台不可达 ≠ 登出"]
    C -->|"成功"| D{"State == LoggedIn?"}
    D -->|是| O2["返回 UName / Mid"]
    D -->|否| O3["AccountExpired<br/>保留凭据，不删除"]
```

---

## 2. Room / Registry 模块（`room.go`、`room_registry.go`）

### 2a. 写路径：`CreateRoom` / `UpdateRoom` / `DeleteRoom`

三者流程一致：**校验 → 落库 → 更新内存 → 通知订阅者**。

```mermaid
flowchart LR
    A["Create / Update / Delete"] --> B{"参数校验"}
    B -->|失败| E1["ErrRoomInvalidArgument"]
    B -->|通过| C["repo 落库"]
    C -->|失败| E2["返回 err<br/>如 ErrRoomAlreadyExists"]
    C -->|成功| D["registry.Add / Update / Remove"]
    D --> N["notifyLocked<br/>合并式唤醒订阅者"]
    N -.->|"chan struct{}"| S(["Subscribe()<br/>→ Recorder reconcile"])
```

| 操作 | 校验 | registry 动作 |
|------|------|---------------|
| `CreateRoom` | `room != nil` 且 `RoomID > 0` | `Add`：登记到内存 |
| `UpdateRoom` | `room != nil` 且 `RoomID > 0` | `Update`：仅同步基础字段，**保留运行时状态** |
| `DeleteRoom` | `roomID > 0` | `Remove`：从内存移除 |

### 2b. 读路径：`GetRoom` / `ListRoomRuntimes`

```mermaid
flowchart LR
    A["GetRoom /<br/>ListRoomRuntimes"] --> B["repo 读取持久化 Room"]
    B --> C["registry.runtime(roomID)<br/>运行时快照"]
    C --> D{"RecordStatus<br/>== Recording?"}
    D -->|是| E["sessionStatsRepo.Stats<br/>补 CurrentFile / BytesWritten /<br/>DownloadSpeed"]
    D -->|否| F["跳过"]
    E --> G["合并为 RoomRuntime"]
    F --> G
```

运行时快照字段：`liveStatus` / `recordStatus` / `quality` / `lastError` 等。

### 2c. Recorder 对 Registry 的运行时写入

所有写入均持锁（`setState`），并影响 2b 读到的快照。

```mermaid
stateDiagram-v2
    [*] --> Idle
    Idle --> Recording: StartRecording<br/>重置 quality / lastError
    Recording --> Recording: SetStreamQuality<br/>记录授予清晰度
    Recording --> Merging: SetMerging
    Merging --> Idle: FinishRecording
```

| 方法 | 作用 |
|------|------|
| `ApplyRoomInfo` | 更新 `liveStatus` / `streamer_name` / `room_title`，并异步回写 `repo.UpdateRoom` |
| `FailRecording` / `NoteError` | 记录 `lastError` |

---

## 3. Recorder 模块

文件：`recorder.go`、`recorder_monitor.go`、`recorder_session.go`、`recorder_session_policy.go`。

按 **监督循环 → 单房间监控 → 录制会话/重连 → 下播确认** 四个粒度拆成四张图（3a–3d），层层下钻：

```
Run / reconcile (3a) ──每房间──▶ runMonitor (3b) ──开播且应录制──▶ runSession (3c) ──断流时──▶ probeLive (3d)
```

### 3a. 守护主循环与调和（`Run` / `reconcile`）

```mermaid
flowchart TD
    S(["Run(ctx)"]) --> A["repo.RecoverPending<br/>收尾上次遗留的合并（失败仅记日志）"]
    A --> B["registry.Subscribe()"]
    B --> C["reconcile（首次）<br/>monitors 为空则记 warn"]
    C --> D{"select 等待"}
    D -->|"wakeup（房间增删改）"| E["reconcile"]
    E --> D
    D -->|"ctx.Done"| F["cancel 所有 monitor<br/>等待 monitors 与 stopping 全部结束"]
```

`reconcile` 让「运行中的 monitor」与「registry 当前房间集合」保持一致：

```mermaid
flowchart TD
    A["回收 stopping 中已完成收尾的 monitor"] --> B["want = registry.Rooms()"]
    B --> C["已有 monitor 的房间不在 want 中<br/>→ cancel，移入 stopping"]
    C --> D{"遍历 want 中每个房间"}
    D -->|"无 monitor（新增）"| E["launchMonitor<br/>→ runMonitor（3b）"]
    D -->|"有 monitor，RecordEnabled 变化"| F["notifyRoomChange<br/>唤醒该 monitor"]
    D -->|"有 monitor，无变化"| G["无操作"]
```

### 3b. 单房间监控与会话策略（`runMonitor` / `runMonitorConnection`）

`runMonitor` 是外层循环：连接出错或断开后，等待 `monitorReconnectDelay` 重新调用 `runMonitorConnection`。

```mermaid
flowchart TD
    A["runMonitorConnection 初始化<br/>· 弹幕 WS（开播检测主通道）<br/>· 兜底轮询定时器（带抖动）<br/>· newSessionPolicy(recordEnabled)<br/>· probeRoomInfo 首次探测"] --> SEL{"select 等待事件"}

    SEL -->|"弹幕 RoomStateUpdates<br/>或 轮询定时器到期"| E1["roomInfoArrived<br/>registry.ApplyRoomInfo<br/>+ policy.OnRoomInfo"]
    SEL -->|"roomChange<br/>（后台改了 RecordEnabled）"| E2["重读 registry.Room<br/>policy.OnRecordEnabled"]
    SEL -->|"currSession.done"| E3["policy.OnSessionFinished"]
    SEL -->|"ctx.Done"| X["若有 currSession：<br/>cancel 并等待 done → 返回"]

    E1 --> ACT{"policy.decide"}
    E2 --> ACT
    E3 --> ACT

    ACT -->|"actionStart"| ST["launchSession（3c）<br/>记录 currSession"]
    ACT -->|"actionStop"| SP["currSession.cancel()<br/>异步收尾，等 done 再触发 OnSessionFinished"]
    ACT -->|"actionNone"| SEL
    ST --> SEL
    SP --> SEL
```

策略动作的触发条件：

| action | 条件 |
|--------|------|
| `actionStart` | 空闲（idle）且 `shouldRecord` |
| `actionStop` | 运行中（running）且 `!shouldRecord` |
| `actionNone` | 其余情况；收尾中（finishing）阶段不产生决策 |

补充：没有 `currSession` 时，弹幕事件（`Events()`）直接丢弃，仅 `RoomStateUpdates` 用于开播判定。

### 3c. 录制会话与断流重连（`runSession` / `runRecordingLoop`）

**会话生命周期**（无论录制循环如何结束，都会走完「合并收尾」）：

```mermaid
flowchart TD
    A(["runSession"]) --> B["registry.StartRecording"]
    B --> C["repo.PrepareSession<br/>建目录 + meta.json"]
    C -->|失败| F1["registry.FailRecording → 结束"]
    C -->|成功| D["runRecordingLoop<br/>（下图）"]
    D --> E["registry.SetMerging"]
    E --> G["repo.FinishSession<br/>脱离已取消的 ctx，finishGracePeriod 超时"]
    G -->|失败| F2["registry.FailRecording → 结束"]
    G -->|成功| H["registry.FinishRecording → Idle<br/>触发 currSession.done"]
```

**录制循环 `runRecordingLoop`**：拉流 → 录制 → 探活 → 判定是否重连。

```mermaid
flowchart TD
    OPEN["OpenLiveStream"] -->|"成功"| REC["SetStreamQuality<br/>RecordSession 写盘<br/>（阻塞至断流/结束）"]
    OPEN -->|"ErrStreamTransient<br/>如 CDN 404"| PROBE
    OPEN -->|"其他错误<br/>如风控 -352/412"| STOP
    OPEN -->|"ctx 已取消"| STOP

    REC --> CTX{"ctx 已取消?"}
    CTX -->|是| STOP
    CTX -->|否| STABLE{"本段稳定录制 ≥ stableResetAfter<br/>且写入了字节?"}
    STABLE -->|是| RESET["reconnects = 0<br/>cdnBudget 恢复满额"]
    STABLE -->|否| PROBE
    RESET --> PROBE

    PROBE{"probeLive（3d）"} -->|"探测失败 或 已下播"| STOP
    PROBE -->|"仍在播"| KIND{"断流类型"}

    KIND -->|"ErrStreamTransient<br/>（拉流失败 或 RecordSession 返回）"| CDN{"cdnBudget > 0?"}
    CDN -->|是| B1["cdnBudget--<br/>指数退避后重试"]
    CDN -->|否| STOP
    KIND -->|"其他（流正常结束/中断）"| AUTO{"AutoReconnect 且<br/>reconnects < MaxReconnect?"}
    AUTO -->|是| B2["reconnects++<br/>sleep ReconnectDelay"]
    AUTO -->|否| STOP

    B1 --> OPEN
    B2 --> OPEN
    STOP(["退出循环 → 进入合并收尾"])
```

退出循环的原因（均保留已录内容，并进入合并收尾）：

| 原因 | 说明 |
|------|------|
| ctx 取消 | 上层主动停止 |
| 非 transient 的拉流错误 | 如风控 -352/412，重试无法恢复，先 `NoteError` |
| 已下播 / 探测失败 | 见 3d |
| `cdnBudget` 耗尽 | CDN 类瞬时错误重试次数用完 |
| 重连预算耗尽 / 未开启 `AutoReconnect` | 普通断流的重连次数用完或不重连 |

两类预算相互独立：`cdnBudget` 针对 CDN 瞬时错误（指数退避），`reconnects` 针对普通断流（固定延迟）；稳定录制一段时间后两者都会重置。

### 3d. 下播确认子流程（`probeLive`）

连续多次确认「不在播」才算真下播，避免短暂抖动导致误结束。

```mermaid
flowchart TD
    A(["probeLive(ctx)"]) --> B["attempt = 0<br/>offlineStreak = 0"]
    B --> C["非首次：sleep offlineConfirmDelay<br/>（期间被取消 → false, false）"]
    C --> D["liveClient.GetRoomInfo"]
    D -->|"失败：ctx 已取消"| X1["false, false"]
    D -->|"失败：其他错误"| L["记录 lastErr<br/>（不计入下播确认）"]
    D -->|"成功"| E["registry.ApplyRoomInfo"]
    E --> F{"Live?"}
    F -->|是| R1["true, true<br/>立即返回：仍在播"]
    F -->|否| G["offlineStreak++"]
    G --> H{"≥ offlineConfirmRounds (3)?"}
    H -->|是| R2["false, true<br/>确认下播"]
    H -->|否| N
    L --> N{"++attempt <<br/>probeMaxAttempts (6)?"}
    N -->|是| C
    N -->|否| R3["NoteError(lastErr)<br/>false, false"]
```

返回值 `(live, ok)` 含义：

| 返回 | 含义 | 调用方处理 |
|------|------|-----------|
| `true, true` | 仍在播 | 继续重连判定 |
| `false, true` | 确认下播 | 正常结束会话 |
| `false, false` | 无法判断（被取消 / 多次失败） | 按正常收尾结束会话 |