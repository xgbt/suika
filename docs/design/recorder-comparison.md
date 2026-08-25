# 录制流程横向对比：录播姬 / biliup / blrec(bilive) / DDTV vs 本服务

**性质**：调研与建议文档，不是已拍板的决策。2026-08-25 基于工作区
`/Users/xgbt/bzhan` 内四份上游仓库检出（BililiveRecorder、biliup、bilive、
DDTV）的逐文件阅读整理；文中所有 suika 现状均与代码核对过（引用到文件）。
从本文采纳的任何一项改动一旦做出架构决策，按惯例记入 `docs/adr/`。

术语遵循 `CONTEXT.md`（Room / Session / Monitor / Fallback poll /
Session policy / Reconcile）；suika 录制链路的完整细节见
`docs/design/bili-recorder.md`，本文不重复。

---

## 1. 五项目定位

| 项目 | 录制内核 | 开播检测 | 最大特色 | 最大短板 |
|---|---|---|---|---|
| **suika** | 纯 Go FLV tag 泵（`internal/data/flv` + `recorder*.go`） | 弹幕 WS 主通道 + 600s±10% Fallback poll | 决策/IO 分层（五条缝）、sessionPolicy 决策矩阵、风控编排、原子合并 + `RecoverPending` 补跑 | 流内时间戳不修、无重复数据防护、无磁盘保护 |
| **BililiveRecorder（录播姬）** | 纯 C# FLV 中间件管线（`BililiveRecorder.Flv`） | 弹幕 WS 主通道 + 180s 轮询兜底 | FLV 修复规则链、边录边回写 keyframes 索引、Polly 风控熔断 | 无会话恢复（靠"文件永远完整"兜底）、无全局并发上限 |
| **biliup** | 纯 Rust nom 解析（`crates/biliup/src/downloader/`） | 30s HTTP 轮询（无 WS） | 关键帧对齐写盘、弹幕滚动与视频段同名配对、上传流水线钩子 | 检测慢（纯轮询）、同步写阻塞 tokio、无磁盘防护 |
| **bilive（内核为锁定版 blrec 2.0.0b4）** | 纯 Python RxPY 管线（wheel 内 `blrec/`） | 弹幕 WS 主通道 + 600±60s 轮询 | 时间戳修复链、弹幕用 B 站服务器时间对齐 + 断流钳制 | 崩溃遗留 `.flv` 无人善后（可永久卡死其 merge 模式）、无 -352 处理 |
| **DDTV** | C# 自研下载，HLS 优先（`Core/Download/`） | 10s 轮询 + 批量接口 1500 房/请求 | `ConfirmStopLive` 三连确认（与本服务 `probeLive` 同源）、init segment 签名比对 | 崩溃恢复几乎为零、弹幕纯内存、无并发上限、风控只防不治 |

参考项目与本服务的移植关系见 `bili-recorder.md` 附录 A（WBI/buvid/
拉流/风控来自 hikami-go，FLV 切段与 LIVE/PREPARING 事件驱动检测参考
blrec 重写）。

## 2. 逐维度对比

✔ 已有 / ▲ 部分 / ✘ 缺失

| 维度 | suika | 录播姬 | biliup | bilive | DDTV |
|---|---|---|---|---|---|
| WS 推送检测开播 | ✔ | ✔ | ✘ | ✔ | ✘（批量轮询） |
| 下播多次确认防误杀 | ✔ 连续 3 轮（`probeLive`） | ✘（靠 PREPARING 事件） | ✘ | ✘ | ✔ 连续 3 次（`ConfirmStopLive`） |
| 原画请求 + 接受降档 | ✔（降档记 meta + warn） | ✔（codec+qn 有序偏好列表） | ✔ | ✘（刻意 250，无 cookie 策略） | ✔（不校验授予值） |
| 排除 P2P CDN（`.mcdn.`） | ✔（排除后空则回退全量） | ✔ | ✘ | ✔（偏好序排最后） | ✘ |
| 编码选择 | ▲ AVC 优先但接受 HEVC | ✔ avc/hevc 偏好列表 | ✔ 免登录源仅保留 avc | ✘ 仅 AVC，HEVC 解析失败 | ✔ 硬编码 avc |
| 关键帧对齐切段 | ▲ 首段在任意首个 tag 开段 | ✔ 文件惰性打开在关键帧组边界 | ✔ 非关键帧驻内存等关键帧 | ✔ `Limiter` 只在关键帧切 | ✘（HLS 天然分片） |
| 时间 + 大小双触发分段 | ✔ 120min / 2.5GiB + 裕度强切 | ✔ ByTime / BySize 二选一 | ✔ 默认仅大小 2500MB | ✔ 仅时长 30min | ✔ |
| 序列头变化强制切段 | ✔（`headerChanged`） | ✔（`HandleNewHeaderRule`） | ✔ | ✘ | ✔（init segment 签名，只比影响拼接的字段） |
| **流内时间戳跳变修复** | ✘（仅 `merge.go` 边界平移） | ✔ ±500ms 阈值 + 帧间隔外推 | ✘（仅告警） | ✔ `fix`/`correct` 链 | ✘ |
| **CDN 重复数据去重** | ✘ | ✔ FarmHash 组指纹，16 组历史，连续 10 组断开 | ✘ | ✘ | ✘ |
| 无数据健康检查 | ✔ 10s×3 轮 | ✔ 10s 网络字节看门狗 | ✔ 30s 读超时 | ✔ 3s 读超时 | ✔ clamp(3×TARGETDURATION, 60, 300)s |
| 断流重连预算 + 稳定重置 | ✔ 稳定录 ≥5min 重置预算 | ✔ 无限重连，仅磁盘满/房间加密放弃 | ✔ 探测仍在播即重置计数 | ✔ 小间隔无限重试 + 连通性探测 | ✔ 无上限重连 + 下播确认收口 |
| 风控 -352/412 处理 | ✔ 最全：刷新重试一次 + 按房冷却阶梯 5/10/20min + getConf 兜底 | ▲ 412 全局熔断 2min + 80% 失败熔断 + 5 并发隔板，无 -352 | ▲ 弹幕 -352 降级匿名端点，412 无专项 | ▲ HTML 页面抓取兜底，无 -352 处理 | ✘ 只有预防（节奏控制），无处置 |
| 弹幕事件类型 | 弹/礼/SC/舰/进场 | 弹/礼/SC/舰（各类开关） | 弹/礼/SC/舰（detail 开关） | 弹/礼/SC/舰/toast + 原始 JSONL 双写 | 弹/礼/SC/舰/toast + **SEND_GIFT_V2 protobuf 解码** |
| 崩溃恢复 | ✔ `RecoverPending` 补合并 + part 续号 + meta 原子写 | ✔ 每 GOP 回写 metadata，文件任意时刻完整 | ▲ `.part` 转正，无孤儿清理 | ✘ 遗留 `.flv` 无 remux，卡死下游 | ✘ 半成品永不修复 |
| 磁盘空间防护 | ✘ | ▲ 识别磁盘满后本次停重试 | ✘ | ▲ 10GiB 阈值告警 | ✘ |
| 并发录制上限 | ✔ 槽位 + 排队 | ✘ | ✔ 信号量（默认 5） | ✘ | ✘ |
| 优雅停机 | ✔ 45s 等待 + 30s finish grace | ▲ | ▲ | ✘（kill -9） | ✘ |

## 3. 结论

suika 的骨架——开播检测（WS 事件驱动 + 兜底轮询 + 下播多轮确认）、
会话启停（电平触发策略 + 决策矩阵测试）、风控编排、崩溃恢复、并发与
停机——在五个项目里属于最完整的一档，没有架构级缺口。差距集中在
**录制泵内部的字节级防御**：时间戳修复、重复数据防护、首段关键帧
对齐、磁盘保护。这恰好是录播姬十年打磨沉淀的部分，其中时间戳修复
在录播姬和 blrec 两个独立实现里都有，说明是 B 站 CDN 的高频问题。

## 4. 建议改进项

### 4.1 高价值（真实缺口，多来源佐证）

#### (1) 流内时间戳跳变修复

- **来源**：录播姬 `Flv/Pipeline/UpdateTimestampJumpRule.cs`、blrec
  `flv/operators/fix.py` + `correct.py`。
- **问题**：B 站 CDN 断流/换源时流内时间戳跳变常见。suika 透传 tag，
  只在 `internal/data/merge.go` 做段间边界平移，段内跳变原样落盘：
  播放器告警、seek 异常，第二阶段弹幕烧录对齐受影响。
- **做法**：在 `data` 层录制泵（`recorder.go` 的 `RecordSession`）维护
  期望时间戳——上一 tag 时间戳 + 帧间隔估算（录播姬取前 2 帧间隔，
  视频合法域 15–50ms 兜底 33ms、音频 20–24ms 兜底 22ms，两者取大）；
  偏差超 ±500ms 即累计一个会话级 `offset` 施加到后续所有 tag。
- **注意**：本服务分段不重置时间戳（跨段绝对毫秒），修复状态必须与
  `headerCache` 一样跨分段存活、在会话级维护；偏移变化点可记入
  `meta.json`（即 blrec 的 joinpoint 概念），供第二阶段消费。
  改动只落在 data 层，biz 保持纯决策。

#### (2) CDN 重复数据指纹去重

- **来源**：录播姬 `Flv/Pipeline/RemoveDuplicatedChunkRule.cs`（独家，
  但防的是真实故障模式）。
- **问题**：CDN 边缘节点循环吐同一段数据时字节持续增长，现有健康
  检查（`defaultHealthInterval` 只看"有没有增长"）完全发现不了，
  会一直录重复内容。
- **做法**：以视频关键帧为界分块，对每块内容算指纹（`hash/maphash`
  或 fnv 即可，不必 FarmHash），与最近 ~16 个指纹比对，命中丢弃整块；
  连续命中超阈值（录播姬用 10）→ 返回 `biz.ErrStreamTransient`，
  让断流决策树重选流地址（等于换 CDN 节点）。

#### (3) 新段等待首个视频关键帧再开文件

- **来源**：biliup `downloader/httpflv.rs`（非关键帧 tag 驻内存缓存，
  关键帧才落盘）、录播姬 `Flv/Writer/FlvProcessingContextWriter.cs`
  （文件惰性打开，首个数据组即关键帧组）。
- **问题**：`RecordSession` 中 `seg == nil → openNewSegment()` 在任意
  首个 tag 就开段。新会话与每次重连后的段首都可能不是关键帧，开头
  数帧不可解码，播放器跳到首个关键帧才能出画面。
- **做法**：开段前丢弃（或暂存）首个视频关键帧之前的 tag；音频流
  （无视频关键帧）豁免，直接开段。十几行改动，可播放性收益直接。

#### (4) 磁盘容量防护 + ENOSPC 全局熔断

- **来源**：bilive `SpaceMonitor`（10GiB 阈值告警）、录播姬（磁盘满是
  仅有的两种"彻底放弃"之一，`IOException.HResult 0x80070070`）。
- **问题**：目前磁盘写失败走普通重连 → 预算耗尽 → 房间
  `RECORD_STATUS_ERROR`（见 `bili-recorder.md` §9）。磁盘满对所有房间
  同时成立，各房间会各自重连、各写各的垃圾尾段，错误日志风暴。
- **做法**：(a) 开新段前 `syscall.Statfs` 查剩余空间，低于阈值直接
  `FailRecording`，`last_error` 写明磁盘不足；(b) 写入错误识别
  `ENOSPC`，包装为与 `ErrRiskControl` 同级的哨兵错误——断流决策树对
  这类错误不重连，避免全盘无意义的重试风暴。

### 4.2 中价值（单来源参考或边缘增强）

#### (5) 412 全局熔断

- **来源**：录播姬 `PollyPolicy.cs`（1 次 412 → 全体 API 静默 2 分钟
  再半开；30s 窗口失败率 >80% 熔断 1 分钟；全房间共享 5 并发隔板）。
- **问题**：`riskGuard` 的冷却阶梯是按房间的；IP 级 412 是全局事件，
  现状是每个房间各自撞一遍、各自进冷却。
- **做法**：`riskGuard` 增加全局级闸门（任一脚 412 → 全局静默 ~2 分钟
  再半开），与按房间阶梯并存。改动集中在 `internal/data/bili/risk.go`。

#### (6) 预算耗尽但房间仍在播时的恢复路径

- **来源**：biliup `server/common/download.rs`（每次断流重新
  `check_stream`，仍在播 → 重试计数清零立即重连）、录播姬
  （`Streaming == true` 即零延迟重走完整取流）。
- **问题**：现状核对过 `sessionPolicy`：预算耗尽属"自然结束"，
  `SessionFinished` 不凭陈旧在播信息重启，要等新的世界状态——房间
  持续在播时弹幕 WS 不推新状态，实际要等 ~600s Fallback poll 才重新
  开录。对"编码器频繁抖但房间健康"的主播是最长 10 分钟录制空窗。
- **做法**（二选一，都不引入无限重试，保住 §9"无重试风暴"原则）：
  a. 会话自然结束后安排一次延迟的主动回退轮询（如 60s 后），给策略
  一次新的世界状态输入；
  b. `recordLoop` 预算耗尽分支改为进入慢速重试（固定 60–120s 间隔，
  慢速重试次数另设上限）。
- **注意**：这是 sessionPolicy 语义变更，按惯例走决策矩阵更新 +
  逐行测试 + 新 ADR。

#### (7) HEVC / enhanced RTMP 的明确取舍

- **来源**：四个参考项目全部只录 AVC——biliup 免登录源只保留
  CODECS 含 avc 的变体，DDTV 硬编码 `codec_name == "avc"`，blrec
  文档明记 HEVC 流解析失败。这不是巧合。
- **问题**：`pickFLVStream` 现在 AVC 优先但接受 HEVC，而
  `flv.IsVideoKeyframe` / `IsAVCSequenceHeader` 按 AVC 布局判定：
  legacy-FLV HEVC 碰巧兼容；B 站在铺的 enhanced-RTMP（FourCC `hvc1`）
  布局下关键帧判定恒为 false——切段退化为超限强切，头注入失效。
- **做法**：`bestFLVStream` 排除非 avc codec（一行级改动，消除整类
  不确定性）。真要支持 HEVC 时再补 FourCC 解析，届时单独立项。
- **状态**：已采纳（2026-08-25，ADR-0004）——`bestFLVStream` 只保留
  `avc` codec，`getRoomPlayInfo` 请求参数同步收窄为 `codec=0`；
  无 AVC 候选时报无候选错误，决策树按非瞬时错误结束场次。

#### (8) `SEND_GIFT_V2` / `USER_TOAST` 预案

- **来源**：DDTV `Core/LiveChat/LiveChatListener.cs`（protobuf
  `SendGiftV2Parser.DecodeToLegacyGiftData` 映射回旧版结构）。
- **问题**：`danmaku.go` 的 `dispatch` 只认 `SEND_GIFT`；B 站部分流量
  已是 `SEND_GIFT_V2`（protobuf 载荷），出现后礼物静默丢失。
- **做法**：短期对未知 cmd 计数打日志（观测迁移进度）；观测到后参照
  DDTV 实现解码，映射进现有 `DanmakuEvent`。`USER_TOAST_MSG` 同理
  （bilive、DDTV 都收）。

### 4.3 低价值 / 不建议引入

| 项 | 来源 | 不做的理由 |
|---|---|---|
| 边录边回写 onMetaData + keyframes 索引（6300 槽位占位） | 录播姬 | 其价值是"无后处理也永远可播"；本服务有收尾合并 + `RecoverPending`，分段只是中间产物，复杂度收益不划算 |
| 弹幕 XML 兼容格式 | 录播姬 / biliup / DDTV | JSONL + `wall_*`/`ts_*` 双时间轴是刻意设计（§12 第二阶段切片素材），比录制期近似对齐更准 |
| HLS/fmp4 备选协议 | biliup / DDTV | fmp4 是无 moov 碎片，DDTV 还得挂 ffmpeg 修复一遍；录制场景 FLV 足够 |
| 模板化文件命名 | 录播姬 Fluid / biliup strftime | 固定布局更简单且机器可读 |
| 内嵌 ffmpeg / MP4 输出 | — | 已有定论：ffmpeg 内嵌路线已否决（`.scratch/ffmpeg-static-builds/research.md`），纯 Go remux 留痕待办（`.scratch/pure-go-mp4/issues/01-flv-to-mp4-remux.md`） |
| 批量开播检测接口（1500 房/请求） | DDTV | 本服务每房常驻一条弹幕 WS，规模化拐点在连接数而非检测请求；房间量级到达前不值得换模型 |
| 封面保存、20MB 碎段过滤 | 录播姬 / biliup | 低成本可选，不影响核心，不优先 |

## 5. 值得保持的现状（对比确认的优势）

- **风控五家最全**：-352 刷新重试、HTTP 412/403/429 分类、按房冷却
  阶梯、弹幕 token 兜底旧接口（`risk.go`）；WBI key 缓存 1h、buvid
  缓存 24h（`wbi.go`/`buvid.go`）不比 biliup 的 2h TTL 差。DDTV/bilive
  基本无处置，录播姬无 -352 处理。
- **崩溃恢复是五家里最完整的主动方案**：blrec 的遗留 `.flv` 会永久
  卡死它的 merge 模式，DDTV 完全不管，录播姬靠"文件永远完整"绕开；
  本服务 `RecoverPending` + meta.json 原子写是正面解决。
- **断流决策树带稳定重置预算**（`stableResetAfter`）、**下播三连确认**
  （仅 DDTV 有等价物）、**并发槽位**（录播姬/DDTV 无全局上限）、
  **优雅停机 45s/30s grace**，在参考项目里都是少数派或没有。
- **架构**：sessionPolicy 决策矩阵逐行有测试、五条缝的决策/IO 分工，
  比任何参考项目都干净——参考项目的业务逻辑普遍糊在录制循环里。

## 6. 建议执行顺序

按性价比：

1. **(7) 排除非 avc codec** — 小时级，消除整类不确定性（已完成，
   ADR-0004）；
2. **(3) 首段关键帧对齐** — 半天，可播放性直接收益；
3. **(4) 磁盘防护** — 一天，防全盘故障；
4. **(1) 时间戳修复** — 两三天 + 测试，高频问题；
5. **(2) 重复数据指纹** — 一天，防御 CDN 病态节点；
6. **(5)(6)(8)** 随后；其中 (6) 走决策矩阵 + ADR 流程。

(1)(2)(3)(4) 都落在 `data` 层，不影响现有分层；落地时各项单独成
`.scratch/<feature>/` issue。

## 附录：上游关键实现索引

| 主题 | 项目 | 位置 |
|---|---|---|
| 时间戳跳变修复 | 录播姬 | `BililiveRecorder.Flv/Pipeline/UpdateTimestampJumpRule.cs`（±500ms 阈值、帧间隔估算、新文件归零） |
| 时间戳修复 | blrec | wheel 内 `blrec/flv/operators/fix.py`（帧率外推）、`correct.py`（初始偏移归零） |
| 重复数据指纹 | 录播姬 | `BililiveRecorder.Flv/Pipeline/RemoveDuplicatedChunkRule.cs`（FarmHash64 组指纹，16 组历史，连续 10 组断开重连） |
| 关键帧对齐写盘 | biliup | `crates/biliup/src/downloader/httpflv.rs`（非关键帧驻缓存，seq header 变化切段） |
| 边录边索引 | 录播姬 | `BililiveRecorder.Flv/Writer/FlvProcessingContextWriter.cs` + `KeyframesScriptDataValue`（6300 槽位定长占位，每 GOP 原地回写） |
| 412 熔断 / 隔板 | 录播姬 | `BililiveRecorder.Core/PollyPolicy.cs` |
| 下播三连确认 | DDTV | `Core/Tools/Basics.cs` `ConfirmStopLive`（连续 3 次直查、失败不计数，与 `probeLive` 同源） |
| 批量开播检测 | DDTV | `Core/Network/Methods/room.cs` `get_status_info_by_uids`（1500/批）+ `Core/RuntimeObject/DetectRoom.cs`（10s 定时 + 重入闸门） |
| 弹幕服务器时间对齐 | blrec | wheel 内 `blrec/core/danmaku_dumper.py`（stime = 服务器时间差 + 断流钳制） |
| 连通性探测 | blrec | wheel 内 `blrec/core/operators/connection_error_handler.py`（HEAD 探测，600s 窗口，区分本机断网） |
| SEND_GIFT_V2 解码 | DDTV | `Core/LiveChat/LiveChatListener.cs` + `SendGiftV2Parser` |
| 磁盘空间告警 | blrec | wheel 内 `blrec/disk_space/`（`SpaceMonitor` 10GiB 阈值） |
| 重连仍在播即重置 | biliup | `crates/biliup-cli/src/server/common/download.rs`（`retry_count` 语义） |
