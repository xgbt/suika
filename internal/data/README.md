# Data

data 层：实现 `biz` 声明的仓储与平台缝，持有 PO 与共享客户端。

| 位置 | 内容 |
|---|---|
| `data.go` | `Data`：共享 sqlite 句柄、`bili.Client`、录制器配置；Wire ProviderSet |
| `room.go` / `credential.go` | `RoomRepo` / `CredentialRepo` 实现（rooms / credentials 表） |
| `recorder*.go` / `remux.go` | `RecorderRepo` / `SessionStatsRepo` 实现：会话目录、分段、meta.json、转封装 |
| `bili/` | 所有与 B 站平台的交互：直播 API、弹幕 WS、WBI 签名、buvid 指纹、风控编排、扫码登录（`biz.LiveClient` / `biz.PassportClient` 的实现） |
| `flv/` | FLV tag 解析子包（切段点判定、头/标签读写） |

分层约束见根目录 `CLAUDE.md`；录制器细节见 `docs/design/bili-recorder.md`。
