package biz

import (
	"context"
	stderrors "errors"
	"io"
	"time"

	v1 "suika/api/room/v1"
	"suika/internal/conf"

	"github.com/go-kratos/kratos/v3/errors"
)

var (
	ErrRoomInternal = errors.InternalServer(v1.ErrorReason_ERROR_REASON_INTERNAL.String(), "recorder internal error")
)

var (
	// ErrStreamTransient 标记 CDN 侧的瞬时故障（HTTP 404、连接被重置等），值得重新选择流地址后重试。
	ErrStreamTransient = stderrors.New("recorder: transient stream error")
	// ErrRiskControl 标记 B 站风控拒绝（-352/412 等）。
	ErrRiskControl = stderrors.New("recorder: risk control triggered")
)

const (
	defaultRoomInfoPollInterval = 600 * time.Second // 拉取房间状态的兜底轮询间隔
	defaultMaxReconnect         = 3                 // 断流决策树最大重连次数
	defaultReconnectDelay       = 10 * time.Second  // 断流决策树重连延迟
	defaultCDNTransientBudget   = 5                 // CDN 瞬时故障的重试预算，超过预算则不再重连
	defaultCDNBackoffBase       = 2 * time.Second   // CDN 瞬时故障首次重试的延迟，随尝试次数指数增长
	defaultCDNBackoffMax        = 60 * time.Second  // CDN 瞬时故障的重试延迟上限
	monitorRedialDelay          = 10 * time.Second  // 弹幕连接重拨前的停顿
	finishGracePeriod           = 30 * time.Second  // 限定关停期间 FinishSession 脱离已取消运行 context 后仍可用的工作时长
	pollJitterFraction          = 5                 // 回退轮询间隔的相对抖动幅度（间隔 +/- fraction/2）
	offlineConfirmRounds        = 3                 // 判定下播所需的连续"未开播"探测次数
	probeMaxAttempts            = 6                 // 单次下播确认内的探测总次数上限（含失败）
	defaultOfflineConfirmDelay  = 3 * time.Second   // 下播确认相邻两次探测的间隔
	defaultStableResetAfter     = 5 * time.Minute   // 泵送稳定录制超过该时长后重置重连预算
)

// 写入 JSONL 的弹幕事件类型。
const (
	EventDanmaku     = "danmaku"
	EventGift        = "gift"
	EventSuperChat   = "superchat"
	EventGuard       = "guard"
	EventEntryEffect = "entry_effect"
)

// RoomInfo 是从 B 站获取的直播间元数据
type RoomInfo struct {
	RoomID        int64
	Live          bool
	Title         string
	StreamerName  string
	LiveStartTime time.Time
}

// StreamQuality 调用 B 站接口获取的授予直播流清晰度信息
type StreamQuality struct {
	Qn   int32
	Desc string
}

// LiveStream 是 LiveClient.OpenLiveStream 打开的一路直播流。
// 由 LiveClient 产生、被 RecorderRepo 消费。
type LiveStream struct {
	URL     string        // 流地址
	Quality StreamQuality // 清晰度信息
	Body    io.ReadCloser // 流读取器，调用方负责关闭
}

// DanmakuEvent 是一条过滤后的弹幕房间事件。各字段的相关性取决于
// Type；落盘的 JSON 形状由 RecorderRepo 决定。
type DanmakuEvent struct {
	Ts       time.Time // 接收时刻
	SendTs   int64     // 平台载荷中的发送时刻（unix 毫秒）；未知为 0
	Type     string
	UID      int64
	Uname    string
	Text     string // 弹幕文本 / SC 文本 / 进场特效文本
	Color    int32  // 弹幕颜色 / SC 颜色
	Mode     int32  // 弹幕模式 / SC 模式
	GiftName string // 礼物名称
	Num      int32  // 礼物/舰长数量
	Price    int64  // 礼物价格（金瓜子）/ SC 价格
	CoinType string // 礼物类型：gold/silver
	Duration int32  // SC 保留秒数
	Level    int32  // 舰长等级
	Raw      []byte // 原始 JSON Payload
}

// RecordingSession 是一次录制会话（同一房间的一次开播）。
type RecordingSession struct {
	RoomID        int64
	RoomName      string
	Title         string
	LiveStartTime time.Time
	Quality       StreamQuality
}

type sessionHandle struct {
	cancel context.CancelFunc
	done   chan struct{}
}

// RecordingResult 一次录制会话的最终结果
type RecordingResult struct {
	BytesWritten int64 // 总字节数
	Parts        int   // 分段数
}

// DanmakuConn 是一个房间的常驻弹幕 websocket，同时服务于开播检测
// （RoomStateUpdates）和弹幕录制（Events）。实现内部自行重连；每次
// 重连成功后重新探测并重新推送房间状态，以补上断连期间错过的
// LIVE/PREPARING 事件。Events 使用有界缓冲，无人消费时丢弃事件。
type DanmakuConn interface {
	// Events 返回一个只读通道，接收弹幕事件。若无人消费则丢弃事件。
	Events() <-chan *DanmakuEvent
	// RoomStateUpdates 返回一个只读通道，接收房间状态更新事件。若无人消费则丢弃事件。
	RoomStateUpdates() <-chan *RoomInfo
	// Close 关闭 websocket 连接。若已关闭则无操作。
	Close() error
}

// LiveClient 是平台直播流和弹幕的客户端接口，网络 IO
type LiveClient interface {
	// GetRoomInfo 获取房间的直播状态和元数据
	GetRoomInfo(ctx context.Context, roomID int64) (*RoomInfo, error)
	// OpenLiveStream 打开房间的直播流，返回一个 LiveStream。若房间未开播则返回错误。
	OpenLiveStream(ctx context.Context, roomID int64) (*LiveStream, error)
	// DanmakuConn 打开房间的弹幕 websocket 连接，返回一个 DanmakuConn。若房间不存在则返回错误。
	DanmakuConn(ctx context.Context, roomID int64) (DanmakuConn, error)
}

// RecorderRepo 是录制器的存储接口。负责磁盘 IO
type RecorderRepo interface {
	// PrepareSession 按"房间 + 开播时间" 创建/定位 目录和 meta.json。
	PrepareSession(ctx context.Context, session *RecordingSession) error
	// RecordSession 将直播流写入磁盘（按配置切分分段），并把事件写入对应的 JSONL 文件，直到流结束或 ctx 被取消。
	RecordSession(ctx context.Context, session *RecordingSession, stream *LiveStream, events <-chan *DanmakuEvent) (*RecordingResult, error)
	// FinishSession 收尾 meta.json 并合并已录分段。
	FinishSession(ctx context.Context, session *RecordingSession) error
	// RecoverPending 完成上次运行遗留的合并工作。
	RecoverPending(ctx context.Context) error
}

// ReconnectPolicy 是断流决策树使用的重连配置（展开后的扁平形式）。
type ReconnectPolicy struct {
	AutoReconnect      bool          // 是否自动重连
	MaxReconnect       int           // 最大重连次数
	ReconnectDelay     time.Duration // 重连延迟
	CDNTransientBudget int           // CDN 瞬时故障的重试预算，超过预算则不再重连
}

// RecorderUsecase 编排房间监控、会话生命周期和断流决策树。它只做
// 决策：所有平台 IO 由 LiveClient 执行，所有存储 IO 由 RecorderRepo
// 执行。房间配置与直播/录制状态存放在共享的 RoomRegistry 中。
type RecorderUsecase struct {
	registry            *RoomRegistry
	repo                RecorderRepo
	liveClient          LiveClient
	pollInterval        time.Duration   // 拉取房间状态的兜底轮询间隔
	rec                 ReconnectPolicy // 断流决策树使用的重连配置
	cdnBackoffBase      time.Duration   // CDN 瞬时故障首次重试的延迟；测试中会调小。
	redialDelay         time.Duration   // 监控重拨的停顿；测试中会调小。
	offlineConfirmDelay time.Duration   // 下播确认相邻两次探测的间隔；测试中会调小。
	stableResetAfter    time.Duration   // 泵送稳定录制超过该时长后重置重连预算；测试中会调小。
}

// recorderRepo 职责按文件拆分：recorder_supervisor.go 监督循环（Run/
// reconcile/monitorHandle），recorder_monitor.go 单房间监控分发
// （watchRoom），recorder_session.go 会话生命周期（launchSession/
// runSession），recorder_reconnect.go 断流决策树（recordLoop/
// probeLive）。session_policy.go 是会话启停策略状态机（ADR-0001）。
func NewRecorderUsecase(c *conf.Recorder, reg *RoomRegistry, repo RecorderRepo, lc LiveClient) *RecorderUsecase {
	uc := &RecorderUsecase{
		registry:     reg,
		repo:         repo,
		liveClient:   lc,
		pollInterval: defaultRoomInfoPollInterval,
		rec: ReconnectPolicy{
			AutoReconnect:      true,
			MaxReconnect:       defaultMaxReconnect,
			ReconnectDelay:     defaultReconnectDelay,
			CDNTransientBudget: defaultCDNTransientBudget,
		},
		cdnBackoffBase:      defaultCDNBackoffBase,
		redialDelay:         monitorRedialDelay,
		offlineConfirmDelay: defaultOfflineConfirmDelay,
		stableResetAfter:    defaultStableResetAfter,
	}
	return uc
}

// sleepCtx 在 ctx 被取消前阻塞 d 时长，若 ctx 被取消则返回 ctx.Err()。
func sleepCtx(ctx context.Context, d time.Duration) error {
	if d <= 0 {
		return ctx.Err()
	}
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return nil
	}
}
