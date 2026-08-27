package biz

import (
	"context"
	stderrors "errors"
	"io"
	"time"

	v1 "suika/api/room/v1"
	"suika/internal/conf"

	"github.com/go-kratos/kratos/v3/errors"
	"github.com/go-kratos/kratos/v3/log"
	"github.com/samber/lo"
)

var (
	// ErrRoomInternal 通用内部错误
	ErrRoomInternal = errors.InternalServer(v1.ErrorReason_ERROR_REASON_INTERNAL.String(), "recorder internal error")
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
	monitorRedialDelay          = 10 * time.Second  // 弹幕连接重拨前的停顿
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

type RecorderUsecase struct {
	roomRegistry *RoomRegistry
	repo         RecorderRepo
	liveClient   LiveClient

	pollInterval          time.Duration   // 拉取房间状态的兜底轮询间隔
	rec                   ReconnectPolicy // 断流决策树使用的重连配置
	cdnBackoffBase        time.Duration   // CDN 瞬时故障首次重试的延迟；测试中会调小。
	monitorReconnectDelay time.Duration   // 监控连接重连前的停顿；测试中会调小。
	offlineConfirmDelay   time.Duration   // 下播确认相邻两次探测的间隔；测试中会调小。
	stableResetAfter      time.Duration   // 泵送稳定录制超过该时长后重置重连预算；测试中会调小。
}

func NewRecorderUsecase(c *conf.Recorder, reg *RoomRegistry, repo RecorderRepo, lc LiveClient) *RecorderUsecase {
	uc := &RecorderUsecase{
		roomRegistry: reg,
		repo:         repo,
		liveClient:   lc,
		pollInterval: defaultRoomInfoPollInterval,
		rec: ReconnectPolicy{
			AutoReconnect:      true,
			MaxReconnect:       defaultMaxReconnect,
			ReconnectDelay:     defaultReconnectDelay,
			CDNTransientBudget: defaultCDNTransientBudget,
		},
		cdnBackoffBase:        defaultCDNBackoffBase,
		monitorReconnectDelay: monitorRedialDelay,
		offlineConfirmDelay:   defaultOfflineConfirmDelay,
		stableResetAfter:      defaultStableResetAfter,
	}
	return uc
}

// Run 运行录制守护进程的主循环：先收尾上次运行遗留的会话，然后作为监督
// 循环持续调和 RoomRegistry 快照与每房间的监控协程。监控跟随房间存在：
// 新建房间无论是否配置录制都立即开始监控，删除房间立即停止监控（活跃会话
// 随之优雅停止）；record_enabled 翻转不影响监控，只作为重评估信号送达监控
// 协程，由其决定开始或停止录制。
func (uc *RecorderUsecase) Run(ctx context.Context) error {
	// 收尾上次运行遗留的合并工作，若失败则记录错误并继续运行。
	if err := uc.repo.RecoverPending(ctx); err != nil {
		log.Error("recorder: recover pending merge", "err", err)
	}

	// 订阅 Room 注册表变更通知
	wakeup, unsubscribe := uc.roomRegistry.Subscribe()
	defer unsubscribe()

	// monitors 表示当前活跃的房间监控协程集合
	monitors := make(map[int64]*monitorHandle)
	// stopping 表示已停止的、等待收尾的监控协程集合
	stopping := make([]*monitorHandle, 0)

	defer func() {
		// 退出时取消所有监控协程
		for _, h := range monitors {
			h.cancel()
		}
		// 并等待所有监控协程收尾完成
		for _, h := range monitors {
			<-h.done
		}
		for _, h := range stopping {
			<-h.done
		}
	}()

	// 程序启动初始化
	uc.reconcile(ctx, monitors, &stopping)
	if len(monitors) == 0 {
		log.Warn("recorder has no configured rooms, idling")
	}

	// 程序运行中, 如果收到 RoomRegistry 变更通知, 则重新触发 reconcile
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-wakeup:
			uc.reconcile(ctx, monitors, &stopping)
		}
	}
}

// reconcile 调和 RoomRegistry 快照与 monitors/stopping 的状态，确保每个房间的监控协程正确启动/停止。
func (uc *RecorderUsecase) reconcile(ctx context.Context, monitors map[int64]*monitorHandle, stopping *[]*monitorHandle) {

	// 回收 retired 中已完成收尾的被移除监控。
	alive := (*stopping)[:0]
	for _, h := range *stopping {
		select {
		case <-h.done:
		default:
			alive = append(alive, h)
		}
	}
	*stopping = alive

	// 获取当前Room注册表快照，按 room_id 建立索引
	want := lo.KeyBy(uc.roomRegistry.Rooms(), func(r Room) int64 { return r.RoomID })

	// 如果 monitors 中的 Room 不在 want 中，则说明该房间已被删除，取消其监控协程并移入 stopping。
	for roomID, h := range monitors {
		if _, ok := want[roomID]; !ok {
			h.cancel()
			*stopping = append(*stopping, h)
			delete(monitors, roomID)
		}
	}

	for roomID, room := range want {
		monitor, ok := monitors[roomID]
		// 初始化启动/中途新增房间, 启动监控协程并登记到 monitors
		if !ok {
			monitor = uc.launchMonitor(ctx, roomID)
			monitor.lastRecordEnabled = room.RecordEnabled
			monitors[roomID] = monitor
			continue
		}

		// 已存在房间监控状态变动，通过发送信号的方式, 让监控协程重新评估是否需要启动/停止录制会话。
		if monitor.lastRecordEnabled != room.RecordEnabled {
			monitor.lastRecordEnabled = room.RecordEnabled
			monitor.notifyRoomChange()
		}
	}
}
