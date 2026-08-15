package biz

import (
	"context"
	stderrors "errors"
	"fmt"
	"io"
	"math/rand/v2"
	"sync"
	"time"

	v1 "suika/api/room/v1"
	"suika/internal/conf"

	"github.com/go-kratos/kratos/v3/errors"
	"github.com/go-kratos/kratos/v3/log"
)

var (
	ErrRoomInternal = errors.InternalServer(v1.ErrorReason_ERROR_REASON_INTERNAL.String(), "recorder internal error")
)

// 用于区分断流原因的哨兵错误：由决策树（biz）判定语义，data 层在
// 错误源头处包装。
var (
	// ErrStreamTransient 标记 CDN 侧的瞬时故障（HTTP 404、连接被重置
	// 等），值得重新选择流地址后重试。
	ErrStreamTransient = stderrors.New("recorder: transient stream error")
	// ErrRiskControl 标记 B 站风控拒绝（-352/412 等）。
	ErrRiskControl = stderrors.New("recorder: risk control triggered")
)

// 录制器默认值。proto 标量无法区分"未设置"和零值，零值在此替换为
// 默认值（与 service.defaultPageSize 同一手法）。
const (
	defaultFallbackPollInterval = 600 * time.Second
	defaultMaxReconnect         = 3
	defaultReconnectDelay       = 10 * time.Second
	defaultCDNTransientBudget   = 5
	defaultCDNBackoffBase       = 2 * time.Second
	cdnBackoffMax               = 60 * time.Second
	// monitorRedialDelay 是弹幕连接重拨前的停顿。
	monitorRedialDelay = 10 * time.Second
	// finishGracePeriod 限定关停期间 FinishSession 脱离已取消的运行
	// context 后仍可用的工作时长。
	finishGracePeriod = 30 * time.Second
	// pollJitterFraction 是回退轮询间隔的相对抖动幅度
	//（间隔 +/- fraction/2）。
	pollJitterFraction = 5 // => +/- 10%
)

// 写入 JSONL 的弹幕事件类型。
const (
	EventDanmaku     = "danmaku"
	EventGift        = "gift"
	EventSuperChat   = "superchat"
	EventGuard       = "guard"
	EventEntryEffect = "entry_effect"
	EventInteract    = "interact_word"
)

// RoomInfo 是平台上报的直播间元数据。
type RoomInfo struct {
	RoomID        int64
	Live          bool
	Title         string
	StreamerName  string
	LiveStartTime time.Time
}

type StreamQuality struct {
	Qn   int32
	Desc string
}

// StreamHandle 是 LiveClient.OpenStream 打开的一路直播流。它对 biz 是
// 不透明的：由 LiveClient 产生、被 RecorderRepo 消费，中间从不被检视
// （同 *sql.Rows 的用法）。
type StreamHandle struct {
	URL     string
	Quality StreamQuality
	Body    io.ReadCloser
}

// DanmakuEvent 是一条过滤后的弹幕房间事件。各字段的相关性取决于
// Type；落盘的 JSON 形状由 RecorderRepo 决定。
type DanmakuEvent struct {
	Ts       time.Time
	Type     string
	UID      int64
	Uname    string
	Text     string // 弹幕文本 / SC 文本 / 进场特效文本
	Color    int32  // 弹幕
	Mode     int32  // 弹幕
	GiftName string // 礼物
	Num      int32  // 礼物/舰长数量
	Price    int64  // 礼物价格（金瓜子）/ SC 价格
	CoinType string // 礼物：gold/silver
	Duration int32  // SC 保留秒数
	Level    int32  // 舰长等级
	Raw      []byte // 原始 JSON 载荷
}

// Session 是一次录制会话（同一房间的一次开播）。
type Session struct {
	RoomID        int64
	RoomName      string
	Title         string
	LiveStartTime time.Time
	Quality       StreamQuality
}

// SessionResult 汇报一次 RecordSession 拉流写入的结束状态。
type SessionResult struct {
	BytesWritten int64
	Parts        int
}

type SessionStats struct {
	CurrentFile  string
	BytesWritten int64
}

// DanmakuConn 是一个房间的常驻弹幕 websocket，同时服务于开播检测
// （RoomStateUpdates）和弹幕录制（Events）。实现内部自行重连；每次
// 重连成功后重新探测并重新推送房间状态，以补上断连期间错过的
// LIVE/PREPARING 事件。Events 使用有界缓冲，无人消费时丢弃事件。
type DanmakuConn interface {
	Events() <-chan *DanmakuEvent
	RoomStateUpdates() <-chan *RoomInfo
	Close() error
}

// LiveClient 是外部平台接缝：所有 B 站流量都从这里走。
type LiveClient interface {
	RoomStatus(ctx context.Context, roomID int64) (*RoomInfo, error)
	OpenStream(ctx context.Context, roomID int64) (*StreamHandle, error)
	DanmakuConn(ctx context.Context, roomID int64) (DanmakuConn, error)
}

// RecorderRepo 是存储接缝：录制目录布局、文件读写与转封装。
type RecorderRepo interface {
	// PrepareSession 按"房间 + 开播时间"创建（或在重启后重新定位）
	// 会话目录和 meta.json。
	PrepareSession(ctx context.Context, session *Session) error
	// RecordSession 将直播流写入磁盘（按配置切分分段），并把事件写入
	// 对应的 JSONL 文件，直到流结束或 ctx 被取消。
	RecordSession(ctx context.Context, session *Session, stream *StreamHandle, events <-chan *DanmakuEvent) (*SessionResult, error)
	// FinishSession 收尾 meta.json 并对已录分段执行转封装。
	FinishSession(ctx context.Context, session *Session) error
	// RecoverPending 完成上次运行遗留的转封装工作。
	RecoverPending(ctx context.Context) error
}

// ReconnectPolicy 是断流决策树使用的重连配置（展开后的扁平形式）。
type ReconnectPolicy struct {
	AutoReconnect      bool
	MaxReconnect       int
	ReconnectDelay     time.Duration
	CDNTransientBudget int
}

// RecorderUsecase 编排房间监控、会话生命周期和断流决策树。它只做
// 决策：所有平台 IO 由 LiveClient 执行，所有存储 IO 由 RecorderRepo
// 执行。房间配置与直播/录制状态存放在共享的 RoomRegistry 中。
type RecorderUsecase struct {
	registry   *RoomRegistry
	repo       RecorderRepo
	liveClient LiveClient

	pollInterval  time.Duration
	maxConcurrent int
	rec           ReconnectPolicy

	// cdnBackoffBase 是 CDN 瞬时故障首次重试的延迟；测试中会调小。
	cdnBackoffBase time.Duration
	// redialDelay 是监控重拨的停顿；测试中会调小。
	redialDelay time.Duration

	slots chan struct{}
}

type sessionHandle struct {
	cancel context.CancelFunc
	done   chan struct{}
}

func NewRecorderUsecase(c *conf.Recorder, reg *RoomRegistry, repo RecorderRepo, lc LiveClient) *RecorderUsecase {
	uc := &RecorderUsecase{
		registry:     reg,
		repo:         repo,
		liveClient:   lc,
		pollInterval: defaultFallbackPollInterval,
		rec: ReconnectPolicy{
			AutoReconnect:      true,
			MaxReconnect:       defaultMaxReconnect,
			ReconnectDelay:     defaultReconnectDelay,
			CDNTransientBudget: defaultCDNTransientBudget,
		},
		cdnBackoffBase: defaultCDNBackoffBase,
		redialDelay:    monitorRedialDelay,
	}
	if c == nil {
		log.Warn("recorder configuration missing, running with zero rooms")
		return uc
	}
	if c.GetFallbackPollInterval() != nil {
		uc.pollInterval = c.GetFallbackPollInterval().AsDuration()
	}
	uc.maxConcurrent = int(c.GetMaxConcurrent())
	if rc := c.GetReconnect(); rc != nil {
		if rc.AutoReconnect != nil {
			uc.rec.AutoReconnect = rc.GetAutoReconnect()
		}
		if rc.GetMaxReconnect() > 0 {
			uc.rec.MaxReconnect = int(rc.GetMaxReconnect())
		}
		if rc.GetReconnectDelay() != nil {
			uc.rec.ReconnectDelay = rc.GetReconnectDelay().AsDuration()
		}
		if rc.GetCdnTransientBudget() > 0 {
			uc.rec.CDNTransientBudget = int(rc.GetCdnTransientBudget())
		}
	}
	if uc.maxConcurrent > 0 {
		uc.slots = make(chan struct{}, uc.maxConcurrent)
	}
	return uc
}

func (uc *RecorderUsecase) Run(ctx context.Context) error {
	rooms := uc.registry.Rooms()
	if len(rooms) == 0 {
		log.Warn("recorder has no configured rooms, idling")
		<-ctx.Done()
		return nil
	}

	if err := uc.repo.RecoverPending(ctx); err != nil {
		log.Error("recorder: recover pending remux", "err", err)
	}

	var wg sync.WaitGroup
	for _, room := range rooms {
		if !room.Enabled {
			continue
		}
		wg.Add(1)
		go func(roomID int64) {
			defer wg.Done()
			uc.monitorRoom(ctx, roomID)
		}(room.RoomID)
	}
	wg.Wait()
	return nil
}

// monitorRoom 维持房间的弹幕连接，断开即重拨，直到 ctx 被取消。
func (uc *RecorderUsecase) monitorRoom(ctx context.Context, roomID int64) {
	for ctx.Err() == nil {
		if err := uc.watchRoom(ctx, roomID); err != nil && ctx.Err() == nil {
			log.Error("room monitor failed", "room", roomID, "err", err)
			uc.registry.NoteError(roomID, err)
		}
		if sleepCtx(ctx, uc.redialDelay) != nil {
			return
		}
	}
}

// watchRoom 持有一条弹幕连接：把控制事件翻译成会话的开始/结束，并运行
// 回退轮询。无活跃会话时事件被直接丢弃；活跃会话的 RecordSession
// 直接消费事件。
func (uc *RecorderUsecase) watchRoom(ctx context.Context, roomID int64) error {

	// 1. 弹幕连接
	conn, err := uc.liveClient.DanmakuConn(ctx, roomID)
	if err != nil {
		return fmt.Errorf("open danmaku conn: %w", err)
	}
	defer conn.Close()

	// 2. 带抖动的回退轮询
	poll := time.NewTimer(jitterDuration(uc.pollInterval, pollJitterFraction))
	defer poll.Stop()

	var active *sessionHandle
	for {
		var events <-chan *DanmakuEvent
		var done chan struct{}
		if active == nil {
			events = conn.Events()
		} else {
			done = active.done
		}

		select {
		case <-ctx.Done():
			if active != nil {
				active.cancel()
				<-active.done
			}
			return nil
		case <-events:
			// 无活跃会话：丢弃
		case <-done:
			active = nil
		case info := <-conn.RoomStateUpdates():
			uc.registry.ApplyRoomInfo(ctx, roomID, info)
			if info.Live && active == nil {
				active = uc.launchSession(ctx, roomID, info, conn.Events())
			} else if !info.Live && active != nil {
				active.cancel()
			}
		case <-poll.C:
			info, err := uc.liveClient.RoomStatus(ctx, roomID)
			if err != nil {
				log.Warn("fallback poll failed", "room", roomID, "err", err)
				uc.registry.NoteError(roomID, err)
			} else {
				uc.registry.ApplyRoomInfo(ctx, roomID, info)
				if info.Live && active == nil {
					active = uc.launchSession(ctx, roomID, info, conn.Events())
				} else if !info.Live && active != nil {
					active.cancel()
				}
			}
			poll.Reset(jitterDuration(uc.pollInterval, pollJitterFraction))
		}
	}
}

// launchSession 启动会话协程，它独占完整的录制循环、FinishSession
// 和槽位释放。
func (uc *RecorderUsecase) launchSession(ctx context.Context, roomID int64, info *RoomInfo, events <-chan *DanmakuEvent) *sessionHandle {
	sctx, cancel := context.WithCancel(ctx)
	h := &sessionHandle{cancel: cancel, done: make(chan struct{})}
	go func() {
		defer close(h.done)
		uc.runSession(sctx, roomID, info, events)
	}()
	return h
}

// runSession 端到端负责一次会话：槽位、准备、录制循环、收尾/转封装。
func (uc *RecorderUsecase) runSession(ctx context.Context, roomID int64, info *RoomInfo, events <-chan *DanmakuEvent) {
	if err := uc.acquireSlot(ctx, roomID); err != nil {
		return
	}
	defer uc.releaseSlot()

	room := uc.registry.Room(roomID)
	session := &Session{
		RoomID:        roomID,
		RoomName:      firstNonEmpty(room.StreamerName, info.StreamerName, fmt.Sprintf("%d", roomID)),
		Title:         info.Title,
		LiveStartTime: info.LiveStartTime,
	}
	uc.registry.StartRecording(roomID)

	if err := uc.repo.PrepareSession(ctx, session); err != nil {
		log.Error("prepare session failed", "room", roomID, "err", err)
		uc.registry.FailRecording(roomID, err)
		return
	}

	uc.recordLoop(ctx, roomID, session, events)

	// 收尾脱离（可能已取消的）运行 context，保证关停期间转封装标记
	// 仍能落盘；遗留部分由下次启动时的 RecoverPending 接管。
	uc.registry.SetRemuxing(roomID)
	fctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), finishGracePeriod)
	defer cancel()
	if err := uc.repo.FinishSession(fctx, session); err != nil {
		log.Error("finish session failed", "room", roomID, "err", err)
		uc.registry.FailRecording(roomID, err)
		return
	}
	uc.registry.FinishRecording(roomID)
}

// recordLoop 是断流决策树：持续拉流直到连接结束，然后重新探测直播
// 状态，要么重连（新分段），要么结束会话并保留已录内容。
func (uc *RecorderUsecase) recordLoop(ctx context.Context, roomID int64, session *Session, events <-chan *DanmakuEvent) {
	reconnects := 0
	cdnBudget := uc.rec.CDNTransientBudget
	cdnAttempt := 0
	for {
		stream, err := uc.liveClient.OpenStream(ctx, roomID)
		if err != nil {
			log.Error("open stream failed", "room", roomID, "err", err)
			uc.registry.NoteError(roomID, err)
			return
		}
		session.Quality = stream.Quality
		result, recErr := uc.repo.RecordSession(ctx, session, stream, events)
		if result != nil {
			log.Info("pump ended", "room", roomID, "bytes", result.BytesWritten, "parts", result.Parts, "err", recErr)
		}
		if ctx.Err() != nil {
			return
		}

		info, err := uc.liveClient.RoomStatus(ctx, roomID)
		if err != nil {
			log.Error("probe live status failed, ending session", "room", roomID, "err", err)
			uc.registry.NoteError(roomID, err)
			return
		}
		uc.registry.ApplyRoomInfo(ctx, roomID, info)
		if !info.Live {
			return
		}

		if stderrors.Is(recErr, ErrStreamTransient) {
			if cdnBudget <= 0 {
				log.Warn("cdn transient budget exhausted, finishing session with recorded content", "room", roomID)
				return
			}
			cdnBudget--
			delay := uc.cdnBackoff(cdnAttempt)
			cdnAttempt++
			log.Warn("transient stream error, re-opening stream", "room", roomID, "err", recErr, "delay", delay)
			if sleepCtx(ctx, delay) != nil {
				return
			}
			continue
		}

		if !uc.rec.AutoReconnect {
			return
		}
		if reconnects >= uc.rec.MaxReconnect {
			log.Warn("reconnect budget exhausted, finishing session with recorded content", "room", roomID)
			return
		}
		reconnects++
		log.Warn("stream interrupted, reconnecting", "room", roomID, "err", recErr, "attempt", reconnects, "max", uc.rec.MaxReconnect, "delay", uc.rec.ReconnectDelay)
		if sleepCtx(ctx, uc.rec.ReconnectDelay) != nil {
			return
		}
	}
}

func (uc *RecorderUsecase) acquireSlot(ctx context.Context, roomID int64) error {
	if uc.slots == nil {
		return nil
	}
	select {
	case uc.slots <- struct{}{}:
		return nil
	default:
		log.Warn("recording slots full, queueing", "room", roomID, "max", uc.maxConcurrent)
	}
	select {
	case uc.slots <- struct{}{}:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (uc *RecorderUsecase) releaseSlot() {
	if uc.slots != nil {
		<-uc.slots
	}
}

func (uc *RecorderUsecase) cdnBackoff(attempt int) time.Duration {
	return min(uc.cdnBackoffBase<<attempt, cdnBackoffMax)
}

// sleepCtx 睡眠 d；ctx 提前取消则返回其错误。
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

// jitterDuration 返回叠加了 +/- 1/(2*div) 抖动的 d。
func jitterDuration(d time.Duration, div int) time.Duration {
	if d <= 0 || div <= 0 {
		return d
	}
	span := int64(d) / int64(div)
	if span <= 0 {
		return d
	}
	return d + time.Duration(rand.Int64N(span)) - time.Duration(span/2)
}

func firstNonEmpty(vals ...string) string {
	for _, v := range vals {
		if v != "" {
			return v
		}
	}
	return ""
}
