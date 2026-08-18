package biz

import (
	"context"
	stderrors "errors"
	"fmt"
	"io"
	"math/rand/v2"
	"time"

	v1 "suika/api/room/v1"
	"suika/internal/conf"

	"github.com/go-kratos/kratos/v3/errors"
	"github.com/go-kratos/kratos/v3/log"
	"github.com/samber/lo"
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
	cdnBackoffMax               = 60 * time.Second  // CDN 瞬时故障的重试延迟上限
	monitorRedialDelay          = 10 * time.Second  // 弹幕连接重拨前的停顿
	finishGracePeriod           = 30 * time.Second  // 限定关停期间 FinishSession 脱离已取消运行 context 后仍可用的工作时长
	pollJitterFraction          = 5                 // 回退轮询间隔的相对抖动幅度（间隔 +/- fraction/2）
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

// RoomInfo 是从 B 站获取的直播间元数据
type RoomInfo struct {
	RoomID        int64
	Live          bool
	Title         string
	StreamerName  string
	LiveStartTime time.Time
}

// 调用 B 站接口获取的授予直播流清晰度信息
type StreamQuality struct {
	Qn   int32
	Desc string
}

// StreamHandle 是 LiveClient.OpenStream 打开的一路直播流。
// 由 LiveClient 产生、被 RecorderRepo 消费
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

// SessionResult 一次录制会话的最终结果
type SessionResult struct {
	BytesWritten int64 // 总字节数
	Parts        int   // 分段数
}

// SessionStats 一次录制会话的当前统计信息
type SessionStats struct {
	CurrentFile  string // 当前正在写入的分段文件名（可能为空）
	BytesWritten int64  // 当前分段已写入的字节数
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
	// OpenStream 打开房间的直播流，返回一个 StreamHandle。若房间未开播则返回错误。
	OpenStream(ctx context.Context, roomID int64) (*StreamHandle, error)
	// DanmakuConn 打开房间的弹幕 websocket 连接，返回一个 DanmakuConn。若房间不存在则返回错误。
	DanmakuConn(ctx context.Context, roomID int64) (DanmakuConn, error)
}

// RecorderRepo 是录制器的存储接口。负责磁盘 IO
type RecorderRepo interface {
	// PrepareSession 按"房间 + 开播时间" 创建/定位 目录和 meta.json。
	PrepareSession(ctx context.Context, session *Session) error
	// RecordSession 将直播流写入磁盘（按配置切分分段），并把事件写入对应的 JSONL 文件，直到流结束或 ctx 被取消。
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

	// slots 是录制槽位的有界缓冲通道，容量为 maxConcurrent。若为 nil，则不限制并发。
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
		pollInterval: defaultRoomInfoPollInterval,
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

// Run 运行录制守护进程的主循环：先收尾上次运行遗留的会话，然后作为监督
// 循环持续调和 RoomRegistry 快照与每房间的监控协程。监控跟随房间存在：
// 新建房间无论启用与否都立即开始监控，删除房间立即停止监控（活跃会话
// 随之优雅停止）；enabled 翻转不影响监控，只作为重评估信号送达监控协
// 程，由其决定开始或停止录制。
func (uc *RecorderUsecase) Run(ctx context.Context) error {

	// 收尾上次运行遗留的转封装工作，若失败则记录错误并继续运行。
	if err := uc.repo.RecoverPending(ctx); err != nil {
		log.Error("recorder: recover pending remux", "err", err)
	}

	// 订阅 Room 注册表变更通知，返回一个通道和取消函数。
	changes, unsubscribe := uc.registry.Subscribe()
	defer unsubscribe()

	// monitors 是当前活跃的房间监控协程集合；retired 是已停止的监控协程集合，等待收尾。
	monitors := make(map[int64]*monitorHandle)
	var retired []*monitorHandle
	defer func() {
		for _, h := range monitors {
			h.cancel()
		}
		for _, h := range monitors {
			<-h.done
		}
		for _, h := range retired {
			<-h.done
		}
	}()

	uc.reconcile(ctx, monitors, &retired)
	if len(monitors) == 0 {
		log.Warn("recorder has no configured rooms, idling")
	}
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-changes:
			uc.reconcile(ctx, monitors, &retired)
		}
	}
}

// monitorHandle 是监督循环管理的一个房间监控协程句柄。roomChanged 是
// 合并式重评估信号（如 enabled 翻转），由监督循环送达 watchRoom。
type monitorHandle struct {
	// enabled 记录最近一次 reconcile 时，房间的监控是否启用。用于监督循环向监控协程投递重评估信号。
	enabled     bool
	roomChanged chan struct{}
	cancel      context.CancelFunc
	done        chan struct{}
}

// signal 向 roomChanged 投递一个重评估信号，若通道已满则丢弃。
func (h *monitorHandle) signal() {
	select {
	case h.roomChanged <- struct{}{}:
	default:
	}
}

// reconcile 按注册表快照调和监控协程集合：为新增房间启动监控；停止并
// 移除已删除房间的监控（移入 retired 自行优雅收尾，不阻塞调和）；
// enabled 翻转的房间投递重评估信号。
func (uc *RecorderUsecase) reconcile(ctx context.Context, monitors map[int64]*monitorHandle, retired *[]*monitorHandle) {

	// 回收已完成收尾的被移除监控。
	alive := (*retired)[:0]
	for _, h := range *retired {
		select {
		case <-h.done:
		default:
			alive = append(alive, h)
		}
	}
	*retired = alive

	rooms := uc.registry.Rooms()
	// want 是当前注册表快照的房间集合，按 roomID 索引。
	want := lo.KeyBy(rooms, func(r Room) int64 { return r.RoomID })

	// 1. 停止并移除已删除房间的监控。
	for roomID, h := range monitors {
		if _, ok := want[roomID]; !ok {
			h.cancel()
			*retired = append(*retired, h)
			delete(monitors, roomID)
		}
	}

	for roomID, room := range want {
		handle, ok := monitors[roomID]

		//  初次启动，或中途新增房间，启动监控协程并登记到 monitors。
		if !ok {
			handle = uc.startMonitor(ctx, roomID)
			handle.enabled = room.Enabled
			monitors[roomID] = handle
			continue
		}

		// 已存在房间监控状态变动，发送重评估信号
		if handle.enabled != room.Enabled {
			handle.enabled = room.Enabled
			handle.signal()
		}
	}
}

// startMonitor 启动指定房间的监控协程。
func (uc *RecorderUsecase) startMonitor(ctx context.Context, roomID int64) *monitorHandle {
	mctx, cancel := context.WithCancel(ctx)
	h := &monitorHandle{
		roomChanged: make(chan struct{}, 1),
		cancel:      cancel,
		done:        make(chan struct{}),
	}
	go func() {
		defer close(h.done)
		uc.monitorRoom(mctx, h.roomChanged, roomID)
	}()
	return h
}

// monitorRoom 维持房间的弹幕连接，断开即重拨，直到 ctx 被取消。
func (uc *RecorderUsecase) monitorRoom(ctx context.Context, roomChanged <-chan struct{}, roomID int64) {
	for ctx.Err() == nil {
		if err := uc.watchRoom(ctx, roomChanged, roomID); err != nil && ctx.Err() == nil {
			log.Error("room monitor failed", "room", roomID, "err", err)
			uc.registry.NoteError(roomID, err)
		}
		if sleepCtx(ctx, uc.redialDelay) != nil {
			return
		}
	}
}

// watchRoom 是单个房间的监控分发器：持有弹幕连接、回退轮询定时器和会话
// 协程的上下文，本身不含任何会话启停判断。启停策略集中在 sessionPolicy：
// 各 select 分支把输入投递给策略（房间信息到达前先应用到注册表），并执
// 行返回的决策——Start 启动会话协程，Stop 取消活跃会话。会话是否启动受
// 房间 enabled 门控；roomChanged 信号只承担重评估的投递，监控本身不受影
// 响。无活跃会话时弹幕事件被直接丢弃；活跃会话的 RecordSession 直接消费
// 事件。
func (uc *RecorderUsecase) watchRoom(ctx context.Context, roomChanged <-chan struct{}, roomID int64) error {

	// 1. 弹幕连接
	danmakuConn, err := uc.liveClient.DanmakuConn(ctx, roomID)
	if err != nil {
		return fmt.Errorf("open danmaku conn: %w", err)
	}
	defer danmakuConn.Close()

	// 2. 带抖动的轮询
	poll := time.NewTimer(jitterDuration(uc.pollInterval, pollJitterFraction))
	defer poll.Stop()

	policy := newSessionPolicy(uc.registry.Room(roomID).Enabled)
	var active *sessionHandle
	for {
		// 3. 事件分发
		var events <-chan *DanmakuEvent
		var done chan struct{}
		if active == nil {
			// 当前无录制会话，持续消费弹幕事件以维持监控
			events = danmakuConn.Events()
		} else {
			// 当前有录制会话，事件由录制会话协程直接消费；watchRoom 只监听其结束信号
			done = active.done
		}

		select {
		case <-ctx.Done():
			if active != nil {
				active.cancel()
				<-active.done // 等待录制会话协程自然结束，避免中途取消导致转封装失败
			}
			return nil
		case <-events:
			// 无LIVE 事件：丢弃
		case <-done:
			// 录制会话结束
			active = uc.executeDecision(ctx, roomID, danmakuConn, nil, policy.SessionFinished())
		case roomInfo := <-danmakuConn.RoomStateUpdates():
			uc.registry.ApplyRoomInfo(ctx, roomID, roomInfo)
			active = uc.executeDecision(ctx, roomID, danmakuConn, active, policy.RoomInfoArrived(roomInfo))
		case <-poll.C:
			roomInfo, err := uc.liveClient.GetRoomInfo(ctx, roomID)
			if err != nil {
				log.Warn("fallback poll failed", "room", roomID, "err", err)
				uc.registry.NoteError(roomID, err)
			} else {
				uc.registry.ApplyRoomInfo(ctx, roomID, roomInfo)
				active = uc.executeDecision(ctx, roomID, danmakuConn, active, policy.RoomInfoArrived(roomInfo))
			}
			poll.Reset(jitterDuration(uc.pollInterval, pollJitterFraction))
		case <-roomChanged:
			// 取得注册表中最新的启用状态后投递给策略；值未变时策略自
			// 行吸收为无操作决策。
			room := uc.registry.Room(roomID)
			active = uc.executeDecision(ctx, roomID, danmakuConn, active, policy.EnabledFlipped(room.Enabled))
		}
	}
}

// executeDecision 执行会话策略的决策并返回（可能更新的）活跃会话句柄：
// Start 以决策携带的房间信息启动会话协程；Stop 取消活跃会话（策略保证
// Stop 只在会话录制中产生）；None 不做任何事。
func (uc *RecorderUsecase) executeDecision(ctx context.Context, roomID int64, conn DanmakuConn, active *sessionHandle, decision policyDecision) *sessionHandle {
	switch decision.kind {
	case decisionStart:
		return uc.launchSession(ctx, roomID, decision.info, conn.Events())
	case decisionStop:
		active.cancel()
	}
	return active
}

// launchSession 启动录制会话协程，独占完整的录制循环、FinishSession和槽位释放。
func (uc *RecorderUsecase) launchSession(ctx context.Context, roomID int64, info *RoomInfo, events <-chan *DanmakuEvent) *sessionHandle {
	sctx, cancel := context.WithCancel(ctx)
	handle := &sessionHandle{cancel: cancel, done: make(chan struct{})}
	go func() {
		defer close(handle.done)
		uc.runSession(sctx, roomID, info, events)
	}()
	return handle
}

// runSession 端到端负责一次会话：槽位、准备、录制循环、收尾/转封装。
func (uc *RecorderUsecase) runSession(ctx context.Context, roomID int64, info *RoomInfo, events <-chan *DanmakuEvent) {

	// acquireSlot 尝试获取一个录制槽位，若已满则阻塞等待或直到 ctx 被取消。
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

	// * 录制循环：持续拉流直到连接结束，然后重新探测直播状态，要么重连（新分段），要么结束会话并保留已录内容。
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

// recordLoop 断流决策树：持续拉流直到连接结束，然后重新探测直播状态，要么重连（新分段），要么结束会话并保留已录内容。
func (uc *RecorderUsecase) recordLoop(ctx context.Context, roomID int64, session *Session, events <-chan *DanmakuEvent) {
	reconnects := 0
	cdnBudget := uc.rec.CDNTransientBudget
	cdnAttempt := 0
	for {
		// 1. 拉流
		stream, err := uc.liveClient.OpenStream(ctx, roomID)
		if err != nil {
			log.Error("open stream failed", "room", roomID, "err", err)
			uc.registry.NoteError(roomID, err)
			return
		}

		// 2. 录制
		session.Quality = stream.Quality
		result, recErr := uc.repo.RecordSession(ctx, session, stream, events)
		if result != nil {
			log.Info("pump ended", "room", roomID, "bytes", result.BytesWritten, "parts", result.Parts, "err", recErr)
		}
		if ctx.Err() != nil {
			return
		}

		// 3. 探测直播状态
		roomInfo, err := uc.liveClient.GetRoomInfo(ctx, roomID)
		if err != nil {
			log.Error("probe live status failed, ending session", "room", roomID, "err", err)
			uc.registry.NoteError(roomID, err)
			return
		}
		uc.registry.ApplyRoomInfo(ctx, roomID, roomInfo)
		if !roomInfo.Live {
			return
		}

		// 4. 断流决策树：CDN 瞬时故障重连、风控拒绝不重连、其他错误按配置重连。
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

// acquireSlot 尝试获取一个录制槽位，若已满则阻塞等待或直到 ctx 被取消
func (uc *RecorderUsecase) acquireSlot(ctx context.Context, roomID int64) error {

	// 未配置 maxConcurrent，则不限制并发
	if uc.slots == nil {
		return nil
	}

	// 尝试非阻塞获取槽位，若已满则阻塞等待,或直到 ctx 被取消
	select {
	case uc.slots <- struct{}{}:
		return nil
	default:
		log.Warn("recording slots full, queueing", "room", roomID, "max", uc.maxConcurrent)
	}

	// 阻塞等待槽位或 ctx 被取消
	select {
	case uc.slots <- struct{}{}:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// releaseSlot 释放一个录制槽位
func (uc *RecorderUsecase) releaseSlot() {
	if uc.slots != nil {
		<-uc.slots
	}
}

// cdnBackoff 返回 CDN 瞬时故障的重试延迟，随尝试次数指数增长，最大不超过 cdnBackoffMax。
func (uc *RecorderUsecase) cdnBackoff(attempt int) time.Duration {
	return min(uc.cdnBackoffBase<<attempt, cdnBackoffMax)
}

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
