package biz

import (
	"context"
	stderrors "errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"suika/internal/conf"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/durationpb"
)

// fakeRepo 为决策树测试模拟 RecorderRepo 行为。
type fakeRepo struct {
	prepareErr  error
	recordQueue []recordOutcome // 每次调用弹出一个；最后一项固定复用
	recordCalls int
	finished    []*Session
	finishErr   error
}

type recordOutcome struct {
	result *SessionResult
	err    error
}

func (r *fakeRepo) PrepareSession(_ context.Context, _ *Session) error { return r.prepareErr }

func (r *fakeRepo) RecordSession(_ context.Context, session *Session, stream *Stream, _ <-chan *DanmakuEvent) (*SessionResult, error) {
	if stream != nil && stream.Body != nil {
		stream.Body.Close()
	}
	r.recordCalls++
	if len(r.recordQueue) == 0 {
		return &SessionResult{}, nil
	}
	out := r.recordQueue[0]
	if len(r.recordQueue) > 1 {
		r.recordQueue = r.recordQueue[1:]
	}
	_ = session
	return out.result, out.err
}

func (r *fakeRepo) FinishSession(_ context.Context, session *Session) error {
	r.finished = append(r.finished, session)
	return r.finishErr
}

func (r *fakeRepo) RecoverPending(context.Context) error { return nil }

// fakeLiveClient 模拟 LiveClient 行为。
type fakeLiveClient struct {
	statusQueue []statusOutcome // 每次调用弹出一个；最后一项固定复用
	statusCalls int
	openErrs    []error // 每次 OpenStream 调用弹出一个
	openCalls   int
}

type statusOutcome struct {
	info *RoomInfo
	err  error
}

func (c *fakeLiveClient) GetRoomInfo(_ context.Context, roomID int64) (*RoomInfo, error) {
	c.statusCalls++
	if len(c.statusQueue) == 0 {
		return &RoomInfo{RoomID: roomID}, nil
	}
	out := c.statusQueue[0]
	if len(c.statusQueue) > 1 {
		c.statusQueue = c.statusQueue[1:]
	}
	return out.info, out.err
}

func (c *fakeLiveClient) OpenStream(_ context.Context, roomID int64) (*Stream, error) {
	c.openCalls++
	if len(c.openErrs) > 0 {
		err := c.openErrs[0]
		if len(c.openErrs) > 1 {
			c.openErrs = c.openErrs[1:]
		}
		if err != nil {
			return nil, err
		}
	}
	return &Stream{URL: fmt.Sprintf("http://cdn/%d", roomID)}, nil
}

func (c *fakeLiveClient) DanmakuConn(context.Context, int64) (DanmakuConn, error) {
	return nil, stderrors.New("not used in decision tree tests")
}

func newTestUsecase(t *testing.T, repo RecorderRepo, lc LiveClient, mutate func(*RecorderUsecase)) *RecorderUsecase {
	t.Helper()
	return newTestUsecaseWithRooms(t, map[int64]*Room{42: {RoomID: 42, StreamerName: "tester", RecordEnabled: true}}, repo, lc, mutate)
}

func newTestUsecaseWithRooms(t *testing.T, rooms map[int64]*Room, repo RecorderRepo, lc LiveClient, mutate func(*RecorderUsecase)) *RecorderUsecase {
	t.Helper()
	c := &conf.Recorder{
		Reconnect: &conf.Recorder_ReconnectOptions{
			ReconnectDelay: durationpb.New(time.Millisecond),
		},
	}
	roomRepo := &fakeRoomRepo{rooms: rooms}
	reg, err := NewRoomRegistry(roomRepo)
	if err != nil {
		t.Fatalf("NewRoomRegistry() error = %v", err)
	}
	uc := NewRecorderUsecase(c, reg, repo, lc)
	uc.cdnBackoffBase = time.Millisecond
	uc.redialDelay = time.Millisecond
	if mutate != nil {
		mutate(uc)
	}
	return uc
}

// waitFor 轮询 cond 直至成立或超时。
func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", what)
}

// waitRecordStatus 轮询 RoomRegistry，直到房间到达期望的录制状态或超时。
func waitRecordStatus(reg *RoomRegistry, roomID int64, want RecordStatus) bool {
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		var got RecordStatus
		var ok bool
		reg.mu.Lock()
		st, found := reg.states[roomID]
		if found {
			got, ok = st.recordStatus, true
		}
		reg.mu.Unlock()
		if ok && got == want {
			return true
		}
		time.Sleep(5 * time.Millisecond)
	}
	return false
}

func liveInfo(roomID int64, live bool) *RoomInfo {
	return &RoomInfo{RoomID: roomID, Live: live, Title: "t", StreamerName: "s"}
}

func TestRecordLoopStopsWhenOffline(t *testing.T) {
	repo := &fakeRepo{recordQueue: []recordOutcome{{result: &SessionResult{BytesWritten: 10}, err: stderrors.New("eof")}}}
	lc := &fakeLiveClient{statusQueue: []statusOutcome{{info: liveInfo(42, false)}}}
	uc := newTestUsecase(t, repo, lc, nil)

	uc.recordLoop(context.Background(), 42, &Session{RoomID: 42}, make(chan *DanmakuEvent))

	if repo.recordCalls != 1 {
		t.Fatalf("recordCalls = %d, want 1", repo.recordCalls)
	}
	if lc.openCalls != 1 {
		t.Fatalf("openCalls = %d, want 1", lc.openCalls)
	}
}

func TestRecordLoopReconnectsWhileLive(t *testing.T) {
	repo := &fakeRepo{recordQueue: []recordOutcome{
		{result: &SessionResult{}, err: stderrors.New("reset")},
		{result: &SessionResult{}, err: stderrors.New("eof")},
	}}
	lc := &fakeLiveClient{statusQueue: []statusOutcome{
		{info: liveInfo(42, true)},
		{info: liveInfo(42, false)},
	}}
	uc := newTestUsecase(t, repo, lc, nil)

	uc.recordLoop(context.Background(), 42, &Session{RoomID: 42}, make(chan *DanmakuEvent))

	if repo.recordCalls != 2 {
		t.Fatalf("recordCalls = %d, want 2", repo.recordCalls)
	}
}

func TestRecordLoopBudgetExhaustedKeepsContent(t *testing.T) {
	repo := &fakeRepo{recordQueue: []recordOutcome{{result: &SessionResult{BytesWritten: 1}, err: stderrors.New("reset")}}}
	lc := &fakeLiveClient{statusQueue: []statusOutcome{{info: liveInfo(42, true)}}}
	uc := newTestUsecase(t, repo, lc, func(u *RecorderUsecase) {
		u.rec.MaxReconnect = 1
	})

	uc.recordLoop(context.Background(), 42, &Session{RoomID: 42}, make(chan *DanmakuEvent))

	// 首次尝试 + 1 次重连，随后放弃并保留已录内容。
	if repo.recordCalls != 2 {
		t.Fatalf("recordCalls = %d, want 2", repo.recordCalls)
	}
}

func TestRecordLoopAutoReconnectDisabled(t *testing.T) {
	repo := &fakeRepo{recordQueue: []recordOutcome{{result: &SessionResult{}, err: stderrors.New("reset")}}}
	lc := &fakeLiveClient{statusQueue: []statusOutcome{{info: liveInfo(42, true)}}}
	uc := newTestUsecase(t, repo, lc, func(u *RecorderUsecase) {
		u.rec.AutoReconnect = false
	})

	uc.recordLoop(context.Background(), 42, &Session{RoomID: 42}, make(chan *DanmakuEvent))

	if repo.recordCalls != 1 {
		t.Fatalf("recordCalls = %d, want 1", repo.recordCalls)
	}
}

func TestRecordLoopCDNTransientUsesSeparateBudget(t *testing.T) {
	transient := fmt.Errorf("cdn 404: %w", ErrStreamTransient)
	repo := &fakeRepo{recordQueue: []recordOutcome{{result: &SessionResult{}, err: transient}}}
	lc := &fakeLiveClient{statusQueue: []statusOutcome{{info: liveInfo(42, true)}}}
	uc := newTestUsecase(t, repo, lc, func(u *RecorderUsecase) {
		u.rec.MaxReconnect = 0 // 常规重连预算置空：只有 CDN 预算生效
		u.rec.CDNTransientBudget = 2
	})

	uc.recordLoop(context.Background(), 42, &Session{RoomID: 42}, make(chan *DanmakuEvent))

	// 首次尝试 + 2 次 CDN 预算内重试。
	if repo.recordCalls != 3 {
		t.Fatalf("recordCalls = %d, want 3", repo.recordCalls)
	}
}

func TestRecordLoopOpenStreamFailureEndsSession(t *testing.T) {
	repo := &fakeRepo{}
	lc := &fakeLiveClient{openErrs: []error{fmt.Errorf("risk: %w", ErrRiskControl)}}
	uc := newTestUsecase(t, repo, lc, nil)

	uc.recordLoop(context.Background(), 42, &Session{RoomID: 42}, make(chan *DanmakuEvent))

	if repo.recordCalls != 0 {
		t.Fatalf("recordCalls = %d, want 0", repo.recordCalls)
	}
}

func TestRecordLoopProbeFailureEndsSession(t *testing.T) {
	repo := &fakeRepo{recordQueue: []recordOutcome{{result: &SessionResult{}, err: stderrors.New("reset")}}}
	lc := &fakeLiveClient{statusQueue: []statusOutcome{{err: stderrors.New("probe down")}}}
	uc := newTestUsecase(t, repo, lc, nil)

	uc.recordLoop(context.Background(), 42, &Session{RoomID: 42}, make(chan *DanmakuEvent))

	if repo.recordCalls != 1 {
		t.Fatalf("recordCalls = %d, want 1", repo.recordCalls)
	}
}

func TestRecordLoopContextCancelStopsImmediately(t *testing.T) {
	repo := &fakeRepo{recordQueue: []recordOutcome{{result: &SessionResult{}, err: context.Canceled}}}
	lc := &fakeLiveClient{statusQueue: []statusOutcome{{info: liveInfo(42, true)}}}
	uc := newTestUsecase(t, repo, lc, nil)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	uc.recordLoop(ctx, 42, &Session{RoomID: 42}, make(chan *DanmakuEvent))

	if repo.recordCalls != 1 {
		t.Fatalf("recordCalls = %d, want 1", repo.recordCalls)
	}
	if lc.statusCalls != 0 {
		t.Fatalf("statusCalls = %d, want 0 (no probe after cancel)", lc.statusCalls)
	}
}

func TestNewRecorderUsecaseNilConfig(t *testing.T) {
	reg, err := NewRoomRegistry(nil)
	if err != nil {
		t.Fatalf("NewRoomRegistry() error = %v", err)
	}
	uc := NewRecorderUsecase(nil, reg, &fakeRepo{}, &fakeLiveClient{})
	if !uc.rec.AutoReconnect || uc.rec.MaxReconnect != defaultMaxReconnect ||
		uc.rec.ReconnectDelay != defaultReconnectDelay ||
		uc.rec.CDNTransientBudget != defaultCDNTransientBudget ||
		uc.pollInterval != defaultRoomInfoPollInterval {
		t.Fatalf("defaults not applied: %+v / %s", uc.rec, uc.pollInterval)
	}
}

func TestNewRecorderUsecaseConfigOverrides(t *testing.T) {
	c := &conf.Recorder{
		FallbackPollInterval: durationpb.New(30 * time.Second),
		MaxConcurrent:        2,
		Reconnect: &conf.Recorder_ReconnectOptions{
			AutoReconnect:      proto.Bool(false),
			MaxReconnect:       7,
			ReconnectDelay:     durationpb.New(3 * time.Second),
			CdnTransientBudget: 9,
		},
	}
	roomRepo := &fakeRoomRepo{rooms: map[int64]*Room{
		1: {RoomID: 1, StreamerName: "a", RecordEnabled: true},
		2: {RoomID: 2, StreamerName: "b"},
	}}
	reg, err := NewRoomRegistry(roomRepo)
	if err != nil {
		t.Fatalf("NewRoomRegistry() error = %v", err)
	}
	uc := NewRecorderUsecase(c, reg, &fakeRepo{}, &fakeLiveClient{})
	if uc.pollInterval != 30*time.Second || uc.maxConcurrent != 2 {
		t.Fatalf("unexpected: poll=%s max=%d", uc.pollInterval, uc.maxConcurrent)
	}
	want := ReconnectPolicy{AutoReconnect: false, MaxReconnect: 7, ReconnectDelay: 3 * time.Second, CDNTransientBudget: 9}
	if uc.rec != want {
		t.Fatalf("rec = %+v, want %+v", uc.rec, want)
	}
	if uc.slots == nil || cap(uc.slots) != 2 {
		t.Fatalf("slots cap = %d, want 2", cap(uc.slots))
	}
}

func TestJitterDurationWithinBand(t *testing.T) {
	base := 600 * time.Second
	for range 100 {
		got := jitterDuration(base, pollJitterFraction)
		lo, hi := base-base/10, base+base/10
		if got < lo || got > hi {
			t.Fatalf("jitterDuration(%s) = %s outside [%s, %s]", base, got, lo, hi)
		}
	}
}

// fakeDanmakuConn 是 watchRoom 测试用的脚本化 DanmakuConn。
type fakeDanmakuConn struct {
	events           chan *DanmakuEvent
	roomStateUpdates chan *RoomInfo
	closed           chan struct{} // 非 nil 时，Close 会投递一个信号
}

func (c *fakeDanmakuConn) Events() <-chan *DanmakuEvent       { return c.events }
func (c *fakeDanmakuConn) RoomStateUpdates() <-chan *RoomInfo { return c.roomStateUpdates }
func (c *fakeDanmakuConn) Close() error {
	if c.closed != nil {
		select {
		case c.closed <- struct{}{}:
		default:
		}
	}
	return nil
}

// isClosed 报告连接是否已被关闭（消费式，仅用于轮询至首次为真）。
func (c *fakeDanmakuConn) isClosed() bool {
	if c.closed == nil {
		return false
	}
	select {
	case <-c.closed:
		return true
	default:
		return false
	}
}

// watchClient 是完全经由弹幕连接驱动的 LiveClient；状态探测默认恒报未
// 开播，pollInfo 非 nil 时固定返回该信息（回退轮询接线测试用），
// pollCalls 计数探测次数。
type watchClient struct {
	conn      DanmakuConn
	pollInfo  *RoomInfo
	pollCalls atomic.Int64
}

func (c *watchClient) GetRoomInfo(_ context.Context, roomID int64) (*RoomInfo, error) {
	c.pollCalls.Add(1)
	if c.pollInfo != nil {
		return c.pollInfo, nil
	}
	return &RoomInfo{RoomID: roomID}, nil
}

func (c *watchClient) OpenStream(_ context.Context, roomID int64) (*Stream, error) {
	return &Stream{URL: fmt.Sprintf("http://cdn/%d", roomID)}, nil
}

func (c *watchClient) DanmakuConn(context.Context, int64) (DanmakuConn, error) {
	return c.conn, nil
}

// pumpBlockRepo 使 RecordSession 阻塞到 context 取消，模拟一路永不
// 断开的直播流。
type pumpBlockRepo struct {
	finished []*Session
}

func (r *pumpBlockRepo) PrepareSession(context.Context, *Session) error { return nil }

func (r *pumpBlockRepo) RecordSession(ctx context.Context, _ *Session, _ *Stream, _ <-chan *DanmakuEvent) (*SessionResult, error) {
	<-ctx.Done()
	return nil, ctx.Err()
}

func (r *pumpBlockRepo) FinishSession(_ context.Context, session *Session) error {
	r.finished = append(r.finished, session)
	return nil
}

func (r *pumpBlockRepo) RecoverPending(context.Context) error { return nil }

func TestWatchRoomCancelsSessionOnOfflineControl(t *testing.T) {
	repo := &pumpBlockRepo{}
	conn := &fakeDanmakuConn{
		events:           make(chan *DanmakuEvent),
		roomStateUpdates: make(chan *RoomInfo),
	}
	uc := newTestUsecase(t, repo, &watchClient{conn: conn}, nil)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	watchDone := make(chan struct{})
	go func() {
		defer close(watchDone)
		if err := uc.watchRoom(ctx, make(chan struct{}, 1), 42); err != nil {
			t.Errorf("watchRoom: %v", err)
		}
	}()

	conn.roomStateUpdates <- liveInfo(42, true)
	if !waitRecordStatus(uc.registry, 42, RecordStatusRecording) {
		t.Fatal("session did not start recording after live control event")
	}

	conn.roomStateUpdates <- liveInfo(42, false)
	if !waitRecordStatus(uc.registry, 42, RecordStatusIdle) {
		t.Fatal("offline control event did not cancel the active session")
	}
	if len(repo.finished) != 1 {
		t.Fatalf("finished sessions = %d, want 1", len(repo.finished))
	}

	cancel()
	<-watchDone
}

// TestWatchRoomGatesSessionsOnRecordEnabled 验证监控与录制的分离：未配置
// 录制的房间照常接收房间状态事件（直播状态可见），但不开启会话；配置录
// 制后若仍在播则立即开录，再关闭录制则立即停止。
func TestWatchRoomGatesSessionsOnRecordEnabled(t *testing.T) {
	repo := &pumpBlockRepo{}
	conn := &fakeDanmakuConn{
		events:           make(chan *DanmakuEvent),
		roomStateUpdates: make(chan *RoomInfo),
	}
	// 房间 42 初始未配置录制。
	uc := newTestUsecaseWithRooms(t, map[int64]*Room{42: {RoomID: 42, StreamerName: "tester"}}, repo, &watchClient{conn: conn}, nil)
	roomChanged := make(chan struct{}, 1)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	watchDone := make(chan struct{})
	go func() {
		defer close(watchDone)
		if err := uc.watchRoom(ctx, roomChanged, 42); err != nil {
			t.Errorf("watchRoom: %v", err)
		}
	}()

	// 未配置录制的房间收到开播事件：直播状态更新，但不得开启会话。
	conn.roomStateUpdates <- liveInfo(42, true)
	waitFor(t, "live status applied", func() bool {
		return uc.registry.runtime(42).LiveStatus == LiveStatusOnAir
	})
	time.Sleep(50 * time.Millisecond)
	if got := uc.registry.runtime(42).RecordStatus; got != RecordStatusIdle {
		t.Fatalf("room with record_enabled=false started recording: record status = %v", got)
	}

	// 开启录制：仍在播，应立即开录。
	uc.registry.Update(Room{RoomID: 42, StreamerName: "tester", RecordEnabled: true})
	roomChanged <- struct{}{}
	if !waitRecordStatus(uc.registry, 42, RecordStatusRecording) {
		t.Fatal("session did not start after turning on recording for a live room")
	}

	// 关闭录制：立即优雅停止。
	uc.registry.Update(Room{RoomID: 42, StreamerName: "tester"})
	roomChanged <- struct{}{}
	if !waitRecordStatus(uc.registry, 42, RecordStatusIdle) {
		t.Fatal("turning off recording did not stop the active session")
	}
	if len(repo.finished) != 1 {
		t.Fatalf("finished sessions = %d, want 1", len(repo.finished))
	}

	// 关闭录制后的开播事件同样不得开启会话。
	conn.roomStateUpdates <- liveInfo(42, true)
	time.Sleep(50 * time.Millisecond)
	if got := uc.registry.runtime(42).RecordStatus; got != RecordStatusIdle {
		t.Fatalf("room with record_enabled=false started recording again: record status = %v", got)
	}

	cancel()
	<-watchDone
}

// TestWatchRoomFallbackPollStartsSession 验证回退轮询的端到端接线：弹幕
// 通道全程沉默，轮询定时器（经同一延迟旋钮模式压缩至毫秒级）触发房间信
// 息拉取，信息投递给会话策略后启动会话。
func TestWatchRoomFallbackPollStartsSession(t *testing.T) {
	repo := &pumpBlockRepo{}
	conn := &fakeDanmakuConn{
		events:           make(chan *DanmakuEvent),
		roomStateUpdates: make(chan *RoomInfo),
	}
	lc := &watchClient{conn: conn, pollInfo: liveInfo(42, true)}
	uc := newTestUsecase(t, repo, lc, func(u *RecorderUsecase) {
		u.pollInterval = 5 * time.Millisecond
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	watchDone := make(chan struct{})
	go func() {
		defer close(watchDone)
		if err := uc.watchRoom(ctx, make(chan struct{}, 1), 42); err != nil {
			t.Errorf("watchRoom: %v", err)
		}
	}()

	// 弹幕房间状态事件一个不发：会话只能由"定时器 → 拉取 → 策略"路径启动。
	if !waitRecordStatus(uc.registry, 42, RecordStatusRecording) {
		t.Fatal("fallback poll did not start the session")
	}
	if lc.pollCalls.Load() == 0 {
		t.Fatal("session started without any fallback poll call")
	}

	cancel()
	<-watchDone
}

// gatedFinishRepo 的 FinishSession 阻塞在 gate 上，模拟缓慢的转封装收尾，
// 让测试可以稳定命中"会话正在停止中"的窗口。
type gatedFinishRepo struct {
	gate     chan struct{}
	prepares atomic.Int64
}

func (r *gatedFinishRepo) PrepareSession(context.Context, *Session) error {
	r.prepares.Add(1)
	return nil
}

func (r *gatedFinishRepo) RecordSession(ctx context.Context, _ *Session, _ *Stream, _ <-chan *DanmakuEvent) (*SessionResult, error) {
	<-ctx.Done()
	return nil, ctx.Err()
}

func (r *gatedFinishRepo) FinishSession(ctx context.Context, _ *Session) error {
	select {
	case <-r.gate:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (r *gatedFinishRepo) RecoverPending(context.Context) error { return nil }

// TestWatchRoomEnableRecordingDuringStopResumesSession 验证竞态路径：关闭
// 录制触发的停止尚在收尾（转封装中）时又重新开启录制，收尾完成后若仍在播
// 应立即恢复录制。
func TestWatchRoomEnableRecordingDuringStopResumesSession(t *testing.T) {
	repo := &gatedFinishRepo{gate: make(chan struct{})}
	conn := &fakeDanmakuConn{
		events:           make(chan *DanmakuEvent),
		roomStateUpdates: make(chan *RoomInfo),
	}
	uc := newTestUsecase(t, repo, &watchClient{conn: conn}, nil)
	roomChanged := make(chan struct{}, 1)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	watchDone := make(chan struct{})
	go func() {
		defer close(watchDone)
		if err := uc.watchRoom(ctx, roomChanged, 42); err != nil {
			t.Errorf("watchRoom: %v", err)
		}
	}()

	conn.roomStateUpdates <- liveInfo(42, true)
	if !waitRecordStatus(uc.registry, 42, RecordStatusRecording) {
		t.Fatal("session did not start recording")
	}

	// 关闭录制：会话进入收尾并阻塞在转封装 gate 上。
	uc.registry.Update(Room{RoomID: 42, StreamerName: "tester"})
	roomChanged <- struct{}{}
	if !waitRecordStatus(uc.registry, 42, RecordStatusRemuxing) {
		t.Fatal("turning off recording did not drive the session into finishing")
	}

	// 收尾完成前重新开启录制。
	uc.registry.Update(Room{RoomID: 42, StreamerName: "tester", RecordEnabled: true})
	roomChanged <- struct{}{}

	// 放行收尾：仍在播，应立即恢复录制（第二个会话）。
	close(repo.gate)
	if !waitRecordStatus(uc.registry, 42, RecordStatusRecording) {
		t.Fatal("session did not resume after finishing completed while live")
	}
	if got := repo.prepares.Load(); got != 2 {
		t.Fatalf("prepares = %d, want 2", got)
	}

	cancel()
	<-watchDone
}

// connSignalingClient 记录每次弹幕连接的建立与关闭，供外部观察监控协程
// 的启停。
type connSignalingClient struct {
	mu    sync.Mutex
	conns []*fakeDanmakuConn
}

func (c *connSignalingClient) GetRoomInfo(_ context.Context, roomID int64) (*RoomInfo, error) {
	return &RoomInfo{RoomID: roomID}, nil
}

func (c *connSignalingClient) OpenStream(_ context.Context, roomID int64) (*Stream, error) {
	return &Stream{URL: fmt.Sprintf("http://cdn/%d", roomID)}, nil
}

func (c *connSignalingClient) DanmakuConn(context.Context, int64) (DanmakuConn, error) {
	conn := &fakeDanmakuConn{
		events:           make(chan *DanmakuEvent),
		roomStateUpdates: make(chan *RoomInfo),
		closed:           make(chan struct{}, 1),
	}
	c.mu.Lock()
	c.conns = append(c.conns, conn)
	c.mu.Unlock()
	return conn, nil
}

func (c *connSignalingClient) count() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.conns)
}

func (c *connSignalingClient) last() *fakeDanmakuConn {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.conns[len(c.conns)-1]
}

// TestRunReconcilesRoomAddAndRemove 验证监督循环：注册表新增房间立即开始
// 监控，删除房间立即停止监控，全程无需重启。
func TestRunReconcilesRoomAddAndRemove(t *testing.T) {
	reg, err := NewRoomRegistry(nil)
	if err != nil {
		t.Fatalf("NewRoomRegistry() error = %v", err)
	}
	client := &connSignalingClient{}
	uc := NewRecorderUsecase(&conf.Recorder{}, reg, &fakeRepo{}, client)
	uc.redialDelay = time.Millisecond

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	runDone := make(chan error, 1)
	go func() { runDone <- uc.Run(ctx) }()

	// 新建房间：监控应立即建连。
	reg.Add(Room{RoomID: 7, StreamerName: "n", RecordEnabled: true})
	waitFor(t, "monitor started for created room", func() bool { return client.count() == 1 })

	// 删除房间：监控应立即停止（弹幕连接被关闭）。
	reg.Remove(7)
	waitFor(t, "monitor stopped for deleted room", func() bool { return client.last().isClosed() })

	// 重新添加：应由新的监控协程接管（新连接）。
	reg.Add(Room{RoomID: 7, StreamerName: "n", RecordEnabled: true})
	waitFor(t, "monitor restarted for re-added room", func() bool { return client.count() == 2 })

	// 取消 Run：应在有限时间内排空并返回。
	reg.Remove(7)
	cancel()
	select {
	case err := <-runDone:
		if err != nil {
			t.Fatalf("Run() = %v, want nil", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("Run did not return after cancel")
	}
}
