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
)

// fakeRepo 为决策树测试模拟 RecorderRepo 行为。
type fakeRepo struct {
	prepareErr  error
	recordQueue []recordOutcome // 每次调用弹出一个；最后一项固定复用
	recordCalls int
	finished    []*RecordingSession
	finishErr   error
}

type recordOutcome struct {
	result *RecordingResult
	err    error
}

func (r *fakeRepo) PrepareSession(_ context.Context, _ *RecordingSession) error { return r.prepareErr }

func (r *fakeRepo) RecordSession(_ context.Context, session *RecordingSession, stream *LiveStream, _ <-chan *DanmakuEvent) (*RecordingResult, error) {
	if stream != nil && stream.Body != nil {
		stream.Body.Close()
	}
	r.recordCalls++
	if len(r.recordQueue) == 0 {
		return &RecordingResult{}, nil
	}
	out := r.recordQueue[0]
	if len(r.recordQueue) > 1 {
		r.recordQueue = r.recordQueue[1:]
	}
	_ = session
	return out.result, out.err
}

func (r *fakeRepo) FinishSession(_ context.Context, session *RecordingSession) error {
	r.finished = append(r.finished, session)
	return r.finishErr
}

func (r *fakeRepo) RecoverPending(context.Context) error { return nil }

// fakeLiveClient 模拟 LiveClient 行为。
type fakeLiveClient struct {
	statusQueue []statusOutcome // 每次调用弹出一个；最后一项固定复用
	statusCalls int
	openErrs    []error // 每次 OpenLiveStream 调用弹出一个
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

func (c *fakeLiveClient) OpenLiveStream(_ context.Context, roomID int64) (*LiveStream, error) {
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
	return &LiveStream{URL: fmt.Sprintf("http://cdn/%d", roomID)}, nil
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
	roomRepo := &fakeRoomRepo{rooms: rooms}
	reg, err := NewRoomRegistry(roomRepo)
	if err != nil {
		t.Fatalf("NewRoomRegistry() error = %v", err)
	}
	uc := NewRecorderUsecase(&conf.Recorder{}, reg, repo, lc)
	uc.rec.ReconnectDelay = time.Millisecond
	uc.cdnBackoffBase = time.Millisecond
	uc.redialDelay = time.Millisecond
	uc.offlineConfirmDelay = time.Millisecond
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

func TestRunRecordingLoopStopsWhenOffline(t *testing.T) {
	repo := &fakeRepo{recordQueue: []recordOutcome{{result: &RecordingResult{BytesWritten: 10}, err: stderrors.New("eof")}}}
	lc := &fakeLiveClient{statusQueue: []statusOutcome{{info: liveInfo(42, false)}}}
	uc := newTestUsecase(t, repo, lc, nil)

	uc.runRecordingLoop(context.Background(), 42, &RecordingSession{RoomID: 42}, make(chan *DanmakuEvent))

	if repo.recordCalls != 1 {
		t.Fatalf("recordCalls = %d, want 1", repo.recordCalls)
	}
	if lc.openCalls != 1 {
		t.Fatalf("openCalls = %d, want 1", lc.openCalls)
	}
}

func TestRunRecordingLoopReconnectsWhileLive(t *testing.T) {
	repo := &fakeRepo{recordQueue: []recordOutcome{
		{result: &RecordingResult{}, err: stderrors.New("reset")},
		{result: &RecordingResult{}, err: stderrors.New("eof")},
	}}
	lc := &fakeLiveClient{statusQueue: []statusOutcome{
		{info: liveInfo(42, true)},
		{info: liveInfo(42, false)},
	}}
	uc := newTestUsecase(t, repo, lc, nil)

	uc.runRecordingLoop(context.Background(), 42, &RecordingSession{RoomID: 42}, make(chan *DanmakuEvent))

	if repo.recordCalls != 2 {
		t.Fatalf("recordCalls = %d, want 2", repo.recordCalls)
	}
}

func TestRunRecordingLoopBudgetExhaustedKeepsContent(t *testing.T) {
	repo := &fakeRepo{recordQueue: []recordOutcome{{result: &RecordingResult{BytesWritten: 1}, err: stderrors.New("reset")}}}
	lc := &fakeLiveClient{statusQueue: []statusOutcome{{info: liveInfo(42, true)}}}
	uc := newTestUsecase(t, repo, lc, func(u *RecorderUsecase) {
		u.rec.MaxReconnect = 1
	})

	uc.runRecordingLoop(context.Background(), 42, &RecordingSession{RoomID: 42}, make(chan *DanmakuEvent))

	// 首次尝试 + 1 次重连，随后放弃并保留已录内容。
	if repo.recordCalls != 2 {
		t.Fatalf("recordCalls = %d, want 2", repo.recordCalls)
	}
}

func TestRunRecordingLoopAutoReconnectDisabled(t *testing.T) {
	repo := &fakeRepo{recordQueue: []recordOutcome{{result: &RecordingResult{}, err: stderrors.New("reset")}}}
	lc := &fakeLiveClient{statusQueue: []statusOutcome{{info: liveInfo(42, true)}}}
	uc := newTestUsecase(t, repo, lc, func(u *RecorderUsecase) {
		u.rec.AutoReconnect = false
	})

	uc.runRecordingLoop(context.Background(), 42, &RecordingSession{RoomID: 42}, make(chan *DanmakuEvent))

	if repo.recordCalls != 1 {
		t.Fatalf("recordCalls = %d, want 1", repo.recordCalls)
	}
}

func TestRunRecordingLoopCDNTransientUsesSeparateBudget(t *testing.T) {
	transient := fmt.Errorf("cdn 404: %w", ErrStreamTransient)
	repo := &fakeRepo{recordQueue: []recordOutcome{{result: &RecordingResult{}, err: transient}}}
	lc := &fakeLiveClient{statusQueue: []statusOutcome{{info: liveInfo(42, true)}}}
	uc := newTestUsecase(t, repo, lc, func(u *RecorderUsecase) {
		u.rec.MaxReconnect = 0 // 常规重连预算置空：只有 CDN 预算生效
		u.rec.CDNTransientBudget = 2
	})

	uc.runRecordingLoop(context.Background(), 42, &RecordingSession{RoomID: 42}, make(chan *DanmakuEvent))

	// 首次尝试 + 2 次 CDN 预算内重试。
	if repo.recordCalls != 3 {
		t.Fatalf("recordCalls = %d, want 3", repo.recordCalls)
	}
}

func TestRunRecordingLoopOpenLiveStreamFailureEndsSession(t *testing.T) {
	repo := &fakeRepo{}
	lc := &fakeLiveClient{openErrs: []error{fmt.Errorf("risk: %w", ErrRiskControl)}}
	uc := newTestUsecase(t, repo, lc, nil)

	uc.runRecordingLoop(context.Background(), 42, &RecordingSession{RoomID: 42}, make(chan *DanmakuEvent))

	if repo.recordCalls != 0 {
		t.Fatalf("recordCalls = %d, want 0", repo.recordCalls)
	}
	if lc.statusCalls != 0 {
		t.Fatalf("statusCalls = %d, want 0 (no probe on non-transient open error)", lc.statusCalls)
	}
	if got := uc.roomRegistry.runtime(42).LastError; got == "" {
		t.Fatalf("LastError is empty, want the open error recorded")
	}
}

func TestRunRecordingLoopOpenTransientOfflineEndsSessionQuietly(t *testing.T) {
	// 主播刚下播、CDN 已撤流：瞬时拉流失败 + 复查已下播 → 正常收尾，不记错误。
	transient := fmt.Errorf("stream http status 404: %w", ErrStreamTransient)
	repo := &fakeRepo{}
	lc := &fakeLiveClient{
		openErrs:    []error{transient},
		statusQueue: []statusOutcome{{info: liveInfo(42, false)}},
	}
	uc := newTestUsecase(t, repo, lc, nil)

	uc.runRecordingLoop(context.Background(), 42, &RecordingSession{RoomID: 42}, make(chan *DanmakuEvent))

	if repo.recordCalls != 0 {
		t.Fatalf("recordCalls = %d, want 0", repo.recordCalls)
	}
	if lc.openCalls != 1 {
		t.Fatalf("openCalls = %d, want 1 (no retry after offline probe)", lc.openCalls)
	}
	if got := uc.roomRegistry.runtime(42).LastError; got != "" {
		t.Fatalf("LastError = %q, want empty for a normal stream end", got)
	}
}

func TestRunRecordingLoopOpenTransientLiveRetriesWithinBudget(t *testing.T) {
	// 瞬时拉流失败但仍在播：按 CDN 瞬时预算退避重试，耗尽后保内容收尾。
	transient := fmt.Errorf("stream http status 404: %w", ErrStreamTransient)
	repo := &fakeRepo{}
	lc := &fakeLiveClient{
		openErrs:    []error{transient},
		statusQueue: []statusOutcome{{info: liveInfo(42, true)}},
	}
	uc := newTestUsecase(t, repo, lc, func(u *RecorderUsecase) {
		u.rec.CDNTransientBudget = 2
	})

	uc.runRecordingLoop(context.Background(), 42, &RecordingSession{RoomID: 42}, make(chan *DanmakuEvent))

	// 首次尝试 + 2 次预算内重试，每次失败后都复查房态。
	if repo.recordCalls != 0 {
		t.Fatalf("recordCalls = %d, want 0", repo.recordCalls)
	}
	if lc.openCalls != 3 {
		t.Fatalf("openCalls = %d, want 3", lc.openCalls)
	}
	if lc.statusCalls != 3 {
		t.Fatalf("statusCalls = %d, want 3 (probe after every transient open failure)", lc.statusCalls)
	}
}

func TestRunRecordingLoopOpenTransientProbeFailureEndsSession(t *testing.T) {
	// 瞬时拉流失败且复查也失败：记错误并结束场次。
	transient := fmt.Errorf("stream http status 404: %w", ErrStreamTransient)
	repo := &fakeRepo{}
	lc := &fakeLiveClient{
		openErrs:    []error{transient},
		statusQueue: []statusOutcome{{err: stderrors.New("probe down")}},
	}
	uc := newTestUsecase(t, repo, lc, nil)

	uc.runRecordingLoop(context.Background(), 42, &RecordingSession{RoomID: 42}, make(chan *DanmakuEvent))

	if repo.recordCalls != 0 {
		t.Fatalf("recordCalls = %d, want 0", repo.recordCalls)
	}
	if lc.openCalls != 1 {
		t.Fatalf("openCalls = %d, want 1", lc.openCalls)
	}
	if got := uc.roomRegistry.runtime(42).LastError; got == "" {
		t.Fatalf("LastError is empty, want the probe error recorded")
	}
}

func TestProbeLiveContextCanceledEndsQuietly(t *testing.T) {
	// 下播竞态：监控因下播事件取消会话的同时复查房态在途，
	// 取消引发的探测失败属正常结束路径，不记错误。
	repo := &fakeRepo{}
	lc := &fakeLiveClient{statusQueue: []statusOutcome{{err: context.Canceled}}}
	uc := newTestUsecase(t, repo, lc, nil)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	live, ok := uc.probeLive(ctx, 42)
	if live || ok {
		t.Fatalf("probeLive = (%v, %v), want (false, false)", live, ok)
	}
	if got := uc.roomRegistry.runtime(42).LastError; got != "" {
		t.Fatalf("LastError = %q, want empty for a ctx-canceled probe", got)
	}
}

func TestRunRecordingLoopProbeFailureEndsSession(t *testing.T) {
	repo := &fakeRepo{recordQueue: []recordOutcome{{result: &RecordingResult{}, err: stderrors.New("reset")}}}
	lc := &fakeLiveClient{statusQueue: []statusOutcome{{err: stderrors.New("probe down")}}}
	uc := newTestUsecase(t, repo, lc, nil)

	uc.runRecordingLoop(context.Background(), 42, &RecordingSession{RoomID: 42}, make(chan *DanmakuEvent))

	if repo.recordCalls != 1 {
		t.Fatalf("recordCalls = %d, want 1", repo.recordCalls)
	}
}

func TestRunRecordingLoopOfflineRequiresRepeatedConfirmation(t *testing.T) {
	// 首次探测说"未开播"但随后复活在播：单次下播结论不得结束场次，
	// 应继续重连；之后连续多次"未开播"才确认下播并收尾。
	repo := &fakeRepo{recordQueue: []recordOutcome{{result: &RecordingResult{}, err: stderrors.New("reset")}}}
	lc := &fakeLiveClient{statusQueue: []statusOutcome{
		{info: liveInfo(42, false)}, // 第一次确认的首轮：未开播
		{info: liveInfo(42, true)},  // 次轮复活在播 → 确认不成立，重连
		{info: liveInfo(42, false)}, // 第二次确认起粘滞未开播 → 三轮后确认
	}}
	uc := newTestUsecase(t, repo, lc, nil)

	uc.runRecordingLoop(context.Background(), 42, &RecordingSession{RoomID: 42}, make(chan *DanmakuEvent))

	// 若单次下播即结束，recordCalls 只会是 1。
	if repo.recordCalls != 2 {
		t.Fatalf("recordCalls = %d, want 2 (single offline probe must not end the session)", repo.recordCalls)
	}
}

// slowStableRepo 的每次泵送都睡眠一小段时间再产出内容，模拟"稳定录制
// 了一段时间"的腿，用于触发预算重置。
type slowStableRepo struct {
	sleep       time.Duration
	result      *RecordingResult
	err         error
	recordCalls int
}

func (r *slowStableRepo) PrepareSession(context.Context, *RecordingSession) error { return nil }

func (r *slowStableRepo) RecordSession(_ context.Context, _ *RecordingSession, stream *LiveStream, _ <-chan *DanmakuEvent) (*RecordingResult, error) {
	if stream != nil && stream.Body != nil {
		stream.Body.Close()
	}
	time.Sleep(r.sleep)
	r.recordCalls++
	return r.result, r.err
}

func (r *slowStableRepo) FinishSession(context.Context, *RecordingSession) error { return nil }
func (r *slowStableRepo) RecoverPending(context.Context) error                   { return nil }

func TestRunRecordingLoopStableRecordingResetsBudget(t *testing.T) {
	// 泵送稳定录制超过阈值后重置重连预算：长直播中的偶发断流不再累计
	// 到耗尽。MaxReconnect=1 下，不重置时第二次探测在播就会因预算耗尽
	// 结束（recordCalls=2）；重置后可持续到第三次探测确认下播（=3）。
	repo := &slowStableRepo{
		sleep:  3 * time.Millisecond,
		result: &RecordingResult{BytesWritten: 1024},
		err:    stderrors.New("reset"),
	}
	lc := &fakeLiveClient{statusQueue: []statusOutcome{
		{info: liveInfo(42, true)},
		{info: liveInfo(42, true)},
		{info: liveInfo(42, false)},
	}}
	uc := newTestUsecase(t, repo, lc, func(u *RecorderUsecase) {
		u.rec.MaxReconnect = 1
		u.stableResetAfter = time.Millisecond
	})

	uc.runRecordingLoop(context.Background(), 42, &RecordingSession{RoomID: 42}, make(chan *DanmakuEvent))

	if repo.recordCalls != 3 {
		t.Fatalf("recordCalls = %d, want 3 (stable legs must reset the reconnect budget)", repo.recordCalls)
	}
}

func TestProbeLiveSingleLiveProbeSuffices(t *testing.T) {
	lc := &fakeLiveClient{statusQueue: []statusOutcome{{info: liveInfo(42, true)}}}
	uc := newTestUsecase(t, &fakeRepo{}, lc, nil)

	live, ok := uc.probeLive(context.Background(), 42)
	if !live || !ok {
		t.Fatalf("probeLive = (%v, %v), want (true, true)", live, ok)
	}
	// "在播"单次探测即成立，不做多余确认。
	if lc.statusCalls != 1 {
		t.Fatalf("statusCalls = %d, want 1", lc.statusCalls)
	}
}

func TestProbeLiveExhaustsAttemptsOnPersistentFailure(t *testing.T) {
	// 探测持续失败：耗尽尝试次数后记错误并返回 (false, false)。
	lc := &fakeLiveClient{statusQueue: []statusOutcome{{err: stderrors.New("probe down")}}}
	uc := newTestUsecase(t, &fakeRepo{}, lc, nil)

	live, ok := uc.probeLive(context.Background(), 42)
	if live || ok {
		t.Fatalf("probeLive = (%v, %v), want (false, false)", live, ok)
	}
	if lc.statusCalls != probeMaxAttempts {
		t.Fatalf("statusCalls = %d, want %d", lc.statusCalls, probeMaxAttempts)
	}
	if got := uc.roomRegistry.runtime(42).LastError; got == "" {
		t.Fatalf("LastError is empty, want the probe error recorded")
	}
}

func TestRunRecordingLoopContextCancelStopsImmediately(t *testing.T) {
	repo := &fakeRepo{recordQueue: []recordOutcome{{result: &RecordingResult{}, err: context.Canceled}}}
	lc := &fakeLiveClient{statusQueue: []statusOutcome{{info: liveInfo(42, true)}}}
	uc := newTestUsecase(t, repo, lc, nil)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	uc.runRecordingLoop(ctx, 42, &RecordingSession{RoomID: 42}, make(chan *DanmakuEvent))

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

func TestNextPollDelayWithinBand(t *testing.T) {
	base := 600 * time.Second
	uc := &RecorderUsecase{pollInterval: base}
	for range 100 {
		got := uc.nextPollDelay()
		lo, hi := base-base/10, base+base/10
		if got < lo || got > hi {
			t.Fatalf("nextPollDelay() = %s outside [%s, %s]", got, lo, hi)
		}
	}
}

// fakeDanmakuConn 是 runMonitorConnection 测试用的脚本化 DanmakuConn。
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

func (c *watchClient) OpenLiveStream(_ context.Context, roomID int64) (*LiveStream, error) {
	return &LiveStream{URL: fmt.Sprintf("http://cdn/%d", roomID)}, nil
}

func (c *watchClient) DanmakuConn(context.Context, int64) (DanmakuConn, error) {
	return c.conn, nil
}

// pumpBlockRepo 使 RecordSession 阻塞到 context 取消，模拟一路永不
// 断开的直播流。
type pumpBlockRepo struct {
	finished []*RecordingSession
}

func (r *pumpBlockRepo) PrepareSession(context.Context, *RecordingSession) error { return nil }

func (r *pumpBlockRepo) RecordSession(ctx context.Context, _ *RecordingSession, _ *LiveStream, _ <-chan *DanmakuEvent) (*RecordingResult, error) {
	<-ctx.Done()
	return nil, ctx.Err()
}

func (r *pumpBlockRepo) FinishSession(_ context.Context, session *RecordingSession) error {
	r.finished = append(r.finished, session)
	return nil
}

func (r *pumpBlockRepo) RecoverPending(context.Context) error { return nil }

func TestRunMonitorConnectionCancelsSessionOnOfflineControl(t *testing.T) {
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
		if err := uc.runMonitorConnection(ctx, make(chan struct{}, 1), 42); err != nil {
			t.Errorf("runMonitorConnection: %v", err)
		}
	}()

	conn.roomStateUpdates <- liveInfo(42, true)
	if !waitRecordStatus(uc.roomRegistry, 42, RecordStatusRecording) {
		t.Fatal("session did not start recording after live control event")
	}

	conn.roomStateUpdates <- liveInfo(42, false)
	if !waitRecordStatus(uc.roomRegistry, 42, RecordStatusIdle) {
		t.Fatal("offline control event did not cancel the active session")
	}
	if len(repo.finished) != 1 {
		t.Fatalf("finished sessions = %d, want 1", len(repo.finished))
	}

	cancel()
	<-watchDone
}

// TestRunMonitorConnectionGatesSessionsOnRecordEnabled 验证监控与录制的分离：未配置
// 录制的房间照常接收房间状态事件（直播状态可见），但不开启会话；配置录
// 制后若仍在播则立即开录，再关闭录制则立即停止。
func TestRunMonitorConnectionGatesSessionsOnRecordEnabled(t *testing.T) {
	repo := &pumpBlockRepo{}
	conn := &fakeDanmakuConn{
		events:           make(chan *DanmakuEvent),
		roomStateUpdates: make(chan *RoomInfo),
	}
	// 房间 42 初始未配置录制。
	uc := newTestUsecaseWithRooms(t, map[int64]*Room{42: {RoomID: 42, StreamerName: "tester"}}, repo, &watchClient{conn: conn}, nil)
	reevaluate := make(chan struct{}, 1)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	watchDone := make(chan struct{})
	go func() {
		defer close(watchDone)
		if err := uc.runMonitorConnection(ctx, reevaluate, 42); err != nil {
			t.Errorf("runMonitorConnection: %v", err)
		}
	}()

	// 未配置录制的房间收到开播事件：直播状态更新，但不得开启会话。
	conn.roomStateUpdates <- liveInfo(42, true)
	waitFor(t, "live status applied", func() bool {
		return uc.roomRegistry.runtime(42).LiveStatus == LiveStatusOnAir
	})
	time.Sleep(50 * time.Millisecond)
	if got := uc.roomRegistry.runtime(42).RecordStatus; got != RecordStatusIdle {
		t.Fatalf("room with record_enabled=false started recording: record status = %v", got)
	}

	// 开启录制：仍在播，应立即开录。
	uc.roomRegistry.Update(Room{RoomID: 42, StreamerName: "tester", RecordEnabled: true})
	reevaluate <- struct{}{}
	if !waitRecordStatus(uc.roomRegistry, 42, RecordStatusRecording) {
		t.Fatal("session did not start after turning on recording for a live room")
	}

	// 关闭录制：立即优雅停止。
	uc.roomRegistry.Update(Room{RoomID: 42, StreamerName: "tester"})
	reevaluate <- struct{}{}
	if !waitRecordStatus(uc.roomRegistry, 42, RecordStatusIdle) {
		t.Fatal("turning off recording did not stop the active session")
	}
	if len(repo.finished) != 1 {
		t.Fatalf("finished sessions = %d, want 1", len(repo.finished))
	}

	// 关闭录制后的开播事件同样不得开启会话。
	conn.roomStateUpdates <- liveInfo(42, true)
	time.Sleep(50 * time.Millisecond)
	if got := uc.roomRegistry.runtime(42).RecordStatus; got != RecordStatusIdle {
		t.Fatalf("room with record_enabled=false started recording again: record status = %v", got)
	}

	cancel()
	<-watchDone
}

// TestRunMonitorConnectionFallbackPollStartsSession 验证回退轮询的端到端接线：弹幕
// 通道全程沉默，轮询定时器（经同一延迟旋钮模式压缩至毫秒级）触发房间信
// 息拉取，信息投递给会话策略后启动会话。
func TestRunMonitorConnectionFallbackPollStartsSession(t *testing.T) {
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
		if err := uc.runMonitorConnection(ctx, make(chan struct{}, 1), 42); err != nil {
			t.Errorf("runMonitorConnection: %v", err)
		}
	}()

	// 弹幕房间状态事件一个不发：会话只能由"定时器 → 拉取 → 策略"路径启动。
	if !waitRecordStatus(uc.roomRegistry, 42, RecordStatusRecording) {
		t.Fatal("fallback poll did not start the session")
	}
	if lc.pollCalls.Load() == 0 {
		t.Fatal("session started without any fallback poll call")
	}

	cancel()
	<-watchDone
}

// gatedFinishRepo 的 FinishSession 阻塞在 gate 上，模拟缓慢的合并收尾，
// 让测试可以稳定命中"会话正在停止中"的窗口。
type gatedFinishRepo struct {
	gate     chan struct{}
	prepares atomic.Int64
}

func (r *gatedFinishRepo) PrepareSession(context.Context, *RecordingSession) error {
	r.prepares.Add(1)
	return nil
}

func (r *gatedFinishRepo) RecordSession(ctx context.Context, _ *RecordingSession, _ *LiveStream, _ <-chan *DanmakuEvent) (*RecordingResult, error) {
	<-ctx.Done()
	return nil, ctx.Err()
}

func (r *gatedFinishRepo) FinishSession(ctx context.Context, _ *RecordingSession) error {
	select {
	case <-r.gate:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (r *gatedFinishRepo) RecoverPending(context.Context) error { return nil }

// TestRunMonitorConnectionEnableRecordingDuringStopResumesSession 验证竞态路径：关闭
// 录制触发的停止尚在收尾（合并中）时又重新开启录制，收尾完成后若仍在播
// 应立即恢复录制。
func TestRunMonitorConnectionEnableRecordingDuringStopResumesSession(t *testing.T) {
	repo := &gatedFinishRepo{gate: make(chan struct{})}
	conn := &fakeDanmakuConn{
		events:           make(chan *DanmakuEvent),
		roomStateUpdates: make(chan *RoomInfo),
	}
	uc := newTestUsecase(t, repo, &watchClient{conn: conn}, nil)
	reevaluate := make(chan struct{}, 1)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	watchDone := make(chan struct{})
	go func() {
		defer close(watchDone)
		if err := uc.runMonitorConnection(ctx, reevaluate, 42); err != nil {
			t.Errorf("runMonitorConnection: %v", err)
		}
	}()

	conn.roomStateUpdates <- liveInfo(42, true)
	if !waitRecordStatus(uc.roomRegistry, 42, RecordStatusRecording) {
		t.Fatal("session did not start recording")
	}

	// 关闭录制：会话进入收尾并阻塞在合并 gate 上。
	uc.roomRegistry.Update(Room{RoomID: 42, StreamerName: "tester"})
	reevaluate <- struct{}{}
	if !waitRecordStatus(uc.roomRegistry, 42, RecordStatusMerging) {
		t.Fatal("turning off recording did not drive the session into finishing")
	}

	// 收尾完成前重新开启录制。
	uc.roomRegistry.Update(Room{RoomID: 42, StreamerName: "tester", RecordEnabled: true})
	reevaluate <- struct{}{}

	// 放行收尾：仍在播，应立即恢复录制（第二个会话）。
	close(repo.gate)
	if !waitRecordStatus(uc.roomRegistry, 42, RecordStatusRecording) {
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

func (c *connSignalingClient) OpenLiveStream(_ context.Context, roomID int64) (*LiveStream, error) {
	return &LiveStream{URL: fmt.Sprintf("http://cdn/%d", roomID)}, nil
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
