package biz

import (
	"context"
	stderrors "errors"
	"fmt"
	"testing"
	"time"

	"suika/internal/conf"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/durationpb"
)

// fakeRepo scripts RecorderRepo behavior for decision-tree tests.
type fakeRepo struct {
	prepareErr  error
	recordQueue []recordOutcome // popped per call; last entry is sticky
	recordCalls int
	finished    []*Session
	finishErr   error
}

type recordOutcome struct {
	result *SessionResult
	err    error
}

func (r *fakeRepo) PrepareSession(_ context.Context, _ *Session) error { return r.prepareErr }

func (r *fakeRepo) RecordSession(_ context.Context, session *Session, stream *StreamHandle, _ <-chan *DanmakuEvent) (*SessionResult, error) {
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

// fakeLiveClient scripts LiveClient behavior.
type fakeLiveClient struct {
	statusQueue []statusOutcome // popped per call; last entry is sticky
	statusCalls int
	openErrs    []error // popped per OpenStream call
	openCalls   int
}

type statusOutcome struct {
	info *RoomInfo
	err  error
}

func (c *fakeLiveClient) RoomStatus(_ context.Context, roomID int64) (*RoomInfo, error) {
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

func (c *fakeLiveClient) OpenStream(_ context.Context, roomID int64) (*StreamHandle, error) {
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
	return &StreamHandle{URL: fmt.Sprintf("http://cdn/%d", roomID)}, nil
}

func (c *fakeLiveClient) DanmakuConn(context.Context, int64) (DanmakuConn, error) {
	return nil, stderrors.New("not used in decision tree tests")
}

func newTestUsecase(t *testing.T, repo RecorderRepo, lc LiveClient, mutate func(*RecorderUsecase)) *RecorderUsecase {
	t.Helper()
	c := &conf.Recorder{
		Reconnect: &conf.Recorder_ReconnectOptions{
			ReconnectDelay: durationpb.New(time.Millisecond),
		},
	}
	roomRepo := &fakeRoomRepo{rooms: map[int64]*Room{42: {RoomID: 42, Name: "tester", Enabled: true}}}
	reg, err := NewRoomRegistry(roomRepo)
	if err != nil {
		t.Fatalf("NewRoomRegistry() error = %v", err)
	}
	uc := NewRecorderUsecase(c, reg, repo, lc)
	uc.cdnBackoffBase = time.Millisecond
	if mutate != nil {
		mutate(uc)
	}
	return uc
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

	// initial attempt + 1 reconnect, then give up keeping content.
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
		u.rec.MaxReconnect = 0 // ordinary budget empty: only the CDN budget applies
		u.rec.CDNTransientBudget = 2
	})

	uc.recordLoop(context.Background(), 42, &Session{RoomID: 42}, make(chan *DanmakuEvent))

	// initial attempt + 2 CDN-budget retries.
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
		uc.pollInterval != defaultFallbackPollInterval {
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
		1: {RoomID: 1, Name: "a", Enabled: true},
		2: {RoomID: 2, Name: "b"},
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

// fakeDanmakuConn is a scripted DanmakuConn for watchRoom tests.
type fakeDanmakuConn struct {
	events  chan *DanmakuEvent
	control chan *RoomInfo
}

func (c *fakeDanmakuConn) Events() <-chan *DanmakuEvent { return c.events }
func (c *fakeDanmakuConn) Control() <-chan *RoomInfo    { return c.control }
func (c *fakeDanmakuConn) Close() error                 { return nil }

// watchClient is a LiveClient driven entirely through its danmaku conn;
// status probes report offline so only control events steer the test.
type watchClient struct{ conn DanmakuConn }

func (c *watchClient) RoomStatus(_ context.Context, roomID int64) (*RoomInfo, error) {
	return &RoomInfo{RoomID: roomID}, nil
}

func (c *watchClient) OpenStream(_ context.Context, roomID int64) (*StreamHandle, error) {
	return &StreamHandle{URL: fmt.Sprintf("http://cdn/%d", roomID)}, nil
}

func (c *watchClient) DanmakuConn(context.Context, int64) (DanmakuConn, error) {
	return c.conn, nil
}

// pumpBlockRepo blocks RecordSession until its context is cancelled, like
// a pump attached to a live stream that never drops on its own.
type pumpBlockRepo struct {
	finished []*Session
}

func (r *pumpBlockRepo) PrepareSession(context.Context, *Session) error { return nil }

func (r *pumpBlockRepo) RecordSession(ctx context.Context, _ *Session, _ *StreamHandle, _ <-chan *DanmakuEvent) (*SessionResult, error) {
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
		events:  make(chan *DanmakuEvent),
		control: make(chan *RoomInfo),
	}
	uc := newTestUsecase(t, repo, &watchClient{conn: conn}, nil)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	watchDone := make(chan struct{})
	go func() {
		defer close(watchDone)
		if err := uc.watchRoom(ctx, 42); err != nil {
			t.Errorf("watchRoom: %v", err)
		}
	}()

	// waitRecord polls the registry until room 42 reaches the wanted
	// record state, or the deadline passes.
	waitRecord := func(want RecordState) bool {
		deadline := time.Now().Add(3 * time.Second)
		for time.Now().Before(deadline) {
			var got RecordState
			var ok bool
			uc.reg.mu.Lock()
			st, found := uc.reg.states[42]
			if found {
				got, ok = st.record, true
			}
			uc.reg.mu.Unlock()
			if ok && got == want {
				return true
			}
			time.Sleep(5 * time.Millisecond)
		}
		return false
	}

	conn.control <- liveInfo(42, true)
	if !waitRecord(RecordRecording) {
		t.Fatal("session did not start recording after live control event")
	}

	conn.control <- liveInfo(42, false)
	if !waitRecord(RecordIdle) {
		t.Fatal("offline control event did not cancel the active session")
	}
	if len(repo.finished) != 1 {
		t.Fatalf("finished sessions = %d, want 1", len(repo.finished))
	}

	cancel()
	<-watchDone
}
