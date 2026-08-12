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

// Typed errors surfaced through the API error reason enum.
var (
	// ErrRoomInternal is returned when the recorder fails internally.
	ErrRoomInternal = errors.InternalServer(v1.ErrorReason_ROOM_INTERNAL.String(), "recorder internal error")
)

// Sentinel errors used to classify stream interruptions. Declared here so
// the decision tree (biz) decides; data wraps them where they originate.
var (
	// ErrStreamTransient marks CDN-transient failures (HTTP 404, connection
	// reset) that are worth retrying with a freshly selected stream URL.
	ErrStreamTransient = stderrors.New("recorder: transient stream error")
	// ErrRiskControl marks Bilibili risk-control rejections (-352/412/...).
	ErrRiskControl = stderrors.New("recorder: risk control triggered")
)

// Recorder defaults. Proto scalars cannot distinguish unset from zero, so
// zero values are replaced here (same trick as service.defaultPageSize).
const (
	defaultFallbackPollInterval = 600 * time.Second
	defaultMaxReconnect         = 3
	defaultReconnectDelay       = 10 * time.Second
	defaultCDNTransientBudget   = 5
	defaultCDNBackoffBase       = 2 * time.Second
	cdnBackoffMax               = 60 * time.Second
	// monitorRedialDelay is the pause before re-dialing the danmaku conn.
	monitorRedialDelay = 10 * time.Second
	// finishGracePeriod bounds FinishSession work detached from the
	// cancelled run context during shutdown.
	finishGracePeriod = 30 * time.Second
	// pollJitterFraction is the relative jitter applied to the fallback
	// poll interval (interval +/- fraction/2).
	pollJitterFraction = 5 // => +/- 10%
)

// Danmaku event types recorded to JSONL.
const (
	EventDanmaku     = "danmaku"
	EventGift        = "gift"
	EventSuperChat   = "superchat"
	EventGuard       = "guard"
	EventEntryEffect = "entry_effect"
	EventInteract    = "interact_word"
)

// RoomInfo is live-room metadata reported by the platform.
type RoomInfo struct {
	RoomID        int64
	Live          bool
	Title         string
	StreamerName  string
	LiveStartTime time.Time
}

// StreamQuality describes the stream quality selected for a session.
type StreamQuality struct {
	Qn   int32
	Desc string
}

// StreamHandle is an open live stream produced by LiveClient.OpenStream.
// It is opaque to biz: produced by the platform seam, consumed by the
// storage seam, never inspected in between (the *sql.Rows pattern).
type StreamHandle struct {
	URL     string
	Quality StreamQuality
	Body    io.ReadCloser
}

// DanmakuEvent is one filtered danmaku-room event. Field relevance depends
// on Type; the storage seam decides the on-disk JSON shape.
type DanmakuEvent struct {
	Ts       time.Time
	Type     string
	UID      int64
	Uname    string
	Text     string // danmaku text / superchat text / entry-effect text
	Color    int32  // danmaku
	Mode     int32  // danmaku
	GiftName string // gift
	Num      int32  // gift/guard count
	Price    int64  // gift price (gold-cash units) / superchat price
	CoinType string // gift: gold/silver
	Duration int32  // superchat retention seconds
	Level    int32  // guard level
	Raw      []byte // original JSON payload
}

// Session is one recording session (one broadcast of one room).
type Session struct {
	RoomID        int64
	RoomName      string
	Title         string
	LiveStartTime time.Time
	Quality       StreamQuality
}

// SessionResult reports how a RecordSession pump run ended.
type SessionResult struct {
	BytesWritten int64
	Parts        int
}

// SessionStats is the in-flight write progress of a room's active session.
type SessionStats struct {
	CurrentFile  string
	BytesWritten int64
}

// DanmakuConn is a resident danmaku websocket for one room, used both for
// live detection (Control) and danmaku recording (Events). Implementations
// reconnect internally; after every reconnect they re-probe the room state
// and re-emit it on Control so missed LIVE/PREPARING events are covered.
// Events uses a bounded buffer and drops events when nobody is reading.
type DanmakuConn interface {
	Events() <-chan *DanmakuEvent
	Control() <-chan *RoomInfo
	Close() error
}

// LiveClient is the external-platform seam: all Bilibili traffic.
type LiveClient interface {
	RoomStatus(ctx context.Context, roomID int64) (*RoomInfo, error)
	OpenStream(ctx context.Context, roomID int64) (*StreamHandle, error)
	DanmakuConn(ctx context.Context, roomID int64) (DanmakuConn, error)
}

// RecorderRepo is the storage seam: recording layout, files, and remux.
type RecorderRepo interface {
	// PrepareSession creates (or re-locates after a restart) the session
	// directory and meta.json for the session's room + live start time.
	PrepareSession(ctx context.Context, session *Session) error
	// RecordSession pumps the stream to disk (splitting segments as
	// configured) and writes events to the matching JSONL files until the
	// stream ends or ctx is cancelled.
	RecordSession(ctx context.Context, session *Session, stream *StreamHandle, events <-chan *DanmakuEvent) (*SessionResult, error)
	// FinishSession finalizes meta.json and remuxes recorded segments.
	FinishSession(ctx context.Context, session *Session) error
	// RecoverPending finishes remux work left over from a previous run.
	RecoverPending(ctx context.Context) error
}

// ReconnectPolicy is the flattened reconnect configuration used by the
// stream-drop decision tree.
type ReconnectPolicy struct {
	AutoReconnect      bool
	MaxReconnect       int
	ReconnectDelay     time.Duration
	CDNTransientBudget int
}

// RecorderUsecase orchestrates room monitoring, session lifecycles, and the
// stream-drop decision tree. It makes decisions only; LiveClient performs
// all platform IO and RecorderRepo performs all storage IO. Room
// configuration and live/record state live in the shared RoomRegistry.
type RecorderUsecase struct {
	reg  *RoomRegistry
	repo RecorderRepo
	lc   LiveClient

	pollInterval  time.Duration
	maxConcurrent int
	rec           ReconnectPolicy

	// cdnBackoffBase is the first CDN-transient retry delay; tests shrink it.
	cdnBackoffBase time.Duration
	// redialDelay pauses monitor redials; tests shrink it.
	redialDelay time.Duration

	slots chan struct{}
}

type sessionHandle struct {
	cancel context.CancelFunc
	done   chan struct{}
}

// NewRecorderUsecase new a Recorder usecase.
func NewRecorderUsecase(c *conf.Recorder, reg *RoomRegistry, repo RecorderRepo, lc LiveClient) *RecorderUsecase {
	uc := &RecorderUsecase{
		reg:          reg,
		repo:         repo,
		lc:           lc,
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

// Run blocks until ctx is cancelled, monitoring every enabled room. It is
// the recorder daemon's main loop (invoked by the recorderJob server).
func (uc *RecorderUsecase) Run(ctx context.Context) error {
	rooms := uc.reg.Rooms()
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

// monitorRoom keeps a danmaku connection alive for the room and redials it
// until ctx is cancelled.
func (uc *RecorderUsecase) monitorRoom(ctx context.Context, roomID int64) {
	for ctx.Err() == nil {
		if err := uc.watchRoom(ctx, roomID); err != nil && ctx.Err() == nil {
			log.Error("room monitor failed", "room", roomID, "err", err)
			uc.reg.NoteError(roomID, err)
		}
		if sleepCtx(ctx, uc.redialDelay) != nil {
			return
		}
	}
}

// watchRoom holds one danmaku connection, translating control events into
// session starts/finishes and running the fallback poller. Events are
// drained (discarded) while no session is active; the active session's
// RecordSession consumes them directly.
func (uc *RecorderUsecase) watchRoom(ctx context.Context, roomID int64) error {
	conn, err := uc.lc.DanmakuConn(ctx, roomID)
	if err != nil {
		return fmt.Errorf("open danmaku conn: %w", err)
	}
	defer conn.Close()

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
			// no active session: discard
		case <-done:
			active = nil
		case info := <-conn.Control():
			uc.reg.ApplyRoomInfo(ctx, roomID, info)
			if info.Live {
				if active == nil {
					active = uc.launchSession(ctx, roomID, info, conn.Events())
				}
			} else if active != nil {
				active.cancel()
			}
		case <-poll.C:
			info, err := uc.lc.RoomStatus(ctx, roomID)
			if err != nil {
				log.Warn("fallback poll failed", "room", roomID, "err", err)
				uc.reg.NoteError(roomID, err)
			} else {
				uc.reg.ApplyRoomInfo(ctx, roomID, info)
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

// launchSession starts the session goroutine that owns the full record
// loop, FinishSession, and slot release.
func (uc *RecorderUsecase) launchSession(ctx context.Context, roomID int64, info *RoomInfo, events <-chan *DanmakuEvent) *sessionHandle {
	sctx, cancel := context.WithCancel(ctx)
	h := &sessionHandle{cancel: cancel, done: make(chan struct{})}
	go func() {
		defer close(h.done)
		uc.runSession(sctx, roomID, info, events)
	}()
	return h
}

// runSession owns one session end to end: slot, prepare, record loop,
// finish/remux.
func (uc *RecorderUsecase) runSession(ctx context.Context, roomID int64, info *RoomInfo, events <-chan *DanmakuEvent) {
	if err := uc.acquireSlot(ctx, roomID); err != nil {
		return
	}
	defer uc.releaseSlot()

	room := uc.reg.Room(roomID)
	session := &Session{
		RoomID:        roomID,
		RoomName:      firstNonEmpty(room.Name, info.StreamerName, fmt.Sprintf("%d", roomID)),
		Title:         info.Title,
		LiveStartTime: info.LiveStartTime,
	}
	uc.reg.StartRecording(roomID)

	if err := uc.repo.PrepareSession(ctx, session); err != nil {
		log.Error("prepare session failed", "room", roomID, "err", err)
		uc.reg.FailRecording(roomID, err)
		return
	}

	uc.recordLoop(ctx, roomID, session, events)

	// Finish detached from the (possibly cancelled) run context so the
	// remux marking still lands during shutdown; leftovers are picked up
	// by RecoverPending on the next start.
	uc.reg.SetRemuxing(roomID)
	fctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), finishGracePeriod)
	defer cancel()
	if err := uc.repo.FinishSession(fctx, session); err != nil {
		log.Error("finish session failed", "room", roomID, "err", err)
		uc.reg.FailRecording(roomID, err)
		return
	}
	uc.reg.FinishRecording(roomID)
}

// recordLoop is the stream-drop decision tree: pump until the connection
// ends, re-probe live status, and either reconnect (new part) or finish
// the session keeping whatever was recorded.
func (uc *RecorderUsecase) recordLoop(ctx context.Context, roomID int64, session *Session, events <-chan *DanmakuEvent) {
	reconnects := 0
	cdnBudget := uc.rec.CDNTransientBudget
	cdnAttempt := 0
	for {
		stream, err := uc.lc.OpenStream(ctx, roomID)
		if err != nil {
			log.Error("open stream failed", "room", roomID, "err", err)
			uc.reg.NoteError(roomID, err)
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

		info, err := uc.lc.RoomStatus(ctx, roomID)
		if err != nil {
			log.Error("probe live status failed, ending session", "room", roomID, "err", err)
			uc.reg.NoteError(roomID, err)
			return
		}
		uc.reg.ApplyRoomInfo(ctx, roomID, info)
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

// sleepCtx sleeps for d or until ctx is done.
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

// jitterDuration returns d jittered by +/- 1/(2*div) of d.
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
