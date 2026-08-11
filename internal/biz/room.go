package biz

import (
	"context"
	"sync"
	"time"

	"suika/internal/conf"

	"github.com/go-kratos/kratos/v3/log"
)

// LiveState is the broadcast state of a room known to the recorder.
type LiveState int

// LiveState values.
const (
	LiveUnknown LiveState = iota
	LivePreparing
	LiveOnAir
)

// RecordState is the recorder's own state for a room.
type RecordState int

// RecordState values.
const (
	RecordIdle RecordState = iota
	RecordRecording
	RecordRemuxing
	RecordError
)

// Room is one monitored live room (sourced from configuration).
type Room struct {
	RoomID  int64
	Name    string
	Enabled bool
}

// RoomRuntime is the status-API view of one room: configuration, live
// state, record state, and in-flight write progress.
type RoomRuntime struct {
	Room             Room
	Live             LiveState
	Record           RecordState
	CurrentFile      string
	BytesWritten     int64
	SessionStartedAt time.Time
	LastError        string
}

// roomState is the mutable runtime state of one registered room.
type roomState struct {
	room             Room
	live             LiveState
	record           RecordState
	sessionStartedAt time.Time
	lastError        string
}

// RoomRegistry holds the configured room list and its runtime state. The
// recorder daemon reads room configuration and writes live/record state;
// the room API only reads full snapshots.
type RoomRegistry struct {
	mu     sync.Mutex
	rooms  []Room
	states map[int64]*roomState
}

// NewRoomRegistry parses the configured room list into a registry. A nil
// configuration is tolerated and yields an empty registry.
func NewRoomRegistry(c *conf.Recorder) *RoomRegistry {
	reg := &RoomRegistry{
		states: make(map[int64]*roomState),
	}
	if c == nil {
		log.Warn("recorder configuration missing, registering zero rooms")
		return reg
	}
	for _, r := range c.GetRooms() {
		room := Room{RoomID: r.GetRoomId(), Name: r.GetName(), Enabled: r.GetEnabled()}
		reg.rooms = append(reg.rooms, room)
		reg.states[room.RoomID] = &roomState{room: room}
	}
	return reg
}

// Rooms returns a snapshot of every configured room, in configuration
// order (no streamer-name backfill).
func (reg *RoomRegistry) Rooms() []Room {
	reg.mu.Lock()
	defer reg.mu.Unlock()
	out := make([]Room, len(reg.rooms))
	copy(out, reg.rooms)
	return out
}

// Room returns the current configuration snapshot of one room, including
// any streamer-name backfill. Unknown room IDs fall back to a bare room.
func (reg *RoomRegistry) Room(roomID int64) Room {
	reg.mu.Lock()
	defer reg.mu.Unlock()
	if st, ok := reg.states[roomID]; ok {
		return st.room
	}
	return Room{RoomID: roomID}
}

// Snapshot returns the runtime view of every configured room, in
// configuration order.
func (reg *RoomRegistry) Snapshot() []*RoomRuntime {
	reg.mu.Lock()
	defer reg.mu.Unlock()
	out := make([]*RoomRuntime, 0, len(reg.rooms))
	for _, room := range reg.rooms {
		st := reg.states[room.RoomID]
		out = append(out, &RoomRuntime{
			Room:             st.room,
			Live:             st.live,
			Record:           st.record,
			SessionStartedAt: st.sessionStartedAt,
			LastError:        st.lastError,
		})
	}
	return out
}

// ApplyRoomInfo records the platform-reported live state of a room and
// backfills the room name from the streamer name when unset.
func (reg *RoomRegistry) ApplyRoomInfo(roomID int64, info *RoomInfo) {
	if info == nil {
		return
	}
	reg.mu.Lock()
	defer reg.mu.Unlock()
	st, ok := reg.states[roomID]
	if !ok {
		return
	}
	if info.Live {
		st.live = LiveOnAir
	} else {
		st.live = LivePreparing
	}
	if st.room.Name == "" && info.StreamerName != "" {
		st.room.Name = info.StreamerName
	}
}

// StartRecording marks a room as actively recording a fresh session.
func (reg *RoomRegistry) StartRecording(roomID int64) {
	reg.setState(roomID, func(st *roomState) {
		st.record = RecordRecording
		st.sessionStartedAt = time.Now()
		st.lastError = ""
	})
}

// SetRemuxing marks a room's session as finishing (remux in progress).
func (reg *RoomRegistry) SetRemuxing(roomID int64) {
	reg.setState(roomID, func(st *roomState) { st.record = RecordRemuxing })
}

// FailRecording marks a room's session as failed with the given error.
func (reg *RoomRegistry) FailRecording(roomID int64, err error) {
	reg.setState(roomID, func(st *roomState) {
		st.record = RecordError
		st.lastError = err.Error()
	})
}

// FinishRecording marks a room idle after its session finalized.
func (reg *RoomRegistry) FinishRecording(roomID int64) {
	reg.setState(roomID, func(st *roomState) {
		st.record = RecordIdle
		st.sessionStartedAt = time.Time{}
	})
}

// NoteError records a monitor/session error against a room without
// changing its record state.
func (reg *RoomRegistry) NoteError(roomID int64, err error) {
	reg.setState(roomID, func(st *roomState) { st.lastError = err.Error() })
}

func (reg *RoomRegistry) setState(roomID int64, fn func(*roomState)) {
	reg.mu.Lock()
	defer reg.mu.Unlock()
	if st, ok := reg.states[roomID]; ok {
		fn(st)
	}
}

// SessionStatsRepo is the narrow stats seam consumed by the room API: it
// reports the in-flight write progress of a room's active session. The
// name RoomRepo is reserved for future room persistence.
type SessionStatsRepo interface {
	SessionStats(ctx context.Context, roomID int64) (*SessionStats, error)
}

// RoomUsecase serves the room status API from the shared registry. It
// reads snapshots only; the recorder daemon owns all state writes.
type RoomUsecase struct {
	reg   *RoomRegistry
	stats SessionStatsRepo
}

// NewRoomUsecase new a Room usecase.
func NewRoomUsecase(reg *RoomRegistry, stats SessionStatsRepo) *RoomUsecase {
	return &RoomUsecase{reg: reg, stats: stats}
}

// ListRooms returns the status-API view of every configured room, in
// configuration order.
func (uc *RoomUsecase) ListRooms(ctx context.Context) ([]*RoomRuntime, error) {
	out := uc.reg.Snapshot()
	for _, rt := range out {
		if rt.Record != RecordRecording {
			continue
		}
		stats, err := uc.stats.SessionStats(ctx, rt.Room.RoomID)
		if err != nil || stats == nil {
			continue
		}
		rt.CurrentFile = stats.CurrentFile
		rt.BytesWritten = stats.BytesWritten
	}
	return out, nil
}
