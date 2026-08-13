package biz

import (
	"context"
	"fmt"
	"math"
	"sync"
	"time"

	"github.com/go-kratos/kratos/v3/log"
)

// roomState is the mutable runtime state of one registered room.
type roomState struct {
	room             Room
	live             LiveState
	record           RecordState
	sessionStartedAt time.Time
	lastError        string
}

// RoomRegistry holds the room list loaded at startup and its runtime
// state. The recorder daemon writes live/record state; the room API reads
// snapshots. CRUD changes made through the room API are persisted in the
// repo and picked up by the registry on the next restart.
type RoomRegistry struct {
	repo RoomRepo
	// mu protects both the registry containers and the mutable fields inside
	// each roomState. It must be held while reading or changing rooms, states,
	// or a room's live/record/session/error fields; protecting only the map
	// would still allow concurrent goroutines to race on the state object.
	// Snapshot methods hold mu while copying state, and mutating methods hold
	// it for the complete read-modify-write operation. Repository I/O must stay
	// outside the critical section so a slow database call cannot block all
	// recorder updates and room reads.
	mu     sync.Mutex
	rooms  []Room
	states map[int64]*roomState
}

// NewRoomRegistry loads the persisted room list from the repo into a
// registry. A nil repo is tolerated and yields an empty registry; a load
// failure fails startup.
func NewRoomRegistry(repo RoomRepo) (*RoomRegistry, error) {
	reg := &RoomRegistry{
		repo:   repo,
		states: make(map[int64]*roomState),
	}
	if repo == nil {
		log.Warn("room repo missing, registering zero rooms")
		return reg, nil
	}
	rooms, err := repo.ListRooms(context.Background(), ListQuery{Offset: 0, Limit: math.MaxInt32})
	if err != nil {
		return nil, fmt.Errorf("room registry: load rooms: %w", err)
	}
	for _, room := range rooms {
		reg.rooms = append(reg.rooms, *room)
		reg.states[room.RoomID] = &roomState{room: *room}
	}
	return reg, nil
}

// Rooms returns a snapshot of every registered room, in load order (no
// streamer-name backfill).
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

// runtime returns a copy of one room's runtime state. Rooms unknown to the
// registry (created after startup) get default state values.
func (reg *RoomRegistry) runtime(roomID int64) *RoomRuntime {
	reg.mu.Lock()
	defer reg.mu.Unlock()
	st, ok := reg.states[roomID]
	if !ok {
		return &RoomRuntime{Room: Room{RoomID: roomID}}
	}
	return &RoomRuntime{
		Room:             st.room,
		Live:             st.live,
		Record:           st.record,
		SessionStartedAt: st.sessionStartedAt,
		LastError:        st.lastError,
	}
}

// ApplyRoomInfo records the platform-reported live state of a room and
// backfills the room name from the streamer name when unset. A backfilled
// name is written back through the room repo so restarts keep it.
func (reg *RoomRegistry) ApplyRoomInfo(ctx context.Context, roomID int64, info *RoomInfo) {
	if info == nil {
		return
	}
	reg.mu.Lock()
	st, ok := reg.states[roomID]
	if !ok {
		reg.mu.Unlock()
		return
	}
	if info.Live {
		st.live = LiveOnAir
	} else {
		st.live = LivePreparing
	}
	var backfilled *Room
	if st.room.Name == "" && info.StreamerName != "" {
		st.room.Name = info.StreamerName
		room := st.room
		backfilled = &room
	}
	reg.mu.Unlock()

	if backfilled != nil && reg.repo != nil {
		if _, err := reg.repo.UpdateRoom(ctx, backfilled); err != nil {
			log.Warn("room registry: persist backfilled room name failed", "room", roomID, "err", err)
		}
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
