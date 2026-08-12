package biz

import (
	"context"
	"fmt"
	"math"
	"sync"
	"time"

	v1 "suika/api/room/v1"

	"github.com/go-kratos/kratos/v3/errors"
	"github.com/go-kratos/kratos/v3/log"
	"go.einride.tech/aip/filtering"
	"go.einride.tech/aip/ordering"
)

var (
	ErrRoomNotFound        = errors.NotFound(v1.ErrorReason_ERROR_REASON_NOT_FOUND.String(), "room not found")
	ErrRoomInvalidArgument = errors.BadRequest(v1.ErrorReason_ERROR_REASON_INVALID_ARGUMENT.String(), "invalid room argument")
	ErrRoomAlreadyExists   = errors.Conflict(v1.ErrorReason_ERROR_REASON_ALREADY_EXISTS.String(), "room already exists")
)

type LiveState int

const (
	LiveUnknown LiveState = iota
	LivePreparing
	LiveOnAir
)

type RecordState int

const (
	RecordIdle RecordState = iota
	RecordRecording
	RecordRemuxing
	RecordError
)

// Room DO
type Room struct {
	RoomID     int64
	Name       string
	Enabled    bool
	CreateTime time.Time
	UpdateTime time.Time
}

// RoomRuntime is the status-API view of one room: persisted fields, live
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

// RoomRepo is a room repo.
type RoomRepo interface {
	FindByRoomID(context.Context, int64) (*Room, error)
	ListRooms(context.Context, ...ListOption) ([]*Room, error)
	CreateRoom(context.Context, *Room) (*Room, error)
	UpdateRoom(context.Context, *Room) (*Room, error)
	DeleteRoom(context.Context, int64) error
}

// ListOption configures room list queries.
type ListOption func(*ListOptions)

// ListOptions are room list query options.
type ListOptions struct {
	Filter  filtering.Filter
	OrderBy ordering.OrderBy
	Offset  int
	Limit   int
}

// ListFilter sets a standard AIP filter.
func ListFilter(filter filtering.Filter) ListOption {
	return func(o *ListOptions) {
		o.Filter = filter
	}
}

// ListOrderBy sets a standard AIP order_by value.
func ListOrderBy(orderBy ordering.OrderBy) ListOption {
	return func(o *ListOptions) {
		o.OrderBy = orderBy
	}
}

// ListOffset sets an offset.
func ListOffset(offset int) ListOption {
	return func(o *ListOptions) {
		o.Offset = offset
	}
}

// ListLimit sets a limit.
func ListLimit(limit int) ListOption {
	return func(o *ListOptions) {
		o.Limit = limit
	}
}

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
	repo   RoomRepo
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
	rooms, err := repo.ListRooms(context.Background(), ListOffset(0), ListLimit(math.MaxInt32))
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

// SessionStatsRepo is the narrow stats seam consumed by the room API: it
// reports the in-flight write progress of a room's active session.
type SessionStatsRepo interface {
	SessionStats(ctx context.Context, roomID int64) (*SessionStats, error)
}

// RoomUsecase serves the room API: CRUD goes through the repo, and reads
// merge the persisted fields with the runtime state of the shared
// registry.
type RoomUsecase struct {
	repo  RoomRepo
	reg   *RoomRegistry
	stats SessionStatsRepo
}

// NewRoomUsecase new a Room usecase.
func NewRoomUsecase(repo RoomRepo, reg *RoomRegistry, stats SessionStatsRepo) *RoomUsecase {
	return &RoomUsecase{repo: repo, reg: reg, stats: stats}
}

// GetRoom returns one room with the runtime state merged from the
// registry.
func (uc *RoomUsecase) GetRoom(ctx context.Context, roomID int64) (*RoomRuntime, error) {
	if roomID <= 0 {
		return nil, ErrRoomInvalidArgument
	}
	room, err := uc.repo.FindByRoomID(ctx, roomID)
	if err != nil {
		return nil, err
	}
	return uc.withRuntime(ctx, room), nil
}

// ListRoomRuntimes lists rooms with the runtime state merged from the registry.
func (uc *RoomUsecase) ListRoomRuntimes(ctx context.Context, opts ...ListOption) ([]*RoomRuntime, error) {
	rooms, err := uc.repo.ListRooms(ctx, opts...)
	if err != nil {
		return nil, err
	}

	roomRuntimes := make([]*RoomRuntime, 0, len(rooms))
	for _, room := range rooms {
		roomRuntimes = append(roomRuntimes, uc.withRuntime(ctx, room))
	}
	return roomRuntimes, nil
}

// CreateRoom registers a new room. The returned view carries default
// runtime values; the recorder picks the room up after a restart.
func (uc *RoomUsecase) CreateRoom(ctx context.Context, room *Room) (*RoomRuntime, error) {
	if room == nil || room.RoomID <= 0 {
		return nil, ErrRoomInvalidArgument
	}
	created, err := uc.repo.CreateRoom(ctx, room)
	if err != nil {
		return nil, err
	}
	return &RoomRuntime{Room: *created}, nil
}

// UpdateRoom updates an existing room. The returned view carries default
// runtime values.
func (uc *RoomUsecase) UpdateRoom(ctx context.Context, room *Room) (*RoomRuntime, error) {
	if room == nil || room.RoomID <= 0 {
		return nil, ErrRoomInvalidArgument
	}
	updated, err := uc.repo.UpdateRoom(ctx, room)
	if err != nil {
		return nil, err
	}
	return &RoomRuntime{Room: *updated}, nil
}

// DeleteRoom removes a room.
func (uc *RoomUsecase) DeleteRoom(ctx context.Context, roomID int64) error {
	if roomID <= 0 {
		return ErrRoomInvalidArgument
	}
	return uc.repo.DeleteRoom(ctx, roomID)
}

// withRuntime merges the persisted room with the registry runtime state
// and the in-flight session stats. Persisted fields always come from the
// repo; stats errors are silently skipped (progress is best-effort).
func (uc *RoomUsecase) withRuntime(ctx context.Context, room *Room) *RoomRuntime {
	rt := uc.reg.runtime(room.RoomID)
	rt.Room = *room
	if rt.Record == RecordRecording {
		stats, err := uc.stats.SessionStats(ctx, room.RoomID)
		if err == nil && stats != nil {
			rt.CurrentFile = stats.CurrentFile
			rt.BytesWritten = stats.BytesWritten
		}
	}
	return rt
}
