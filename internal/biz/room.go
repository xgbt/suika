package biz

import (
	"context"
	"time"

	v1 "suika/api/room/v1"

	"github.com/go-kratos/kratos/v3/errors"
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
	ListRooms(context.Context, ListQuery) ([]*Room, error)
	CreateRoom(context.Context, *Room) (*Room, error)
	UpdateRoom(context.Context, *Room) (*Room, error)
	DeleteRoom(context.Context, int64) error
}

type ListQuery struct {
	RoomID  *int64
	Name    *string
	Enabled *bool
	Offset  int
	Limit   int
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
func (uc *RoomUsecase) ListRoomRuntimes(ctx context.Context, query ListQuery) ([]*RoomRuntime, error) {
	rooms, err := uc.repo.ListRooms(ctx, query)
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
