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

// Room 领域对象（DO）。
type Room struct {
	RoomID       int64
	StreamerName string
	RoomTitle    string
	Enabled      bool
	CreateTime   time.Time
	UpdateTime   time.Time
}

// RoomRuntime 是 Room 的运行时状态, 包含持久化字段、RoomRegistry 运行时状态与会话写入进度。
// 服务于 RoomUsecase 的 GetRoom、ListRoomRuntimes, 返回给 API 层 DTO 转换使用。
type RoomRuntime struct {
	Room             Room
	LiveState        LiveState
	RecordState      RecordState
	CurrentFile      string
	BytesWritten     int64
	SessionStartedAt time.Time
	LastError        string
}

type RoomRepo interface {
	GetByRoomID(context.Context, int64) (*Room, error)
	ListRooms(context.Context, ListQuery) ([]*Room, error)
	CreateRoom(context.Context, *Room) (*Room, error)
	UpdateRoom(context.Context, *Room) (*Room, error)
	DeleteRoom(context.Context, int64) error
}

type ListQuery struct {
	RoomID       *int64
	StreamerName *string
	RoomTitle    *string
	Enabled      *bool
	Offset       int
	Limit        int
}

type SessionStatsRepo interface {
	// SessionStats 返回房间当前录制 session 的写入进度。若房间未在录制中或已结束，则返回 nil。
	SessionStats(ctx context.Context, roomID int64) (*SessionStats, error)
}

// RoomUsecase 服务房间 API：增删改经由仓储持久化；
// 将持久化字段 Room 与 RoomRegistry 中的运行时状态合并后返回。
type RoomUsecase struct {
	repo  RoomRepo
	reg   *RoomRegistry
	stats SessionStatsRepo
}

func NewRoomUsecase(repo RoomRepo, reg *RoomRegistry, stats SessionStatsRepo) *RoomUsecase {
	return &RoomUsecase{repo: repo, reg: reg, stats: stats}
}

func (uc *RoomUsecase) GetRoom(ctx context.Context, roomID int64) (*RoomRuntime, error) {
	if roomID <= 0 {
		return nil, ErrRoomInvalidArgument
	}
	room, err := uc.repo.GetByRoomID(ctx, roomID)
	if err != nil {
		return nil, err
	}
	return uc.withRuntime(ctx, room), nil
}

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

func (uc *RoomUsecase) DeleteRoom(ctx context.Context, roomID int64) error {
	if roomID <= 0 {
		return ErrRoomInvalidArgument
	}
	return uc.repo.DeleteRoom(ctx, roomID)
}

// withRuntime 将持久化字段 Room 与 RoomRegistry 中的运行时状态合并后返回 RoomRuntime。
func (uc *RoomUsecase) withRuntime(ctx context.Context, room *Room) *RoomRuntime {
	runtime := uc.reg.runtime(room.RoomID)
	runtime.Room = *room
	if runtime.RecordState == RecordRecording {
		stats, err := uc.stats.SessionStats(ctx, room.RoomID)
		if err == nil && stats != nil {
			runtime.CurrentFile = stats.CurrentFile
			runtime.BytesWritten = stats.BytesWritten
		}
	}
	return runtime
}
