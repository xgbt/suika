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

type LiveStatus int

const (
	LiveStatusUnknown LiveStatus = iota
	LiveStatusPreparing
	LiveStatusOnAir
)

type RecordStatus int

const (
	RecordStatusIdle RecordStatus = iota
	RecordStatusRecording
	RecordStatusRemuxing
	RecordStatusError
)

// Room 是房间的领域对象，包含持久化的房间信息和审计时间。
type Room struct {
	RoomID        int64     // 房间 ID
	StreamerName  string    // 主播名称
	RoomTitle     string    // 房间标题
	RecordEnabled bool      // 是否启用录制
	CreateTime    time.Time // 创建时间
	UpdateTime    time.Time // 更新时间
}

// RoomRuntime 是面向读取的房间运行时快照，由房间信息、运行状态和当前录制会话进度组成。
type RoomRuntime struct {
	Room             Room         // 房间基础信息
	LiveStatus       LiveStatus   // 当前直播状态
	RecordStatus     RecordStatus // 当前录制状态
	SessionStartedAt time.Time    // 当前录制会话开始时间
	LastError        string       // 最近一次监控或录制错误
	CurrentFile      string       // 当前录制会话正在写入的分段文件
	BytesWritten     int64        // 当前分段已写入的字节数
	DownloadSpeed    int64        // 当前下载速度（字节/秒）
}

type RoomRepo interface {
	GetByRoomID(context.Context, int64) (*Room, error)
	ListRooms(context.Context, ListQuery) ([]*Room, error)
	CreateRoom(context.Context, *Room) (*Room, error)
	UpdateRoom(context.Context, *Room) (*Room, error)
	DeleteRoom(context.Context, int64) error
}

// ListQuery 房间列表查询条件, 用于 RoomRepo.ListRooms 查询
type ListQuery struct {
	RoomID        *int64
	StreamerName  *string
	RoomTitle     *string
	RecordEnabled *bool
	Offset        int
	Limit         int
}

// SessionStats 是当前录制会话的写入进度快照。
type SessionStats struct {
	CurrentFile   string // 当前正在写入的分段文件名，可能为空
	BytesWritten  int64  // 当前分段已写入的字节数
	DownloadSpeed int64  // 当前下载速度（字节/秒）
}

// SessionStatsRepo 提供房间当前录制会话的写入进度，实际由 RecorderRepo 实现。
type SessionStatsRepo interface {
	// SessionStats 返回房间当前录制会话的写入进度。房间未录制或会话已结束时返回 nil。
	SessionStats(ctx context.Context, roomID int64) (*SessionStats, error)
}

type RoomUsecase struct {
	repo             RoomRepo
	registry         *RoomRegistry
	sessionStatsRepo SessionStatsRepo
}

func NewRoomUsecase(repo RoomRepo, reg *RoomRegistry, stats SessionStatsRepo) *RoomUsecase {
	return &RoomUsecase{repo: repo, registry: reg, sessionStatsRepo: stats}
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
	uc.registry.Add(*created)
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
	uc.registry.Update(*updated)
	return &RoomRuntime{Room: *updated}, nil
}

func (uc *RoomUsecase) DeleteRoom(ctx context.Context, roomID int64) error {
	if roomID <= 0 {
		return ErrRoomInvalidArgument
	}
	if err := uc.repo.DeleteRoom(ctx, roomID); err != nil {
		return err
	}
	uc.registry.Remove(roomID)
	return nil
}

// withRuntime 将持久化字段 Room 与 RoomRegistry 中的运行时状态合并后返回 RoomRuntime。
func (uc *RoomUsecase) withRuntime(ctx context.Context, room *Room) *RoomRuntime {
	runtime := uc.registry.runtime(room.RoomID)
	runtime.Room = *room

	// 如果房间正在录制中，尝试获取当前录制 session 的写入进度
	if runtime.RecordStatus == RecordStatusRecording {
		stats, err := uc.sessionStatsRepo.SessionStats(ctx, room.RoomID)
		if err == nil && stats != nil {
			runtime.CurrentFile = stats.CurrentFile
			runtime.BytesWritten = stats.BytesWritten
			runtime.DownloadSpeed = stats.DownloadSpeed
		}
	}
	return runtime
}
