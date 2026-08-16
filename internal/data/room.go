package data

import (
	"context"
	stderrors "errors"
	"time"

	"suika/internal/biz"

	sqlite3 "github.com/mattn/go-sqlite3"
	"gorm.io/gorm"
)

type roomPO struct {
	RoomID       int64 `gorm:"primaryKey"`
	StreamerName string
	RoomTitle    string
	Enabled      bool
	CreateTime   time.Time `gorm:"autoCreateTime"`
	UpdateTime   time.Time `gorm:"autoUpdateTime"`
}

func (roomPO) TableName() string { return "rooms" }

func toRoomPO(room *biz.Room) *roomPO {
	if room == nil {
		return nil
	}
	return &roomPO{
		RoomID:       room.RoomID,
		StreamerName: room.StreamerName,
		RoomTitle:    room.RoomTitle,
		Enabled:      room.Enabled,
		CreateTime:   room.CreateTime,
		UpdateTime:   room.UpdateTime,
	}
}

func toRoomDO(po *roomPO) *biz.Room {
	if po == nil {
		return nil
	}
	return &biz.Room{
		RoomID:       po.RoomID,
		StreamerName: po.StreamerName,
		RoomTitle:    po.RoomTitle,
		Enabled:      po.Enabled,
		CreateTime:   po.CreateTime,
		UpdateTime:   po.UpdateTime,
	}
}

type roomRepo struct {
	data *Data
}

func NewRoomRepo(d *Data) biz.RoomRepo {
	return &roomRepo{data: d}
}

func (r *roomRepo) GetByRoomID(ctx context.Context, roomID int64) (*biz.Room, error) {
	var po roomPO
	err := r.data.db.WithContext(ctx).Where("room_id = ?", roomID).First(&po).Error
	if err != nil {
		if stderrors.Is(err, gorm.ErrRecordNotFound) {
			return nil, biz.ErrRoomNotFound
		}
		return nil, err
	}
	return toRoomDO(&po), nil
}

func (r *roomRepo) ListRooms(ctx context.Context, queryOpt biz.ListQuery) ([]*biz.Room, error) {
	if queryOpt.Limit <= 0 {
		queryOpt.Limit = 20
	}
	if queryOpt.Offset < 0 {
		return nil, biz.ErrRoomInvalidArgument
	}

	query := r.data.db.WithContext(ctx).Model(&roomPO{})
	if queryOpt.RoomID != nil {
		query = query.Where("room_id = ?", *queryOpt.RoomID)
	}
	if queryOpt.StreamerName != nil {
		query = query.Where("streamer_name = ?", *queryOpt.StreamerName)
	}
	if queryOpt.RoomTitle != nil {
		query = query.Where("room_title = ?", *queryOpt.RoomTitle)
	}
	if queryOpt.Enabled != nil {
		query = query.Where("enabled = ?", *queryOpt.Enabled)
	}
	query = query.Order("room_id ASC")

	var pos []roomPO
	if err := query.Offset(queryOpt.Offset).Limit(queryOpt.Limit).Find(&pos).Error; err != nil {
		return nil, err
	}
	rooms := make([]*biz.Room, 0, len(pos))
	for i := range pos {
		rooms = append(rooms, toRoomDO(&pos[i]))
	}

	return rooms, nil
}

func (r *roomRepo) CreateRoom(ctx context.Context, room *biz.Room) (*biz.Room, error) {
	po := toRoomPO(room)
	if err := r.data.db.WithContext(ctx).Create(po).Error; err != nil {
		if isRoomConstraintError(err) {
			return nil, biz.ErrRoomAlreadyExists
		}
		return nil, err
	}

	return toRoomDO(po), nil
}

func (r *roomRepo) UpdateRoom(ctx context.Context, room *biz.Room) (*biz.Room, error) {
	po := toRoomPO(room)
	result := r.data.db.WithContext(ctx).Model(&roomPO{}).
		Where("room_id = ?", po.RoomID).
		Updates(map[string]any{
			"streamer_name": po.StreamerName,
			"room_title":    po.RoomTitle,
			"enabled":       po.Enabled,
		})
	if result.Error != nil {
		return nil, result.Error
	}
	if result.RowsAffected == 0 {
		return nil, biz.ErrRoomNotFound
	}

	return r.GetByRoomID(ctx, room.RoomID)
}

func (r *roomRepo) BackfillRoomIdentity(ctx context.Context, roomID int64, streamerName string, roomTitle string) (bool, error) {
	if streamerName == "" && roomTitle == "" {
		return false, nil
	}
	updates := map[string]any{}
	if streamerName != "" {
		updates["streamer_name"] = gorm.Expr("CASE WHEN streamer_name = '' THEN ? ELSE streamer_name END", streamerName)
	}
	if roomTitle != "" {
		updates["room_title"] = gorm.Expr("CASE WHEN room_title = '' THEN ? ELSE room_title END", roomTitle)
	}
	result := r.data.db.WithContext(ctx).Model(&roomPO{}).
		Where("room_id = ?", roomID).
		Updates(updates)
	if result.Error != nil {
		return false, result.Error
	}
	return result.RowsAffected > 0, nil
}

func (r *roomRepo) DeleteRoom(ctx context.Context, roomID int64) error {
	result := r.data.db.WithContext(ctx).Where("room_id = ?", roomID).Delete(&roomPO{})
	if result.Error != nil {
		return result.Error
	}
	if result.RowsAffected == 0 {
		return biz.ErrRoomNotFound
	}

	return nil
}

// isRoomConstraintError 判断 err 是否为 sqlite 约束冲突，
// 即 room_id 主键已存在。
func isRoomConstraintError(err error) bool {
	var sqliteErr sqlite3.Error
	return stderrors.As(err, &sqliteErr) && sqliteErr.Code == sqlite3.ErrConstraint
}
