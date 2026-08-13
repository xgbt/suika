package data

import (
	"context"
	stderrors "errors"
	"time"

	"suika/internal/biz"

	sqlite3 "github.com/mattn/go-sqlite3"
	"gorm.io/gorm"
)

// roomPO is the persistent shape of a room. room_id is the caller-provided
// platform room id and serves as the primary key (no autoincrement).
type roomPO struct {
	RoomID     int64 `gorm:"primaryKey"`
	Name       string
	Enabled    bool
	CreateTime time.Time `gorm:"autoCreateTime"`
	UpdateTime time.Time `gorm:"autoUpdateTime"`
}

// TableName pins the table name (the struct name would pluralize to
// "room_pos").
func (roomPO) TableName() string { return "rooms" }

// newRoom converts a biz room to its persistent shape (write path).
func newRoom(room *biz.Room) *roomPO {
	if room == nil {
		return nil
	}
	return &roomPO{
		RoomID:     room.RoomID,
		Name:       room.Name,
		Enabled:    room.Enabled,
		CreateTime: room.CreateTime,
		UpdateTime: room.UpdateTime,
	}
}

// toBiz converts a persistent room back to the biz shape (read path).
func toBiz(po *roomPO) *biz.Room {
	if po == nil {
		return nil
	}
	return &biz.Room{
		RoomID:     po.RoomID,
		Name:       po.Name,
		Enabled:    po.Enabled,
		CreateTime: po.CreateTime,
		UpdateTime: po.UpdateTime,
	}
}

type roomRepo struct {
	data *Data
}

// NewRoomRepo new a Room repo.
func NewRoomRepo(d *Data) biz.RoomRepo {
	return &roomRepo{data: d}
}

func (r *roomRepo) FindByRoomID(ctx context.Context, roomID int64) (*biz.Room, error) {
	var po roomPO
	err := r.data.db.WithContext(ctx).Where("room_id = ?", roomID).First(&po).Error
	if err != nil {
		if stderrors.Is(err, gorm.ErrRecordNotFound) {
			return nil, biz.ErrRoomNotFound
		}
		return nil, err
	}
	return toBiz(&po), nil
}

func (r *roomRepo) ListRooms(ctx context.Context, queryOpt biz.ListQuery) ([]*biz.Room, error) {
	query := queryOpt
	if query.Limit <= 0 {
		query.Limit = 20
	}
	if query.Offset < 0 || query.Limit <= 0 {
		return nil, biz.ErrRoomInvalidArgument
	}

	dbQuery := r.data.db.WithContext(ctx).Model(&roomPO{})
	if query.RoomID != nil {
		dbQuery = dbQuery.Where("room_id = ?", *query.RoomID)
	}
	if query.Name != nil {
		dbQuery = dbQuery.Where("name = ?", *query.Name)
	}
	if query.Enabled != nil {
		dbQuery = dbQuery.Where("enabled = ?", *query.Enabled)
	}
	dbQuery = dbQuery.Order("room_id ASC")

	var pos []roomPO
	if err := dbQuery.Offset(query.Offset).Limit(query.Limit).Find(&pos).Error; err != nil {
		return nil, err
	}
	rooms := make([]*biz.Room, 0, len(pos))
	for i := range pos {
		rooms = append(rooms, toBiz(&pos[i]))
	}
	return rooms, nil
}

func (r *roomRepo) CreateRoom(ctx context.Context, room *biz.Room) (*biz.Room, error) {
	po := newRoom(room)
	if err := r.data.db.WithContext(ctx).Create(po).Error; err != nil {
		if isRoomConstraintError(err) {
			return nil, biz.ErrRoomAlreadyExists
		}
		return nil, err
	}
	return toBiz(po), nil
}

func (r *roomRepo) UpdateRoom(ctx context.Context, room *biz.Room) (*biz.Room, error) {
	po := newRoom(room)
	result := r.data.db.WithContext(ctx).Model(&roomPO{}).
		Where("room_id = ?", po.RoomID).
		Updates(map[string]any{
			"name":    po.Name,
			"enabled": po.Enabled,
		})
	if result.Error != nil {
		return nil, result.Error
	}
	if result.RowsAffected == 0 {
		return nil, biz.ErrRoomNotFound
	}
	return r.FindByRoomID(ctx, room.RoomID)
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

// isRoomConstraintError reports whether err is a sqlite constraint
// violation, i.e. the room_id primary key already exists.
func isRoomConstraintError(err error) bool {
	var sqliteErr sqlite3.Error
	return stderrors.As(err, &sqliteErr) && sqliteErr.Code == sqlite3.ErrConstraint
}
