package data

import (
	"context"
	stderrors "errors"
	"fmt"
	"strings"
	"time"

	"suika/internal/biz"

	sqlite3 "github.com/mattn/go-sqlite3"
	"go.einride.tech/aip/filtering"
	"go.einride.tech/aip/ordering"
	expr "google.golang.org/genproto/googleapis/api/expr/v1alpha1"
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

func (r *roomRepo) ListRooms(ctx context.Context, opts ...biz.ListOption) ([]*biz.Room, error) {
	options := biz.ListOptions{Limit: 20}
	for _, opt := range opts {
		opt(&options)
	}
	if options.Offset < 0 || options.Limit <= 0 {
		return nil, biz.ErrRoomInvalidArgument
	}

	query := r.data.db.WithContext(ctx).Model(&roomPO{})
	if options.Filter.CheckedExpr != nil {
		where, args, err := buildRoomFilter(options.Filter.CheckedExpr.GetExpr())
		if err != nil {
			return nil, biz.ErrRoomInvalidArgument
		}
		query = query.Where(where, args...)
	}
	orderBy, err := buildRoomOrderBy(options.OrderBy)
	if err != nil {
		return nil, biz.ErrRoomInvalidArgument
	}
	query = query.Order(orderBy)

	var pos []roomPO
	if err := query.Offset(options.Offset).Limit(options.Limit).Find(&pos).Error; err != nil {
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

// roomColumns maps the whitelisted AIP field names to roomPO columns. Only
// persisted fields are filterable/orderable; runtime fields never reach
// storage.
var roomColumns = map[string]string{
	"room_id":     "room_id",
	"name":        "name",
	"enabled":     "enabled",
	"create_time": "create_time",
	"update_time": "update_time",
}

// roomTimestampColumns are the columns compared as timestamps.
var roomTimestampColumns = map[string]bool{
	"create_time": true,
	"update_time": true,
}

// roomFilterOperators maps AIP comparison functions to SQL operators.
var roomFilterOperators = map[string]string{
	filtering.FunctionEquals:        "=",
	filtering.FunctionNotEquals:     "!=",
	filtering.FunctionLessThan:      "<",
	filtering.FunctionLessEquals:    "<=",
	filtering.FunctionGreaterThan:   ">",
	filtering.FunctionGreaterEquals: ">=",
}

// buildRoomFilter translates a parsed AIP filter expression into a gorm
// WHERE clause. Every identifier must be whitelisted in roomColumns;
// anything unsupported is an error (mapped to ErrRoomInvalidArgument by
// the caller).
func buildRoomFilter(e *expr.Expr) (string, []any, error) {
	switch v := e.GetExprKind().(type) {
	case *expr.Expr_CallExpr:
		fn := v.CallExpr.GetFunction()
		args := v.CallExpr.GetArgs()
		switch fn {
		case filtering.FunctionAnd, filtering.FunctionOr:
			op := " AND "
			if fn == filtering.FunctionOr {
				op = " OR "
			}
			clauses := make([]string, 0, len(args))
			var values []any
			for _, arg := range args {
				clause, args, err := buildRoomFilter(arg)
				if err != nil {
					return "", nil, err
				}
				clauses = append(clauses, "("+clause+")")
				values = append(values, args...)
			}
			return strings.Join(clauses, op), values, nil
		case filtering.FunctionNot:
			if len(args) != 1 {
				return "", nil, fmt.Errorf("unsupported NOT expression")
			}
			clause, values, err := buildRoomFilter(args[0])
			if err != nil {
				return "", nil, err
			}
			return "NOT (" + clause + ")", values, nil
		case filtering.FunctionHas:
			// `field:"value"` is a substring match, only supported on name.
			if len(args) != 2 {
				return "", nil, fmt.Errorf("unsupported match expression")
			}
			col, err := roomFilterColumn(args[0])
			if err != nil {
				return "", nil, err
			}
			if col != "name" {
				return "", nil, fmt.Errorf("unsupported match field %q", col)
			}
			value, ok := roomFilterStringConst(args[1])
			if !ok {
				return "", nil, fmt.Errorf("unsupported match value")
			}
			return col + " LIKE ? ESCAPE '\\'", []any{"%" + escapeLike(value) + "%"}, nil
		default:
			if _, ok := roomFilterOperators[fn]; !ok {
				return "", nil, fmt.Errorf("unsupported filter function %q", fn)
			}
			return buildRoomComparison(fn, args)
		}
	case *expr.Expr_IdentExpr:
		// A bare identifier is a boolean test, e.g. `enabled` / `NOT enabled`.
		col, err := roomFilterColumn(e)
		if err != nil {
			return "", nil, err
		}
		if col != "enabled" {
			return "", nil, fmt.Errorf("unsupported bare filter field %q", v.IdentExpr.GetName())
		}
		return col + " = ?", []any{true}, nil
	default:
		return "", nil, fmt.Errorf("unsupported filter expression")
	}
}

// buildRoomComparison translates one `ident op value` comparison; the
// constant may sit on either side (the operator is flipped accordingly).
func buildRoomComparison(fn string, args []*expr.Expr) (string, []any, error) {
	if len(args) != 2 {
		return "", nil, fmt.Errorf("unsupported comparison")
	}
	identIdx, constIdx := 0, 1
	op := roomFilterOperators[fn]
	if _, err := roomFilterColumn(args[0]); err != nil {
		identIdx, constIdx = 1, 0
		switch fn {
		case filtering.FunctionLessThan:
			op = ">"
		case filtering.FunctionLessEquals:
			op = ">="
		case filtering.FunctionGreaterThan:
			op = "<"
		case filtering.FunctionGreaterEquals:
			op = "<="
		}
	}
	col, err := roomFilterColumn(args[identIdx])
	if err != nil {
		return "", nil, err
	}
	value, ok := roomFilterValue(col, args[constIdx])
	if !ok {
		return "", nil, fmt.Errorf("unsupported filter value for %q", col)
	}
	return col + " " + op + " ?", []any{value}, nil
}

// roomFilterColumn resolves a whitelisted filter identifier to its column.
func roomFilterColumn(e *expr.Expr) (string, error) {
	ident := e.GetIdentExpr()
	if ident == nil {
		return "", fmt.Errorf("expected identifier")
	}
	col, ok := roomColumns[ident.GetName()]
	if !ok {
		return "", fmt.Errorf("unsupported filter field %q", ident.GetName())
	}
	return col, nil
}

// roomFilterValue extracts a bind value for col from a constant expression.
// Timestamp columns accept `timestamp("...")` calls and bare RFC3339
// strings.
func roomFilterValue(col string, e *expr.Expr) (any, bool) {
	if call := e.GetCallExpr(); call != nil && call.GetFunction() == filtering.FunctionTimestamp {
		args := call.GetArgs()
		if len(args) != 1 {
			return nil, false
		}
		s, ok := roomFilterStringConst(args[0])
		if !ok {
			return nil, false
		}
		ts, err := time.Parse(time.RFC3339, s)
		if err != nil {
			return nil, false
		}
		return ts, true
	}
	cons := e.GetConstExpr()
	if cons == nil {
		return nil, false
	}
	switch k := cons.GetConstantKind().(type) {
	case *expr.Constant_Int64Value:
		return k.Int64Value, true
	case *expr.Constant_BoolValue:
		return k.BoolValue, true
	case *expr.Constant_StringValue:
		if roomTimestampColumns[col] {
			ts, err := time.Parse(time.RFC3339, k.StringValue)
			if err != nil {
				return nil, false
			}
			return ts, true
		}
		return k.StringValue, true
	default:
		return nil, false
	}
}

func roomFilterStringConst(e *expr.Expr) (string, bool) {
	cons := e.GetConstExpr()
	if cons == nil {
		return "", false
	}
	v, ok := cons.GetConstantKind().(*expr.Constant_StringValue)
	if !ok {
		return "", false
	}
	return v.StringValue, true
}

// escapeLike escapes LIKE wildcards so user input matches literally.
func escapeLike(s string) string {
	return strings.NewReplacer(`\`, `\\`, `%`, `\%`, `_`, `\_`).Replace(s)
}

// buildRoomOrderBy translates an AIP order_by into a gorm ORDER BY clause.
// Paths must be whitelisted in roomColumns. Without an explicit order the
// list falls back to room_id so pagination stays deterministic.
func buildRoomOrderBy(orderBy ordering.OrderBy) (string, error) {
	if len(orderBy.Fields) == 0 {
		return "room_id ASC", nil
	}
	parts := make([]string, 0, len(orderBy.Fields))
	for _, field := range orderBy.Fields {
		col, ok := roomColumns[field.Path]
		if !ok {
			return "", fmt.Errorf("unsupported order_by field %q", field.Path)
		}
		dir := "ASC"
		if field.Desc {
			dir = "DESC"
		}
		parts = append(parts, col+" "+dir)
	}
	return strings.Join(parts, ", "), nil
}
