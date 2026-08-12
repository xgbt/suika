package service

import (
	"context"

	v1 "suika/api/room/v1"
	"suika/internal/biz"

	"go.einride.tech/aip/fieldmask"
	"go.einride.tech/aip/filtering"
	"go.einride.tech/aip/ordering"
	"go.einride.tech/aip/pagination"
	"google.golang.org/protobuf/types/known/emptypb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const (
	defaultPageSize = 20
)

// updatableRoomFields are the only field paths accepted by UpdateRoom;
// room_id is immutable and the runtime fields are server-populated.
var updatableRoomFields = map[string]bool{
	"name":    true,
	"enabled": true,
}

// RoomService is a room service.
type RoomService struct {
	v1.UnimplementedRoomServiceServer

	uc *biz.RoomUsecase
}

// NewRoomService new a room service.
func NewRoomService(uc *biz.RoomUsecase) *RoomService {
	return &RoomService{uc: uc}
}

// CreateRoom registers a new room.
func (s *RoomService) CreateRoom(ctx context.Context, req *v1.CreateRoomRequest) (*v1.CreateRoomResponse, error) {
	rt, err := s.uc.CreateRoom(ctx, convertRoom(req.GetRoom()))
	if err != nil {
		return nil, err
	}
	return &v1.CreateRoomResponse{Room: convertRoomReply(rt)}, nil
}

// GetRoom returns one room with its runtime state merged in.
func (s *RoomService) GetRoom(ctx context.Context, req *v1.GetRoomRequest) (*v1.GetRoomResponse, error) {
	rt, err := s.uc.GetRoom(ctx, req.GetRoomId())
	if err != nil {
		return nil, err
	}
	return &v1.GetRoomResponse{Room: convertRoomReply(rt)}, nil
}

// ListRooms lists rooms with filtering, ordering, and pagination.
func (s *RoomService) ListRooms(ctx context.Context, req *v1.ListRoomsRequest) (*v1.ListRoomsResponse, error) {
	declarations, err := filtering.NewDeclarations(
		filtering.DeclareStandardFunctions(),
		filtering.DeclareIdent("room_id", filtering.TypeInt),
		filtering.DeclareIdent("name", filtering.TypeString),
		filtering.DeclareIdent("enabled", filtering.TypeBool),
		filtering.DeclareIdent("create_time", filtering.TypeTimestamp),
		filtering.DeclareIdent("update_time", filtering.TypeTimestamp),
	)
	if err != nil {
		return nil, err
	}
	filter, err := filtering.ParseFilter(req, declarations)
	if err != nil {
		return nil, err
	}
	pageToken, err := pagination.ParsePageToken(req)
	if err != nil {
		return nil, err
	}
	orderBy, err := ordering.ParseOrderBy(req)
	if err != nil {
		return nil, err
	}
	if err := orderBy.ValidateForPaths("room_id", "name", "enabled", "create_time", "update_time"); err != nil {
		return nil, err
	}
	if req.PageSize <= 0 {
		req.PageSize = defaultPageSize
	}
	rts, err := s.uc.ListRooms(ctx,
		biz.ListFilter(filter),
		biz.ListOrderBy(orderBy),
		biz.ListLimit(int(req.PageSize)),
		biz.ListOffset(int(pageToken.Offset)),
	)
	if err != nil {
		return nil, err
	}
	set := &v1.ListRoomsResponse{
		Rooms: make([]*v1.Room, 0, len(rts)),
	}
	if len(rts) >= int(req.PageSize) {
		set.NextPageToken = pageToken.Next(req).String()
	}
	for _, rt := range rts {
		set.Rooms = append(set.Rooms, convertRoomReply(rt))
	}
	return set, nil
}

// UpdateRoom applies a partial update to an existing room.
func (s *RoomService) UpdateRoom(ctx context.Context, req *v1.UpdateRoomRequest) (*v1.UpdateRoomResponse, error) {
	if req.GetRoom().GetRoomId() <= 0 || req.GetUpdateMask() == nil || len(req.GetUpdateMask().GetPaths()) == 0 {
		return nil, biz.ErrRoomInvalidArgument
	}
	for _, path := range req.GetUpdateMask().GetPaths() {
		if !updatableRoomFields[path] {
			return nil, biz.ErrRoomInvalidArgument
		}
	}
	currentResponse, err := s.GetRoom(ctx, &v1.GetRoomRequest{RoomId: req.GetRoom().GetRoomId()})
	if err != nil {
		return nil, err
	}
	current := currentResponse.GetRoom()
	fieldmask.Update(req.GetUpdateMask(), current, req.GetRoom())
	rt, err := s.uc.UpdateRoom(ctx, convertRoom(current))
	if err != nil {
		return nil, err
	}
	return &v1.UpdateRoomResponse{Room: convertRoomReply(rt)}, nil
}

// DeleteRoom removes a room.
func (s *RoomService) DeleteRoom(ctx context.Context, req *v1.DeleteRoomRequest) (*v1.DeleteRoomResponse, error) {
	if err := s.uc.DeleteRoom(ctx, req.GetRoomId()); err != nil {
		return nil, err
	}
	return &v1.DeleteRoomResponse{Empty: &emptypb.Empty{}}, nil
}

func convertRoom(in *v1.Room) *biz.Room {
	if in == nil {
		return nil
	}
	return &biz.Room{
		RoomID:  in.GetRoomId(),
		Name:    in.GetName(),
		Enabled: in.GetEnabled(),
	}
}

// convertRoomReply converts the merged runtime view back to a DTO. It is a
// free function so unary handlers share the runtime enum mapping.
func convertRoomReply(rt *biz.RoomRuntime) *v1.Room {
	if rt == nil {
		return nil
	}
	var liveStatus v1.LiveStatus
	switch rt.Live {
	case biz.LivePreparing:
		liveStatus = v1.LiveStatus_LIVE_STATUS_PREPARING
	case biz.LiveOnAir:
		liveStatus = v1.LiveStatus_LIVE_STATUS_LIVE
	default:
		liveStatus = v1.LiveStatus_LIVE_STATUS_UNSPECIFIED
	}
	var recordStatus v1.RecordStatus
	switch rt.Record {
	case biz.RecordRecording:
		recordStatus = v1.RecordStatus_RECORD_STATUS_RECORDING
	case biz.RecordRemuxing:
		recordStatus = v1.RecordStatus_RECORD_STATUS_REMUXING
	case biz.RecordError:
		recordStatus = v1.RecordStatus_RECORD_STATUS_ERROR
	default:
		recordStatus = v1.RecordStatus_RECORD_STATUS_IDLE
	}
	room := &v1.Room{
		RoomId:       rt.Room.RoomID,
		Name:         rt.Room.Name,
		Enabled:      rt.Room.Enabled,
		LiveStatus:   liveStatus,
		RecordStatus: recordStatus,
		CurrentFile:  rt.CurrentFile,
		BytesWritten: rt.BytesWritten,
		LastError:    rt.LastError,
	}
	if !rt.Room.CreateTime.IsZero() {
		room.CreateTime = timestamppb.New(rt.Room.CreateTime)
	}
	if !rt.Room.UpdateTime.IsZero() {
		room.UpdateTime = timestamppb.New(rt.Room.UpdateTime)
	}
	if !rt.SessionStartedAt.IsZero() {
		room.SessionStartedAt = timestamppb.New(rt.SessionStartedAt)
	}
	return room
}
