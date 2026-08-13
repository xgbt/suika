package service

import (
	"context"

	v1 "suika/api/room/v1"
	"suika/internal/biz"

	"go.einride.tech/aip/fieldmask"
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
	"streamer_name": true,
	"room_title":    true,
	"enabled":       true,
}

type RoomService struct {
	v1.UnimplementedRoomServiceServer

	uc *biz.RoomUsecase
}

func NewRoomService(uc *biz.RoomUsecase) *RoomService {
	return &RoomService{uc: uc}
}

func (s *RoomService) CreateRoom(ctx context.Context, req *v1.CreateRoomRequest) (*v1.CreateRoomResponse, error) {
	rt, err := s.uc.CreateRoom(ctx, convertRoom(req.GetRoom()))
	if err != nil {
		return nil, err
	}
	return &v1.CreateRoomResponse{Room: convertRoomReply(rt)}, nil
}

func (s *RoomService) GetRoom(ctx context.Context, req *v1.GetRoomRequest) (*v1.GetRoomResponse, error) {
	roomRuntime, err := s.uc.GetRoom(ctx, req.GetRoomId())
	if err != nil {
		return nil, err
	}
	return &v1.GetRoomResponse{Room: convertRoomReply(roomRuntime)}, nil
}

func (s *RoomService) ListRooms(ctx context.Context, req *v1.ListRoomsRequest) (*v1.ListRoomsResponse, error) {
	if req == nil {
		return nil, biz.ErrRoomInvalidArgument
	}
	pageToken, err := pagination.ParsePageToken(req)
	if err != nil {
		return nil, err
	}
	if req.PageSize <= 0 {
		req.PageSize = defaultPageSize
	}
	query := biz.ListQuery{
		Offset: int(pageToken.Offset),
		Limit:  int(req.PageSize),
	}
	if req.RoomId != nil {
		roomID := req.GetRoomId()
		query.RoomID = &roomID
	}
	if req.StreamerName != nil {
		streamerName := req.GetStreamerName()
		query.StreamerName = &streamerName
	}
	if req.RoomTitle != nil {
		roomTitle := req.GetRoomTitle()
		query.RoomTitle = &roomTitle
	}
	if req.Enabled != nil {
		enabled := req.GetEnabled()
		query.Enabled = &enabled
	}

	roomRuntimes, err := s.uc.ListRoomRuntimes(ctx, query)
	if err != nil {
		return nil, err
	}

	response := &v1.ListRoomsResponse{
		Rooms: make([]*v1.Room, 0, len(roomRuntimes)),
	}
	if len(roomRuntimes) >= int(req.PageSize) {
		response.NextPageToken = pageToken.Next(req).String()
	}
	for _, rt := range roomRuntimes {
		response.Rooms = append(response.Rooms, convertRoomReply(rt))
	}

	return response, nil
}

func (s *RoomService) UpdateRoom(ctx context.Context, req *v1.UpdateRoomRequest) (*v1.UpdateRoomResponse, error) {
	if req.GetRoom().GetRoomId() <= 0 || req.GetUpdateMask() == nil || len(req.GetUpdateMask().GetPaths()) == 0 {
		return nil, biz.ErrRoomInvalidArgument
	}
	for _, path := range req.GetUpdateMask().GetPaths() {
		if !updatableRoomFields[path] {
			return nil, biz.ErrRoomInvalidArgument
		}
	}
	curResp, err := s.GetRoom(ctx, &v1.GetRoomRequest{RoomId: req.GetRoom().GetRoomId()})
	if err != nil {
		return nil, err
	}
	curRoom := curResp.GetRoom()
	fieldmask.Update(req.GetUpdateMask(), curRoom, req.GetRoom())
	rt, err := s.uc.UpdateRoom(ctx, convertRoom(curRoom))
	if err != nil {
		return nil, err
	}
	return &v1.UpdateRoomResponse{Room: convertRoomReply(rt)}, nil
}

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
		RoomID:       in.GetRoomId(),
		StreamerName: in.GetStreamerName(),
		RoomTitle:    in.GetRoomTitle(),
		Enabled:      in.GetEnabled(),
	}
}

// convertRoomReply converts the merged runtime view back to a DTO.
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
		StreamerName: rt.Room.StreamerName,
		RoomTitle:    rt.Room.RoomTitle,
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
