package service

import (
	"context"

	v1 "suika/api/room/v1"
	"suika/internal/biz"

	"google.golang.org/protobuf/types/known/timestamppb"
)

// RoomService is a room service.
type RoomService struct {
	v1.UnimplementedRoomServiceServer

	uc *biz.RoomUsecase
}

// NewRoomService new a room service.
func NewRoomService(uc *biz.RoomUsecase) *RoomService {
	return &RoomService{uc: uc}
}

// ListRooms returns the live/record status of every configured room.
func (s *RoomService) ListRooms(ctx context.Context, req *v1.ListRoomsRequest) (*v1.RoomSet, error) {
	rts, err := s.uc.ListRooms(ctx)
	if err != nil {
		return nil, err
	}
	reply := &v1.RoomSet{
		Rooms: make([]*v1.Room, 0, len(rts)),
	}
	for _, rt := range rts {
		reply.Rooms = append(reply.Rooms, convertRoomReply(rt))
	}
	return reply, nil
}

func convertRoomReply(rt *biz.RoomRuntime) *v1.Room {
	if rt == nil {
		return nil
	}
	var liveStatus v1.LiveStatus
	switch rt.Live {
	case biz.LivePreparing:
		liveStatus = v1.LiveStatus_PREPARING
	case biz.LiveOnAir:
		liveStatus = v1.LiveStatus_LIVE
	default:
		liveStatus = v1.LiveStatus_LIVE_STATUS_UNSPECIFIED
	}
	var recordStatus v1.RecordStatus
	switch rt.Record {
	case biz.RecordRecording:
		recordStatus = v1.RecordStatus_RECORDING
	case biz.RecordRemuxing:
		recordStatus = v1.RecordStatus_REMUXING
	case biz.RecordError:
		recordStatus = v1.RecordStatus_ERROR
	default:
		recordStatus = v1.RecordStatus_IDLE
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
	if !rt.SessionStartedAt.IsZero() {
		room.SessionStartedAt = timestamppb.New(rt.SessionStartedAt)
	}
	return room
}
