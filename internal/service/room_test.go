package service

import (
	"context"
	"errors"
	"testing"
	"time"

	v1 "suika/api/room/v1"
	"suika/internal/biz"
	"suika/internal/conf"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// fakeSessionStatsRepo implements biz.SessionStatsRepo with no storage IO.
type fakeSessionStatsRepo struct {
	stats map[int64]*biz.SessionStats
	errs  map[int64]error
	calls []int64
}

func (f *fakeSessionStatsRepo) SessionStats(_ context.Context, roomID int64) (*biz.SessionStats, error) {
	f.calls = append(f.calls, roomID)
	if err, ok := f.errs[roomID]; ok {
		return nil, err
	}
	return f.stats[roomID], nil
}

func newTestRoomService(c *conf.Recorder, stats biz.SessionStatsRepo) (*RoomService, *biz.RoomRegistry) {
	reg := biz.NewRoomRegistry(c)
	uc := biz.NewRoomUsecase(reg, stats)
	return NewRoomService(uc), reg
}

func TestRoomServiceListRooms(t *testing.T) {
	ctx := context.Background()
	c := &conf.Recorder{
		Rooms: []*conf.Recorder_Room{
			{RoomId: 1001, Name: "live-room", Enabled: true},
			{RoomId: 2002, Name: "disabled-room", Enabled: false},
			{RoomId: 3003, Name: "recording-room", Enabled: true},
			{RoomId: 4004, Name: "stats-error-room", Enabled: true},
			{RoomId: 5005, Name: "remuxing-room", Enabled: true},
		},
	}
	stats := &fakeSessionStatsRepo{
		stats: map[int64]*biz.SessionStats{
			3003: {CurrentFile: "recordings/recording-room/part-0001.flv", BytesWritten: 123456789},
		},
		errs: map[int64]error{
			4004: errors.New("storage unavailable"),
		},
	}
	svc, reg := newTestRoomService(c, stats)

	// Simulate the daemon's state writes through the shared registry.
	reg.ApplyRoomInfo(3003, &biz.RoomInfo{RoomID: 3003, Live: true})
	reg.StartRecording(3003)
	reg.StartRecording(4004)
	reg.SetRemuxing(5005)

	reply, err := svc.ListRooms(ctx, &v1.ListRoomsRequest{})
	if err != nil {
		t.Fatalf("ListRooms() error = %v", err)
	}
	rooms := reply.GetRooms()
	if len(rooms) != 5 {
		t.Fatalf("ListRooms() rooms len = %d, want 5", len(rooms))
	}
	// Disabled rooms are listed too, in configuration order.
	if rooms[0].GetRoomId() != 1001 || rooms[0].GetName() != "live-room" || !rooms[0].GetEnabled() {
		t.Fatalf("ListRooms() first room = %+v, want enabled room 1001 in config order", rooms[0])
	}
	if rooms[1].GetRoomId() != 2002 || rooms[1].GetName() != "disabled-room" || rooms[1].GetEnabled() {
		t.Fatalf("ListRooms() second room = %+v, want disabled room 2002 in config order", rooms[1])
	}
	// Fresh rooms start unknown/idle with no session in flight.
	if rooms[0].GetLiveStatus() != v1.LiveStatus_LIVE_STATUS_UNSPECIFIED {
		t.Fatalf("ListRooms() live_status = %v, want LIVE_STATUS_UNSPECIFIED", rooms[0].GetLiveStatus())
	}
	if rooms[0].GetRecordStatus() != v1.RecordStatus_IDLE {
		t.Fatalf("ListRooms() record_status = %v, want IDLE", rooms[0].GetRecordStatus())
	}
	if rooms[0].GetSessionStartedAt() != nil {
		t.Fatalf("ListRooms() session_started_at = %v, want nil for idle room", rooms[0].GetSessionStartedAt())
	}
	if rooms[0].GetCurrentFile() != "" || rooms[0].GetBytesWritten() != 0 || rooms[0].GetLastError() != "" {
		t.Fatalf("ListRooms() idle room progress = %+v, want zero values", rooms[0])
	}
	// Recording rooms get the live state mapped and session stats merged in.
	if rooms[2].GetRoomId() != 3003 || rooms[2].GetName() != "recording-room" {
		t.Fatalf("ListRooms() third room = %+v, want recording room 3003 in config order", rooms[2])
	}
	if rooms[2].GetLiveStatus() != v1.LiveStatus_LIVE {
		t.Fatalf("ListRooms() recording room live_status = %v, want LIVE", rooms[2].GetLiveStatus())
	}
	if rooms[2].GetRecordStatus() != v1.RecordStatus_RECORDING {
		t.Fatalf("ListRooms() recording room record_status = %v, want RECORDING", rooms[2].GetRecordStatus())
	}
	if rooms[2].GetCurrentFile() != "recordings/recording-room/part-0001.flv" || rooms[2].GetBytesWritten() != 123456789 {
		t.Fatalf("ListRooms() recording room progress = %+v, want session stats merged", rooms[2])
	}
	if rooms[2].GetSessionStartedAt() == nil {
		t.Fatalf("ListRooms() recording room session_started_at = %v, want set", rooms[2].GetSessionStartedAt())
	}
	// A recording room whose stats lookup fails is still listed, without progress.
	if rooms[3].GetRoomId() != 4004 || rooms[3].GetRecordStatus() != v1.RecordStatus_RECORDING {
		t.Fatalf("ListRooms() fourth room = %+v, want recording room 4004", rooms[3])
	}
	if rooms[3].GetCurrentFile() != "" || rooms[3].GetBytesWritten() != 0 {
		t.Fatalf("ListRooms() stats-error room progress = %+v, want zero values on stats error", rooms[3])
	}
	// Remuxing rooms stay listed without a stats lookup.
	if rooms[4].GetRoomId() != 5005 || rooms[4].GetRecordStatus() != v1.RecordStatus_REMUXING {
		t.Fatalf("ListRooms() fifth room = %+v, want remuxing room 5005", rooms[4])
	}
	// Session stats are only queried for recording rooms.
	if len(stats.calls) != 2 || stats.calls[0] != 3003 || stats.calls[1] != 4004 {
		t.Fatalf("SessionStats() calls = %v, want exactly [3003 4004]", stats.calls)
	}
}

func TestConvertRoomStatus(t *testing.T) {
	startedAt := time.Date(2026, 8, 11, 12, 30, 0, 0, time.UTC)
	tests := []struct {
		name string
		in   *biz.RoomRuntime
		want *v1.RoomStatus
	}{
		{
			name: "unknown and idle with zero time",
			in: &biz.RoomRuntime{
				Room: biz.Room{RoomID: 1, Name: "room-one", Enabled: true},
			},
			want: &v1.RoomStatus{
				RoomId:       1,
				Name:         "room-one",
				Enabled:      true,
				LiveStatus:   v1.LiveStatus_LIVE_STATUS_UNSPECIFIED,
				RecordStatus: v1.RecordStatus_IDLE,
			},
		},
		{
			name: "preparing",
			in: &biz.RoomRuntime{
				Room: biz.Room{RoomID: 2, Name: "room-two"},
				Live: biz.LivePreparing,
			},
			want: &v1.RoomStatus{
				RoomId:       2,
				Name:         "room-two",
				LiveStatus:   v1.LiveStatus_PREPARING,
				RecordStatus: v1.RecordStatus_IDLE,
			},
		},
		{
			name: "on air recording passes progress through",
			in: &biz.RoomRuntime{
				Room:             biz.Room{RoomID: 3, Name: "room-three", Enabled: true},
				Live:             biz.LiveOnAir,
				Record:           biz.RecordRecording,
				CurrentFile:      "recordings/room-three/part-0001.flv",
				BytesWritten:     123456789,
				SessionStartedAt: startedAt,
			},
			want: &v1.RoomStatus{
				RoomId:           3,
				Name:             "room-three",
				Enabled:          true,
				LiveStatus:       v1.LiveStatus_LIVE,
				RecordStatus:     v1.RecordStatus_RECORDING,
				CurrentFile:      "recordings/room-three/part-0001.flv",
				BytesWritten:     123456789,
				SessionStartedAt: timestamppb.New(startedAt),
			},
		},
		{
			name: "remuxing",
			in: &biz.RoomRuntime{
				Room:   biz.Room{RoomID: 4, Name: "room-four"},
				Record: biz.RecordRemuxing,
			},
			want: &v1.RoomStatus{
				RoomId:       4,
				Name:         "room-four",
				LiveStatus:   v1.LiveStatus_LIVE_STATUS_UNSPECIFIED,
				RecordStatus: v1.RecordStatus_REMUXING,
			},
		},
		{
			name: "error passes last_error through",
			in: &biz.RoomRuntime{
				Room:      biz.Room{RoomID: 5, Name: "room-five", Enabled: true},
				Live:      biz.LivePreparing,
				Record:    biz.RecordError,
				LastError: "prepare session failed: disk full",
			},
			want: &v1.RoomStatus{
				RoomId:       5,
				Name:         "room-five",
				Enabled:      true,
				LiveStatus:   v1.LiveStatus_PREPARING,
				RecordStatus: v1.RecordStatus_ERROR,
				LastError:    "prepare session failed: disk full",
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := convertRoomStatus(tt.in)
			if !proto.Equal(got, tt.want) {
				t.Fatalf("convertRoomStatus() = %+v, want %+v", got, tt.want)
			}
		})
	}

	if got := convertRoomStatus(nil); got != nil {
		t.Fatalf("convertRoomStatus(nil) = %+v, want nil", got)
	}
}
