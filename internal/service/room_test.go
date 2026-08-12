package service

import (
	"context"
	stderrors "errors"
	"fmt"
	"path/filepath"
	"testing"
	"time"

	v1 "suika/api/room/v1"
	"suika/internal/biz"
	"suika/internal/conf"
	"suika/internal/data"

	kratoserrors "github.com/go-kratos/kratos/v3/errors"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/fieldmaskpb"
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

// newTestData builds a real *data.Data backed by a fresh sqlite file.
func newTestData(t *testing.T) *data.Data {
	t.Helper()
	confData := &conf.Data{
		Database: &conf.Data_Database{
			Driver: "sqlite",
			Source: filepath.Join(t.TempDir(), "test.db"),
		},
	}
	// RemuxEnabled=false keeps NewData from probing for ffmpeg.
	d, cleanup, err := data.NewData(confData, &conf.Recorder{RemuxEnabled: proto.Bool(false)})
	if err != nil {
		t.Fatalf("NewData() error = %v", err)
	}
	t.Cleanup(cleanup)
	return d
}

// roomEnv is the full service chain over one *data.Data, as wireApp builds
// it. Building a second env over the same data simulates a restart: the
// registry re-loads the persisted rooms once at construction.
type roomEnv struct {
	svc   *RoomService
	reg   *biz.RoomRegistry
	stats *fakeSessionStatsRepo
}

func newTestRoomEnv(t *testing.T, d *data.Data) *roomEnv {
	t.Helper()
	repo := data.NewRoomRepo(d)
	reg, err := biz.NewRoomRegistry(repo)
	if err != nil {
		t.Fatalf("NewRoomRegistry() error = %v", err)
	}
	stats := &fakeSessionStatsRepo{}
	uc := biz.NewRoomUsecase(repo, reg, stats)
	return &roomEnv{svc: NewRoomService(uc), reg: reg, stats: stats}
}

func TestRoomServiceCRUD(t *testing.T) {
	ctx := context.Background()
	svc := newTestRoomEnv(t, newTestData(t)).svc

	created, err := svc.CreateRoom(ctx, &v1.CreateRoomRequest{
		Room: &v1.Room{RoomId: 1001, Name: "streamer-a", Enabled: true},
	})
	if err != nil {
		t.Fatalf("CreateRoom() error = %v", err)
	}
	if created.GetRoomId() != 1001 || created.GetName() != "streamer-a" || !created.GetEnabled() {
		t.Fatalf("CreateRoom() = %+v, want created room", created)
	}
	if created.GetCreateTime() == nil || created.GetUpdateTime() == nil {
		t.Fatal("CreateRoom() did not set timestamps")
	}
	// Runtime fields carry default values on create responses.
	if created.GetLiveStatus() != v1.LiveStatus_LIVE_STATUS_UNSPECIFIED {
		t.Fatalf("CreateRoom() live_status = %v, want LIVE_STATUS_UNSPECIFIED", created.GetLiveStatus())
	}
	if created.GetRecordStatus() != v1.RecordStatus_IDLE {
		t.Fatalf("CreateRoom() record_status = %v, want IDLE", created.GetRecordStatus())
	}
	if created.GetSessionStartedAt() != nil || created.GetCurrentFile() != "" || created.GetBytesWritten() != 0 || created.GetLastError() != "" {
		t.Fatalf("CreateRoom() runtime progress = %+v, want zero values", created)
	}

	got, err := svc.GetRoom(ctx, &v1.GetRoomRequest{RoomId: 1001})
	if err != nil {
		t.Fatalf("GetRoom() error = %v", err)
	}
	if got.GetName() != "streamer-a" || !got.GetEnabled() {
		t.Fatalf("GetRoom() = %+v, want created room", got)
	}

	updated, err := svc.UpdateRoom(ctx, &v1.UpdateRoomRequest{
		Room:       &v1.Room{RoomId: 1001, Name: "streamer-b"},
		UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"name"}},
	})
	if err != nil {
		t.Fatalf("UpdateRoom(name) error = %v", err)
	}
	if updated.GetName() != "streamer-b" || !updated.GetEnabled() {
		t.Fatalf("UpdateRoom(name) = %+v, want renamed room with enabled kept", updated)
	}

	disabled, err := svc.UpdateRoom(ctx, &v1.UpdateRoomRequest{
		Room:       &v1.Room{RoomId: 1001, Enabled: false},
		UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"enabled"}},
	})
	if err != nil {
		t.Fatalf("UpdateRoom(enabled) error = %v", err)
	}
	if disabled.GetEnabled() || disabled.GetName() != "streamer-b" {
		t.Fatalf("UpdateRoom(enabled) = %+v, want disabled room with name kept", disabled)
	}

	if _, err := svc.DeleteRoom(ctx, &v1.DeleteRoomRequest{RoomId: 1001}); err != nil {
		t.Fatalf("DeleteRoom() error = %v", err)
	}
	if _, err := svc.GetRoom(ctx, &v1.GetRoomRequest{RoomId: 1001}); !kratoserrors.IsNotFound(err) {
		t.Fatalf("GetRoom() after delete error = %v, want not found", err)
	}

	// An empty name is accepted on create (platform backfill later).
	if _, err := svc.CreateRoom(ctx, &v1.CreateRoomRequest{Room: &v1.Room{RoomId: 2002}}); err != nil {
		t.Fatalf("CreateRoom(empty name) error = %v", err)
	}
}

func TestRoomServiceListRoomsPagination(t *testing.T) {
	ctx := context.Background()
	svc := newTestRoomEnv(t, newTestData(t)).svc

	for _, id := range []int64{1, 2, 3} {
		if _, err := svc.CreateRoom(ctx, &v1.CreateRoomRequest{
			Room: &v1.Room{RoomId: id, Name: fmt.Sprintf("room-%d", id)},
		}); err != nil {
			t.Fatalf("CreateRoom(%d) error = %v", id, err)
		}
	}

	firstPage, err := svc.ListRooms(ctx, &v1.ListRoomsRequest{PageSize: 2})
	if err != nil {
		t.Fatalf("ListRooms(first page) error = %v", err)
	}
	if len(firstPage.GetRooms()) != 2 {
		t.Fatalf("ListRooms(first page) len = %d, want 2", len(firstPage.GetRooms()))
	}
	if firstPage.GetNextPageToken() == "" {
		t.Fatal("ListRooms(first page) next_page_token is empty")
	}
	if firstPage.GetRooms()[0].GetRoomId() != 1 {
		t.Fatalf("ListRooms(first page) first id = %d, want room_id order", firstPage.GetRooms()[0].GetRoomId())
	}

	secondPage, err := svc.ListRooms(ctx, &v1.ListRoomsRequest{
		PageSize:  2,
		PageToken: firstPage.GetNextPageToken(),
	})
	if err != nil {
		t.Fatalf("ListRooms(second page) error = %v", err)
	}
	if len(secondPage.GetRooms()) != 1 {
		t.Fatalf("ListRooms(second page) len = %d, want 1", len(secondPage.GetRooms()))
	}
	if secondPage.GetNextPageToken() != "" {
		t.Fatalf("ListRooms(second page) next_page_token = %q, want empty", secondPage.GetNextPageToken())
	}
	if secondPage.GetRooms()[0].GetRoomId() != 3 {
		t.Fatalf("ListRooms(second page) id = %d, want 3", secondPage.GetRooms()[0].GetRoomId())
	}
}

func TestRoomServiceListRoomsFilterAndOrderBy(t *testing.T) {
	ctx := context.Background()
	svc := newTestRoomEnv(t, newTestData(t)).svc

	for _, room := range []*v1.Room{
		{RoomId: 1, Name: "alpha-streamer", Enabled: true},
		{RoomId: 2, Name: "beta-streamer", Enabled: false},
		{RoomId: 3, Name: "gamma", Enabled: true},
	} {
		if _, err := svc.CreateRoom(ctx, &v1.CreateRoomRequest{Room: room}); err != nil {
			t.Fatalf("CreateRoom(%d) error = %v", room.GetRoomId(), err)
		}
	}

	// Substring match on name combined with a bare boolean ident.
	filtered, err := svc.ListRooms(ctx, &v1.ListRoomsRequest{
		PageSize: 10,
		Filter:   `name:"streamer" AND enabled`,
	})
	if err != nil {
		t.Fatalf("ListRooms(filter) error = %v", err)
	}
	if len(filtered.GetRooms()) != 1 || filtered.GetRooms()[0].GetRoomId() != 1 {
		t.Fatalf("ListRooms(filter) = %+v, want only enabled streamer room 1", filtered.GetRooms())
	}

	ordered, err := svc.ListRooms(ctx, &v1.ListRoomsRequest{PageSize: 10, OrderBy: "room_id desc"})
	if err != nil {
		t.Fatalf("ListRooms(order_by) error = %v", err)
	}
	if len(ordered.GetRooms()) != 3 ||
		ordered.GetRooms()[0].GetRoomId() != 3 ||
		ordered.GetRooms()[1].GetRoomId() != 2 ||
		ordered.GetRooms()[2].GetRoomId() != 1 {
		t.Fatalf("ListRooms(order_by) = %+v, want room_id desc", ordered.GetRooms())
	}

	// Timestamp ranges accept bare RFC3339 strings.
	ranged, err := svc.ListRooms(ctx, &v1.ListRoomsRequest{
		PageSize: 10,
		Filter:   `create_time >= "2020-01-01T00:00:00Z"`,
	})
	if err != nil {
		t.Fatalf("ListRooms(create_time range) error = %v", err)
	}
	if len(ranged.GetRooms()) != 3 {
		t.Fatalf("ListRooms(create_time range) len = %d, want 3", len(ranged.GetRooms()))
	}
}

func TestRoomServiceValidation(t *testing.T) {
	ctx := context.Background()
	svc := newTestRoomEnv(t, newTestData(t)).svc

	if _, err := svc.CreateRoom(ctx, &v1.CreateRoomRequest{Room: &v1.Room{RoomId: 0, Name: "bad"}}); !kratoserrors.IsBadRequest(err) {
		t.Fatalf("CreateRoom(zero id) error = %v, want bad request", err)
	}
	if _, err := svc.CreateRoom(ctx, &v1.CreateRoomRequest{Room: &v1.Room{RoomId: -1}}); !kratoserrors.IsBadRequest(err) {
		t.Fatalf("CreateRoom(negative id) error = %v, want bad request", err)
	}

	if _, err := svc.CreateRoom(ctx, &v1.CreateRoomRequest{Room: &v1.Room{RoomId: 1001, Name: "first"}}); err != nil {
		t.Fatalf("CreateRoom() error = %v", err)
	}
	if _, err := svc.CreateRoom(ctx, &v1.CreateRoomRequest{Room: &v1.Room{RoomId: 1001}}); !kratoserrors.IsConflict(err) {
		t.Fatalf("CreateRoom(duplicate) error = %v, want conflict", err)
	}

	if _, err := svc.UpdateRoom(ctx, &v1.UpdateRoomRequest{
		Room:       &v1.Room{RoomId: 1001, Name: "x"},
		UpdateMask: &fieldmaskpb.FieldMask{},
	}); !kratoserrors.IsBadRequest(err) {
		t.Fatalf("UpdateRoom(empty mask) error = %v, want bad request", err)
	}
	if _, err := svc.UpdateRoom(ctx, &v1.UpdateRoomRequest{
		Room:       &v1.Room{RoomId: 1001, Name: "x"},
		UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"room_id"}},
	}); !kratoserrors.IsBadRequest(err) {
		t.Fatalf("UpdateRoom(room_id path) error = %v, want bad request", err)
	}
	if _, err := svc.UpdateRoom(ctx, &v1.UpdateRoomRequest{
		Room:       &v1.Room{RoomId: 9999, Name: "x"},
		UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"name"}},
	}); !kratoserrors.IsNotFound(err) {
		t.Fatalf("UpdateRoom(missing) error = %v, want not found", err)
	}
	if _, err := svc.DeleteRoom(ctx, &v1.DeleteRoomRequest{RoomId: 9999}); !kratoserrors.IsNotFound(err) {
		t.Fatalf("DeleteRoom(missing) error = %v, want not found", err)
	}

	if _, err := svc.ListRooms(ctx, &v1.ListRoomsRequest{PageToken: "bad-token"}); err == nil {
		t.Fatal("ListRooms(bad token) error = nil, want error")
	}
	if _, err := svc.ListRooms(ctx, &v1.ListRoomsRequest{Filter: `unknown:"value"`}); err == nil {
		t.Fatal("ListRooms(unsupported filter) error = nil, want error")
	}
	if _, err := svc.ListRooms(ctx, &v1.ListRoomsRequest{Filter: `live_status=2`}); err == nil {
		t.Fatal("ListRooms(runtime filter) error = nil, want error")
	}
	if _, err := svc.ListRooms(ctx, &v1.ListRoomsRequest{OrderBy: "live_status"}); err == nil {
		t.Fatal("ListRooms(unsupported order_by) error = nil, want error")
	}
}

func TestRoomServiceListRoomsMergesRuntime(t *testing.T) {
	ctx := context.Background()
	d := newTestData(t)
	env := newTestRoomEnv(t, d)

	for _, room := range []*v1.Room{
		{RoomId: 1001, Name: "live-room", Enabled: true},
		{RoomId: 2002, Name: "disabled-room", Enabled: false},
		{RoomId: 3003, Name: "recording-room", Enabled: true},
		{RoomId: 4004, Name: "stats-error-room", Enabled: true},
		{RoomId: 5005, Name: "remuxing-room", Enabled: true},
	} {
		if _, err := env.svc.CreateRoom(ctx, &v1.CreateRoomRequest{Room: room}); err != nil {
			t.Fatalf("CreateRoom(%d) error = %v", room.GetRoomId(), err)
		}
	}

	// Restart: a fresh registry over the same database re-loads the rooms.
	env = newTestRoomEnv(t, d)
	env.stats.stats = map[int64]*biz.SessionStats{
		3003: {CurrentFile: "recordings/recording-room/part-0001.flv", BytesWritten: 123456789},
	}
	env.stats.errs = map[int64]error{
		4004: stderrors.New("storage unavailable"),
	}

	// Simulate the daemon's state writes through the shared registry.
	env.reg.ApplyRoomInfo(ctx, 3003, &biz.RoomInfo{RoomID: 3003, Live: true})
	env.reg.StartRecording(3003)
	env.reg.StartRecording(4004)
	env.reg.SetRemuxing(5005)

	reply, err := env.svc.ListRooms(ctx, &v1.ListRoomsRequest{PageSize: 10})
	if err != nil {
		t.Fatalf("ListRooms() error = %v", err)
	}
	rooms := reply.GetRooms()
	if len(rooms) != 5 {
		t.Fatalf("ListRooms() rooms len = %d, want 5", len(rooms))
	}
	if rooms[0].GetRoomId() != 1001 || rooms[0].GetName() != "live-room" || !rooms[0].GetEnabled() {
		t.Fatalf("ListRooms() first room = %+v, want room 1001 in room_id order", rooms[0])
	}
	if rooms[1].GetRoomId() != 2002 || rooms[1].GetName() != "disabled-room" || rooms[1].GetEnabled() {
		t.Fatalf("ListRooms() second room = %+v, want disabled room 2002", rooms[1])
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
	// Recording rooms get the live state mapped and session stats merged in.
	if rooms[2].GetRoomId() != 3003 || rooms[2].GetName() != "recording-room" {
		t.Fatalf("ListRooms() third room = %+v, want recording room 3003", rooms[2])
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
	if len(env.stats.calls) != 2 || env.stats.calls[0] != 3003 || env.stats.calls[1] != 4004 {
		t.Fatalf("SessionStats() calls = %v, want exactly [3003 4004]", env.stats.calls)
	}

	// Rooms created after startup are served from the database with default
	// runtime values: CRUD does not hot-load the recorder registry.
	if _, err := env.svc.CreateRoom(ctx, &v1.CreateRoomRequest{
		Room: &v1.Room{RoomId: 6006, Name: "late-room", Enabled: true},
	}); err != nil {
		t.Fatalf("CreateRoom(late) error = %v", err)
	}
	late, err := env.svc.GetRoom(ctx, &v1.GetRoomRequest{RoomId: 6006})
	if err != nil {
		t.Fatalf("GetRoom(late) error = %v", err)
	}
	if late.GetLiveStatus() != v1.LiveStatus_LIVE_STATUS_UNSPECIFIED || late.GetRecordStatus() != v1.RecordStatus_IDLE {
		t.Fatalf("GetRoom(late) runtime = %v/%v, want default values", late.GetLiveStatus(), late.GetRecordStatus())
	}
}

func TestConvertRoomReply(t *testing.T) {
	startedAt := time.Date(2026, 8, 11, 12, 30, 0, 0, time.UTC)
	createdAt := time.Date(2026, 8, 1, 0, 0, 0, 0, time.UTC)
	updatedAt := time.Date(2026, 8, 10, 0, 0, 0, 0, time.UTC)
	tests := []struct {
		name string
		in   *biz.RoomRuntime
		want *v1.Room
	}{
		{
			name: "unknown and idle with zero time",
			in: &biz.RoomRuntime{
				Room: biz.Room{RoomID: 1, Name: "room-one", Enabled: true},
			},
			want: &v1.Room{
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
			want: &v1.Room{
				RoomId:       2,
				Name:         "room-two",
				LiveStatus:   v1.LiveStatus_PREPARING,
				RecordStatus: v1.RecordStatus_IDLE,
			},
		},
		{
			name: "on air recording passes progress through",
			in: &biz.RoomRuntime{
				Room:             biz.Room{RoomID: 3, Name: "room-three", Enabled: true, CreateTime: createdAt, UpdateTime: updatedAt},
				Live:             biz.LiveOnAir,
				Record:           biz.RecordRecording,
				CurrentFile:      "recordings/room-three/part-0001.flv",
				BytesWritten:     123456789,
				SessionStartedAt: startedAt,
			},
			want: &v1.Room{
				RoomId:           3,
				Name:             "room-three",
				Enabled:          true,
				LiveStatus:       v1.LiveStatus_LIVE,
				RecordStatus:     v1.RecordStatus_RECORDING,
				CurrentFile:      "recordings/room-three/part-0001.flv",
				BytesWritten:     123456789,
				SessionStartedAt: timestamppb.New(startedAt),
				CreateTime:       timestamppb.New(createdAt),
				UpdateTime:       timestamppb.New(updatedAt),
			},
		},
		{
			name: "remuxing",
			in: &biz.RoomRuntime{
				Room:   biz.Room{RoomID: 4, Name: "room-four"},
				Record: biz.RecordRemuxing,
			},
			want: &v1.Room{
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
			want: &v1.Room{
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
			got := convertRoomReply(tt.in)
			if !proto.Equal(got, tt.want) {
				t.Fatalf("convertRoomReply() = %+v, want %+v", got, tt.want)
			}
		})
	}

	if got := convertRoomReply(nil); got != nil {
		t.Fatalf("convertRoomReply(nil) = %+v, want nil", got)
	}
}
