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

// fakeSessionStatsRepo 实现 biz.SessionStatsRepo，不进行存储 IO。
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

// newTestData 构建以全新 sqlite 文件为后端的真实 *data.Data。
func newTestData(t *testing.T) *data.Data {
	t.Helper()
	confData := &conf.Data{
		Database: &conf.Data_Database{
			Source: filepath.Join(t.TempDir(), "test.db"),
		},
	}
	// RemuxEnabled=false 使 NewData 不去探测 ffmpeg。
	d, cleanup, err := data.NewData(confData, &conf.Recorder{RemuxEnabled: proto.Bool(false)})
	if err != nil {
		t.Fatalf("NewData() error = %v", err)
	}
	t.Cleanup(cleanup)
	return d
}

// roomEnv 是在同一个 *data.Data 上按 wireApp 的方式搭起的完整服务链。
// 在同一个 data 上再建一个 env 即模拟一次重启：RoomRegistry 只在构造时
// 重新加载持久化的 Room。
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
		Room: &v1.Room{RoomId: 1001, RecordEnabled: true},
	})
	if err != nil {
		t.Fatalf("CreateRoom() error = %v", err)
	}
	createdRoom := created.GetRoom()
	if createdRoom.GetRoomId() != 1001 || createdRoom.GetStreamerName() != "" || createdRoom.GetRoomTitle() != "" || !createdRoom.GetRecordEnabled() {
		t.Fatalf("CreateRoom() = %+v, want created room", created)
	}
	if createdRoom.GetCreateTime() == nil || createdRoom.GetUpdateTime() == nil {
		t.Fatal("CreateRoom() did not set timestamps")
	}
	// 创建响应中的运行时字段携带默认值。
	if createdRoom.GetLiveStatus() != v1.LiveStatus_LIVE_STATUS_UNSPECIFIED {
		t.Fatalf("CreateRoom() live_status = %v, want LIVE_STATUS_UNSPECIFIED", createdRoom.GetLiveStatus())
	}
	if createdRoom.GetRecordStatus() != v1.RecordStatus_RECORD_STATUS_IDLE {
		t.Fatalf("CreateRoom() record_status = %v, want IDLE", createdRoom.GetRecordStatus())
	}
	if createdRoom.GetSessionStartedAt() != nil || createdRoom.GetCurrentFile() != "" || createdRoom.GetBytesWritten() != 0 || createdRoom.GetLastError() != "" {
		t.Fatalf("CreateRoom() runtime progress = %+v, want zero values", created)
	}

	got, err := svc.GetRoom(ctx, &v1.GetRoomRequest{RoomId: 1001})
	if err != nil {
		t.Fatalf("GetRoom() error = %v", err)
	}
	gotRoom := got.GetRoom()
	if gotRoom.GetStreamerName() != "" || gotRoom.GetRoomTitle() != "" || !gotRoom.GetRecordEnabled() {
		t.Fatalf("GetRoom() = %+v, want created room", got)
	}

	updated, err := svc.UpdateRoom(ctx, &v1.UpdateRoomRequest{
		Room:       &v1.Room{RoomId: 1001, RecordEnabled: false},
		UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"record_enabled"}},
	})
	if err != nil || updated.GetRoom().GetRecordEnabled() {
		t.Fatalf("UpdateRoom(record_enabled) = %+v, error = %v, want disabled", updated, err)
	}
	if _, err := svc.UpdateRoom(ctx, &v1.UpdateRoomRequest{
		Room:       &v1.Room{RoomId: 1001, StreamerName: "platform-owned"},
		UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"streamer_name"}},
	}); !kratoserrors.IsBadRequest(err) {
		t.Fatalf("UpdateRoom(streamer_name) error = %v, want bad request", err)
	}

	if _, err := svc.DeleteRoom(ctx, &v1.DeleteRoomRequest{RoomId: 1001}); err != nil {
		t.Fatalf("DeleteRoom() error = %v", err)
	}
	if _, err := svc.GetRoom(ctx, &v1.GetRoomRequest{RoomId: 1001}); !kratoserrors.IsNotFound(err) {
		t.Fatalf("GetRoom() after delete error = %v, want not found", err)
	}

	// 创建时允许空元数据（之后由平台回填）。
	if _, err := svc.CreateRoom(ctx, &v1.CreateRoomRequest{Room: &v1.Room{RoomId: 2002}}); err != nil {
		t.Fatalf("CreateRoom(empty metadata) error = %v", err)
	}
}

func TestRoomServiceListRoomsPagination(t *testing.T) {
	ctx := context.Background()
	svc := newTestRoomEnv(t, newTestData(t)).svc

	for _, id := range []int64{1, 2, 3} {
		if _, err := svc.CreateRoom(ctx, &v1.CreateRoomRequest{
			Room: &v1.Room{RoomId: id, StreamerName: fmt.Sprintf("room-%d", id)},
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

func TestRoomServiceListRoomsOptionalQuery(t *testing.T) {
	ctx := context.Background()
	svc := newTestRoomEnv(t, newTestData(t)).svc

	for _, room := range []*v1.Room{
		{RoomId: 1, StreamerName: "alpha-streamer", RecordEnabled: true},
		{RoomId: 2, StreamerName: "beta-streamer", RecordEnabled: false},
		{RoomId: 3, StreamerName: "gamma", RecordEnabled: true},
	} {
		if _, err := svc.CreateRoom(ctx, &v1.CreateRoomRequest{Room: room}); err != nil {
			t.Fatalf("CreateRoom(%d) error = %v", room.GetRoomId(), err)
		}
	}

	byRecordEnabled, err := svc.ListRooms(ctx, &v1.ListRoomsRequest{
		PageSize:      10,
		RecordEnabled: proto.Bool(true),
	})
	if err != nil {
		t.Fatalf("ListRooms(record_enabled=true) error = %v", err)
	}
	if len(byRecordEnabled.GetRooms()) != 2 || byRecordEnabled.GetRooms()[0].GetRoomId() != 1 || byRecordEnabled.GetRooms()[1].GetRoomId() != 3 {
		t.Fatalf("ListRooms(record_enabled=true) = %+v, want rooms 1 and 3", byRecordEnabled.GetRooms())
	}

	byRoomID, err := svc.ListRooms(ctx, &v1.ListRoomsRequest{
		PageSize: 10,
		RoomId:   proto.Int64(2),
	})
	if err != nil {
		t.Fatalf("ListRooms(room_id=2) error = %v", err)
	}
	if len(byRoomID.GetRooms()) != 1 || byRoomID.GetRooms()[0].GetRoomId() != 2 {
		t.Fatalf("ListRooms(room_id=2) = %+v, want only room 2", byRoomID.GetRooms())
	}

	byName, err := svc.ListRooms(ctx, &v1.ListRoomsRequest{
		PageSize:     10,
		StreamerName: proto.String("gamma"),
	})
	if err != nil {
		t.Fatalf("ListRooms(streamer_name=gamma) error = %v", err)
	}
	if len(byName.GetRooms()) != 1 || byName.GetRooms()[0].GetRoomId() != 3 {
		t.Fatalf("ListRooms(streamer_name=gamma) = %+v, want only room 3", byName.GetRooms())
	}
}

func TestRoomServiceValidation(t *testing.T) {
	ctx := context.Background()
	svc := newTestRoomEnv(t, newTestData(t)).svc

	if _, err := svc.CreateRoom(ctx, &v1.CreateRoomRequest{Room: &v1.Room{RoomId: 0, StreamerName: "bad"}}); !kratoserrors.IsBadRequest(err) {
		t.Fatalf("CreateRoom(zero id) error = %v, want bad request", err)
	}
	if _, err := svc.CreateRoom(ctx, &v1.CreateRoomRequest{Room: &v1.Room{RoomId: -1}}); !kratoserrors.IsBadRequest(err) {
		t.Fatalf("CreateRoom(negative id) error = %v, want bad request", err)
	}

	if _, err := svc.CreateRoom(ctx, &v1.CreateRoomRequest{Room: &v1.Room{RoomId: 1001, StreamerName: "first"}}); err != nil {
		t.Fatalf("CreateRoom() error = %v", err)
	}
	if _, err := svc.CreateRoom(ctx, &v1.CreateRoomRequest{Room: &v1.Room{RoomId: 1001}}); !kratoserrors.IsConflict(err) {
		t.Fatalf("CreateRoom(duplicate) error = %v, want conflict", err)
	}

	if _, err := svc.DeleteRoom(ctx, &v1.DeleteRoomRequest{RoomId: 9999}); !kratoserrors.IsNotFound(err) {
		t.Fatalf("DeleteRoom(missing) error = %v, want not found", err)
	}

	if _, err := svc.ListRooms(ctx, &v1.ListRoomsRequest{PageToken: "bad-token"}); err == nil {
		t.Fatal("ListRooms(bad token) error = nil, want error")
	}
	if _, err := svc.ListRooms(ctx, nil); !kratoserrors.IsBadRequest(err) {
		t.Fatalf("ListRooms(nil req) error = %v, want bad request", err)
	}
}

func TestRoomServiceListRoomsMergesRuntime(t *testing.T) {
	ctx := context.Background()
	d := newTestData(t)
	env := newTestRoomEnv(t, d)

	for _, room := range []*v1.Room{
		{RoomId: 1001, StreamerName: "live-room", RecordEnabled: true},
		{RoomId: 2002, StreamerName: "disabled-room", RecordEnabled: false},
		{RoomId: 3003, StreamerName: "recording-room", RecordEnabled: true},
		{RoomId: 4004, StreamerName: "stats-error-room", RecordEnabled: true},
		{RoomId: 5005, StreamerName: "remuxing-room", RecordEnabled: true},
	} {
		if _, err := env.svc.CreateRoom(ctx, &v1.CreateRoomRequest{Room: room}); err != nil {
			t.Fatalf("CreateRoom(%d) error = %v", room.GetRoomId(), err)
		}
	}

	// 重启：在同一数据库上新建 RoomRegistry，重新加载房间。
	env = newTestRoomEnv(t, d)
	env.stats.stats = map[int64]*biz.SessionStats{
		3003: {CurrentFile: "recordings/recording-room/part-0001.flv", BytesWritten: 123456789},
	}
	env.stats.errs = map[int64]error{
		4004: stderrors.New("storage unavailable"),
	}

	// 通过共享的 RoomRegistry 模拟守护进程的状态写入。
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
	if rooms[0].GetRoomId() != 1001 || rooms[0].GetStreamerName() != "live-room" || !rooms[0].GetRecordEnabled() {
		t.Fatalf("ListRooms() first room = %+v, want room 1001 in room_id order", rooms[0])
	}
	if rooms[1].GetRoomId() != 2002 || rooms[1].GetStreamerName() != "disabled-room" || rooms[1].GetRecordEnabled() {
		t.Fatalf("ListRooms() second room = %+v, want room 2002 with recording off", rooms[1])
	}
	// 新房间以 unknown/idle 起步，没有进行中的会话。
	if rooms[0].GetLiveStatus() != v1.LiveStatus_LIVE_STATUS_UNSPECIFIED {
		t.Fatalf("ListRooms() live_status = %v, want LIVE_STATUS_UNSPECIFIED", rooms[0].GetLiveStatus())
	}
	if rooms[0].GetRecordStatus() != v1.RecordStatus_RECORD_STATUS_IDLE {
		t.Fatalf("ListRooms() record_status = %v, want IDLE", rooms[0].GetRecordStatus())
	}
	if rooms[0].GetSessionStartedAt() != nil {
		t.Fatalf("ListRooms() session_started_at = %v, want nil for idle room", rooms[0].GetSessionStartedAt())
	}
	// 录制中的房间会映射直播状态并合并会话统计。
	if rooms[2].GetRoomId() != 3003 || rooms[2].GetStreamerName() != "recording-room" {
		t.Fatalf("ListRooms() third room = %+v, want recording room 3003", rooms[2])
	}
	if rooms[2].GetLiveStatus() != v1.LiveStatus_LIVE_STATUS_LIVE {
		t.Fatalf("ListRooms() recording room live_status = %v, want LIVE", rooms[2].GetLiveStatus())
	}
	if rooms[2].GetRecordStatus() != v1.RecordStatus_RECORD_STATUS_RECORDING {
		t.Fatalf("ListRooms() recording room record_status = %v, want RECORDING", rooms[2].GetRecordStatus())
	}
	if rooms[2].GetCurrentFile() != "recordings/recording-room/part-0001.flv" || rooms[2].GetBytesWritten() != 123456789 {
		t.Fatalf("ListRooms() recording room progress = %+v, want session stats merged", rooms[2])
	}
	if rooms[2].GetSessionStartedAt() == nil {
		t.Fatalf("ListRooms() recording room session_started_at = %v, want set", rooms[2].GetSessionStartedAt())
	}
	// 统计查询失败的录制中房间仍会列出，只是没有进度。
	if rooms[3].GetRoomId() != 4004 || rooms[3].GetRecordStatus() != v1.RecordStatus_RECORD_STATUS_RECORDING {
		t.Fatalf("ListRooms() fourth room = %+v, want recording room 4004", rooms[3])
	}
	if rooms[3].GetCurrentFile() != "" || rooms[3].GetBytesWritten() != 0 {
		t.Fatalf("ListRooms() stats-error room progress = %+v, want zero values on stats error", rooms[3])
	}
	// 转封装中的房间正常列出，但不查询统计。
	if rooms[4].GetRoomId() != 5005 || rooms[4].GetRecordStatus() != v1.RecordStatus_RECORD_STATUS_REMUXING {
		t.Fatalf("ListRooms() fifth room = %+v, want remuxing room 5005", rooms[4])
	}
	// 会话统计只查询录制中的房间。
	if len(env.stats.calls) != 2 || env.stats.calls[0] != 3003 || env.stats.calls[1] != 4004 {
		t.Fatalf("SessionStats() calls = %v, want exactly [3003 4004]", env.stats.calls)
	}

	// 启动后创建的房间直接从数据库返回，运行时字段取默认值：
	// CRUD 不会热加载 RoomRegistry。
	if _, err := env.svc.CreateRoom(ctx, &v1.CreateRoomRequest{
		Room: &v1.Room{RoomId: 6006, StreamerName: "late-room", RecordEnabled: true},
	}); err != nil {
		t.Fatalf("CreateRoom(late) error = %v", err)
	}
	late, err := env.svc.GetRoom(ctx, &v1.GetRoomRequest{RoomId: 6006})
	if err != nil {
		t.Fatalf("GetRoom(late) error = %v", err)
	}
	lateRoom := late.GetRoom()
	if lateRoom.GetLiveStatus() != v1.LiveStatus_LIVE_STATUS_UNSPECIFIED || lateRoom.GetRecordStatus() != v1.RecordStatus_RECORD_STATUS_IDLE {
		t.Fatalf("GetRoom(late) runtime = %v/%v, want default values", lateRoom.GetLiveStatus(), lateRoom.GetRecordStatus())
	}
}

func TestRoomServicePlatformRefreshOverridesStreamerName(t *testing.T) {
	ctx := context.Background()
	d := newTestData(t)
	seed := newTestRoomEnv(t, d)
	if _, err := seed.svc.CreateRoom(ctx, &v1.CreateRoomRequest{Room: &v1.Room{RoomId: 7007, RecordEnabled: true}}); err != nil {
		t.Fatalf("CreateRoom(seed) error = %v", err)
	}
	env := newTestRoomEnv(t, d)

	env.reg.ApplyRoomInfo(ctx, 7007, &biz.RoomInfo{RoomID: 7007, Live: true, StreamerName: "streamer-name"})

	got, err := env.svc.GetRoom(ctx, &v1.GetRoomRequest{RoomId: 7007})
	if err != nil {
		t.Fatalf("GetRoom() error = %v", err)
	}
	// 平台上报的非空身份直接回填房间信息。
	if got.GetRoom().GetStreamerName() != "streamer-name" {
		t.Fatalf("GetRoom() streamer_name = %q, want streamer-name", got.GetRoom().GetStreamerName())
	}
	if got.GetRoom().GetLiveStatus() != v1.LiveStatus_LIVE_STATUS_LIVE {
		t.Fatalf("GetRoom() live_status = %v, want LIVE", got.GetRoom().GetLiveStatus())
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
				Room: biz.Room{RoomID: 1, StreamerName: "room-one", RecordEnabled: true},
			},
			want: &v1.Room{
				RoomId:        1,
				StreamerName:  "room-one",
				RecordEnabled: true,
				LiveStatus:    v1.LiveStatus_LIVE_STATUS_UNSPECIFIED,
				RecordStatus:  v1.RecordStatus_RECORD_STATUS_IDLE,
			},
		},
		{
			name: "preparing",
			in: &biz.RoomRuntime{
				Room:       biz.Room{RoomID: 2, StreamerName: "room-two"},
				LiveStatus: biz.LiveStatusPreparing,
			},
			want: &v1.Room{
				RoomId:       2,
				StreamerName: "room-two",
				LiveStatus:   v1.LiveStatus_LIVE_STATUS_PREPARING,
				RecordStatus: v1.RecordStatus_RECORD_STATUS_IDLE,
			},
		},
		{
			name: "on air recording passes progress through",
			in: &biz.RoomRuntime{
				Room:             biz.Room{RoomID: 3, StreamerName: "room-three", RecordEnabled: true, CreateTime: createdAt, UpdateTime: updatedAt},
				LiveStatus:       biz.LiveStatusOnAir,
				RecordStatus:     biz.RecordStatusRecording,
				CurrentFile:      "recordings/room-three/part-0001.flv",
				BytesWritten:     123456789,
				SessionStartedAt: startedAt,
			},
			want: &v1.Room{
				RoomId:           3,
				StreamerName:     "room-three",
				RecordEnabled:    true,
				LiveStatus:       v1.LiveStatus_LIVE_STATUS_LIVE,
				RecordStatus:     v1.RecordStatus_RECORD_STATUS_RECORDING,
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
				Room:         biz.Room{RoomID: 4, StreamerName: "room-four"},
				RecordStatus: biz.RecordStatusRemuxing,
			},
			want: &v1.Room{
				RoomId:       4,
				StreamerName: "room-four",
				LiveStatus:   v1.LiveStatus_LIVE_STATUS_UNSPECIFIED,
				RecordStatus: v1.RecordStatus_RECORD_STATUS_REMUXING,
			},
		},
		{
			name: "error passes last_error through",
			in: &biz.RoomRuntime{
				Room:         biz.Room{RoomID: 5, StreamerName: "room-five", RecordEnabled: true},
				LiveStatus:   biz.LiveStatusPreparing,
				RecordStatus: biz.RecordStatusError,
				LastError:    "prepare session failed: disk full",
			},
			want: &v1.Room{
				RoomId:        5,
				StreamerName:  "room-five",
				RecordEnabled: true,
				LiveStatus:    v1.LiveStatus_LIVE_STATUS_PREPARING,
				RecordStatus:  v1.RecordStatus_RECORD_STATUS_ERROR,
				LastError:     "prepare session failed: disk full",
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := toRoomDTO(tt.in)
			if !proto.Equal(got, tt.want) {
				t.Fatalf("convertRoomReply() = %+v, want %+v", got, tt.want)
			}
		})
	}

	if got := toRoomDTO(nil); got != nil {
		t.Fatalf("convertRoomReply(nil) = %+v, want nil", got)
	}
}
