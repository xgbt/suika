package biz

import (
	"context"
	stderrors "errors"
	"maps"
	"slices"
	"testing"
	"time"
)

// fakeStatsRepo 为房间 API 测试模拟 SessionStatsRepo 行为。
type fakeStatsRepo struct {
	stats    map[int64]*SessionStats
	failures map[int64]error
	calls    []int64
}

func (r *fakeStatsRepo) SessionStats(_ context.Context, roomID int64) (*SessionStats, error) {
	r.calls = append(r.calls, roomID)
	if err, ok := r.failures[roomID]; ok {
		return nil, err
	}
	if s, ok := r.stats[roomID]; ok {
		return s, nil
	}
	return nil, nil
}

// fakeRoomRepo 为 RoomRegistry 和 usecase 测试模拟 RoomRepo 行为。
type fakeRoomRepo struct {
	rooms     map[int64]*Room
	listErr   error
	updateErr error
	updates   []*Room
}

func (r *fakeRoomRepo) GetByRoomID(_ context.Context, roomID int64) (*Room, error) {
	if room, ok := r.rooms[roomID]; ok {
		return room, nil
	}
	return nil, ErrRoomNotFound
}

func (r *fakeRoomRepo) ListRooms(_ context.Context, _ ListQuery) ([]*Room, error) {
	if r.listErr != nil {
		return nil, r.listErr
	}
	out := make([]*Room, 0, len(r.rooms))
	for _, id := range slices.Sorted(maps.Keys(r.rooms)) {
		out = append(out, r.rooms[id])
	}
	return out, nil
}

func (r *fakeRoomRepo) CreateRoom(_ context.Context, room *Room) (*Room, error) {
	if r.rooms == nil {
		r.rooms = make(map[int64]*Room)
	}
	if _, ok := r.rooms[room.RoomID]; ok {
		return nil, ErrRoomAlreadyExists
	}
	r.rooms[room.RoomID] = room
	return room, nil
}

func (r *fakeRoomRepo) UpdateRoom(_ context.Context, room *Room) (*Room, error) {
	r.updates = append(r.updates, room)
	if r.updateErr != nil {
		return nil, r.updateErr
	}
	if _, ok := r.rooms[room.RoomID]; !ok {
		return nil, ErrRoomNotFound
	}
	r.rooms[room.RoomID] = room
	return room, nil
}

func (r *fakeRoomRepo) DeleteRoom(_ context.Context, roomID int64) error {
	if _, ok := r.rooms[roomID]; !ok {
		return ErrRoomNotFound
	}
	delete(r.rooms, roomID)
	return nil
}

func TestNewRoomRegistryLoadsRooms(t *testing.T) {
	repo := &fakeRoomRepo{rooms: map[int64]*Room{
		2: {RoomID: 2, StreamerName: "b"},
		1: {RoomID: 1, StreamerName: "a", Enabled: true},
	}}
	reg, err := NewRoomRegistry(repo)
	if err != nil {
		t.Fatalf("NewRoomRegistry() error = %v", err)
	}
	rooms := reg.Rooms()
	if len(rooms) != 2 {
		t.Fatalf("rooms = %d, want 2", len(rooms))
	}
	// Rooms() 由 map 迭代产生，顺序不作保证：按 RoomID 索引后断言。
	byID := make(map[int64]Room, len(rooms))
	for _, room := range rooms {
		byID[room.RoomID] = room
	}
	if got, ok := byID[1]; !ok || got.StreamerName != "a" || !got.Enabled {
		t.Fatalf("room 1 = %+v, present = %v", got, ok)
	}
	if got, ok := byID[2]; !ok || got.StreamerName != "b" || got.Enabled {
		t.Fatalf("room 2 = %+v, present = %v", got, ok)
	}
}

func TestNewRoomRegistryNilRepo(t *testing.T) {
	reg, err := NewRoomRegistry(nil)
	if err != nil {
		t.Fatalf("NewRoomRegistry(nil) error = %v", err)
	}
	if rooms := reg.Rooms(); len(rooms) != 0 {
		t.Fatalf("rooms = %d, want 0", len(rooms))
	}
}

func TestNewRoomRegistryLoadError(t *testing.T) {
	repo := &fakeRoomRepo{listErr: stderrors.New("db down")}
	if _, err := NewRoomRegistry(repo); err == nil {
		t.Fatal("NewRoomRegistry() error = nil, want load error")
	}
}

func TestRoomRegistryAddUpdateRemove(t *testing.T) {
	reg, err := NewRoomRegistry(nil)
	if err != nil {
		t.Fatalf("NewRoomRegistry() error = %v", err)
	}
	changes, unsubscribe := reg.Subscribe()
	defer unsubscribe()

	// Add：立即可见。
	reg.Add(Room{RoomID: 1, StreamerName: "a", Enabled: true})
	if len(reg.Rooms()) != 1 || !reg.Room(1).Enabled {
		t.Fatalf("registry after Add = %+v", reg.Rooms())
	}

	// Update：刷新房间字段，保留运行时状态。
	reg.StartRecording(1)
	reg.Update(Room{RoomID: 1, StreamerName: "b", Enabled: false})
	rt := reg.runtime(1)
	if rt.Room.StreamerName != "b" || rt.Room.Enabled {
		t.Fatalf("room after Update = %+v", rt.Room)
	}
	if rt.RecordStatus != RecordStatusRecording {
		t.Fatalf("record status after Update = %v, want recording preserved", rt.RecordStatus)
	}

	// Remove：立即不可见；迟到的状态写入静默忽略。
	reg.Remove(1)
	if len(reg.Rooms()) != 0 {
		t.Fatalf("registry after Remove = %+v", reg.Rooms())
	}
	reg.NoteError(1, stderrors.New("late write"))
	if got := reg.runtime(1).LastError; got != "" {
		t.Fatalf("last error after Remove = %q, want empty", got)
	}

	// 通知合并：三次变更只积压一个唤醒信号。
	select {
	case <-changes:
	case <-time.After(time.Second):
		t.Fatal("no change signal after mutations")
	}
	select {
	case <-changes:
		t.Fatal("unexpected extra change signal")
	case <-time.After(50 * time.Millisecond):
	}

	// 退订后不再收到信号。
	unsubscribe()
	reg.Add(Room{RoomID: 2})
	select {
	case <-changes:
		t.Fatal("change signal delivered after unsubscribe")
	case <-time.After(50 * time.Millisecond):
	}
}

func TestRoomUsecaseCRUDSyncsRegistry(t *testing.T) {
	repo := &fakeRoomRepo{rooms: map[int64]*Room{1: {RoomID: 1, StreamerName: "a"}}}
	reg, err := NewRoomRegistry(repo)
	if err != nil {
		t.Fatalf("NewRoomRegistry() error = %v", err)
	}
	uc := NewRoomUsecase(repo, reg, &fakeStatsRepo{})
	ctx := context.Background()

	if _, err := uc.CreateRoom(ctx, &Room{RoomID: 2, StreamerName: "b", Enabled: true}); err != nil {
		t.Fatalf("CreateRoom: %v", err)
	}
	if got := reg.Room(2); got.StreamerName != "b" || !got.Enabled {
		t.Fatalf("registry after create = %+v", got)
	}

	if _, err := uc.UpdateRoom(ctx, &Room{RoomID: 2, StreamerName: "b2", Enabled: false}); err != nil {
		t.Fatalf("UpdateRoom: %v", err)
	}
	if got := reg.Room(2); got.StreamerName != "b2" || got.Enabled {
		t.Fatalf("registry after update = %+v", got)
	}

	if err := uc.DeleteRoom(ctx, 2); err != nil {
		t.Fatalf("DeleteRoom: %v", err)
	}
	if len(reg.Rooms()) != 1 {
		t.Fatalf("registry after delete has %d rooms, want 1", len(reg.Rooms()))
	}

	// 持久化失败时注册表保持不变。
	repo.updateErr = stderrors.New("db locked")
	if _, err := uc.UpdateRoom(ctx, &Room{RoomID: 1, StreamerName: "x"}); err == nil {
		t.Fatal("UpdateRoom error = nil, want persist error")
	}
	if got := reg.Room(1); got.StreamerName != "a" {
		t.Fatalf("registry changed despite persist failure: %+v", got)
	}
}

func TestApplyRoomInfoUpdatesIdentityThroughRepo(t *testing.T) {
	repo := &fakeRoomRepo{rooms: map[int64]*Room{1: {RoomID: 1, Enabled: true}}}
	reg, err := NewRoomRegistry(repo)
	if err != nil {
		t.Fatalf("NewRoomRegistry() error = %v", err)
	}

	reg.ApplyRoomInfo(context.Background(), 1, &RoomInfo{RoomID: 1, Live: true, StreamerName: "streamer", Title: "title-a"})

	if got := reg.Room(1).StreamerName; got != "streamer" {
		t.Fatalf("streamer_name = %q, want streamer", got)
	}
	if got := reg.Room(1).RoomTitle; got != "title-a" {
		t.Fatalf("room_title = %q, want title-a", got)
	}
	if len(repo.updates) != 1 || repo.updates[0].RoomID != 1 || repo.updates[0].StreamerName != "streamer" || repo.updates[0].RoomTitle != "title-a" {
		t.Fatalf("repo.updates = %+v, want one update", repo.updates)
	}

	// 再次上报新值，内存和持久化都应更新。
	reg.ApplyRoomInfo(context.Background(), 1, &RoomInfo{RoomID: 1, Live: false, StreamerName: "other", Title: "title-b"})
	if got := reg.Room(1).StreamerName; got != "other" {
		t.Fatalf("streamer_name after update = %q, want other", got)
	}
	if len(repo.updates) != 2 {
		t.Fatalf("repo.updates = %d, want 2", len(repo.updates))
	}
}

func TestApplyRoomInfoSurvivesRepoFailure(t *testing.T) {
	repo := &fakeRoomRepo{rooms: map[int64]*Room{1: {RoomID: 1}}, updateErr: stderrors.New("db locked")}
	reg, err := NewRoomRegistry(repo)
	if err != nil {
		t.Fatalf("NewRoomRegistry() error = %v", err)
	}

	reg.ApplyRoomInfo(context.Background(), 1, &RoomInfo{RoomID: 1, Live: true, StreamerName: "streamer", Title: "title-a"})

	// 持久化失败时，内存中的回填仍要生效。
	if got := reg.Room(1).StreamerName; got != "streamer" {
		t.Fatalf("backfilled streamer_name = %q, want streamer", got)
	}
	if got := reg.Room(1).RoomTitle; got != "title-a" {
		t.Fatalf("backfilled room_title = %q, want title-a", got)
	}
}

func TestListRoomsMergesStateAndStats(t *testing.T) {
	stats := &fakeStatsRepo{
		stats: map[int64]*SessionStats{
			1: {CurrentFile: "/rec/1_part2.flv", BytesWritten: 4096},
			// 房间 2 有统计数据但未在录制：不得合并进度。
			2: {CurrentFile: "/rec/2_part1.flv", BytesWritten: 1},
		},
		failures: map[int64]error{3: stderrors.New("stats unavailable")},
	}
	repo := &fakeRoomRepo{rooms: map[int64]*Room{
		1: {RoomID: 1, StreamerName: "a", Enabled: true},
		2: {RoomID: 2, StreamerName: "b"},
		3: {RoomID: 3, StreamerName: "c", Enabled: true},
	}}
	reg, err := NewRoomRegistry(repo)
	if err != nil {
		t.Fatalf("NewRoomRegistry() error = %v", err)
	}
	reg.ApplyRoomInfo(context.Background(), 1, liveInfo(1, true))
	reg.setState(1, func(st *roomState) {
		st.recordStatus = RecordStatusRecording
		st.sessionStartedAt = time.Unix(100, 0)
	})
	reg.NoteError(2, stderrors.New("boom"))
	reg.StartRecording(3)

	uc := NewRoomUsecase(repo, reg, stats)
	out, err := uc.ListRoomRuntimes(context.Background(), ListQuery{Offset: 0, Limit: 20})
	if err != nil {
		t.Fatal(err)
	}
	if len(out) != 3 {
		t.Fatalf("len(out) = %d, want 3", len(out))
	}
	if out[0].Room.RoomID != 1 || out[1].Room.RoomID != 2 || out[2].Room.RoomID != 3 {
		t.Fatalf("room order not preserved: %+v", out)
	}
	if out[0].LiveStatus != LiveStatusOnAir || out[0].RecordStatus != RecordStatusRecording {
		t.Fatalf("room 1 state = %v/%v", out[0].LiveStatus, out[0].RecordStatus)
	}
	if out[0].CurrentFile != "/rec/1_part2.flv" || out[0].BytesWritten != 4096 {
		t.Fatalf("room 1 stats = %q/%d", out[0].CurrentFile, out[0].BytesWritten)
	}
	if !out[0].SessionStartedAt.Equal(time.Unix(100, 0)) {
		t.Fatalf("session start = %v", out[0].SessionStartedAt)
	}
	if out[1].LastError != "boom" || out[1].Room.Enabled {
		t.Fatalf("room 2 = %+v", out[1])
	}
	if out[1].CurrentFile != "" || out[1].BytesWritten != 0 {
		t.Fatalf("room 2 unexpectedly got stats: %q/%d", out[1].CurrentFile, out[1].BytesWritten)
	}
	// 录制中但统计查询失败的房间：静默跳过，不报错。
	if out[2].RecordStatus != RecordStatusRecording {
		t.Fatalf("room 3 state = %v", out[2].RecordStatus)
	}
	if out[2].CurrentFile != "" || out[2].BytesWritten != 0 {
		t.Fatalf("room 3 stats = %q/%d, want zero values after stats error", out[2].CurrentFile, out[2].BytesWritten)
	}
	// 只有录制中的房间才会查询统计。
	if len(stats.calls) != 2 || stats.calls[0] != 1 || stats.calls[1] != 3 {
		t.Fatalf("stats calls = %v, want [1 3]", stats.calls)
	}
}

func TestRoomUsecaseValidation(t *testing.T) {
	reg, err := NewRoomRegistry(nil)
	if err != nil {
		t.Fatalf("NewRoomRegistry() error = %v", err)
	}
	repo := &fakeRoomRepo{}
	uc := NewRoomUsecase(repo, reg, &fakeStatsRepo{})
	ctx := context.Background()

	if _, err := uc.CreateRoom(ctx, nil); !stderrors.Is(err, ErrRoomInvalidArgument) {
		t.Fatalf("CreateRoom(nil) error = %v, want invalid argument", err)
	}
	if _, err := uc.CreateRoom(ctx, &Room{RoomID: 0}); !stderrors.Is(err, ErrRoomInvalidArgument) {
		t.Fatalf("CreateRoom(zero id) error = %v, want invalid argument", err)
	}
	if _, err := uc.GetRoom(ctx, 0); !stderrors.Is(err, ErrRoomInvalidArgument) {
		t.Fatalf("GetRoom(zero id) error = %v, want invalid argument", err)
	}
	if _, err := uc.UpdateRoom(ctx, nil); !stderrors.Is(err, ErrRoomInvalidArgument) {
		t.Fatalf("UpdateRoom(nil) error = %v, want invalid argument", err)
	}
	if err := uc.DeleteRoom(ctx, -1); !stderrors.Is(err, ErrRoomInvalidArgument) {
		t.Fatalf("DeleteRoom(negative id) error = %v, want invalid argument", err)
	}
	// 允许空的主播元数据：之后由平台 API 回填。
	if _, err := uc.CreateRoom(ctx, &Room{RoomID: 7}); err != nil {
		t.Fatalf("CreateRoom(empty metadata) error = %v, want success", err)
	}
}

func TestRoomUsecaseRepoErrors(t *testing.T) {
	reg, err := NewRoomRegistry(nil)
	if err != nil {
		t.Fatalf("NewRoomRegistry() error = %v", err)
	}
	repo := &fakeRoomRepo{rooms: map[int64]*Room{1: {RoomID: 1, StreamerName: "a"}}}
	uc := NewRoomUsecase(repo, reg, &fakeStatsRepo{})
	ctx := context.Background()

	if _, err := uc.GetRoom(ctx, 2); !stderrors.Is(err, ErrRoomNotFound) {
		t.Fatalf("GetRoom(missing) error = %v, want not found", err)
	}
	if _, err := uc.CreateRoom(ctx, &Room{RoomID: 1}); !stderrors.Is(err, ErrRoomAlreadyExists) {
		t.Fatalf("CreateRoom(duplicate) error = %v, want already exists", err)
	}
	if _, err := uc.UpdateRoom(ctx, &Room{RoomID: 2, StreamerName: "x"}); !stderrors.Is(err, ErrRoomNotFound) {
		t.Fatalf("UpdateRoom(missing) error = %v, want not found", err)
	}
	if err := uc.DeleteRoom(ctx, 2); !stderrors.Is(err, ErrRoomNotFound) {
		t.Fatalf("DeleteRoom(missing) error = %v, want not found", err)
	}
}
