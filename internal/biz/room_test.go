package biz

import (
	"context"
	stderrors "errors"
	"testing"
	"time"

	"suika/internal/conf"
)

// fakeStatsRepo scripts SessionStatsRepo behavior for the room API tests.
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

func TestNewRoomRegistryNilConfig(t *testing.T) {
	reg := NewRoomRegistry(nil)
	if rooms := reg.Rooms(); len(rooms) != 0 {
		t.Fatalf("rooms = %d, want 0", len(rooms))
	}
	if snap := reg.Snapshot(); len(snap) != 0 {
		t.Fatalf("snapshot = %d, want 0", len(snap))
	}
}

func TestNewRoomRegistryParsesRooms(t *testing.T) {
	c := &conf.Recorder{Rooms: []*conf.Recorder_Room{
		{RoomId: 1, Name: "a", Enabled: true},
		{RoomId: 2, Name: "b", Enabled: false},
	}}
	reg := NewRoomRegistry(c)
	rooms := reg.Rooms()
	if len(rooms) != 2 {
		t.Fatalf("rooms = %d, want 2", len(rooms))
	}
	if rooms[0].RoomID != 1 || rooms[0].Name != "a" || !rooms[0].Enabled {
		t.Fatalf("room[0] = %+v", rooms[0])
	}
	if rooms[1].RoomID != 2 || rooms[1].Name != "b" || rooms[1].Enabled {
		t.Fatalf("room[1] = %+v", rooms[1])
	}
}

func TestListRoomsMergesStateAndStats(t *testing.T) {
	stats := &fakeStatsRepo{
		stats: map[int64]*SessionStats{
			1: {CurrentFile: "/rec/1_part2.flv", BytesWritten: 4096},
			// room 2 has stats available but is not recording: they must
			// not be merged.
			2: {CurrentFile: "/rec/2_part1.flv", BytesWritten: 1},
		},
		failures: map[int64]error{3: stderrors.New("stats unavailable")},
	}
	c := &conf.Recorder{Rooms: []*conf.Recorder_Room{
		{RoomId: 1, Name: "a", Enabled: true},
		{RoomId: 2, Name: "b", Enabled: false},
		{RoomId: 3, Name: "c", Enabled: true},
	}}
	reg := NewRoomRegistry(c)
	reg.ApplyRoomInfo(1, liveInfo(1, true))
	reg.setState(1, func(st *roomState) {
		st.record = RecordRecording
		st.sessionStartedAt = time.Unix(100, 0)
	})
	reg.NoteError(2, stderrors.New("boom"))
	reg.StartRecording(3)

	uc := NewRoomUsecase(reg, stats)
	out, err := uc.ListRooms(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(out) != 3 {
		t.Fatalf("len(out) = %d, want 3", len(out))
	}
	if out[0].Room.RoomID != 1 || out[1].Room.RoomID != 2 || out[2].Room.RoomID != 3 {
		t.Fatalf("configuration order not preserved: %+v", out)
	}
	if out[0].Live != LiveOnAir || out[0].Record != RecordRecording {
		t.Fatalf("room 1 state = %v/%v", out[0].Live, out[0].Record)
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
	// recording room whose stats call fails: skipped without an error.
	if out[2].Record != RecordRecording {
		t.Fatalf("room 3 state = %v", out[2].Record)
	}
	if out[2].CurrentFile != "" || out[2].BytesWritten != 0 {
		t.Fatalf("room 3 stats = %q/%d, want zero values after stats error", out[2].CurrentFile, out[2].BytesWritten)
	}
	// stats are only requested for RecordRecording rooms.
	if len(stats.calls) != 2 || stats.calls[0] != 1 || stats.calls[1] != 3 {
		t.Fatalf("stats calls = %v, want [1 3]", stats.calls)
	}
}
