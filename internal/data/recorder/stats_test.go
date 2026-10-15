package recorder

import (
	"bytes"
	"context"
	"io"
	"sync"
	"testing"
	"time"

	"suika/internal/biz"
	"suika/internal/data/flv"
)

func TestStatsUnknownRoom(t *testing.T) {
	store := newStatsStore()
	got, err := store.Stats(context.Background(), 12345)
	if err != nil || got != nil {
		t.Fatalf("Stats for a room without a session = %+v, %v; want nil, nil", got, err)
	}
}

func TestStatsSnapshotAndReset(t *testing.T) {
	ctx := context.Background()
	store := newStatsStore()

	s := store.getOrCreateStats(1)
	if again := store.getOrCreateStats(1); again != s {
		t.Fatal("getOrCreateStats must return the same instance for a room")
	}
	if other := store.getOrCreateStats(2); other == s {
		t.Fatal("rooms must not share stats")
	}

	// 创建后尚无写入：文件名为空串而非 nil 转换出的异常值。
	got, err := store.Stats(ctx, 1)
	if err != nil || got == nil || *got != (biz.SessionStats{}) {
		t.Fatalf("fresh stats = %+v, %v; want zero value", got, err)
	}

	s.setCurrentFile("/rec/a.flv")
	s.setBytesWritten(4096)
	s.setDownloadSpeed(512)
	got, _ = store.Stats(ctx, 1)
	want := biz.SessionStats{CurrentFile: "/rec/a.flv", BytesWritten: 4096, DownloadSpeed: 512}
	if got == nil || *got != want {
		t.Fatalf("stats = %+v, want %+v", got, want)
	}
	if other, _ := store.Stats(ctx, 2); other == nil || *other != (biz.SessionStats{}) {
		t.Fatalf("room 2 stats = %+v, want untouched zero value", other)
	}

	s.reset()
	got, _ = store.Stats(ctx, 1)
	if got == nil || *got != (biz.SessionStats{}) {
		t.Fatalf("stats after reset = %+v, want zero value", got)
	}
}

// Stats 在 HTTP/gRPC 处理协程中读取，泵送协程同时写入：用 -race 运行
// 以验证字段访问是并发安全的。
func TestStatsConcurrentAccess(t *testing.T) {
	store := newStatsStore()
	ctx := context.Background()
	var wg sync.WaitGroup

	wg.Add(1)
	go func() {
		defer wg.Done()
		s := store.getOrCreateStats(1)
		for i := range 500 {
			s.setBytesWritten(int64(i))
			s.setDownloadSpeed(int64(i))
			s.setCurrentFile("f")
			if i%100 == 0 {
				s.reset()
			}
		}
	}()
	for range 3 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range 500 {
				_, _ = store.Stats(ctx, 1)
				store.getOrCreateStats(1)
			}
		}()
	}
	wg.Wait()
}

func TestPrepareSessionResetsStatsBetweenSessions(t *testing.T) {
	repo := newTestRepo(t, nil)
	ctx := context.Background()

	metaTag, videoSeq, audioSeq := testInitTags()
	key0 := &flv.Tag{Type: flv.TagVideo, Timestamp: 0, Data: []byte{0x17, 0x01, 0, 0, 0, 0xAA}}
	inter40 := &flv.Tag{Type: flv.TagVideo, Timestamp: 40, Data: []byte{0x27, 0x01, 0, 0, 0, 0xBB}}
	tags := []*flv.Tag{metaTag, videoSeq, audioSeq, key0, inter40}
	var wantBytes int64
	for _, tag := range []*flv.Tag{key0, inter40} { // 初始化标签不计入
		wantBytes += int64(len(tag.AppendTo(nil)))
	}
	pump := func(session *biz.RecordingSession) {
		t.Helper()
		stream := &biz.LiveStream{
			Quality: biz.StreamQuality{Qn: 10000, Desc: "source"},
			Body:    io.NopCloser(bytes.NewReader(buildFLVStream(t, tags...))),
		}
		if _, err := repo.PumpSession(ctx, session, stream, nil); err != nil {
			t.Fatalf("PumpSession: %v", err)
		}
	}

	first := testSession()
	if err := repo.PrepareSession(ctx, first); err != nil {
		t.Fatal(err)
	}
	pump(first)
	stats, err := repo.Stats(ctx, first.RoomID)
	if err != nil || stats == nil || stats.BytesWritten != wantBytes {
		t.Fatalf("stats after first session = %+v, %v; want %d bytes", stats, err, wantBytes)
	}

	// 同一房间的下一场开播：PrepareSession 必须清零写入进度，
	// bytes_written 不得跨会话累加。
	second := testSession()
	second.LiveStartTime = first.LiveStartTime.Add(2 * time.Hour)
	if err := repo.PrepareSession(ctx, second); err != nil {
		t.Fatal(err)
	}
	stats, err = repo.Stats(ctx, second.RoomID)
	if err != nil {
		t.Fatal(err)
	}
	if stats == nil || stats.BytesWritten != 0 || stats.CurrentFile != "" {
		t.Fatalf("stats at second session start = %+v, want zeroed", stats)
	}
	pump(second)
	stats, err = repo.Stats(ctx, second.RoomID)
	if err != nil || stats == nil || stats.BytesWritten != wantBytes {
		t.Fatalf("stats after second session = %+v, %v; want %d bytes (no cross-session accumulation)", stats, err, wantBytes)
	}
}
