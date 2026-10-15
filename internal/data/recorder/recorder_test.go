package recorder

import (
	"bytes"
	"context"
	"errors"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"suika/internal/biz"
	"suika/internal/conf"
	"suika/internal/data/flv"
)

// 本文件覆盖 recorder.go：会话生命周期（Prepare / Finish / Recover）。
// 其余按源文件拆分：meta / paths / segment / recorder_pump / merge /
// stats / pump_monitor / dedup，共用辅助见 helpers_test.go。

func TestNewRecorderRepoConfigMapping(t *testing.T) {
	r := NewRecorderRepo(nil).(*recorderRepo)
	if r.recordRoot != defaultRecordRoot {
		t.Fatalf("defaults not applied: %+v", r)
	}

	// 空配置与空 record_root 同样回落到默认值。
	r = NewRecorderRepo(&conf.Recorder{}).(*recorderRepo)
	if r.recordRoot != defaultRecordRoot {
		t.Fatalf("empty record_root = %q, want default %q", r.recordRoot, defaultRecordRoot)
	}

	r = NewRecorderRepo(&conf.Recorder{RecordRoot: "/srv/recordings"}).(*recorderRepo)
	if r.recordRoot != "/srv/recordings" {
		t.Fatalf("recordRoot = %q, want %q", r.recordRoot, "/srv/recordings")
	}
}

func TestPrepareSessionCreatesMeta(t *testing.T) {
	repo := newTestRepo(t, nil)
	session := testSession()
	if err := repo.PrepareSession(context.Background(), session); err != nil {
		t.Fatalf("PrepareSession: %v", err)
	}

	lay, err := sessionPaths(repo.recordRoot, session)
	if err != nil {
		t.Fatal(err)
	}
	meta, err := loadMeta(lay.metaPath())
	if err != nil {
		t.Fatal(err)
	}
	if meta.RoomID != session.RoomID || meta.RoomName != session.StreamerName || meta.Title != session.Title {
		t.Fatalf("identity fields = %+v", meta)
	}
	if meta.LiveStartTime != session.LiveStartTime.Unix() {
		t.Fatalf("LiveStartTime = %d, want %d", meta.LiveStartTime, session.LiveStartTime.Unix())
	}
	if meta.Status != metaStatusRecording || meta.EndTime != 0 || len(meta.Segments) != 0 {
		t.Fatalf("fresh meta = %+v, want recording with no segments", meta)
	}
}

func TestPrepareSessionRejectsInvalidSession(t *testing.T) {
	repo := newTestRepo(t, nil)
	if err := repo.PrepareSession(context.Background(), nil); !errors.Is(err, biz.ErrRoomInternal) {
		t.Fatalf("nil session err = %v, want ErrRoomInternal", err)
	}
	if err := repo.PrepareSession(context.Background(), &biz.RecordingSession{}); !errors.Is(err, biz.ErrRoomInternal) {
		t.Fatalf("zero room err = %v, want ErrRoomInternal", err)
	}
}

func TestPrepareSessionResumeKeepsSegments(t *testing.T) {
	repo := newTestRepo(t, nil)
	ctx := context.Background()
	session := testSession()

	if err := repo.PrepareSession(ctx, session); err != nil {
		t.Fatalf("PrepareSession: %v", err)
	}
	lay, err := sessionPaths(repo.recordRoot, session)
	if err != nil {
		t.Fatal(err)
	}
	metaPath := lay.metaPath()

	// 模拟崩溃/重启前已录好的一个分段
	appendSegmentMeta(metaPath, &segmentWriter{
		part:        1,
		videoPath:   lay.segmentVideoPath(1),
		danmakuPath: lay.segmentDanmakuPath(1),
		wallStart:   session.LiveStartTime,
	})

	restart := *session
	if err := repo.PrepareSession(ctx, &restart); err != nil {
		t.Fatalf("PrepareSession resume: %v", err)
	}
	meta, err := loadMeta(metaPath)
	if err != nil {
		t.Fatal(err)
	}
	if meta.Status != metaStatusRecording {
		t.Fatalf("resume status = %q, want %q", meta.Status, metaStatusRecording)
	}
	if meta.LiveStartTime != session.LiveStartTime.Unix() {
		t.Fatalf("LiveStartTime = %d, want original %d", meta.LiveStartTime, session.LiveStartTime.Unix())
	}
	if len(meta.Segments) != 1 || meta.Segments[0].Part != 1 {
		t.Fatalf("segments lost on resume: %+v", meta.Segments)
	}
}

func TestPrepareSessionResumeUpdatesTitleVariants(t *testing.T) {
	// meta 路径内嵌的是净化后的标题，因此只有净化后基座相同的标题才会
	// 走到续录分支；此时落盘标题会刷新为重启会话带来的标题。
	repo := newTestRepo(t, nil)
	ctx := context.Background()
	session := testSession()
	session.Title = "a/b"
	if err := repo.PrepareSession(ctx, session); err != nil {
		t.Fatal(err)
	}
	lay, err := sessionPaths(repo.recordRoot, session)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.HasSuffix(lay.base, "_a_b") {
		t.Fatalf("base = %q, want suffix _a_b", lay.base)
	}
	metaPath := lay.metaPath()

	restart := *session
	restart.Title = "a b" // 净化后基座相同
	restart.StreamerName = session.StreamerName
	if err := repo.PrepareSession(ctx, &restart); err != nil {
		t.Fatal(err)
	}
	meta, err := loadMeta(metaPath)
	if err != nil {
		t.Fatal(err)
	}
	if meta.Title != "a b" {
		t.Fatalf("title = %q, want refreshed %q", meta.Title, "a b")
	}
}

func TestFinishSessionWithoutMetaIsNoop(t *testing.T) {
	repo := newTestRepo(t, nil)
	if err := repo.FinishSession(context.Background(), testSession()); err != nil {
		t.Fatalf("want nil for missing meta.json, got %v", err)
	}
}

func TestFinishSessionStampsEndTime(t *testing.T) {
	metaTag, videoSeq, audioSeq := testInitTags()
	repo := newTestRepo(t, nil)
	_, metaPath := seedMergeSession(t, repo, buildFLVStream(t, metaTag, videoSeq, audioSeq))

	before := time.Now().Unix()
	if err := repo.FinishSession(context.Background(), testSession()); err != nil {
		t.Fatalf("FinishSession: %v", err)
	}
	meta, err := loadMeta(metaPath)
	if err != nil {
		t.Fatal(err)
	}
	if meta.EndTime < before {
		t.Fatalf("EndTime = %d, want >= %d", meta.EndTime, before)
	}
}

func TestFinishedSessionCanAppendAfterRecordingIsReenabled(t *testing.T) {
	metaTag, videoSeq, audioSeq := testInitTags()
	firstKey := &flv.Tag{Type: flv.TagVideo, Timestamp: 0, Data: []byte{0x17, 0x01, 0, 0, 0, 0xAA}}
	firstEnd := &flv.Tag{Type: flv.TagVideo, Timestamp: 10_000, Data: []byte{0x27, 0x01, 0, 0, 0, 0xBB}}
	secondKey := &flv.Tag{Type: flv.TagVideo, Timestamp: 0, Data: []byte{0x17, 0x01, 0, 0, 0, 0xCC}}
	secondEnd := &flv.Tag{Type: flv.TagVideo, Timestamp: 5_000, Data: []byte{0x27, 0x01, 0, 0, 0, 0xDD}}

	repo := newTestRepo(t, nil)
	session := testSession()
	record := func(tags ...*flv.Tag) {
		t.Helper()
		if err := repo.PrepareSession(context.Background(), session); err != nil {
			t.Fatal(err)
		}
		stream := &biz.LiveStream{
			Quality: biz.StreamQuality{Qn: 10000, Desc: "source"},
			Body:    io.NopCloser(bytes.NewReader(buildFLVStream(t, tags...))),
		}
		if _, err := repo.PumpSession(context.Background(), session, stream, nil); err != nil {
			t.Fatal(err)
		}
		if err := repo.FinishSession(context.Background(), session); err != nil {
			t.Fatal(err)
		}
	}

	record(metaTag, videoSeq, audioSeq, firstKey, firstEnd)
	record(metaTag, videoSeq, audioSeq, secondKey, secondEnd)

	lay, err := sessionPaths(repo.recordRoot, session)
	if err != nil {
		t.Fatal(err)
	}
	_, tags := readSegmentTags(t, lay.mergedVideoPath())
	wantVideoSeq2 := &flv.Tag{Type: flv.TagVideo, Timestamp: 10_000, Data: videoSeq.Data}
	wantAudioSeq2 := &flv.Tag{Type: flv.TagAudio, Timestamp: 10_000, Data: audioSeq.Data}
	wantSecondKey := &flv.Tag{Type: flv.TagVideo, Timestamp: 10_000, Data: secondKey.Data}
	wantSecondEnd := &flv.Tag{Type: flv.TagVideo, Timestamp: 15_000, Data: secondEnd.Data}
	assertTagsEqual(t, tags, []*flv.Tag{
		videoSeq, audioSeq, firstKey, firstEnd,
		wantVideoSeq2, wantAudioSeq2, wantSecondKey, wantSecondEnd,
	})

	meta, err := loadMeta(lay.metaPath())
	if err != nil {
		t.Fatal(err)
	}
	if meta.Status != metaStatusDone || meta.MergedVideo != lay.mergedVideoName() {
		t.Fatalf("meta = %+v, want completed appended recording", meta)
	}
}

func TestArchiveMergedSessionRollsBackVideoWhenDanmakuIsMissing(t *testing.T) {
	lay := sessionLayout{dir: t.TempDir(), base: "session"}
	videoName := lay.mergedVideoName()
	videoPath := lay.mergedVideoPath()
	content := []byte("merged video")
	if err := os.WriteFile(videoPath, content, 0o644); err != nil {
		t.Fatal(err)
	}
	meta := &sessionMeta{
		MergedVideo:   videoName,
		MergedDanmaku: lay.mergedDanmakuName(),
	}

	if err := archiveMergedSession(lay, meta); err == nil {
		t.Fatal("archiveMergedSession succeeded with missing danmaku")
	}
	if got, err := os.ReadFile(videoPath); err != nil || !bytes.Equal(got, content) {
		t.Fatalf("merged video was not rolled back: %q, %v", got, err)
	}
	if len(meta.Segments) != 0 || meta.MergedVideo != videoName {
		t.Fatalf("meta changed after failed archive: %+v", meta)
	}
}

// 归档把合并产物还原成一个历史分段：编号接续、范围取原分段的并集、
// 合并字段与结束时间清零，等待续录后重新合并。
func TestArchiveMergedSessionRestoresSingleSegment(t *testing.T) {
	lay := sessionLayout{dir: t.TempDir(), base: "session"}
	if err := os.WriteFile(lay.mergedVideoPath(), []byte("merged video"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(lay.mergedDanmakuPath(), []byte("danmaku"), 0o644); err != nil {
		t.Fatal(err)
	}
	meta := &sessionMeta{
		Status:        metaStatusDone,
		EndTime:       999,
		MergedVideo:   lay.mergedVideoName(),
		MergedDanmaku: lay.mergedDanmakuName(),
		Segments: []segmentMeta{
			{Part: 1, WallStart: 1000, WallEnd: 2000, TsStart: 0, TsEnd: 500},
			{Part: 2, WallStart: 2000, WallEnd: 3000, TsStart: 500, TsEnd: 900},
		},
	}

	if err := archiveMergedSession(lay, meta); err != nil {
		t.Fatalf("archiveMergedSession: %v", err)
	}

	if len(meta.Segments) != 1 {
		t.Fatalf("segments = %+v, want the merged output as one segment", meta.Segments)
	}
	seg := meta.Segments[0]
	want := segmentMeta{
		Part: 1, Video: lay.segmentVideoName(1), Danmaku: lay.segmentDanmakuName(1),
		WallStart: 1000, WallEnd: 3000, TsStart: 0, TsEnd: 900, Bytes: int64(len("merged video")),
	}
	if seg != want {
		t.Fatalf("segment = %+v, want %+v", seg, want)
	}
	if meta.MergedVideo != "" || meta.MergedDanmaku != "" || meta.EndTime != 0 {
		t.Fatalf("merge fields not cleared: %+v", meta)
	}

	// 文件已改名为分段，原合并产物名不复存在。
	for _, gone := range []string{lay.mergedVideoPath(), lay.mergedDanmakuPath()} {
		if _, err := os.Stat(gone); !errors.Is(err, fs.ErrNotExist) {
			t.Fatalf("%s must be renamed away (stat err = %v)", gone, err)
		}
	}
	for _, kept := range []string{lay.segmentVideoPath(1), lay.segmentDanmakuPath(1)} {
		if _, err := os.Stat(kept); err != nil {
			t.Fatalf("archived segment file missing: %v", err)
		}
	}
}

func TestRecoverPendingFinishesInterruptedSessions(t *testing.T) {
	metaTag, videoSeq, audioSeq := testInitTags()
	part1 := buildFLVStream(t, metaTag, videoSeq, audioSeq)
	repo := newTestRepo(t, nil)
	lay, metaPath := seedMergeSession(t, repo, part1)
	// 模拟合并期间崩溃
	updateMeta(metaPath, func(meta *sessionMeta) { meta.Status = metaStatusMerging })

	if err := repo.RecoverPending(context.Background()); err != nil {
		t.Fatalf("RecoverPending: %v", err)
	}
	meta, err := loadMeta(metaPath)
	if err != nil {
		t.Fatal(err)
	}
	if meta.Status != metaStatusDone {
		t.Fatalf("status = %q, want %q", meta.Status, metaStatusDone)
	}
	if meta.MergedVideo != lay.mergedVideoName() {
		t.Fatalf("merge bookkeeping = %+v", meta)
	}
}

// 录制中崩溃是最常见的中断：状态仍是 recording、没有结束时间。恢复时
// 补上结束时间并完成合并。
func TestRecoverPendingFinishesRecordingSessions(t *testing.T) {
	metaTag, videoSeq, audioSeq := testInitTags()
	part1 := buildFLVStream(t, metaTag, videoSeq, audioSeq)
	repo := newTestRepo(t, nil)
	lay, metaPath := seedMergeSession(t, repo, part1) // PrepareSession 后状态即 recording

	before := time.Now().Unix()
	if err := repo.RecoverPending(context.Background()); err != nil {
		t.Fatalf("RecoverPending: %v", err)
	}
	meta, err := loadMeta(metaPath)
	if err != nil {
		t.Fatal(err)
	}
	if meta.Status != metaStatusDone || meta.MergedVideo != lay.mergedVideoName() {
		t.Fatalf("meta = %+v, want done with merged output", meta)
	}
	if meta.EndTime < before {
		t.Fatalf("EndTime = %d, want stamped at recovery (>= %d)", meta.EndTime, before)
	}
}

// 旧版本遗留的未知状态（如 "remuxing"）：跳过并原样保留。
func TestRecoverPendingSkipsUnknownStatus(t *testing.T) {
	metaTag, videoSeq, audioSeq := testInitTags()
	part1 := buildFLVStream(t, metaTag, videoSeq, audioSeq)
	repo := newTestRepo(t, nil)
	lay, metaPath := seedMergeSession(t, repo, part1)
	updateMeta(metaPath, func(meta *sessionMeta) { meta.Status = "remuxing" })

	if err := repo.RecoverPending(context.Background()); err != nil {
		t.Fatalf("RecoverPending: %v", err)
	}
	meta, err := loadMeta(metaPath)
	if err != nil {
		t.Fatal(err)
	}
	if meta.Status != "remuxing" {
		t.Fatalf("status = %q, want legacy status untouched", meta.Status)
	}
	if _, err := os.Stat(lay.segmentVideoPath(1)); err != nil {
		t.Fatalf("legacy source must be left in place: %v", err)
	}
	if _, err := os.Stat(lay.mergedVideoPath()); !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("no merged file must be produced for legacy sessions (stat err = %v)", err)
	}
}

// partial 且源分段齐全：重试合并。
func TestRecoverPendingRetriesPartialWithSources(t *testing.T) {
	metaTag, videoSeq, audioSeq := testInitTags()
	part1 := buildFLVStream(t, metaTag, videoSeq, audioSeq)
	repo := newTestRepo(t, nil)
	lay, metaPath := seedMergeSession(t, repo, part1)
	updateMeta(metaPath, func(meta *sessionMeta) { meta.Status = metaStatusPartial })

	if err := repo.RecoverPending(context.Background()); err != nil {
		t.Fatalf("RecoverPending: %v", err)
	}
	meta, err := loadMeta(metaPath)
	if err != nil {
		t.Fatal(err)
	}
	if meta.Status != metaStatusDone || meta.MergedVideo != lay.mergedVideoName() {
		t.Fatalf("meta = %+v, want done with merged output", meta)
	}
}

// partial 但源分段缺失（如旧版本的转封装产物）：原样保留，不反复报错。
func TestRecoverPendingLeavesPartialWithoutSources(t *testing.T) {
	metaTag, videoSeq, audioSeq := testInitTags()
	part1 := buildFLVStream(t, metaTag, videoSeq, audioSeq)
	repo := newTestRepo(t, nil)
	lay, metaPath := seedMergeSession(t, repo, part1, nil) // part2 未落盘
	updateMeta(metaPath, func(meta *sessionMeta) { meta.Status = metaStatusPartial })

	if err := repo.RecoverPending(context.Background()); err != nil {
		t.Fatalf("RecoverPending: %v", err)
	}
	meta, err := loadMeta(metaPath)
	if err != nil {
		t.Fatal(err)
	}
	if meta.Status != metaStatusPartial {
		t.Fatalf("status = %q, want partial left as-is", meta.Status)
	}
	if _, err := os.Stat(lay.mergedVideoPath()); !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("no merged file must be produced (stat err = %v)", err)
	}
}

// 一个损坏的 meta.json 不能挡住其他会话的恢复。
func TestRecoverPendingSkipsUnreadableMeta(t *testing.T) {
	metaTag, videoSeq, audioSeq := testInitTags()
	part1 := buildFLVStream(t, metaTag, videoSeq, audioSeq)
	repo := newTestRepo(t, nil)
	lay, metaPath := seedMergeSession(t, repo, part1)

	badDir := filepath.Join(repo.recordRoot, "1_broken", "2026-08-11")
	if err := os.MkdirAll(badDir, 0o755); err != nil {
		t.Fatal(err)
	}
	badMeta := filepath.Join(badDir, "20260811_2000_x"+metaExt)
	if err := os.WriteFile(badMeta, []byte("{not json"), 0o644); err != nil {
		t.Fatal(err)
	}

	if err := repo.RecoverPending(context.Background()); err != nil {
		t.Fatalf("RecoverPending: %v", err)
	}
	meta, err := loadMeta(metaPath)
	if err != nil {
		t.Fatal(err)
	}
	if meta.Status != metaStatusDone || meta.MergedVideo != lay.mergedVideoName() {
		t.Fatalf("healthy session not recovered: %+v", meta)
	}
	if got, err := os.ReadFile(badMeta); err != nil || string(got) != "{not json" {
		t.Fatalf("corrupt meta must be left untouched: %q, %v", got, err)
	}
}

func TestRecoverPendingStopsOnCanceledContext(t *testing.T) {
	metaTag, videoSeq, audioSeq := testInitTags()
	repo := newTestRepo(t, nil)
	lay, metaPath := seedMergeSession(t, repo, buildFLVStream(t, metaTag, videoSeq, audioSeq))

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := repo.RecoverPending(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("err = %v, want context.Canceled", err)
	}
	meta, err := loadMeta(metaPath)
	if err != nil {
		t.Fatal(err)
	}
	if meta.Status != metaStatusRecording {
		t.Fatalf("status = %q, want session untouched after cancel", meta.Status)
	}
	if _, err := os.Stat(lay.mergedVideoPath()); !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("no merge may run after cancel (stat err = %v)", err)
	}
}
