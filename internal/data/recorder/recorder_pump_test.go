package recorder

import (
	"bytes"
	"context"
	"errors"
	"io"
	"os"
	"strings"
	"testing"
	"time"

	"suika/internal/biz"
	"suika/internal/data/flv"
)

// closeTracker 记录 Body 是否被关闭：PumpSession 在任何退出路径上都必须
// 关闭流，否则连接泄漏。
type closeTracker struct {
	io.Reader
	closed bool
}

func (c *closeTracker) Close() error {
	c.closed = true
	return nil
}

func TestPumpSessionProgressIncludesBufferedBlock(t *testing.T) {
	stats := &sessionStats{}
	loop := &pumpState{
		stats:     stats,
		baseBytes: 100,
		result:    biz.RecordingResult{BytesWritten: 200},
	}
	tag := &flv.Tag{Type: flv.TagVideo, Timestamp: 1000, Data: []byte{0x27, 0x01, 0xAA}}
	loop.guard.add(tag)
	loop.updateProgress()

	want := int64(100 + 200 + len(tag.Data) + flv.TagOverhead)
	got := stats.bytesWritten()
	if got != want {
		t.Fatalf("progress = %d, want %d", got, want)
	}
}

func TestPumpSessionRejectsNilStream(t *testing.T) {
	repo := newTestRepo(t, nil)
	if _, err := repo.PumpSession(context.Background(), testSession(), nil, nil); !errors.Is(err, biz.ErrRoomInternal) {
		t.Fatalf("err = %v, want ErrRoomInternal", err)
	}
	if _, err := repo.PumpSession(context.Background(), testSession(), &biz.LiveStream{}, nil); !errors.Is(err, biz.ErrRoomInternal) {
		t.Fatalf("err = %v, want ErrRoomInternal", err)
	}
}

// 流开头不是合法 FLV：按瞬时错误处理（交给断流决策树重连），不落任何
// 分段，且仍要关闭 Body。
func TestPumpSessionRejectsBadHeader(t *testing.T) {
	repo := newTestRepo(t, nil)
	ctx := context.Background()
	session := testSession()
	if err := repo.PrepareSession(ctx, session); err != nil {
		t.Fatal(err)
	}

	body := &closeTracker{Reader: strings.NewReader("<html>403 forbidden, definitely not flv</html>")}
	_, err := repo.PumpSession(ctx, session, &biz.LiveStream{Body: body}, nil)
	if !errors.Is(err, biz.ErrStreamTransient) {
		t.Fatalf("err = %v, want ErrStreamTransient", err)
	}
	if !body.closed {
		t.Fatal("stream body must be closed on header failure")
	}

	lay, err := sessionPaths(repo.recordRoot, session)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(lay.segmentVideoPath(1)); err == nil {
		t.Fatal("no segment may be created for an invalid stream")
	}
}

// 流中途被截断（连接断开时最后一个 tag 不完整）：以 ErrStreamTransient
// 返回，但此前已收到的完整数据（含去重缓冲中尚未裁决的块）必须落盘。
func TestPumpSessionTruncatedStreamKeepsReceivedData(t *testing.T) {
	repo := newTestRepo(t, nil)
	ctx := context.Background()
	session := testSession()
	if err := repo.PrepareSession(ctx, session); err != nil {
		t.Fatal(err)
	}

	metaTag, videoSeq, audioSeq := testInitTags()
	key0 := &flv.Tag{Type: flv.TagVideo, Timestamp: 0, Data: []byte{0x17, 0x01, 0, 0, 0, 0xAA}}
	inter40 := &flv.Tag{Type: flv.TagVideo, Timestamp: 40, Data: []byte{0x27, 0x01, 0, 0, 0, 0xBB}}
	inter60 := &flv.Tag{Type: flv.TagVideo, Timestamp: 60, Data: []byte{0x27, 0x01, 0, 0, 0, 0xCC}}
	data := buildFLVStream(t, metaTag, videoSeq, audioSeq, key0, inter40, inter60)
	data = data[:len(data)-6] // 砍掉最后一个 tag 的 PreviousTagSize 与部分载荷

	body := &closeTracker{Reader: bytes.NewReader(data)}
	result, err := repo.PumpSession(ctx, session, &biz.LiveStream{
		Quality: biz.StreamQuality{Qn: 10000, Desc: "source"},
		Body:    body,
	}, nil)
	if !errors.Is(err, biz.ErrStreamTransient) {
		t.Fatalf("err = %v, want ErrStreamTransient", err)
	}
	if !body.closed {
		t.Fatal("stream body must be closed")
	}
	if result == nil || result.Parts != 1 {
		t.Fatalf("result = %+v, want 1 part", result)
	}

	lay, err := sessionPaths(repo.recordRoot, session)
	if err != nil {
		t.Fatal(err)
	}
	_, got := readSegmentTags(t, lay.segmentVideoPath(1))
	assertTagsEqual(t, got, []*flv.Tag{metaTag, videoSeq, audioSeq, key0, inter40})

	// 分段已正常收尾：meta 里记录了结束时间戳与字节数。
	meta, err := loadMeta(lay.metaPath())
	if err != nil {
		t.Fatal(err)
	}
	if len(meta.Segments) != 1 || meta.Segments[0].TsEnd != 40 || meta.Segments[0].Bytes == 0 {
		t.Fatalf("segment not finalized in meta: %+v", meta.Segments)
	}
}

// 视频流里始终没有关键帧：不开段、不写盘，也不算错误。
func TestPumpSessionWithoutKeyframeWritesNothing(t *testing.T) {
	repo := newTestRepo(t, nil)
	ctx := context.Background()
	session := testSession()
	if err := repo.PrepareSession(ctx, session); err != nil {
		t.Fatal(err)
	}

	metaTag, videoSeq, audioSeq := testInitTags()
	inter := &flv.Tag{Type: flv.TagVideo, Timestamp: 20, Data: []byte{0x27, 0x01, 0, 0, 0, 0xAA}}
	result, err := repo.PumpSession(ctx, session, &biz.LiveStream{
		Quality: biz.StreamQuality{Qn: 10000, Desc: "source"},
		Body:    io.NopCloser(bytes.NewReader(buildFLVStream(t, metaTag, videoSeq, audioSeq, inter))),
	}, nil)
	if err != nil {
		t.Fatalf("PumpSession: %v", err)
	}
	if result.Parts != 0 || result.BytesWritten != 0 {
		t.Fatalf("result = %+v, want nothing written", result)
	}

	lay, err := sessionPaths(repo.recordRoot, session)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(lay.segmentVideoPath(1)); err == nil {
		t.Fatal("no segment file may exist without a keyframe")
	}
	meta, err := loadMeta(lay.metaPath())
	if err != nil {
		t.Fatal(err)
	}
	if len(meta.Segments) != 0 {
		t.Fatalf("segments = %+v, want none", meta.Segments)
	}
}

func TestPumpSessionSingleSegment(t *testing.T) {
	repo := newTestRepo(t, nil)
	ctx := context.Background()
	session := testSession()
	if err := repo.PrepareSession(ctx, session); err != nil {
		t.Fatal(err)
	}

	metaTag, videoSeq, audioSeq := testInitTags()
	key0 := &flv.Tag{Type: flv.TagVideo, Timestamp: 0, Data: []byte{0x17, 0x01, 0, 0, 0, 0xAA}}
	inter40 := &flv.Tag{Type: flv.TagVideo, Timestamp: 40, Data: []byte{0x27, 0x01, 0, 0, 0, 0xBB}}
	tags := []*flv.Tag{metaTag, videoSeq, audioSeq, key0, inter40}

	var wantBytes int64
	for _, tag := range []*flv.Tag{key0, inter40} { // 初始化标签不计入
		wantBytes += int64(len(tag.AppendTo(nil)))
	}

	stream := &biz.LiveStream{
		Quality: biz.StreamQuality{Qn: 10000, Desc: "source"},
		Body:    io.NopCloser(bytes.NewReader(buildFLVStream(t, tags...))),
	}
	// events 传 nil：永远不就绪，不会有弹幕事件插入。
	result, err := repo.PumpSession(ctx, session, stream, nil)
	if err != nil {
		t.Fatalf("PumpSession: %v", err)
	}
	if result.Parts != 1 || result.BytesWritten != wantBytes {
		t.Fatalf("result = %+v, want 1 part / %d bytes", result, wantBytes)
	}

	lay, err := sessionPaths(repo.recordRoot, session)
	if err != nil {
		t.Fatal(err)
	}
	// part1 在首个视频关键帧处开启，初始化标签取自缓存注入，
	// 流内容原样写入：任何初始化标签都不会出现两次。
	videoPath := lay.segmentVideoPath(1)
	header, gotTags := readSegmentTags(t, videoPath)
	if !header.HasAudio || !header.HasVideo {
		t.Fatalf("header flags = %+v", header)
	}
	assertTagsEqual(t, gotTags, []*flv.Tag{metaTag, videoSeq, audioSeq, key0, inter40})

	metaPath := lay.metaPath()
	meta, err := loadMeta(metaPath)
	if err != nil {
		t.Fatal(err)
	}
	if meta.Quality.Qn != 10000 || meta.Quality.Desc != "source" {
		t.Fatalf("quality = %+v", meta.Quality)
	}
	if len(meta.Segments) != 1 {
		t.Fatalf("segments = %+v, want 1", meta.Segments)
	}
	seg := meta.Segments[0]
	if seg.Part != 1 || seg.Video != lay.segmentVideoName(1) || seg.Danmaku != lay.segmentDanmakuName(1) {
		t.Fatalf("segment = %+v", seg)
	}
	if seg.TsStart != 0 || seg.TsEnd != 40 {
		t.Fatalf("segment bookkeeping = %+v", seg)
	}
	if fi, err := os.Stat(videoPath); err != nil || seg.Bytes != fi.Size() {
		t.Fatalf("segment bytes = %d, file size/+err = %d/%v", seg.Bytes, fi.Size(), err)
	}

	stats, err := repo.Stats(ctx, session.RoomID)
	if err != nil || stats == nil {
		t.Fatalf("Stats = %+v, %v", stats, err)
	}
	if stats.BytesWritten != wantBytes || stats.CurrentFile != videoPath {
		t.Fatalf("stats = %+v, want %d bytes at %s", stats, wantBytes, videoPath)
	}
}

// 断流重连后同一场次的第二次 PumpSession 调用：写入进度跨调用累加（重连
// 不重置），分段编号接续，不覆盖第一次连接写出的分段。
func TestPumpSessionReconnectContinuesSession(t *testing.T) {
	repo := newTestRepo(t, nil)
	ctx := context.Background()
	session := testSession()
	if err := repo.PrepareSession(ctx, session); err != nil {
		t.Fatal(err)
	}

	metaTag, videoSeq, audioSeq := testInitTags()
	key0 := &flv.Tag{Type: flv.TagVideo, Timestamp: 0, Data: []byte{0x17, 0x01, 0, 0, 0, 0xAA}}
	inter40 := &flv.Tag{Type: flv.TagVideo, Timestamp: 40, Data: []byte{0x27, 0x01, 0, 0, 0, 0xBB}}
	keyB := &flv.Tag{Type: flv.TagVideo, Timestamp: 0, Data: []byte{0x17, 0x01, 0, 0, 0, 0xCC}}
	interB := &flv.Tag{Type: flv.TagVideo, Timestamp: 40, Data: []byte{0x27, 0x01, 0, 0, 0, 0xDD}}
	pump := func(tags ...*flv.Tag) *biz.RecordingResult {
		t.Helper()
		result, err := repo.PumpSession(ctx, session, &biz.LiveStream{
			Quality: biz.StreamQuality{Qn: 10000, Desc: "source"},
			Body:    io.NopCloser(bytes.NewReader(buildFLVStream(t, tags...))),
		}, nil)
		if err != nil {
			t.Fatalf("PumpSession: %v", err)
		}
		return result
	}

	first := pump(metaTag, videoSeq, audioSeq, key0, inter40)
	second := pump(metaTag, videoSeq, audioSeq, keyB, interB)
	// RecordingResult 只统计各自这次调用。
	if first.Parts != 1 || second.Parts != 1 || first.BytesWritten != second.BytesWritten {
		t.Fatalf("results = %+v, %+v; want 1 part each with equal bytes", first, second)
	}

	lay, err := sessionPaths(repo.recordRoot, session)
	if err != nil {
		t.Fatal(err)
	}
	_, part1 := readSegmentTags(t, lay.segmentVideoPath(1))
	assertTagsEqual(t, part1, []*flv.Tag{metaTag, videoSeq, audioSeq, key0, inter40})
	_, part2 := readSegmentTags(t, lay.segmentVideoPath(2))
	assertTagsEqual(t, part2, []*flv.Tag{metaTag, videoSeq, audioSeq, keyB, interB})

	meta, err := loadMeta(lay.metaPath())
	if err != nil {
		t.Fatal(err)
	}
	if len(meta.Segments) != 2 || meta.Segments[0].Part != 1 || meta.Segments[1].Part != 2 {
		t.Fatalf("segments = %+v, want parts 1 and 2", meta.Segments)
	}

	// 进度跨重连持续累加（只有 PrepareSession 开新会话才清零）。
	stats, err := repo.Stats(ctx, session.RoomID)
	if err != nil || stats == nil {
		t.Fatalf("Stats = %+v, %v", stats, err)
	}
	if want := first.BytesWritten + second.BytesWritten; stats.BytesWritten != want {
		t.Fatalf("stats.BytesWritten = %d, want %d accumulated across reconnects", stats.BytesWritten, want)
	}
}

// ctx 取消（关闭录制开关、停机、删除房间）时：返回 ctx 错误，且去重
// 缓冲里已收到但尚未裁决的块必须落盘；弹幕事件只写入已开启的分段，
// 开段之前到达的事件被丢弃。
func TestPumpSessionCancelFlushesPendingBlockAndRoutesDanmaku(t *testing.T) {
	repo := newTestRepo(t, nil)
	session := testSession()
	if err := repo.PrepareSession(context.Background(), session); err != nil {
		t.Fatal(err)
	}

	metaTag, videoSeq, audioSeq := testInitTags()
	key0 := &flv.Tag{Type: flv.TagVideo, Timestamp: 0, Data: []byte{0x17, 0x01, 0, 0, 0, 0xAA}}
	inter40 := &flv.Tag{Type: flv.TagVideo, Timestamp: 40, Data: []byte{0x27, 0x01, 0, 0, 0, 0xBB}}
	feed := func(tags ...*flv.Tag) []byte {
		var b []byte
		for _, tag := range tags {
			b = tag.AppendTo(b)
		}
		return b
	}

	pr, pw := io.Pipe()
	t.Cleanup(func() { pw.Close() }) // 解除后台读取协程的阻塞
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	events := make(chan *biz.DanmakuEvent)

	type outcome struct {
		result *biz.RecordingResult
		err    error
	}
	done := make(chan outcome, 1)
	go func() {
		result, err := repo.PumpSession(ctx, session, &biz.LiveStream{
			Quality: biz.StreamQuality{Qn: 10000, Desc: "source"},
			Body:    pr,
		}, events)
		done <- outcome{result, err}
	}()

	if _, err := pw.Write((&flv.FileHeader{Version: 1, HasAudio: true, HasVideo: true}).Bytes()); err != nil {
		t.Fatal(err)
	}
	// 尚未开段：事件被静默丢弃（无阻塞、无落盘）。
	events <- &biz.DanmakuEvent{TS: time.Unix(1, 0), Type: biz.EventDanmaku, Text: "early"}

	// 首个关键帧到达后开段；等到当前文件出现再发事件，保证事件落入该分段。
	if _, err := pw.Write(feed(metaTag, videoSeq, audioSeq, key0)); err != nil {
		t.Fatal(err)
	}
	waitUntil(t, "segment to open", func() bool {
		stats, _ := repo.Stats(context.Background(), session.RoomID)
		return stats != nil && stats.CurrentFile != ""
	})
	events <- &biz.DanmakuEvent{TS: time.Unix(2, 0), Type: biz.EventDanmaku, Text: "live"}

	if _, err := pw.Write(feed(inter40)); err != nil {
		t.Fatal(err)
	}
	// 进度含去重缓冲中的字节：等到 key0 + inter40 都进入缓冲再取消。
	wantBytes := int64(len(key0.AppendTo(nil)) + len(inter40.AppendTo(nil)))
	waitUntil(t, "tags to be buffered", func() bool {
		stats, _ := repo.Stats(context.Background(), session.RoomID)
		return stats != nil && stats.BytesWritten == wantBytes
	})
	cancel()

	got := <-done
	if !errors.Is(got.err, context.Canceled) {
		t.Fatalf("err = %v, want context.Canceled", got.err)
	}
	if got.result.Parts != 1 || got.result.BytesWritten != wantBytes {
		t.Fatalf("result = %+v, want 1 part / %d bytes (pending block must be flushed)", got.result, wantBytes)
	}

	lay, err := sessionPaths(repo.recordRoot, session)
	if err != nil {
		t.Fatal(err)
	}
	_, tags := readSegmentTags(t, lay.segmentVideoPath(1))
	assertTagsEqual(t, tags, []*flv.Tag{metaTag, videoSeq, audioSeq, key0, inter40})

	danmaku, err := os.ReadFile(lay.segmentDanmakuPath(1))
	if err != nil {
		t.Fatal(err)
	}
	if lines := strings.Count(string(danmaku), "\n"); lines != 1 || !strings.Contains(string(danmaku), `"text":"live"`) {
		t.Fatalf("danmaku file = %q, want exactly the post-open event", danmaku)
	}

	// 取消也要正常收尾分段记录。
	meta, err := loadMeta(lay.metaPath())
	if err != nil {
		t.Fatal(err)
	}
	if len(meta.Segments) != 1 || meta.Segments[0].TsEnd != 40 || meta.Segments[0].WallEnd == 0 {
		t.Fatalf("segment not finalized after cancel: %+v", meta.Segments)
	}
}

func TestPumpSessionSplitsAtKeyframe(t *testing.T) {
	repo := newTestRepo(t, nil)
	setSplitLimits(t, maxSegmentSize, 50*time.Millisecond) // 测试中使用亚分钟粒度
	ctx := context.Background()
	session := testSession()
	if err := repo.PrepareSession(ctx, session); err != nil {
		t.Fatal(err)
	}

	metaTag, videoSeq, audioSeq := testInitTags()
	key0 := &flv.Tag{Type: flv.TagVideo, Timestamp: 0, Data: []byte{0x17, 0x01, 0, 0, 0, 0xAA}}
	inter40 := &flv.Tag{Type: flv.TagVideo, Timestamp: 40, Data: []byte{0x27, 0x01, 0, 0, 0, 0xBB}}
	key100 := &flv.Tag{Type: flv.TagVideo, Timestamp: 100, Data: []byte{0x17, 0x01, 0, 0, 0, 0xCC}}
	inter120 := &flv.Tag{Type: flv.TagVideo, Timestamp: 120, Data: []byte{0x27, 0x01, 0, 0, 0, 0xDD}}
	tags := []*flv.Tag{metaTag, videoSeq, audioSeq, key0, inter40, key100, inter120}

	var wantBytes int64
	for _, tag := range []*flv.Tag{key0, inter40, key100, inter120} { // 初始化标签不计入
		wantBytes += int64(len(tag.AppendTo(nil)))
	}

	stream := &biz.LiveStream{
		Quality: biz.StreamQuality{Qn: 10000, Desc: "source"},
		Body:    io.NopCloser(bytes.NewReader(buildFLVStream(t, tags...))),
	}
	result, err := repo.PumpSession(ctx, session, stream, nil)
	if err != nil {
		t.Fatalf("PumpSession: %v", err)
	}
	if result.Parts != 2 || result.BytesWritten != wantBytes {
		t.Fatalf("result = %+v, want 2 parts / %d bytes", result, wantBytes)
	}

	lay, err := sessionPaths(repo.recordRoot, session)
	if err != nil {
		t.Fatal(err)
	}
	_, part1 := readSegmentTags(t, lay.segmentVideoPath(1))
	assertTagsEqual(t, part1, []*flv.Tag{metaTag, videoSeq, audioSeq, key0, inter40})

	// part2 以完整重注入的头缓存开始，随后是触发切分的关键帧。
	_, part2 := readSegmentTags(t, lay.segmentVideoPath(2))
	assertTagsEqual(t, part2, []*flv.Tag{metaTag, videoSeq, audioSeq, key100, inter120})

	meta, err := loadMeta(lay.metaPath())
	if err != nil {
		t.Fatal(err)
	}
	if len(meta.Segments) != 2 {
		t.Fatalf("segments = %+v, want 2", meta.Segments)
	}
	if meta.Segments[0].TsEnd != 40 {
		t.Fatalf("part1 TsEnd = %d, want 40", meta.Segments[0].TsEnd)
	}
	if meta.Segments[1].TsStart != 100 || meta.Segments[1].TsEnd != 120 {
		t.Fatalf("part2 ts = %d..%d, want 100..120", meta.Segments[1].TsStart, meta.Segments[1].TsEnd)
	}
}

// 回归：触发新分段的那个 tag 必须恰好写入一次。此前拉流写入会在开启
// part1 前先缓存首个初始化标签，openSegment 重注入（此时已非空的）缓存，
// 拉流写入又把同一个标签写了一遍——part1 里的 onMetaData 因此重复。
func TestPumpSessionSingleSegmentHeadersWrittenOnce(t *testing.T) {
	repo := newTestRepo(t, nil)
	ctx := context.Background()
	session := testSession()
	if err := repo.PrepareSession(ctx, session); err != nil {
		t.Fatal(err)
	}

	metaTag, videoSeq, audioSeq := testInitTags()
	key0 := &flv.Tag{Type: flv.TagVideo, Timestamp: 0, Data: []byte{0x17, 0x01, 0, 0, 0, 0xAA}}
	audio20 := &flv.Tag{Type: flv.TagAudio, Timestamp: 20, Data: []byte{0xAF, 0x01, 0x09}}
	inter40 := &flv.Tag{Type: flv.TagVideo, Timestamp: 40, Data: []byte{0x27, 0x01, 0, 0, 0, 0xBB}}
	tags := []*flv.Tag{metaTag, videoSeq, audioSeq, key0, audio20, inter40}

	stream := &biz.LiveStream{
		Quality: biz.StreamQuality{Qn: 10000, Desc: "source"},
		Body:    io.NopCloser(bytes.NewReader(buildFLVStream(t, tags...))),
	}
	result, err := repo.PumpSession(ctx, session, stream, nil)
	if err != nil {
		t.Fatalf("PumpSession: %v", err)
	}
	if result.Parts != 1 {
		t.Fatalf("parts = %d, want 1", result.Parts)
	}

	lay, err := sessionPaths(repo.recordRoot, session)
	if err != nil {
		t.Fatal(err)
	}
	_, got := readSegmentTags(t, lay.segmentVideoPath(1))
	if n := countMatchingTags(got, metaTag); n != 1 {
		t.Errorf("onMetaData appears %d times in part1, want exactly 1", n)
	}
	if n := countMatchingTags(got, videoSeq); n != 1 {
		t.Errorf("AVC sequence header appears %d times in part1, want exactly 1", n)
	}
	if n := countMatchingTags(got, audioSeq); n != 1 {
		t.Errorf("AAC sequence header appears %d times in part1, want exactly 1", n)
	}
	// part1 以空缓存开启：流内容原样写入。
	assertTagsEqual(t, got, tags)
}

// 回归（切分的一半）：part2 必须把缓存的 metadata / AVC / AAC 头各恰好
// 重注入一次——开启 part2 的切分关键帧不在缓存中，所以那边也不会重复。
func TestPumpSessionSplitHeadersWrittenOnce(t *testing.T) {
	repo := newTestRepo(t, nil)
	setSplitLimits(t, maxSegmentSize, 50*time.Millisecond) // 测试中使用亚分钟粒度
	ctx := context.Background()
	session := testSession()
	if err := repo.PrepareSession(ctx, session); err != nil {
		t.Fatal(err)
	}

	metaTag, videoSeq, audioSeq := testInitTags()
	key0 := &flv.Tag{Type: flv.TagVideo, Timestamp: 0, Data: []byte{0x17, 0x01, 0, 0, 0, 0xAA}}
	inter40 := &flv.Tag{Type: flv.TagVideo, Timestamp: 40, Data: []byte{0x27, 0x01, 0, 0, 0, 0xBB}}
	key100 := &flv.Tag{Type: flv.TagVideo, Timestamp: 100, Data: []byte{0x17, 0x01, 0, 0, 0, 0xCC}}
	audio110 := &flv.Tag{Type: flv.TagAudio, Timestamp: 110, Data: []byte{0xAF, 0x01, 0x09}}
	inter120 := &flv.Tag{Type: flv.TagVideo, Timestamp: 120, Data: []byte{0x27, 0x01, 0, 0, 0, 0xDD}}
	tags := []*flv.Tag{metaTag, videoSeq, audioSeq, key0, inter40, key100, audio110, inter120}

	stream := &biz.LiveStream{
		Quality: biz.StreamQuality{Qn: 10000, Desc: "source"},
		Body:    io.NopCloser(bytes.NewReader(buildFLVStream(t, tags...))),
	}
	result, err := repo.PumpSession(ctx, session, stream, nil)
	if err != nil {
		t.Fatalf("PumpSession: %v", err)
	}
	if result.Parts != 2 {
		t.Fatalf("parts = %d, want 2", result.Parts)
	}

	lay, err := sessionPaths(repo.recordRoot, session)
	if err != nil {
		t.Fatal(err)
	}
	_, part1 := readSegmentTags(t, lay.segmentVideoPath(1))
	assertTagsEqual(t, part1, []*flv.Tag{metaTag, videoSeq, audioSeq, key0, inter40})

	_, part2 := readSegmentTags(t, lay.segmentVideoPath(2))
	if n := countMatchingTags(part2, metaTag); n != 1 {
		t.Errorf("onMetaData appears %d times in part2, want exactly 1", n)
	}
	if n := countMatchingTags(part2, videoSeq); n != 1 {
		t.Errorf("AVC sequence header appears %d times in part2, want exactly 1", n)
	}
	if n := countMatchingTags(part2, audioSeq); n != 1 {
		t.Errorf("AAC sequence header appears %d times in part2, want exactly 1", n)
	}
	assertTagsEqual(t, part2, []*flv.Tag{metaTag, videoSeq, audioSeq, key100, audio110, inter120})
}

// TestPumpSessionSplitsOnSeqHeaderChange 验证流中途序列头变化（CDN 换
// 源、主播改码率）触发强制切段：视频与音频序列头各变化一次，产生三段；
// 每段从缓存注入当时的旧初始化标签，新序列头作为首个正文标签写入。
func TestPumpSessionSplitsOnSeqHeaderChange(t *testing.T) {
	repo := newTestRepo(t, nil)
	ctx := context.Background()
	session := testSession()
	if err := repo.PrepareSession(ctx, session); err != nil {
		t.Fatal(err)
	}

	metaTag, videoSeqA, audioSeqA := testInitTags()
	key0 := &flv.Tag{Type: flv.TagVideo, Timestamp: 0, Data: []byte{0x17, 0x01, 0, 0, 0, 0xAA}}
	inter40 := &flv.Tag{Type: flv.TagVideo, Timestamp: 40, Data: []byte{0x27, 0x01, 0, 0, 0, 0xBB}}
	videoSeqB := &flv.Tag{Type: flv.TagVideo, Timestamp: 50, Data: []byte{0x17, 0x00, 0, 0, 0, 9, 9, 9}}
	key60 := &flv.Tag{Type: flv.TagVideo, Timestamp: 60, Data: []byte{0x17, 0x01, 0, 0, 0, 0xCC}}
	inter80 := &flv.Tag{Type: flv.TagVideo, Timestamp: 80, Data: []byte{0x27, 0x01, 0, 0, 0, 0xDD}}
	audioSeqB := &flv.Tag{Type: flv.TagAudio, Timestamp: 90, Data: []byte{0xAF, 0x00, 0x11, 0x90}}
	key100 := &flv.Tag{Type: flv.TagVideo, Timestamp: 100, Data: []byte{0x17, 0x01, 0, 0, 0, 0xEE}}
	tags := []*flv.Tag{metaTag, videoSeqA, audioSeqA, key0, inter40, videoSeqB, key60, inter80, audioSeqB, key100}

	stream := &biz.LiveStream{
		Quality: biz.StreamQuality{Qn: 10000, Desc: "source"},
		Body:    io.NopCloser(bytes.NewReader(buildFLVStream(t, tags...))),
	}
	result, err := repo.PumpSession(ctx, session, stream, nil)
	if err != nil {
		t.Fatalf("PumpSession: %v", err)
	}
	if result.Parts != 3 {
		t.Fatalf("parts = %d, want 3 (split on video and audio seq header changes)", result.Parts)
	}

	lay, err := sessionPaths(repo.recordRoot, session)
	if err != nil {
		t.Fatal(err)
	}
	_, part1 := readSegmentTags(t, lay.segmentVideoPath(1))
	assertTagsEqual(t, part1, []*flv.Tag{metaTag, videoSeqA, audioSeqA, key0, inter40})

	// part2 由视频序列头变化触发：注入变化前的缓存头，新序列头为首个正文标签。
	_, part2 := readSegmentTags(t, lay.segmentVideoPath(2))
	assertTagsEqual(t, part2, []*flv.Tag{metaTag, videoSeqA, audioSeqA, videoSeqB, key60, inter80})

	// part3 由音频序列头变化触发：此时缓存的视频序列头已是 B。
	_, part3 := readSegmentTags(t, lay.segmentVideoPath(3))
	assertTagsEqual(t, part3, []*flv.Tag{metaTag, videoSeqB, audioSeqA, audioSeqB, key100})
}

// TestPumpSessionRepeatedSeqHeaderDoesNotSplit 验证重复出现的相同序列头
// （字节一致）不触发切段：只有解码配置真正变化才值得切。
func TestPumpSessionRepeatedSeqHeaderDoesNotSplit(t *testing.T) {
	repo := newTestRepo(t, nil)
	ctx := context.Background()
	session := testSession()
	if err := repo.PrepareSession(ctx, session); err != nil {
		t.Fatal(err)
	}

	metaTag, videoSeq, audioSeq := testInitTags()
	key0 := &flv.Tag{Type: flv.TagVideo, Timestamp: 0, Data: []byte{0x17, 0x01, 0, 0, 0, 0xAA}}
	videoSeqRepeat := &flv.Tag{Type: flv.TagVideo, Timestamp: 20, Data: videoSeq.Data}
	audioSeqRepeat := &flv.Tag{Type: flv.TagAudio, Timestamp: 30, Data: audioSeq.Data}
	key40 := &flv.Tag{Type: flv.TagVideo, Timestamp: 40, Data: []byte{0x17, 0x01, 0, 0, 0, 0xBB}}
	tags := []*flv.Tag{metaTag, videoSeq, audioSeq, key0, videoSeqRepeat, audioSeqRepeat, key40}

	stream := &biz.LiveStream{
		Quality: biz.StreamQuality{Qn: 10000, Desc: "source"},
		Body:    io.NopCloser(bytes.NewReader(buildFLVStream(t, tags...))),
	}
	result, err := repo.PumpSession(ctx, session, stream, nil)
	if err != nil {
		t.Fatalf("PumpSession: %v", err)
	}
	if result.Parts != 1 {
		t.Fatalf("parts = %d, want 1 (identical seq headers must not split)", result.Parts)
	}

	lay, err := sessionPaths(repo.recordRoot, session)
	if err != nil {
		t.Fatal(err)
	}
	_, part1 := readSegmentTags(t, lay.segmentVideoPath(1))
	assertTagsEqual(t, part1, tags)
}

// TestPumpSessionSplitsAtSizeLimit 验证大小切分的端到端行为：阈值设为
// 250 字节，正文 tag 每个 20 字节、每 5 个一个关键帧。每个分段写到约
// 282 字节（82 字节的头 + 10 个正文 tag）后越过阈值，在下一个关键帧处
// 切分，共产出 4 段；每段（含最后一段）都 ≥ 阈值，且切分点都是关键帧。
func TestPumpSessionSplitsAtSizeLimit(t *testing.T) {
	repo := newTestRepo(t, nil)
	setSplitLimits(t, 250, maxSegmentDuration) // 测试中使用小阈值
	ctx := context.Background()
	session := testSession()
	if err := repo.PrepareSession(ctx, session); err != nil {
		t.Fatal(err)
	}

	metaTag, videoSeq, audioSeq := testInitTags()
	tags := []*flv.Tag{metaTag, videoSeq, audioSeq}
	for i := range 40 {
		data := []byte{0x27, 0x01, 0, 0, 0, byte(i)} // 帧间 NALU
		if i%5 == 0 {
			data[0] = 0x17 // 每 5 个 tag 一个关键帧
		}
		tags = append(tags, &flv.Tag{Type: flv.TagVideo, Timestamp: int64((i + 1) * 10), Data: data})
	}

	stream := &biz.LiveStream{
		Quality: biz.StreamQuality{Qn: 10000, Desc: "source"},
		Body:    io.NopCloser(bytes.NewReader(buildFLVStream(t, tags...))),
	}
	result, err := repo.PumpSession(ctx, session, stream, nil)
	if err != nil {
		t.Fatalf("PumpSession: %v", err)
	}
	if result.Parts != 4 {
		t.Fatalf("parts = %d, want 4", result.Parts)
	}

	lay, err := sessionPaths(repo.recordRoot, session)
	if err != nil {
		t.Fatal(err)
	}
	meta, err := loadMeta(lay.metaPath())
	if err != nil {
		t.Fatal(err)
	}
	for i, seg := range meta.Segments {
		if seg.Bytes < 250 {
			t.Fatalf("segment %d bytes = %d, want >= 250", seg.Part, seg.Bytes)
		}
		// part2 起：注入的三个初始化标签之后首个正文标签必须是关键帧（切分点）。
		if i == 0 {
			continue
		}
		_, segTags := readSegmentTags(t, lay.filePath(seg.Video))
		if len(segTags) < 4 || !segTags[3].IsAVCKeyframe() {
			t.Fatalf("segment %d first body tag after injected headers is not a keyframe", seg.Part)
		}
	}
}

// TestPumpSessionWaitsForFirstKeyframe 验证新场次的首个分段只在第一个视
// 频关键帧处开启：关键帧之前的正文标签被丢弃（流内重连后的新段同理），
// 初始化标签照常入缓存并在开段时注入，保证段首即关键帧、独立可解码。
func TestPumpSessionWaitsForFirstKeyframe(t *testing.T) {
	repo := newTestRepo(t, nil)
	ctx := context.Background()
	session := testSession()
	if err := repo.PrepareSession(ctx, session); err != nil {
		t.Fatal(err)
	}

	metaTag, videoSeq, audioSeq := testInitTags()
	earlyInter1 := &flv.Tag{Type: flv.TagVideo, Timestamp: 0, Data: []byte{0x27, 0x01, 0, 0, 0, 0x01}}
	earlyInter2 := &flv.Tag{Type: flv.TagVideo, Timestamp: 20, Data: []byte{0x27, 0x01, 0, 0, 0, 0x02}}
	key40 := &flv.Tag{Type: flv.TagVideo, Timestamp: 40, Data: []byte{0x17, 0x01, 0, 0, 0, 0xAA}}
	inter60 := &flv.Tag{Type: flv.TagVideo, Timestamp: 60, Data: []byte{0x27, 0x01, 0, 0, 0, 0xBB}}

	stream := &biz.LiveStream{
		Quality: biz.StreamQuality{Qn: 10000, Desc: "source"},
		Body: io.NopCloser(bytes.NewReader(buildFLVStream(t,
			metaTag, videoSeq, audioSeq, earlyInter1, earlyInter2, key40, inter60))),
	}
	result, err := repo.PumpSession(ctx, session, stream, nil)
	if err != nil {
		t.Fatalf("PumpSession: %v", err)
	}
	if result.Parts != 1 {
		t.Fatalf("parts = %d, want 1", result.Parts)
	}

	want := []*flv.Tag{metaTag, videoSeq, audioSeq, key40, inter60}
	var wantBytes int64
	for _, tag := range []*flv.Tag{key40, inter60} {
		wantBytes += int64(len(tag.AppendTo(nil)))
	}
	if result.BytesWritten != wantBytes {
		t.Fatalf("bytes = %d, want %d (pre-keyframe tags must not count)", result.BytesWritten, wantBytes)
	}

	lay, err := sessionPaths(repo.recordRoot, session)
	if err != nil {
		t.Fatal(err)
	}
	_, got := readSegmentTags(t, lay.segmentVideoPath(1))
	assertTagsEqual(t, got, want)
}

// TestPumpSessionAudioOnlyOpensWithoutKeyframe 验证纯音频流的豁免：文件
// 头无视频轨时没有关键帧可等，首个标签即开段。
func TestPumpSessionAudioOnlyOpensWithoutKeyframe(t *testing.T) {
	repo := newTestRepo(t, nil)
	ctx := context.Background()
	session := testSession()
	if err := repo.PrepareSession(ctx, session); err != nil {
		t.Fatal(err)
	}

	audioSeq := &flv.Tag{Type: flv.TagAudio, Timestamp: 0, Data: []byte{0xAF, 0x00, 0x12, 0x10}}
	audio20 := &flv.Tag{Type: flv.TagAudio, Timestamp: 20, Data: []byte{0xAF, 0x01, 0x09}}
	stream := &biz.LiveStream{
		Quality: biz.StreamQuality{Qn: 10000, Desc: "source"},
		Body: io.NopCloser(bytes.NewReader(buildFLVStreamWithHeader(t,
			&flv.FileHeader{Version: 1, HasAudio: true, HasVideo: false},
			audioSeq, audio20))),
	}
	result, err := repo.PumpSession(ctx, session, stream, nil)
	if err != nil {
		t.Fatalf("PumpSession: %v", err)
	}
	if result.Parts != 1 {
		t.Fatalf("parts = %d, want 1", result.Parts)
	}

	lay, err := sessionPaths(repo.recordRoot, session)
	if err != nil {
		t.Fatal(err)
	}
	_, got := readSegmentTags(t, lay.segmentVideoPath(1))
	assertTagsEqual(t, got, []*flv.Tag{audioSeq, audio20})
}

// TestPumpSessionDropsDuplicateBlocks 验证 CDN 循环吐流的去重：内容指纹
// （类型+载荷）重复的块整块丢弃不落盘，唯一块正常写入；未达断开上限时
// 会话正常收尾。
func TestPumpSessionDropsDuplicateBlocks(t *testing.T) {
	repo := newTestRepo(t, nil)
	ctx := context.Background()
	session := testSession()
	if err := repo.PrepareSession(ctx, session); err != nil {
		t.Fatal(err)
	}

	metaTag, videoSeq, audioSeq := testInitTags()
	blockA := []*flv.Tag{
		{Type: flv.TagVideo, Timestamp: 0, Data: []byte{0x17, 0x01, 0, 0, 0, 0xAA}},
		{Type: flv.TagVideo, Timestamp: 20, Data: []byte{0x27, 0x01, 0, 0, 0, 0xBB}},
	}
	blockB := []*flv.Tag{
		{Type: flv.TagVideo, Timestamp: 40, Data: []byte{0x17, 0x01, 0, 0, 0, 0xCC}},
		{Type: flv.TagVideo, Timestamp: 60, Data: []byte{0x27, 0x01, 0, 0, 0, 0xDD}},
	}
	// 循环重放：类型与载荷和 blockA/B 一致，时间戳继续前进。
	replayA := []*flv.Tag{
		{Type: flv.TagVideo, Timestamp: 80, Data: blockA[0].Data},
		{Type: flv.TagVideo, Timestamp: 100, Data: blockA[1].Data},
	}
	replayB := []*flv.Tag{
		{Type: flv.TagVideo, Timestamp: 120, Data: blockB[0].Data},
		{Type: flv.TagVideo, Timestamp: 140, Data: blockB[1].Data},
	}
	// 尾部唯一块：给重放块一个边界裁决点（EOF 的排水不做去重裁决）。
	tail := []*flv.Tag{
		{Type: flv.TagVideo, Timestamp: 160, Data: []byte{0x17, 0x01, 0, 0, 0, 0xEE}},
		{Type: flv.TagVideo, Timestamp: 180, Data: []byte{0x27, 0x01, 0, 0, 0, 0xFF}},
	}

	tags := append([]*flv.Tag{metaTag, videoSeq, audioSeq},
		append(append(append(append(
			blockA, blockB...,
		), replayA...), replayB...), tail...)...)

	stream := &biz.LiveStream{
		Quality: biz.StreamQuality{Qn: 10000, Desc: "source"},
		Body:    io.NopCloser(bytes.NewReader(buildFLVStream(t, tags...))),
	}
	result, err := repo.PumpSession(ctx, session, stream, nil)
	if err != nil {
		t.Fatalf("PumpSession: %v (streak 2 must stay below disconnect threshold)", err)
	}
	if result.Parts != 1 {
		t.Fatalf("parts = %d, want 1", result.Parts)
	}

	want := append([]*flv.Tag{metaTag, videoSeq, audioSeq},
		append(append(blockA, blockB...), tail...)...)
	var wantBytes int64
	for _, tag := range want[3:] { // 跳过初始化标签
		wantBytes += int64(len(tag.AppendTo(nil)))
	}
	if result.BytesWritten != wantBytes {
		t.Fatalf("bytes = %d, want %d (dropped blocks must not count)", result.BytesWritten, wantBytes)
	}

	lay, err := sessionPaths(repo.recordRoot, session)
	if err != nil {
		t.Fatal(err)
	}
	_, got := readSegmentTags(t, lay.segmentVideoPath(1))
	assertTagsEqual(t, got, want)
}

// TestPumpSessionDisconnectsOnCDNLoop 验证连续重复块达到上限时泵送以
// ErrStreamTransient 中止，交由断流决策树换流地址重连（换 CDN 节点）。
func TestPumpSessionDisconnectsOnCDNLoop(t *testing.T) {
	repo := newTestRepo(t, nil)
	ctx := context.Background()
	session := testSession()
	if err := repo.PrepareSession(ctx, session); err != nil {
		t.Fatal(err)
	}

	metaTag, videoSeq, audioSeq := testInitTags()
	// 同一内容反复重放：首次出现正常落盘，之后每块都是重复。
	loopKey := []byte{0x17, 0x01, 0, 0, 0, 0xAA}
	loopInter := []byte{0x27, 0x01, 0, 0, 0, 0xBB}

	tags := []*flv.Tag{metaTag, videoSeq, audioSeq}
	for i := range dupDisconnectStreak + 2 {
		base := int64(i) * 100
		tags = append(
			tags,
			&flv.Tag{Type: flv.TagVideo, Timestamp: base, Data: loopKey},
			&flv.Tag{Type: flv.TagVideo, Timestamp: base + 20, Data: loopInter},
		)
	}

	stream := &biz.LiveStream{
		Quality: biz.StreamQuality{Qn: 10000, Desc: "source"},
		Body:    io.NopCloser(bytes.NewReader(buildFLVStream(t, tags...))),
	}
	result, err := repo.PumpSession(ctx, session, stream, nil)
	if !errors.Is(err, biz.ErrStreamTransient) {
		t.Fatalf("err = %v, want ErrStreamTransient on CDN loop disconnect", err)
	}
	if result.Parts != 1 {
		t.Fatalf("parts = %d, want 1", result.Parts)
	}

	want := []*flv.Tag{
		metaTag, videoSeq, audioSeq,
		{Type: flv.TagVideo, Timestamp: 0, Data: loopKey},
		{Type: flv.TagVideo, Timestamp: 20, Data: loopInter},
	}
	lay, err := sessionPaths(repo.recordRoot, session)
	if err != nil {
		t.Fatal(err)
	}
	_, got := readSegmentTags(t, lay.segmentVideoPath(1))
	assertTagsEqual(t, got, want)
}

// TestPumpSessionSplitOverrunFlushesPendingBlock 验证强制切分（时长超限
// 且仍无关键帧）会先裁决缓冲块：在途数据不能因为关段而丢失。
func TestPumpSessionSplitOverrunFlushesPendingBlock(t *testing.T) {
	repo := newTestRepo(t, nil)
	setSplitLimits(t, maxSegmentSize, 50*time.Millisecond) // 测试中使用亚分钟粒度
	ctx := context.Background()
	session := testSession()
	if err := repo.PrepareSession(ctx, session); err != nil {
		t.Fatal(err)
	}

	metaTag, videoSeq, audioSeq := testInitTags()
	key0 := &flv.Tag{Type: flv.TagVideo, Timestamp: 0, Data: []byte{0x17, 0x01, 0, 0, 0, 0xAA}}
	key40 := &flv.Tag{Type: flv.TagVideo, Timestamp: 40, Data: []byte{0x17, 0x01, 0, 0, 0, 0xBB}}
	// ts=20050 距开播 20.05s，超过 50ms+15s 的强切线，且不是关键帧。
	overrun := &flv.Tag{Type: flv.TagVideo, Timestamp: 20_050, Data: []byte{0x27, 0x01, 0, 0, 0, 0xCC}}
	keyAfter := &flv.Tag{Type: flv.TagVideo, Timestamp: 20_090, Data: []byte{0x17, 0x01, 0, 0, 0, 0xDD}}
	tags := []*flv.Tag{metaTag, videoSeq, audioSeq, key0, key40, overrun, keyAfter}

	stream := &biz.LiveStream{
		Quality: biz.StreamQuality{Qn: 10000, Desc: "source"},
		Body:    io.NopCloser(bytes.NewReader(buildFLVStream(t, tags...))),
	}
	result, err := repo.PumpSession(ctx, session, stream, nil)
	if err != nil {
		t.Fatalf("PumpSession: %v", err)
	}
	if result.Parts != 2 {
		t.Fatalf("parts = %d, want 2 (overrun force split)", result.Parts)
	}

	lay, err := sessionPaths(repo.recordRoot, session)
	if err != nil {
		t.Fatal(err)
	}
	// part1 必须含强切前缓冲的 key40，不能丢。
	_, part1 := readSegmentTags(t, lay.segmentVideoPath(1))
	assertTagsEqual(t, part1, []*flv.Tag{metaTag, videoSeq, audioSeq, key0, key40})
	_, part2 := readSegmentTags(t, lay.segmentVideoPath(2))
	assertTagsEqual(t, part2, []*flv.Tag{metaTag, videoSeq, audioSeq, overrun, keyAfter})
}
