package recorder

import (
	"context"
	"errors"
	"io/fs"
	"os"
	"testing"

	"suika/internal/data/flv"
)

// writeSegments 把每个 FLV 字节流写成会话目录下的一个分段文件，返回对应的
// 分段登记（不含弹幕）。
func writeSegments(t *testing.T, lay sessionLayout, streams ...[]byte) []segmentMeta {
	t.Helper()
	segs := make([]segmentMeta, 0, len(streams))
	for i, content := range streams {
		part := i + 1
		if err := os.WriteFile(lay.segmentVideoPath(part), content, 0o644); err != nil {
			t.Fatal(err)
		}
		segs = append(segs, segmentMeta{Part: part, Video: lay.segmentVideoName(part)})
	}
	return segs
}

func dirEntryNames(t *testing.T, dir string) []string {
	t.Helper()
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	names := make([]string, 0, len(entries))
	for _, e := range entries {
		names = append(names, e.Name())
	}
	return names
}

// 换源重连后新分段的内容时间戳可能从 0 重新开始：合并必须把整段内容
// 平移到边界之后，否则播放器看到的时间戳会回跳。
func TestMergeSessionFilesRebasesResetTimestamps(t *testing.T) {
	metaTag, videoSeq, audioSeq := testInitTags()
	keyA := &flv.Tag{Type: flv.TagVideo, Timestamp: 0, Data: []byte{0x17, 0x01, 0, 0, 0, 0xAA}}
	interA := &flv.Tag{Type: flv.TagVideo, Timestamp: 1000, Data: []byte{0x27, 0x01, 0, 0, 0, 0xBB}}
	// part2 的内容时间戳重新从 0 开始。
	keyB := &flv.Tag{Type: flv.TagVideo, Timestamp: 0, Data: []byte{0x17, 0x01, 0, 0, 0, 0xCC}}
	interB := &flv.Tag{Type: flv.TagVideo, Timestamp: 40, Data: []byte{0x27, 0x01, 0, 0, 0, 0xDD}}

	lay := sessionLayout{dir: t.TempDir(), base: "base"}
	segs := writeSegments(
		t, lay,
		buildFLVStream(t, metaTag, videoSeq, audioSeq, keyA, interA),
		buildFLVStream(t, metaTag, videoSeq, audioSeq, keyB, interB),
	)

	videoName, danmakuName, err := MergeSessionFiles(context.Background(), lay, segs)
	if err != nil {
		t.Fatalf("MergeSessionFiles: %v", err)
	}
	if videoName != lay.mergedVideoName() || danmakuName != "" {
		t.Fatalf("names = %q, %q", videoName, danmakuName)
	}

	_, tags := readSegmentTags(t, lay.mergedVideoPath())
	assertTagsEqual(t, tags, []*flv.Tag{
		videoSeq, audioSeq, keyA, interA,
		{Type: flv.TagVideo, Timestamp: 1000, Data: videoSeq.Data},
		{Type: flv.TagAudio, Timestamp: 1000, Data: audioSeq.Data},
		{Type: flv.TagVideo, Timestamp: 1000, Data: keyB.Data},
		{Type: flv.TagVideo, Timestamp: 1040, Data: interB.Data},
	})
	for i := 1; i < len(tags); i++ {
		if tags[i].Timestamp < tags[i-1].Timestamp {
			t.Fatalf("timestamps not monotonic at tag %d: %d < %d", i, tags[i].Timestamp, tags[i-1].Timestamp)
		}
	}
}

// 分段登记了弹幕文件名但磁盘上没有（或根本没登记）：不产出合并弹幕。
func TestMergeSessionFilesWithoutDanmakuSources(t *testing.T) {
	metaTag, videoSeq, audioSeq := testInitTags()
	lay := sessionLayout{dir: t.TempDir(), base: "base"}
	segs := writeSegments(t, lay, buildFLVStream(t, metaTag, videoSeq, audioSeq))
	segs[0].Danmaku = lay.segmentDanmakuName(1) // 已登记，但文件不存在

	videoName, danmakuName, err := MergeSessionFiles(context.Background(), lay, segs)
	if err != nil {
		t.Fatalf("MergeSessionFiles: %v", err)
	}
	if videoName != lay.mergedVideoName() || danmakuName != "" {
		t.Fatalf("names = %q, %q; want video only", videoName, danmakuName)
	}
	if _, err := os.Stat(lay.mergedDanmakuPath()); !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("merged danmaku must not exist (stat err = %v)", err)
	}
}

// 弹幕合并失败时，已经写好的合并视频要撤回：只有视频与弹幕都成功，
// 调用方才能据此删源，半成品不能留在会话目录里。
func TestMergeSessionFilesDanmakuFailureRemovesVideo(t *testing.T) {
	metaTag, videoSeq, audioSeq := testInitTags()
	lay := sessionLayout{dir: t.TempDir(), base: "base"}
	segs := writeSegments(t, lay, buildFLVStream(t, metaTag, videoSeq, audioSeq))
	// 弹幕"文件"是目录：Stat 通过，但读取必然失败。
	if err := os.Mkdir(lay.segmentDanmakuPath(1), 0o755); err != nil {
		t.Fatal(err)
	}
	segs[0].Danmaku = lay.segmentDanmakuName(1)

	if _, _, err := MergeSessionFiles(context.Background(), lay, segs); err == nil {
		t.Fatal("MergeSessionFiles succeeded with an unreadable danmaku source")
	}
	// 目录里只剩源分段与源弹幕，没有合并产物，也没有临时文件。
	got := dirEntryNames(t, lay.dir)
	if len(got) != 2 {
		t.Fatalf("dir entries = %v, want only the two source entries", got)
	}
	if _, err := os.Stat(lay.segmentVideoPath(1)); err != nil {
		t.Fatalf("source segment must survive: %v", err)
	}
}

func TestMergeSessionFilesCanceledContext(t *testing.T) {
	metaTag, videoSeq, audioSeq := testInitTags()
	lay := sessionLayout{dir: t.TempDir(), base: "base"}
	segs := writeSegments(t, lay, buildFLVStream(t, metaTag, videoSeq, audioSeq))

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, _, err := MergeSessionFiles(ctx, lay, segs); !errors.Is(err, context.Canceled) {
		t.Fatalf("err = %v, want context.Canceled", err)
	}
	// 取消不得留下合并产物或临时文件，源分段原样保留。
	if got := dirEntryNames(t, lay.dir); len(got) != 1 || got[0] != lay.segmentVideoName(1) {
		t.Fatalf("dir entries = %v, want only the source segment", got)
	}
}

func TestFinishSessionMergeSingleSegment(t *testing.T) {
	metaTag, videoSeq, audioSeq := testInitTags()
	key0 := &flv.Tag{Type: flv.TagVideo, Timestamp: 0, Data: []byte{0x17, 0x01, 0, 0, 0, 0xAA}}
	inter40 := &flv.Tag{Type: flv.TagVideo, Timestamp: 40, Data: []byte{0x27, 0x01, 0, 0, 0, 0xBB}}
	repo := newTestRepo(t, nil)
	lay, metaPath := seedMergeSession(t, repo, buildFLVStream(t, metaTag, videoSeq, audioSeq, key0, inter40))
	danmu := `{"ts":1,"type":"danmaku","text":"hi"}` + "\n"
	if err := os.WriteFile(lay.segmentDanmakuPath(1), []byte(danmu), 0o644); err != nil {
		t.Fatal(err)
	}

	if err := repo.FinishSession(context.Background(), testSession()); err != nil {
		t.Fatalf("FinishSession: %v", err)
	}

	meta, err := loadMeta(metaPath)
	if err != nil {
		t.Fatal(err)
	}
	if meta.Status != metaStatusDone {
		t.Fatalf("status = %q, want %q", meta.Status, metaStatusDone)
	}
	if meta.MergedVideo != lay.mergedVideoName() || meta.MergedDanmaku != lay.mergedDanmakuName() {
		t.Fatalf("merge bookkeeping = %+v", meta)
	}

	// 合并产物 = 头 + 除 onMetaData 外的全部标签。
	_, tags := readSegmentTags(t, lay.mergedVideoPath())
	assertTagsEqual(t, tags, []*flv.Tag{videoSeq, audioSeq, key0, inter40})

	// 源分段与源弹幕验证后删除；弹幕内容完整保留。
	for _, src := range []string{lay.segmentVideoPath(1), lay.segmentDanmakuPath(1)} {
		if _, err := os.Stat(src); !errors.Is(err, fs.ErrNotExist) {
			t.Fatalf("%s must be removed after a verified merge (stat err = %v)", src, err)
		}
	}
	if got, err := os.ReadFile(lay.mergedDanmakuPath()); err != nil || string(got) != danmu {
		t.Fatalf("merged danmaku = %q, %v", got, err)
	}
}

// 多段合并：第 2 段重新注入的序列头时间戳平移到合并边界，全片时间戳
// 单调不回跳；两段的弹幕按 part 顺序拼接。
func TestFinishSessionMergeMultiSegmentRebasesBoundaryHeaders(t *testing.T) {
	metaTag, videoSeq, audioSeq := testInitTags()
	key0 := &flv.Tag{Type: flv.TagVideo, Timestamp: 0, Data: []byte{0x17, 0x01, 0, 0, 0, 0xAA}}
	inter40 := &flv.Tag{Type: flv.TagVideo, Timestamp: 40, Data: []byte{0x27, 0x01, 0, 0, 0, 0xBB}}
	// 2 小时分段边界后的内容标签：绝对时间戳延续。
	const boundary = 7_200_000
	keyB := &flv.Tag{Type: flv.TagVideo, Timestamp: boundary, Data: []byte{0x17, 0x01, 0, 0, 0, 0xCC}}
	interB := &flv.Tag{Type: flv.TagVideo, Timestamp: boundary + 40, Data: []byte{0x27, 0x01, 0, 0, 0, 0xDD}}

	part1 := buildFLVStream(t, metaTag, videoSeq, audioSeq, key0, inter40)
	// part2 模拟切段后的重注入：序列头保留近零时间戳，内容标签延续。
	part2 := buildFLVStream(t, metaTag, videoSeq, audioSeq, keyB, interB)
	repo := newTestRepo(t, nil)
	lay, metaPath := seedMergeSession(t, repo, part1, part2)
	if err := os.WriteFile(lay.segmentDanmakuPath(1), []byte("line1\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(lay.segmentDanmakuPath(2), []byte("line2\n"), 0o644); err != nil {
		t.Fatal(err)
	}

	if err := repo.FinishSession(context.Background(), testSession()); err != nil {
		t.Fatalf("FinishSession: %v", err)
	}
	meta, err := loadMeta(metaPath)
	if err != nil {
		t.Fatal(err)
	}
	if meta.Status != metaStatusDone {
		t.Fatalf("status = %q, want %q", meta.Status, metaStatusDone)
	}
	for _, part := range []int{1, 2} {
		if _, err := os.Stat(lay.segmentVideoPath(part)); !errors.Is(err, fs.ErrNotExist) {
			t.Fatalf("segment %d source must be removed after a verified merge (stat err = %v)", part, err)
		}
	}

	_, tags := readSegmentTags(t, lay.mergedVideoPath())
	// 边界处的序列头被平移到 part1 的最后时间戳（40）。
	wantVideoSeq2 := &flv.Tag{Type: flv.TagVideo, Timestamp: 40, Data: videoSeq.Data}
	wantAudioSeq2 := &flv.Tag{Type: flv.TagAudio, Timestamp: 40, Data: audioSeq.Data}
	assertTagsEqual(t, tags, []*flv.Tag{videoSeq, audioSeq, key0, inter40, wantVideoSeq2, wantAudioSeq2, keyB, interB})
	for i := 1; i < len(tags); i++ {
		if tags[i].Timestamp < tags[i-1].Timestamp {
			t.Fatalf("timestamps not monotonic at tag %d: %d < %d", i, tags[i].Timestamp, tags[i-1].Timestamp)
		}
	}
	if n := countMatchingTags(tags, metaTag); n != 0 {
		t.Errorf("onMetaData appears %d times in merged file, want 0", n)
	}

	if got, err := os.ReadFile(lay.mergedDanmakuPath()); err != nil || string(got) != "line1\nline2\n" {
		t.Fatalf("merged danmaku = %q, %v", got, err)
	}
}

// 合并失败（分段损坏）：记录错误、状态 partial、源文件全部保留、
// 不留临时文件。
func TestFinishSessionMergeFailureKeepsSegments(t *testing.T) {
	metaTag, videoSeq, audioSeq := testInitTags()
	part1 := buildFLVStream(t, metaTag, videoSeq, audioSeq)
	repo := newTestRepo(t, nil)
	lay, metaPath := seedMergeSession(t, repo, part1, []byte("not an flv"))

	if err := repo.FinishSession(context.Background(), testSession()); err != nil {
		t.Fatalf("FinishSession records merge failure in meta, got %v", err)
	}

	meta, err := loadMeta(metaPath)
	if err != nil {
		t.Fatal(err)
	}
	if meta.Status != metaStatusPartial {
		t.Fatalf("status = %q, want %q", meta.Status, metaStatusPartial)
	}
	if meta.MergedVideo != "" {
		t.Fatalf("merge bookkeeping = %+v, want empty after failed merge", meta)
	}
	var sawMergeErr bool
	for _, e := range meta.Errors {
		if e.Stage == "merge" {
			sawMergeErr = true
		}
	}
	if !sawMergeErr {
		t.Fatalf("errors = %+v, want a merge-stage error", meta.Errors)
	}
	// 源分段原样保留，无合并产物与临时文件残留。
	for part := 1; part <= 2; part++ {
		if _, err := os.Stat(lay.segmentVideoPath(part)); err != nil {
			t.Fatalf("segment %d source must survive a failed merge: %v", part, err)
		}
	}
	for _, path := range []string{lay.mergedVideoPath(), lay.mergedVideoPath() + ".tmp"} {
		if _, err := os.Stat(path); !errors.Is(err, fs.ErrNotExist) {
			t.Fatalf("%s must not exist after a failed merge (stat err = %v)", path, err)
		}
	}
}

// 分段源文件缺失：合并失败，状态 partial。
func TestFinishSessionMergeMissingSegmentMarksPartial(t *testing.T) {
	metaTag, videoSeq, audioSeq := testInitTags()
	part1 := buildFLVStream(t, metaTag, videoSeq, audioSeq)
	repo := newTestRepo(t, nil)
	_, metaPath := seedMergeSession(t, repo, part1, nil) // part2 未落盘

	if err := repo.FinishSession(context.Background(), testSession()); err != nil {
		t.Fatalf("FinishSession: %v", err)
	}
	meta, err := loadMeta(metaPath)
	if err != nil {
		t.Fatal(err)
	}
	if meta.Status != metaStatusPartial {
		t.Fatalf("status = %q, want %q", meta.Status, metaStatusPartial)
	}
}
