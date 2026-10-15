package recorder

import (
	"bytes"
	"context"
	"io"
	"os"
	"path/filepath"
	"testing"
	"time"

	"suika/internal/biz"
	"suika/internal/conf"
	"suika/internal/data/flv"
)

// 本文件是包内各 *_test.go 共用的测试辅助。

// newTestRepo 构建以全新临时目录为 record_root 的 recorderRepo。
func newTestRepo(t *testing.T, c *conf.Recorder) *recorderRepo {
	t.Helper()
	if c == nil {
		c = &conf.Recorder{}
	}
	c.RecordRoot = t.TempDir()
	repo, ok := NewRecorderRepo(c).(*recorderRepo)
	if !ok {
		t.Fatal("NewRecorderRepo did not return *recorderRepo")
	}
	return repo
}

// setSplitLimits 临时改写分段上限，测试结束后还原；依赖测试不并行。
func setSplitLimits(t *testing.T, size int64, dur time.Duration) {
	t.Helper()
	oldSize, oldDur := maxSegmentSize, maxSegmentDuration
	maxSegmentSize, maxSegmentDuration = size, dur
	t.Cleanup(func() { maxSegmentSize, maxSegmentDuration = oldSize, oldDur })
}

func testSession() *biz.RecordingSession {
	return &biz.RecordingSession{
		RoomID:        42,
		StreamerName:  "tester",
		Title:         "stream title",
		LiveStartTime: time.Date(2026, 8, 11, 20, 0, 0, 0, time.UTC),
		Quality:       biz.StreamQuality{Qn: 10000, Desc: "source"},
	}
}

// testInitTags 返回各测试共用的初始化标签（onMetaData、AVC/AAC 序列头）。
func testInitTags() (metaTag, videoSeq, audioSeq *flv.Tag) {
	metaTag = &flv.Tag{Type: flv.TagScript, Timestamp: 0, Data: []byte{0x02, 0x00, 0x0a, 'o', 'n', 'M', 'e', 't', 'a', 'D', 'a', 't', 'a'}}
	videoSeq = &flv.Tag{Type: flv.TagVideo, Timestamp: 0, Data: []byte{0x17, 0x00, 0, 0, 0, 1, 2, 3}}
	audioSeq = &flv.Tag{Type: flv.TagAudio, Timestamp: 0, Data: []byte{0xAF, 0x00, 0x12, 0x10}}
	return
}

// waitUntil 轮询 cond 直到为真；超时视为测试失败。用于等待另一个
// goroutine 中的泵送推进到已知状态。
func waitUntil(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(2 * time.Millisecond)
	}
}

func touchFiles(t *testing.T, dir string, names ...string) {
	t.Helper()
	for _, name := range names {
		if err := os.WriteFile(filepath.Join(dir, name), nil, 0o644); err != nil {
			t.Fatal(err)
		}
	}
}

func buildFLVStream(t *testing.T, tags ...*flv.Tag) []byte {
	t.Helper()
	return buildFLVStreamWithHeader(t, &flv.FileHeader{Version: 1, HasAudio: true, HasVideo: true}, tags...)
}

// buildFLVStreamWithHeader 用给定的文件头构造字节流（纯音频等场景需要
// 自定义头标志）。
func buildFLVStreamWithHeader(t *testing.T, header *flv.FileHeader, tags ...*flv.Tag) []byte {
	t.Helper()
	var buf bytes.Buffer
	buf.Write(header.Bytes())
	for _, tag := range tags {
		buf.Write(tag.AppendTo(nil))
	}
	return buf.Bytes()
}

func readSegmentTags(t *testing.T, path string) (*flv.FileHeader, []*flv.Tag) {
	t.Helper()
	f, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	header, err := flv.ParseHeader(f)
	if err != nil {
		t.Fatal(err)
	}
	var tags []*flv.Tag
	for {
		tag, err := flv.ReadTag(f)
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		tags = append(tags, tag)
	}
	return header, tags
}

func assertTagsEqual(t *testing.T, got, want []*flv.Tag) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("tag count = %d, want %d (got %+v)", len(got), len(want), got)
	}
	for i := range want {
		if got[i].Type != want[i].Type || got[i].Timestamp != want[i].Timestamp || !bytes.Equal(got[i].Data, want[i].Data) {
			t.Fatalf("tag %d = %+v, want %+v", i, got[i], want[i])
		}
	}
}

// countMatchingTags 统计 got 中与 want 相等（类型、时间戳、数据）的
// tag 数量。
func countMatchingTags(got []*flv.Tag, want *flv.Tag) int {
	n := 0
	for _, tag := range got {
		if tag.Type == want.Type && tag.Timestamp == want.Timestamp && bytes.Equal(tag.Data, want.Data) {
			n++
		}
	}
	return n
}

// seedMergeSession 准备一个会话目录：meta.json + 每个 part 一个分段
// （FLV 内容由参数给定；nil 表示磁盘上不落该文件，仅登记进 meta），
// 返回会话布局与 meta.json 路径。
func seedMergeSession(t *testing.T, repo *recorderRepo, parts ...[]byte) (sessionLayout, string) {
	t.Helper()
	ctx := context.Background()
	session := testSession()
	if err := repo.PrepareSession(ctx, session); err != nil {
		t.Fatal(err)
	}
	lay, err := sessionPaths(repo.recordRoot, session)
	if err != nil {
		t.Fatal(err)
	}
	metaPath := lay.metaPath()
	for i, content := range parts {
		part := i + 1
		videoPath := lay.segmentVideoPath(part)
		danmuPath := lay.segmentDanmakuPath(part)
		if content != nil {
			if err := os.WriteFile(videoPath, content, 0o644); err != nil {
				t.Fatal(err)
			}
		}
		if err := os.WriteFile(danmuPath, nil, 0o644); err != nil {
			t.Fatal(err)
		}
		appendSegmentMeta(metaPath, &segmentWriter{
			part:        part,
			videoPath:   videoPath,
			danmakuPath: danmuPath,
			wallStart:   session.LiveStartTime,
		})
	}
	return lay, metaPath
}
