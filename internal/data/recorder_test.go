package data

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"suika/internal/biz"
	"suika/internal/conf"
	"suika/internal/data/flv"
)

// --- 辅助函数 ---

// newTestRepo 构建以全新临时目录为 record_root 的 recorderRepo。
func newTestRepo(t *testing.T, d *Data, c *conf.Recorder) *recorderRepo {
	t.Helper()
	if d == nil {
		d = &Data{}
	}
	if c == nil {
		c = &conf.Recorder{}
	}
	c.RecordRoot = t.TempDir()
	repo, ok := NewRecorderRepo(d, c).(*recorderRepo)
	if !ok {
		t.Fatal("NewRecorderRepo did not return *recorderRepo")
	}
	return repo
}

func testSession() *biz.RecordingSession {
	return &biz.RecordingSession{
		RoomID:        42,
		RoomName:      "tester",
		Title:         "stream title",
		LiveStartTime: time.Date(2026, 8, 11, 20, 0, 0, 0, time.UTC),
		Quality:       biz.StreamQuality{Qn: 10000, Desc: "source"},
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
	var buf bytes.Buffer
	buf.Write((&flv.FileHeader{Version: 1, HasAudio: true, HasVideo: true}).Bytes())
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

// --- meta.json ---

func TestMetaJSONRoundTrip(t *testing.T) {
	path := filepath.Join(t.TempDir(), "sess.meta.json")
	in := &sessionMeta{
		RoomID:        42,
		RoomName:      "房间名",
		Title:         "直播标题",
		LiveStartTime: 1_700_000_000,
		EndTime:       1_700_003_600,
		Quality:       qualityMeta{Qn: 10000, Desc: "原画"},
		Status:        metaStatusDone,
		Segments: []segmentMeta{
			{
				Part: 1, Video: "base_part1.flv", Danmaku: "base_part1.danmu.jsonl",
				WallStart: 1_700_000_000, WallEnd: 1_700_003_600,
				TsStart: 0, TsEnd: 7_200_000, Bytes: 123456,
			},
			{
				Part: 2, Video: "base_part2.flv", FLVKept: true, Danmaku: "base_part2.danmu.jsonl",
			},
		},
		MergedVideo:   "base.flv",
		MergedDanmaku: "base.danmu.jsonl",
		Errors:        []errorMeta{{Time: 55, Stage: "record", Msg: "write failed"}},
	}

	before := time.Now().Unix()
	if err := saveMeta(path, in); err != nil {
		t.Fatalf("saveMeta: %v", err)
	}
	// saveMeta 会写入 UpdatedAt，并通过临时文件原子落盘。
	if in.UpdatedAt < before {
		t.Errorf("UpdatedAt = %d, want >= %d (saveMeta must stamp it)", in.UpdatedAt, before)
	}
	if _, err := os.Stat(path + ".tmp"); !errors.Is(err, fs.ErrNotExist) {
		t.Errorf("temp file left behind after saveMeta (stat err = %v)", err)
	}

	got, err := loadMeta(path)
	if err != nil {
		t.Fatalf("loadMeta: %v", err)
	}
	in.UpdatedAt, got.UpdatedAt = 0, 0 // 上面已校验过；这里单独比较
	if !reflect.DeepEqual(got, in) {
		t.Fatalf("roundtrip mismatch:\n got %+v\nwant %+v", got, in)
	}
}

func TestLoadMetaMissingFile(t *testing.T) {
	_, err := loadMeta(filepath.Join(t.TempDir(), "nope.meta.json"))
	if err == nil {
		t.Fatal("want error for missing meta file")
	}
	if !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("err = %v, want fs.ErrNotExist", err)
	}
}

func TestLoadMetaCorruptJSON(t *testing.T) {
	path := filepath.Join(t.TempDir(), "bad.meta.json")
	if err := os.WriteFile(path, []byte("{not json"), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := loadMeta(path); err == nil {
		t.Fatal("want error for corrupt meta file")
	}
}

// --- sanitizeSegment ---

func TestSanitizeSegment(t *testing.T) {
	cases := []struct {
		name string
		in   string
		max  int
		want string
	}{
		{"slashes", `a/b\c`, 64, "a_b_c"},
		{"all unsafe chars collapse", `\/:*?"<>|`, 64, "untitled"},
		{"whitespace runs collapse", "a  b\tc", 64, "a_b_c"},
		{"chinese survives", "主播的 直播间", 64, "主播的_直播间"},
		{"control chars", "a\x01b\x7fc", 64, "a_b_c"},
		{"pure dots survive", "...", 64, "..."},
		{"empty falls back", "", 64, "untitled"},
		{"only whitespace falls back", "   ", 64, "untitled"},
		{"edge underscores trimmed", "_a b_", 64, "a_b"},
		{"truncated to max runes", strings.Repeat("a", 70), 64, strings.Repeat("a", 64)},
		{"truncation trims trailing underscore", strings.Repeat("a", 63) + " b", 64, strings.Repeat("a", 63)},
		{"name length limit", strings.Repeat("字", 40), 32, strings.Repeat("字", 32)},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := sanitizeSegment(tc.in, tc.max)
			if got != tc.want {
				t.Fatalf("sanitizeSegment(%q, %d) = %q, want %q", tc.in, tc.max, got, tc.want)
			}
			if strings.ContainsAny(got, `/\`) {
				t.Errorf("result %q contains a path separator", got)
			}
			if again := sanitizeSegment(got, tc.max); again != got {
				t.Errorf("result not stable: second pass = %q", again)
			}
		})
	}
}

// --- nextPartNumber ---

func TestNextPartNumber(t *testing.T) {
	const base = "20260811_2000_title"
	cases := []struct {
		name  string
		files []string
		dirOK bool // false => 指向不存在的目录
		want  int
	}{
		{"empty directory", nil, true, 1},
		{"missing directory", nil, false, 1},
		{
			name:  "two existing parts",
			files: []string{base + "_part1.flv", base + "_part2.flv"},
			dirOK: true,
			want:  3,
		},
		{
			name: "unrelated files ignored, padded and mp4 counted",
			files: []string{
				base + "_part001.flv",       // 前导零编号：Atoi 能处理
				base + "_part002.mp4",       // 历史遗留的 mp4 分段也计入
				"other_part99.flv",          // 基座前缀不同
				base + "_partX.flv",         // 编号非数字
				base + "_part3.danmu.jsonl", // 扩展符不符
				"random.txt",
			},
			dirOK: true,
			want:  3, // max(1, 2) + 1
		},
		{
			name:  "high part number",
			files: []string{base + "_part999.flv"},
			dirOK: true,
			want:  1000,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			if tc.dirOK {
				touchFiles(t, dir, tc.files...)
			} else {
				dir = filepath.Join(dir, "does-not-exist")
			}
			if got := nextPartNumber(dir, base); got != tc.want {
				t.Fatalf("nextPartNumber = %d, want %d", got, tc.want)
			}
		})
	}
}

// --- shouldSplit ---

func TestShouldSplit(t *testing.T) {
	key := []byte{0x17, 0x01, 0, 0, 0}   // 关键帧 NALU
	inter := []byte{0x27, 0x01, 0, 0, 0} // 帧间 NALU
	audio := []byte{0xAF, 0x01, 0x09}
	script := []byte{0x02, 0x00, 0x0a, 'o', 'n', 'M', 'e', 't', 'a', 'D', 'a', 't', 'a'}

	const ms = int64(1) // 时间戳单位
	minute := int64(time.Minute / time.Millisecond)
	overrun := int64(splitOverrun / time.Millisecond)

	video := func(ts int64, data []byte) *flv.Tag {
		return &flv.Tag{Type: flv.TagVideo, Timestamp: ts, Data: data}
	}
	cases := []struct {
		name     string
		dur      time.Duration
		hasStart bool
		startTs  int64
		tag      *flv.Tag
		want     bool
	}{
		{"splitting disabled", 0, true, 0, video(minute, key), false},
		{"segment has no start yet", time.Minute, false, 0, video(minute*2, key), false},
		{"under duration keyframe", time.Minute, true, 1000 * ms, video(60_999*ms, key), false},
		{"at duration keyframe splits", time.Minute, true, 1000 * ms, video(61_000*ms, key), true},
		{"past duration inter within overrun waits", time.Minute, true, 0, video((minute+overrun-1)*ms, inter), false},
		{"overrun exhausted forces split", time.Minute, true, 0, video((minute+overrun)*ms, inter), true},
		{"overrun forces on audio too", time.Minute, true, 0,
			&flv.Tag{Type: flv.TagAudio, Timestamp: (minute + overrun + 1) * ms, Data: audio}, true},
		{"overrun forces on metadata too", time.Minute, true, 0,
			&flv.Tag{Type: flv.TagScript, Timestamp: (minute + overrun) * ms, Data: script}, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			r := &recorderRepo{segmentDuration: tc.dur}
			seg := &segmentFile{hasStart: tc.hasStart, startTs: tc.startTs}
			if got := r.shouldSplit(seg, tc.tag); got != tc.want {
				t.Fatalf("shouldSplit = %v, want %v", got, tc.want)
			}
		})
	}
}

func TestShouldSplitBySize(t *testing.T) {
	key := []byte{0x17, 0x01, 0, 0, 0}   // 关键帧 NALU
	inter := []byte{0x27, 0x01, 0, 0, 0} // 帧间 NALU

	const limit = int64(1000)
	overrun := limit / sizeSplitOverrunDivisor

	video := func(data []byte) *flv.Tag {
		return &flv.Tag{Type: flv.TagVideo, Timestamp: 0, Data: data}
	}
	cases := []struct {
		name     string
		maxBytes int64
		bytes    int64
		hasStart bool
		tag      *flv.Tag
		want     bool
	}{
		{"size splitting disabled", 0, limit * 10, true, video(key), false},
		{"segment has no start yet", limit, limit, false, video(key), false},
		{"under limit keyframe", limit, limit - 1, true, video(key), false},
		{"at limit keyframe splits", limit, limit, true, video(key), true},
		{"past limit inter within overrun waits", limit, limit + overrun - 1, true, video(inter), false},
		{"overrun exhausted forces split on inter", limit, limit + overrun, true, video(inter), true},
		{"past limit keyframe splits even mid-overrun", limit, limit + overrun/2, true, video(key), true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			// segmentDuration 置 0：只验证大小触发一路。
			r := &recorderRepo{maxSegmentBytes: tc.maxBytes}
			seg := &segmentFile{hasStart: tc.hasStart, bytes: tc.bytes}
			if got := r.shouldSplit(seg, tc.tag); got != tc.want {
				t.Fatalf("shouldSplit = %v, want %v", got, tc.want)
			}
		})
	}
}

// --- 构造函数 / 路径 ---

func TestNewRecorderRepoConfigMapping(t *testing.T) {
	r := NewRecorderRepo(&Data{}, nil).(*recorderRepo)
	if r.recordRoot != defaultRecordRoot ||
		r.segmentDuration != defaultSegmentMinutes*time.Minute ||
		r.maxSegmentBytes != defaultMaxSegmentBytes ||
		r.healthInterval != defaultHealthInterval ||
		r.healthFailRounds != defaultHealthRounds {
		t.Fatalf("defaults not applied: %+v", r)
	}

	r = NewRecorderRepo(&Data{}, &conf.Recorder{RecordRoot: "/srv/recordings"}).(*recorderRepo)
	if r.recordRoot != "/srv/recordings" {
		t.Fatalf("recordRoot = %q, want %q", r.recordRoot, "/srv/recordings")
	}
}

func TestSessionPaths(t *testing.T) {
	repo := newTestRepo(t, nil, nil)
	dir, base, err := repo.sessionPaths(testSession())
	if err != nil {
		t.Fatal(err)
	}
	wantDir := filepath.Join(repo.recordRoot, "42_tester", "2026-08-11")
	if dir != wantDir {
		t.Fatalf("dir = %q, want %q", dir, wantDir)
	}
	if want := "20260811_2000_stream_title"; base != want {
		t.Fatalf("base = %q, want %q", base, want)
	}

	if _, _, err := repo.sessionPaths(nil); !errors.Is(err, biz.ErrRoomInternal) {
		t.Fatalf("nil session err = %v, want ErrRoomInternal", err)
	}
	if _, _, err := repo.sessionPaths(&biz.RecordingSession{RoomID: 0}); !errors.Is(err, biz.ErrRoomInternal) {
		t.Fatalf("zero room err = %v, want ErrRoomInternal", err)
	}
}

func TestPrepareSessionResumeKeepsSegments(t *testing.T) {
	repo := newTestRepo(t, nil, nil)
	ctx := context.Background()
	session := testSession()

	if err := repo.PrepareSession(ctx, session); err != nil {
		t.Fatalf("PrepareSession: %v", err)
	}
	dir, base, err := repo.sessionPaths(session)
	if err != nil {
		t.Fatal(err)
	}
	metaPath := filepath.Join(dir, base+".meta.json")

	// 模拟崩溃/重启前已录好的一个分段
	repo.appendSegmentMeta(metaPath, &segmentFile{
		part:      1,
		videoPath: filepath.Join(dir, base+"_part1.flv"),
		danmuPath: filepath.Join(dir, base+"_part1.danmu.jsonl"),
		wallStart: session.LiveStartTime,
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
	repo := newTestRepo(t, nil, nil)
	ctx := context.Background()
	session := testSession()
	session.Title = "a/b"
	if err := repo.PrepareSession(ctx, session); err != nil {
		t.Fatal(err)
	}
	dir, base, err := repo.sessionPaths(session)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.HasSuffix(base, "_a_b") {
		t.Fatalf("base = %q, want suffix _a_b", base)
	}
	metaPath := filepath.Join(dir, base+".meta.json")

	restart := *session
	restart.Title = "a b" // 净化后基座相同
	restart.RoomName = session.RoomName
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

func TestPrepareSessionResetsStatsBetweenSessions(t *testing.T) {
	repo := newTestRepo(t, nil, nil)
	ctx := context.Background()

	metaTag := &flv.Tag{Type: flv.TagScript, Timestamp: 0, Data: []byte{0x02, 0x00, 0x0a, 'o', 'n', 'M', 'e', 't', 'a', 'D', 'a', 't', 'a'}}
	videoSeq := &flv.Tag{Type: flv.TagVideo, Timestamp: 0, Data: []byte{0x17, 0x00, 0, 0, 0, 1, 2, 3}}
	audioSeq := &flv.Tag{Type: flv.TagAudio, Timestamp: 0, Data: []byte{0xAF, 0x00, 0x12, 0x10}}
	key0 := &flv.Tag{Type: flv.TagVideo, Timestamp: 0, Data: []byte{0x17, 0x01, 0, 0, 0, 0xAA}}
	inter40 := &flv.Tag{Type: flv.TagVideo, Timestamp: 40, Data: []byte{0x27, 0x01, 0, 0, 0, 0xBB}}
	tags := []*flv.Tag{metaTag, videoSeq, audioSeq, key0, inter40}
	var wantBytes int64
	for _, tag := range tags {
		wantBytes += int64(len(tag.AppendTo(nil)))
	}
	pump := func(session *biz.RecordingSession) {
		t.Helper()
		stream := &biz.LiveStream{
			Quality: biz.StreamQuality{Qn: 10000, Desc: "source"},
			Body:    io.NopCloser(bytes.NewReader(buildFLVStream(t, tags...))),
		}
		if _, err := repo.RecordSession(ctx, session, stream, nil); err != nil {
			t.Fatalf("RecordSession: %v", err)
		}
	}

	first := testSession()
	if err := repo.PrepareSession(ctx, first); err != nil {
		t.Fatal(err)
	}
	pump(first)
	stats, err := repo.SessionStats(ctx, first.RoomID)
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
	stats, err = repo.SessionStats(ctx, second.RoomID)
	if err != nil {
		t.Fatal(err)
	}
	if stats == nil || stats.BytesWritten != 0 || stats.CurrentFile != "" {
		t.Fatalf("stats at second session start = %+v, want zeroed", stats)
	}
	pump(second)
	stats, err = repo.SessionStats(ctx, second.RoomID)
	if err != nil || stats == nil || stats.BytesWritten != wantBytes {
		t.Fatalf("stats after second session = %+v, %v; want %d bytes (no cross-session accumulation)", stats, err, wantBytes)
	}
}

// --- 分段文件 ---

func TestOpenSegmentReinjectsCachedHeaders(t *testing.T) {
	header := &flv.FileHeader{Version: 1, HasAudio: true, HasVideo: true}
	metaTag := &flv.Tag{Type: flv.TagScript, Timestamp: 0, Data: []byte{0x02, 0x00, 0x0a, 'o', 'n', 'M', 'e', 't', 'a', 'D', 'a', 't', 'a'}}
	videoSeq := &flv.Tag{Type: flv.TagVideo, Timestamp: 0, Data: []byte{0x17, 0x00, 0, 0, 0, 1, 2, 3}}
	audioSeq := &flv.Tag{Type: flv.TagAudio, Timestamp: 0, Data: []byte{0xAF, 0x00, 0x12, 0x10}}
	cache := &headerCache{metadata: metaTag, videoSeq: videoSeq, audioSeq: audioSeq}

	seg, err := openSegment(t.TempDir(), "base", 1, header, cache)
	if err != nil {
		t.Fatal(err)
	}
	if err := seg.close(); err != nil {
		t.Fatal(err)
	}

	gotHeader, tags := readSegmentTags(t, seg.videoPath)
	if *gotHeader != *header {
		t.Fatalf("header = %+v, want %+v", gotHeader, header)
	}
	assertTagsEqual(t, tags, []*flv.Tag{metaTag, videoSeq, audioSeq})

	fi, err := os.Stat(seg.videoPath)
	if err != nil {
		t.Fatal(err)
	}
	if seg.bytes != fi.Size() {
		t.Fatalf("seg.bytes = %d, file size = %d", seg.bytes, fi.Size())
	}
	if fi2, err := os.Stat(seg.danmuPath); err != nil || fi2.Size() != 0 {
		t.Fatalf("danmu file = %+v, %v; want empty existing file", fi2, err)
	}
}

func TestSegmentWriteDanmakuEvents(t *testing.T) {
	header := &flv.FileHeader{Version: 1, HasAudio: true, HasVideo: true}
	seg, err := openSegment(t.TempDir(), "base", 1, header, &headerCache{})
	if err != nil {
		t.Fatal(err)
	}
	danmaku := &biz.DanmakuEvent{
		Ts: time.Unix(123, 0), SendTs: 1755633600123, Type: biz.EventDanmaku,
		UID: 7, Uname: "user", Text: "你好", Color: 16777215, Mode: 1,
		Raw: []byte(`{"info":"x"}`),
	}
	gift := &biz.DanmakuEvent{
		Ts: time.Unix(124, 0), Type: biz.EventGift,
		UID: 7, Uname: "user", GiftName: "火箭", Num: 2, Price: 1000, CoinType: "gold",
	}
	if err := seg.writeEvent(danmaku); err != nil {
		t.Fatal(err)
	}
	if err := seg.writeEvent(gift); err != nil {
		t.Fatal(err)
	}
	if err := seg.close(); err != nil {
		t.Fatal(err)
	}

	data, err := os.ReadFile(seg.danmuPath)
	if err != nil {
		t.Fatal(err)
	}
	lines := strings.Split(strings.TrimRight(string(data), "\n"), "\n")
	if len(lines) != 2 {
		t.Fatalf("lines = %d, want 2: %q", len(lines), data)
	}
	var first, second danmuLine
	if err := json.Unmarshal([]byte(lines[0]), &first); err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal([]byte(lines[1]), &second); err != nil {
		t.Fatal(err)
	}
	if first.Ts != 123_000 || first.SendTs != 1755633600123 || first.Type != biz.EventDanmaku ||
		first.UID != 7 || first.Text != "你好" || first.Color != 16777215 || first.Mode != 1 {
		t.Fatalf("danmaku line = %+v", first)
	}
	if string(first.Raw) != `{"info":"x"}` {
		t.Fatalf("raw = %s", first.Raw)
	}
	if second.Type != biz.EventGift || second.GiftName != "火箭" || second.Num != 2 ||
		second.Price != 1000 || second.CoinType != "gold" || second.SendTs != 0 {
		t.Fatalf("gift line = %+v", second)
	}
}

// --- RecordSession ---

func TestRecordSessionRejectsNilStream(t *testing.T) {
	repo := newTestRepo(t, nil, nil)
	if _, err := repo.RecordSession(context.Background(), testSession(), nil, nil); !errors.Is(err, biz.ErrRoomInternal) {
		t.Fatalf("err = %v, want ErrRoomInternal", err)
	}
	if _, err := repo.RecordSession(context.Background(), testSession(), &biz.LiveStream{}, nil); !errors.Is(err, biz.ErrRoomInternal) {
		t.Fatalf("err = %v, want ErrRoomInternal", err)
	}
}

func TestRecordSessionSingleSegment(t *testing.T) {
	repo := newTestRepo(t, nil, nil)
	ctx := context.Background()
	session := testSession()
	if err := repo.PrepareSession(ctx, session); err != nil {
		t.Fatal(err)
	}

	metaTag := &flv.Tag{Type: flv.TagScript, Timestamp: 0, Data: []byte{0x02, 0x00, 0x0a, 'o', 'n', 'M', 'e', 't', 'a', 'D', 'a', 't', 'a'}}
	videoSeq := &flv.Tag{Type: flv.TagVideo, Timestamp: 0, Data: []byte{0x17, 0x00, 0, 0, 0, 1, 2, 3}}
	audioSeq := &flv.Tag{Type: flv.TagAudio, Timestamp: 0, Data: []byte{0xAF, 0x00, 0x12, 0x10}}
	key0 := &flv.Tag{Type: flv.TagVideo, Timestamp: 0, Data: []byte{0x17, 0x01, 0, 0, 0, 0xAA}}
	inter40 := &flv.Tag{Type: flv.TagVideo, Timestamp: 40, Data: []byte{0x27, 0x01, 0, 0, 0, 0xBB}}
	tags := []*flv.Tag{metaTag, videoSeq, audioSeq, key0, inter40}

	var wantBytes int64
	for _, tag := range tags {
		wantBytes += int64(len(tag.AppendTo(nil)))
	}

	stream := &biz.LiveStream{
		Quality: biz.StreamQuality{Qn: 10000, Desc: "source"},
		Body:    io.NopCloser(bytes.NewReader(buildFLVStream(t, tags...))),
	}
	// events 传 nil：永远不就绪，不会有弹幕事件插入。
	result, err := repo.RecordSession(ctx, session, stream, nil)
	if err != nil {
		t.Fatalf("RecordSession: %v", err)
	}
	if result.Parts != 1 || result.BytesWritten != wantBytes {
		t.Fatalf("result = %+v, want 1 part / %d bytes", result, wantBytes)
	}

	dir, base, err := repo.sessionPaths(session)
	if err != nil {
		t.Fatal(err)
	}
	// part1 在第一个（metadata）tag 处以空缓存开启，流内容原样写入：
	// 任何头标签都不会出现两次。
	videoPath := filepath.Join(dir, base+"_part1.flv")
	header, gotTags := readSegmentTags(t, videoPath)
	if !header.HasAudio || !header.HasVideo {
		t.Fatalf("header flags = %+v", header)
	}
	assertTagsEqual(t, gotTags, []*flv.Tag{metaTag, videoSeq, audioSeq, key0, inter40})

	metaPath := filepath.Join(dir, base+".meta.json")
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
	if seg.Part != 1 || seg.Video != base+"_part1.flv" || seg.Danmaku != base+"_part1.danmu.jsonl" {
		t.Fatalf("segment = %+v", seg)
	}
	if seg.TsStart != 0 || seg.TsEnd != 40 {
		t.Fatalf("segment bookkeeping = %+v", seg)
	}
	if fi, err := os.Stat(videoPath); err != nil || seg.Bytes != fi.Size() {
		t.Fatalf("segment bytes = %d, file size/+err = %d/%v", seg.Bytes, fi.Size(), err)
	}

	stats, err := repo.SessionStats(ctx, session.RoomID)
	if err != nil || stats == nil {
		t.Fatalf("SessionStats = %+v, %v", stats, err)
	}
	if stats.BytesWritten != wantBytes || stats.CurrentFile != videoPath {
		t.Fatalf("stats = %+v, want %d bytes at %s", stats, wantBytes, videoPath)
	}
}

func TestRecordSessionConcurrentPumpsAllocateDistinctSegments(t *testing.T) {
	repo := newTestRepo(t, nil, nil)
	ctx := context.Background()
	session := testSession()
	if err := repo.PrepareSession(ctx, session); err != nil {
		t.Fatal(err)
	}

	metaTag, videoSeq, audioSeq := mergeTestTags()
	streamBytes := buildFLVStream(t, metaTag, videoSeq, audioSeq)
	start := make(chan struct{})
	errs := make(chan error, 2)
	for range 2 {
		go func() {
			<-start
			stream := &biz.LiveStream{
				Quality: biz.StreamQuality{Qn: 10000, Desc: "source"},
				Body:    io.NopCloser(bytes.NewReader(streamBytes)),
			}
			_, err := repo.RecordSession(ctx, session, stream, nil)
			errs <- err
		}()
	}
	close(start)
	for range 2 {
		if err := <-errs; err != nil {
			t.Fatalf("RecordSession: %v", err)
		}
	}

	dir, base, err := repo.sessionPaths(session)
	if err != nil {
		t.Fatal(err)
	}
	meta, err := loadMeta(filepath.Join(dir, base+".meta.json"))
	if err != nil {
		t.Fatal(err)
	}
	if len(meta.Segments) != 2 {
		t.Fatalf("segments = %+v, want two distinct segments", meta.Segments)
	}
	if meta.Segments[0].Part == meta.Segments[1].Part {
		t.Fatalf("segments reused part number: %+v", meta.Segments)
	}
	for _, seg := range meta.Segments {
		if _, err := os.Stat(filepath.Join(dir, seg.Video)); err != nil {
			t.Fatalf("segment %q missing: %v", seg.Video, err)
		}
	}
}

func TestRecordSessionSplitsAtKeyframe(t *testing.T) {
	repo := newTestRepo(t, nil, nil)
	repo.segmentDuration = 50 * time.Millisecond // 测试中使用亚分钟粒度
	ctx := context.Background()
	session := testSession()
	if err := repo.PrepareSession(ctx, session); err != nil {
		t.Fatal(err)
	}

	metaTag := &flv.Tag{Type: flv.TagScript, Timestamp: 0, Data: []byte{0x02, 0x00, 0x0a, 'o', 'n', 'M', 'e', 't', 'a', 'D', 'a', 't', 'a'}}
	videoSeq := &flv.Tag{Type: flv.TagVideo, Timestamp: 0, Data: []byte{0x17, 0x00, 0, 0, 0, 1, 2, 3}}
	audioSeq := &flv.Tag{Type: flv.TagAudio, Timestamp: 0, Data: []byte{0xAF, 0x00, 0x12, 0x10}}
	key0 := &flv.Tag{Type: flv.TagVideo, Timestamp: 0, Data: []byte{0x17, 0x01, 0, 0, 0, 0xAA}}
	inter40 := &flv.Tag{Type: flv.TagVideo, Timestamp: 40, Data: []byte{0x27, 0x01, 0, 0, 0, 0xBB}}
	key100 := &flv.Tag{Type: flv.TagVideo, Timestamp: 100, Data: []byte{0x17, 0x01, 0, 0, 0, 0xCC}}
	inter120 := &flv.Tag{Type: flv.TagVideo, Timestamp: 120, Data: []byte{0x27, 0x01, 0, 0, 0, 0xDD}}
	tags := []*flv.Tag{metaTag, videoSeq, audioSeq, key0, inter40, key100, inter120}

	var wantBytes int64
	for _, tag := range tags {
		wantBytes += int64(len(tag.AppendTo(nil)))
	}

	stream := &biz.LiveStream{
		Quality: biz.StreamQuality{Qn: 10000, Desc: "source"},
		Body:    io.NopCloser(bytes.NewReader(buildFLVStream(t, tags...))),
	}
	result, err := repo.RecordSession(ctx, session, stream, nil)
	if err != nil {
		t.Fatalf("RecordSession: %v", err)
	}
	if result.Parts != 2 || result.BytesWritten != wantBytes {
		t.Fatalf("result = %+v, want 2 parts / %d bytes", result, wantBytes)
	}

	dir, base, err := repo.sessionPaths(session)
	if err != nil {
		t.Fatal(err)
	}
	_, part1 := readSegmentTags(t, filepath.Join(dir, base+"_part1.flv"))
	assertTagsEqual(t, part1, []*flv.Tag{metaTag, videoSeq, audioSeq, key0, inter40})

	// part2 以完整重注入的头缓存开始，随后是触发切分的关键帧。
	_, part2 := readSegmentTags(t, filepath.Join(dir, base+"_part2.flv"))
	assertTagsEqual(t, part2, []*flv.Tag{metaTag, videoSeq, audioSeq, key100, inter120})

	meta, err := loadMeta(filepath.Join(dir, base+".meta.json"))
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
// part1 前先缓存首个头标签，openSegment 重注入（此时已非空的）缓存，
// 拉流写入又把同一个标签写了一遍——part1 里的 onMetaData 因此重复。
func TestRecordSessionSingleSegmentHeadersWrittenOnce(t *testing.T) {
	repo := newTestRepo(t, nil, nil)
	ctx := context.Background()
	session := testSession()
	if err := repo.PrepareSession(ctx, session); err != nil {
		t.Fatal(err)
	}

	metaTag := &flv.Tag{Type: flv.TagScript, Timestamp: 0, Data: []byte{0x02, 0x00, 0x0a, 'o', 'n', 'M', 'e', 't', 'a', 'D', 'a', 't', 'a'}}
	videoSeq := &flv.Tag{Type: flv.TagVideo, Timestamp: 0, Data: []byte{0x17, 0x00, 0, 0, 0, 1, 2, 3}}
	audioSeq := &flv.Tag{Type: flv.TagAudio, Timestamp: 0, Data: []byte{0xAF, 0x00, 0x12, 0x10}}
	key0 := &flv.Tag{Type: flv.TagVideo, Timestamp: 0, Data: []byte{0x17, 0x01, 0, 0, 0, 0xAA}}
	audio20 := &flv.Tag{Type: flv.TagAudio, Timestamp: 20, Data: []byte{0xAF, 0x01, 0x09}}
	inter40 := &flv.Tag{Type: flv.TagVideo, Timestamp: 40, Data: []byte{0x27, 0x01, 0, 0, 0, 0xBB}}
	tags := []*flv.Tag{metaTag, videoSeq, audioSeq, key0, audio20, inter40}

	stream := &biz.LiveStream{
		Quality: biz.StreamQuality{Qn: 10000, Desc: "source"},
		Body:    io.NopCloser(bytes.NewReader(buildFLVStream(t, tags...))),
	}
	result, err := repo.RecordSession(ctx, session, stream, nil)
	if err != nil {
		t.Fatalf("RecordSession: %v", err)
	}
	if result.Parts != 1 {
		t.Fatalf("parts = %d, want 1", result.Parts)
	}

	dir, base, err := repo.sessionPaths(session)
	if err != nil {
		t.Fatal(err)
	}
	_, got := readSegmentTags(t, filepath.Join(dir, base+"_part1.flv"))
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
func TestRecordSessionSplitHeadersWrittenOnce(t *testing.T) {
	repo := newTestRepo(t, nil, nil)
	repo.segmentDuration = 50 * time.Millisecond // 测试中使用亚分钟粒度
	ctx := context.Background()
	session := testSession()
	if err := repo.PrepareSession(ctx, session); err != nil {
		t.Fatal(err)
	}

	metaTag := &flv.Tag{Type: flv.TagScript, Timestamp: 0, Data: []byte{0x02, 0x00, 0x0a, 'o', 'n', 'M', 'e', 't', 'a', 'D', 'a', 't', 'a'}}
	videoSeq := &flv.Tag{Type: flv.TagVideo, Timestamp: 0, Data: []byte{0x17, 0x00, 0, 0, 0, 1, 2, 3}}
	audioSeq := &flv.Tag{Type: flv.TagAudio, Timestamp: 0, Data: []byte{0xAF, 0x00, 0x12, 0x10}}
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
	result, err := repo.RecordSession(ctx, session, stream, nil)
	if err != nil {
		t.Fatalf("RecordSession: %v", err)
	}
	if result.Parts != 2 {
		t.Fatalf("parts = %d, want 2", result.Parts)
	}

	dir, base, err := repo.sessionPaths(session)
	if err != nil {
		t.Fatal(err)
	}
	_, part1 := readSegmentTags(t, filepath.Join(dir, base+"_part1.flv"))
	assertTagsEqual(t, part1, []*flv.Tag{metaTag, videoSeq, audioSeq, key0, inter40})

	_, part2 := readSegmentTags(t, filepath.Join(dir, base+"_part2.flv"))
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

// TestRecordSessionSplitsOnSeqHeaderChange 验证流中途序列头变化（CDN 换
// 源、主播改码率）触发强制切段：视频与音频序列头各变化一次，产生三段；
// 每段从缓存注入当时的旧头标签，新序列头作为首个正文标签写入。
func TestRecordSessionSplitsOnSeqHeaderChange(t *testing.T) {
	repo := newTestRepo(t, nil, nil)
	ctx := context.Background()
	session := testSession()
	if err := repo.PrepareSession(ctx, session); err != nil {
		t.Fatal(err)
	}

	metaTag := &flv.Tag{Type: flv.TagScript, Timestamp: 0, Data: []byte{0x02, 0x00, 0x0a, 'o', 'n', 'M', 'e', 't', 'a', 'D', 'a', 't', 'a'}}
	videoSeqA := &flv.Tag{Type: flv.TagVideo, Timestamp: 0, Data: []byte{0x17, 0x00, 0, 0, 0, 1, 2, 3}}
	audioSeqA := &flv.Tag{Type: flv.TagAudio, Timestamp: 0, Data: []byte{0xAF, 0x00, 0x12, 0x10}}
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
	result, err := repo.RecordSession(ctx, session, stream, nil)
	if err != nil {
		t.Fatalf("RecordSession: %v", err)
	}
	if result.Parts != 3 {
		t.Fatalf("parts = %d, want 3 (split on video and audio seq header changes)", result.Parts)
	}

	dir, base, err := repo.sessionPaths(session)
	if err != nil {
		t.Fatal(err)
	}
	_, part1 := readSegmentTags(t, filepath.Join(dir, base+"_part1.flv"))
	assertTagsEqual(t, part1, []*flv.Tag{metaTag, videoSeqA, audioSeqA, key0, inter40})

	// part2 由视频序列头变化触发：注入变化前的缓存头，新序列头为首个正文标签。
	_, part2 := readSegmentTags(t, filepath.Join(dir, base+"_part2.flv"))
	assertTagsEqual(t, part2, []*flv.Tag{metaTag, videoSeqA, audioSeqA, videoSeqB, key60, inter80})

	// part3 由音频序列头变化触发：此时缓存的视频序列头已是 B。
	_, part3 := readSegmentTags(t, filepath.Join(dir, base+"_part3.flv"))
	assertTagsEqual(t, part3, []*flv.Tag{metaTag, videoSeqB, audioSeqA, audioSeqB, key100})
}

// TestRecordSessionRepeatedSeqHeaderDoesNotSplit 验证重复出现的相同序列头
// （字节一致）不触发切段：只有解码配置真正变化才值得切。
func TestRecordSessionRepeatedSeqHeaderDoesNotSplit(t *testing.T) {
	repo := newTestRepo(t, nil, nil)
	ctx := context.Background()
	session := testSession()
	if err := repo.PrepareSession(ctx, session); err != nil {
		t.Fatal(err)
	}

	metaTag := &flv.Tag{Type: flv.TagScript, Timestamp: 0, Data: []byte{0x02, 0x00, 0x0a, 'o', 'n', 'M', 'e', 't', 'a', 'D', 'a', 't', 'a'}}
	videoSeq := &flv.Tag{Type: flv.TagVideo, Timestamp: 0, Data: []byte{0x17, 0x00, 0, 0, 0, 1, 2, 3}}
	audioSeq := &flv.Tag{Type: flv.TagAudio, Timestamp: 0, Data: []byte{0xAF, 0x00, 0x12, 0x10}}
	key0 := &flv.Tag{Type: flv.TagVideo, Timestamp: 0, Data: []byte{0x17, 0x01, 0, 0, 0, 0xAA}}
	videoSeqRepeat := &flv.Tag{Type: flv.TagVideo, Timestamp: 20, Data: videoSeq.Data}
	audioSeqRepeat := &flv.Tag{Type: flv.TagAudio, Timestamp: 30, Data: audioSeq.Data}
	key40 := &flv.Tag{Type: flv.TagVideo, Timestamp: 40, Data: []byte{0x17, 0x01, 0, 0, 0, 0xBB}}
	tags := []*flv.Tag{metaTag, videoSeq, audioSeq, key0, videoSeqRepeat, audioSeqRepeat, key40}

	stream := &biz.LiveStream{
		Quality: biz.StreamQuality{Qn: 10000, Desc: "source"},
		Body:    io.NopCloser(bytes.NewReader(buildFLVStream(t, tags...))),
	}
	result, err := repo.RecordSession(ctx, session, stream, nil)
	if err != nil {
		t.Fatalf("RecordSession: %v", err)
	}
	if result.Parts != 1 {
		t.Fatalf("parts = %d, want 1 (identical seq headers must not split)", result.Parts)
	}

	dir, base, err := repo.sessionPaths(session)
	if err != nil {
		t.Fatal(err)
	}
	_, part1 := readSegmentTags(t, filepath.Join(dir, base+"_part1.flv"))
	assertTagsEqual(t, part1, tags)
}

// TestRecordSessionSplitsAtSizeLimit 验证大小切分的端到端行为：阈值设为
// 250 字节，正文 tag 每个 20 字节、每 5 个一个关键帧。每个分段写到约
// 282 字节（82 字节的头 + 10 个正文 tag）后越过阈值，在下一个关键帧处
// 切分，共产出 4 段；每段（含最后一段）都 ≥ 阈值，且切分点都是关键帧。
func TestRecordSessionSplitsAtSizeLimit(t *testing.T) {
	repo := newTestRepo(t, nil, nil)
	repo.maxSegmentBytes = 250 // 测试中使用小阈值
	ctx := context.Background()
	session := testSession()
	if err := repo.PrepareSession(ctx, session); err != nil {
		t.Fatal(err)
	}

	metaTag := &flv.Tag{Type: flv.TagScript, Timestamp: 0, Data: []byte{0x02, 0x00, 0x0a, 'o', 'n', 'M', 'e', 't', 'a', 'D', 'a', 't', 'a'}}
	videoSeq := &flv.Tag{Type: flv.TagVideo, Timestamp: 0, Data: []byte{0x17, 0x00, 0, 0, 0, 1, 2, 3}}
	audioSeq := &flv.Tag{Type: flv.TagAudio, Timestamp: 0, Data: []byte{0xAF, 0x00, 0x12, 0x10}}
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
	result, err := repo.RecordSession(ctx, session, stream, nil)
	if err != nil {
		t.Fatalf("RecordSession: %v", err)
	}
	if result.Parts != 4 {
		t.Fatalf("parts = %d, want 4", result.Parts)
	}

	dir, base, err := repo.sessionPaths(session)
	if err != nil {
		t.Fatal(err)
	}
	meta, err := loadMeta(filepath.Join(dir, base+".meta.json"))
	if err != nil {
		t.Fatal(err)
	}
	for i, seg := range meta.Segments {
		if seg.Bytes < 250 {
			t.Fatalf("segment %d bytes = %d, want >= 250", seg.Part, seg.Bytes)
		}
		// part2 起：注入的三个头标签之后首个正文标签必须是关键帧（切分点）。
		if i == 0 {
			continue
		}
		_, segTags := readSegmentTags(t, filepath.Join(dir, seg.Video))
		if len(segTags) < 4 || !segTags[3].IsVideoKeyframe() {
			t.Fatalf("segment %d first body tag after injected headers is not a keyframe", seg.Part)
		}
	}
}

// --- FinishSession / 收尾合并 ---

// seedMergeSession 准备一个会话目录：meta.json + 每个 part 一个分段
// （FLV 内容由参数给定；nil 表示磁盘上不落该文件，仅登记进 meta），
// 返回断言所需的路径。
func seedMergeSession(t *testing.T, repo *recorderRepo, parts ...[]byte) (dir, base, metaPath string) {
	t.Helper()
	ctx := context.Background()
	session := testSession()
	if err := repo.PrepareSession(ctx, session); err != nil {
		t.Fatal(err)
	}
	var err error
	dir, base, err = repo.sessionPaths(session)
	if err != nil {
		t.Fatal(err)
	}
	metaPath = filepath.Join(dir, base+".meta.json")
	for i, content := range parts {
		part := i + 1
		videoPath := filepath.Join(dir, fmt.Sprintf("%s_part%d.flv", base, part))
		danmuPath := filepath.Join(dir, fmt.Sprintf("%s_part%d.danmu.jsonl", base, part))
		if content != nil {
			if err := os.WriteFile(videoPath, content, 0o644); err != nil {
				t.Fatal(err)
			}
		}
		if err := os.WriteFile(danmuPath, nil, 0o644); err != nil {
			t.Fatal(err)
		}
		repo.appendSegmentMeta(metaPath, &segmentFile{
			part:      part,
			videoPath: videoPath,
			danmuPath: danmuPath,
			wallStart: session.LiveStartTime,
		})
	}
	return dir, base, metaPath
}

// mergeTestTags 返回合并测试共用的头标签。
func mergeTestTags() (metaTag, videoSeq, audioSeq *flv.Tag) {
	metaTag = &flv.Tag{Type: flv.TagScript, Timestamp: 0, Data: []byte{0x02, 0x00, 0x0a, 'o', 'n', 'M', 'e', 't', 'a', 'D', 'a', 't', 'a'}}
	videoSeq = &flv.Tag{Type: flv.TagVideo, Timestamp: 0, Data: []byte{0x17, 0x00, 0, 0, 0, 1, 2, 3}}
	audioSeq = &flv.Tag{Type: flv.TagAudio, Timestamp: 0, Data: []byte{0xAF, 0x00, 0x12, 0x10}}
	return
}

func TestFinishSessionWithoutMetaIsNoop(t *testing.T) {
	repo := newTestRepo(t, nil, nil)
	if err := repo.FinishSession(context.Background(), testSession()); err != nil {
		t.Fatalf("want nil for missing meta.json, got %v", err)
	}
}

func TestFinishSessionMergeDisabledKeepsSegments(t *testing.T) {
	metaTag, videoSeq, audioSeq := mergeTestTags()
	key0 := &flv.Tag{Type: flv.TagVideo, Timestamp: 0, Data: []byte{0x17, 0x01, 0, 0, 0, 0xAA}}
	content := buildFLVStream(t, metaTag, videoSeq, audioSeq, key0)
	repo := newTestRepo(t, &Data{mergeEnabled: false}, nil)
	dir, base, metaPath := seedMergeSession(t, repo, content)

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
	if meta.MergedVideo != "" || meta.MergedDanmaku != "" {
		t.Fatalf("merge bookkeeping = %+v, want empty when merge disabled", meta)
	}
	if meta.EndTime == 0 || meta.Quality.Qn != 10000 || meta.Quality.Desc != "source" {
		t.Fatalf("finish bookkeeping = %+v", meta)
	}
	if seg := meta.Segments[0]; !seg.FLVKept {
		t.Fatalf("segment = %+v, want flv kept", seg)
	}
	if _, err := os.Stat(filepath.Join(dir, base+".flv")); !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("no merged file must be produced when merge is disabled (stat err = %v)", err)
	}
	if got, err := os.ReadFile(filepath.Join(dir, base+"_part1.flv")); err != nil || !bytes.Equal(got, content) {
		t.Fatalf("source flv modified: %v", err)
	}
}

// 单段场次同样走完整合并路径：产物是 {base}.flv，onMetaData 被跳过，
// 源分段验证后删除。
func TestFinishSessionMergeSingleSegment(t *testing.T) {
	metaTag, videoSeq, audioSeq := mergeTestTags()
	key0 := &flv.Tag{Type: flv.TagVideo, Timestamp: 0, Data: []byte{0x17, 0x01, 0, 0, 0, 0xAA}}
	inter40 := &flv.Tag{Type: flv.TagVideo, Timestamp: 40, Data: []byte{0x27, 0x01, 0, 0, 0, 0xBB}}
	repo := newTestRepo(t, &Data{mergeEnabled: true}, nil)
	dir, base, metaPath := seedMergeSession(t, repo, buildFLVStream(t, metaTag, videoSeq, audioSeq, key0, inter40))
	danmu := `{"ts":1,"type":"danmaku","text":"hi"}` + "\n"
	if err := os.WriteFile(filepath.Join(dir, base+"_part1.danmu.jsonl"), []byte(danmu), 0o644); err != nil {
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
	if meta.MergedVideo != base+".flv" || meta.MergedDanmaku != base+".danmu.jsonl" {
		t.Fatalf("merge bookkeeping = %+v", meta)
	}
	if seg := meta.Segments[0]; seg.FLVKept {
		t.Fatalf("segment = %+v, want source dropped after verified merge", seg)
	}

	// 合并产物 = 头 + 除 onMetaData 外的全部标签。
	_, tags := readSegmentTags(t, filepath.Join(dir, base+".flv"))
	assertTagsEqual(t, tags, []*flv.Tag{videoSeq, audioSeq, key0, inter40})

	// 源分段与源弹幕验证后删除；弹幕内容完整保留。
	for _, name := range []string{base + "_part1.flv", base + "_part1.danmu.jsonl"} {
		if _, err := os.Stat(filepath.Join(dir, name)); !errors.Is(err, fs.ErrNotExist) {
			t.Fatalf("%s must be removed after a verified merge (stat err = %v)", name, err)
		}
	}
	if got, err := os.ReadFile(filepath.Join(dir, base+".danmu.jsonl")); err != nil || string(got) != danmu {
		t.Fatalf("merged danmaku = %q, %v", got, err)
	}
}

func TestFinishedSessionCanAppendAfterRecordingIsReenabled(t *testing.T) {
	metaTag, videoSeq, audioSeq := mergeTestTags()
	firstKey := &flv.Tag{Type: flv.TagVideo, Timestamp: 0, Data: []byte{0x17, 0x01, 0, 0, 0, 0xAA}}
	firstEnd := &flv.Tag{Type: flv.TagVideo, Timestamp: 10_000, Data: []byte{0x27, 0x01, 0, 0, 0, 0xBB}}
	secondKey := &flv.Tag{Type: flv.TagVideo, Timestamp: 0, Data: []byte{0x17, 0x01, 0, 0, 0, 0xCC}}
	secondEnd := &flv.Tag{Type: flv.TagVideo, Timestamp: 5_000, Data: []byte{0x27, 0x01, 0, 0, 0, 0xDD}}

	repo := newTestRepo(t, &Data{mergeEnabled: true}, nil)
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
		if _, err := repo.RecordSession(context.Background(), session, stream, nil); err != nil {
			t.Fatal(err)
		}
		if err := repo.FinishSession(context.Background(), session); err != nil {
			t.Fatal(err)
		}
	}

	record(metaTag, videoSeq, audioSeq, firstKey, firstEnd)
	record(metaTag, videoSeq, audioSeq, secondKey, secondEnd)

	dir, base, err := repo.sessionPaths(session)
	if err != nil {
		t.Fatal(err)
	}
	_, tags := readSegmentTags(t, filepath.Join(dir, base+".flv"))
	wantVideoSeq2 := &flv.Tag{Type: flv.TagVideo, Timestamp: 10_000, Data: videoSeq.Data}
	wantAudioSeq2 := &flv.Tag{Type: flv.TagAudio, Timestamp: 10_000, Data: audioSeq.Data}
	wantSecondKey := &flv.Tag{Type: flv.TagVideo, Timestamp: 10_000, Data: secondKey.Data}
	wantSecondEnd := &flv.Tag{Type: flv.TagVideo, Timestamp: 15_000, Data: secondEnd.Data}
	assertTagsEqual(t, tags, []*flv.Tag{
		videoSeq, audioSeq, firstKey, firstEnd,
		wantVideoSeq2, wantAudioSeq2, wantSecondKey, wantSecondEnd,
	})

	meta, err := loadMeta(filepath.Join(dir, base+".meta.json"))
	if err != nil {
		t.Fatal(err)
	}
	if meta.Status != metaStatusDone || meta.MergedVideo != base+".flv" {
		t.Fatalf("meta = %+v, want completed appended recording", meta)
	}
}

func TestArchiveMergedSessionRollsBackVideoWhenDanmakuIsMissing(t *testing.T) {
	dir := t.TempDir()
	const base = "session"
	videoName := base + ".flv"
	videoPath := filepath.Join(dir, videoName)
	content := []byte("merged video")
	if err := os.WriteFile(videoPath, content, 0o644); err != nil {
		t.Fatal(err)
	}
	meta := &sessionMeta{
		MergedVideo:   videoName,
		MergedDanmaku: base + ".danmu.jsonl",
	}

	if err := archiveMergedSession(dir, base, meta); err == nil {
		t.Fatal("archiveMergedSession succeeded with missing danmaku")
	}
	if got, err := os.ReadFile(videoPath); err != nil || !bytes.Equal(got, content) {
		t.Fatalf("merged video was not rolled back: %q, %v", got, err)
	}
	if len(meta.Segments) != 0 || meta.MergedVideo != videoName {
		t.Fatalf("meta changed after failed archive: %+v", meta)
	}
}

// 多段合并：第 2 段重新注入的序列头时间戳平移到合并边界，全片时间戳
// 单调不回跳；两段的弹幕按 part 顺序拼接。
func TestFinishSessionMergeMultiSegmentRebasesBoundaryHeaders(t *testing.T) {
	metaTag, videoSeq, audioSeq := mergeTestTags()
	key0 := &flv.Tag{Type: flv.TagVideo, Timestamp: 0, Data: []byte{0x17, 0x01, 0, 0, 0, 0xAA}}
	inter40 := &flv.Tag{Type: flv.TagVideo, Timestamp: 40, Data: []byte{0x27, 0x01, 0, 0, 0, 0xBB}}
	// 2 小时分段边界后的内容标签：绝对时间戳延续。
	const boundary = 7_200_000
	keyB := &flv.Tag{Type: flv.TagVideo, Timestamp: boundary, Data: []byte{0x17, 0x01, 0, 0, 0, 0xCC}}
	interB := &flv.Tag{Type: flv.TagVideo, Timestamp: boundary + 40, Data: []byte{0x27, 0x01, 0, 0, 0, 0xDD}}

	part1 := buildFLVStream(t, metaTag, videoSeq, audioSeq, key0, inter40)
	// part2 模拟切段后的重注入：序列头保留近零时间戳，内容标签延续。
	part2 := buildFLVStream(t, metaTag, videoSeq, audioSeq, keyB, interB)
	repo := newTestRepo(t, &Data{mergeEnabled: true}, nil)
	dir, base, metaPath := seedMergeSession(t, repo, part1, part2)
	if err := os.WriteFile(filepath.Join(dir, base+"_part1.danmu.jsonl"), []byte("line1\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, base+"_part2.danmu.jsonl"), []byte("line2\n"), 0o644); err != nil {
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
	for i, seg := range meta.Segments {
		if seg.FLVKept {
			t.Fatalf("segment %d = %+v, want source dropped", i+1, seg)
		}
	}

	mergedPath := filepath.Join(dir, base+".flv")
	_, tags := readSegmentTags(t, mergedPath)
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

	if got, err := os.ReadFile(filepath.Join(dir, base+".danmu.jsonl")); err != nil || string(got) != "line1\nline2\n" {
		t.Fatalf("merged danmaku = %q, %v", got, err)
	}
}

// 合并失败（分段损坏）：记录错误、状态 partial、源文件全部保留、
// 不留临时文件。
func TestFinishSessionMergeFailureKeepsSegments(t *testing.T) {
	metaTag, videoSeq, audioSeq := mergeTestTags()
	part1 := buildFLVStream(t, metaTag, videoSeq, audioSeq)
	repo := newTestRepo(t, &Data{mergeEnabled: true}, nil)
	dir, base, metaPath := seedMergeSession(t, repo, part1, []byte("not an flv"))

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
	for i, seg := range meta.Segments {
		if !seg.FLVKept {
			t.Fatalf("segment %d = %+v, want flv kept after failed merge", i+1, seg)
		}
	}
	// 源文件原样保留，无合并产物与临时文件残留。
	if _, err := os.Stat(filepath.Join(dir, base+"_part2.flv")); err != nil {
		t.Fatalf("source flv must survive a failed merge: %v", err)
	}
	for _, name := range []string{base + ".flv", base + ".flv.tmp"} {
		if _, err := os.Stat(filepath.Join(dir, name)); !errors.Is(err, fs.ErrNotExist) {
			t.Fatalf("%s must not exist after a failed merge (stat err = %v)", name, err)
		}
	}
}

// 分段源文件缺失：合并失败，状态 partial。
func TestFinishSessionMergeMissingSegmentMarksPartial(t *testing.T) {
	metaTag, videoSeq, audioSeq := mergeTestTags()
	part1 := buildFLVStream(t, metaTag, videoSeq, audioSeq)
	repo := newTestRepo(t, &Data{mergeEnabled: true}, nil)
	_, _, metaPath := seedMergeSession(t, repo, part1, nil) // part2 未落盘

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

// --- RecoverPending ---

func TestRecoverPendingFinishesInterruptedSessions(t *testing.T) {
	metaTag, videoSeq, audioSeq := mergeTestTags()
	part1 := buildFLVStream(t, metaTag, videoSeq, audioSeq)
	repo := newTestRepo(t, &Data{mergeEnabled: true}, nil)
	_, base, metaPath := seedMergeSession(t, repo, part1)
	// 模拟合并期间崩溃
	repo.updateMeta(metaPath, func(meta *sessionMeta) { meta.Status = metaStatusMerging })

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
	if meta.MergedVideo != base+".flv" {
		t.Fatalf("merge bookkeeping = %+v", meta)
	}
}

// 旧版本遗留的未知状态（如 "remuxing"）：跳过并原样保留。
func TestRecoverPendingSkipsUnknownStatus(t *testing.T) {
	metaTag, videoSeq, audioSeq := mergeTestTags()
	part1 := buildFLVStream(t, metaTag, videoSeq, audioSeq)
	repo := newTestRepo(t, &Data{mergeEnabled: true}, nil)
	dir, base, metaPath := seedMergeSession(t, repo, part1)
	repo.updateMeta(metaPath, func(meta *sessionMeta) { meta.Status = "remuxing" })

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
	if _, err := os.Stat(filepath.Join(dir, base+"_part1.flv")); err != nil {
		t.Fatalf("legacy source must be left in place: %v", err)
	}
	if _, err := os.Stat(filepath.Join(dir, base+".flv")); !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("no merged file must be produced for legacy sessions (stat err = %v)", err)
	}
}

// partial 且源分段齐全：重试合并。
func TestRecoverPendingRetriesPartialWithSources(t *testing.T) {
	metaTag, videoSeq, audioSeq := mergeTestTags()
	part1 := buildFLVStream(t, metaTag, videoSeq, audioSeq)
	repo := newTestRepo(t, &Data{mergeEnabled: true}, nil)
	_, base, metaPath := seedMergeSession(t, repo, part1)
	repo.updateMeta(metaPath, func(meta *sessionMeta) { meta.Status = metaStatusPartial })

	if err := repo.RecoverPending(context.Background()); err != nil {
		t.Fatalf("RecoverPending: %v", err)
	}
	meta, err := loadMeta(metaPath)
	if err != nil {
		t.Fatal(err)
	}
	if meta.Status != metaStatusDone || meta.MergedVideo != base+".flv" {
		t.Fatalf("meta = %+v, want done with merged output", meta)
	}
}

// partial 但源分段缺失（如旧版本的转封装产物）：原样保留，不反复报错。
func TestRecoverPendingLeavesPartialWithoutSources(t *testing.T) {
	metaTag, videoSeq, audioSeq := mergeTestTags()
	part1 := buildFLVStream(t, metaTag, videoSeq, audioSeq)
	repo := newTestRepo(t, &Data{mergeEnabled: true}, nil)
	dir, base, metaPath := seedMergeSession(t, repo, part1, nil) // part2 未落盘
	repo.updateMeta(metaPath, func(meta *sessionMeta) { meta.Status = metaStatusPartial })

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
	if _, err := os.Stat(filepath.Join(dir, base+".flv")); !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("no merged file must be produced (stat err = %v)", err)
	}
}
