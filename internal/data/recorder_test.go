package data

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
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

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/durationpb"
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

func testSession() *biz.Session {
	return &biz.Session{
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
				Part: 1, Video: "base_part1.mp4", Danmaku: "base_part1.danmu.jsonl",
				WallStart: 1_700_000_000, WallEnd: 1_700_003_600,
				TsStart: 0, TsEnd: 7_200_000, Bytes: 123456,
				RemuxStatus: remuxStatusOK,
			},
			{
				Part: 2, Video: "base_part2.flv", FLVKept: true, Danmaku: "base_part2.danmu.jsonl",
				RemuxStatus: remuxStatusFailed, RemuxError: "ffmpeg exploded",
			},
		},
		Errors: []metaError{{Time: 55, Stage: "record", Msg: "write failed"}},
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
				base + "_part002.mp4",       // 已转封装的分段也计入
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

// --- 构造函数 / 路径 ---

func TestNewRecorderRepoConfigMapping(t *testing.T) {
	r := NewRecorderRepo(&Data{}, nil).(*recorderRepo)
	if r.recordRoot != defaultRecordRoot ||
		r.segmentDuration != defaultSegmentMinutes*time.Minute ||
		r.healthInterval != defaultHealthInterval ||
		r.healthFailRounds != defaultHealthRounds {
		t.Fatalf("defaults not applied: %+v", r)
	}

	c := &conf.Recorder{
		RecordRoot:     "/srv/recordings",
		SegmentMinutes: proto.Int32(30),
		Reconnect: &conf.Recorder_ReconnectOptions{
			HealthCheckInterval:   durationpb.New(7 * time.Second),
			HealthCheckFailRounds: 5,
		},
	}
	r = NewRecorderRepo(&Data{}, c).(*recorderRepo)
	if r.recordRoot != "/srv/recordings" || r.segmentDuration != 30*time.Minute ||
		r.healthInterval != 7*time.Second || r.healthFailRounds != 5 {
		t.Fatalf("overrides not applied: %+v", r)
	}

	// 显式零值（可与未设置区分）表示关闭切分
	r = NewRecorderRepo(&Data{}, &conf.Recorder{SegmentMinutes: proto.Int32(0)}).(*recorderRepo)
	if r.segmentDuration != 0 {
		t.Fatalf("segmentDuration = %v, want 0 (explicitly disabled)", r.segmentDuration)
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
	if _, _, err := repo.sessionPaths(&biz.Session{RoomID: 0}); !errors.Is(err, biz.ErrRoomInternal) {
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
	pump := func(session *biz.Session) {
		t.Helper()
		stream := &biz.Stream{
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
		Ts: time.Unix(123, 0), Type: biz.EventDanmaku,
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
	if first.Ts != 123_000 || first.Type != biz.EventDanmaku || first.UID != 7 ||
		first.Text != "你好" || first.Color != 16777215 || first.Mode != 1 {
		t.Fatalf("danmaku line = %+v", first)
	}
	if string(first.Raw) != `{"info":"x"}` {
		t.Fatalf("raw = %s", first.Raw)
	}
	if second.Type != biz.EventGift || second.GiftName != "火箭" || second.Num != 2 ||
		second.Price != 1000 || second.CoinType != "gold" {
		t.Fatalf("gift line = %+v", second)
	}
}

// --- RecordSession ---

func TestRecordSessionRejectsNilStream(t *testing.T) {
	repo := newTestRepo(t, nil, nil)
	if _, err := repo.RecordSession(context.Background(), testSession(), nil, nil); !errors.Is(err, biz.ErrRoomInternal) {
		t.Fatalf("err = %v, want ErrRoomInternal", err)
	}
	if _, err := repo.RecordSession(context.Background(), testSession(), &biz.Stream{}, nil); !errors.Is(err, biz.ErrRoomInternal) {
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

	stream := &biz.Stream{
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
	if seg.RemuxStatus != remuxStatusPending || seg.TsStart != 0 || seg.TsEnd != 40 {
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

	stream := &biz.Stream{
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

	stream := &biz.Stream{
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

	stream := &biz.Stream{
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

// --- FinishSession / finalizeSegments ---

// seedPendingSession 准备一个会话目录：写入 meta.json 和一个待处理分段
// （FLV 内容由参数给定），返回断言所需的路径。
func seedPendingSession(t *testing.T, repo *recorderRepo, flvContent []byte) (dir, metaPath, flvName, mp4Name string) {
	t.Helper()
	ctx := context.Background()
	session := testSession()
	if err := repo.PrepareSession(ctx, session); err != nil {
		t.Fatal(err)
	}
	var base string
	dir, base, err := repo.sessionPaths(session)
	if err != nil {
		t.Fatal(err)
	}
	metaPath = filepath.Join(dir, base+".meta.json")
	flvName = base + "_part1.flv"
	mp4Name = base + "_part1.mp4"
	if flvContent != nil {
		if err := os.WriteFile(filepath.Join(dir, flvName), flvContent, 0o644); err != nil {
			t.Fatal(err)
		}
	}
	repo.appendSegmentMeta(metaPath, &segmentFile{
		part:      1,
		videoPath: filepath.Join(dir, flvName),
		danmuPath: filepath.Join(dir, base+"_part1.danmu.jsonl"),
		wallStart: session.LiveStartTime,
	})
	return dir, metaPath, flvName, mp4Name
}

func TestFinishSessionWithoutMetaIsNoop(t *testing.T) {
	repo := newTestRepo(t, nil, nil)
	if err := repo.FinishSession(context.Background(), testSession()); err != nil {
		t.Fatalf("want nil for missing meta.json, got %v", err)
	}
}

func TestFinishSessionRemuxDisabledKeepsFLV(t *testing.T) {
	repo := newTestRepo(t, &Data{remuxEnabled: false}, nil)
	content := []byte("fake flv bytes")
	dir, metaPath, flvName, mp4Name := seedPendingSession(t, repo, content)

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
	if meta.EndTime == 0 || meta.Quality.Qn != 10000 || meta.Quality.Desc != "source" {
		t.Fatalf("finish bookkeeping = %+v", meta)
	}
	if len(meta.Segments) != 1 {
		t.Fatalf("segments = %+v", meta.Segments)
	}
	seg := meta.Segments[0]
	if seg.RemuxStatus != remuxStatusOK || !seg.FLVKept || seg.Video != flvName {
		t.Fatalf("segment = %+v, want ok / flv_kept / %s", seg, flvName)
	}
	if _, err := os.Stat(filepath.Join(dir, mp4Name)); !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("mp4 must not be produced without ffmpeg (stat err = %v)", err)
	}
	if got, err := os.ReadFile(filepath.Join(dir, flvName)); err != nil || !bytes.Equal(got, content) {
		t.Fatalf("source flv modified: %q, %v", got, err)
	}
}

func TestFinishSessionRemuxSuccessReplacesFLV(t *testing.T) {
	ffmpeg, _, countFile := writeFakeFFmpeg(t, t.TempDir(), 0)
	repo := newTestRepo(t, &Data{remuxEnabled: true, ffmpegPath: ffmpeg}, nil)
	dir, metaPath, flvName, mp4Name := seedPendingSession(t, repo, []byte("fake flv bytes"))

	if err := repo.FinishSession(context.Background(), testSession()); err != nil {
		t.Fatalf("FinishSession: %v", err)
	}
	if n := readCount(t, countFile); n != 1 {
		t.Fatalf("ffmpeg invocations = %d, want 1", n)
	}

	meta, err := loadMeta(metaPath)
	if err != nil {
		t.Fatal(err)
	}
	if meta.Status != metaStatusDone {
		t.Fatalf("status = %q, want %q", meta.Status, metaStatusDone)
	}
	seg := meta.Segments[0]
	if seg.RemuxStatus != remuxStatusOK || seg.FLVKept || seg.Video != mp4Name {
		t.Fatalf("segment = %+v, want ok / mp4 / flv dropped", seg)
	}
	if got, err := os.ReadFile(filepath.Join(dir, mp4Name)); err != nil || string(got) != "FAKE_MP4_DATA" {
		t.Fatalf("mp4 = %q, %v", got, err)
	}
	if _, err := os.Stat(filepath.Join(dir, flvName)); !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("source flv must be removed after a verified remux (stat err = %v)", err)
	}
}

func TestFinishSessionRemuxFailureKeepsFLV(t *testing.T) {
	ffmpeg, _, countFile := writeFakeFFmpeg(t, t.TempDir(), 999) // 恒定失败
	repo := newTestRepo(t, &Data{remuxEnabled: true, ffmpegPath: ffmpeg}, nil)
	dir, metaPath, flvName, mp4Name := seedPendingSession(t, repo, []byte("fake flv bytes"))

	// finalize 把失败记录在 meta 中，而不是返回错误
	if err := repo.FinishSession(context.Background(), testSession()); err != nil {
		t.Fatalf("FinishSession: %v", err)
	}
	if n := readCount(t, countFile); n != 2 {
		t.Fatalf("ffmpeg invocations = %d, want 2 (remuxWithRetry retries once)", n)
	}

	meta, err := loadMeta(metaPath)
	if err != nil {
		t.Fatal(err)
	}
	if meta.Status != metaStatusPartial {
		t.Fatalf("status = %q, want %q", meta.Status, metaStatusPartial)
	}
	seg := meta.Segments[0]
	if seg.RemuxStatus != remuxStatusFailed || !seg.FLVKept || seg.RemuxError == "" {
		t.Fatalf("segment = %+v, want failed / flv kept / error recorded", seg)
	}
	if !strings.Contains(seg.RemuxError, "fake ffmpeg failure") {
		t.Fatalf("RemuxError = %q, want the ffmpeg stderr captured", seg.RemuxError)
	}
	if _, err := os.Stat(filepath.Join(dir, flvName)); err != nil {
		t.Fatalf("source flv must survive a failed remux: %v", err)
	}
	if _, err := os.Stat(filepath.Join(dir, mp4Name)); !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("no mp4 must exist after a failed remux (stat err = %v)", err)
	}
}

func TestFinishSessionEmptyFFmpegPathMarksFailed(t *testing.T) {
	// 开启转封装但未解析到 ffmpeg：remuxWithRetry 收到空路径，
	// 必须优雅失败而不是 panic。
	repo := newTestRepo(t, &Data{remuxEnabled: true}, nil)
	dir, metaPath, flvName, mp4Name := seedPendingSession(t, repo, []byte("fake flv bytes"))

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
	seg := meta.Segments[0]
	if seg.RemuxStatus != remuxStatusFailed || !seg.FLVKept || !strings.Contains(seg.RemuxError, "ffmpeg") {
		t.Fatalf("segment = %+v, want failed with an ffmpeg error", seg)
	}
	if _, err := os.Stat(filepath.Join(dir, flvName)); err != nil {
		t.Fatalf("source flv must survive: %v", err)
	}
	if _, err := os.Stat(filepath.Join(dir, mp4Name)); !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("no mp4 must exist (stat err = %v)", err)
	}
}

func TestFinalizeSegmentsMissingSourceRecovery(t *testing.T) {
	// 这里不会调用 ffmpegPath：两个分支都只看文件系统
	//（源缺失，或 mp4 此前已转封装完成）。
	repo := newTestRepo(t, &Data{remuxEnabled: true, ffmpegPath: "/should/not/be/called"}, nil)
	ctx := context.Background()
	session := testSession()
	if err := repo.PrepareSession(ctx, session); err != nil {
		t.Fatal(err)
	}
	dir, base, err := repo.sessionPaths(session)
	if err != nil {
		t.Fatal(err)
	}
	metaPath := filepath.Join(dir, base+".meta.json")
	// part1：磁盘上 flv 和 mp4 都不存在；part2：flv 已删，mp4 已存在。
	if err := os.WriteFile(filepath.Join(dir, base+"_part2.mp4"), []byte("done"), 0o644); err != nil {
		t.Fatal(err)
	}
	repo.appendSegmentMeta(metaPath, &segmentFile{part: 1, videoPath: filepath.Join(dir, base+"_part1.flv"), danmuPath: filepath.Join(dir, base+"_part1.danmu.jsonl"), wallStart: session.LiveStartTime})
	repo.appendSegmentMeta(metaPath, &segmentFile{part: 2, videoPath: filepath.Join(dir, base+"_part2.flv"), danmuPath: filepath.Join(dir, base+"_part2.danmu.jsonl"), wallStart: session.LiveStartTime})

	if err := repo.FinishSession(ctx, session); err != nil {
		t.Fatalf("FinishSession: %v", err)
	}
	meta, err := loadMeta(metaPath)
	if err != nil {
		t.Fatal(err)
	}
	if meta.Status != metaStatusPartial {
		t.Fatalf("status = %q, want %q", meta.Status, metaStatusPartial)
	}
	if got := meta.Segments[0]; got.RemuxStatus != remuxStatusFailed || got.RemuxError != "source flv missing" {
		t.Fatalf("missing-source segment = %+v", got)
	}
	if got := meta.Segments[1]; got.RemuxStatus != remuxStatusOK || got.Video != base+"_part2.mp4" || got.FLVKept {
		t.Fatalf("already-remuxed segment = %+v", got)
	}
}

// --- RecoverPending ---

func TestRecoverPendingFinishesInterruptedSessions(t *testing.T) {
	repo := newTestRepo(t, &Data{remuxEnabled: false}, nil)
	_, metaPath, flvName, _ := seedPendingSession(t, repo, []byte("fake flv bytes"))
	// 模拟转封装期间崩溃
	repo.updateMeta(metaPath, func(meta *sessionMeta) { meta.Status = metaStatusRemuxing })

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
	if seg := meta.Segments[0]; seg.RemuxStatus != remuxStatusOK || !seg.FLVKept || seg.Video != flvName {
		t.Fatalf("segment = %+v, want ok / flv kept", seg)
	}
}
