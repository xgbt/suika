package recorder

import (
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"
)

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
				Part: 2, Video: "base_part2.flv", Danmaku: "base_part2.danmu.jsonl",
			},
		},
		MergedVideo:   "base.flv",
		MergedDanmaku: "base.danmu.jsonl",
		Errors:        []errorMeta{{Ts: 55, Stage: "record", Msg: "write failed"}},
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

// updateMeta 读不出 meta 时不得调用 fn，更不能覆盖损坏的文件：损坏
// 的 meta.json 留给人工排查，不能被一次"读改写"悄悄抹掉。
func TestUpdateMetaSkipsUnreadableMeta(t *testing.T) {
	dir := t.TempDir()
	called := false
	fn := func(*sessionMeta) { called = true }

	missing := filepath.Join(dir, "nope.meta.json")
	updateMeta(missing, fn)
	if _, err := os.Stat(missing); !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("updateMeta must not create a missing meta (stat err = %v)", err)
	}

	corrupt := filepath.Join(dir, "bad.meta.json")
	if err := os.WriteFile(corrupt, []byte("{not json"), 0o644); err != nil {
		t.Fatal(err)
	}
	updateMeta(corrupt, fn)
	if got, err := os.ReadFile(corrupt); err != nil || string(got) != "{not json" {
		t.Fatalf("corrupt meta was overwritten: %q, %v", got, err)
	}

	if called {
		t.Fatal("fn must not run when meta cannot be loaded")
	}
}

func TestSegmentMetaBookkeeping(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "s.meta.json")
	if err := saveMeta(path, &sessionMeta{Status: metaStatusRecording}); err != nil {
		t.Fatal(err)
	}

	start := time.Unix(1_700_000_000, 0)
	newWriter := func(part int, name string) *segmentWriter {
		return &segmentWriter{
			part:        part,
			videoPath:   filepath.Join(dir, name+".flv"),
			danmakuPath: filepath.Join(dir, name+".danmu.jsonl"),
			wallStart:   start,
		}
	}
	w1, w2 := newWriter(1, "s_part1"), newWriter(2, "s_part2")
	appendSegmentMeta(path, w1)
	appendSegmentMeta(path, w2)

	// 收尾只回填对应 part 的字段，不影响其他分段。
	w1.startTs, w1.lastTs, w1.bytesWritten = 100, 900, 4096
	before := time.Now().Unix()
	finishSegmentMeta(path, w1)

	meta, err := loadMeta(path)
	if err != nil {
		t.Fatal(err)
	}
	if len(meta.Segments) != 2 {
		t.Fatalf("segments = %+v, want 2", meta.Segments)
	}
	s1, s2 := meta.Segments[0], meta.Segments[1]
	// meta 里只存相对会话目录的文件名，不存绝对路径。
	if s1.Part != 1 || s1.Video != "s_part1.flv" || s1.Danmaku != "s_part1.danmu.jsonl" || s1.WallStart != start.Unix() {
		t.Fatalf("segment 1 = %+v", s1)
	}
	if s1.TsStart != 100 || s1.TsEnd != 900 || s1.Bytes != 4096 || s1.WallEnd < before {
		t.Fatalf("segment 1 not finalized: %+v", s1)
	}
	if s2.Part != 2 || s2.WallEnd != 0 || s2.TsEnd != 0 || s2.Bytes != 0 {
		t.Fatalf("segment 2 must stay untouched: %+v", s2)
	}
}

func TestAppendMetaError(t *testing.T) {
	path := filepath.Join(t.TempDir(), "e.meta.json")
	if err := saveMeta(path, &sessionMeta{Status: metaStatusRecording}); err != nil {
		t.Fatal(err)
	}

	before := time.Now().Unix()
	appendMetaError(path, "record", errors.New("disk full"))
	appendMetaError(path, "merge", errors.New("bad flv"))

	meta, err := loadMeta(path)
	if err != nil {
		t.Fatal(err)
	}
	if len(meta.Errors) != 2 {
		t.Fatalf("errors = %+v, want 2", meta.Errors)
	}
	if e := meta.Errors[0]; e.Stage != "record" || e.Msg != "disk full" || e.Ts < before {
		t.Fatalf("errors[0] = %+v", e)
	}
	if e := meta.Errors[1]; e.Stage != "merge" || e.Msg != "bad flv" {
		t.Fatalf("errors[1] = %+v", e)
	}
}
