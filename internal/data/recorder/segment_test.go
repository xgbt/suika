package recorder

import (
	"encoding/json"
	"errors"
	"io/fs"
	"os"
	"strings"
	"testing"
	"time"

	"suika/internal/biz"
	"suika/internal/data/flv"
)

func TestInitTagsSeqHeaderChanged(t *testing.T) {
	metaTag, videoSeq, audioSeq := testInitTags()
	var it initTags

	// 首次出现不算变化：缓存里还没有可比较的旧头。
	if it.seqHeaderChanged(videoSeq) || it.seqHeaderChanged(audioSeq) {
		t.Fatal("first sight of a sequence header must not count as a change")
	}

	it.observe(metaTag)
	it.observe(videoSeq)
	// 只缓存了视频头时，音频头仍是首次出现。
	if it.seqHeaderChanged(audioSeq) {
		t.Fatal("audio header must not count as changed while none is cached")
	}
	it.observe(audioSeq)

	otherVideo := &flv.Tag{Type: flv.TagVideo, Data: []byte{0x17, 0x00, 0, 0, 0, 9, 9, 9}}
	otherAudio := &flv.Tag{Type: flv.TagAudio, Data: []byte{0xAF, 0x00, 0x11, 0x90}}
	otherMeta := &flv.Tag{Type: flv.TagScript, Data: []byte{0x02, 0x00, 0x01, 'x'}}
	keyframe := &flv.Tag{Type: flv.TagVideo, Data: []byte{0x17, 0x01, 0, 0, 0, 0xAA}}
	audioFrame := &flv.Tag{Type: flv.TagAudio, Data: []byte{0xAF, 0x01, 0x09}}

	cases := []struct {
		name string
		tag  *flv.Tag
		want bool
	}{
		{"identical video header", &flv.Tag{Type: flv.TagVideo, Timestamp: 50, Data: videoSeq.Data}, false},
		{"identical audio header", &flv.Tag{Type: flv.TagAudio, Timestamp: 50, Data: audioSeq.Data}, false},
		{"different video header", otherVideo, true},
		{"different audio header", otherAudio, true},
		// metadata 只是展示用提示值，不影响解码器配置。
		{"different metadata", otherMeta, false},
		{"keyframe", keyframe, false},
		{"audio frame", audioFrame, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := it.seqHeaderChanged(tc.tag); got != tc.want {
				t.Fatalf("seqHeaderChanged = %v, want %v", got, tc.want)
			}
		})
	}
}

func TestInitTagsObserveOnlyCachesInitTags(t *testing.T) {
	metaTag, videoSeq, audioSeq := testInitTags()
	keyframe := &flv.Tag{Type: flv.TagVideo, Data: []byte{0x17, 0x01, 0, 0, 0, 0xAA}}
	audioFrame := &flv.Tag{Type: flv.TagAudio, Data: []byte{0xAF, 0x01, 0x09}}

	var it initTags
	it.observe(keyframe)
	it.observe(audioFrame)
	if it.metadata != nil || it.videoSeqHeader != nil || it.audioSeqHeader != nil {
		t.Fatalf("body tags must not be cached: %+v", it)
	}

	it.observe(metaTag)
	it.observe(videoSeq)
	it.observe(audioSeq)
	if it.metadata != metaTag || it.videoSeqHeader != videoSeq || it.audioSeqHeader != audioSeq {
		t.Fatalf("init tags not cached: %+v", it)
	}

	// 后来者覆盖前者：新分段注入的永远是最近一次的头。
	newer := &flv.Tag{Type: flv.TagVideo, Timestamp: 99, Data: []byte{0x17, 0x00, 0, 0, 0, 7, 7, 7}}
	it.observe(newer)
	if it.videoSeqHeader != newer {
		t.Fatal("video seq header must be replaced by the latest one")
	}
}

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
		{
			"overrun forces on audio too", time.Minute, true, 0,
			&flv.Tag{Type: flv.TagAudio, Timestamp: (minute + overrun + 1) * ms, Data: audio}, true,
		},
		{
			"overrun forces on metadata too", time.Minute, true, 0,
			&flv.Tag{Type: flv.TagScript, Timestamp: (minute + overrun) * ms, Data: script}, true,
		},
		// 时间戳回退（换源后时间戳重置）时已录时长为负，不应触发切分。
		{"timestamp before segment start", time.Minute, true, 5_000 * ms, video(1_000*ms, key), false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			seg := &segmentWriter{hasStart: tc.hasStart, startTs: tc.startTs}
			setSplitLimits(t, 0, tc.dur)
			if got := seg.shouldSplit(tc.tag); got != tc.want {
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
			seg := &segmentWriter{hasStart: tc.hasStart, bytesWritten: tc.bytes}
			setSplitLimits(t, tc.maxBytes, 0)
			if got := seg.shouldSplit(tc.tag); got != tc.want {
				t.Fatalf("shouldSplit = %v, want %v", got, tc.want)
			}
		})
	}
}

// 大小与时长两个上限相互独立，任一触发即切分。
func TestShouldSplitEitherLimitTriggers(t *testing.T) {
	key := &flv.Tag{Type: flv.TagVideo, Timestamp: 100, Data: []byte{0x17, 0x01, 0, 0, 0}}
	setSplitLimits(t, 1000, 50*time.Millisecond)

	cases := []struct {
		name  string
		bytes int64
		tag   *flv.Tag
		want  bool
	}{
		{"neither reached", 999, &flv.Tag{Type: flv.TagVideo, Timestamp: 49, Data: key.Data}, false},
		{"only size reached", 1000, &flv.Tag{Type: flv.TagVideo, Timestamp: 49, Data: key.Data}, true},
		{"only duration reached", 10, key, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			seg := &segmentWriter{hasStart: true, startTs: 0, bytesWritten: tc.bytes}
			if got := seg.shouldSplit(tc.tag); got != tc.want {
				t.Fatalf("shouldSplit = %v, want %v", got, tc.want)
			}
		})
	}
}

func TestOpenSegmentReinjectsCachedHeaders(t *testing.T) {
	header := &flv.FileHeader{Version: 1, HasAudio: true, HasVideo: true}
	metaTag, videoSeq, audioSeq := testInitTags()
	cache := &initTags{metadata: metaTag, videoSeqHeader: videoSeq, audioSeqHeader: audioSeq}

	seg, err := OpenSegment(sessionLayout{dir: t.TempDir(), base: "base"}, header, cache)
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
	if seg.bytesWritten != fi.Size() {
		t.Fatalf("seg.bytesWritten = %d, file size = %d", seg.bytesWritten, fi.Size())
	}
	if fi2, err := os.Stat(seg.danmakuPath); err != nil || fi2.Size() != 0 {
		t.Fatalf("danmu file = %+v, %v; want empty existing file", fi2, err)
	}
	// 初始化标签只属于段首前导，不算正文：切分时长以首个正文标签为起点。
	if seg.hasStart {
		t.Fatal("preamble tags must not mark the segment as started")
	}
}

// 缓存里缺哪个头就不写哪个，不能写出 nil 标签。
func TestOpenSegmentSkipsMissingCachedHeaders(t *testing.T) {
	header := &flv.FileHeader{Version: 1, HasAudio: true, HasVideo: true}
	_, videoSeq, _ := testInitTags()

	seg, err := OpenSegment(sessionLayout{dir: t.TempDir(), base: "base"}, header, &initTags{videoSeqHeader: videoSeq})
	if err != nil {
		t.Fatal(err)
	}
	if err := seg.close(); err != nil {
		t.Fatal(err)
	}
	_, tags := readSegmentTags(t, seg.videoPath)
	assertTagsEqual(t, tags, []*flv.Tag{videoSeq})
}

func TestOpenSegmentNumbersPartsSequentially(t *testing.T) {
	header := &flv.FileHeader{Version: 1, HasAudio: true, HasVideo: true}
	lay := sessionLayout{dir: t.TempDir(), base: "base"}

	first, err := OpenSegment(lay, header, &initTags{})
	if err != nil {
		t.Fatal(err)
	}
	if err := first.close(); err != nil {
		t.Fatal(err)
	}
	second, err := OpenSegment(lay, header, &initTags{})
	if err != nil {
		t.Fatal(err)
	}
	if err := second.close(); err != nil {
		t.Fatal(err)
	}

	if first.part != 1 || second.part != 2 {
		t.Fatalf("parts = %d, %d, want 1, 2", first.part, second.part)
	}
	if first.videoPath != lay.segmentVideoPath(1) || second.videoPath != lay.segmentVideoPath(2) {
		t.Fatalf("paths = %q, %q", first.videoPath, second.videoPath)
	}
	// 第二次开段不得截断第一个分段。
	for _, path := range []string{first.videoPath, first.danmakuPath} {
		if _, err := os.Stat(path); err != nil {
			t.Fatalf("part1 file lost after opening part2: %v", err)
		}
	}
}

// 开段中途失败（这里让弹幕文件路径被目录占用）必须回收已创建的视频
// 文件，不留下无 meta 登记的孤儿分段。
func TestOpenSegmentFailureCleansUp(t *testing.T) {
	header := &flv.FileHeader{Version: 1, HasAudio: true, HasVideo: true}
	lay := sessionLayout{dir: t.TempDir(), base: "base"}
	if err := os.Mkdir(lay.segmentDanmakuPath(1), 0o755); err != nil {
		t.Fatal(err)
	}

	if seg, err := OpenSegment(lay, header, &initTags{}); err == nil {
		seg.close()
		t.Fatal("OpenSegment succeeded although the danmaku path is a directory")
	}
	if _, err := os.Stat(lay.segmentVideoPath(1)); !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("video file leaked after failed OpenSegment (stat err = %v)", err)
	}
}

func TestSegmentWriteBodyTagTracksRange(t *testing.T) {
	header := &flv.FileHeader{Version: 1, HasAudio: true, HasVideo: true}
	seg, err := OpenSegment(sessionLayout{dir: t.TempDir(), base: "base"}, header, &initTags{})
	if err != nil {
		t.Fatal(err)
	}
	preamble := seg.bytesWritten
	if want := int64(len(header.Bytes())); preamble != want {
		t.Fatalf("preamble bytes = %d, want %d", preamble, want)
	}

	first := &flv.Tag{Type: flv.TagVideo, Timestamp: 100, Data: []byte{0x17, 0x01, 0, 0, 0, 0xAA}}
	second := &flv.Tag{Type: flv.TagVideo, Timestamp: 200, Data: []byte{0x27, 0x01, 0, 0, 0, 0xBB}}
	n1, err := seg.writeBodyTag(first)
	if err != nil {
		t.Fatal(err)
	}
	if want := int64(len(first.Data) + flv.TagOverhead); n1 != want {
		t.Fatalf("writeBodyTag returned %d, want %d", n1, want)
	}
	if !seg.hasStart || seg.startTs != 100 || seg.lastTs != 100 {
		t.Fatalf("after first tag: hasStart=%v startTs=%d lastTs=%d", seg.hasStart, seg.startTs, seg.lastTs)
	}

	if _, err := seg.writeBodyTag(second); err != nil {
		t.Fatal(err)
	}
	// 起点只由首个正文标签决定，终点随最新标签推进。
	if seg.startTs != 100 || seg.lastTs != 200 {
		t.Fatalf("after second tag: startTs=%d lastTs=%d, want 100/200", seg.startTs, seg.lastTs)
	}

	if err := seg.close(); err != nil {
		t.Fatal(err)
	}
	fi, err := os.Stat(seg.videoPath)
	if err != nil {
		t.Fatal(err)
	}
	if seg.bytesWritten != fi.Size() {
		t.Fatalf("bytesWritten = %d, file size = %d", seg.bytesWritten, fi.Size())
	}
}

// close 的任一步骤失败都不能跳过后续步骤，且错误要合并返回；重复关闭
// 时两个文件句柄都已失效，两个错误都应出现。
func TestSegmentCloseReportsAllErrors(t *testing.T) {
	header := &flv.FileHeader{Version: 1, HasAudio: true, HasVideo: true}
	seg, err := OpenSegment(sessionLayout{dir: t.TempDir(), base: "base"}, header, &initTags{})
	if err != nil {
		t.Fatal(err)
	}
	if err := seg.close(); err != nil {
		t.Fatalf("first close: %v", err)
	}

	err = seg.close()
	if err == nil {
		t.Fatal("second close must report the already-closed files")
	}
	for _, want := range []string{"close video file", "close danmaku file"} {
		if !strings.Contains(err.Error(), want) {
			t.Fatalf("err = %q, missing %q", err, want)
		}
	}
}

func TestSegmentWriteDanmakuEvents(t *testing.T) {
	header := &flv.FileHeader{Version: 1, HasAudio: true, HasVideo: true}
	seg, err := OpenSegment(sessionLayout{dir: t.TempDir(), base: "base"}, header, &initTags{})
	if err != nil {
		t.Fatal(err)
	}
	danmaku := &biz.DanmakuEvent{
		TS: time.Unix(123, 0), SendTS: 1755633600123, Type: biz.EventDanmaku,
		UID: 7, Uname: "user", Text: "你好", Color: 16777215, Mode: 1,
		Raw: []byte(`{"info":"x"}`),
	}
	gift := &biz.DanmakuEvent{
		TS: time.Unix(124, 0), Type: biz.EventGift,
		UID: 7, Uname: "user", GiftName: "火箭", Num: 2, Price: 1000, CoinType: "gold",
	}
	if err := seg.writeDanmaku(danmaku); err != nil {
		t.Fatal(err)
	}
	if err := seg.writeDanmaku(gift); err != nil {
		t.Fatal(err)
	}
	if err := seg.close(); err != nil {
		t.Fatal(err)
	}

	data, err := os.ReadFile(seg.danmakuPath)
	if err != nil {
		t.Fatal(err)
	}
	lines := strings.Split(strings.TrimRight(string(data), "\n"), "\n")
	if len(lines) != 2 {
		t.Fatalf("lines = %d, want 2: %q", len(lines), data)
	}
	var first, second danmakuEventPO
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
