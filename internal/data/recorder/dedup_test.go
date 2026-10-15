package recorder

import (
	"testing"

	"suika/internal/data/flv"
)

// dupBlock 构造一个两标签的块：关键帧 + 帧间，载荷由 k 决定。
func dupBlock(k byte, ts int64) []*flv.Tag {
	return []*flv.Tag{
		{Type: flv.TagVideo, Timestamp: ts, Data: []byte{0x17, 0x01, k}},
		{Type: flv.TagVideo, Timestamp: ts + 10, Data: []byte{0x27, 0x01, k}},
	}
}

func TestDedupGuardDropsRepeatedBlock(t *testing.T) {
	var g dedupGuard

	feed := func(tags []*flv.Tag) {
		for _, tag := range tags {
			g.push(tag)
		}
	}

	// 首个块：唯一，应写入。
	feed(dupBlock(0xAA, 0))
	buf, disconnect := g.flush()
	if disconnect || len(buf) != 2 {
		t.Fatalf("first block = (%d tags, disconnect=%v), want unique and writable", len(buf), disconnect)
	}

	// 内容相同、时间戳前进：仍是重复，丢弃并计数。
	feed(dupBlock(0xAA, 100))
	buf, disconnect = g.flush()
	if disconnect || buf != nil {
		t.Fatalf("replayed block = (%v, disconnect=%v), want dropped", buf, disconnect)
	}
	if g.streak != 1 {
		t.Fatalf("streak = %d, want 1", g.streak)
	}

	// 不同内容：正常写入并重置连续计数。
	feed(dupBlock(0xBB, 200))
	buf, disconnect = g.flush()
	if disconnect || len(buf) != 2 {
		t.Fatalf("distinct block = (%d tags, disconnect=%v), want writable", len(buf), disconnect)
	}
	if g.streak != 0 {
		t.Fatalf("streak = %d, want reset to 0", g.streak)
	}
}

func TestDedupGuardBlockBoundaries(t *testing.T) {
	var g dedupGuard
	g.push(&flv.Tag{Type: flv.TagVideo, Timestamp: 0, Data: []byte{0x27, 0x01, 1}})

	if !g.atBoundary(&flv.Tag{Type: flv.TagVideo, Timestamp: 10, Data: []byte{0x17, 0x01, 2}}) {
		t.Error("video keyframe must close the current block")
	}
	if !g.atBoundary(&flv.Tag{Type: flv.TagVideo, Timestamp: 10, Data: []byte{0x17, 0x00, 2}}) {
		t.Error("sequence header must close the current block")
	}
	if !g.atBoundary(&flv.Tag{Type: flv.TagScript, Timestamp: 10, Data: []byte{0x02}}) {
		t.Error("metadata tag must close the current block")
	}
	if !g.atBoundary(&flv.Tag{Type: flv.TagVideo, Timestamp: dedupGapMs, Data: []byte{0x27, 0x01, 2}}) {
		t.Error("timestamp gap beyond threshold must close the current block")
	}
	if g.atBoundary(&flv.Tag{Type: flv.TagVideo, Timestamp: 10, Data: []byte{0x27, 0x01, 2}}) {
		t.Error("ordinary inter frame within limits must not close the block")
	}
}

func TestDedupGuardEmptyBoundary(t *testing.T) {
	var g dedupGuard
	if g.atBoundary(&flv.Tag{Type: flv.TagVideo, Timestamp: 0, Data: []byte{0x17, 0x01, 1}}) {
		t.Error("empty guard has no block to close")
	}
	if buf, disconnect := g.flush(); buf != nil || disconnect {
		t.Errorf("flushing an empty guard = (%v, %v), want noop", buf, disconnect)
	}
}

func TestDedupGuardDisconnectAfterStreak(t *testing.T) {
	var g dedupGuard

	// 首次出现：唯一。
	for _, tag := range dupBlock(0xAA, 0) {
		g.push(tag)
	}
	if buf, disconnect := g.flush(); disconnect || len(buf) != 2 {
		t.Fatalf("first occurrence = (%d tags, disconnect=%v), want unique", len(buf), disconnect)
	}

	// 后续重复逐一丢弃，未达上限不断开。
	for i := 1; i < dedupMaxStreak; i++ {
		for _, tag := range dupBlock(0xAA, int64(i)*100) {
			g.push(tag)
		}
		if buf, disconnect := g.flush(); disconnect || buf != nil {
			t.Fatalf("duplicate %d = (%v, disconnect=%v), want dropped without disconnect", i, buf, disconnect)
		}
	}

	// 第 dedupMaxStreak 个连续重复块触发断开。
	for _, tag := range dupBlock(0xAA, int64(dedupMaxStreak)*100) {
		g.push(tag)
	}
	if buf, disconnect := g.flush(); !disconnect || buf != nil {
		t.Fatalf("duplicate %d = (%v, disconnect=%v), want disconnect", dedupMaxStreak, buf, disconnect)
	}
}

func TestDedupGuardDrainSkipsVerdict(t *testing.T) {
	var g dedupGuard
	block := dupBlock(0xAA, 0)
	for _, tag := range block {
		g.push(tag)
	}
	got := g.drain()
	if len(got) != 2 {
		t.Fatalf("drain = %d tags, want 2", len(got))
	}
	// 未裁决的内容不进指纹窗口：同样的块再来一次仍是"唯一"。
	for _, tag := range block {
		g.push(tag)
	}
	if buf, disconnect := g.flush(); disconnect || len(buf) != 2 {
		t.Fatalf("block after drain = (%d tags, disconnect=%v), want unique", len(buf), disconnect)
	}
}
