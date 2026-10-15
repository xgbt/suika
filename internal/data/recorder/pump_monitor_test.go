package recorder

import (
	"testing"
	"time"
)

func TestSpeedTrackerSample(t *testing.T) {
	t0 := time.Unix(1_700_000_000, 0)
	st := newSpeedTracker(t0)

	st.addReceived(1000)
	if bps, ok := st.sample(t0.Add(time.Second)); !ok || bps != 1000 {
		t.Fatalf("sample = %d, %v; want 1000, true", bps, ok)
	}

	// 速度是相邻两次采样之间的增量，而不是累计平均。
	st.addReceived(500)
	if bps, ok := st.sample(t0.Add(3 * time.Second)); !ok || bps != 250 {
		t.Fatalf("sample = %d, %v; want 250, true", bps, ok)
	}

	// 同一时刻重复采样：无效且不推进状态。
	if bps, ok := st.sample(t0.Add(3 * time.Second)); ok || bps != 0 {
		t.Fatalf("zero-elapsed sample = %d, %v; want 0, false", bps, ok)
	}
	// 时钟回拨同理。
	if _, ok := st.sample(t0.Add(2 * time.Second)); ok {
		t.Fatal("backwards clock must not produce a sample")
	}

	// 期间没有新数据：速度为 0，且采样有效（断流时界面应显示 0）。
	if bps, ok := st.sample(t0.Add(4 * time.Second)); !ok || bps != 0 {
		t.Fatalf("idle sample = %d, %v; want 0, true", bps, ok)
	}
}

func TestHealthMonitorCheck(t *testing.T) {
	const rounds = 3
	var h healthMonitor

	// 从未写入过数据也算"无新数据"，连续 rounds 轮后判定异常。
	for i := 1; i < rounds; i++ {
		if err := h.check(0, rounds); err != nil {
			t.Fatalf("round %d: unexpected error %v", i, err)
		}
	}
	if err := h.check(0, rounds); err == nil {
		t.Fatalf("round %d: want error after %d idle rounds", rounds, rounds)
	}

	// 一旦有新数据落盘，连续失败计数清零。
	if err := h.check(10, rounds); err != nil {
		t.Fatalf("growth must recover: %v", err)
	}
	if h.failRounds != 0 {
		t.Fatalf("failRounds = %d, want 0 after growth", h.failRounds)
	}

	// 字节数不再增长即为失败，哪怕不为零。
	for i := 1; i < rounds; i++ {
		if err := h.check(10, rounds); err != nil {
			t.Fatalf("stalled round %d: unexpected error %v", i, err)
		}
	}
	if err := h.check(10, rounds); err == nil {
		t.Fatal("stalled stream must be reported unhealthy")
	}
}

// 重复块被丢弃时 resetFailRounds 清零计数，避免 CDN 重复块被误判为
// 无新数据；清零后仍需完整的 rounds 轮才会再次告警。
func TestHealthMonitorResetFailRounds(t *testing.T) {
	const rounds = 3
	var h healthMonitor
	for range rounds - 1 {
		if err := h.check(0, rounds); err != nil {
			t.Fatal(err)
		}
	}
	h.resetFailRounds()
	for range rounds - 1 {
		if err := h.check(0, rounds); err != nil {
			t.Fatalf("error right after reset: %v", err)
		}
	}
	if err := h.check(0, rounds); err == nil {
		t.Fatal("want error once rounds idle checks accumulate again")
	}
}
