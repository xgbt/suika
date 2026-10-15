package recorder

import (
	"fmt"
	"time"
)

const (
	healthCheckInterval = 10 * time.Second // 健康巡检间隔，该间隔内无新数据落盘计为一次失败
	healthMaxFailRounds = 3                // 连续失败达到该轮数判定录制异常
)

// speedTracker 按接收字节增量计算下载速度采样，供 sessionStats 展示。生命周期
// 仅限单次 PumpSession 调用（一次连接尝试），断流重连后从零重新计算；
// 与跨整个 Session（包含其内所有重连）持久的 sessionStats 相对。
type speedTracker struct {
	receiveBytes int64     // 本次会话累计接收字节（网络接收口径）
	lastSample   int64     // 上次采样时的累计接收字节
	lastSampleAt time.Time // 上次采样时刻
}

func newSpeedTracker(now time.Time) speedTracker {
	return speedTracker{lastSampleAt: now}
}

func (st *speedTracker) addReceived(n int64) {
	st.receiveBytes += n
}

// sample 返回距上次采样以来的瞬时下载速度（字节/秒）；elapsed<=0 时 ok 为 false。
func (st *speedTracker) sample(now time.Time) (bps int64, ok bool) {
	delta := max(st.receiveBytes-st.lastSample, 0)
	elapsed := now.Sub(st.lastSampleAt)
	if elapsed <= 0 {
		return 0, false
	}
	st.lastSample = st.receiveBytes
	st.lastSampleAt = now
	return int64(float64(delta) / elapsed.Seconds()), true
}

// healthMonitor 跟踪连续无新数据落盘的轮数，是 PumpSession 的内部状态。
type healthMonitor struct {
	lastGrowth int64 // 上次检查时的累计写入字节数
	failRounds int   // 连续未见新数据的轮数
}

// check 用当前累计写入字节数更新状态，连续失败达到 maxRounds 时返回错误。
func (h *healthMonitor) check(written int64, maxRounds int) error {
	if written > h.lastGrowth {
		h.lastGrowth = written
		h.failRounds = 0
		return nil
	}
	h.failRounds++
	if h.failRounds < maxRounds {
		return nil
	}
	return fmt.Errorf("recording unhealthy: no new data for %d rounds", h.failRounds)
}

// resetFailRounds 清零连续失败计数：CDN 重复块被丢弃不计入无新数据。
func (h *healthMonitor) resetFailRounds() {
	h.failRounds = 0
}
