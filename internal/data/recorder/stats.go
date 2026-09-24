package recorder

import (
	"context"
	"sync/atomic"
	"time"

	"suika/internal/biz"

	"github.com/spf13/cast"
)

// pumpStats 是 biz.SessionStats 的内部实现，使用原子字段避免锁竞争。
// 生命周期与所属 Session（录制会话：一次连续直播，定义见 CONTEXT.md）等
// 长：跨该 Session 内因断流重连而产生的多次 PumpSession 调用（每次调用即
// 一次连接尝试）持久，按房间 ID 索引，仅在 PrepareSession 开启新 Session 时重置
// 一次；与生命周期仅限单次 PumpSession 调用的 speedTracker 相对。每个房间
// 的录制守护进程在写入直播/录制状态时更新 pumpStats，然后通过 Stats() 读取快照。
type pumpStats struct {
	file  atomic.Value // string，当前正在写入的分段文件名，可能为空
	bytes atomic.Int64 // 当前分段已写入的字节数
	speed atomic.Int64 // 当前下载速度（字节/秒）
}

func (ps *pumpStats) reset() {
	ps.setBytesWritten(0)
	ps.setCurrentFile("")
	ps.setDownloadSpeed(0)
}

func (ps *pumpStats) setCurrentFile(path string) {
	ps.file.Store(path)
}

func (ps *pumpStats) setBytesWritten(n int64) {
	ps.bytes.Store(n)
}

func (ps *pumpStats) setDownloadSpeed(n int64) {
	ps.speed.Store(n)
}

func (ps *pumpStats) bytesWritten() int64 {
	return ps.bytes.Load()
}

// snapshot 返回 pumpStats 当前状态的一次性拷贝。
func (ps *pumpStats) snapshot() *biz.SessionStats {
	return &biz.SessionStats{
		CurrentFile:   cast.ToString(ps.file.Load()),
		BytesWritten:  ps.bytes.Load(),
		DownloadSpeed: ps.speed.Load(),
	}
}

// Stats 读取 pumpStats 的原子字段，返回 SessionStats。
func (r *recorderRepo) Stats(_ context.Context, roomID int64) (*biz.SessionStats, error) {
	r.mu.Lock()
	ps, ok := r.stats[roomID]
	r.mu.Unlock()

	if !ok {
		return nil, nil
	}
	return ps.snapshot(), nil
}

// getOrCreateStats 返回指定房间的 pumpStats，不存在时创建。
func (r *recorderRepo) getOrCreateStats(roomID int64) *pumpStats {
	r.mu.Lock()
	defer r.mu.Unlock()

	ps, ok := r.stats[roomID]
	if !ok {
		ps = &pumpStats{}
		r.stats[roomID] = ps
	}
	return ps
}

// speedTracker 按接收字节增量计算下载速度采样，供 pumpStats 展示。生命周期
// 仅限单次 PumpSession 调用（一次连接尝试），断流重连后从零重新计算；
// 与跨整个 Session（包含其内所有重连）持久的 pumpStats 相对。
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
