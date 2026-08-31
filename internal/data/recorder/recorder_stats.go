package recorder

import (
	"context"
	"sync/atomic"

	"suika/internal/biz"
)

// pumpStats 是 SessionStats 的内部实现，使用原子字段避免锁竞争。
// 每个房间的录制守护进程在写入直播/录制状态时更新 pumpStats，然后通过 SessionStats() 读取快照
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
	file, _ := ps.file.Load().(string)
	return &biz.SessionStats{
		CurrentFile:   file,
		BytesWritten:  ps.bytes.Load(),
		DownloadSpeed: ps.speed.Load(),
	}
}

// SessionStats 读取 pumpStats 的原子字段，返回 SessionStats
func (r *recorderRepo) SessionStats(_ context.Context, roomID int64) (*biz.SessionStats, error) {
	r.mu.Lock()
	ps, ok := r.stats[roomID]
	r.mu.Unlock()

	if !ok {
		return nil, nil
	}
	return ps.snapshot(), nil
}

// statsFor 返回指定房间的 pumpStats，不存在时创建。
func (r *recorderRepo) statsFor(roomID int64) *pumpStats {
	r.mu.Lock()
	defer r.mu.Unlock()

	ps, ok := r.stats[roomID]
	if !ok {
		ps = &pumpStats{}
		r.stats[roomID] = ps
	}
	return ps
}
