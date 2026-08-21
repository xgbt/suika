package data

import (
	"context"
	"sync/atomic"

	"suika/internal/biz"
)

// pumpStats 是 SessionStats 的内部实现，使用原子字段避免锁竞争。
// 每个房间的录制守护进程在写入直播/录制状态时更新 pumpStats，然后通过 SessionStats() 读取快照
type pumpStats struct {
	file  atomic.Value // string
	bytes atomic.Int64
	speed atomic.Int64
}

// SessionStats 读取 pumpStats 的原子字段，返回 SessionStats
func (r *recorderRepo) SessionStats(_ context.Context, roomID int64) (*biz.SessionStats, error) {
	r.mu.Lock()
	ps, ok := r.stats[roomID]
	r.mu.Unlock()

	if !ok {
		return nil, nil
	}
	file, _ := ps.file.Load().(string)
	return &biz.SessionStats{
		CurrentFile:      file,
		BytesWritten:     ps.bytes.Load(),
		DownloadSpeedBPS: ps.speed.Load(),
	}, nil
}

// statsFor 返回指定房间的 pumpStats
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
