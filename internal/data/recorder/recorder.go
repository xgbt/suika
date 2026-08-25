package recorder

import (
	"sync"
	"time"

	"suika/internal/biz"
	"suika/internal/conf"
)

const (
	defaultRecordRoot     = "./recordings"   // 默认录制目录
	defaultSegmentMinutes = 120              // 分段时长（分钟）
	defaultHealthInterval = 10 * time.Second // 健康检查间隔，录制守护进程在该间隔内未见新数据则计为一次失败
	defaultHealthRounds   = 3                // 健康检查失败轮数，连续失败达到该轮数则判定录制异常

	// defaultMaxSegmentBytes 分段大小上限（2.5 GiB，对齐 biliup 默认值）：
	// 原画长直播的单段体积和崩溃时的损失半径由此封顶，与时长上限取或。
	defaultMaxSegmentBytes int64 = 2_684_354_560
)

// recorderRepo 实现 biz.RecorderRepo：录制目录布局、FLV 拉流写入、meta.json 簿记与收尾合并。
//
// 具体职责按文件拆分：recorder_pump.go 拉流写入循环、recorder_lifecycle.go
// 会话生命周期编排（Prepare/Finish/Recover）、recorder_meta.go meta.json
// 的 schema 与 CRUD、recorder_segment.go 分段文件与头标签缓存、
// recorder_split.go 切分策略、recorder_dedup.go CDN 循环吐流去重、
// recorder_pathing.go 会话目录/文件名派生、recorder_stats.go 写入进度
// 统计、merge.go 收尾合并。
type recorderRepo struct {
	// recordRoot 录制根目录
	recordRoot string
	// segmentDuration 分段时长，为 0 时不按时间切分
	segmentDuration time.Duration
	// maxSegmentBytes 分段大小上限，为 0 时不按大小切分
	maxSegmentBytes int64
	// healthInterval 健康检查间隔，录制守护进程在该间隔内未见新数据则计为一次失败。
	healthInterval time.Duration
	// healthFailRounds 连续健康检查失败轮数，达到该轮数则判定录制异常。
	healthFailRounds int

	mu        sync.Mutex
	segmentMu sync.Mutex
	stats     map[int64]*pumpStats
}

func NewRecorderRepo(c *conf.Recorder) biz.RecorderRepo {
	r := &recorderRepo{
		recordRoot:       defaultRecordRoot,
		segmentDuration:  defaultSegmentMinutes * time.Minute,
		maxSegmentBytes:  defaultMaxSegmentBytes,
		healthInterval:   defaultHealthInterval,
		healthFailRounds: defaultHealthRounds,
		stats:            make(map[int64]*pumpStats),
	}
	if c != nil && c.GetRecordRoot() != "" {
		r.recordRoot = c.GetRecordRoot()
	}
	return r
}

// NewSessionStatsRepo 将 RecorderRepo 转为 biz.SessionStatsRepo。
func NewSessionStatsRepo(repo biz.RecorderRepo) biz.SessionStatsRepo {
	return repo.(biz.SessionStatsRepo)
}
