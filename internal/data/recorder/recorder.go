package recorder

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	"suika/internal/biz"
	"suika/internal/conf"

	"github.com/go-kratos/kratos/v3/log"
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
// recorder_paths.go 会话目录/文件名派生、recorder_stats.go 写入进度
// 统计、recorder_merge.go 收尾合并。
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

	mu        sync.Mutex           // 保护 stats 与 meta.json 的读改写
	segmentMu sync.Mutex           // 串行化分段编号探测与创建，避免并发录制泵选中同一 part
	stats     map[int64]*pumpStats // 按房间 ID 索引的写入进度
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

// PrepareSession 创建（或在重启后重新定位）会话目录和 meta.json。
func (r *recorderRepo) PrepareSession(ctx context.Context, session *biz.RecordingSession) error {

	// 获取目录和文件名前缀
	dir, base, err := sessionPaths(r.recordRoot, session)
	if err != nil {
		return err
	}
	// 创建目录
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return err
	}
	r.statsFor(session.RoomID).reset() // 一次录制会话启动时，把写入进度清零

	r.mu.Lock()
	defer r.mu.Unlock()

	// 读取 meta.json
	metaPath := filepath.Join(dir, base+".meta.json")
	if meta, err := loadMeta(metaPath); err == nil {
		if meta.Status == metaStatusDone && meta.MergedVideo != "" {
			if err := archiveMergedSession(dir, base, meta); err != nil {
				return err
			}
		}
		// 之前已经录制过，已存在 meta.json, 更新 meta.json 的状态为 recording, 并更新标题和房间名
		meta.Status = metaStatusRecording
		meta.Title = session.Title
		meta.RoomName = session.StreamerName
		return saveMeta(metaPath, meta)
	}

	// 目录下不存在 meta.json 时，创建新的 meta.json
	start := session.LiveStartTime
	if start.IsZero() {
		start = time.Now()
	}
	meta := &sessionMeta{
		RoomID:        session.RoomID,
		RoomName:      session.StreamerName,
		Title:         session.Title,
		LiveStartTime: start.Unix(),
		Status:        metaStatusRecording,
	}
	return saveMeta(metaPath, meta)
}

// FinishSession 收尾 meta.json 并合并所有已录分段。
func (r *recorderRepo) FinishSession(ctx context.Context, session *biz.RecordingSession) error {
	dir, base, err := sessionPaths(r.recordRoot, session)
	if err != nil {
		return err
	}
	metaPath := filepath.Join(dir, base+".meta.json")

	r.mu.Lock()
	meta, err := loadMeta(metaPath)
	if err != nil {
		r.mu.Unlock()
		if os.IsNotExist(err) {
			return nil // 没有录到任何内容
		}
		return err
	}
	meta.Status = metaStatusMerging
	meta.EndTime = time.Now().Unix()
	meta.Title = session.Title
	meta.Quality = qualityMeta(session.Quality)
	err = saveMeta(metaPath, meta)
	r.mu.Unlock()
	if err != nil {
		return err
	}
	return r.finalizeSession(ctx, metaPath, meta)
}

// archiveMergedSession 将已完成会话的合并产物恢复为历史分段，使同一直播
// 场次关闭录制后再次开启时，可以继续追加而不是覆盖旧产物。
func archiveMergedSession(dir, base string, meta *sessionMeta) error {
	part := nextPartNumber(dir, base)
	videoName := fmt.Sprintf("%s_part%d.flv", base, part)
	videoPath := filepath.Join(dir, videoName)
	mergedVideoPath := filepath.Join(dir, meta.MergedVideo)
	fi, err := os.Stat(mergedVideoPath)
	if err != nil {
		return fmt.Errorf("archive merged session: %w", err)
	}
	if err := os.Rename(mergedVideoPath, videoPath); err != nil {
		return fmt.Errorf("archive merged session: %w", err)
	}

	danmuName := ""
	if meta.MergedDanmaku != "" {
		danmuName = fmt.Sprintf("%s_part%d.danmu.jsonl", base, part)
		if err := os.Rename(filepath.Join(dir, meta.MergedDanmaku), filepath.Join(dir, danmuName)); err != nil {
			_ = os.Rename(videoPath, mergedVideoPath)
			return fmt.Errorf("archive merged danmaku: %w", err)
		}
	}

	var wallStart, wallEnd, tsStart, tsEnd int64
	for _, seg := range meta.Segments {
		if wallStart == 0 || seg.WallStart != 0 && seg.WallStart < wallStart {
			wallStart = seg.WallStart
		}
		wallEnd = max(wallEnd, seg.WallEnd)
		if tsStart == 0 || seg.TsStart < tsStart {
			tsStart = seg.TsStart
		}
		tsEnd = max(tsEnd, seg.TsEnd)
	}
	meta.Segments = []segmentMeta{{
		Part: part, Video: videoName, FLVKept: true, Danmaku: danmuName,
		WallStart: wallStart, WallEnd: wallEnd, TsStart: tsStart, TsEnd: tsEnd, Bytes: fi.Size(),
	}}
	meta.MergedVideo = ""
	meta.MergedDanmaku = ""
	meta.EndTime = 0
	return nil
}

// finalizeSession 执行会话收尾：把全部分段合并为单个文件。合并失败不向
// 上返回错误，而是记录在 meta.json（状态 partial、源分段保留），由
// 下次启动的 RecoverPending 重试；只有合并产物验证通过后才删除源分段。
func (r *recorderRepo) finalizeSession(ctx context.Context, metaPath string, meta *sessionMeta) error {
	dir := filepath.Dir(metaPath)

	base := sessionBaseFromMetaPath(metaPath)
	videoName, danmuName, err := mergeSessionFiles(ctx, dir, base, meta.Segments)
	if err != nil {
		for i := range meta.Segments {
			meta.Segments[i].FLVKept = true
		}
		meta.Status = metaStatusPartial
		meta.Errors = append(meta.Errors, errorMeta{Time: time.Now().Unix(), Stage: "merge", Msg: err.Error()})
		log.Error("merge failed, keeping segments", "dir", dir, "err", err)
		return r.persistMeta(metaPath, meta)
	}

	meta.MergedVideo = videoName
	meta.MergedDanmaku = danmuName
	for i := range meta.Segments {
		seg := &meta.Segments[i]
		_ = os.Remove(filepath.Join(dir, seg.Video))
		if seg.Danmaku != "" {
			_ = os.Remove(filepath.Join(dir, seg.Danmaku))
		}
		seg.FLVKept = false
	}
	meta.Status = metaStatusDone
	return r.persistMeta(metaPath, meta)
}

// RecoverPending 扫描录制根目录下的所有 meta.json，完成上次运行遗留
// 的合并工作。
func (r *recorderRepo) RecoverPending(ctx context.Context) error {
	pattern := filepath.Join(r.recordRoot, "*", "*", "*.meta.json")
	paths, err := filepath.Glob(pattern)
	if err != nil {
		return err
	}
	for _, path := range paths {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		r.mu.Lock()
		meta, err := loadMeta(path)
		r.mu.Unlock()
		if err != nil {
			log.Warn("recover: unreadable meta.json", "path", path, "err", err)
			continue
		}
		switch meta.Status {
		case metaStatusMerging, metaStatusRecording:
			log.Info("recovering unfinished session", "path", path, "status", meta.Status)
			meta.Status = metaStatusMerging
			if meta.EndTime == 0 {
				meta.EndTime = time.Now().Unix()
			}
			if err := r.persistMeta(path, meta); err != nil {
				log.Warn("recover: persist meta failed", "path", path, "err", err)
			}
			if err := r.finalizeSession(ctx, path, meta); err != nil {
				log.Warn("recover: finalize failed", "path", path, "err", err)
			}
		case metaStatusPartial:
			// 合并失败且源分段仍在磁盘上才值得重试；源文件缺失
			//（如旧版本遗留的转封装产物）时原样保留。
			if allSegmentSourcesExist(filepath.Dir(path), meta.Segments) {
				log.Info("retrying failed merge", "path", path)
				if err := r.finalizeSession(ctx, path, meta); err != nil {
					log.Warn("recover: finalize failed", "path", path, "err", err)
				}
			}
		case metaStatusDone:
			// 无需处理。
		default:
			log.Warn("recover: unknown session status, skipping", "path", path, "status", meta.Status)
		}
	}
	return nil
}
