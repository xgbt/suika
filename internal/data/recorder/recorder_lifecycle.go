package recorder

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"suika/internal/biz"

	"github.com/go-kratos/kratos/v3/log"
)

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

	r.mu.Lock()
	defer r.mu.Unlock()

	// 一次录制会话启动时, 把写入进度清零
	ps, ok := r.stats[session.RoomID]
	if !ok {
		ps = &pumpStats{}
		r.stats[session.RoomID] = ps
	}
	ps.reset()

	// 读取 meta.json，
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
	meta.Quality = qualityMeta{Qn: session.Quality.Qn, Desc: session.Quality.Desc}
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
			r.persistMeta(path, meta)
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
