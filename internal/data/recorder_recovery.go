package data

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/go-kratos/kratos/v3/log"
)

func (r *recorderRepo) finalizeSegments(ctx context.Context, metaPath string, meta *sessionMeta) error {
	dir := filepath.Dir(metaPath)
	allOK := true
	for i := range meta.Segments {
		seg := &meta.Segments[i]
		if seg.RemuxStatus == remuxStatusOK {
			continue
		}
		if !r.d.remuxEnabled {
			seg.RemuxStatus = remuxStatusOK
			seg.FLVKept = true
			r.persistMeta(metaPath, meta)
			continue
		}
		flvPath := filepath.Join(dir, seg.Video)
		mp4Name := strings.TrimSuffix(seg.Video, ".flv") + ".mp4"
		mp4Path := filepath.Join(dir, mp4Name)

		if _, err := os.Stat(flvPath); err != nil {
			if _, merr := os.Stat(mp4Path); merr == nil {
				seg.RemuxStatus = remuxStatusOK
				seg.Video = mp4Name
				seg.FLVKept = false
			} else {
				seg.RemuxStatus = remuxStatusFailed
				seg.RemuxError = "source flv missing"
				allOK = false
			}
			r.persistMeta(metaPath, meta)
			continue
		}

		if err := remuxWithRetry(ctx, r.d.ffmpegPath, flvPath, mp4Path, meta.Title, meta.RoomName, meta.LiveStartTime); err != nil {
			seg.RemuxStatus = remuxStatusFailed
			seg.RemuxError = err.Error()
			seg.FLVKept = true
			allOK = false
			log.Error("remux failed, keeping flv", "file", flvPath, "err", err)
		} else if fi, serr := os.Stat(mp4Path); serr != nil || fi.Size() == 0 {
			seg.RemuxStatus = remuxStatusFailed
			seg.RemuxError = "remux output missing or empty"
			seg.FLVKept = true
			allOK = false
		} else {
			_ = os.Remove(flvPath)
			seg.RemuxStatus = remuxStatusOK
			seg.Video = mp4Name
			seg.FLVKept = false
		}
		r.persistMeta(metaPath, meta)
	}
	if allOK {
		meta.Status = metaStatusDone
	} else {
		meta.Status = metaStatusPartial
	}
	return r.persistMeta(metaPath, meta)
}

// RecoverPending 扫描录制根目录下的所有 meta.json，完成上次运行遗留
// 的转封装工作。
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
		case metaStatusRemuxing, metaStatusRecording:
			log.Info("recovering unfinished session", "path", path, "status", meta.Status)
			meta.Status = metaStatusRemuxing
			if meta.EndTime == 0 {
				meta.EndTime = time.Now().Unix()
			}
			r.persistMeta(path, meta)
			if err := r.finalizeSegments(ctx, path, meta); err != nil {
				log.Warn("recover: finalize failed", "path", path, "err", err)
			}
		case metaStatusPartial, metaStatusDone:
			if hasRetryableSegments(meta) {
				log.Info("retrying failed segments", "path", path)
				if err := r.finalizeSegments(ctx, path, meta); err != nil {
					log.Warn("recover: finalize failed", "path", path, "err", err)
				}
			}
		}
	}
	return nil
}
