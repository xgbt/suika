package data

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	"suika/internal/biz"
	"suika/internal/conf"
	"suika/internal/data/flv"

	"github.com/go-kratos/kratos/v3/log"
)

const (
	// 默认录制目录
	defaultRecordRoot = "./recordings"
	// 默认分段时长（分钟），为 0 时不切分
	defaultSegmentMinutes = 120
	// 默认健康检查间隔，录制守护进程在该间隔内未见新数据则计为一次失败。
	defaultHealthInterval = 60 * time.Second
	// 默认健康检查失败轮数，连续失败达到该轮数则判定录制异常。
	defaultHealthRounds = 3
	// splitOverrun 限定分段在等待关键帧切点时最多超出目标时长多久。
	splitOverrun = 15 * time.Second
	// maxTitleLen 限定 meta.json 中 title 字段的最大长度，超过则截断。
	maxTitleLen = 64
	// maxNameLen 限定 meta.json 中 room_name 字段的最大长度，超过则截断。
	maxNameLen = 32
)

// recorderRepo 实现 biz.RecorderRepo：录制目录布局、FLV 拉流写入、meta.json 簿记与转封装。
type recorderRepo struct {
	d *Data

	// recordRoot 录制根目录
	recordRoot string
	// segmentDuration 分段时长，为 0 时不切分
	segmentDuration time.Duration
	// healthInterval 健康检查间隔，录制守护进程在该间隔内未见新数据则计为一次失败。
	healthInterval time.Duration
	// healthFailRounds 连续健康检查失败轮数，达到该轮数则判定录制异常。
	healthFailRounds int

	mu    sync.Mutex
	stats map[int64]*pumpStats
}

func NewRecorderRepo(d *Data, c *conf.Recorder) biz.RecorderRepo {
	r := &recorderRepo{
		d:                d,
		recordRoot:       defaultRecordRoot,
		segmentDuration:  defaultSegmentMinutes * time.Minute,
		healthInterval:   defaultHealthInterval,
		healthFailRounds: defaultHealthRounds,
		stats:            make(map[int64]*pumpStats),
	}
	if c == nil {
		return r
	}
	if c.GetRecordRoot() != "" {
		r.recordRoot = c.GetRecordRoot()
	}
	if c.SegmentMinutes != nil {
		r.segmentDuration = time.Duration(c.GetSegmentMinutes()) * time.Minute
	}
	if rc := c.GetReconnect(); rc != nil {
		if rc.GetHealthCheckInterval() != nil {
			r.healthInterval = rc.GetHealthCheckInterval().AsDuration()
		}
		if rc.GetHealthCheckFailRounds() > 0 {
			r.healthFailRounds = int(rc.GetHealthCheckFailRounds())
		}
	}
	return r
}

// NewSessionStatsRepo 将 RecorderRepo 转为 biz.SessionStatsRepo。
func NewSessionStatsRepo(repo biz.RecorderRepo) biz.SessionStatsRepo {
	return repo.(biz.SessionStatsRepo)
}

// PrepareSession 创建（或在重启后重新定位）会话目录和 meta.json。
func (r *recorderRepo) PrepareSession(ctx context.Context, session *biz.Session) error {

	// 获取目录和文件名前缀
	dir, base, err := r.sessionPaths(session)
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
	ps.bytes.Store(0)
	ps.file.Store("")

	// 读取 meta.json，
	metaPath := filepath.Join(dir, base+".meta.json")
	if meta, err := loadMeta(metaPath); err == nil {
		// 之前已经录制过，已存在 meta.json, 更新 meta.json 的状态为 recording, 并更新标题和房间名
		meta.Status = metaStatusRecording
		meta.Title = session.Title
		meta.RoomName = session.RoomName
		return saveMeta(metaPath, meta)
	}

	// 目录下不存在 meta.json 时，创建新的 meta.json
	start := session.LiveStartTime
	if start.IsZero() {
		start = time.Now()
	}
	meta := &sessionMeta{
		RoomID:        session.RoomID,
		RoomName:      session.RoomName,
		Title:         session.Title,
		LiveStartTime: start.Unix(),
		Status:        metaStatusRecording,
	}
	return saveMeta(metaPath, meta)
}

// RecordSession 将直播流写入磁盘（按配置切分分段）并把弹幕事件写入对应的 JSONL 文件。
func (r *recorderRepo) RecordSession(ctx context.Context, session *biz.Session, stream *biz.StreamHandle, events <-chan *biz.DanmakuEvent) (*biz.SessionResult, error) {
	if stream == nil || stream.Body == nil {
		return nil, biz.ErrRoomInternal
	}
	defer stream.Body.Close()

	dir, base, err := r.sessionPaths(session)
	if err != nil {
		return nil, err
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	metaPath := filepath.Join(dir, base+".meta.json")

	header, err := flv.ParseHeader(stream.Body)
	if err != nil {
		return nil, fmt.Errorf("%w: %v", biz.ErrStreamTransient, err)
	}

	stats := r.statsFor(session.RoomID)
	baseBytes := stats.bytes.Load()
	stats.file.Store("")

	// 把授予的清晰度记入 meta。
	r.updateMeta(metaPath, func(meta *sessionMeta) {
		meta.Quality = qualityMeta{Qn: stream.Quality.Qn, Desc: stream.Quality.Desc}
		meta.Title = session.Title
	})

	// 拉流写入循环：按分段时长切分，写入 meta.json。
	type tagRead struct {
		tag *flv.Tag
		err error
	}
	tagCh := make(chan tagRead, 512)
	go func() {
		for {
			tag, err := flv.ReadTag(stream.Body)
			tagCh <- tagRead{tag, err}
			if err != nil {
				return
			}
		}
	}()

	var (
		cache        headerCache
		seg          *segmentFile
		result       biz.SessionResult
		sessionBytes int64
		lastGrowth   int64
		failRounds   int
	)
	health := time.NewTicker(r.healthInterval)
	defer health.Stop()

	// openNewSegment 打开新分段文件，写入头标签并更新 meta.json。
	openNewSegment := func() error {
		part := nextPartNumber(dir, base)
		newSeg, err := openSegment(dir, base, part, header, &cache)
		if err != nil {
			return err
		}
		seg = newSeg
		result.Parts++
		stats.file.Store(seg.videoPath)
		r.appendSegmentMeta(metaPath, seg)
		log.Info("segment opened", "room", session.RoomID, "part", part, "file", seg.videoPath)
		return nil
	}

	// closeSegment 关闭当前分段文件，写入尾标签并更新 meta.json。
	closeSegment := func() {
		if seg == nil {
			return
		}
		if err := seg.close(); err != nil {
			log.Error("close segment failed", "room", session.RoomID, "file", seg.videoPath, "err", err)
		}
		r.finishSegmentMeta(metaPath, seg)
		seg = nil
	}

	for {
		select {
		case <-ctx.Done():
			closeSegment()
			return &result, ctx.Err()
		case tr := <-tagCh:
			if tr.err != nil {
				closeSegment()
				if tr.err == io.EOF {
					return &result, nil
				}
				return &result, fmt.Errorf("%w: %v", biz.ErrStreamTransient, tr.err)
			}
			tag := tr.tag
			if seg == nil {
				if err := openNewSegment(); err != nil {
					r.appendMetaError(metaPath, "record", err)
					return &result, err
				}
			} else if r.shouldSplit(seg, tag) {
				closeSegment()
				if err := openNewSegment(); err != nil {
					r.appendMetaError(metaPath, "record", err)
					return &result, err
				}
			}
			// 头标签只在开/切分段决策之后才入缓存：触发新分段的那个
			// 标签不能从缓存重注入，否则会被写两次（openSegment 写一次、
			// 下面的拉流写入又一次）。切分前已见过的头标签仍会完整重注入。
			switch {
			case tag.IsMetadata():
				cache.metadata = tag
			case tag.IsAVCSequenceHeader():
				cache.videoSeq = tag
			case tag.IsAACSequenceHeader():
				cache.audioSeq = tag
			}
			n, err := seg.writeTag(tag)
			sessionBytes += n
			result.BytesWritten = sessionBytes
			stats.bytes.Store(baseBytes + sessionBytes)
			if err != nil {
				closeSegment()
				r.appendMetaError(metaPath, "record", err)
				return &result, err
			}
		case ev := <-events:
			if seg == nil {
				continue
			}
			if err := seg.writeEvent(ev); err != nil {
				log.Warn("danmaku write failed", "room", session.RoomID, "err", err)
			}
		case <-health.C:
			if sessionBytes > lastGrowth {
				lastGrowth = sessionBytes
				failRounds = 0
				continue
			}
			failRounds++
			if failRounds >= r.healthFailRounds {
				closeSegment()
				return &result, fmt.Errorf("recording unhealthy: no new data for %d rounds", failRounds)
			}
		}
	}
}

// shouldSplit 判断下一个 tag 是否应开启新分段：达到目标时长且该 tag
// 是关键帧，或者超时预算耗尽（强制切分）。
func (r *recorderRepo) shouldSplit(seg *segmentFile, tag *flv.Tag) bool {
	if r.segmentDuration <= 0 || !seg.hasStart {
		return false
	}
	elapsed := time.Duration(tag.Timestamp-seg.startTs) * time.Millisecond
	if elapsed < r.segmentDuration {
		return false
	}
	return tag.IsVideoKeyframe() || elapsed >= r.segmentDuration+splitOverrun
}

// FinishSession 收尾 meta.json 并对所有已录分段执行转封装。
func (r *recorderRepo) FinishSession(ctx context.Context, session *biz.Session) error {
	dir, base, err := r.sessionPaths(session)
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
	meta.Status = metaStatusRemuxing
	meta.EndTime = time.Now().Unix()
	meta.Title = session.Title
	meta.Quality = qualityMeta{Qn: session.Quality.Qn, Desc: session.Quality.Desc}
	err = saveMeta(metaPath, meta)
	r.mu.Unlock()
	if err != nil {
		return err
	}
	return r.finalizeSegments(ctx, metaPath, meta)
}

// --- 辅助函数 ---

// sessionPaths 计算会话目录和文件名前缀（所有分段与 meta 文件共享的日期/时间/标题前缀）。
//
// 目录结构示例：
//
//	recordings/
//	└── 12345_主播名/
//	    └── 2024-06-01/
//	        ├── 20240601_1504_直播标题.meta.json
//	        ├── 20240601_1504_直播标题_part1.flv
//	        └── 20240601_1504_直播标题_part1.danmu.jsonl
//
// 返回值：
//   - dir  : recordings/12345_主播名/2024-06-01
//   - base : 20240601_1504_直播标题
func (r *recorderRepo) sessionPaths(session *biz.Session) (dir string, base string, err error) {
	if session == nil || session.RoomID <= 0 {
		return "", "", biz.ErrRoomInternal
	}

	start := session.LiveStartTime
	if start.IsZero() {
		start = time.Now()
	}

	roomDir := fmt.Sprintf("%d_%s", session.RoomID, sanitizeSegment(session.RoomName, maxNameLen))
	dir = filepath.Join(r.recordRoot, roomDir, start.Format("2006-01-02"))
	base = start.Format("20060102_1504") + "_" + sanitizeSegment(session.Title, maxTitleLen)
	return
}

var (
	// unsafeChars 匹配文件名不安全字符：控制字符、路径分隔符及 Unicode 空白，
	// + 折叠连续匹配为单个下划线。
	unsafeChars       = regexp.MustCompile(`[\x00-\x1f\x7f\\/:*?"<>|\s\p{Z}]+`)
	partSuffixPattern = regexp.MustCompile(`_part(\d+)\.(flv|mp4)$`)
)

// sanitizeSegment 清理文件名, 替换不安全字符、截断过长片段
func sanitizeSegment(s string, max int) string {
	s = strings.Trim(unsafeChars.ReplaceAllString(s, "_"), "_")
	if runes := []rune(s); len(runes) > max {
		s = strings.TrimRight(string(runes[:max]), "_")
	}
	if s == "" {
		return "untitled"
	}
	return s
}

// nextPartNumber 扫描会话目录推导下一个分段编号
// 同时覆盖重连和崩溃重启两种情况
func nextPartNumber(dir, base string) int {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return 1
	}
	maxPart := 0
	prefix := base + "_part"
	for _, e := range entries {
		name := e.Name()
		if !strings.HasPrefix(name, prefix) {
			continue
		}
		m := partSuffixPattern.FindStringSubmatch(name)
		if m == nil {
			continue
		}
		if n, err := strconv.Atoi(m[1]); err == nil && n > maxPart {
			maxPart = n
		}
	}
	return maxPart + 1
}

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
