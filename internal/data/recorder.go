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
	dir, base, err := r.sessionPaths(session)
	if err != nil {
		return err
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return err
	}
	metaPath := filepath.Join(dir, base+".meta.json")

	r.mu.Lock()
	defer r.mu.Unlock()

	// 会话（重新）启动时把写入进度清零，与 RoomRegistry.StartRecording
	// 重置 sessionStartedAt/lastError 相对应：否则 RecordSession 的 baseBytes
	// 续算逻辑会在进程存活期间，把同一房间新一轮开播的字节数累加到
	// 上一场会话的总数上。重启续录不受影响——统计在内存中，新进程里
	// 本来就是零。
	ps, ok := r.stats[session.RoomID]
	if !ok {
		ps = &pumpStats{}
		r.stats[session.RoomID] = ps
	}
	ps.bytes.Store(0)
	ps.file.Store("")

	if meta, err := loadMeta(metaPath); err == nil {
		// 重启续录：同一会话目录，保留已录分段。
		meta.Status = metaStatusRecording
		if session.Title != "" {
			meta.Title = session.Title
		}
		if session.RoomName != "" {
			meta.RoomName = session.RoomName
		}
		return saveMeta(metaPath, meta)
	}

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
		if session.Title != "" {
			meta.Title = session.Title
		}
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
	if session.Title != "" {
		meta.Title = session.Title
	}
	meta.Quality = qualityMeta{Qn: session.Quality.Qn, Desc: session.Quality.Desc}
	err = saveMeta(metaPath, meta)
	r.mu.Unlock()
	if err != nil {
		return err
	}
	return r.finalizeSegments(ctx, metaPath, meta)
}

// --- 辅助函数 ---

// sessionPaths 计算会话目录和文件名基座（所有分段与 meta 文件共享的日期/时间/标题前缀）。
// 样例: recordings/12345_主播名/2024-06-01/20240601_1504_直播标题
// 返回 (dir, base, error)
// dir : recordings/12345_主播名/2024-06-01
// base : 20240601_1504_直播标题
func (r *recorderRepo) sessionPaths(session *biz.Session) (string, string, error) {
	if session == nil || session.RoomID <= 0 {
		return "", "", biz.ErrRoomInternal
	}
	start := session.LiveStartTime
	if start.IsZero() {
		start = time.Now()
	}
	roomDir := fmt.Sprintf("%d_%s", session.RoomID, sanitizeSegment(session.RoomName, maxNameLen))
	dir := filepath.Join(r.recordRoot, roomDir, start.Format("2006-01-02"))
	base := start.Format("20060102_1504") + "_" + sanitizeSegment(session.Title, maxTitleLen)
	return dir, base, nil
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
