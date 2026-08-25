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
	defaultRecordRoot     = "./recordings"   // 默认录制目录
	defaultSegmentMinutes = 120              // 分段时长（分钟）
	defaultHealthInterval = 10 * time.Second // 健康检查间隔，录制守护进程在该间隔内未见新数据则计为一次失败
	defaultHealthRounds   = 3                // 健康检查失败轮数，连续失败达到该轮数则判定录制异常
	splitOverrun          = 15 * time.Second // 分段在等待关键帧切点时最多超出目标时长
	maxTitleLen           = 64               // meta.json 中 title 字段的最大长度，超过则截断
	maxNameLen            = 32               // meta.json 中 room_name 字段的最大长度，超过则截断

	// defaultMaxSegmentBytes 分段大小上限（2.5 GiB，对齐 biliup 默认值）：
	// 原画长直播的单段体积和崩溃时的损失半径由此封顶，与时长上限取或。
	defaultMaxSegmentBytes int64 = 2_684_354_560
	// sizeSplitOverrunDivisor 大小切分等待关键帧的强切裕度：超出阈值
	// 1/该值 仍未等到关键帧则强制切分（GOP 增量相对 GiB 级阈值可忽略，
	// 裕度只是无关键帧病态流的保险）。
	sizeSplitOverrunDivisor = 10
)

// recorderRepo 实现 biz.RecorderRepo：录制目录布局、FLV 拉流写入、meta.json 簿记与收尾合并。
type recorderRepo struct {
	d *Data

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

func NewRecorderRepo(d *Data, c *conf.Recorder) biz.RecorderRepo {
	r := &recorderRepo{
		d:                d,
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

// PrepareSession 创建（或在重启后重新定位）会话目录和 meta.json。
func (r *recorderRepo) PrepareSession(ctx context.Context, session *biz.RecordingSession) error {

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
	ps.speed.Store(0)

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
func (r *recorderRepo) RecordSession(ctx context.Context, session *biz.RecordingSession, stream *biz.LiveStream, events <-chan *biz.DanmakuEvent) (*biz.RecordingResult, error) {
	if stream == nil || stream.Body == nil {
		return nil, biz.ErrRoomInternal
	}
	defer stream.Body.Close()

	// 获取会话目录和文件名前缀
	dir, base, err := r.sessionPaths(session)
	if err != nil {
		return nil, err
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	metaPath := filepath.Join(dir, base+".meta.json")

	// 读取 FLV 文件头
	header, err := flv.ParseHeader(stream.Body)
	if err != nil {
		return nil, fmt.Errorf("%w: %v", biz.ErrStreamTransient, err)
	}

	// 记录当前会话的写入进度
	stats := r.statsFor(session.RoomID)
	// 记录本次录制之前的写入进度
	baseBytes := stats.bytes.Load()
	stats.file.Store("")

	// 把 CDN 实际授予的清晰度记入 meta
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
		guard        dupGuard
		seg          *segmentFile
		result       biz.RecordingResult
		sessionBytes int64
		lastGrowth   int64
		lastSampleAt = time.Now()
		lastSample   int64
		failRounds   int
	)
	health := time.NewTicker(r.healthInterval)
	defer health.Stop()
	speedSampler := time.NewTicker(time.Second)
	defer speedSampler.Stop()

	// openNewSegment 打开新分段文件，写入头标签并更新 meta.json。
	openNewSegment := func() error {
		// 编号探测和 O_TRUNC 创建必须串行，否则并发的录制泵会同时选中
		// 同一个 part，并截断彼此刚写入的分段。
		r.segmentMu.Lock()
		defer r.segmentMu.Unlock()

		part := nextPartNumber(dir, base)
		newSeg, err := openSegment(dir, base, part, header, &cache)
		if err != nil {
			return err
		}
		seg = newSeg
		result.Parts++
		stats.file.Store(seg.videoPath)
		// 注入的头标签同样是本场次的实际写入字节（等待关键帧后 part1 的
		// 头标签走注入而非泵送；切分段每段重注入），计入写入进度；
		// FLV 文件头本身不计，与既有口径一致。
		for _, ht := range []*flv.Tag{cache.metadata, cache.videoSeq, cache.audioSeq} {
			if ht == nil {
				continue
			}
			sessionBytes += int64(len(ht.Data)) + tagEnvelopeOverhead
		}
		result.BytesWritten = sessionBytes
		stats.bytes.Store(baseBytes + sessionBytes)
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

	// flushBlock 结束当前缓冲块并裁决：指纹唯一 → 整块写入当前分段；
	// 重复 → 整块丢弃并重置健康失败轮数（流还活着，只是在循环）；连续
	// 重复达到上限 → 返回包装为 ErrStreamTransient 的错误，由断流决策树
	// 换流地址重连（换 CDN 节点）。写错误记入 meta.json。
	flushBlock := func() error {
		buf, disconnect := guard.close()
		if disconnect {
			return fmt.Errorf("%w: cdn looping duplicate stream data", biz.ErrStreamTransient)
		}
		if buf == nil {
			failRounds = 0
			log.Warn("duplicate stream block dropped", "room", session.RoomID, "streak", guard.streak)
			return nil
		}
		for _, bt := range buf {
			n, werr := seg.writeTag(bt)
			sessionBytes += n
			result.BytesWritten = sessionBytes
			stats.bytes.Store(baseBytes + sessionBytes)
			if werr != nil {
				r.appendMetaError(metaPath, "record", werr)
				return werr
			}
		}
		return nil
	}

	// drainPending 把未关闭的缓冲块原样写入（不做重复裁决），用于流结
	// 束/中止时尽量保留已收到的数据。
	drainPending := func() {
		for _, bt := range guard.takeAll() {
			n, werr := seg.writeTag(bt)
			sessionBytes += n
			result.BytesWritten = sessionBytes
			stats.bytes.Store(baseBytes + sessionBytes)
			if werr != nil {
				log.Warn("drain pending block failed", "room", session.RoomID, "err", werr)
				return
			}
		}
	}

	for {
		select {
		// 关闭录制开关、停机、房间被删除
		case <-ctx.Done():
			drainPending()
			closeSegment()
			return &result, ctx.Err()
		// 读取 tag
		case tr := <-tagCh:
			// 读取 tag 失败，可能是流断开或其他瞬时错误
			if tr.err != nil {
				drainPending()
				closeSegment()
				// EOF 表示流干净结束
				if tr.err == io.EOF {
					return &result, nil
				}
				// 其他瞬时错误，则返回，让上层决定是否重连
				return &result, fmt.Errorf("%w: %v", biz.ErrStreamTransient, tr.err)
			}

			tag := tr.tag
			if seg == nil {
				// 新段等待首个视频关键帧再开文件：关键帧之前的标签丢弃
				// （头标签仍照常入缓存，供开段注入），保证段首即关键帧、
				// 独立可解码；纯音频流没有视频关键帧，豁免等待。
				if header.HasVideo && !tag.IsVideoKeyframe() {
					cacheHeaderTag(&cache, tag)
					continue
				}
				if err := openNewSegment(); err != nil {
					r.appendMetaError(metaPath, "record", err)
					return &result, err
				}
			} else if guard.boundary(tag) {
				// 块边界：先裁决缓冲块落盘，切段判定才能用上最新的段状态。
				if err := flushBlock(); err != nil {
					closeSegment()
					return &result, err
				}
			}

			// 切段判定在块裁决之后；强切路径（超限/序列头变化）同样要先
			// 结束缓冲块，避免关段时把在途数据留在缓冲里丢失。
			if r.shouldSplit(seg, tag) {
				if err := flushBlock(); err != nil {
					closeSegment()
					return &result, err
				}
				closeSegment()
				if err := openNewSegment(); err != nil {
					r.appendMetaError(metaPath, "record", err)
					return &result, err
				}
			} else if headerChanged(&cache, tag) {
				// 流中途序列头变化（CDN 换源、主播改码率）：继续写入旧分
				// 段会把两种解码配置拼进同一个文件，强制切段。新段按既有
				// 规则从缓存注入旧头标签，新序列头作为首个正文标签紧随其
				// 后写入，播放器以最新的序列头为准。
				log.Warn("sequence header changed, splitting segment",
					"room", session.RoomID, "part", seg.part)
				if err := flushBlock(); err != nil {
					closeSegment()
					return &result, err
				}
				closeSegment()
				if err := openNewSegment(); err != nil {
					r.appendMetaError(metaPath, "record", err)
					return &result, err
				}
			}
			// 头标签只在开/切分段决策之后才入缓存：触发新分段的那个
			// 标签不能从缓存重注入，否则会被写两次（openSegment 写一次、
			// 下面的拉流写入又一次）。切分前已见过的头标签仍会完整重注入。
			cacheHeaderTag(&cache, tag)
			guard.add(tag)
		// 弹幕/礼物等事件写入
		case ev := <-events:
			if seg == nil {
				continue
			}
			if err := seg.writeEvent(ev); err != nil {
				log.Warn("danmaku write failed", "room", session.RoomID, "err", err)
				// 尽力而为, 不影响录制主流程
			}
		// 下载速度采样
		case <-speedSampler.C:
			now := time.Now()
			delta := max(sessionBytes-lastSample, 0)
			elapsed := now.Sub(lastSampleAt)
			if elapsed <= 0 {
				continue
			}
			stats.speed.Store(int64(float64(delta) / elapsed.Seconds()))
			lastSample = sessionBytes
			lastSampleAt = now
		// 健康检查：在 healthInterval 内未见新数据则计为一次失败，连续 failRounds 次则判定录制异常。
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

// shouldSplit 判断下一个 tag 是否应开启新分段。两个独立触发条件，都优先
// 等待视频关键帧以保证分段可独立播放：
//  1. 大小：已写字节达到上限，且该 tag 是关键帧；或超出上限的
//     1/sizeSplitOverrunDivisor 裕度仍无关键帧则强制切分；
//  2. 时长：达到目标时长且该 tag 是关键帧；或超出 splitOverrun 强制切分。
func (r *recorderRepo) shouldSplit(seg *segmentFile, tag *flv.Tag) bool {
	if !seg.hasStart {
		return false
	}
	if r.maxSegmentBytes > 0 && seg.bytes >= r.maxSegmentBytes {
		overrun := r.maxSegmentBytes / sizeSplitOverrunDivisor
		if tag.IsVideoKeyframe() || seg.bytes >= r.maxSegmentBytes+overrun {
			return true
		}
	}
	if r.segmentDuration <= 0 {
		return false
	}
	elapsed := time.Duration(tag.Timestamp-seg.startTs) * time.Millisecond
	if elapsed < r.segmentDuration {
		return false
	}
	return tag.IsVideoKeyframe() || elapsed >= r.segmentDuration+splitOverrun
}

// FinishSession 收尾 meta.json 并合并所有已录分段。
func (r *recorderRepo) FinishSession(ctx context.Context, session *biz.RecordingSession) error {
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
func (r *recorderRepo) sessionPaths(session *biz.RecordingSession) (dir string, base string, err error) {
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

	if !r.d.mergeEnabled {
		for i := range meta.Segments {
			meta.Segments[i].FLVKept = true
		}
		meta.Status = metaStatusDone
		return r.persistMeta(metaPath, meta)
	}

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
