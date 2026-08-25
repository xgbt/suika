package recorder

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
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

	// defaultMaxSegmentBytes 分段大小上限（2.5 GiB，对齐 biliup 默认值）：
	// 原画长直播的单段体积和崩溃时的损失半径由此封顶，与时长上限取或。
	defaultMaxSegmentBytes int64 = 2_684_354_560
)

// recorderRepo 实现 biz.RecorderRepo：录制目录布局、FLV 拉流写入、meta.json 簿记与收尾合并。
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
	dir, base, err := sessionPaths(r.recordRoot, session)
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
	baseBytes := stats.bytesWritten()
	stats.setCurrentFile("")

	// 把 CDN 实际授予的清晰度记入 meta
	r.updateMeta(metaPath, func(meta *sessionMeta) {
		meta.Quality = qualityMeta{Qn: stream.Quality.Qn, Desc: stream.Quality.Desc}
		meta.Title = session.Title
	})

	loop := newRecordSessionLoop(r, session, dir, base, metaPath, header, stats, baseBytes)
	tagCh := startTagReader(stream.Body)

	var (
		lastSampleAt = time.Now()
		lastSample   int64
	)
	health := time.NewTicker(r.healthInterval)
	defer health.Stop()
	speedSampler := time.NewTicker(time.Second)
	defer speedSampler.Stop()

	for {
		select {
		// 关闭录制开关、停机、房间被删除
		case <-ctx.Done():
			loop.drainPending()
			loop.closeSegment()
			return &loop.result, ctx.Err()
		// 读取 tag
		case tr := <-tagCh:
			// 读取 tag 失败，可能是流断开或其他瞬时错误
			if tr.err != nil {
				loop.drainPending()
				loop.closeSegment()
				// EOF 表示流干净结束
				if tr.err == io.EOF {
					return &loop.result, nil
				}
				// 其他瞬时错误，则返回，让上层决定是否重连
				return &loop.result, fmt.Errorf("%w: %v", biz.ErrStreamTransient, tr.err)
			}

			tag := tr.tag
			// 下载速度统计基于实际接收流量（receiveBytes），而非块裁决后的落盘字节（writtenBytes），
			// 避免去重/缓冲导致的写盘脉冲把速度采样打成 0。
			loop.receiveBytes += int64(len(tag.Data)) + tagEnvelopeOverhead
			if loop.seg == nil {
				// 新段等待首个视频关键帧再开文件：关键帧之前的标签丢弃
				// （头标签仍照常入缓存，供开段注入），保证段首即关键帧、
				// 独立可解码；纯音频流没有视频关键帧，豁免等待。
				if header.HasVideo && !tag.IsVideoKeyframe() {
					loop.headers.absorb(tag)
					continue
				}
				if err := loop.openNewSegment(); err != nil {
					r.appendMetaError(metaPath, "record", err)
					return &loop.result, err
				}
			} else if loop.guard.boundary(tag) {
				// 块边界：先裁决缓冲块落盘，切段判定才能用上最新的段状态。
				if err := loop.flushBlock(); err != nil {
					loop.closeSegment()
					return &loop.result, err
				}
			}

			// 切段判定在块裁决之后；强切路径（超限/序列头变化）同样要先
			// 结束缓冲块，避免关段时把在途数据留在缓冲里丢失。
			if r.shouldSplit(loop.seg, tag) {
				if err := loop.flushBlock(); err != nil {
					loop.closeSegment()
					return &loop.result, err
				}
				loop.closeSegment()
				if err := loop.openNewSegment(); err != nil {
					r.appendMetaError(metaPath, "record", err)
					return &loop.result, err
				}
			} else if loop.headers.changed(tag) {
				// 流中途序列头变化（CDN 换源、主播改码率）：继续写入旧分
				// 段会把两种解码配置拼进同一个文件，强制切段。新段按既有
				// 规则从缓存注入旧头标签，新序列头作为首个正文标签紧随其
				// 后写入，播放器以最新的序列头为准。
				log.Warn("sequence header changed, splitting segment",
					"room", session.RoomID, "part", loop.seg.part)
				if err := loop.flushBlock(); err != nil {
					loop.closeSegment()
					return &loop.result, err
				}
				loop.closeSegment()
				if err := loop.openNewSegment(); err != nil {
					r.appendMetaError(metaPath, "record", err)
					return &loop.result, err
				}
			}
			// 头标签只在开/切分段决策之后才入缓存：触发新分段的那个
			// 标签不能从缓存重注入，否则会被写两次（openSegment 写一次、
			// 下面的拉流写入又一次）。切分前已见过的头标签仍会完整重注入。
			loop.headers.absorb(tag)
			loop.guard.add(tag)
		// 弹幕/礼物等事件写入
		case ev := <-events:
			if loop.seg == nil {
				continue
			}
			if err := loop.seg.writeEvent(ev); err != nil {
				log.Warn("danmaku write failed", "room", session.RoomID, "err", err)
				// 尽力而为, 不影响录制主流程
			}
		// 下载速度采样
		case <-speedSampler.C:
			now := time.Now()
			delta := max(loop.receiveBytes-lastSample, 0)
			elapsed := now.Sub(lastSampleAt)
			if elapsed <= 0 {
				continue
			}
			stats.setDownloadSpeed(int64(float64(delta) / elapsed.Seconds()))
			lastSample = loop.receiveBytes
			lastSampleAt = now
		// 健康检查：在 healthInterval 内未见新数据则计为一次失败，连续 failRounds 次则判定录制异常。
		case <-health.C:
			if loop.writtenBytes > loop.lastGrowth {
				loop.lastGrowth = loop.writtenBytes
				loop.failRounds = 0
				continue
			}
			loop.failRounds++
			if loop.failRounds >= r.healthFailRounds {
				loop.closeSegment()
				return &loop.result, fmt.Errorf("recording unhealthy: no new data for %d rounds", loop.failRounds)
			}
		}
	}
}

type tagRead struct {
	tag *flv.Tag
	err error
}

func startTagReader(body io.Reader) <-chan tagRead {
	tagCh := make(chan tagRead, 512)
	go func() {
		for {
			tag, err := flv.ReadTag(body)
			tagCh <- tagRead{tag: tag, err: err}
			if err != nil {
				return
			}
		}
	}()
	return tagCh
}

type recordSessionLoop struct {
	repo    *recorderRepo
	session *biz.RecordingSession
	dir     string
	base    string
	meta    string
	header  *flv.FileHeader

	stats     *pumpStats
	baseBytes int64

	headers segmentHeaders
	guard   dupGuard
	seg     *segmentFile

	result       biz.RecordingResult
	receiveBytes int64 // 本次会话累计接收字节（网络接收口径，用于下载速度采样）
	writtenBytes int64 // 本次会话累计写盘字节（落盘口径，用于 bytes_written 与健康检查）
	lastGrowth   int64
	failRounds   int
}

func newRecordSessionLoop(
	repo *recorderRepo,
	session *biz.RecordingSession,
	dir, base, metaPath string,
	header *flv.FileHeader,
	stats *pumpStats,
	baseBytes int64,
) *recordSessionLoop {
	return &recordSessionLoop{
		repo:      repo,
		session:   session,
		dir:       dir,
		base:      base,
		meta:      metaPath,
		header:    header,
		stats:     stats,
		baseBytes: baseBytes,
	}
}

func (l *recordSessionLoop) openNewSegment() error {
	// 编号探测和 O_TRUNC 创建必须串行，否则并发的录制泵会同时选中
	// 同一个 part，并截断彼此刚写入的分段。
	l.repo.segmentMu.Lock()
	defer l.repo.segmentMu.Unlock()

	part := nextPartNumber(l.dir, l.base)
	seg, err := openSegment(l.dir, l.base, part, l.header, &l.headers)
	if err != nil {
		return err
	}

	l.seg = seg
	l.result.Parts++
	l.stats.setCurrentFile(seg.videoPath)

	// 注入的头标签同样是本场次的实际写入字节（等待关键帧后 part1 的
	// 头标签走注入而非泵送；切分段每段重注入），计入写入进度；
	// FLV 文件头本身不计，与既有口径一致。
	l.headers.forEachReinject(func(ht *flv.Tag) {
		l.addWrittenBytes(int64(len(ht.Data)) + tagEnvelopeOverhead)
	})

	l.repo.appendSegmentMeta(l.meta, seg)
	log.Info("segment opened", "room", l.session.RoomID, "part", part, "file", seg.videoPath)
	return nil
}

func (l *recordSessionLoop) closeSegment() {
	if l.seg == nil {
		return
	}
	if err := l.seg.close(); err != nil {
		log.Error("close segment failed", "room", l.session.RoomID, "file", l.seg.videoPath, "err", err)
	}
	l.repo.finishSegmentMeta(l.meta, l.seg)
	l.seg = nil
}

func (l *recordSessionLoop) flushBlock() error {
	buf, disconnect := l.guard.close()
	if disconnect {
		return fmt.Errorf("%w: cdn looping duplicate stream data", biz.ErrStreamTransient)
	}
	if buf == nil {
		l.failRounds = 0
		log.Warn("duplicate stream block dropped", "room", l.session.RoomID, "streak", l.guard.streak)
		return nil
	}
	for _, bt := range buf {
		if err := l.writeTag(bt, true); err != nil {
			return err
		}
	}
	return nil
}

func (l *recordSessionLoop) drainPending() {
	for _, bt := range l.guard.takeAll() {
		if err := l.writeTag(bt, false); err != nil {
			log.Warn("drain pending block failed", "room", l.session.RoomID, "err", err)
			return
		}
	}
}

func (l *recordSessionLoop) writeTag(tag *flv.Tag, persistError bool) error {
	n, err := l.seg.writeTag(tag)
	l.addWrittenBytes(n)
	if err != nil && persistError {
		l.repo.appendMetaError(l.meta, "record", err)
	}
	return err
}

func (l *recordSessionLoop) addWrittenBytes(n int64) {
	l.writtenBytes += n
	l.result.BytesWritten = l.writtenBytes
	l.stats.setBytesWritten(l.baseBytes + l.writtenBytes)
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
