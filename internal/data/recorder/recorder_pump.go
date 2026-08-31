package recorder

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"

	"suika/internal/biz"
	"suika/internal/data/flv"

	"github.com/go-kratos/kratos/v3/log"
)

// RecordSession 将直播流写入磁盘（按配置切分分段），并把弹幕事件写入对应
// 的 JSONL 文件，直到流结束或 ctx 被取消。
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
		meta.Quality = qualityMeta(stream.Quality)
		meta.Title = session.Title
	})

	loop := newRecordSessionLoop(r, session, dir, base, metaPath, header, stats, baseBytes)
	tagCh := startTagReader(stream.Body)

	health := time.NewTicker(r.healthInterval)
	defer health.Stop()
	speedSampler := time.NewTicker(time.Second)
	defer speedSampler.Stop()

	for {
		select {
		case <-ctx.Done(): // 关闭录制开关、停机、房间被删除
			loop.stop()
			return &loop.result, ctx.Err()
		case tr := <-tagCh:
			// 读取 tag 失败：EOF 表示流干净结束，其他瞬时错误则返回，让上层决定是否重连。
			if tr.err != nil {
				loop.stop()
				if tr.err == io.EOF {
					return &loop.result, nil
				}
				return &loop.result, fmt.Errorf("%w: %v", biz.ErrStreamTransient, tr.err)
			}
			if err := loop.handleTag(tr.tag); err != nil {
				return &loop.result, err
			}
		case ev := <-events: // 弹幕/礼物等事件写入
			loop.handleEvent(ev)
		case <-speedSampler.C:
			loop.sampleSpeed()
		case <-health.C:
			if err := loop.checkHealth(); err != nil {
				return &loop.result, err
			}
		}
	}
}

// tagRead 是后台读取协程向 tagCh 投递的一次读取结果。
type tagRead struct {
	tag *flv.Tag // 成功读到的标签；err 非空时为 nil
	err error    // 读取失败或流结束（io.EOF）时非空
}

// startTagReader 启动一个后台协程持续从 body 读取 FLV 标签并投递到返回的
// channel，直到出错（含 EOF）后退出；使阻塞的标签读取与主循环的 tick、
// 事件通道解耦。
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

// recordSessionLoop 是单次 RecordSession 调用的会话态：随调用生命周期
// 创建和销毁，串联段边界、去重、写入进度等每次拉流独有的可变状态，不
// 与其他并发会话共享（跨会话共享状态仍在 recorderRepo 上）。
type recordSessionLoop struct {
	repo    *recorderRepo         // 所属仓储，读取切分/健康检查配置
	session *biz.RecordingSession // 本次录制的会话信息
	dir     string                // 会话目录
	base    string                // 会话文件名前缀
	meta    string                // meta.json 路径
	header  *flv.FileHeader       // 拉流解析出的 FLV 文件头

	stats     *pumpStats // 房间级写入进度，跨多次 RecordSession 调用（重连）共享
	baseBytes int64      // 本次调用开始前 stats 已有的写入字节数，用于换算绝对进度

	headers segmentHeaders // 头标签缓存，供新分段/强制切分段重注入
	guard   dupGuard       // CDN 循环吐流去重状态
	seg     *segmentFile   // 当前打开的分段文件，nil 表示尚未开段

	result       biz.RecordingResult // 待返回给调用方的最终结果
	receiveBytes int64               // 本次会话累计接收字节（网络接收口径，用于下载速度采样）
	writtenBytes int64               // 本次会话累计写盘字节（落盘口径，用于 bytes_written 与健康检查）
	lastGrowth   int64               // 上次健康检查时的 writtenBytes，用于判断本轮是否有新数据落盘
	failRounds   int                 // 连续健康检查失败轮数

	lastSampleAt time.Time // 下载速度采样：上次采样时刻
	lastSample   int64     // 下载速度采样：上次采样时的累计接收字节
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
		repo:         repo,
		session:      session,
		dir:          dir,
		base:         base,
		meta:         metaPath,
		header:       header,
		stats:        stats,
		baseBytes:    baseBytes,
		lastSampleAt: time.Now(),
	}
}

// handleTag 处理一个拉流读到的 FLV tag：开启首个分段（等待关键帧）、
// 按块边界裁决去重缓冲、按大小/时长或序列头变化切分段，最后计入头
// 标签缓存与去重块。
func (l *recordSessionLoop) handleTag(tag *flv.Tag) error {
	// 下载速度统计基于实际接收流量（receiveBytes），而非块裁决后的落盘
	// 字节（writtenBytes），避免去重/缓冲导致的写盘脉冲把速度采样打成 0。
	l.receiveBytes += int64(len(tag.Data)) + flv.TagEnvelopeSize

	if l.seg == nil {
		// 新段等待首个视频关键帧再开文件：关键帧之前的标签丢弃（头标签
		// 仍照常入缓存，供开段注入），保证段首即关键帧、独立可解码；
		// 纯音频流没有视频关键帧，豁免等待。
		if l.header.HasVideo && !tag.IsVideoKeyframe() {
			l.headers.absorb(tag)
			return nil
		}
		if err := l.openNewSegment(); err != nil {
			l.repo.appendMetaError(l.meta, "record", err)
			return err
		}
	} else if l.guard.boundary(tag) {
		// 块边界：先裁决缓冲块落盘，切段判定才能用上最新的段状态。
		if err := l.flushBlock(); err != nil {
			l.closeSegment()
			return err
		}
	}

	// 切段判定在块裁决之后；强切路径（超限/序列头变化）同样要先结束
	// 缓冲块，避免关段时把在途数据留在缓冲里丢失。
	switch {
	case l.repo.shouldSplit(l.seg, tag):
		if err := l.rotateSegment(); err != nil {
			return err
		}
	case l.headers.changed(tag):
		// 流中途序列头变化（CDN 换源、主播改码率）：继续写入旧分段会把
		// 两种解码配置拼进同一个文件，强制切段。新段按既有规则从缓存
		// 注入旧头标签，新序列头作为首个正文标签紧随其后写入，播放器
		// 以最新的序列头为准。
		log.Warn("sequence header changed, splitting segment", "room", l.session.RoomID, "part", l.seg.part)
		if err := l.rotateSegment(); err != nil {
			return err
		}
	}

	// 头标签只在开/切分段决策之后才入缓存：触发新分段的那个标签不能从
	// 缓存重注入，否则会被写两次（openSegment 写一次、上面的拉流写入
	// 又一次）。切分前已见过的头标签仍会完整重注入。
	l.headers.absorb(tag)
	l.guard.add(tag)
	return nil
}

// handleEvent 把弹幕/礼物等事件写入当前分段；尚未开段时静默丢弃。
func (l *recordSessionLoop) handleEvent(ev *biz.DanmakuEvent) {
	if l.seg == nil {
		return
	}
	if err := l.seg.writeEvent(ev); err != nil {
		log.Warn("danmaku write failed", "room", l.session.RoomID, "err", err) // 尽力而为, 不影响录制主流程
	}
}

// sampleSpeed 按接收字节增量采样下载速度。
func (l *recordSessionLoop) sampleSpeed() {
	now := time.Now()
	delta := max(l.receiveBytes-l.lastSample, 0)
	elapsed := now.Sub(l.lastSampleAt)
	if elapsed <= 0 {
		return
	}
	l.stats.setDownloadSpeed(int64(float64(delta) / elapsed.Seconds()))
	l.lastSample = l.receiveBytes
	l.lastSampleAt = now
}

// checkHealth 在 healthInterval 内未见新数据则计为一次失败，连续达到
// healthFailRounds 次后关段并返回错误，判定本次录制异常。
func (l *recordSessionLoop) checkHealth() error {
	if l.writtenBytes > l.lastGrowth {
		l.lastGrowth = l.writtenBytes
		l.failRounds = 0
		return nil
	}
	l.failRounds++
	if l.failRounds < l.repo.healthFailRounds {
		return nil
	}
	l.closeSegment()
	return fmt.Errorf("recording unhealthy: no new data for %d rounds", l.failRounds)
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
		l.addWrittenBytes(int64(len(ht.Data)) + flv.TagEnvelopeSize)
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

// stop 在流干净结束或调用方取消时收尾：尽力落盘在途缓冲块，然后关段。
func (l *recordSessionLoop) stop() {
	l.drainPending()
	l.closeSegment()
}

// rotateSegment 结束当前分段并立即开启新分段，用于达到切分阈值或序列头
// 变化的强切路径；先裁决缓冲块落盘，避免在途数据留在缓冲里丢失。
func (l *recordSessionLoop) rotateSegment() error {
	if err := l.flushBlock(); err != nil {
		l.closeSegment()
		return err
	}
	l.closeSegment()
	if err := l.openNewSegment(); err != nil {
		l.repo.appendMetaError(l.meta, "record", err)
		return err
	}
	return nil
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
