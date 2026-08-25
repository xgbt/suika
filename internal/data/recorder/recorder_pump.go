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
			loop.receiveBytes += int64(len(tag.Data)) + flv.TagEnvelopeSize
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

// recordSessionLoop 是单次 RecordSession 调用的会话态：随调用生命周期
// 创建和销毁，串联段边界、去重、写入进度等每次拉流独有的可变状态，不
// 与其他并发会话共享（跨会话共享状态仍在 recorderRepo 上）。
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
