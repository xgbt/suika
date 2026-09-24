package recorder

import (
	"context"
	"fmt"
	"io"
	"os"
	"time"

	"suika/internal/biz"
	"suika/internal/data/flv"

	"github.com/go-kratos/kratos/v3/log"
)

// PumpSession 为 session 泵送一次直播流连接：将 stream 写入磁盘（按配置切分
// 分段），并把弹幕事件写入对应的 JSONL 文件，直到这次连接的流结束或 ctx 被
// 取消。一个 Session（录制会话：一次连续直播，定义见 CONTEXT.md）在断流重连
// 时会多次调用本方法（biz.RecorderUsecase.runRecordingLoop 是唯一调用方）；写入
// 进度（sessionStats，见 stats.go）跨这些调用持久，不随重连重置。
func (repo *recorderRepo) PumpSession(ctx context.Context, session *biz.RecordingSession, stream *biz.LiveStream, events <-chan *biz.DanmakuEvent) (*biz.RecordingResult, error) {
	// 检查直播流是否有效
	if stream == nil || stream.Body == nil {
		return nil, biz.ErrRoomInternal
	}
	defer stream.Body.Close()

	// 获取会话目录和文件名前缀
	lay, err := sessionPaths(repo.recordRoot, session)
	if err != nil {
		return nil, err
	}
	if err := os.MkdirAll(lay.dir, 0o755); err != nil {
		return nil, err
	}

	// 读取直播流 FLV 文件头
	header, err := flv.ParseHeader(stream.Body)
	if err != nil {
		return nil, fmt.Errorf("%w: %v", biz.ErrStreamTransient, err)
	}

	// 初始化当前会话的写入进度，确保新分段的统计从零开始。
	stats := repo.getOrCreateStats(session.RoomID)
	stats.setCurrentFile("")

	// 把 CDN 实际授予的清晰度记入 meta
	repo.updateMeta(lay.metaPath(), func(meta *sessionMeta) {
		meta.Quality = qualityMeta(stream.Quality)
		meta.Title = session.Title
	})

	state := newPumpState(repo, session.RoomID, lay, header, stats)
	tagCh := flv.ReadTags(ctx, stream.Body)

	health := time.NewTicker(repo.healthInterval)
	defer health.Stop()
	speedSampler := time.NewTicker(time.Second)
	defer speedSampler.Stop()

	for {
		select {
		case <-ctx.Done(): // 关闭录制开关、停机、房间被删除
			state.stop()
			return &state.result, ctx.Err()
		case tr := <-tagCh:
			// 读取 tag 失败：EOF 表示流干净结束，其他瞬时错误则返回，让上层决定是否重连。
			if tr.Err != nil {
				state.stop()
				if tr.Err == io.EOF {
					return &state.result, nil
				}
				return &state.result, fmt.Errorf("%w: %v", biz.ErrStreamTransient, tr.Err)
			}
			if err := state.handleTag(tr.Tag); err != nil {
				return &state.result, err
			}
		case ev := <-events: // 弹幕/礼物等事件写入
			state.handleEvent(ev)
		case <-speedSampler.C:
			if speed, ok := state.speed.sample(time.Now()); ok {
				state.stats.setDownloadSpeed(speed)
			}
		case <-health.C:
			if err := state.health.check(state.result.BytesWritten, state.repo.healthFailRounds); err != nil {
				state.closeSegment()
				return &state.result, err
			}
		}
	}
}

// pumpState 是单次 PumpSession 调用的会话态：随调用生命周期
// 创建和销毁，串联段边界、去重、写入进度等每次拉流独有的可变状态，不
// 与其他并发会话共享（跨会话共享状态仍在 recorderRepo 上）。
type pumpState struct {
	repo      *recorderRepo       // 所属仓储，读取切分/健康检查配置
	roomID    int64               // 当前录制房间 ID
	lay       sessionLayout       // 会话在磁盘上的位置（目录 + 文件名前缀）
	header    *flv.FileHeader     // 拉流解析出的 FLV 文件头
	stats     *sessionStats       // 房间级写入进度，跨多次 PumpSession 调用（重连）共享
	baseBytes int64               // 本次调用开始前 stats 已有的写入字节数，用于换算绝对进度
	headers   segmentHeaders      // 头标签缓存，供新分段/强制切分段重注入
	guard     dupGuard            // CDN 循环吐流去重状态
	writer    *segmentWriter      // 当前打开的分段文件，nil 表示尚未开段
	result    biz.RecordingResult // 待返回给调用方的最终结果；BytesWritten 即累计写盘字节（落盘口径）
	health    healthMonitor       // 健康检查状态：连续无新数据落盘的轮数
	speed     speedTracker        // 下载速度采样状态
}

func newPumpState(
	repo *recorderRepo,
	roomID int64,
	lay sessionLayout,
	header *flv.FileHeader,
	stats *sessionStats,
) *pumpState {
	return &pumpState{
		repo:      repo,
		roomID:    roomID,
		lay:       lay,
		header:    header,
		stats:     stats,
		baseBytes: stats.bytesWritten(),
		speed:     newSpeedTracker(time.Now()),
	}
}

// handleTag 处理一个拉流读到的 FLV tag：开启首个分段（等待关键帧）、
// 按块边界裁决去重缓冲、按大小/时长或序列头变化切分段，最后计入头
// 标签缓存与去重块。
func (s *pumpState) handleTag(tag *flv.Tag) error {
	// 下载速度统计基于实际接收流量，而非块裁决后的落盘
	// 字节（writtenBytes），避免去重/缓冲导致的写盘脉冲把速度采样打成 0。
	s.speed.addReceived(int64(len(tag.Data)) + flv.TagEnvelopeSize)

	// 如果当前尚未有打开的分段，则尝试开新段。新段会等待首个视频关键帧（如有）再真正创建文件。
	if s.writer == nil {
		// 新段等待首个视频关键帧再开文件：关键帧之前的标签丢弃（头标签
		// 仍照常入缓存，供开段注入），保证段首即关键帧、独立可解码；
		// 纯音频流没有视频关键帧，豁免等待。
		if s.header.HasVideo && !tag.IsVideoKeyframe() {
			s.headers.observe(tag)
			return nil
		}
		if err := s.openNewSegment(); err != nil {
			return err
		}
	} else if s.guard.boundary(tag) {
		// 块边界：先裁决缓冲块落盘，切段判定才能用上最新的段状态。
		if err := s.flushBlock(); err != nil {
			s.closeSegment()
			return err
		}
	}

	// 切段判定在块裁决之后；强切路径（超限/序列头变化）同样要先结束
	// 缓冲块，避免关段时把在途数据留在缓冲里丢失。
	switch {
	case s.repo.shouldSplit(s.writer, tag):
		if err := s.rotateSegment(); err != nil {
			return err
		}
	case s.headers.sequenceHeaderChanged(tag):
		// 流中途序列头变化（CDN 换源、主播改码率）：继续写入旧分段会把
		// 两种解码配置拼进同一个文件，强制切段。新段按既有规则从缓存
		// 注入旧头标签，新序列头作为首个正文标签紧随其后写入，播放器
		// 以最新的序列头为准。
		log.Warn("sequence header changed, splitting segment", "room", s.roomID, "part", s.writer.part)
		if err := s.rotateSegment(); err != nil {
			return err
		}
	}

	// 头标签只在开/切分段决策之后才入缓存：触发新分段的那个标签不能从
	// 缓存重注入，否则会被写两次（openSegment 写一次、上面的拉流写入
	// 又一次）。切分前已见过的头标签仍会完整重注入。
	s.headers.observe(tag)
	s.guard.add(tag)
	// 当前块仍在去重缓冲中，但这些字节已经属于本次录制；先反映到
	// 运行时统计，块关闭时 addWrittenBytes 会把最终落盘值校正回来。
	s.updateProgress()
	return nil
}

// handleEvent 把弹幕/礼物等事件写入当前分段；尚未开段时静默丢弃。
func (s *pumpState) handleEvent(ev *biz.DanmakuEvent) {
	if s.writer == nil {
		return
	}
	if err := s.writer.writeEvent(ev); err != nil {
		log.Warn("danmaku write failed", "room", s.roomID, "err", err) // 尽力而为, 不影响录制主流程
	}
}

// healthMonitor 跟踪连续无新数据落盘的轮数，是 pumpSessionLoop 的内部状态。
type healthMonitor struct {
	lastGrowth int64 // 上次检查时的累计写入字节数
	failRounds int   // 连续未见新数据的轮数
}

// check 用当前累计写入字节数更新状态，连续失败达到 maxRounds 时返回错误。
func (h *healthMonitor) check(written int64, maxRounds int) error {
	if written > h.lastGrowth {
		h.lastGrowth = written
		h.failRounds = 0
		return nil
	}
	h.failRounds++
	if h.failRounds < maxRounds {
		return nil
	}
	return fmt.Errorf("recording unhealthy: no new data for %d rounds", h.failRounds)
}

// resetFailRounds 清零连续失败计数：CDN 重复块被丢弃不计入无新数据。
func (h *healthMonitor) resetFailRounds() {
	h.failRounds = 0
}

func (s *pumpState) openNewSegment() error {
	// 编号探测和 O_TRUNC 创建必须串行，否则并发的录制泵会同时选中
	// 同一个 part，并截断彼此刚写入的分段。
	s.repo.segmentMu.Lock()
	defer s.repo.segmentMu.Unlock()

	part := nextPartNumber(s.lay)
	writer, headerTagBytes, err := openSegment(s.lay, part, s.header, &s.headers)
	if err != nil {
		s.repo.appendMetaError(s.lay.metaPath(), "record", err)
		return err
	}

	s.writer = writer
	s.result.Parts++
	s.stats.setCurrentFile(writer.videoPath)

	// 注入的头标签同样是本场次的实际写入字节（等待关键帧后 part1 的
	// 头标签走注入而非泵送；切分段每段重注入），计入写入进度；
	// FLV 文件头本身不计，与既有口径一致。
	s.addWrittenBytes(headerTagBytes)

	s.repo.appendSegmentMeta(s.lay.metaPath(), writer)
	log.Info("segment opened", "room", s.roomID, "part", part, "file", writer.videoPath)
	return nil
}

func (s *pumpState) closeSegment() {
	if s.writer == nil {
		return
	}
	if err := s.writer.close(); err != nil {
		log.Error("close segment failed", "room", s.roomID, "file", s.writer.videoPath, "err", err)
	}
	s.repo.finishSegmentMeta(s.lay.metaPath(), s.writer)
	s.writer = nil
}

// stop 在流干净结束或调用方取消时收尾：尽力落盘在途缓冲块，然后关段。
func (s *pumpState) stop() {
	// 尝试落盘在途缓冲块，避免丢失数据。
	for _, tag := range s.guard.takeAll() {
		if err := s.writeTag(tag, false); err != nil {
			log.Warn("drain pending block failed", "room", s.roomID, "err", err)
			return
		}
	}

	// 关闭当前分段，确保所有在途数据已落盘。
	s.closeSegment()
}

// rotateSegment 结束当前分段并立即开启新分段，用于达到切分阈值或序列头
// 变化的强切路径；先裁决缓冲块落盘，避免在途数据留在缓冲里丢失。
func (s *pumpState) rotateSegment() error {
	if err := s.flushBlock(); err != nil {
		s.closeSegment()
		return err
	}
	s.closeSegment()
	if err := s.openNewSegment(); err != nil {
		return err
	}
	return nil
}

func (s *pumpState) flushBlock() error {
	buf, disconnect := s.guard.close()
	if disconnect {
		return fmt.Errorf("%w: cdn looping duplicate stream data", biz.ErrStreamTransient)
	}
	if buf == nil {
		s.health.resetFailRounds()
		log.Warn("duplicate stream block dropped", "room", s.roomID, "streak", s.guard.streak)
		return nil
	}
	for _, bt := range buf {
		if err := s.writeTag(bt, true); err != nil {
			return err
		}
	}
	return nil
}

// writeTag 将单个 FLV 标签写入当前分段，并根据 persistError 决定是否记录元信息错误。
func (s *pumpState) writeTag(tag *flv.Tag, persistError bool) error {
	// 将单个 FLV 标签写入当前分段
	n, err := s.writer.writeTag(tag)

	// 更新写入进度，即使写入失败也记录已写入的字节数
	s.addWrittenBytes(n)

	if err != nil && persistError {
		s.repo.appendMetaError(s.lay.metaPath(), "record", err)
	}
	return err
}

func (s *pumpState) addWrittenBytes(n int64) {
	s.result.BytesWritten += n
	s.updateProgress()
}

func (s *pumpState) updateProgress() {
	s.stats.setBytesWritten(s.baseBytes + s.result.BytesWritten + s.guard.bufBytes)
}
