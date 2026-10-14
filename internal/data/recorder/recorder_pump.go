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
	layout, err := sessionPaths(repo.recordRoot, session)
	if err != nil {
		return nil, err
	}
	if err := os.MkdirAll(layout.dir, 0o755); err != nil {
		return nil, err
	}

	// 读取直播流 FLV 文件头
	fileHeader, err := flv.ParseHeader(stream.Body)
	if err != nil {
		return nil, fmt.Errorf("%w: %v", biz.ErrStreamTransient, err)
	}

	// 初始化当前会话的写入进度，确保新分段的统计从零开始。
	stats := repo.getOrCreateStats(session.RoomID)
	stats.setCurrentFile("")

	// 把 CDN 实际授予的清晰度记入 meta
	repo.updateMeta(layout.metaPath(), func(meta *sessionMeta) {
		meta.Quality = qualityMeta(stream.Quality)
		meta.Title = session.Title
	})

	state := newPumpState(repo, session.RoomID, layout, fileHeader, stats)
	tagCh := flv.ReadTags(ctx, stream.Body)

	health := time.NewTicker(repo.healthInterval)
	defer health.Stop()

	speedSampler := time.NewTicker(time.Second)
	defer speedSampler.Stop()

	for {
		select {
		// 上下文取消：录制开关关闭、停机或房间被删除
		case <-ctx.Done():
			state.stop()
			return &state.result, ctx.Err()

		// 读取新的 FLV tag
		case tagResult, ok := <-tagCh:
			// 通道已关闭说明读取协程退出了，且没有投递终止结果——只可能是
			// ctx 被取消（出错时终止结果会先投递，走下面的 tr.Err 分支）。
			// 必须在这里收尾：关闭的通道会持续投递零值 TagResult，直接当
			// 正常 tag 交给 handleTag 就会解引用 nil 的 *flv.Tag 而 panic。
			if !ok {
				state.stop()
				return &state.result, ctx.Err()
			}

			// 读取 tag 失败
			if tagResult.Err != nil {
				state.stop()
				// EOF 表示流干净结束
				if tagResult.Err == io.EOF {
					return &state.result, nil
				}
				// 其他瞬时错误则返回，让上层决定是否重连
				return &state.result, fmt.Errorf("%w: %v", biz.ErrStreamTransient, tagResult.Err)
			}

			if err := state.handleTag(tagResult.Tag); err != nil {
				return &state.result, err
			}

		// 弹幕/礼物等事件写入
		case ev := <-events:
			state.handleEvent(ev)

		// 速度采样
		case <-speedSampler.C:
			if speed, ok := state.speed.sample(time.Now()); ok {
				state.stats.setDownloadSpeed(speed)
			}

		// 健康检查
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
	repo        *recorderRepo       // 所属仓储，读取切分/健康检查配置
	roomID      int64               // 当前录制房间 ID
	layout      sessionLayout       // 会话在磁盘上的位置（目录 + 文件名前缀）
	fileHeader  *flv.FileHeader     // 拉流解析出的 FLV 文件头
	headerCache headerCache         // 头标签缓存，供新分段/强制切分段重注入
	stats       *sessionStats       // 房间级写入进度，跨多次 PumpSession 调用（重连）共享
	baseBytes   int64               // 本次调用开始前 stats 已有的写入字节数，用于换算绝对进度
	guard       dupGuard            // CDN 循环吐流去重状态
	writer      *segmentWriter      // 当前打开的分段文件，nil 表示尚未开段
	result      biz.RecordingResult // 待返回给调用方的最终结果；BytesWritten 即累计写盘的正文 tag 字节，不含文件头与头标签

	speed  speedTracker  // 下载速度采样状态
	health healthMonitor // 健康检查状态：连续无新数据落盘的轮数
}

func newPumpState(repo *recorderRepo, roomID int64, layout sessionLayout, fileHeader *flv.FileHeader, stats *sessionStats) *pumpState {
	return &pumpState{
		repo:       repo,
		roomID:     roomID,
		layout:     layout,
		fileHeader: fileHeader,
		stats:      stats,
		baseBytes:  stats.bytesWritten(),
		speed:      newSpeedTracker(time.Now()),
	}
}

// handleTag 处理拉流读到的一个 FLV tag
func (s *pumpState) handleTag(tag *flv.Tag) error {
	// 速度按实际接收字节统计，而非落盘字节：去重缓冲会把写盘集中成脉冲，
	// 若按落盘口径采样，多数采样周期会得到 0。
	s.speed.addReceived(int64(len(tag.Data)) + flv.TagOverhead)

	// 分段须从视频关键帧开始：尚未开段时丢弃首个关键帧之前的正文 tag
	// 头标签仍需缓存，供开段时注入。
	if s.writer == nil && s.fileHeader.HasVideo && !tag.IsAVCKeyframe() {
		s.headerCache.remember(tag)
		return nil
	}

	switch {
	// 当前没有可写分段时，开新段
	case s.writer == nil:
		if err := s.openNewSegment(); err != nil {
			return err
		}
	// 当前 tag 是块边界, 说明可以落盘当前缓冲块了
	case s.guard.isBoundary(tag):
		if err := s.flushBlock(); err != nil {
			s.closeSegment()
			return err
		}
	}

	// 切段判定：大小或时长超限时优先切段；否则检查序列头是否变化。
	// 序列头变化时若继续写旧段，两种解码配置会混入同一文件，因此强制切段。
	switch {
	case ShouldSplit(s.writer, tag, s.repo.maxSegmentBytes, s.repo.maxSegmentMintes):
		if err := s.rotateSegment(); err != nil {
			return err
		}
	case s.headerCache.sequenceHeaderChanged(tag):
		log.Warn("sequence header changed, splitting segment", "room", s.roomID, "part", s.writer.part)
		// 强制切段，确保新序列头写入新段。
		if err := s.rotateSegment(); err != nil {
			return err
		}
	}

	// 记录状态。头标签须在切段判定之后入缓存：序列头变化触发的切段会把新序列头
	// 注入新段，若此时已入缓存，同一个 tag 还会作为正文再写一次，造成重复。
	s.headerCache.remember(tag)
	s.guard.add(tag)
	// 进度计入去重缓冲中已接收但未落盘的字节（见 updateProgress），块落盘后
	// 这部分转为已写字节，进度随之校正为实际落盘值。
	s.updateProgress()
	return nil
}

// handleEvent 把弹幕/礼物等事件写入当前分段；尚未开段时静默丢弃。
func (s *pumpState) handleEvent(ev *biz.DanmakuEvent) {
	if s.writer == nil {
		return
	}
	if err := s.writer.writeDanmaku(ev); err != nil {
		log.Warn("danmaku write failed", "room", s.roomID, "err", err) // 尽力而为, 不影响录制主流程
	}
}

func (s *pumpState) openNewSegment() error {
	writer, err := OpenSegment(s.layout, s.fileHeader, &s.headerCache)
	if err != nil {
		s.repo.appendMetaError(s.layout.metaPath(), "record", err)
		return err
	}

	s.writer = writer
	s.result.Parts++
	s.stats.setCurrentFile(writer.videoPath)

	s.repo.appendSegmentMeta(s.layout.metaPath(), writer)
	log.Info("segment opened", "room", s.roomID, "part", writer.part, "file", writer.videoPath)
	return nil
}

func (s *pumpState) closeSegment() {
	if s.writer == nil {
		return
	}
	if err := s.writer.close(); err != nil {
		log.Error("close segment failed", "room", s.roomID, "file", s.writer.videoPath, "err", err)
	}
	s.repo.finishSegmentMeta(s.layout.metaPath(), s.writer)
	s.writer = nil
}

// stop 在流干净结束或调用方取消时收尾：尽力落盘在途缓冲块，然后关段。
func (s *pumpState) stop() {
	// 尝试落盘在途缓冲块，避免丢失数据。写失败时中断排空，但仍要往下走
	// 到 closeSegment()：否则句柄不关、缓冲区不刷、meta 的收尾字段不写，
	// 而这些恰恰是磁盘写满时最需要保住的东西。
	for _, tag := range s.guard.takeAll() {
		if err := s.writeFLVTag(tag, false); err != nil {
			log.Warn("drain pending block failed", "room", s.roomID, "err", err)
			break
		}
	}

	// 关闭当前分段，确保所有在途数据已落盘。
	s.closeSegment()
}

// rotateSegment 结束当前分段并立即开启新分段
func (s *pumpState) rotateSegment() error {
	// 先尝试落盘当前缓冲块，避免在途数据丢失。
	if err := s.flushBlock(); err != nil {
		s.closeSegment()
		return err
	}

	// 关闭当前分段，确保所有在途数据已落盘。
	s.closeSegment()

	// 开启新分段，准备继续写入数据。
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

	for _, tag := range buf {
		if err := s.writeFLVTag(tag, true); err != nil {
			return err
		}
	}

	return nil
}

// writeFLVTag 将单个 FLV 标签写入当前分段，并根据 persistError 决定是否记录元信息错误。
func (s *pumpState) writeFLVTag(tag *flv.Tag, persistError bool) error {
	// 将单个 FLV 标签写入当前分段
	n, err := s.writer.writeBodyTag(tag)

	// 更新写入进度，即使写入失败也记录已写入的字节数
	s.addWrittenBytes(n)

	if err != nil && persistError {
		s.repo.appendMetaError(s.layout.metaPath(), "record", err)
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
