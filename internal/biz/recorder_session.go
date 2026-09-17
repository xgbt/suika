package biz

import (
	"context"
	stderrors "errors"
	"fmt"
	"time"

	"suika/internal/utils"

	"github.com/go-kratos/kratos/v3/log"
	"github.com/samber/lo"
)

const (
	finishGracePeriod    = 30 * time.Second // 限定关停期间 FinishSession 脱离已取消运行 context 后仍可用的工作时长
	offlineConfirmRounds = 3                // 判定下播所需的连续"未开播"探测次数
	probeMaxAttempts     = 6                // 单次下播确认内的探测总次数上限（含失败）
	defaultCDNBackoffMax = 60 * time.Second // CDN 瞬时故障的重试延迟上限
)

type sessionHandle struct {
	cancel context.CancelFunc
	done   chan struct{}
}

// launchSession 异步启动录制会话，并返回其生命周期句柄。
func (uc *RecorderUsecase) launchSession(ctx context.Context, info *RoomInfo, events <-chan *DanmakuEvent) *sessionHandle {
	sctx, cancel := context.WithCancel(ctx)
	h := &sessionHandle{
		cancel: cancel,
		done:   make(chan struct{}),
	}

	go func() {
		defer close(h.done)
		uc.runSession(sctx, info, events)
	}()

	return h
}

// runSession 同步执行一次完整会话，包括准备、录制和收尾合并。
func (uc *RecorderUsecase) runSession(ctx context.Context, info *RoomInfo, events <-chan *DanmakuEvent) {
	roomID := roomIDFromCtx(ctx)
	room := uc.roomRegistry.Room(roomID)
	session := &RecordingSession{
		RoomID:        roomID,
		StreamerName:  lo.CoalesceOrEmpty(room.StreamerName, info.StreamerName, fmt.Sprintf("%d", roomID)),
		Title:         info.Title,
		LiveStartTime: info.LiveStartTime,
	}

	// 准备会话目录与 meta.json。
	uc.roomRegistry.StartRecording(roomID)
	if err := uc.repo.PrepareSession(ctx, session); err != nil {
		log.Error("prepare session failed", "room", roomID, "err", err)
		uc.roomRegistry.FailRecording(roomID, err)
		return
	}

	// 持续录制，直到结束或被取消。
	uc.runRecordingLoop(ctx, session, events)

	// 收尾阶段使用脱离取消的 context，尽量保证 meta.json 的合并状态可落盘。
	uc.roomRegistry.SetMerging(roomID)
	fctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), finishGracePeriod)
	defer cancel()
	if err := uc.repo.FinishSession(fctx, session); err != nil {
		log.Error("finish session failed", "room", roomID, "err", err)
		uc.roomRegistry.FailRecording(roomID, err)
		return
	}

	uc.roomRegistry.FinishRecording(roomID)
}

// runRecordingLoop 执行会话内的流录制和断流重连，直到直播结束或重连策略决定结束会话。
func (uc *RecorderUsecase) runRecordingLoop(ctx context.Context, session *RecordingSession, events <-chan *DanmakuEvent) {
	roomID := roomIDFromCtx(ctx)

	reconnects := 0
	cdnBudget := uc.rec.CDNTransientBudget
	cdnAttempt := 0
	for {
		// 1. 拉流
		stream, openErr := uc.liveClient.OpenLiveStream(ctx, roomID)
		if openErr != nil {
			if ctx.Err() != nil {
				return
			}
			// 非瞬时故障（风控拒绝等）无法靠重试恢复：记 lastError 并结束场次。
			if !stderrors.Is(openErr, ErrStreamTransient) {
				log.Error("open stream failed", "room", roomID, "err", openErr)
				uc.roomRegistry.NoteError(roomID, openErr)
				return
			}
			// 瞬时故障（CDN 404 等）最常见的原因是主播刚下播、流已被撤：
			// 先复查房态，已下播则属正常结束，不记错误；仍在播则按 CDN
			// 瞬时预算退避重试。
			live, probeOK := uc.probeLive(ctx)
			if !probeOK {
				return
			}
			if !live {
				log.Info("stream gone, room offline; ending session", "room", roomID, "err", openErr)
				return
			}
			if cdnBudget <= 0 {
				log.Warn("cdn transient budget exhausted, finishing session with recorded content", "room", roomID)
				return
			}
			cdnBudget--
			delay := uc.cdnBackoff(cdnAttempt)
			cdnAttempt++
			log.Warn("open stream failed, retrying", "room", roomID, "err", openErr, "delay", delay)
			if utils.SleepCtx(ctx, delay) != nil {
				return
			}
			continue
		}

		// 2. 录制
		session.Quality = stream.Quality
		uc.roomRegistry.SetStreamQuality(roomID, stream.Quality)
		legStart := time.Now()
		result, recErr := uc.repo.RecordSession(ctx, session, stream, events)
		if result != nil {
			log.Info("pump ended", "room", roomID, "bytes", result.BytesWritten, "parts", result.Parts, "err", recErr)
		}
		if ctx.Err() != nil {
			return
		}

		// 泵送稳定录制超过阈值且写入了内容：本场次是健康的，重置重连预
		// 算，让长直播中的偶发断流不再累计到耗尽。预算只保护"反复开局
		// 即坏"的房间，不应掐死持续产出内容的会话。
		if uc.stableResetAfter > 0 && time.Since(legStart) >= uc.stableResetAfter &&
			result != nil && result.BytesWritten > 0 {
			if reconnects > 0 || cdnBudget < uc.rec.CDNTransientBudget {
				log.Info("recording stable, resetting reconnect budget",
					"room", roomID, "ran", time.Since(legStart).Round(time.Second))
			}
			reconnects = 0
			cdnBudget = uc.rec.CDNTransientBudget
			cdnAttempt = 0
		}

		// 3. 探测直播状态
		live, probeOK := uc.probeLive(ctx)
		if !probeOK || !live {
			return
		}

		// 4. CDN 瞬时故障重连、风控拒绝不重连、其他错误按配置重连。
		if stderrors.Is(recErr, ErrStreamTransient) {
			if cdnBudget <= 0 {
				log.Warn("cdn transient budget exhausted, finishing session with recorded content", "room", roomID)
				return
			}
			cdnBudget--
			delay := uc.cdnBackoff(cdnAttempt)
			cdnAttempt++
			log.Warn("transient stream error, re-opening stream", "room", roomID, "err", recErr, "delay", delay)
			if utils.SleepCtx(ctx, delay) != nil {
				return
			}
			continue
		}

		if !uc.rec.AutoReconnect {
			return
		}
		if reconnects >= uc.rec.MaxReconnect {
			log.Warn("reconnect budget exhausted, finishing session with recorded content", "room", roomID)
			return
		}
		reconnects++
		log.Warn(
			"stream interrupted, reconnecting",
			"room", roomID,
			"err", recErr,
			"attempt", reconnects,
			"max", uc.rec.MaxReconnect,
			"delay", uc.rec.ReconnectDelay,
		)
		if utils.SleepCtx(ctx, uc.rec.ReconnectDelay) != nil {
			return
		}
	}
}

// probeLive 复查并落盘房态：
// 1) 任意一次探测到在播立即返回 live=true；
// 2) 需连续 offlineConfirmRounds 次未开播才判定下播；
// 3) 探测失败不计入下播确认，超过 probeMaxAttempts 仍无结论则记错并返回 ok=false；
// 4) 若由 ctx 取消导致失败，按正常收尾静默返回。
func (uc *RecorderUsecase) probeLive(ctx context.Context) (live, ok bool) {
	roomID := roomIDFromCtx(ctx)

	var lastErr error
	offlineStreak := 0
	for attempt := range probeMaxAttempts {
		if attempt > 0 {
			if utils.SleepCtx(ctx, uc.offlineConfirmDelay) != nil {
				// 监控或会话已取消，按正常结束路径返回。
				return false, false
			}
		}
		roomInfo, err := uc.liveClient.GetRoomInfo(ctx, roomID)
		if err != nil {
			if ctx.Err() != nil {
				return false, false
			}
			lastErr = err // 探测失败不计入下播确认，继续探测
			continue
		}
		uc.roomRegistry.ApplyRoomInfo(ctx, roomID, roomInfo)
		if roomInfo.Live {
			// 单次探测在播即可成立，录制优先。
			return true, true
		}
		offlineStreak++
		if offlineStreak >= offlineConfirmRounds {
			// 连续多次未开播才判下播，降低接口抖动影响。
			return false, true
		}
	}
	log.Error("probe live status failed, ending session", "room", roomID, "err", lastErr)
	uc.roomRegistry.NoteError(roomID, lastErr)
	return false, false
}

// cdnBackoff 返回 CDN 瞬时故障的重试延迟，随尝试次数指数增长，最大不超过 cdnBackoffMax。
func (uc *RecorderUsecase) cdnBackoff(attempt int) time.Duration {
	return min(uc.cdnBackoffBase<<attempt, defaultCDNBackoffMax)
}
