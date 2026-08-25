package biz

import (
	"context"
	stderrors "errors"
	"time"

	"suika/internal/utils"

	"github.com/go-kratos/kratos/v3/log"
)

// runRecordingLoop 执行会话内的流录制和断流重连，直到直播结束或重连策略决定结束会话。
func (uc *RecorderUsecase) runRecordingLoop(ctx context.Context, roomID int64, session *RecordingSession, events <-chan *DanmakuEvent) {
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
			live, ok := uc.probeLive(ctx, roomID)
			if !ok {
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
		live, ok := uc.probeLive(ctx, roomID)
		if !ok || !live {
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
		log.Warn("stream interrupted, reconnecting", "room", roomID, "err", recErr, "attempt", reconnects, "max", uc.rec.MaxReconnect, "delay", uc.rec.ReconnectDelay)
		if utils.SleepCtx(ctx, uc.rec.ReconnectDelay) != nil {
			return
		}
	}
}

// probeLive 复查房间的直播状态并应用到注册表。单次探测说"在播"即成立
// （录制优先）；"未开播"需要连续 offlineConfirmRounds 次确认，避免单次
// 接口抖动或轮次切换瞬间把场次提前结束。探测失败不计入下播确认，累计
// probeMaxAttempts 次仍无定论时记错误并返回 ok=false，调用方应结束场次；
// 若失败由 ctx 取消引起（如监控已因下播事件取消了本场次），属正常结束
// 路径，静默返回、不记错误。
func (uc *RecorderUsecase) probeLive(ctx context.Context, roomID int64) (live, ok bool) {
	var lastErr error
	offlineStreak := 0
	for attempt := range probeMaxAttempts {
		if attempt > 0 {
			if utils.SleepCtx(ctx, uc.offlineConfirmDelay) != nil {
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
			return true, true
		}
		offlineStreak++
		if offlineStreak >= offlineConfirmRounds {
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
