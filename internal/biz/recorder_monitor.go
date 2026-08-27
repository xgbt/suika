package biz

import (
	"context"
	"fmt"
	"math/rand/v2"
	"time"

	"suika/internal/utils"

	"github.com/go-kratos/kratos/v3/log"
)

const (
	pollJitterFraction = 5 // 轮询间隔的相对抖动幅度（间隔 +/- fraction/2）
)

// monitorHandle 是 supervisor 管理单个房间 Monitor 的生命周期句柄。
type monitorHandle struct {
	lastRecordEnabled bool
	roomChange        chan struct{}
	cancel            context.CancelFunc
	done              chan struct{}
}

// notifyRoomChange 发送信号到 roomChange 通道, 通知 Monitor 重新读取房间状态并评估 Session 策略
func (h *monitorHandle) notifyRoomChange() {
	select {
	case h.roomChange <- struct{}{}:
	default:
	}
}

// launchMonitor 异步启动指定房间的监控，并返回其生命周期句柄。
func (uc *RecorderUsecase) launchMonitor(ctx context.Context, roomID int64) *monitorHandle {
	mctx, cancel := context.WithCancel(ctx)
	h := &monitorHandle{
		roomChange: make(chan struct{}, 1), // 缓冲 1，避免重复请求阻塞
		cancel:     cancel,
		done:       make(chan struct{}),
	}

	go func() {
		defer close(h.done)
		uc.runMonitor(mctx, h.roomChange, roomID)
	}()

	return h
}

// runMonitor 维持房间的弹幕连接，断开后重拨，直到 ctx 被取消。
func (uc *RecorderUsecase) runMonitor(ctx context.Context, roomChange <-chan struct{}, roomID int64) {
	for {
		// 单次连接结束后重新拨号，直到 ctx 被取消。
		if err := uc.runMonitorConnection(ctx, roomChange, roomID); err != nil && ctx.Err() == nil {
			log.Error("room monitor failed", "room", roomID, "err", err)
			uc.roomRegistry.NoteError(roomID, err)
		}
		if utils.SleepCtx(ctx, uc.monitorReconnectDelay) != nil {
			return
		}
	}
}

// runMonitorConnection 运行实际的房间监控连接，直到连接断开或 ctx 被取消。连接断开后返回错误，调用方可决定是否重拨
func (uc *RecorderUsecase) runMonitorConnection(ctx context.Context, roomChange <-chan struct{}, roomID int64) error {
	// 弹幕连接：开播检测主通道，录制期间同时提供弹幕事件。
	danmakuConn, err := uc.liveClient.DanmakuConn(ctx, roomID)
	if err != nil {
		return fmt.Errorf("open danmaku conn: %w", err)
	}
	defer danmakuConn.Close()

	// 兜底轮询：弹幕连接没有推送房态时，主动拉取房间信息。
	// 轮询间隔加入抖动，避免多个房间同时请求平台接口。
	poll := time.NewTimer(uc.nextPollDelay())
	defer poll.Stop()

	policy := newSessionPolicy(uc.roomRegistry.Room(roomID).RecordEnabled)
	var active *sessionHandle
	applyDecision := func(active *sessionHandle, action sessionAction) *sessionHandle {
		switch action.kind {
		case actionStart:
			return uc.launchSession(ctx, roomID, action.info, danmakuConn.Events())
		case actionStop:
			if active == nil {
				// 理论上 Stop 仅在 running 阶段产生；若出现 nil 句柄，记录异常便于排查策略/接线回归。
				log.Error("session stop decision without active session", "room", roomID)
				return nil
			}
			active.cancel()
			return active
		case actionNone:
			return active
		default:
			log.Error("unknown session action", "room", roomID, "action", action.kind.String())
			return active
		}
	}

	// roomInfoArrived 是弹幕推送与回退轮询两路房间信息的共同动作：
	// 先应用到注册表，再投递给策略决策。
	roomInfoArrived := func(roomInfo *RoomInfo) {
		uc.roomRegistry.ApplyRoomInfo(ctx, roomID, roomInfo)
		active = applyDecision(active, policy.RoomInfoArrived(roomInfo))
	}

	for {
		// events / done 借助 nil 通道互斥启用：无活跃会话时排空弹幕事件通道；
		// 有活跃会话时由录制协程独占消费事件，监控循环只监听其结束信号。
		var events <-chan *DanmakuEvent
		var done chan struct{}
		if active == nil {
			events = danmakuConn.Events()
		} else {
			done = active.done
		}

		select {
		// ctx 取消：优雅结束监控；若有活跃会话，先取消并等待其自然
		// 结束，避免中途取消导致合并失败。
		case <-ctx.Done():
			if active != nil {
				active.cancel()
				<-active.done
			}
			return nil
		// 无活跃会话：丢弃弹幕事件，防止陈旧事件积压混入下一个会话的录制
		case <-events:
		// 录制会话已结束
		case <-done:
			active = applyDecision(nil, policy.SessionFinished())
		// 弹幕连接推送了房间状态变化
		case roomInfo := <-danmakuConn.RoomStateUpdates():
			roomInfoArrived(roomInfo)
		// 轮询: 主动请求房间信息
		case <-poll.C:
			roomInfo, err := uc.liveClient.GetRoomInfo(ctx, roomID)
			if err != nil && ctx.Err() == nil {
				log.Warn("fallback poll failed", "room", roomID, "err", err)
				uc.roomRegistry.NoteError(roomID, err)
			} else if err == nil {
				roomInfoArrived(roomInfo)
			}
			poll.Reset(uc.nextPollDelay())
		// 管理后台变更了房间记录：重读最新录制开关投递给策略
		case <-roomChange:
			room := uc.roomRegistry.Room(roomID)
			active = applyDecision(active, policy.RecordEnabledUpdated(room.RecordEnabled))
		}
	}
}

// nextPollDelay 返回下一次兜底轮询的延迟：pollInterval 加均匀抖动
// （± 1/pollJitterFraction 的一半），避免多房间的轮询在同一时刻打到
// 平台接口。
func (uc *RecorderUsecase) nextPollDelay() time.Duration {
	d := uc.pollInterval
	if d <= 0 {
		return d
	}
	span := int64(d) / pollJitterFraction
	if span <= 0 {
		return d
	}
	return d + time.Duration(rand.Int64N(span)) - time.Duration(span/2)
}
