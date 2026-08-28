package biz

import (
	"context"
	"fmt"
	"time"

	"github.com/go-kratos/kratos/v3/log"
	"github.com/samber/lo"
)

const (
	finishGracePeriod = 30 * time.Second // 限定关停期间 FinishSession 脱离已取消运行 context 后仍可用的工作时长
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
