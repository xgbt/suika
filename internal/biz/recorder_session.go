package biz

import (
	"context"
	"fmt"

	"github.com/go-kratos/kratos/v3/log"
)

type sessionHandle struct {
	cancel context.CancelFunc
	done   chan struct{}
}

// launchSession 异步启动录制会话，并返回其生命周期句柄。
func (uc *RecorderUsecase) launchSession(ctx context.Context, roomID int64, info *RoomInfo, events <-chan *DanmakuEvent) *sessionHandle {
	sctx, cancel := context.WithCancel(ctx)
	h := &sessionHandle{
		cancel: cancel,
		done:   make(chan struct{}),
	}

	go func() {
		defer close(h.done)
		uc.runSession(sctx, roomID, info, events)
	}()

	return h
}

// runSession 同步执行一次完整会话，包括准备、录制和收尾合并。
func (uc *RecorderUsecase) runSession(ctx context.Context, roomID int64, info *RoomInfo, events <-chan *DanmakuEvent) {
	// 1. 准备会话目录和 meta.json
	room := uc.roomRegistry.Room(roomID)
	session := &RecordingSession{
		RoomID:        roomID,
		RoomName:      firstNonEmpty(room.StreamerName, info.StreamerName, fmt.Sprintf("%d", roomID)),
		Title:         info.Title,
		LiveStartTime: info.LiveStartTime,
	}
	uc.roomRegistry.StartRecording(roomID)
	if err := uc.repo.PrepareSession(ctx, session); err != nil {
		log.Error("prepare session failed", "room", roomID, "err", err)
		uc.roomRegistry.FailRecording(roomID, err)
		return
	}

	// 2. 录制循环：持续拉流直到连接结束，然后重新探测直播状态；要么重连并创建新分段，要么结束会话并保留已录内容。
	uc.runRecordingLoop(ctx, roomID, session, events)

	// 3. 收尾脱离（可能已取消的）运行 context，保证关停期间合并标记
	// 仍能落盘；遗留部分由下次启动时的 RecoverPending 接管。
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

func firstNonEmpty(vals ...string) string {
	for _, v := range vals {
		if v != "" {
			return v
		}
	}
	return ""
}
