package biz

import (
	"context"
	"fmt"

	"github.com/go-kratos/kratos/v3/log"
)

// launchSession 启动录制会话协程，独占完整的录制循环和 FinishSession。
func (uc *RecorderUsecase) launchSession(ctx context.Context, roomID int64, info *RoomInfo, events <-chan *DanmakuEvent) *sessionHandle {
	sctx, cancel := context.WithCancel(ctx)
	handle := &sessionHandle{cancel: cancel, done: make(chan struct{})}
	go func() {
		defer close(handle.done)
		uc.runSession(sctx, roomID, info, events)
	}()
	return handle
}

// runSession 端到端负责一次会话：准备、录制循环、收尾/合并。
func (uc *RecorderUsecase) runSession(ctx context.Context, roomID int64, info *RoomInfo, events <-chan *DanmakuEvent) {
	// 1. 准备会话目录和 meta.json
	room := uc.registry.Room(roomID)
	session := &RecordingSession{
		RoomID:        roomID,
		RoomName:      firstNonEmpty(room.StreamerName, info.StreamerName, fmt.Sprintf("%d", roomID)),
		Title:         info.Title,
		LiveStartTime: info.LiveStartTime,
	}
	uc.registry.StartRecording(roomID)
	if err := uc.repo.PrepareSession(ctx, session); err != nil {
		log.Error("prepare session failed", "room", roomID, "err", err)
		uc.registry.FailRecording(roomID, err)
		return
	}

	// *2. 录制循环：持续拉流直到连接结束，然后重新探测直播状态，要么重连（新分段），要么结束会话并保留已录内容。
	uc.recordLoop(ctx, roomID, session, events)

	// 3. 收尾脱离（可能已取消的）运行 context，保证关停期间合并标记
	// 仍能落盘；遗留部分由下次启动时的 RecoverPending 接管。
	uc.registry.SetMerging(roomID)
	fctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), finishGracePeriod)
	defer cancel()
	if err := uc.repo.FinishSession(fctx, session); err != nil {
		log.Error("finish session failed", "room", roomID, "err", err)
		uc.registry.FailRecording(roomID, err)
		return
	}

	uc.registry.FinishRecording(roomID)
}

func firstNonEmpty(vals ...string) string {
	for _, v := range vals {
		if v != "" {
			return v
		}
	}
	return ""
}
