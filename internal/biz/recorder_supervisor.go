package biz

import (
	"context"

	"github.com/go-kratos/kratos/v3/log"
	"github.com/samber/lo"
)

// Run 运行录制守护进程的主循环：先收尾上次运行遗留的会话，然后作为监督
// 循环持续调和 RoomRegistry 快照与每房间的监控协程。监控跟随房间存在：
// 新建房间无论是否配置录制都立即开始监控，删除房间立即停止监控（活跃会话
// 随之优雅停止）；record_enabled 翻转不影响监控，只作为重评估信号送达监控
// 协程，由其决定开始或停止录制。
func (uc *RecorderUsecase) Run(ctx context.Context) error {

	// 收尾上次运行遗留的合并工作，若失败则记录错误并继续运行。
	if err := uc.repo.RecoverPending(ctx); err != nil {
		log.Error("recorder: recover pending merge", "err", err)
	}

	// 订阅 Room 注册表变更通知，返回一个通道和取消函数。
	changes, unsubscribe := uc.registry.Subscribe()
	defer unsubscribe()

	// monitors 是当前活跃的房间监控协程集合；retired 是已停止的监控协程集合，等待收尾。
	monitors := make(map[int64]*monitorHandle)
	var retired []*monitorHandle
	defer func() {
		for _, h := range monitors {
			h.cancel()
		}
		for _, h := range monitors {
			<-h.done
		}
		for _, h := range retired {
			<-h.done
		}
	}()

	uc.reconcile(ctx, monitors, &retired)
	if len(monitors) == 0 {
		log.Warn("recorder has no configured rooms, idling")
	}

	for {
		select {
		case <-ctx.Done():
			return nil
		case <-changes:
			uc.reconcile(ctx, monitors, &retired)
		}
	}
}

// monitorHandle 是监督循环管理的一个房间监控协程句柄
// roomChanged 表示合并式重评估信号（如 record_enabled 翻转），由监督循环送达 watchRoom。
type monitorHandle struct {
	recordEnabled bool          // 当前房间的录制开关状态，监督循环维护；watchRoom 只读。
	roomChanged   chan struct{} // 重评估信号 channel, 当管理后台操作 record_enabled 时发送信号，watchRoom 监听并执行决策。
	cancel        context.CancelFunc
	done          chan struct{}
}

// signal 向 roomChanged 投递一个重评估信号，若通道已满则丢弃
func (h *monitorHandle) signal() {
	select {
	case h.roomChanged <- struct{}{}:
	default:
	}
}

// reconcile 按注册表快照调和监控协程集合：为新增房间启动监控；停止并
// 移除已删除房间的监控（移入 retired 自行优雅收尾，不阻塞调和）；
// record_enabled 翻转的房间投递重评估信号。
func (uc *RecorderUsecase) reconcile(ctx context.Context, monitors map[int64]*monitorHandle, retired *[]*monitorHandle) {

	// 回收 retired 中已完成收尾的被移除监控。
	alive := (*retired)[:0]
	for _, h := range *retired {
		select {
		case <-h.done:
		default:
			alive = append(alive, h)
		}
	}
	*retired = alive

	// 获取当前Room注册表快照，按 room_id 建立索引
	want := lo.KeyBy(uc.registry.Rooms(), func(r Room) int64 { return r.RoomID })

	// 1. 如果 monitors 中存在的房间不在 want 中，则说明该房间已被删除，取消其监控协程并移入 retired。
	for roomID, h := range monitors {
		if _, ok := want[roomID]; !ok {
			h.cancel()
			*retired = append(*retired, h)
			delete(monitors, roomID)
		}
	}

	for roomID, room := range want {
		monitor, ok := monitors[roomID]

		// case 1 初次启动
		// case 2 中途新增房间
		// 启动监控协程并登记到 monitors
		if !ok {
			monitor = uc.startMonitor(ctx, roomID)
			monitor.recordEnabled = room.RecordEnabled
			monitors[roomID] = monitor
			continue
		}

		// 已存在房间监控状态变动，通过发送信号的方式, 让监控协程重新评估是否需要启动/停止录制会话。
		if monitor.recordEnabled != room.RecordEnabled {
			monitor.recordEnabled = room.RecordEnabled
			monitor.signal()
		}
	}
}

// startMonitor 启动指定房间的监控协程。
func (uc *RecorderUsecase) startMonitor(ctx context.Context, roomID int64) *monitorHandle {
	mctx, cancel := context.WithCancel(ctx)
	h := &monitorHandle{
		roomChanged: make(chan struct{}, 1),
		cancel:      cancel,
		done:        make(chan struct{}),
	}
	go func() {
		defer close(h.done)
		uc.monitorRoom(mctx, h.roomChanged, roomID)
	}()
	return h
}
