package biz

import (
	"context"
	"fmt"
	"math"
	"sync"
	"time"

	"github.com/go-kratos/kratos/v3/log"
	"github.com/samber/lo"
)

// roomState 是 RoomRegistry 内部维护的房间条目，包含房间基础信息和可变运行状态。
type roomState struct {
	room             Room         // 房间基础信息
	liveStatus       LiveStatus   // 当前直播状态
	recordStatus     RecordStatus // 当前录制状态
	sessionStartedAt time.Time    // 当前录制会话开始时间
	lastError        string       // 最近一次监控或录制错误
}

// RoomRegistry 持有房间列表及其运行时状态，是房间配置的唯一事实源：
// 启动时从repo加载数据到内存，此后房间 API 的增删改在落库成功后同步回写注册表，
// 录制守护进程据此实时增删监控，无需重启。录制守护进程写入直播/录制
// 状态，房间 API 读取快照。
type RoomRegistry struct {
	repo RoomRepo
	// mu 同时保护 RoomRegistry 容器和每个 roomState 内部的可变字段：读改
	// rooms、states 或房间的 live/record/session/error 字段都必须持锁，
	// 只保护 map 仍会在状态对象上产生数据竞争。快照方法持锁拷贝状态，
	// 修改方法持锁完成整个读-改-写。仓储 IO 必须放在临界区之外，避免
	// 慢速数据库调用阻塞所有录制更新和房间读取。
	mu     sync.Mutex
	states map[int64]*roomState
	// subscribers 接收合并式变更唤醒信号，见 Subscribe。
	subscribers []chan struct{}
}

func NewRoomRegistry(repo RoomRepo) (*RoomRegistry, error) {
	reg := &RoomRegistry{
		repo:   repo,
		states: make(map[int64]*roomState),
	}
	if repo == nil {
		log.Warn("room repo missing, registering zero rooms")
		return reg, nil
	}
	rooms, err := repo.ListRooms(context.Background(), ListQuery{Offset: 0, Limit: math.MaxInt32})
	if err != nil {
		return nil, fmt.Errorf("room registry: load rooms: %w", err)
	}
	for _, room := range rooms {
		reg.states[room.RoomID] = &roomState{room: *room}
	}
	return reg, nil
}

func (reg *RoomRegistry) Rooms() []Room {
	reg.mu.Lock()
	defer reg.mu.Unlock()

	out := lo.MapToSlice(reg.states, func(roomID int64, st *roomState) Room {
		return st.room
	})
	return out
}

func (reg *RoomRegistry) Room(roomID int64) Room {
	reg.mu.Lock()
	defer reg.mu.Unlock()

	if st, ok := reg.states[roomID]; ok {
		return st.room
	}
	return Room{RoomID: roomID}
}

// Subscribe 订阅房间集合变更通知，返回一个 channel 用于接收唤醒信号, 以及一个取消订阅函数。
// 唤醒信号是合并式的：多次变更只会触发一次唤醒，订阅者应在收到唤醒后主动拉取最新房间列表。
// 订阅者应在不再需要时调用取消订阅函数，否则会造成内存泄漏。
func (reg *RoomRegistry) Subscribe() (<-chan struct{}, func()) {
	changes := make(chan struct{}, 1)
	reg.mu.Lock()
	reg.subscribers = append(reg.subscribers, changes)
	reg.mu.Unlock()

	unsubscribe := func() {
		reg.mu.Lock()
		defer reg.mu.Unlock()
		for i, sub := range reg.subscribers {
			if sub == changes {
				reg.subscribers = append(reg.subscribers[:i], reg.subscribers[i+1:]...)
				return
			}
		}
	}

	return changes, unsubscribe
}

// Add 在房间创建落库成功后登记到注册表，使其立即对录制守护进程可见。
// 重复登记（不应发生，仓储会先拒绝重复房间）时刷新房间字段。
func (reg *RoomRegistry) Add(room Room) {
	reg.mu.Lock()
	defer reg.mu.Unlock()
	if st, ok := reg.states[room.RoomID]; ok {
		st.room = room
	} else {
		reg.states[room.RoomID] = &roomState{room: room}
	}
	reg.notifyLocked()
}

// Update 在房间更新落库成功后同步注册表中的房间字段，保留已有的运行时
// 状态。房间不存在时忽略（落库成功而注册表缺失属于异常时序，无可为）。
func (reg *RoomRegistry) Update(room Room) {
	reg.mu.Lock()
	defer reg.mu.Unlock()
	if st, ok := reg.states[room.RoomID]; ok {
		st.room = room
		reg.notifyLocked()
	}
}

// Remove 在房间删除落库成功后将房间移出注册表。录制守护进程随之停止该
// 房间的监控；迟到的状态写入（NoteError、FinishRecording 等）因房间不
// 存在而自动忽略。
func (reg *RoomRegistry) Remove(roomID int64) {
	reg.mu.Lock()
	defer reg.mu.Unlock()
	if _, ok := reg.states[roomID]; !ok {
		return
	}
	delete(reg.states, roomID)
	reg.notifyLocked()
}

// notifyLocked 向订阅者发送合并式唤醒信号
// locked 表示调用方须已持有 mu, 避免在持锁时调用外部函数（channel send）而导致死锁。
func (reg *RoomRegistry) notifyLocked() {
	for _, sub := range reg.subscribers {
		// 非阻塞发送提醒
		select {
		case sub <- struct{}{}:
		default:
		}
	}
}

// runtime 提取某房间的运行时状态快照
// roomState -> RoomRuntime
func (reg *RoomRegistry) runtime(roomID int64) *RoomRuntime {
	reg.mu.Lock()
	defer reg.mu.Unlock()

	st, ok := reg.states[roomID]
	if !ok {
		return &RoomRuntime{Room: Room{RoomID: roomID}}
	}

	return &RoomRuntime{
		Room:             st.room,
		LiveStatus:       st.liveStatus,
		RecordStatus:     st.recordStatus,
		SessionStartedAt: st.sessionStartedAt,
		LastError:        st.lastError,
	}
}

// ApplyRoomInfo 记录从B 站获取的房间信息, 并回填到 DB
func (reg *RoomRegistry) ApplyRoomInfo(ctx context.Context, roomID int64, info *RoomInfo) {
	if info == nil {
		return
	}

	reg.mu.Lock()
	st, ok := reg.states[roomID]
	if !ok {
		reg.mu.Unlock()
		return
	}
	if info.Live {
		st.liveStatus = LiveStatusOnAir
	} else {
		st.liveStatus = LiveStatusPreparing
	}
	if info.StreamerName != "" {
		st.room.StreamerName = info.StreamerName
	}
	if info.Title != "" {
		st.room.RoomTitle = info.Title
	}
	roomSnapshot := st.room
	reg.mu.Unlock()

	if _, err := reg.repo.UpdateRoom(ctx, &roomSnapshot); err != nil {
		log.Warn("room registry: persist room identity update failed", "room", roomID, "err", err)
	}
}

// StartRecording 将房间标记为正在录制新会话。
func (reg *RoomRegistry) StartRecording(roomID int64) {
	reg.setState(roomID, func(st *roomState) {
		st.recordStatus = RecordStatusRecording
		st.sessionStartedAt = time.Now()
		st.lastError = ""
	})
}

// SetRemuxing 将房间会话标记为收尾中（正在转封装）。
func (reg *RoomRegistry) SetRemuxing(roomID int64) {
	reg.setState(roomID, func(st *roomState) { st.recordStatus = RecordStatusRemuxing })
}

// FailRecording 将房间会话标记为失败并记录错误。
func (reg *RoomRegistry) FailRecording(roomID int64, err error) {
	reg.setState(roomID, func(st *roomState) {
		st.recordStatus = RecordStatusError
		st.lastError = err.Error()
	})
}

// FinishRecording 在会话收尾完成后将房间恢复空闲。
func (reg *RoomRegistry) FinishRecording(roomID int64) {
	reg.setState(roomID, func(st *roomState) {
		st.recordStatus = RecordStatusIdle
		st.sessionStartedAt = time.Time{}
	})
}

// NoteError 记录监控/会话错误，不改变录制状态。
func (reg *RoomRegistry) NoteError(roomID int64, err error) {
	reg.setState(roomID, func(st *roomState) { st.lastError = err.Error() })
}

func (reg *RoomRegistry) setState(roomID int64, fn func(*roomState)) {
	reg.mu.Lock()
	defer reg.mu.Unlock()
	if st, ok := reg.states[roomID]; ok {
		fn(st)
	}
}
