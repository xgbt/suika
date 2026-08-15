package biz

import (
	"context"
	"fmt"
	"math"
	"sync"
	"time"

	"github.com/go-kratos/kratos/v3/log"
)

// roomState 是单个已注册房间的可变运行时状态。
type roomState struct {
	room             Room
	live             LiveState
	record           RecordState
	sessionStartedAt time.Time
	lastError        string
}

// RoomRegistry 持有启动时加载的房间列表及其运行时状态。录制守护进程
// 写入直播/录制状态，房间 API 读取快照。房间 API 的增删改立即持久化
// 到仓储，RoomRegistry 在下次重启时才重新加载。
type RoomRegistry struct {
	repo RoomRepo
	// mu 同时保护 RoomRegistry 容器和每个 roomState 内部的可变字段：读改
	// rooms、states 或房间的 live/record/session/error 字段都必须持锁，
	// 只保护 map 仍会在状态对象上产生数据竞争。快照方法持锁拷贝状态，
	// 修改方法持锁完成整个读-改-写。仓储 IO 必须放在临界区之外，避免
	// 慢速数据库调用阻塞所有录制更新和房间读取。
	mu     sync.Mutex
	rooms  []Room
	states map[int64]*roomState
}

// NewRoomRegistry 从仓储加载 Room 列表构建 RoomRegistry。允许 repo 为
// nil（得到空 RoomRegistry）；加载失败则启动失败。
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
		reg.rooms = append(reg.rooms, *room)
		reg.states[room.RoomID] = &roomState{room: *room}
	}
	return reg, nil
}

// Rooms 按加载顺序返回所有已注册房间的快照。
func (reg *RoomRegistry) Rooms() []Room {
	reg.mu.Lock()
	defer reg.mu.Unlock()
	out := make([]Room, len(reg.rooms))
	copy(out, reg.rooms)
	return out
}

// Room 返回单个房间当前的配置快照（含已回填的主播元数据）。未知房间
// 退化为只带房间号的空房间。
func (reg *RoomRegistry) Room(roomID int64) Room {
	reg.mu.Lock()
	defer reg.mu.Unlock()

	if st, ok := reg.states[roomID]; ok {
		return st.room
	}
	return Room{RoomID: roomID}
}

// runtime 返回单个房间运行时状态的拷贝。RoomRegistry 中不存在的房间
// （启动后新建的）返回默认状态值。
func (reg *RoomRegistry) runtime(roomID int64) *RoomRuntime {
	reg.mu.Lock()
	defer reg.mu.Unlock()
	st, ok := reg.states[roomID]
	if !ok {
		return &RoomRuntime{Room: Room{RoomID: roomID}}
	}
	return &RoomRuntime{
		Room:             st.room,
		Live:             st.live,
		Record:           st.record,
		SessionStartedAt: st.sessionStartedAt,
		LastError:        st.lastError,
	}
}

// ApplyRoomInfo 记录平台上报的房间直播状态，并在主播名/标题为空时回填。
// 回填值会经 RoomRepo 写回持久化，重启后仍然保留。
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
		st.live = LiveOnAir
	} else {
		st.live = LivePreparing
	}
	var backfilledStreamerName string
	var backfilledRoomTitle string
	if st.room.StreamerName == "" && info.StreamerName != "" {
		backfilledStreamerName = info.StreamerName
	}
	if st.room.RoomTitle == "" && info.Title != "" {
		backfilledRoomTitle = info.Title
	}
	reg.mu.Unlock()

	if (backfilledStreamerName != "" || backfilledRoomTitle != "") && reg.repo != nil {
		updated, err := reg.repo.BackfillRoomIdentity(ctx, roomID, backfilledStreamerName, backfilledRoomTitle)
		if err != nil {
			log.Warn("room registry: persist backfilled room metadata failed", "room", roomID, "err", err)
			reg.mu.Lock()
			if st, ok := reg.states[roomID]; ok {
				if st.room.StreamerName == "" && backfilledStreamerName != "" {
					st.room.StreamerName = backfilledStreamerName
				}
				if st.room.RoomTitle == "" && backfilledRoomTitle != "" {
					st.room.RoomTitle = backfilledRoomTitle
				}
				for i := range reg.rooms {
					if reg.rooms[i].RoomID == roomID {
						if reg.rooms[i].StreamerName == "" && backfilledStreamerName != "" {
							reg.rooms[i].StreamerName = backfilledStreamerName
						}
						if reg.rooms[i].RoomTitle == "" && backfilledRoomTitle != "" {
							reg.rooms[i].RoomTitle = backfilledRoomTitle
						}
						break
					}
				}
			}
			reg.mu.Unlock()
			return
		}
		if updated {
			reg.mu.Lock()
			if st, ok := reg.states[roomID]; ok {
				if st.room.StreamerName == "" && backfilledStreamerName != "" {
					st.room.StreamerName = backfilledStreamerName
				}
				if st.room.RoomTitle == "" && backfilledRoomTitle != "" {
					st.room.RoomTitle = backfilledRoomTitle
				}
				for i := range reg.rooms {
					if reg.rooms[i].RoomID == roomID {
						if reg.rooms[i].StreamerName == "" && backfilledStreamerName != "" {
							reg.rooms[i].StreamerName = backfilledStreamerName
						}
						if reg.rooms[i].RoomTitle == "" && backfilledRoomTitle != "" {
							reg.rooms[i].RoomTitle = backfilledRoomTitle
						}
						break
					}
				}
			}
			reg.mu.Unlock()
		}
	}
}

// StartRecording 将房间标记为正在录制新会话。
func (reg *RoomRegistry) StartRecording(roomID int64) {
	reg.setState(roomID, func(st *roomState) {
		st.record = RecordRecording
		st.sessionStartedAt = time.Now()
		st.lastError = ""
	})
}

// SetRemuxing 将房间会话标记为收尾中（正在转封装）。
func (reg *RoomRegistry) SetRemuxing(roomID int64) {
	reg.setState(roomID, func(st *roomState) { st.record = RecordRemuxing })
}

// FailRecording 将房间会话标记为失败并记录错误。
func (reg *RoomRegistry) FailRecording(roomID int64, err error) {
	reg.setState(roomID, func(st *roomState) {
		st.record = RecordError
		st.lastError = err.Error()
	})
}

// FinishRecording 在会话收尾完成后将房间恢复空闲。
func (reg *RoomRegistry) FinishRecording(roomID int64) {
	reg.setState(roomID, func(st *roomState) {
		st.record = RecordIdle
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
